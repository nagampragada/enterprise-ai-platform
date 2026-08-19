"""Bounded transaction runner for one Local Folder synchronization attempt."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Callable
from uuid import UUID

from sqlalchemy.orm import Session

from application.services.connector_sync_execution_service import (
    AcquiredSyncAttempt,
    ConnectorSyncExecutionService,
)
from application.services.connector_sync_retry_policy import SyncFailureKind, classify_exception
from application.services.staged_local_folder_synchronization_service import (
    LocalFolderDiscoveredEntry,
    LocalFolderPreparationService,
    LocalFolderSynchronizationSnapshot,
    PreparedLocalFolderItem,
    StagedLocalFolderSynchronizationService,
)
from infrastructure.repositories.connector_sync_job_repository import (
    LostSyncJobLease,
    StaleSyncJobFence,
    SyncJobCancellationConflict,
    SyncJobLease,
)

DEFAULT_LEASE_DURATION = timedelta(minutes=5)
DEFAULT_HEARTBEAT_TARGET = timedelta(seconds=60)
DEFAULT_STEPS_PER_INVOCATION = 1
MAX_STEPS_PER_INVOCATION = 10
DEFAULT_BATCH_SIZE = 100
_WORKER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$")


class InvalidLocalFolderWorkerConfiguration(ValueError):
    """Raised when bounded worker configuration is invalid."""


class UnsupportedLocalFolderJob(RuntimeError):
    """Raised when a claimed job cannot be dispatched to Local Folder."""


@dataclass(frozen=True)
class LocalFolderAttemptContext:
    organization_id: UUID
    job_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    sync_run_id: UUID
    attempt_number: int
    worker_id: str
    lease_id: UUID
    fencing_token: int
    lease_expires_at: datetime
    mode: str
    trigger_type: str
    max_attempts: int


@dataclass(frozen=True)
class LocalFolderWorkerResult:
    outcome: str
    job_id: UUID | None
    sync_run_id: UUID | None
    attempt_number: int | None
    steps_processed: int


ExecutionServiceFactory = Callable[[Session], ConnectorSyncExecutionService]
StagedLocalFolderServiceFactory = Callable[[Session], StagedLocalFolderSynchronizationService]
SessionFactory = Callable[[], Session]


class LocalFolderSyncWorker:
    """Run at most one claimed attempt with a caller-bounded continuation budget."""

    def __init__(
        self,
        session_factory: SessionFactory,
        execution_service_factory: ExecutionServiceFactory,
        staged_service_factory: StagedLocalFolderServiceFactory,
        preparation_service: LocalFolderPreparationService,
        *,
        worker_id: str,
        lease_duration: timedelta = DEFAULT_LEASE_DURATION,
        heartbeat_target: timedelta = DEFAULT_HEARTBEAT_TARGET,
        steps_per_invocation: int = DEFAULT_STEPS_PER_INVOCATION,
        batch_size: int = DEFAULT_BATCH_SIZE,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._session_factory = session_factory
        self._execution_service_factory = execution_service_factory
        self._staged_service_factory = staged_service_factory
        self._preparation = preparation_service
        self._worker_id = _worker_identifier(worker_id)
        self._lease_duration = _positive_duration("lease_duration", lease_duration, maximum=3600)
        self._heartbeat_target = _positive_duration(
            "heartbeat_target", heartbeat_target, maximum=self._lease_duration.total_seconds()
        )
        self._steps_per_invocation = _bounded_integer(
            "steps_per_invocation", steps_per_invocation, MAX_STEPS_PER_INVOCATION
        )
        self._batch_size = _bounded_integer("batch_size", batch_size, 100)
        self._clock = clock

    def run_one(self, organization_id: UUID) -> LocalFolderWorkerResult:
        context = self.claim_one(organization_id)
        if context is None:
            return LocalFolderWorkerResult("no_work", None, None, None, 0)
        return self.execute(context)

    def claim_one(self, organization_id: UUID) -> LocalFolderAttemptContext | None:
        session = self._session_factory()
        try:
            acquired = self._execution_service_factory(session).acquire_one(
                organization_id,
                worker_id=self._worker_id,
                lease_duration=self._lease_duration,
            )
            if acquired is None:
                session.rollback()
                return None
            session.commit()
            return self.attempt_context(acquired)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def attempt_context(self, acquired: AcquiredSyncAttempt) -> LocalFolderAttemptContext:
        if not isinstance(acquired, AcquiredSyncAttempt):
            raise InvalidLocalFolderWorkerConfiguration("acquired attempt is invalid")
        lease = acquired.lease
        return LocalFolderAttemptContext(
            lease.organization_id,
            lease.job_id,
            lease.connector_id,
            lease.connector_scope_id,
            acquired.sync_run_id,
            lease.attempt_number,
            self._worker_id,
            lease.lease_id,
            lease.fencing_token,
            lease.lease_expires_at,
            lease.mode,
            lease.trigger_type,
            lease.max_attempts,
        )

    def heartbeat(self, context: LocalFolderAttemptContext) -> LocalFolderAttemptContext:
        _context(context, self._worker_id)
        session = self._session_factory()
        try:
            renewed = self._execution_service_factory(session).heartbeat(
                _lease(context),
                worker_id=self._worker_id,
                lease_duration=self._lease_duration,
            )
            session.commit()
            return replace(context, lease_expires_at=renewed.lease_expires_at)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def execute(self, context: LocalFolderAttemptContext) -> LocalFolderWorkerResult:
        context = _context(context, self._worker_id)
        steps = 0
        for _ in range(self._steps_per_invocation):
            try:
                context = self.heartbeat(context)
            except SyncJobCancellationConflict:
                return _result(self._acknowledge_cancellation(context), context, steps)
            except (LostSyncJobLease, StaleSyncJobFence):
                return _result("lost_lease", context, steps)
            outcome, context = self._execute_step(context)
            steps += 1
            if outcome != "in_progress":
                return _result(outcome, context, steps)
        return _result("in_progress", context, steps)

    def _execute_step(
        self, context: LocalFolderAttemptContext
    ) -> tuple[str, LocalFolderAttemptContext]:
        try:
            snapshot = self._load_snapshot(context)
            if snapshot.phase == "completed":
                return "completed", context
            if snapshot.phase == "reconciliation":
                outcome = self._persist_reconciliation(context, snapshot)
                return outcome, context
            entry = self._preparation.discover_next(snapshot)
            prepared: PreparedLocalFolderItem | None = None
            if entry is not None:
                item_snapshot = self._load_item_snapshot(context, snapshot, entry)
                prepared = self._preparation.prepare_item(snapshot, item_snapshot, entry)
            outcome = self._persist_discovery(context, snapshot, prepared)
            return outcome, context
        except SyncJobCancellationConflict:
            return self._acknowledge_cancellation(context), context
        except (LostSyncJobLease, StaleSyncJobFence):
            return "lost_lease", context
        except Exception as error:
            return self._record_failure(context, error), context

    def _load_snapshot(
        self, context: LocalFolderAttemptContext
    ) -> LocalFolderSynchronizationSnapshot:
        session = self._session_factory()
        try:
            snapshot = self._staged_service_factory(session).snapshot(
                _lease(context), context.sync_run_id,
                worker_id=self._worker_id, now=self._now(),
            )
            session.commit()
            return snapshot
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _load_item_snapshot(
        self,
        context: LocalFolderAttemptContext,
        snapshot: LocalFolderSynchronizationSnapshot,
        entry: LocalFolderDiscoveredEntry,
    ):
        session = self._session_factory()
        try:
            result = self._staged_service_factory(session).item_snapshot(
                _lease(context), snapshot, entry, worker_id=self._worker_id
            )
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _persist_discovery(
        self,
        context: LocalFolderAttemptContext,
        snapshot: LocalFolderSynchronizationSnapshot,
        prepared: PreparedLocalFolderItem | None,
    ) -> str:
        session = self._session_factory()
        try:
            result = self._staged_service_factory(session).persist_discovery(
                _lease(context), snapshot, prepared,
                worker_id=self._worker_id, now=self._now(),
            )
            self._execution_service_factory(session).heartbeat(
                _lease(context),
                worker_id=self._worker_id,
                lease_duration=self._lease_duration,
            )
            session.commit()
            return "in_progress" if result.outcome != "completed" else "completed"
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _persist_reconciliation(
        self, context: LocalFolderAttemptContext, snapshot: LocalFolderSynchronizationSnapshot
    ) -> str:
        session = self._session_factory()
        try:
            result = self._staged_service_factory(session).reconcile(
                _lease(context), snapshot, worker_id=self._worker_id,
                now=self._now(), limit=self._batch_size,
            )
            if result.outcome != "completed":
                self._execution_service_factory(session).heartbeat(
                    _lease(context),
                    worker_id=self._worker_id,
                    lease_duration=self._lease_duration,
                )
            session.commit()
            return result.outcome
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _acknowledge_cancellation(self, context: LocalFolderAttemptContext) -> str:
        session = self._session_factory()
        try:
            self._execution_service_factory(session).acknowledge_cancellation(
                _lease(context), worker_id=self._worker_id
            )
            session.commit()
            return "cancelled"
        except (LostSyncJobLease, StaleSyncJobFence):
            session.rollback()
            return "lost_lease"
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _record_failure(self, context: LocalFolderAttemptContext, error: BaseException) -> str:
        classification = _classify_chain(error)
        session = self._session_factory()
        acknowledge = False
        try:
            result = self._execution_service_factory(session).fail_attempt(
                _lease(context),
                worker_id=self._worker_id,
                kind=classification,
            )
            session.commit()
            return "retry_scheduled" if result.status == "retry_wait" else "failed"
        except SyncJobCancellationConflict:
            session.rollback()
            acknowledge = True
        except (LostSyncJobLease, StaleSyncJobFence):
            session.rollback()
            return "lost_lease"
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        if acknowledge:
            return self._acknowledge_cancellation(context)
        raise RuntimeError("failure outcome was not resolved")

    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise InvalidLocalFolderWorkerConfiguration("clock must return timezone-aware time")
        return value


def _classify_chain(error: BaseException) -> SyncFailureKind:
    current: BaseException | None = error
    fallback = SyncFailureKind.UNKNOWN_INTERNAL
    for _ in range(8):
        if current is None:
            break
        classification = classify_exception(current)
        if classification.kind is not SyncFailureKind.UNKNOWN_INTERNAL:
            return classification.kind
        fallback = classification.kind
        current = current.__cause__
    return fallback


def _lease(context: LocalFolderAttemptContext) -> SyncJobLease:
    return SyncJobLease(
        context.organization_id,
        context.job_id,
        context.connector_id,
        context.connector_scope_id,
        context.mode,
        context.trigger_type,
        context.attempt_number,
        context.max_attempts,
        context.lease_id,
        context.fencing_token,
        context.lease_expires_at,
    )


def _context(value: object, worker_id: str) -> LocalFolderAttemptContext:
    if not isinstance(value, LocalFolderAttemptContext) or value.worker_id != worker_id:
        raise InvalidLocalFolderWorkerConfiguration("attempt context is invalid")
    return value


def _result(
    outcome: str, context: LocalFolderAttemptContext, steps: int
) -> LocalFolderWorkerResult:
    return LocalFolderWorkerResult(
        outcome, context.job_id, context.sync_run_id, context.attempt_number, steps
    )


def _worker_identifier(value: object) -> str:
    if not isinstance(value, str) or not _WORKER_ID.fullmatch(value):
        raise InvalidLocalFolderWorkerConfiguration("worker_id is invalid")
    return value


def _positive_duration(name: str, value: object, *, maximum: float) -> timedelta:
    if not isinstance(value, timedelta):
        raise InvalidLocalFolderWorkerConfiguration(f"{name} is invalid")
    seconds = value.total_seconds()
    if seconds <= 0 or seconds > maximum:
        raise InvalidLocalFolderWorkerConfiguration(f"{name} is outside the allowed range")
    return value


def _bounded_integer(name: str, value: object, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise InvalidLocalFolderWorkerConfiguration(f"{name} must be between 1 and {maximum}")
    return value