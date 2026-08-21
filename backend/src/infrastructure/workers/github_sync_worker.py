"""Bounded orchestration for an already-claimed GitHub synchronization attempt."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Callable

from sqlalchemy.orm import Session

from application.services.connector_sync_execution_service import ConnectorSyncExecutionService
from application.services.connector_sync_retry_policy import SyncFailureKind
from application.services.github_staged_synchronization_service import (
    GitHubStagedSynchronizationService,
    GitHubSynchronizationPreparationService,
    InvalidGitHubStagedSynchronizationRequest,
    classify_github_synchronization_failure,
)
from infrastructure.repositories.connector_sync_job_repository import (
    LostSyncJobLease,
    StaleSyncJobFence,
    SyncJobCancellationConflict,
    SyncJobLease,
)
from infrastructure.workers.lease_heartbeat import LeaseHeartbeat, LeaseHeartbeatFailure
from infrastructure.workers.local_folder_sync_worker import (
    LocalFolderAttemptContext,
    LocalFolderWorkerResult,
)

SessionFactory = Callable[[], Session]
ExecutionFactory = Callable[[Session], ConnectorSyncExecutionService]
StagedFactory = Callable[[Session], GitHubStagedSynchronizationService]


class GitHubSyncWorker:
    def __init__(
        self,
        session_factory: SessionFactory,
        execution_factory: ExecutionFactory,
        staged_factory: StagedFactory,
        preparation: GitHubSynchronizationPreparationService,
        *,
        worker_id: str,
        lease_duration: timedelta,
        heartbeat_interval: timedelta,
        heartbeat_shutdown_timeout: timedelta,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._sessions = session_factory
        self._execution = execution_factory
        self._staged = staged_factory
        self._preparation = preparation
        self._worker_id = worker_id
        self._lease_duration = lease_duration
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_shutdown_timeout = heartbeat_shutdown_timeout
        self._clock = clock

    def execute(self, context: LocalFolderAttemptContext) -> LocalFolderWorkerResult:
        lease = _lease(context)
        try:
            self._validate(lease, context.sync_run_id)
            with LeaseHeartbeat(
                self._sessions,
                self._execution,
                lease,
                worker_id=self._worker_id,
                lease_duration=self._lease_duration,
                interval=self._heartbeat_interval,
                shutdown_timeout=self._heartbeat_shutdown_timeout,
            ) as heartbeat:
                outcome = self._continue(lease, context.sync_run_id, heartbeat)
            return _result(outcome, context)
        except SyncJobCancellationConflict:
            return _result(self._cancel(lease), context)
        except (LostSyncJobLease, StaleSyncJobFence):
            return _result("lost_lease", context)
        except LeaseHeartbeatFailure as error:
            cause = error.__cause__
            if isinstance(cause, SyncJobCancellationConflict):
                return _result(self._cancel(lease), context)
            if isinstance(cause, (LostSyncJobLease, StaleSyncJobFence)):
                return _result("lost_lease", context)
            return _result(self._fail(lease, error), context)
        except Exception as error:
            return _result(self._fail(lease, error), context)

    def _continue(self, lease: SyncJobLease, run_id, heartbeat: LeaseHeartbeat) -> str:
        snapshot = self._read(lambda service: service.snapshot(
            lease, run_id, worker_id=self._worker_id
        ))
        heartbeat.raise_if_failed()
        if snapshot.cursor is None:
            self._progress(lease, run_id, heartbeat)
            cursor = self._preparation.resolve_snapshot(snapshot.authorization)
            heartbeat.raise_if_failed()
            self._write(lambda service: service.pin_snapshot(
                lease, snapshot, cursor, worker_id=self._worker_id, now=self._now()
            ), lease, run_id, heartbeat)
            return "in_progress"
        if snapshot.cursor.phase == "reconciliation":
            result = self._write(lambda service: service.reconcile(
                lease, snapshot, worker_id=self._worker_id, now=self._now()
            ), lease, run_id, heartbeat)
            return result.outcome
        if snapshot.cursor.phase == "complete":
            raise InvalidGitHubStagedSynchronizationRequest(
                "completed GitHub cursor cannot have a running lease"
            )
        progress = lambda: self._progress(lease, run_id, heartbeat)
        batch = self._preparation.discover_batch(
            snapshot.authorization, snapshot.cursor, progress_check=progress
        )
        heartbeat.raise_if_failed()
        item_snapshots = self._read(lambda service: service.item_snapshots(
            lease, snapshot, batch, worker_id=self._worker_id
        ))
        heartbeat.raise_if_failed()
        prepared = self._preparation.prepare_batch(
            snapshot.authorization, item_snapshots, batch, progress_check=progress
        )
        heartbeat.raise_if_failed()
        result = self._write(lambda service: service.persist_batch(
            lease, snapshot, prepared, worker_id=self._worker_id, now=self._now()
        ), lease, run_id, heartbeat)
        return result.outcome

    def _validate(self, lease, run_id) -> None:
        session = self._sessions()
        try:
            self._execution(session).validate_attempt(lease, run_id, worker_id=self._worker_id)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _progress(self, lease, run_id, heartbeat) -> None:
        heartbeat.raise_if_failed()
        self._validate(lease, run_id)
        heartbeat.raise_if_failed()

    def _read(self, operation):
        session = self._sessions()
        try:
            result = operation(self._staged(session))
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _write(self, operation, lease, run_id, heartbeat):
        heartbeat.raise_if_failed()
        heartbeat.stop()
        session = self._sessions()
        try:
            execution = self._execution(session)
            execution.validate_attempt(lease, run_id, worker_id=self._worker_id)
            heartbeat.raise_if_failed()
            result = operation(self._staged(session))
            heartbeat.raise_if_failed()
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _cancel(self, lease) -> str:
        session = self._sessions()
        try:
            self._execution(session).acknowledge_cancellation(lease, worker_id=self._worker_id)
            session.commit()
            return "cancelled"
        except (LostSyncJobLease, StaleSyncJobFence):
            session.rollback()
            return "lost_lease"
        finally:
            session.close()

    def _fail(self, lease, error) -> str:
        classification = classify_github_synchronization_failure(error)
        kind = classification.kind
        if kind is SyncFailureKind.CANCELLED:
            return "lost_lease"
        session = self._sessions()
        try:
            result = self._execution(session).fail_attempt(
                lease, worker_id=self._worker_id, kind=kind
            )
            session.commit()
            return "retry_scheduled" if result.status == "retry_wait" else "failed"
        except SyncJobCancellationConflict:
            session.rollback()
            return self._cancel(lease)
        except (LostSyncJobLease, StaleSyncJobFence):
            session.rollback()
            return "lost_lease"
        finally:
            session.close()

    def _now(self):
        return self._clock()


def _lease(context: LocalFolderAttemptContext) -> SyncJobLease:
    return SyncJobLease(
        context.organization_id, context.job_id, context.connector_id,
        context.connector_scope_id, context.mode, context.trigger_type,
        context.attempt_number, context.max_attempts, context.lease_id,
        context.fencing_token, context.lease_expires_at,
    )


def _result(outcome: str, context: LocalFolderAttemptContext) -> LocalFolderWorkerResult:
    return LocalFolderWorkerResult(outcome, context.job_id, context.sync_run_id,
                                   context.attempt_number, 1)
