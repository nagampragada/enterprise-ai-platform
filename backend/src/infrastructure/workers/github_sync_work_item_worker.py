"""One-at-a-time execution of durable GitHub file-work ledger items."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Callable
from uuid import UUID

from sqlalchemy.orm import Session

from application.services.connector_sync_retry_policy import (
    ConnectorSyncRetryPolicy,
    FailureClassification,
    SyncFailureKind,
)
from application.services.github_staged_synchronization_service import (
    GitHubSynchronizationPreparationService,
    classify_github_synchronization_failure,
)
from application.services.github_sync_work_processing_service import (
    GitHubSyncWorkProcessingService,
)
from domain.connectors.sync_work_ledger import FileWorkCounters, FileWorkLease
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    ConnectorSyncWorkLedgerRepository,
    FileWorkCancellationConflict,
    LostFileWorkLease,
    StaleFileWorkFence,
)
from infrastructure.workers.lease_heartbeat import (
    FileWorkLeaseHeartbeat,
    LeaseHeartbeatFailure,
)


SessionFactory = Callable[[], Session]
ServiceFactory = Callable[[Session], GitHubSyncWorkProcessingService]

_QUARANTINED_FAILURES = frozenset(
    {
        SyncFailureKind.VALIDATION,
        SyncFailureKind.UNSUPPORTED_CONTENT,
        SyncFailureKind.PERMANENT_PROVIDER,
    }
)

_GRACEFUL_SHUTDOWN_CLASSIFICATION = FailureClassification(
    SyncFailureKind.TRANSIENT_PERSISTENCE,
    "internal",
    "shutdown_grace_expired",
    True,
)


@dataclass(frozen=True)
class GitHubFileWorkExecution:
    """Bounded, nonsecret result from one ledger claim attempt."""

    outcome: str
    work_item_id: UUID | None = None
    attempt_number: int | None = None
    counters: FileWorkCounters = FileWorkCounters()
    reason_code: str | None = None
    organization_id: UUID | None = None
    fairness_claim_sequence: int | None = None


class FileWorkGracefulShutdownExpired(RuntimeError):
    """Raised at a safe progress boundary after the shutdown grace window."""


class GitHubSyncWorkItemWorker:
    """Claim and process at most one GitHub file per bounded operation."""

    def __init__(
        self,
        session_factory: SessionFactory,
        service_factory: ServiceFactory,
        preparation: GitHubSynchronizationPreparationService,
        retry_policy: ConnectorSyncRetryPolicy,
        *,
        worker_id: str,
        lease_duration: timedelta,
        heartbeat_interval: timedelta,
        heartbeat_shutdown_timeout: timedelta,
        recovery_limit: int,
        organization_fair_claims: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        progress_check: Callable[[], None] = lambda: None,
    ) -> None:
        self._sessions = session_factory
        self._service = service_factory
        self._preparation = preparation
        self._retry = retry_policy
        self._worker_id = worker_id
        self._lease_duration = lease_duration
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_shutdown_timeout = heartbeat_shutdown_timeout
        self._recovery_limit = recovery_limit
        self._organization_fair_claims = organization_fair_claims
        self._clock = clock
        self._progress_check = progress_check

    def execute_one(self) -> str:
        """Preserve the Slice 1 one-item outcome contract."""
        return self.execute_one_result().outcome

    def execute_one_result(
        self, *, claim_allowed: Callable[[], bool] | None = None
    ) -> GitHubFileWorkExecution:
        lease = (
            self._recover_and_claim()
            if claim_allowed is None
            else self._recover_and_claim(claim_allowed)
        )
        if lease is None:
            return GitHubFileWorkExecution("no_work")
        try:
            with FileWorkLeaseHeartbeat(
                self._sessions,
                lease,
                worker_id=self._worker_id,
                lease_duration=self._lease_duration,
                interval=self._heartbeat_interval,
                shutdown_timeout=self._heartbeat_shutdown_timeout,
            ) as heartbeat:
                def progress() -> None:
                    heartbeat.raise_if_failed()
                    self._progress_check()

                context = self._transaction(
                    lambda service: service.load_context(
                        lease,
                        worker_id=self._worker_id,
                        now=self._now(),
                        lease_duration=self._lease_duration,
                    )
                )
                progress()
                prepared = self._preparation.prepare_file(
                    context.authorization,
                    context.snapshot,
                    context.entry,
                    context.item_snapshot,
                    progress_check=progress,
                )
                progress()
                result = self._transaction(
                    lambda service: service.persist(
                        lease,
                        context,
                        prepared,
                        worker_id=self._worker_id,
                        now=self._now(),
                        lease_duration=self._lease_duration,
                    )
                )
            outcome = (
                "completed"
                if result.work_item.status.value in {"succeeded", "skipped"}
                else "failed"
            )
            return GitHubFileWorkExecution(
                outcome,
                lease.work_item_id,
                lease.attempt_number,
                result.work_item.counters,
                organization_id=lease.organization_id,
                fairness_claim_sequence=lease.fairness_claim_sequence,
            )
        except FileWorkCancellationConflict:
            return GitHubFileWorkExecution(
                self._cancel(lease), lease.work_item_id, lease.attempt_number,
                organization_id=lease.organization_id,
                fairness_claim_sequence=lease.fairness_claim_sequence,
            )
        except (LostFileWorkLease, StaleFileWorkFence):
            return GitHubFileWorkExecution(
                "lost_lease", lease.work_item_id, lease.attempt_number,
                organization_id=lease.organization_id,
                fairness_claim_sequence=lease.fairness_claim_sequence,
            )
        except LeaseHeartbeatFailure as error:
            cause = error.__cause__
            if isinstance(cause, FileWorkCancellationConflict):
                return GitHubFileWorkExecution(
                    self._cancel(lease), lease.work_item_id, lease.attempt_number,
                    organization_id=lease.organization_id,
                    fairness_claim_sequence=lease.fairness_claim_sequence,
                )
            if isinstance(cause, (LostFileWorkLease, StaleFileWorkFence)):
                return GitHubFileWorkExecution(
                    "lost_lease", lease.work_item_id, lease.attempt_number,
                    organization_id=lease.organization_id,
                    fairness_claim_sequence=lease.fairness_claim_sequence,
                )
            return self._failure_execution(lease, error)
        except Exception as error:
            return self._failure_execution(lease, error)

    def _failure_execution(
        self, lease: FileWorkLease, error: BaseException
    ) -> GitHubFileWorkExecution:
        outcome = self._fail(lease, error)
        reason_code = (
            "shutdown_grace_expired"
            if outcome == "retry_scheduled"
            and isinstance(error, FileWorkGracefulShutdownExpired)
            else None
        )
        return GitHubFileWorkExecution(
            outcome,
            lease.work_item_id,
            lease.attempt_number,
            reason_code=reason_code,
            organization_id=lease.organization_id,
            fairness_claim_sequence=lease.fairness_claim_sequence,
        )

    def _recover_and_claim(
        self, claim_allowed: Callable[[], bool] = lambda: True
    ) -> FileWorkLease | None:
        session = self._sessions()
        try:
            repository = ConnectorSyncWorkLedgerRepository(session)
            repository.recover_expired_available(
                provider_key="github",
                profile_fingerprint=self._preparation.profile.fingerprint,
                now=self._now(),
                limit=self._recovery_limit,
            )
            lease = None
            if claim_allowed():
                claim = (
                    repository.claim_next_available_fair
                    if self._organization_fair_claims
                    else repository.claim_next_available
                )
                lease = claim(
                    provider_key="github",
                    profile_fingerprint=self._preparation.profile.fingerprint,
                    worker_id=self._worker_id,
                    now=self._now(),
                    lease_duration=self._lease_duration,
                )
            session.commit()
            return lease
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _transaction(self, operation):
        session = self._sessions()
        try:
            result = operation(self._service(session))
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _cancel(self, lease: FileWorkLease) -> str:
        session = self._sessions()
        try:
            ConnectorSyncWorkLedgerRepository(session).acknowledge_cancellation(
                lease, worker_id=self._worker_id, now=self._now()
            )
            session.commit()
            return "cancelled"
        except (LostFileWorkLease, StaleFileWorkFence):
            session.rollback()
            return "lost_lease"
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _fail(self, lease: FileWorkLease, error: BaseException) -> str:
        classification = (
            _GRACEFUL_SHUTDOWN_CLASSIFICATION
            if isinstance(error, FileWorkGracefulShutdownExpired)
            else classify_github_synchronization_failure(error)
        )
        if classification.kind is SyncFailureKind.CANCELLED:
            return "lost_lease"
        now = self._now()
        retry_at = None
        quarantine_reason = None
        if classification.retryable and lease.attempt_number < lease.max_attempts:
            retry_at = now + timedelta(
                seconds=self._retry.delay_seconds(
                    attempt_count=lease.attempt_number,
                    kind=classification.kind,
                )
            )
        elif classification.kind in _QUARANTINED_FAILURES:
            quarantine_reason = classification.error_code
        session = self._sessions()
        try:
            result = ConnectorSyncWorkLedgerRepository(session).record_failure(
                lease,
                worker_id=self._worker_id,
                error_category=classification.error_category,
                error_code=classification.error_code,
                now=now,
                retry_at=retry_at,
                quarantine_reason_code=quarantine_reason,
                counters=FileWorkCounters(),
            )
            session.commit()
            if result.status.value == "retry_wait":
                return "retry_scheduled"
            if result.status.value == "quarantined":
                return "quarantined"
            return "failed"
        except FileWorkCancellationConflict:
            session.rollback()
            return self._cancel(lease)
        except (LostFileWorkLease, StaleFileWorkFence):
            session.rollback()
            return "lost_lease"
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("GitHub file-work clock is invalid")
        return value
