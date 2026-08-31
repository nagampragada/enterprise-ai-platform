"""One-at-a-time execution of durable GitHub file-work ledger items."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Callable

from sqlalchemy.orm import Session

from application.services.connector_sync_retry_policy import (
    ConnectorSyncRetryPolicy,
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
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
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
        self._clock = clock

    def execute_one(self) -> str:
        lease = self._recover_and_claim()
        if lease is None:
            return "no_work"
        try:
            with FileWorkLeaseHeartbeat(
                self._sessions,
                lease,
                worker_id=self._worker_id,
                lease_duration=self._lease_duration,
                interval=self._heartbeat_interval,
                shutdown_timeout=self._heartbeat_shutdown_timeout,
            ) as heartbeat:
                context = self._transaction(
                    lambda service: service.load_context(
                        lease,
                        worker_id=self._worker_id,
                        now=self._now(),
                        lease_duration=self._lease_duration,
                    )
                )
                heartbeat.raise_if_failed()
                prepared = self._preparation.prepare_file(
                    context.authorization,
                    context.snapshot,
                    context.entry,
                    context.item_snapshot,
                    progress_check=heartbeat.raise_if_failed,
                )
                heartbeat.raise_if_failed()
                heartbeat.stop()
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
            return "completed" if result.work_item.status.value in {"succeeded", "skipped"} else "failed"
        except FileWorkCancellationConflict:
            return self._cancel(lease)
        except (LostFileWorkLease, StaleFileWorkFence):
            return "lost_lease"
        except LeaseHeartbeatFailure as error:
            cause = error.__cause__
            if isinstance(cause, FileWorkCancellationConflict):
                return self._cancel(lease)
            if isinstance(cause, (LostFileWorkLease, StaleFileWorkFence)):
                return "lost_lease"
            return self._fail(lease, error)
        except Exception as error:
            return self._fail(lease, error)

    def _recover_and_claim(self) -> FileWorkLease | None:
        session = self._sessions()
        try:
            repository = ConnectorSyncWorkLedgerRepository(session)
            repository.recover_expired_available(
                provider_key="github",
                profile_fingerprint=self._preparation.profile.fingerprint,
                now=self._now(),
                limit=self._recovery_limit,
            )
            lease = repository.claim_next_available(
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
        finally:
            session.close()

    def _fail(self, lease: FileWorkLease, error: BaseException) -> str:
        classification = classify_github_synchronization_failure(error)
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
        finally:
            session.close()

    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("GitHub file-work clock is invalid")
        return value
