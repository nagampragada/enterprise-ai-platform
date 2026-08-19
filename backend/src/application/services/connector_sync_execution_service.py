"""Bounded control-plane orchestration for connector synchronization jobs.

Callers own every transaction. Acquisition plus run allocation occurs in one
short transaction that must be committed before provider work. Heartbeats and
terminal/retry transitions occur in separate short transactions using the
same lease identity and fencing generation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable
from uuid import UUID

from application.services.connector_sync_retry_policy import (
    ConnectorSyncRetryPolicy,
    RetryPolicyViolation,
    SyncFailureKind,
)
from infrastructure.db.models import ConnectorSyncRun
from infrastructure.repositories.connector_sync_job_repository import (
    ConnectorSyncJobRepository,
    EnqueueResult,
    SyncJobAttemptState,
    SyncJobHistoryItem,
    SyncJobLease,
)


@dataclass(frozen=True)
class AcquiredSyncAttempt:
    lease: SyncJobLease
    sync_run_id: UUID


class ConnectorSyncExecutionService:
    def __init__(
        self,
        repository: ConnectorSyncJobRepository,
        retry_policy: ConnectorSyncRetryPolicy,
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self._repository = repository
        self._retry_policy = retry_policy
        self._clock = clock

    def enqueue(
        self,
        organization_id: UUID,
        connector_id: UUID,
        connector_scope_id: UUID,
        *,
        mode: str,
        trigger_type: str,
        requested_by_user_id: UUID | None = None,
        max_attempts: int = 3,
        priority: int = 100,
    ) -> EnqueueResult:
        return self._repository.enqueue_or_coalesce(
            organization_id,
            connector_id,
            connector_scope_id,
            mode=mode,
            trigger_type=trigger_type,
            requested_by_user_id=requested_by_user_id,
            max_attempts=max_attempts,
            priority=priority,
            now=self._now(),
        )

    def acquire_one(
        self,
        organization_id: UUID,
        *,
        worker_id: str,
        lease_duration: timedelta,
        connector_id: UUID | None = None,
    ) -> AcquiredSyncAttempt | None:
        now = self._now()
        lease = self._repository.acquire_next(
            organization_id,
            worker_id=worker_id,
            lease_duration=lease_duration,
            connector_id=connector_id,
            now=now,
        )
        if lease is None:
            return None
        run: ConnectorSyncRun = self._repository.create_attempt_run(
            lease,
            worker_id=worker_id,
            now=now,
        )
        return AcquiredSyncAttempt(lease, run.id)

    def acquire_one_local_folder(
        self,
        *,
        worker_id: str,
        lease_duration: timedelta,
    ) -> AcquiredSyncAttempt | None:
        """Acquire one Local Folder attempt across tenants for the internal host."""
        now = self._now()
        lease = self._repository.acquire_next_local_folder(
            worker_id=worker_id,
            lease_duration=lease_duration,
            now=now,
        )
        if lease is None:
            return None
        run: ConnectorSyncRun = self._repository.create_attempt_run(
            lease,
            worker_id=worker_id,
            now=now,
        )
        return AcquiredSyncAttempt(lease, run.id)

    def heartbeat(
        self,
        lease: SyncJobLease,
        *,
        worker_id: str,
        lease_duration: timedelta,
    ) -> SyncJobLease:
        return self._repository.renew_heartbeat(
            lease,
            worker_id=worker_id,
            lease_duration=lease_duration,
            now=self._now(),
        )

    def validate_attempt(
        self,
        lease: SyncJobLease,
        sync_run_id: UUID,
        *,
        worker_id: str,
    ) -> SyncJobAttemptState:
        return self._repository.validate_attempt(
            lease,
            sync_run_id,
            worker_id=worker_id,
            now=self._now(),
        )

    def request_cancellation(
        self,
        organization_id: UUID,
        job_id: UUID,
        *,
        requested_by_user_id: UUID | None = None,
        reason_code: str = "user_requested",
    ) -> SyncJobHistoryItem:
        return self._repository.request_cancellation(
            organization_id,
            job_id,
            requested_by_user_id=requested_by_user_id,
            reason_code=reason_code,
            now=self._now(),
        )

    def acknowledge_cancellation(
        self,
        lease: SyncJobLease,
        *,
        worker_id: str,
    ) -> SyncJobHistoryItem:
        return self._repository.acknowledge_cancellation(
            lease,
            worker_id=worker_id,
            now=self._now(),
        )

    def complete_success(
        self,
        lease: SyncJobLease,
        *,
        worker_id: str,
    ) -> SyncJobHistoryItem:
        return self._repository.complete_success(
            lease,
            worker_id=worker_id,
            now=self._now(),
        )

    def fail_attempt(
        self,
        lease: SyncJobLease,
        *,
        worker_id: str,
        kind: SyncFailureKind,
        retry_after_seconds: float | None = None,
    ) -> SyncJobHistoryItem:
        if kind is SyncFailureKind.CANCELLED:
            raise RetryPolicyViolation("cancellation must be acknowledged explicitly")
        now = self._now()
        decision = self._retry_policy.decide(
            kind=kind,
            attempt_count=lease.attempt_number,
            max_attempts=lease.max_attempts,
            now=now,
            retry_after_seconds=retry_after_seconds,
        )
        return self._repository.record_failure(
            lease,
            worker_id=worker_id,
            now=now,
            error_category=decision.classification.error_category,
            error_code=decision.classification.error_code,
            retry_at=decision.retry_at,
        )

    def recover_expired(
        self,
        *,
        limit: int,
        organization_id: UUID | None = None,
    ) -> tuple[SyncJobHistoryItem, ...]:
        now = self._now()
        expired = self._repository.lock_expired(
            now=now,
            limit=limit,
            organization_id=organization_id,
        )
        return self._recover_expired(expired, now)

    def recover_expired_local_folder(
        self,
        *,
        limit: int,
    ) -> tuple[SyncJobHistoryItem, ...]:
        """Recover expired Local Folder attempts across tenants for the internal host."""
        now = self._now()
        expired = self._repository.lock_expired_local_folder(now=now, limit=limit)
        return self._recover_expired(expired, now)

    def _recover_expired(self, expired, now: datetime) -> tuple[SyncJobHistoryItem, ...]:
        recovered: list[SyncJobHistoryItem] = []
        for item in expired:
            retry_at = None
            if not item.cancellation_requested and item.attempt_count < item.max_attempts:
                delay = self._retry_policy.delay_seconds(
                    kind=SyncFailureKind.TRANSIENT_PERSISTENCE,
                    attempt_count=item.attempt_count,
                )
                retry_at = now + timedelta(seconds=delay)
            recovered.append(
                self._repository.recover_expired(item, now=now, retry_at=retry_at)
            )
        return tuple(recovered)

    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise RetryPolicyViolation("clock must return a timezone-aware datetime")
        return value