"""Tenant-safe durable connector synchronization job execution control."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable
from uuid import UUID, uuid4

from sqlalchemy import and_, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from infrastructure.db.models import Connector, ConnectorSyncJob, ConnectorSyncRun
from infrastructure.repositories.connector_repository import (
    _require_aware,
    _require_choice,
    _require_code,
    _require_limit,
    _require_uuid,
)

DEFAULT_MAX_ATTEMPTS = 3
HARD_MAX_ATTEMPTS = 5
MAX_LEASE_SECONDS = 60 * 60
MAX_RECOVERY_LIMIT = 100
NONTERMINAL_STATUSES = frozenset({"queued", "running", "retry_wait"})
JOB_STATUSES = NONTERMINAL_STATUSES | frozenset({"succeeded", "failed", "cancelled"})
JOB_MODES = frozenset({"initial", "incremental", "reconciliation"})
JOB_TRIGGERS = frozenset({"manual", "scheduled", "webhook", "system"})
ERROR_CATEGORIES = frozenset(
    {
        "configuration",
        "authentication",
        "authorization",
        "rate_limit",
        "source_read",
        "extraction",
        "persistence",
        "embedding",
        "permission",
        "internal",
    }
)
_WORKER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$")


class InvalidSyncJobRequest(ValueError):
    """Raised before SQL execution when execution-control input is invalid."""


class SyncJobConflict(RuntimeError):
    """Raised when a requested mutation conflicts with durable job state."""


class SyncJobNotFound(RuntimeError):
    """Raised when a tenant-scoped job does not exist."""


class LostSyncJobLease(RuntimeError):
    """Raised when lease ownership or validity can no longer be proven."""


class StaleSyncJobFence(LostSyncJobLease):
    """Raised when a worker presents an obsolete fencing generation."""


class InvalidSyncJobTransition(SyncJobConflict):
    """Raised when a lifecycle transition is not valid."""


class SyncJobCancellationConflict(SyncJobConflict):
    """Raised when cancellation prevents a worker-owned transition."""


class SyncJobPersistenceError(RuntimeError):
    """Raised when execution-control persistence fails safely."""


@dataclass(frozen=True)
class EnqueueResult:
    job_id: UUID
    status: str
    coalesced: bool


@dataclass(frozen=True)
class SyncJobLease:
    organization_id: UUID
    job_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    mode: str
    trigger_type: str
    attempt_number: int
    max_attempts: int
    lease_id: UUID
    fencing_token: int
    lease_expires_at: datetime


@dataclass(frozen=True)
class SyncJobHistoryItem:
    job_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    mode: str
    trigger_type: str
    status: str
    priority: int
    attempt_count: int
    max_attempts: int
    next_attempt_at: datetime | None
    cancellation_requested: bool
    completed_at: datetime | None
    last_error_category: str | None
    last_error_code: str | None
    created_at: datetime


@dataclass(frozen=True)
class SyncJobPageCursor:
    created_at: datetime
    job_id: UUID


@dataclass(frozen=True)
class SyncJobPage:
    items: tuple[SyncJobHistoryItem, ...]
    limit: int
    has_more: bool
    next_cursor: SyncJobPageCursor | None


@dataclass(frozen=True)
class ExpiredSyncJobLease:
    organization_id: UUID
    job_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    attempt_count: int
    max_attempts: int
    lease_id: UUID
    fencing_token: int
    cancellation_requested: bool


@dataclass(frozen=True)
class SyncJobAttemptState:
    organization_id: UUID
    job_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    sync_run_id: UUID
    attempt_number: int
    cancellation_requested: bool


class ConnectorSyncJobRepository:
    def __init__(
        self,
        session: Session,
        *,
        lease_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._session = session
        self._lease_id_factory = lease_id_factory

    def enqueue_or_coalesce(
        self,
        organization_id: UUID,
        connector_id: UUID,
        connector_scope_id: UUID,
        *,
        mode: str,
        trigger_type: str,
        now: datetime,
        requested_by_user_id: UUID | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        priority: int = 100,
    ) -> EnqueueResult:
        organization_id = _uuid("organization_id", organization_id)
        connector_id = _uuid("connector_id", connector_id)
        connector_scope_id = _uuid("connector_scope_id", connector_scope_id)
        mode = _choice("mode", mode, JOB_MODES)
        trigger_type = _choice("trigger_type", trigger_type, JOB_TRIGGERS)
        now = _aware("now", now)
        requester = (
            _uuid("requested_by_user_id", requested_by_user_id)
            if requested_by_user_id is not None
            else None
        )
        max_attempts = _max_attempts(max_attempts)
        priority = _priority(priority)
        job_id = uuid4()
        statement = (
            insert(ConnectorSyncJob)
            .values(
                id=job_id,
                organization_id=organization_id,
                connector_id=connector_id,
                connector_scope_id=connector_scope_id,
                mode=mode,
                trigger_type=trigger_type,
                status="queued",
                requested_by_user_id=requester,
                priority=priority,
                attempt_count=0,
                max_attempts=max_attempts,
                next_attempt_at=now,
                fencing_token=0,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing(
                index_elements=["organization_id", "connector_scope_id"],
                index_where=text("status IN ('queued', 'running', 'retry_wait')"),
            )
            .returning(ConnectorSyncJob.id)
        )
        try:
            created_id = self._session.execute(statement).scalar_one_or_none()
            if created_id is not None:
                return EnqueueResult(created_id, "queued", False)
            existing = self._session.execute(
                select(ConnectorSyncJob.id, ConnectorSyncJob.status).where(
                    ConnectorSyncJob.organization_id == organization_id,
                    ConnectorSyncJob.connector_id == connector_id,
                    ConnectorSyncJob.connector_scope_id == connector_scope_id,
                    ConnectorSyncJob.status.in_(NONTERMINAL_STATUSES),
                )
            ).one_or_none()
        except IntegrityError as exc:
            raise SyncJobConflict("synchronization job could not be enqueued") from exc
        except SQLAlchemyError as exc:
            raise SyncJobPersistenceError("synchronization job could not be enqueued") from exc
        if existing is None:
            raise SyncJobConflict("synchronization job could not be coalesced")
        return EnqueueResult(existing.id, existing.status, True)

    def acquire_next(
        self,
        organization_id: UUID,
        *,
        worker_id: str,
        lease_duration: timedelta,
        now: datetime,
        connector_id: UUID | None = None,
    ) -> SyncJobLease | None:
        organization_id = _uuid("organization_id", organization_id)
        worker_id = _worker_id(worker_id)
        now = _aware("now", now)
        duration = _lease_duration(lease_duration)
        connector = _uuid("connector_id", connector_id) if connector_id is not None else None
        lease_id = self._lease_uuid()
        candidate = select(ConnectorSyncJob.id).where(
            ConnectorSyncJob.organization_id == organization_id,
            ConnectorSyncJob.status.in_(("queued", "retry_wait")),
            ConnectorSyncJob.next_attempt_at <= now,
            ConnectorSyncJob.cancel_requested_at.is_(None),
            ConnectorSyncJob.attempt_count < ConnectorSyncJob.max_attempts,
        )
        if connector is not None:
            candidate = candidate.where(ConnectorSyncJob.connector_id == connector)
        candidate = (
            candidate.order_by(
                ConnectorSyncJob.priority,
                ConnectorSyncJob.next_attempt_at,
                ConnectorSyncJob.created_at,
                ConnectorSyncJob.id,
            )
            .with_for_update(skip_locked=True)
            .limit(1)
            .cte("eligible_sync_job")
        )
        statement = (
            update(ConnectorSyncJob)
            .where(
                ConnectorSyncJob.id == candidate.c.id,
                ConnectorSyncJob.organization_id == organization_id,
                ConnectorSyncJob.status.in_(("queued", "retry_wait")),
                ConnectorSyncJob.next_attempt_at <= now,
                ConnectorSyncJob.cancel_requested_at.is_(None),
                ConnectorSyncJob.attempt_count < ConnectorSyncJob.max_attempts,
            )
            .values(
                status="running",
                attempt_count=ConnectorSyncJob.attempt_count + 1,
                fencing_token=ConnectorSyncJob.fencing_token + 1,
                next_attempt_at=None,
                lease_owner=worker_id,
                lease_id=lease_id,
                lease_acquired_at=now,
                lease_expires_at=now + duration,
                heartbeat_at=now,
                updated_at=now,
            )
            .returning(ConnectorSyncJob)
        )
        row = self._updated(statement, "synchronization job could not be acquired")
        return _lease(row) if row is not None else None

    def acquire_next_local_folder(
        self,
        *,
        worker_id: str,
        lease_duration: timedelta,
        now: datetime,
    ) -> SyncJobLease | None:
        """Claim one Local Folder job across tenants for the internal worker host."""
        worker_id = _worker_id(worker_id)
        now = _aware("now", now)
        duration = _lease_duration(lease_duration)
        lease_id = self._lease_uuid()
        local_folder = select(Connector.id).where(
            Connector.organization_id == ConnectorSyncJob.organization_id,
            Connector.id == ConnectorSyncJob.connector_id,
            Connector.connector_type == "local_folder",
        ).exists()
        candidate = (
            select(ConnectorSyncJob.id)
            .where(
                local_folder,
                ConnectorSyncJob.status.in_(("queued", "retry_wait")),
                ConnectorSyncJob.next_attempt_at <= now,
                ConnectorSyncJob.cancel_requested_at.is_(None),
                ConnectorSyncJob.attempt_count < ConnectorSyncJob.max_attempts,
            )
            .order_by(
                ConnectorSyncJob.priority,
                ConnectorSyncJob.next_attempt_at,
                ConnectorSyncJob.created_at,
                ConnectorSyncJob.id,
            )
            .with_for_update(of=ConnectorSyncJob, skip_locked=True)
            .limit(1)
            .cte("eligible_local_folder_sync_job")
        )
        statement = (
            update(ConnectorSyncJob)
            .where(
                ConnectorSyncJob.id == candidate.c.id,
                local_folder,
                ConnectorSyncJob.status.in_(("queued", "retry_wait")),
                ConnectorSyncJob.next_attempt_at <= now,
                ConnectorSyncJob.cancel_requested_at.is_(None),
                ConnectorSyncJob.attempt_count < ConnectorSyncJob.max_attempts,
            )
            .values(
                status="running",
                attempt_count=ConnectorSyncJob.attempt_count + 1,
                fencing_token=ConnectorSyncJob.fencing_token + 1,
                next_attempt_at=None,
                lease_owner=worker_id,
                lease_id=lease_id,
                lease_acquired_at=now,
                lease_expires_at=now + duration,
                heartbeat_at=now,
                updated_at=now,
            )
            .returning(ConnectorSyncJob)
        )
        row = self._updated(statement, "Local Folder synchronization job could not be acquired")
        return _lease(row) if row is not None else None

    def renew_heartbeat(
        self,
        lease: SyncJobLease,
        *,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> SyncJobLease:
        lease = _valid_lease(lease)
        worker_id = _worker_id(worker_id)
        now = _aware("now", now)
        duration = _lease_duration(lease_duration)
        statement = (
            update(ConnectorSyncJob)
            .where(
                *_ownership_predicates(lease, worker_id, now),
                ConnectorSyncJob.cancel_requested_at.is_(None),
            )
            .values(heartbeat_at=now, lease_expires_at=now + duration, updated_at=now)
            .returning(ConnectorSyncJob)
        )
        row = self._updated(statement, "synchronization heartbeat could not be persisted")
        if row is None:
            self._raise_lost_lease(lease, worker_id, cancellation=True)
        return _lease(row)

    def validate_attempt(
        self,
        lease: SyncJobLease,
        sync_run_id: UUID,
        *,
        worker_id: str,
        now: datetime,
    ) -> SyncJobAttemptState:
        lease = _valid_lease(lease)
        sync_run_id = _uuid("sync_run_id", sync_run_id)
        worker_id = _worker_id(worker_id)
        now = _aware("now", now)
        statement = (
            select(ConnectorSyncJob, ConnectorSyncRun.id)
            .join(
                ConnectorSyncRun,
                and_(
                    ConnectorSyncRun.organization_id == ConnectorSyncJob.organization_id,
                    ConnectorSyncRun.connector_id == ConnectorSyncJob.connector_id,
                    ConnectorSyncRun.connector_scope_id == ConnectorSyncJob.connector_scope_id,
                    ConnectorSyncRun.sync_job_id == ConnectorSyncJob.id,
                    ConnectorSyncRun.job_attempt_number == ConnectorSyncJob.attempt_count,
                ),
            )
            .where(
                *_ownership_predicates(lease, worker_id, now),
                ConnectorSyncRun.id == sync_run_id,
                ConnectorSyncRun.status == "running",
                ConnectorSyncJob.cancel_requested_at.is_(None),
            )
            .with_for_update(of=ConnectorSyncJob)
        )
        try:
            row = self._session.execute(statement).one_or_none()
        except SQLAlchemyError as exc:
            raise SyncJobPersistenceError("synchronization attempt could not be validated") from exc
        if row is None:
            self._raise_lost_lease(lease, worker_id, cancellation=True)
        job, run_id = row
        return SyncJobAttemptState(
            job.organization_id,
            job.id,
            job.connector_id,
            job.connector_scope_id,
            run_id,
            job.attempt_count,
            job.cancel_requested_at is not None,
        )

    def request_cancellation(
        self,
        organization_id: UUID,
        job_id: UUID,
        *,
        now: datetime,
        requested_by_user_id: UUID | None = None,
        reason_code: str = "user_requested",
    ) -> SyncJobHistoryItem:
        organization_id = _uuid("organization_id", organization_id)
        job_id = _uuid("job_id", job_id)
        now = _aware("now", now)
        requester = (
            _uuid("requested_by_user_id", requested_by_user_id)
            if requested_by_user_id is not None
            else None
        )
        reason_code = _code("reason_code", reason_code)
        row = self._one(
            select(ConnectorSyncJob)
            .where(
                ConnectorSyncJob.organization_id == organization_id,
                ConnectorSyncJob.id == job_id,
            )
            .with_for_update(),
            "synchronization job could not be read",
        )
        if row is None:
            raise SyncJobNotFound("synchronization job was not found")
        if row.cancel_requested_at is not None:
            return _history(row)
        if row.status in {"succeeded", "failed"}:
            raise InvalidSyncJobTransition("terminal synchronization job cannot be cancelled")
        if row.status == "cancelled":
            return _history(row)
        values: dict[str, object] = {
            "cancel_requested_at": now,
            "cancel_requested_by_user_id": requester,
            "cancel_reason_code": reason_code,
            "updated_at": now,
        }
        if row.status in {"queued", "retry_wait"}:
            values.update(status="cancelled", completed_at=now, next_attempt_at=None)
        statement = (
            update(ConnectorSyncJob)
            .where(
                ConnectorSyncJob.organization_id == organization_id,
                ConnectorSyncJob.id == job_id,
                ConnectorSyncJob.status == row.status,
                ConnectorSyncJob.cancel_requested_at.is_(None),
            )
            .values(**values)
            .returning(ConnectorSyncJob)
        )
        updated = self._updated(statement, "cancellation request could not be persisted")
        if updated is None:
            raise SyncJobCancellationConflict("cancellation request conflicted with job state")
        return _history(updated)

    def acknowledge_cancellation(
        self,
        lease: SyncJobLease,
        *,
        worker_id: str,
        now: datetime,
    ) -> SyncJobHistoryItem:
        lease = _valid_lease(lease)
        worker_id = _worker_id(worker_id)
        now = _aware("now", now)
        statement = (
            update(ConnectorSyncJob)
            .where(
                *_ownership_predicates(lease, worker_id, now),
                ConnectorSyncJob.cancel_requested_at.is_not(None),
            )
            .values(
                status="cancelled",
                completed_at=now,
                next_attempt_at=None,
                **_cleared_lease(),
                updated_at=now,
            )
            .returning(ConnectorSyncJob)
        )
        row = self._updated(statement, "cancellation acknowledgement could not be persisted")
        if row is None:
            self._raise_lost_lease(lease, worker_id, cancellation=True)
        self._finish_attempt_run(lease, status="cancelled", now=now)
        return _history(row)

    def complete_success(
        self,
        lease: SyncJobLease,
        *,
        worker_id: str,
        now: datetime,
    ) -> SyncJobHistoryItem:
        lease = _valid_lease(lease)
        worker_id = _worker_id(worker_id)
        now = _aware("now", now)
        statement = (
            update(ConnectorSyncJob)
            .where(
                *_ownership_predicates(lease, worker_id, now),
                ConnectorSyncJob.cancel_requested_at.is_(None),
            )
            .values(
                status="succeeded",
                completed_at=now,
                next_attempt_at=None,
                **_cleared_lease(),
                updated_at=now,
            )
            .returning(ConnectorSyncJob)
        )
        row = self._updated(statement, "synchronization completion could not be persisted")
        if row is None:
            self._raise_lost_lease(lease, worker_id, cancellation=True)
        self._finish_attempt_run(lease, status="completed", now=now)
        return _history(row)

    def record_failure(
        self,
        lease: SyncJobLease,
        *,
        worker_id: str,
        now: datetime,
        error_category: str,
        error_code: str,
        retry_at: datetime | None,
    ) -> SyncJobHistoryItem:
        lease = _valid_lease(lease)
        worker_id = _worker_id(worker_id)
        now = _aware("now", now)
        category = _choice("error_category", error_category, ERROR_CATEGORIES)
        code = _code("error_code", error_code)
        if retry_at is not None:
            retry_at = _aware("retry_at", retry_at)
            if retry_at < now:
                raise InvalidSyncJobRequest("retry_at cannot be earlier than now")
            if lease.attempt_number >= lease.max_attempts:
                raise InvalidSyncJobRequest("retry cannot exceed maximum attempts")
        target = "retry_wait" if retry_at is not None else "failed"
        statement = (
            update(ConnectorSyncJob)
            .where(
                *_ownership_predicates(lease, worker_id, now),
                ConnectorSyncJob.cancel_requested_at.is_(None),
                ConnectorSyncJob.attempt_count == lease.attempt_number,
                or_(
                    retry_at is None,
                    ConnectorSyncJob.attempt_count < ConnectorSyncJob.max_attempts,
                ),
            )
            .values(
                status=target,
                next_attempt_at=retry_at,
                completed_at=now if target == "failed" else None,
                last_error_category=category,
                last_error_code=code,
                last_error_summary=None,
                **_cleared_lease(),
                updated_at=now,
            )
            .returning(ConnectorSyncJob)
        )
        row = self._updated(statement, "synchronization failure could not be persisted")
        if row is None:
            self._raise_lost_lease(lease, worker_id, cancellation=True)
        self._finish_attempt_run(lease, status="failed", now=now)
        return _history(row)

    def lock_expired(
        self,
        *,
        now: datetime,
        limit: int,
        organization_id: UUID | None = None,
    ) -> tuple[ExpiredSyncJobLease, ...]:
        now = _aware("now", now)
        limit = _recovery_limit(limit)
        organization = (
            _uuid("organization_id", organization_id) if organization_id is not None else None
        )
        statement = select(ConnectorSyncJob).where(
            ConnectorSyncJob.status == "running",
            ConnectorSyncJob.lease_expires_at <= now,
        )
        if organization is not None:
            statement = statement.where(ConnectorSyncJob.organization_id == organization)
        statement = (
            statement.order_by(ConnectorSyncJob.lease_expires_at, ConnectorSyncJob.id)
            .with_for_update(skip_locked=True)
            .limit(limit)
        )
        rows = self._all(statement, "expired synchronization jobs could not be read")
        return tuple(_expired(row) for row in rows)

    def lock_expired_local_folder(
        self,
        *,
        now: datetime,
        limit: int,
    ) -> tuple[ExpiredSyncJobLease, ...]:
        """Lock expired Local Folder jobs across tenants for internal recovery."""
        now = _aware("now", now)
        limit = _recovery_limit(limit)
        local_folder = select(Connector.id).where(
            Connector.organization_id == ConnectorSyncJob.organization_id,
            Connector.id == ConnectorSyncJob.connector_id,
            Connector.connector_type == "local_folder",
        ).exists()
        statement = (
            select(ConnectorSyncJob)
            .where(
                local_folder,
                ConnectorSyncJob.status == "running",
                ConnectorSyncJob.lease_expires_at <= now,
            )
            .order_by(ConnectorSyncJob.lease_expires_at, ConnectorSyncJob.id)
            .with_for_update(of=ConnectorSyncJob, skip_locked=True)
            .limit(limit)
        )
        rows = self._all(statement, "expired Local Folder synchronization jobs could not be read")
        return tuple(_expired(row) for row in rows)

    def recover_expired(
        self,
        expired: ExpiredSyncJobLease,
        *,
        now: datetime,
        retry_at: datetime | None,
    ) -> SyncJobHistoryItem:
        expired = _valid_expired(expired)
        now = _aware("now", now)
        if expired.cancellation_requested and retry_at is not None:
            raise InvalidSyncJobRequest("cancelled expired job cannot retry")
        if retry_at is not None:
            retry_at = _aware("retry_at", retry_at)
            if retry_at < now or expired.attempt_count >= expired.max_attempts:
                raise InvalidSyncJobRequest("expired-job retry is invalid")
        target = (
            "cancelled"
            if expired.cancellation_requested
            else "retry_wait" if retry_at is not None else "failed"
        )
        values: dict[str, object] = {
            "status": target,
            "next_attempt_at": retry_at,
            "completed_at": now if target in {"failed", "cancelled"} else None,
            **_cleared_lease(),
            "updated_at": now,
        }
        if target != "cancelled":
            values.update(
                last_error_category="internal",
                last_error_code="lease_expired",
                last_error_summary=None,
            )
        statement = (
            update(ConnectorSyncJob)
            .where(
                ConnectorSyncJob.organization_id == expired.organization_id,
                ConnectorSyncJob.id == expired.job_id,
                ConnectorSyncJob.status == "running",
                ConnectorSyncJob.lease_id == expired.lease_id,
                ConnectorSyncJob.fencing_token == expired.fencing_token,
                ConnectorSyncJob.attempt_count == expired.attempt_count,
                ConnectorSyncJob.lease_expires_at <= now,
            )
            .values(**values)
            .returning(ConnectorSyncJob)
        )
        row = self._updated(statement, "expired synchronization job could not be recovered")
        if row is None:
            raise LostSyncJobLease("expired synchronization lease is no longer recoverable")
        lease = SyncJobLease(
            expired.organization_id,
            expired.job_id,
            expired.connector_id,
            expired.connector_scope_id,
            "incremental",
            "system",
            expired.attempt_count,
            expired.max_attempts,
            expired.lease_id,
            expired.fencing_token,
            now,
        )
        self._finish_attempt_run(
            lease,
            status="cancelled" if target == "cancelled" else "failed",
            now=now,
        )
        return _history(row)

    def create_attempt_run(
        self,
        lease: SyncJobLease,
        *,
        worker_id: str,
        now: datetime,
    ) -> ConnectorSyncRun:
        lease = _valid_lease(lease)
        worker_id = _worker_id(worker_id)
        now = _aware("now", now)
        job = self._one(
            select(ConnectorSyncJob)
            .where(*_ownership_predicates(lease, worker_id, now))
            .with_for_update(),
            "synchronization job could not be locked",
        )
        if job is None:
            self._raise_lost_lease(lease, worker_id)
        if job.cancel_requested_at is not None:
            raise SyncJobCancellationConflict("cancelled synchronization cannot allocate a run")
        run = ConnectorSyncRun(
            id=uuid4(),
            organization_id=lease.organization_id,
            connector_id=lease.connector_id,
            connector_scope_id=lease.connector_scope_id,
            sync_job_id=lease.job_id,
            job_attempt_number=lease.attempt_number,
            mode=job.mode if lease.attempt_number == 1 else "retry",
            trigger_type=job.trigger_type if lease.attempt_number == 1 else "retry",
            status="running",
            initiated_by_user_id=job.requested_by_user_id,
            started_at=now,
            heartbeat_at=now,
            run_metadata={"orchestrator": "connector_sync_job", "schema_version": 1},
        )
        self._session.add(run)
        try:
            self._session.flush()
        except IntegrityError as exc:
            raise SyncJobConflict("synchronization attempt run already exists") from exc
        except SQLAlchemyError as exc:
            raise SyncJobPersistenceError("synchronization attempt run could not be created") from exc
        return run

    def get(self, organization_id: UUID, job_id: UUID) -> SyncJobHistoryItem | None:
        row = self._one(
            select(ConnectorSyncJob).where(
                ConnectorSyncJob.organization_id == _uuid("organization_id", organization_id),
                ConnectorSyncJob.id == _uuid("job_id", job_id),
            ),
            "synchronization job could not be read",
        )
        return _history(row) if row is not None else None

    def get_current(
        self,
        organization_id: UUID,
        connector_scope_id: UUID,
    ) -> SyncJobHistoryItem | None:
        row = self._one(
            select(ConnectorSyncJob).where(
                ConnectorSyncJob.organization_id == _uuid("organization_id", organization_id),
                ConnectorSyncJob.connector_scope_id
                == _uuid("connector_scope_id", connector_scope_id),
                ConnectorSyncJob.status.in_(NONTERMINAL_STATUSES),
            ),
            "current synchronization job could not be read",
        )
        return _history(row) if row is not None else None

    def list_history(
        self,
        organization_id: UUID,
        *,
        limit: int = 100,
        cursor: SyncJobPageCursor | None = None,
        connector_id: UUID | None = None,
        connector_scope_id: UUID | None = None,
        status: str | None = None,
    ) -> SyncJobPage:
        organization_id = _uuid("organization_id", organization_id)
        limit = _page_limit(limit)
        _cursor(cursor)
        statement = select(ConnectorSyncJob).where(
            ConnectorSyncJob.organization_id == organization_id
        )
        if connector_id is not None:
            statement = statement.where(
                ConnectorSyncJob.connector_id == _uuid("connector_id", connector_id)
            )
        if connector_scope_id is not None:
            statement = statement.where(
                ConnectorSyncJob.connector_scope_id
                == _uuid("connector_scope_id", connector_scope_id)
            )
        if status is not None:
            statement = statement.where(
                ConnectorSyncJob.status == _choice("status", status, JOB_STATUSES)
            )
        if cursor is not None:
            statement = statement.where(
                or_(
                    ConnectorSyncJob.created_at > cursor.created_at,
                    and_(
                        ConnectorSyncJob.created_at == cursor.created_at,
                        ConnectorSyncJob.id > cursor.job_id,
                    ),
                )
            )
        rows = self._all(
            statement.order_by(ConnectorSyncJob.created_at, ConnectorSyncJob.id).limit(limit + 1),
            "synchronization job history could not be read",
        )
        items = tuple(_history(row) for row in rows[:limit])
        has_more = len(rows) > limit
        next_cursor = (
            SyncJobPageCursor(items[-1].created_at, items[-1].job_id)
            if has_more and items
            else None
        )
        return SyncJobPage(items, limit, has_more, next_cursor)

    def _finish_attempt_run(self, lease: SyncJobLease, *, status: str, now: datetime) -> None:
        values: dict[str, object] = {"status": status, "finished_at": now, "heartbeat_at": now}
        if status == "cancelled":
            values["cancel_requested_at"] = now
        statement = (
            update(ConnectorSyncRun)
            .where(
                ConnectorSyncRun.organization_id == lease.organization_id,
                ConnectorSyncRun.sync_job_id == lease.job_id,
                ConnectorSyncRun.job_attempt_number == lease.attempt_number,
                ConnectorSyncRun.status.in_(("running", "cancelling")),
            )
            .values(**values)
        )
        try:
            result = self._session.execute(statement)
        except SQLAlchemyError as exc:
            raise SyncJobPersistenceError("synchronization attempt history could not be finalized") from exc
        if result.rowcount == 1:
            return
        try:
            existing_status = self._session.execute(
                select(ConnectorSyncRun.status).where(
                    ConnectorSyncRun.organization_id == lease.organization_id,
                    ConnectorSyncRun.sync_job_id == lease.job_id,
                    ConnectorSyncRun.job_attempt_number == lease.attempt_number,
                )
            ).scalar_one_or_none()
        except SQLAlchemyError as exc:
            raise SyncJobPersistenceError("synchronization attempt history could not be verified") from exc
        if existing_status != status:
            raise SyncJobPersistenceError("synchronization attempt history could not be finalized")

    def _raise_lost_lease(
        self,
        lease: SyncJobLease,
        worker_id: str,
        *,
        cancellation: bool = False,
    ) -> None:
        try:
            row = self._session.execute(
                select(
                    ConnectorSyncJob.status,
                    ConnectorSyncJob.lease_owner,
                    ConnectorSyncJob.lease_id,
                    ConnectorSyncJob.fencing_token,
                    ConnectorSyncJob.cancel_requested_at,
                ).where(
                    ConnectorSyncJob.organization_id == lease.organization_id,
                    ConnectorSyncJob.id == lease.job_id,
                )
            ).one_or_none()
        except SQLAlchemyError as exc:
            raise SyncJobPersistenceError("synchronization lease could not be verified") from exc
        if row is not None and row.fencing_token != lease.fencing_token:
            raise StaleSyncJobFence("synchronization fencing token is stale")
        if (
            row is None
            or row.status != "running"
            or row.lease_owner != worker_id
            or row.lease_id != lease.lease_id
        ):
            raise LostSyncJobLease("synchronization lease is no longer owned")
        if cancellation and row is not None and row.cancel_requested_at is not None:
            raise SyncJobCancellationConflict("synchronization cancellation is pending")
        raise LostSyncJobLease("synchronization lease is no longer owned")

    def _updated(self, statement, message: str) -> ConnectorSyncJob | None:
        try:
            return self._session.execute(statement).scalar_one_or_none()
        except IntegrityError as exc:
            raise SyncJobConflict(message) from exc
        except SQLAlchemyError as exc:
            raise SyncJobPersistenceError(message) from exc

    def _one(self, statement, message: str):
        try:
            return self._session.execute(statement).scalar_one_or_none()
        except SQLAlchemyError as exc:
            raise SyncJobPersistenceError(message) from exc

    def _all(self, statement, message: str) -> list[ConnectorSyncJob]:
        try:
            return list(self._session.execute(statement).scalars().all())
        except SQLAlchemyError as exc:
            raise SyncJobPersistenceError(message) from exc

    def _lease_uuid(self) -> UUID:
        value = self._lease_id_factory()
        if not isinstance(value, UUID):
            raise InvalidSyncJobRequest("lease identity factory returned an invalid value")
        return value


def _ownership_predicates(lease: SyncJobLease, worker_id: str, now: datetime):
    return (
        ConnectorSyncJob.organization_id == lease.organization_id,
        ConnectorSyncJob.id == lease.job_id,
        ConnectorSyncJob.status == "running",
        ConnectorSyncJob.lease_owner == worker_id,
        ConnectorSyncJob.lease_id == lease.lease_id,
        ConnectorSyncJob.fencing_token == lease.fencing_token,
        ConnectorSyncJob.attempt_count == lease.attempt_number,
        ConnectorSyncJob.lease_expires_at > now,
    )


def _cleared_lease() -> dict[str, None]:
    return {
        "lease_owner": None,
        "lease_id": None,
        "lease_acquired_at": None,
        "lease_expires_at": None,
        "heartbeat_at": None,
    }


def _lease(row: ConnectorSyncJob) -> SyncJobLease:
    if row.lease_id is None or row.lease_expires_at is None:
        raise SyncJobPersistenceError("acquired synchronization lease is incomplete")
    return SyncJobLease(
        row.organization_id,
        row.id,
        row.connector_id,
        row.connector_scope_id,
        row.mode,
        row.trigger_type,
        row.attempt_count,
        row.max_attempts,
        row.lease_id,
        row.fencing_token,
        row.lease_expires_at,
    )


def _history(row: ConnectorSyncJob) -> SyncJobHistoryItem:
    return SyncJobHistoryItem(
        row.id,
        row.connector_id,
        row.connector_scope_id,
        row.mode,
        row.trigger_type,
        row.status,
        row.priority,
        row.attempt_count,
        row.max_attempts,
        row.next_attempt_at,
        row.cancel_requested_at is not None,
        row.completed_at,
        row.last_error_category,
        row.last_error_code,
        row.created_at,
    )


def _expired(row: ConnectorSyncJob) -> ExpiredSyncJobLease:
    if row.lease_id is None:
        raise SyncJobPersistenceError("expired synchronization lease is incomplete")
    return ExpiredSyncJobLease(
        row.organization_id,
        row.id,
        row.connector_id,
        row.connector_scope_id,
        row.attempt_count,
        row.max_attempts,
        row.lease_id,
        row.fencing_token,
        row.cancel_requested_at is not None,
    )


def _valid_lease(value: object) -> SyncJobLease:
    if not isinstance(value, SyncJobLease):
        raise InvalidSyncJobRequest("synchronization lease is invalid")
    _uuid("organization_id", value.organization_id)
    _uuid("job_id", value.job_id)
    _uuid("connector_id", value.connector_id)
    _uuid("connector_scope_id", value.connector_scope_id)
    _uuid("lease_id", value.lease_id)
    _max_attempts(value.max_attempts)
    if (
        value.attempt_number < 1
        or value.attempt_number > value.max_attempts
        or value.fencing_token != value.attempt_number
    ):
        raise InvalidSyncJobRequest("synchronization lease generation is invalid")
    _aware("lease_expires_at", value.lease_expires_at)
    return value


def _valid_expired(value: object) -> ExpiredSyncJobLease:
    if not isinstance(value, ExpiredSyncJobLease):
        raise InvalidSyncJobRequest("expired synchronization lease is invalid")
    _uuid("organization_id", value.organization_id)
    _uuid("job_id", value.job_id)
    _uuid("connector_id", value.connector_id)
    _uuid("connector_scope_id", value.connector_scope_id)
    _uuid("lease_id", value.lease_id)
    _max_attempts(value.max_attempts)
    if (
        value.attempt_count < 1
        or value.attempt_count > value.max_attempts
        or value.fencing_token != value.attempt_count
    ):
        raise InvalidSyncJobRequest("expired synchronization generation is invalid")
    return value


def _uuid(name: str, value: object) -> UUID:
    try:
        return _require_uuid(name, value)
    except ValueError as exc:
        raise InvalidSyncJobRequest(f"{name} is invalid") from exc


def _aware(name: str, value: datetime) -> datetime:
    try:
        return _require_aware(name, value)
    except ValueError as exc:
        raise InvalidSyncJobRequest(f"{name} is invalid") from exc


def _choice(name: str, value: object, choices: frozenset[str]) -> str:
    try:
        return _require_choice(name, value, choices)
    except ValueError as exc:
        raise InvalidSyncJobRequest(f"{name} is invalid") from exc


def _code(name: str, value: object) -> str:
    try:
        return _require_code(name, value)
    except ValueError as exc:
        raise InvalidSyncJobRequest(f"{name} is invalid") from exc


def _worker_id(value: object) -> str:
    if not isinstance(value, str) or not _WORKER_ID.fullmatch(value):
        raise InvalidSyncJobRequest("worker_id is invalid")
    return value


def _max_attempts(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= HARD_MAX_ATTEMPTS:
        raise InvalidSyncJobRequest(f"max_attempts must be between 1 and {HARD_MAX_ATTEMPTS}")
    return value


def _priority(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not -32768 <= value <= 32767:
        raise InvalidSyncJobRequest("priority is invalid")
    return value


def _lease_duration(value: object) -> timedelta:
    if not isinstance(value, timedelta):
        raise InvalidSyncJobRequest("lease_duration is invalid")
    seconds = value.total_seconds()
    if seconds <= 0 or seconds > MAX_LEASE_SECONDS:
        raise InvalidSyncJobRequest("lease_duration is outside the allowed range")
    return value


def _page_limit(value: object) -> int:
    try:
        return _require_limit(value)
    except ValueError as exc:
        raise InvalidSyncJobRequest("limit is invalid") from exc


def _recovery_limit(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_RECOVERY_LIMIT:
        raise InvalidSyncJobRequest(f"limit must be between 1 and {MAX_RECOVERY_LIMIT}")
    return value


def _cursor(value: SyncJobPageCursor | None) -> None:
    if value is None:
        return
    if not isinstance(value, SyncJobPageCursor):
        raise InvalidSyncJobRequest("cursor is invalid")
    _aware("cursor.created_at", value.created_at)
    _uuid("cursor.job_id", value.job_id)
