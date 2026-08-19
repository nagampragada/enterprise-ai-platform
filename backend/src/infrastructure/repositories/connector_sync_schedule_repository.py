"""Tenant-safe persistence for recurring connector synchronization schedules."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from infrastructure.db.models import ConnectorSyncSchedule
from infrastructure.repositories.connector_repository import _require_aware, _require_uuid

MIN_INTERVAL_SECONDS = 900
MAX_INTERVAL_SECONDS = 2_592_000
PAUSE_REASONS = frozenset(
    {"administrator_paused", "connector_inactive", "scope_inactive", "connector_unsupported"}
)


class InvalidSyncScheduleRequest(ValueError):
    """Raised when schedule persistence input is invalid."""


class SyncScheduleConflict(RuntimeError):
    """Raised when a schedule mutation conflicts with persisted state."""


class SyncSchedulePersistenceError(RuntimeError):
    """Raised when schedule persistence fails safely."""


class ConnectorSyncScheduleRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def create_or_replace(
        self,
        organization_id: UUID,
        connector_id: UUID,
        connector_scope_id: UUID,
        *,
        interval_seconds: int,
        next_run_at: datetime,
        created_by_user_id: UUID,
        now: datetime,
    ) -> ConnectorSyncSchedule:
        organization_id = _uuid("organization_id", organization_id)
        connector_id = _uuid("connector_id", connector_id)
        connector_scope_id = _uuid("connector_scope_id", connector_scope_id)
        created_by_user_id = _uuid("created_by_user_id", created_by_user_id)
        interval_seconds = _interval(interval_seconds)
        next_run_at = _aware("next_run_at", next_run_at)
        now = _aware("now", now)
        statement = (
            insert(ConnectorSyncSchedule)
            .values(
                id=uuid4(),
                organization_id=organization_id,
                connector_id=connector_id,
                connector_scope_id=connector_scope_id,
                status="active",
                interval_seconds=interval_seconds,
                next_run_at=next_run_at,
                last_due_at=None,
                last_enqueued_at=None,
                last_job_id=None,
                pause_reason_code=None,
                paused_at=None,
                created_by_user_id=created_by_user_id,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_update(
                constraint="uq_sync_schedules_scope",
                set_={
                    "status": "active",
                    "interval_seconds": interval_seconds,
                    "next_run_at": next_run_at,
                    "last_due_at": None,
                    "last_enqueued_at": None,
                    "last_job_id": None,
                    "pause_reason_code": None,
                    "paused_at": None,
                    "created_by_user_id": created_by_user_id,
                    "updated_at": now,
                },
            )
            .returning(ConnectorSyncSchedule)
        )
        return self._required_updated(statement, "synchronization schedule could not be saved")

    def get(
        self,
        organization_id: UUID,
        connector_id: UUID,
        connector_scope_id: UUID,
        *,
        lock: bool = False,
    ) -> ConnectorSyncSchedule | None:
        statement = select(ConnectorSyncSchedule).where(
            ConnectorSyncSchedule.organization_id == _uuid("organization_id", organization_id),
            ConnectorSyncSchedule.connector_id == _uuid("connector_id", connector_id),
            ConnectorSyncSchedule.connector_scope_id
            == _uuid("connector_scope_id", connector_scope_id),
        )
        return self._one(statement.with_for_update() if lock else statement)

    def lock_next_due(self, *, now: datetime) -> ConnectorSyncSchedule | None:
        now = _aware("now", now)
        statement = (
            select(ConnectorSyncSchedule)
            .where(
                ConnectorSyncSchedule.status == "active",
                ConnectorSyncSchedule.next_run_at <= now,
            )
            .order_by(ConnectorSyncSchedule.next_run_at, ConnectorSyncSchedule.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        return self._one(statement)

    def pause(
        self,
        organization_id: UUID,
        connector_id: UUID,
        connector_scope_id: UUID,
        *,
        reason_code: str,
        now: datetime,
    ) -> ConnectorSyncSchedule | None:
        reason_code = _pause_reason(reason_code)
        return self._updated(
            update(ConnectorSyncSchedule)
            .where(*self._identity(organization_id, connector_id, connector_scope_id))
            .values(
                status="paused",
                pause_reason_code=reason_code,
                paused_at=_aware("now", now),
                updated_at=now,
            )
            .returning(ConnectorSyncSchedule),
            "synchronization schedule could not be paused",
        )

    def resume(
        self,
        organization_id: UUID,
        connector_id: UUID,
        connector_scope_id: UUID,
        *,
        next_run_at: datetime,
        now: datetime,
    ) -> ConnectorSyncSchedule | None:
        next_run_at = _aware("next_run_at", next_run_at)
        now = _aware("now", now)
        return self._updated(
            update(ConnectorSyncSchedule)
            .where(*self._identity(organization_id, connector_id, connector_scope_id))
            .values(
                status="active",
                next_run_at=next_run_at,
                pause_reason_code=None,
                paused_at=None,
                updated_at=now,
            )
            .returning(ConnectorSyncSchedule),
            "synchronization schedule could not be resumed",
        )

    def record_due(
        self,
        schedule: ConnectorSyncSchedule,
        *,
        due_at: datetime,
        enqueued_at: datetime,
        job_id: UUID,
        next_run_at: datetime,
    ) -> ConnectorSyncSchedule:
        if not isinstance(schedule, ConnectorSyncSchedule):
            raise InvalidSyncScheduleRequest("schedule is invalid")
        due_at = _aware("due_at", due_at)
        enqueued_at = _aware("enqueued_at", enqueued_at)
        job_id = _uuid("job_id", job_id)
        next_run_at = _aware("next_run_at", next_run_at)
        statement = (
            update(ConnectorSyncSchedule)
            .where(
                ConnectorSyncSchedule.organization_id == schedule.organization_id,
                ConnectorSyncSchedule.id == schedule.id,
                ConnectorSyncSchedule.status == "active",
                ConnectorSyncSchedule.next_run_at == due_at,
            )
            .values(
                last_due_at=due_at,
                last_enqueued_at=enqueued_at,
                last_job_id=job_id,
                next_run_at=next_run_at,
                updated_at=enqueued_at,
            )
            .returning(ConnectorSyncSchedule)
        )
        return self._required_updated(statement, "due synchronization schedule could not be advanced")

    def delete(
        self, organization_id: UUID, connector_id: UUID, connector_scope_id: UUID
    ) -> bool:
        statement = (
            delete(ConnectorSyncSchedule)
            .where(*self._identity(organization_id, connector_id, connector_scope_id))
            .returning(ConnectorSyncSchedule.id)
        )
        try:
            return self._session.execute(statement).scalar_one_or_none() is not None
        except SQLAlchemyError as exc:
            raise SyncSchedulePersistenceError("synchronization schedule could not be deleted") from exc

    @staticmethod
    def _identity(organization_id: UUID, connector_id: UUID, connector_scope_id: UUID):
        return (
            ConnectorSyncSchedule.organization_id == _uuid("organization_id", organization_id),
            ConnectorSyncSchedule.connector_id == _uuid("connector_id", connector_id),
            ConnectorSyncSchedule.connector_scope_id
            == _uuid("connector_scope_id", connector_scope_id),
        )

    def _one(self, statement):
        try:
            return self._session.execute(statement).scalar_one_or_none()
        except SQLAlchemyError as exc:
            raise SyncSchedulePersistenceError("synchronization schedule could not be read") from exc

    def _updated(self, statement, message: str):
        try:
            return self._session.execute(statement).scalar_one_or_none()
        except IntegrityError as exc:
            raise SyncScheduleConflict(message) from exc
        except SQLAlchemyError as exc:
            raise SyncSchedulePersistenceError(message) from exc

    def _required_updated(self, statement, message: str):
        row = self._updated(statement, message)
        if row is None:
            raise SyncScheduleConflict(message)
        return row


def _uuid(name: str, value: object) -> UUID:
    try:
        return _require_uuid(name, value)
    except ValueError as exc:
        raise InvalidSyncScheduleRequest(f"{name} is invalid") from exc


def _aware(name: str, value: datetime) -> datetime:
    try:
        return _require_aware(name, value)
    except ValueError as exc:
        raise InvalidSyncScheduleRequest(f"{name} is invalid") from exc


def _interval(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidSyncScheduleRequest("interval_seconds is invalid")
    if not MIN_INTERVAL_SECONDS <= value <= MAX_INTERVAL_SECONDS:
        raise InvalidSyncScheduleRequest(
            f"interval_seconds must be between {MIN_INTERVAL_SECONDS} and {MAX_INTERVAL_SECONDS}"
        )
    return value


def _pause_reason(value: object) -> str:
    if not isinstance(value, str) or value not in PAUSE_REASONS:
        raise InvalidSyncScheduleRequest("pause reason is invalid")
    return value