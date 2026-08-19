"""Recurring interval synchronization scheduling with caller-owned transactions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Callable
from uuid import UUID

from infrastructure.db.models import ConnectorSyncSchedule
from infrastructure.repositories.connector_repository import ConnectorRepository
from infrastructure.repositories.connector_scope_repository import ConnectorScopeRepository
from infrastructure.repositories.connector_sync_job_repository import ConnectorSyncJobRepository
from infrastructure.repositories.connector_sync_schedule_repository import (
    ConnectorSyncScheduleRepository,
    InvalidSyncScheduleRequest,
    MAX_INTERVAL_SECONDS,
    MIN_INTERVAL_SECONDS,
)


class SyncScheduleNotFound(RuntimeError):
    """Raised when a tenant-scoped schedule or scope is unavailable."""


class SyncScheduleResourceConflict(RuntimeError):
    """Raised when connector or scope lifecycle blocks scheduling."""


@dataclass(frozen=True)
class SyncScheduleView:
    schedule_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    status: str
    interval_seconds: int
    next_run_at: datetime
    last_due_at: datetime | None
    last_enqueued_at: datetime | None
    last_job_id: UUID | None
    pause_reason_code: str | None
    paused_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class DueScheduleResult:
    outcome: str
    schedule_id: UUID | None
    organization_id: UUID | None
    job_id: UUID | None
    coalesced: bool
    next_run_at: datetime | None


class ConnectorSyncScheduleService:
    def __init__(
        self,
        session,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._connectors = ConnectorRepository(session)
        self._scopes = ConnectorScopeRepository(session)
        self._schedules = ConnectorSyncScheduleRepository(session)
        self._jobs = ConnectorSyncJobRepository(session)
        self._clock = clock

    def create_or_replace(
        self,
        organization_id: UUID,
        creator_user_id: UUID,
        connector_id: UUID,
        connector_scope_id: UUID,
        *,
        interval_seconds: int,
        first_run_at: datetime | None = None,
    ) -> SyncScheduleView:
        now = self._now()
        interval_seconds = validate_interval_seconds(interval_seconds)
        self._require_active_local_folder(organization_id, connector_id, connector_scope_id, lock=True)
        next_run_at = now + timedelta(seconds=interval_seconds) if first_run_at is None else _aware(first_run_at)
        if next_run_at < now:
            raise InvalidSyncScheduleRequest("first_run_at cannot be earlier than now")
        return _view(
            self._schedules.create_or_replace(
                organization_id,
                connector_id,
                connector_scope_id,
                interval_seconds=interval_seconds,
                next_run_at=next_run_at,
                created_by_user_id=creator_user_id,
                now=now,
            )
        )

    def get(
        self, organization_id: UUID, connector_id: UUID, connector_scope_id: UUID
    ) -> SyncScheduleView:
        self._require_scope(organization_id, connector_id, connector_scope_id)
        schedule = self._schedules.get(organization_id, connector_id, connector_scope_id)
        if schedule is None:
            raise SyncScheduleNotFound("synchronization schedule was not found")
        return _view(schedule)

    def pause(
        self, organization_id: UUID, connector_id: UUID, connector_scope_id: UUID
    ) -> SyncScheduleView:
        self._require_scope(organization_id, connector_id, connector_scope_id)
        schedule = self._schedules.pause(
            organization_id,
            connector_id,
            connector_scope_id,
            reason_code="administrator_paused",
            now=self._now(),
        )
        if schedule is None:
            raise SyncScheduleNotFound("synchronization schedule was not found")
        return _view(schedule)

    def resume(
        self, organization_id: UUID, connector_id: UUID, connector_scope_id: UUID
    ) -> SyncScheduleView:
        self._require_active_local_folder(organization_id, connector_id, connector_scope_id, lock=True)
        current = self._schedules.get(
            organization_id, connector_id, connector_scope_id, lock=True
        )
        if current is None:
            raise SyncScheduleNotFound("synchronization schedule was not found")
        now = self._now()
        next_run_at = calculate_next_future(current.next_run_at, current.interval_seconds, now)
        schedule = self._schedules.resume(
            organization_id,
            connector_id,
            connector_scope_id,
            next_run_at=next_run_at,
            now=now,
        )
        if schedule is None:
            raise SyncScheduleNotFound("synchronization schedule was not found")
        return _view(schedule)

    def delete(
        self, organization_id: UUID, connector_id: UUID, connector_scope_id: UUID
    ) -> None:
        self._require_scope(organization_id, connector_id, connector_scope_id)
        if not self._schedules.delete(organization_id, connector_id, connector_scope_id):
            raise SyncScheduleNotFound("synchronization schedule was not found")

    def process_one_due(self) -> DueScheduleResult:
        now = self._now()
        schedule = self._schedules.lock_next_due(now=now)
        if schedule is None:
            return DueScheduleResult("no_work", None, None, None, False, None)
        connector = self._connectors.lock_by_id(schedule.organization_id, schedule.connector_id)
        scope = self._scopes.lock_by_id(schedule.organization_id, schedule.connector_scope_id)
        reason = _invalid_resource_reason(connector, scope, schedule.connector_id)
        if reason is not None:
            paused = self._schedules.pause(
                schedule.organization_id,
                schedule.connector_id,
                schedule.connector_scope_id,
                reason_code=reason,
                now=now,
            )
            if paused is None:
                raise SyncScheduleResourceConflict("due schedule could not be paused")
            return DueScheduleResult(
                "paused", schedule.id, schedule.organization_id, None, False, schedule.next_run_at
            )
        due_at = schedule.next_run_at
        result = self._jobs.enqueue_or_coalesce(
            schedule.organization_id,
            schedule.connector_id,
            schedule.connector_scope_id,
            mode="incremental",
            trigger_type="scheduled",
            now=now,
        )
        next_run_at = calculate_next_future(due_at, schedule.interval_seconds, now)
        self._schedules.record_due(
            schedule,
            due_at=due_at,
            enqueued_at=now,
            job_id=result.job_id,
            next_run_at=next_run_at,
        )
        return DueScheduleResult(
            "coalesced" if result.coalesced else "enqueued",
            schedule.id,
            schedule.organization_id,
            result.job_id,
            result.coalesced,
            next_run_at,
        )

    def _require_scope(self, organization_id: UUID, connector_id: UUID, scope_id: UUID):
        connector = self._connectors.get_by_id(organization_id, connector_id)
        scope = self._scopes.get_by_id(organization_id, scope_id)
        if connector is None or scope is None or scope.connector_id != connector_id:
            raise SyncScheduleNotFound("connector scope was not found")
        return connector, scope

    def _require_active_local_folder(
        self, organization_id: UUID, connector_id: UUID, scope_id: UUID, *, lock: bool
    ) -> None:
        connector = (
            self._connectors.lock_by_id(organization_id, connector_id)
            if lock else self._connectors.get_by_id(organization_id, connector_id)
        )
        scope = (
            self._scopes.lock_by_id(organization_id, scope_id)
            if lock else self._scopes.get_by_id(organization_id, scope_id)
        )
        if connector is None or scope is None or scope.connector_id != connector_id:
            raise SyncScheduleNotFound("connector scope was not found")
        reason = _invalid_resource_reason(connector, scope, connector_id)
        if reason is not None:
            raise SyncScheduleResourceConflict("connector scope is not schedulable")

    def _now(self) -> datetime:
        return _aware(self._clock())


def calculate_next_future(anchor: datetime, interval_seconds: int, now: datetime) -> datetime:
    anchor = _aware(anchor)
    now = _aware(now)
    interval_seconds = validate_interval_seconds(interval_seconds)
    if anchor > now:
        return anchor
    interval = timedelta(seconds=interval_seconds)
    elapsed_intervals = (now - anchor) // interval
    return anchor + interval * (elapsed_intervals + 1)


def validate_interval_seconds(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidSyncScheduleRequest("interval_seconds is invalid")
    if not MIN_INTERVAL_SECONDS <= value <= MAX_INTERVAL_SECONDS:
        raise InvalidSyncScheduleRequest(
            f"interval_seconds must be between {MIN_INTERVAL_SECONDS} and {MAX_INTERVAL_SECONDS}"
        )
    return value


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise InvalidSyncScheduleRequest("time must be timezone-aware")
    return value


def _invalid_resource_reason(connector, scope, connector_id: UUID) -> str | None:
    if connector is None or connector.status != "active":
        return "connector_inactive"
    if connector.connector_type != "local_folder":
        return "connector_unsupported"
    if scope is None or scope.connector_id != connector_id or scope.status != "active":
        return "scope_inactive"
    return None


def _view(row: ConnectorSyncSchedule) -> SyncScheduleView:
    return SyncScheduleView(
        row.id,
        row.connector_id,
        row.connector_scope_id,
        row.status,
        row.interval_seconds,
        row.next_run_at,
        row.last_due_at,
        row.last_enqueued_at,
        row.last_job_id,
        row.pause_reason_code,
        row.paused_at,
        row.created_at,
        row.updated_at,
    )