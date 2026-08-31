"""Tenant-safe durable repository generation and file-work control ledger."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Callable
from uuid import UUID, uuid4

from sqlalchemy import func, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from domain.connectors.sync_work_ledger import (
    FileWorkCounters,
    FileWorkItemView,
    FileWorkLease,
    FileWorkMaterialization,
    FileWorkMaterializationView,
    FileWorkManifestEntry,
    FileWorkStatus,
    GenerationBarrierSummary,
    ManifestRegistrationResult,
    MAX_MANIFEST_BATCH_SIZE,
    RepositoryGenerationRegistration,
    RepositoryGenerationStatus,
    RepositoryGenerationView,
    TERMINAL_FILE_WORK_STATUSES,
    TERMINAL_GENERATION_STATUSES,
)
from infrastructure.db.models import (
    ConnectorSyncFileMaterialization,
    ConnectorSyncFileMaterializationChunk,
    ConnectorSyncFileWorkItem,
    ConnectorSyncGeneration,
    ConnectorSyncJob,
)


MAX_CLAIM_LIMIT = 500
MAX_LEASE_SECONDS = 3600
FAILURE_CATEGORIES = frozenset(
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


class InvalidSyncWorkLedgerRequest(ValueError):
    """Raised when work-ledger input violates the bounded contract."""


class SyncWorkLedgerNotFound(RuntimeError):
    """Raised when tenant-qualified generation or work is unavailable."""


class SyncWorkLedgerConflict(RuntimeError):
    """Raised when durable work state conflicts with the requested operation."""


class SyncWorkLedgerPersistenceError(RuntimeError):
    """Raised when a work-ledger database operation fails."""


class LostFileWorkLease(SyncWorkLedgerConflict):
    """Raised when file work is no longer owned by the supplied lease."""


class StaleFileWorkFence(LostFileWorkLease):
    """Raised when a stale file worker presents an earlier fencing token."""


class FileWorkCancellationConflict(SyncWorkLedgerConflict):
    """Raised when ordinary work completion races a cancellation request."""


class ConnectorSyncWorkLedgerRepository:
    """Caller-transaction-owned control plane; not wired to a live worker."""

    def __init__(
        self,
        session: Session,
        *,
        generation_id_factory: Callable[[], UUID] = uuid4,
        work_item_id_factory: Callable[[], UUID] = uuid4,
        lease_id_factory: Callable[[], UUID] = uuid4,
        materialization_id_factory: Callable[[], UUID] = uuid4,
        materialization_chunk_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._session = session
        self._generation_id_factory = generation_id_factory
        self._work_item_id_factory = work_item_id_factory
        self._lease_id_factory = lease_id_factory
        self._materialization_id_factory = materialization_id_factory
        self._materialization_chunk_id_factory = materialization_chunk_id_factory

    def register_generation(
        self, request: RepositoryGenerationRegistration
    ) -> tuple[RepositoryGenerationView, bool]:
        if not isinstance(request, RepositoryGenerationRegistration):
            raise InvalidSyncWorkLedgerRequest("generation registration is invalid")
        job = self._one(
            select(ConnectorSyncJob)
            .where(
                ConnectorSyncJob.organization_id == request.organization_id,
                ConnectorSyncJob.connector_id == request.connector_id,
                ConnectorSyncJob.connector_scope_id == request.connector_scope_id,
                ConnectorSyncJob.id == request.sync_job_id,
            )
            .with_for_update(),
            "generation job validation failed",
        )
        if job is None:
            raise SyncWorkLedgerNotFound("generation job context was not found")
        existing = self._one(
            select(ConnectorSyncGeneration).where(
                ConnectorSyncGeneration.organization_id == request.organization_id,
                ConnectorSyncGeneration.sync_job_id == request.sync_job_id,
            ),
            "generation lookup failed",
        )
        if existing is not None:
            if not _generation_matches(existing, request):
                raise SyncWorkLedgerConflict("sync job is already bound to another generation")
            return _generation_view(existing), False
        row = ConnectorSyncGeneration(
            id=self._new_uuid("generation_id", self._generation_id_factory),
            organization_id=request.organization_id,
            connector_id=request.connector_id,
            connector_scope_id=request.connector_scope_id,
            sync_job_id=request.sync_job_id,
            provider_key=request.provider_key,
            repository_identity=request.repository_identity,
            branch_name=request.branch_name,
            commit_object_id=request.commit_object_id,
            root_tree_object_id=request.root_tree_object_id,
            profile_fingerprint=request.profile_fingerprint,
            status=RepositoryGenerationStatus.DISCOVERING.value,
            discovery_complete=False,
            reconciliation_eligible=False,
            resync_required=False,
            items_discovered=0,
            items_registered=0,
            declared_bytes=0,
            created_at=request.created_at,
            updated_at=request.created_at,
        )
        self._session.add(row)
        self._flush("generation could not be registered")
        return _generation_view(row), True

    def get_generation(
        self, organization_id: UUID, generation_id: UUID
    ) -> RepositoryGenerationView | None:
        row = self._generation(organization_id, generation_id)
        return _generation_view(row) if row is not None else None

    def register_manifest(
        self,
        organization_id: UUID,
        generation_id: UUID,
        entries: Sequence[FileWorkManifestEntry],
        *,
        now: datetime,
    ) -> ManifestRegistrationResult:
        organization_id = _uuid("organization_id", organization_id)
        generation_id = _uuid("generation_id", generation_id)
        now = _aware("now", now)
        if isinstance(entries, (str, bytes)) or not isinstance(entries, Sequence) or not entries:
            raise InvalidSyncWorkLedgerRequest("manifest entries must be a nonempty sequence")
        if len(entries) > MAX_MANIFEST_BATCH_SIZE:
            raise InvalidSyncWorkLedgerRequest(
                f"manifest batch must not exceed {MAX_MANIFEST_BATCH_SIZE} entries"
            )
        if any(not isinstance(entry, FileWorkManifestEntry) for entry in entries):
            raise InvalidSyncWorkLedgerRequest("manifest entry is invalid")
        generation = self._locked_generation(organization_id, generation_id)
        if generation is None:
            raise SyncWorkLedgerNotFound("generation was not found")
        if generation.status in {status.value for status in TERMINAL_GENERATION_STATUSES}:
            raise SyncWorkLedgerConflict("terminal generation cannot accept manifest work")

        keyed: dict[tuple[str, str, str, str], FileWorkManifestEntry] = {}
        by_source_hash: dict[str, FileWorkManifestEntry] = {}
        for entry in entries:
            if entry.profile_fingerprint != generation.profile_fingerprint:
                raise InvalidSyncWorkLedgerRequest("manifest profile does not match generation")
            identity = _manifest_identity(entry)
            previous = keyed.get(identity)
            if previous is not None and previous != entry:
                raise SyncWorkLedgerConflict("manifest logical identity collision")
            keyed[identity] = entry
            previous_source = by_source_hash.get(identity[0])
            if previous_source is not None and previous_source != entry:
                raise SyncWorkLedgerConflict("manifest source identity collision")
            by_source_hash[identity[0]] = entry

        logical_keys = tuple(keyed)
        existing_sources = self._all(
            select(ConnectorSyncFileWorkItem).where(
                ConnectorSyncFileWorkItem.organization_id == organization_id,
                ConnectorSyncFileWorkItem.generation_id == generation_id,
                ConnectorSyncFileWorkItem.source_key_hash.in_(tuple(by_source_hash)),
            ),
            "manifest source identity lookup failed",
        )
        for row in existing_sources:
            if not _manifest_row_matches(row, by_source_hash[row.source_key_hash]):
                raise SyncWorkLedgerConflict(
                    "manifest source identity resolves to different attributes"
                )

        if generation.discovery_complete:
            rows = self._all(
                select(ConnectorSyncFileWorkItem).where(
                    ConnectorSyncFileWorkItem.organization_id == organization_id,
                    ConnectorSyncFileWorkItem.generation_id == generation_id,
                    tuple_(
                        ConnectorSyncFileWorkItem.source_key_hash,
                        ConnectorSyncFileWorkItem.provider_blob_id,
                        ConnectorSyncFileWorkItem.provider_revision_id,
                        ConnectorSyncFileWorkItem.profile_fingerprint,
                    ).in_(logical_keys),
                ),
                "completed manifest replay lookup failed",
            )
            by_key = {
                (
                    row.source_key_hash,
                    row.provider_blob_id,
                    row.provider_revision_id,
                    row.profile_fingerprint,
                ): row
                for row in rows
            }
            if set(by_key) != set(logical_keys):
                raise SyncWorkLedgerConflict(
                    "completed discovery cannot accept new manifest work"
                )
            for identity, entry in keyed.items():
                if not _manifest_row_matches(by_key[identity], entry):
                    raise SyncWorkLedgerConflict(
                        "manifest identity resolves to different attributes"
                    )
            return ManifestRegistrationResult(
                generation.id,
                0,
                len(logical_keys),
                tuple(by_key[key].id for key in logical_keys),
            )

        values = [
            {
                "id": self._new_uuid("work_item_id", self._work_item_id_factory),
                "organization_id": generation.organization_id,
                "connector_id": generation.connector_id,
                "connector_scope_id": generation.connector_scope_id,
                "generation_id": generation.id,
                "source_item_key": entry.source_item_key,
                "source_key_hash": identity[0],
                "repository_path": entry.repository_path,
                "provider_blob_id": entry.provider_blob_id,
                "provider_revision_id": entry.provider_revision_id,
                "profile_fingerprint": entry.profile_fingerprint,
                "file_size_bytes": entry.file_size_bytes,
                "file_extension": entry.file_extension,
                "mime_type": entry.mime_type,
                "status": FileWorkStatus.PENDING.value,
                "attempt_count": 0,
                "max_attempts": entry.max_attempts,
                "next_attempt_at": now,
                "fencing_token": 0,
                "downloaded_bytes": 0,
                "extracted_characters": 0,
                "chunk_count": 0,
                "embedding_batch_count": 0,
                "created_at": now,
                "updated_at": now,
            }
            for identity, entry in keyed.items()
        ]
        statement = (
            insert(ConnectorSyncFileWorkItem)
            .values(values)
            .on_conflict_do_nothing(constraint="uq_sync_file_work_logical_identity")
            .returning(
                ConnectorSyncFileWorkItem.id,
                ConnectorSyncFileWorkItem.source_key_hash,
                ConnectorSyncFileWorkItem.provider_blob_id,
                ConnectorSyncFileWorkItem.provider_revision_id,
                ConnectorSyncFileWorkItem.profile_fingerprint,
            )
        )
        try:
            inserted = self._session.execute(statement).all()
        except SQLAlchemyError as exc:
            raise SyncWorkLedgerPersistenceError("manifest registration failed") from exc
        rows = self._all(
            select(ConnectorSyncFileWorkItem).where(
                ConnectorSyncFileWorkItem.organization_id == organization_id,
                ConnectorSyncFileWorkItem.generation_id == generation_id,
                tuple_(
                    ConnectorSyncFileWorkItem.source_key_hash,
                    ConnectorSyncFileWorkItem.provider_blob_id,
                    ConnectorSyncFileWorkItem.provider_revision_id,
                    ConnectorSyncFileWorkItem.profile_fingerprint,
                ).in_(logical_keys),
            ),
            "registered manifest lookup failed",
        )
        by_key = {
            (
                row.source_key_hash,
                row.provider_blob_id,
                row.provider_revision_id,
                row.profile_fingerprint,
            ): row
            for row in rows
        }
        if set(by_key) != set(logical_keys):
            raise SyncWorkLedgerPersistenceError("registered manifest is incomplete")
        for identity, entry in keyed.items():
            row = by_key[identity]
            if not _manifest_row_matches(row, entry):
                raise SyncWorkLedgerConflict("manifest identity resolves to different attributes")
        inserted_keys = {
            (row.source_key_hash, row.provider_blob_id, row.provider_revision_id, row.profile_fingerprint)
            for row in inserted
        }
        generation.items_discovered += len(inserted_keys)
        generation.items_registered += len(inserted_keys)
        generation.declared_bytes += sum(
            keyed[key].file_size_bytes or 0 for key in inserted_keys
        )
        generation.updated_at = now
        self._flush("manifest counters could not be updated")
        ordered_ids = tuple(by_key[key].id for key in logical_keys)
        return ManifestRegistrationResult(
            generation.id,
            len(inserted_keys),
            len(logical_keys) - len(inserted_keys),
            ordered_ids,
        )

    def mark_discovery_complete(
        self, organization_id: UUID, generation_id: UUID, *, now: datetime
    ) -> RepositoryGenerationView:
        now = _aware("now", now)
        row = self._locked_generation(_uuid("organization_id", organization_id), _uuid("generation_id", generation_id))
        if row is None:
            raise SyncWorkLedgerNotFound("generation was not found")
        if row.status in {status.value for status in TERMINAL_GENERATION_STATUSES}:
            raise SyncWorkLedgerConflict("terminal generation cannot complete discovery")
        if not row.discovery_complete:
            row.discovery_complete = True
            row.discovery_completed_at = now
            row.status = RepositoryGenerationStatus.PROCESSING.value
            row.updated_at = now
            self._flush("generation discovery could not be completed")
        return _generation_view(row)

    def require_follow_up(
        self, organization_id: UUID, generation_id: UUID, *, now: datetime
    ) -> RepositoryGenerationView:
        now = _aware("now", now)
        row = self._locked_generation(_uuid("organization_id", organization_id), _uuid("generation_id", generation_id))
        if row is None:
            raise SyncWorkLedgerNotFound("generation was not found")
        if not row.resync_required:
            row.resync_required = True
            row.resync_requested_at = now
            row.updated_at = now
            self._flush("generation follow-up intent could not be recorded")
        return _generation_view(row)

    def claim_next(
        self,
        organization_id: UUID,
        generation_id: UUID,
        *,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> FileWorkLease | None:
        organization_id = _uuid("organization_id", organization_id)
        generation_id = _uuid("generation_id", generation_id)
        worker_id = _worker_id(worker_id)
        now = _aware("now", now)
        lease_duration = _lease_duration(lease_duration)
        statement = (
            select(ConnectorSyncFileWorkItem)
            .join(
                ConnectorSyncGeneration,
                (ConnectorSyncGeneration.organization_id == ConnectorSyncFileWorkItem.organization_id)
                & (ConnectorSyncGeneration.id == ConnectorSyncFileWorkItem.generation_id),
            )
            .where(
                ConnectorSyncFileWorkItem.organization_id == organization_id,
                ConnectorSyncFileWorkItem.generation_id == generation_id,
                ConnectorSyncFileWorkItem.status.in_(
                    (FileWorkStatus.PENDING.value, FileWorkStatus.RETRY_WAIT.value)
                ),
                ConnectorSyncFileWorkItem.next_attempt_at <= now,
                ConnectorSyncFileWorkItem.cancel_requested_at.is_(None),
                ConnectorSyncFileWorkItem.attempt_count < ConnectorSyncFileWorkItem.max_attempts,
                ConnectorSyncGeneration.status.in_(
                    (RepositoryGenerationStatus.DISCOVERING.value, RepositoryGenerationStatus.PROCESSING.value)
                ),
            )
            .order_by(
                ConnectorSyncFileWorkItem.next_attempt_at,
                ConnectorSyncFileWorkItem.id,
            )
            .with_for_update(of=ConnectorSyncFileWorkItem, skip_locked=True)
            .limit(1)
        )
        return self._claim_one(statement, worker_id=worker_id, now=now, lease_duration=lease_duration)

    def claim_next_available(
        self,
        *,
        provider_key: str,
        profile_fingerprint: str,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> FileWorkLease | None:
        """Claim one ready item across tenants after immutable discovery completes."""
        provider_key = _code("provider_key", provider_key, 64)
        profile_fingerprint = _identifier(
            "profile_fingerprint", profile_fingerprint, 255
        )
        worker_id = _worker_id(worker_id)
        now = _aware("now", now)
        lease_duration = _lease_duration(lease_duration)
        statement = (
            select(ConnectorSyncFileWorkItem)
            .join(
                ConnectorSyncGeneration,
                (ConnectorSyncGeneration.organization_id == ConnectorSyncFileWorkItem.organization_id)
                & (ConnectorSyncGeneration.id == ConnectorSyncFileWorkItem.generation_id),
            )
            .join(
                ConnectorSyncJob,
                (ConnectorSyncJob.organization_id == ConnectorSyncGeneration.organization_id)
                & (ConnectorSyncJob.connector_id == ConnectorSyncGeneration.connector_id)
                & (ConnectorSyncJob.connector_scope_id == ConnectorSyncGeneration.connector_scope_id)
                & (ConnectorSyncJob.id == ConnectorSyncGeneration.sync_job_id),
            )
            .where(
                ConnectorSyncGeneration.provider_key == provider_key,
                ConnectorSyncGeneration.profile_fingerprint == profile_fingerprint,
                ConnectorSyncGeneration.status == RepositoryGenerationStatus.PROCESSING.value,
                ConnectorSyncGeneration.discovery_complete.is_(True),
                ConnectorSyncJob.status != "cancelled",
                ConnectorSyncJob.cancel_requested_at.is_(None),
                ConnectorSyncFileWorkItem.status.in_(
                    (FileWorkStatus.PENDING.value, FileWorkStatus.RETRY_WAIT.value)
                ),
                ConnectorSyncFileWorkItem.next_attempt_at <= now,
                ConnectorSyncFileWorkItem.cancel_requested_at.is_(None),
                ConnectorSyncFileWorkItem.attempt_count < ConnectorSyncFileWorkItem.max_attempts,
            )
            .order_by(
                ConnectorSyncGeneration.created_at,
                ConnectorSyncGeneration.id,
                ConnectorSyncFileWorkItem.next_attempt_at,
                ConnectorSyncFileWorkItem.id,
            )
            .with_for_update(of=ConnectorSyncFileWorkItem, skip_locked=True)
            .limit(1)
        )
        return self._claim_one(statement, worker_id=worker_id, now=now, lease_duration=lease_duration)

    def _claim_one(
        self,
        statement,
        *,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> FileWorkLease | None:
        row = self._one(statement, "file work claim failed")
        if row is None:
            return None
        lease_id = self._new_uuid("lease_id", self._lease_id_factory)
        row.status = FileWorkStatus.RUNNING.value
        row.attempt_count += 1
        row.fencing_token += 1
        row.next_attempt_at = None
        row.lease_owner = worker_id
        row.lease_id = lease_id
        row.lease_acquired_at = now
        row.lease_expires_at = now + lease_duration
        row.heartbeat_at = now
        row.last_error_category = None
        row.last_error_code = None
        row.quarantine_reason_code = None
        row.downloaded_bytes = 0
        row.extracted_characters = 0
        row.chunk_count = 0
        row.embedding_batch_count = 0
        row.updated_at = now
        self._flush("file work could not be claimed")
        return _lease(row)

    def heartbeat(
        self,
        lease: FileWorkLease,
        *,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> FileWorkLease:
        now = _aware("now", now)
        row = self._locked_lease(lease, worker_id=worker_id, now=now)
        if row.cancel_requested_at is not None:
            raise FileWorkCancellationConflict("file work cancellation is pending")
        row.heartbeat_at = now
        row.lease_expires_at = now + _lease_duration(lease_duration)
        row.updated_at = now
        self._flush("file work heartbeat failed")
        return _lease(row)

    def complete(
        self,
        lease: FileWorkLease,
        *,
        worker_id: str,
        outcome: FileWorkStatus,
        counters: FileWorkCounters,
        now: datetime,
    ) -> FileWorkItemView:
        if outcome not in {FileWorkStatus.SUCCEEDED, FileWorkStatus.SKIPPED}:
            raise InvalidSyncWorkLedgerRequest("file work completion outcome is invalid")
        if not isinstance(counters, FileWorkCounters):
            raise InvalidSyncWorkLedgerRequest("file work counters are invalid")
        now = _aware("now", now)
        row = self._locked_lease(lease, worker_id=worker_id, now=now)
        if row.cancel_requested_at is not None:
            raise FileWorkCancellationConflict("file work cancellation is pending")
        _apply_terminal(row, outcome.value, now, counters=counters)
        self._flush("file work completion failed")
        return _work_view(row)

    def record_failure(
        self,
        lease: FileWorkLease,
        *,
        worker_id: str,
        error_category: str,
        error_code: str,
        now: datetime,
        retry_at: datetime | None = None,
        quarantine_reason_code: str | None = None,
        counters: FileWorkCounters = FileWorkCounters(),
    ) -> FileWorkItemView:
        error_category = _failure_category(error_category)
        error_code = _code("error_code", error_code, 128)
        if not isinstance(counters, FileWorkCounters):
            raise InvalidSyncWorkLedgerRequest("file work counters are invalid")
        now = _aware("now", now)
        if retry_at is not None:
            retry_at = _aware("retry_at", retry_at)
            if retry_at < now:
                raise InvalidSyncWorkLedgerRequest("retry_at cannot precede now")
        if quarantine_reason_code is not None:
            quarantine_reason_code = _code(
                "quarantine_reason_code", quarantine_reason_code, 128
            )
            if retry_at is not None:
                raise InvalidSyncWorkLedgerRequest("quarantined work cannot be retry scheduled")
        row = self._locked_lease(lease, worker_id=worker_id, now=now)
        if row.cancel_requested_at is not None:
            raise FileWorkCancellationConflict("file work cancellation is pending")
        row.last_error_category = error_category
        row.last_error_code = error_code
        _apply_counters(row, counters)
        if quarantine_reason_code is not None:
            row.quarantine_reason_code = quarantine_reason_code
            _apply_terminal(row, FileWorkStatus.QUARANTINED.value, now, preserve_counters=True)
        elif retry_at is not None and row.attempt_count < row.max_attempts:
            row.status = FileWorkStatus.RETRY_WAIT.value
            row.next_attempt_at = retry_at
            row.terminal_at = None
            _clear_lease(row)
            row.updated_at = now
        else:
            _apply_terminal(row, FileWorkStatus.FAILED.value, now, preserve_counters=True)
        self._flush("file work failure transition failed")
        return _work_view(row)

    def request_cancellation(
        self,
        organization_id: UUID,
        generation_id: UUID,
        work_item_id: UUID,
        *,
        reason_code: str,
        now: datetime,
    ) -> FileWorkItemView:
        organization_id = _uuid("organization_id", organization_id)
        generation_id = _uuid("generation_id", generation_id)
        work_item_id = _uuid("work_item_id", work_item_id)
        reason_code = _code("reason_code", reason_code, 64)
        now = _aware("now", now)
        row = self._one(
            select(ConnectorSyncFileWorkItem)
            .where(
                ConnectorSyncFileWorkItem.organization_id == organization_id,
                ConnectorSyncFileWorkItem.generation_id == generation_id,
                ConnectorSyncFileWorkItem.id == work_item_id,
            )
            .with_for_update(),
            "file work cancellation lookup failed",
        )
        if row is None:
            raise SyncWorkLedgerNotFound("file work item was not found")
        if row.status in {status.value for status in TERMINAL_FILE_WORK_STATUSES}:
            return _work_view(row)
        if row.cancel_requested_at is None:
            row.cancel_requested_at = now
            row.cancel_reason_code = reason_code
        if row.status in {FileWorkStatus.PENDING.value, FileWorkStatus.RETRY_WAIT.value}:
            _apply_terminal(row, FileWorkStatus.CANCELLED.value, now)
        else:
            row.updated_at = now
        self._flush("file work cancellation failed")
        return _work_view(row)

    def acknowledge_cancellation(
        self, lease: FileWorkLease, *, worker_id: str, now: datetime
    ) -> FileWorkItemView:
        now = _aware("now", now)
        row = self._locked_lease(lease, worker_id=worker_id, now=now, allow_cancellation=True)
        if row.cancel_requested_at is None:
            raise FileWorkCancellationConflict("file work cancellation was not requested")
        _apply_terminal(row, FileWorkStatus.CANCELLED.value, now)
        self._flush("file work cancellation acknowledgement failed")
        return _work_view(row)

    def recover_expired(
        self,
        organization_id: UUID,
        generation_id: UUID,
        *,
        now: datetime,
        limit: int,
    ) -> tuple[FileWorkItemView, ...]:
        organization_id = _uuid("organization_id", organization_id)
        generation_id = _uuid("generation_id", generation_id)
        now = _aware("now", now)
        limit = _limit(limit, MAX_CLAIM_LIMIT)
        rows = self._all(
            select(ConnectorSyncFileWorkItem)
            .where(
                ConnectorSyncFileWorkItem.organization_id == organization_id,
                ConnectorSyncFileWorkItem.generation_id == generation_id,
                ConnectorSyncFileWorkItem.status == FileWorkStatus.RUNNING.value,
                ConnectorSyncFileWorkItem.lease_expires_at <= now,
            )
            .order_by(ConnectorSyncFileWorkItem.lease_expires_at, ConnectorSyncFileWorkItem.id)
            .with_for_update(skip_locked=True)
            .limit(limit),
            "expired file work lookup failed",
        )
        self._recover_rows(rows, now)
        return tuple(_work_view(row) for row in rows)

    def recover_expired_available(
        self,
        *,
        provider_key: str,
        profile_fingerprint: str,
        now: datetime,
        limit: int,
    ) -> tuple[FileWorkItemView, ...]:
        """Recover expired provider work across tenants without exposing payloads."""
        provider_key = _code("provider_key", provider_key, 64)
        profile_fingerprint = _identifier(
            "profile_fingerprint", profile_fingerprint, 255
        )
        now = _aware("now", now)
        limit = _limit(limit, MAX_CLAIM_LIMIT)
        rows = self._all(
            select(ConnectorSyncFileWorkItem)
            .join(
                ConnectorSyncGeneration,
                (ConnectorSyncGeneration.organization_id == ConnectorSyncFileWorkItem.organization_id)
                & (ConnectorSyncGeneration.id == ConnectorSyncFileWorkItem.generation_id),
            )
            .join(
                ConnectorSyncJob,
                (ConnectorSyncJob.organization_id == ConnectorSyncGeneration.organization_id)
                & (ConnectorSyncJob.connector_id == ConnectorSyncGeneration.connector_id)
                & (ConnectorSyncJob.connector_scope_id == ConnectorSyncGeneration.connector_scope_id)
                & (ConnectorSyncJob.id == ConnectorSyncGeneration.sync_job_id),
            )
            .where(
                ConnectorSyncGeneration.provider_key == provider_key,
                ConnectorSyncGeneration.profile_fingerprint == profile_fingerprint,
                ConnectorSyncGeneration.status == RepositoryGenerationStatus.PROCESSING.value,
                ConnectorSyncGeneration.discovery_complete.is_(True),
                ConnectorSyncJob.status != "cancelled",
                ConnectorSyncJob.cancel_requested_at.is_(None),
                ConnectorSyncFileWorkItem.status == FileWorkStatus.RUNNING.value,
                ConnectorSyncFileWorkItem.lease_expires_at <= now,
            )
            .order_by(ConnectorSyncFileWorkItem.lease_expires_at, ConnectorSyncFileWorkItem.id)
            .with_for_update(of=ConnectorSyncFileWorkItem, skip_locked=True)
            .limit(limit),
            "expired file work lookup failed",
        )
        self._recover_rows(rows, now)
        return tuple(_work_view(row) for row in rows)

    def _recover_rows(self, rows, now: datetime) -> None:
        for row in rows:
            if row.cancel_requested_at is not None:
                _apply_terminal(row, FileWorkStatus.CANCELLED.value, now)
            elif row.attempt_count < row.max_attempts:
                row.status = FileWorkStatus.RETRY_WAIT.value
                row.next_attempt_at = now
                row.last_error_category = "internal"
                row.last_error_code = "lease_expired"
                _clear_lease(row)
                row.updated_at = now
            else:
                row.last_error_category = "internal"
                row.last_error_code = "lease_expired"
                _apply_terminal(row, FileWorkStatus.FAILED.value, now)
        if rows:
            self._flush("expired file work recovery failed")

    def barrier_summary(
        self, organization_id: UUID, generation_id: UUID
    ) -> GenerationBarrierSummary:
        organization_id = _uuid("organization_id", organization_id)
        generation_id = _uuid("generation_id", generation_id)
        generation = self._generation(organization_id, generation_id)
        if generation is None:
            raise SyncWorkLedgerNotFound("generation was not found")
        grouped = self._session.execute(
            select(ConnectorSyncFileWorkItem.status, func.count())
            .where(
                ConnectorSyncFileWorkItem.organization_id == organization_id,
                ConnectorSyncFileWorkItem.generation_id == generation_id,
            )
            .group_by(ConnectorSyncFileWorkItem.status)
        ).all()
        counts = {status: int(count) for status, count in grouped}
        total = sum(counts.values())
        terminal = sum(counts.get(status.value, 0) for status in TERMINAL_FILE_WORK_STATUSES)
        nonterminal = total - terminal
        open_barrier = (
            generation.discovery_complete
            and nonterminal == 0
            and generation.status not in {status.value for status in TERMINAL_GENERATION_STATUSES}
        )
        return GenerationBarrierSummary(
            generation.id,
            generation.discovery_complete,
            total,
            counts.get(FileWorkStatus.PENDING.value, 0),
            counts.get(FileWorkStatus.RUNNING.value, 0),
            counts.get(FileWorkStatus.RETRY_WAIT.value, 0),
            counts.get(FileWorkStatus.SUCCEEDED.value, 0),
            counts.get(FileWorkStatus.SKIPPED.value, 0),
            counts.get(FileWorkStatus.QUARANTINED.value, 0),
            counts.get(FileWorkStatus.FAILED.value, 0),
            counts.get(FileWorkStatus.CANCELLED.value, 0),
            nonterminal,
            open_barrier,
        )

    def get_work_item(
        self, organization_id: UUID, generation_id: UUID, work_item_id: UUID
    ) -> FileWorkItemView | None:
        row = self._one(
            select(ConnectorSyncFileWorkItem).where(
                ConnectorSyncFileWorkItem.organization_id == _uuid("organization_id", organization_id),
                ConnectorSyncFileWorkItem.generation_id == _uuid("generation_id", generation_id),
                ConnectorSyncFileWorkItem.id == _uuid("work_item_id", work_item_id),
            ),
            "file work lookup failed",
        )
        return _work_view(row) if row is not None else None

    def get_materialization(
        self, organization_id: UUID, generation_id: UUID, work_item_id: UUID
    ) -> FileWorkMaterializationView | None:
        organization_id = _uuid("organization_id", organization_id)
        generation_id = _uuid("generation_id", generation_id)
        work_item_id = _uuid("work_item_id", work_item_id)
        row = self._one(
            select(ConnectorSyncFileMaterialization).where(
                ConnectorSyncFileMaterialization.organization_id == organization_id,
                ConnectorSyncFileMaterialization.generation_id == generation_id,
                ConnectorSyncFileMaterialization.work_item_id == work_item_id,
            ),
            "file materialization lookup failed",
        )
        if row is None:
            return None
        count = self._one(
            select(func.count(ConnectorSyncFileMaterializationChunk.id)).where(
                ConnectorSyncFileMaterializationChunk.organization_id == organization_id,
                ConnectorSyncFileMaterializationChunk.generation_id == generation_id,
                ConnectorSyncFileMaterializationChunk.materialization_id == row.id,
            ),
            "file materialization chunk count failed",
        )
        if count != row.chunk_count:
            raise SyncWorkLedgerPersistenceError(
                "file materialization chunks are incomplete"
            )
        return _materialization_view(row)

    def stage_materialization_and_complete(
        self,
        lease: FileWorkLease,
        *,
        worker_id: str,
        generation: RepositoryGenerationView,
        work_item: FileWorkItemView,
        materialization: FileWorkMaterialization,
        counters: FileWorkCounters,
        now: datetime,
    ) -> tuple[FileWorkItemView, FileWorkMaterializationView, bool]:
        """Atomically stage immutable output and complete the currently fenced item."""
        if not isinstance(generation, RepositoryGenerationView):
            raise InvalidSyncWorkLedgerRequest("generation view is invalid")
        if not isinstance(work_item, FileWorkItemView):
            raise InvalidSyncWorkLedgerRequest("file work view is invalid")
        if not isinstance(materialization, FileWorkMaterialization):
            raise InvalidSyncWorkLedgerRequest("file materialization is invalid")
        if not isinstance(counters, FileWorkCounters):
            raise InvalidSyncWorkLedgerRequest("file work counters are invalid")
        now = _aware("now", now)

        work_row = self._locked_lease(lease, worker_id=worker_id, now=now)
        generation_row = self._locked_generation(
            lease.organization_id, lease.generation_id
        )
        if generation_row is None:
            raise SyncWorkLedgerNotFound("file materialization generation was not found")
        job = self._one(
            select(ConnectorSyncJob)
            .where(
                ConnectorSyncJob.organization_id == generation_row.organization_id,
                ConnectorSyncJob.connector_id == generation_row.connector_id,
                ConnectorSyncJob.connector_scope_id == generation_row.connector_scope_id,
                ConnectorSyncJob.id == generation_row.sync_job_id,
            )
            .with_for_update(),
            "file materialization job validation failed",
        )
        if (
            job is None
            or job.status == "cancelled"
            or job.cancel_requested_at is not None
            or generation_row.status != RepositoryGenerationStatus.PROCESSING.value
            or not generation_row.discovery_complete
            or _generation_view(generation_row) != generation
            or _work_view(work_row).work_item_id != work_item.work_item_id
            or _work_view(work_row).source_item_key != work_item.source_item_key
            or _work_view(work_row).repository_path != work_item.repository_path
            or _work_view(work_row).provider_blob_id != work_item.provider_blob_id
            or _work_view(work_row).provider_revision_id != work_item.provider_revision_id
            or _work_view(work_row).profile_fingerprint != work_item.profile_fingerprint
            or not _materialization_matches_context(
                materialization, generation, work_item
            )
        ):
            raise SyncWorkLedgerConflict(
                "file materialization attribution changed"
            )

        existing = self._one(
            select(ConnectorSyncFileMaterialization)
            .where(
                ConnectorSyncFileMaterialization.organization_id == lease.organization_id,
                ConnectorSyncFileMaterialization.generation_id == lease.generation_id,
                ConnectorSyncFileMaterialization.work_item_id == lease.work_item_id,
            )
            .with_for_update(),
            "file materialization lock failed",
        )
        created = existing is None
        if existing is None:
            existing = ConnectorSyncFileMaterialization(
                id=self._new_uuid(
                    "materialization_id", self._materialization_id_factory
                ),
                organization_id=lease.organization_id,
                connector_id=lease.connector_id,
                connector_scope_id=lease.connector_scope_id,
                generation_id=lease.generation_id,
                work_item_id=lease.work_item_id,
                repository_identity=materialization.repository_identity,
                branch_name=materialization.branch_name,
                root_tree_object_id=materialization.root_tree_object_id,
                source_item_key=materialization.source_item_key,
                source_key_hash=_source_key_hash(
                    materialization.source_item_key,
                    materialization.repository_path,
                ),
                repository_path=materialization.repository_path,
                provider_blob_id=materialization.provider_blob_id,
                provider_revision_id=materialization.provider_revision_id,
                profile_fingerprint=materialization.profile_fingerprint,
                content_checksum=materialization.content_checksum,
                title=materialization.title,
                mime_type=materialization.mime_type,
                embedding_model=materialization.embedding_model,
                chunk_count=len(materialization.chunks),
                created_at=now,
            )
            self._session.add(existing)
            self._flush("file materialization could not be created")
            self._session.add_all(
                ConnectorSyncFileMaterializationChunk(
                    id=self._new_uuid(
                        "materialization_chunk_id",
                        self._materialization_chunk_id_factory,
                    ),
                    organization_id=lease.organization_id,
                    generation_id=lease.generation_id,
                    materialization_id=existing.id,
                    chunk_index=chunk.chunk_index,
                    chunk_text=chunk.chunk_text,
                    content_hash=chunk.content_hash,
                    embedding=[float(value) for value in chunk.embedding],
                    embedding_model=chunk.embedding_model,
                    created_at=now,
                )
                for chunk in materialization.chunks
            )
            self._flush("file materialization chunks could not be created")
        else:
            chunks = self._all(
                select(ConnectorSyncFileMaterializationChunk)
                .where(
                    ConnectorSyncFileMaterializationChunk.organization_id
                    == lease.organization_id,
                    ConnectorSyncFileMaterializationChunk.generation_id
                    == lease.generation_id,
                    ConnectorSyncFileMaterializationChunk.materialization_id
                    == existing.id,
                )
                .order_by(ConnectorSyncFileMaterializationChunk.chunk_index)
                .with_for_update(),
                "file materialization chunks could not be locked",
            )
            if not _persisted_materialization_matches(
                existing, chunks, materialization
            ):
                raise SyncWorkLedgerConflict(
                    "file materialization conflicts with immutable output"
                )

        _apply_terminal(work_row, FileWorkStatus.SUCCEEDED.value, now, counters=counters)
        self._flush("file materialization completion failed")
        return _work_view(work_row), _materialization_view(existing), created

    def _generation(self, organization_id: UUID, generation_id: UUID):
        return self._one(
            select(ConnectorSyncGeneration).where(
                ConnectorSyncGeneration.organization_id == _uuid("organization_id", organization_id),
                ConnectorSyncGeneration.id == _uuid("generation_id", generation_id),
            ),
            "generation lookup failed",
        )

    def _locked_generation(self, organization_id: UUID, generation_id: UUID):
        return self._one(
            select(ConnectorSyncGeneration)
            .where(
                ConnectorSyncGeneration.organization_id == organization_id,
                ConnectorSyncGeneration.id == generation_id,
            )
            .with_for_update(),
            "generation lock failed",
        )

    def _locked_lease(
        self,
        lease: FileWorkLease,
        *,
        worker_id: str,
        now: datetime,
        allow_cancellation: bool = False,
    ) -> ConnectorSyncFileWorkItem:
        if not isinstance(lease, FileWorkLease):
            raise InvalidSyncWorkLedgerRequest("file work lease is invalid")
        worker_id = _worker_id(worker_id)
        if worker_id != lease.worker_id:
            raise LostFileWorkLease("file work lease is no longer owned")
        row = self._one(
            select(ConnectorSyncFileWorkItem)
            .where(
                ConnectorSyncFileWorkItem.organization_id == lease.organization_id,
                ConnectorSyncFileWorkItem.connector_id == lease.connector_id,
                ConnectorSyncFileWorkItem.connector_scope_id == lease.connector_scope_id,
                ConnectorSyncFileWorkItem.generation_id == lease.generation_id,
                ConnectorSyncFileWorkItem.id == lease.work_item_id,
                ConnectorSyncFileWorkItem.status == FileWorkStatus.RUNNING.value,
                ConnectorSyncFileWorkItem.lease_owner == worker_id,
                ConnectorSyncFileWorkItem.lease_id == lease.lease_id,
                ConnectorSyncFileWorkItem.fencing_token == lease.fencing_token,
                ConnectorSyncFileWorkItem.attempt_count == lease.attempt_number,
                ConnectorSyncFileWorkItem.lease_expires_at > now,
            )
            .with_for_update(),
            "file work lease validation failed",
        )
        if row is None:
            self._raise_lost_lease(lease)
        if not allow_cancellation and row.cancel_requested_at is not None:
            raise FileWorkCancellationConflict("file work cancellation is pending")
        return row

    def _raise_lost_lease(self, lease: FileWorkLease) -> None:
        row = self._one(
            select(ConnectorSyncFileWorkItem).where(
                ConnectorSyncFileWorkItem.organization_id == lease.organization_id,
                ConnectorSyncFileWorkItem.connector_id == lease.connector_id,
                ConnectorSyncFileWorkItem.connector_scope_id == lease.connector_scope_id,
                ConnectorSyncFileWorkItem.generation_id == lease.generation_id,
                ConnectorSyncFileWorkItem.id == lease.work_item_id,
            ),
            "file work lease diagnosis failed",
        )
        if row is not None and row.fencing_token != lease.fencing_token:
            raise StaleFileWorkFence("file work fencing token is stale")
        raise LostFileWorkLease("file work lease is no longer owned")

    def _new_uuid(self, name: str, factory: Callable[[], UUID]) -> UUID:
        try:
            value = factory()
        except Exception as exc:
            raise InvalidSyncWorkLedgerRequest(f"{name} factory failed") from exc
        return _uuid(name, value)

    def _one(self, statement, message: str):
        try:
            return self._session.execute(statement).scalar_one_or_none()
        except SQLAlchemyError as exc:
            raise SyncWorkLedgerPersistenceError(message) from exc

    def _all(self, statement, message: str):
        try:
            return list(self._session.execute(statement).scalars().all())
        except SQLAlchemyError as exc:
            raise SyncWorkLedgerPersistenceError(message) from exc

    def _flush(self, message: str) -> None:
        try:
            self._session.flush()
        except IntegrityError as exc:
            raise SyncWorkLedgerConflict(message) from exc
        except SQLAlchemyError as exc:
            raise SyncWorkLedgerPersistenceError(message) from exc


def _generation_matches(row, request: RepositoryGenerationRegistration) -> bool:
    return all(
        getattr(row, field) == getattr(request, field)
        for field in (
            "organization_id",
            "connector_id",
            "connector_scope_id",
            "sync_job_id",
            "provider_key",
            "repository_identity",
            "branch_name",
            "commit_object_id",
            "root_tree_object_id",
            "profile_fingerprint",
        )
    )


def _manifest_identity(entry: FileWorkManifestEntry) -> tuple[str, str, str, str]:
    return (
        _source_key_hash(entry.source_item_key, entry.repository_path),
        entry.provider_blob_id,
        entry.provider_revision_id,
        entry.profile_fingerprint,
    )


def _source_key_hash(source_item_key: str, repository_path: str) -> str:
    digest = hashlib.sha256()
    digest.update(source_item_key.encode("utf-8"))
    digest.update(b"\x00")
    digest.update(repository_path.encode("utf-8"))
    return digest.hexdigest()


def _materialization_matches_context(
    materialization: FileWorkMaterialization,
    generation: RepositoryGenerationView,
    work_item: FileWorkItemView,
) -> bool:
    return (
        materialization.repository_identity == generation.repository_identity
        and materialization.branch_name == generation.branch_name
        and materialization.root_tree_object_id == generation.root_tree_object_id
        and materialization.source_item_key == work_item.source_item_key
        and materialization.repository_path == work_item.repository_path
        and materialization.provider_blob_id == work_item.provider_blob_id
        and materialization.provider_revision_id == generation.commit_object_id
        and materialization.provider_revision_id == work_item.provider_revision_id
        and materialization.profile_fingerprint == generation.profile_fingerprint
        and materialization.profile_fingerprint == work_item.profile_fingerprint
    )


def _persisted_materialization_matches(row, rows, requested) -> bool:
    header_matches = (
        row.repository_identity == requested.repository_identity
        and row.branch_name == requested.branch_name
        and row.root_tree_object_id == requested.root_tree_object_id
        and row.source_item_key == requested.source_item_key
        and row.source_key_hash
        == _source_key_hash(requested.source_item_key, requested.repository_path)
        and row.repository_path == requested.repository_path
        and row.provider_blob_id == requested.provider_blob_id
        and row.provider_revision_id == requested.provider_revision_id
        and row.profile_fingerprint == requested.profile_fingerprint
        and row.content_checksum == requested.content_checksum
        and row.title == requested.title
        and row.mime_type == requested.mime_type
        and row.embedding_model == requested.embedding_model
        and row.chunk_count == len(requested.chunks)
        and len(rows) == len(requested.chunks)
    )
    if not header_matches:
        return False
    return all(
        row_chunk.chunk_index == requested_chunk.chunk_index
        and row_chunk.chunk_text == requested_chunk.chunk_text
        and row_chunk.content_hash == requested_chunk.content_hash
        and row_chunk.embedding_model == requested_chunk.embedding_model
        and len(row_chunk.embedding) == len(requested_chunk.embedding)
        and all(
            math.isclose(float(left), float(right), rel_tol=1e-6, abs_tol=1e-7)
            for left, right in zip(
                row_chunk.embedding, requested_chunk.embedding, strict=True
            )
        )
        for row_chunk, requested_chunk in zip(rows, requested.chunks, strict=True)
    )


def _manifest_row_matches(row, entry: FileWorkManifestEntry) -> bool:
    return (
        row.source_item_key == entry.source_item_key
        and row.repository_path == entry.repository_path
        and row.provider_blob_id == entry.provider_blob_id
        and row.provider_revision_id == entry.provider_revision_id
        and row.profile_fingerprint == entry.profile_fingerprint
        and row.file_size_bytes == entry.file_size_bytes
        and row.file_extension == entry.file_extension
        and row.mime_type == entry.mime_type
        and row.max_attempts == entry.max_attempts
    )


def _apply_terminal(
    row,
    status: str,
    now: datetime,
    *,
    counters: FileWorkCounters | None = None,
    preserve_counters: bool = False,
) -> None:
    if counters is not None:
        _apply_counters(row, counters)
    elif not preserve_counters:
        _apply_counters(row, FileWorkCounters())
    row.status = status
    row.next_attempt_at = None
    row.terminal_at = now
    row.updated_at = now
    _clear_lease(row)


def _apply_counters(row, counters: FileWorkCounters) -> None:
    row.downloaded_bytes = counters.downloaded_bytes
    row.extracted_characters = counters.extracted_characters
    row.chunk_count = counters.chunk_count
    row.embedding_batch_count = counters.embedding_batch_count


def _clear_lease(row) -> None:
    row.lease_owner = None
    row.lease_id = None
    row.lease_acquired_at = None
    row.lease_expires_at = None
    row.heartbeat_at = None


def _lease(row) -> FileWorkLease:
    if row.lease_id is None or row.lease_owner is None or row.lease_expires_at is None:
        raise SyncWorkLedgerPersistenceError("persisted file work lease is incomplete")
    return FileWorkLease(
        row.organization_id,
        row.connector_id,
        row.connector_scope_id,
        row.generation_id,
        row.id,
        row.lease_owner,
        row.lease_id,
        row.fencing_token,
        row.attempt_count,
        row.max_attempts,
        row.lease_expires_at,
    )


def _generation_view(row) -> RepositoryGenerationView:
    return RepositoryGenerationView(
        row.id,
        row.organization_id,
        row.connector_id,
        row.connector_scope_id,
        row.sync_job_id,
        row.provider_key,
        row.repository_identity,
        row.branch_name,
        row.commit_object_id,
        row.root_tree_object_id,
        row.profile_fingerprint,
        RepositoryGenerationStatus(row.status),
        row.discovery_complete,
        row.discovery_completed_at,
        row.reconciliation_eligible,
        row.reconciliation_eligible_at,
        row.resync_required,
        row.resync_requested_at,
        row.items_discovered,
        row.items_registered,
        row.declared_bytes,
        row.created_at,
        row.updated_at,
        row.terminal_at,
    )


def _work_view(row) -> FileWorkItemView:
    return FileWorkItemView(
        row.id,
        row.organization_id,
        row.connector_id,
        row.connector_scope_id,
        row.generation_id,
        row.source_item_key,
        row.repository_path,
        row.provider_blob_id,
        row.provider_revision_id,
        row.profile_fingerprint,
        row.file_size_bytes,
        row.file_extension,
        row.mime_type,
        FileWorkStatus(row.status),
        row.attempt_count,
        row.max_attempts,
        row.next_attempt_at,
        row.cancel_requested_at is not None,
        row.last_error_category,
        row.last_error_code,
        row.quarantine_reason_code,
        FileWorkCounters(
            row.downloaded_bytes,
            row.extracted_characters,
            row.chunk_count,
            row.embedding_batch_count,
        ),
        row.created_at,
        row.updated_at,
        row.terminal_at,
    )


def _materialization_view(row) -> FileWorkMaterializationView:
    return FileWorkMaterializationView(
        row.id,
        row.organization_id,
        row.connector_id,
        row.connector_scope_id,
        row.generation_id,
        row.work_item_id,
        row.provider_blob_id,
        row.provider_revision_id,
        row.profile_fingerprint,
        row.chunk_count,
        row.created_at,
    )


def _uuid(name: str, value: object) -> UUID:
    if not isinstance(value, UUID):
        raise InvalidSyncWorkLedgerRequest(f"{name} must be a UUID")
    return value


def _aware(name: str, value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise InvalidSyncWorkLedgerRequest(f"{name} must be timezone-aware")
    return value


def _worker_id(value: object) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip() or len(value) > 255:
        raise InvalidSyncWorkLedgerRequest("worker_id is invalid")
    return value


def _lease_duration(value: object) -> timedelta:
    if (
        not isinstance(value, timedelta)
        or not 0 < value.total_seconds() <= MAX_LEASE_SECONDS
    ):
        raise InvalidSyncWorkLedgerRequest("lease_duration is invalid")
    return value


def _code(name: str, value: object, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or value.lower() != value
        or not value[0].isalpha()
        or any(not (character.isalnum() or character == "_") for character in value)
    ):
        raise InvalidSyncWorkLedgerRequest(f"{name} must be a normalized code")
    return value


def _identifier(name: str, value: object, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or value.lower() != value
        or any(
            not (character.isalnum() or character in "._:/-")
            for character in value
        )
    ):
        raise InvalidSyncWorkLedgerRequest(
            f"{name} must be a normalized identifier"
        )
    return value


def _failure_category(value: object) -> str:
    if value not in FAILURE_CATEGORIES:
        raise InvalidSyncWorkLedgerRequest("error_category is invalid")
    return str(value)


def _limit(value: object, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise InvalidSyncWorkLedgerRequest(f"limit must be between 1 and {maximum}")
    return value
