"""Tenant-safe durable repository generation and file-work control ledger."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import PurePosixPath
from typing import Callable
from uuid import UUID, uuid4

from sqlalchemy import BigInteger
from sqlalchemy import Sequence as SqlSequence
from sqlalchemy import and_, exists, func, or_, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, aliased

from domain.connectors.sync_work_ledger import (
    FileWorkCounters,
    FileWorkItemView,
    FileWorkLease,
    FileWorkMaterialization,
    FileWorkMaterializationView,
    FileWorkManifestEntry,
    FileWorkStatus,
    DiscoveryRegistrationResult,
    GenerationActivationStatus,
    GenerationActivationView,
    GenerationBarrierSummary,
    GenerationCitationProjectionProfile,
    GenerationObservationDisposition,
    GenerationPromotionRequest,
    GenerationPromotionResult,
    GenerationReconciliationRequest,
    GenerationReconciliationResult,
    GenerationSourceObservation,
    ManifestRegistrationResult,
    MAX_MANIFEST_BATCH_SIZE,
    MAX_RECONCILIATION_BATCH_SIZE,
    RepositoryGenerationRegistration,
    RepositoryGenerationStatus,
    RepositoryGenerationView,
    TERMINAL_FILE_WORK_STATUSES,
    TERMINAL_GENERATION_STATUSES,
)
from domain.connectors.sync_control_reservation import ControlReservationOwner
from infrastructure.db.models import (
    ConnectorSyncFileMaterialization,
    ConnectorSyncFileMaterializationChunk,
    ConnectorSyncFileWorkItem,
    ConnectorSyncControlReservation,
    ConnectorSyncGeneration,
    ConnectorSyncGenerationActivation,
    ConnectorSyncGenerationObservation,
    ConnectorSyncJob,
    ConnectorSyncOrganizationClaimSchedule,
    ConnectorScope,
    Document,
    DocumentIndexingState,
    DocumentVersion,
    DocumentVersionDocument,
    Organization,
    SourceItem,
    SourceItemScopeMembership,
)


MAX_CLAIM_LIMIT = 500
MAX_LEASE_SECONDS = 3600
MAX_FAIR_SELECTION_ATTEMPTS = 500
FAIR_CLAIM_SEQUENCE = SqlSequence(
    "connector_sync_org_fair_claim_seq", data_type=BigInteger()
)
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


@dataclass(frozen=True)
class TargetedFileWorkClaimResult:
    """Sanitized result from one exact reservation-aware acquisition."""

    outcome: str
    lease: FileWorkLease | None = None
    mutated: bool = False


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
        activation_id_factory: Callable[[], UUID] = uuid4,
        observation_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._session = session
        self._generation_id_factory = generation_id_factory
        self._work_item_id_factory = work_item_id_factory
        self._lease_id_factory = lease_id_factory
        self._materialization_id_factory = materialization_id_factory
        self._materialization_chunk_id_factory = materialization_chunk_id_factory
        self._activation_id_factory = activation_id_factory
        self._observation_id_factory = observation_id_factory

    def register_generation(
        self, request: RepositoryGenerationRegistration
    ) -> tuple[RepositoryGenerationView, bool]:
        if not isinstance(request, RepositoryGenerationRegistration):
            raise InvalidSyncWorkLedgerRequest("generation registration is invalid")
        scope = self._one(
            select(ConnectorScope.id)
            .where(
                ConnectorScope.organization_id == request.organization_id,
                ConnectorScope.connector_id == request.connector_id,
                ConnectorScope.id == request.connector_scope_id,
            )
            .with_for_update(),
            "generation scope lock failed",
        )
        if scope is None:
            raise SyncWorkLedgerNotFound("generation scope context was not found")
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
            manifest_schema_version=request.manifest_schema_version,
            reconciliation_started_at=None,
            reconciliation_completed_at=None,
            reconciled_membership_count=0,
            reconciled_source_count=0,
            reconciled_document_count=0,
        )
        self._session.add(row)
        self._flush("generation could not be registered")
        return _generation_view(row), True

    def get_generation(
        self, organization_id: UUID, generation_id: UUID
    ) -> RepositoryGenerationView | None:
        row = self._generation(organization_id, generation_id)
        return _generation_view(row) if row is not None else None

    def register_discovery_batch(
        self,
        organization_id: UUID,
        generation_id: UUID,
        observations: Sequence[GenerationSourceObservation],
        work_entries: Sequence[FileWorkManifestEntry],
        *,
        now: datetime,
    ) -> DiscoveryRegistrationResult:
        """Atomically record the authoritative observed manifest and its work subset."""
        organization_id = _uuid("organization_id", organization_id)
        generation_id = _uuid("generation_id", generation_id)
        now = _aware("now", now)
        if (
            isinstance(observations, (str, bytes))
            or not isinstance(observations, Sequence)
            or not observations
            or len(observations) > MAX_MANIFEST_BATCH_SIZE
            or any(not isinstance(row, GenerationSourceObservation) for row in observations)
        ):
            raise InvalidSyncWorkLedgerRequest("generation observations are invalid")
        if (
            isinstance(work_entries, (str, bytes))
            or not isinstance(work_entries, Sequence)
            or len(work_entries) > MAX_MANIFEST_BATCH_SIZE
            or any(not isinstance(row, FileWorkManifestEntry) for row in work_entries)
        ):
            raise InvalidSyncWorkLedgerRequest("generation work entries are invalid")
        generation = self._locked_generation(organization_id, generation_id)
        if generation is None:
            raise SyncWorkLedgerNotFound("generation was not found")
        if generation.manifest_schema_version != 2:
            raise SyncWorkLedgerConflict("generation manifest schema is incompatible")
        if generation.status in {status.value for status in TERMINAL_GENERATION_STATUSES}:
            raise SyncWorkLedgerConflict("terminal generation cannot accept observations")

        keyed: dict[str, GenerationSourceObservation] = {}
        for observation in observations:
            if observation.profile_fingerprint != generation.profile_fingerprint:
                raise InvalidSyncWorkLedgerRequest(
                    "observation profile does not match generation"
                )
            source_hash = _source_key_hash(
                observation.source_item_key, observation.repository_path
            )
            previous = keyed.get(source_hash)
            if previous is not None and previous != observation:
                raise SyncWorkLedgerConflict("observation source identity collision")
            keyed[source_hash] = observation

        work_by_hash = {
            _source_key_hash(entry.source_item_key, entry.repository_path): entry
            for entry in work_entries
        }
        if len(work_by_hash) != len(work_entries):
            raise SyncWorkLedgerConflict("manifest source identity collision")
        for source_hash, entry in work_by_hash.items():
            observation = keyed.get(source_hash)
            if (
                observation is None
                or observation.disposition is not GenerationObservationDisposition.ELIGIBLE
                or observation.source_item_key != entry.source_item_key
                or observation.repository_path != entry.repository_path
                or observation.provider_object_id != entry.provider_blob_id
                or observation.provider_revision_id != entry.provider_revision_id
                or observation.profile_fingerprint != entry.profile_fingerprint
            ):
                raise SyncWorkLedgerConflict(
                    "generation work is not an eligible observed source"
                )
        eligible_hashes = {
            source_hash
            for source_hash, observation in keyed.items()
            if observation.disposition is GenerationObservationDisposition.ELIGIBLE
        }
        if set(work_by_hash) != eligible_hashes:
            raise SyncWorkLedgerConflict("eligible observations and work entries differ")

        if generation.discovery_complete:
            rows = self._all(
                select(ConnectorSyncGenerationObservation).where(
                    ConnectorSyncGenerationObservation.organization_id
                    == organization_id,
                    ConnectorSyncGenerationObservation.generation_id == generation_id,
                    ConnectorSyncGenerationObservation.source_key_hash.in_(tuple(keyed)),
                ),
                "completed observation replay lookup failed",
            )
            persisted = {row.source_key_hash: row for row in rows}
            if set(persisted) != set(keyed):
                raise SyncWorkLedgerConflict(
                    "completed discovery cannot accept new observations"
                )
            for source_hash, observation in keyed.items():
                if not _observation_row_matches(persisted[source_hash], observation):
                    raise SyncWorkLedgerConflict(
                        "observation identity resolves to different attributes"
                    )
            work_result = (
                self.register_manifest(
                    organization_id,
                    generation_id,
                    work_entries,
                    now=now,
                    _count_as_discovered=False,
                )
                if work_entries
                else ManifestRegistrationResult(generation_id, 0, 0, ())
            )
            return DiscoveryRegistrationResult(
                generation_id,
                0,
                len(keyed),
                work_result.created_count,
                work_result.existing_count,
                tuple(persisted[source_hash].id for source_hash in keyed),
                work_result.work_item_ids,
            )

        values = [
            {
                "id": self._new_uuid("observation_id", self._observation_id_factory),
                "organization_id": generation.organization_id,
                "connector_id": generation.connector_id,
                "connector_scope_id": generation.connector_scope_id,
                "generation_id": generation.id,
                "source_item_key": observation.source_item_key,
                "source_key_hash": source_hash,
                "repository_path": observation.repository_path,
                "provider_object_id": observation.provider_object_id,
                "provider_revision_id": observation.provider_revision_id,
                "profile_fingerprint": observation.profile_fingerprint,
                "entry_type": observation.entry_type,
                "disposition": observation.disposition.value,
                "file_size_bytes": observation.file_size_bytes,
                "observed_at": now,
            }
            for source_hash, observation in keyed.items()
        ]
        try:
            inserted = self._session.execute(
                insert(ConnectorSyncGenerationObservation)
                .values(values)
                .on_conflict_do_nothing(
                    constraint="uq_sync_generation_observations_source"
                )
                .returning(
                    ConnectorSyncGenerationObservation.id,
                    ConnectorSyncGenerationObservation.source_key_hash,
                )
            ).all()
        except SQLAlchemyError as exc:
            raise SyncWorkLedgerPersistenceError(
                "generation observation registration failed"
            ) from exc
        rows = self._all(
            select(ConnectorSyncGenerationObservation).where(
                ConnectorSyncGenerationObservation.organization_id == organization_id,
                ConnectorSyncGenerationObservation.generation_id == generation_id,
                ConnectorSyncGenerationObservation.source_key_hash.in_(tuple(keyed)),
            ),
            "registered observation lookup failed",
        )
        persisted = {row.source_key_hash: row for row in rows}
        if set(persisted) != set(keyed):
            raise SyncWorkLedgerPersistenceError(
                "registered generation observations are incomplete"
            )
        for source_hash, observation in keyed.items():
            if not _observation_row_matches(persisted[source_hash], observation):
                raise SyncWorkLedgerConflict(
                    "observation identity resolves to different attributes"
                )
        inserted_hashes = {row.source_key_hash for row in inserted}
        generation.items_discovered += len(inserted_hashes)
        generation.updated_at = now

        work_result = (
            self.register_manifest(
                organization_id,
                generation_id,
                work_entries,
                now=now,
                _count_as_discovered=False,
            )
            if work_entries
            else ManifestRegistrationResult(generation_id, 0, 0, ())
        )
        self._flush("generation discovery counters could not be updated")
        return DiscoveryRegistrationResult(
            generation_id,
            len(inserted_hashes),
            len(keyed) - len(inserted_hashes),
            work_result.created_count,
            work_result.existing_count,
            tuple(persisted[source_hash].id for source_hash in keyed),
            work_result.work_item_ids,
        )

    def register_manifest(
        self,
        organization_id: UUID,
        generation_id: UUID,
        entries: Sequence[FileWorkManifestEntry],
        *,
        now: datetime,
        _count_as_discovered: bool = True,
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
        if _count_as_discovered:
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
        self,
        organization_id: UUID,
        generation_id: UUID,
        *,
        now: datetime,
        reservation_id: UUID | None = None,
        planner_lease_id: UUID | None = None,
    ) -> RepositoryGenerationView:
        now = _aware("now", now)
        organization_id = _uuid("organization_id", organization_id)
        generation_id = _uuid("generation_id", generation_id)
        if (reservation_id is None) != (planner_lease_id is None):
            raise InvalidSyncWorkLedgerRequest(
                "controlled discovery ownership is incomplete"
            )
        row = self._locked_generation(organization_id, generation_id)
        if row is None:
            raise SyncWorkLedgerNotFound("generation was not found")
        if row.status in {status.value for status in TERMINAL_GENERATION_STATUSES}:
            raise SyncWorkLedgerConflict("terminal generation cannot complete discovery")
        reservation = None
        if reservation_id is not None:
            reservation_id = _uuid("reservation_id", reservation_id)
            planner_lease_id = _uuid("planner_lease_id", planner_lease_id)
            # Cancellation/failure paths lock job then reservation. Establish
            # that same order before moving the capability to its work item so
            # the final planner transaction cannot deadlock by inversion.
            job = self._one(
                select(ConnectorSyncJob)
                .where(
                    ConnectorSyncJob.organization_id == organization_id,
                    ConnectorSyncJob.connector_id == row.connector_id,
                    ConnectorSyncJob.connector_scope_id == row.connector_scope_id,
                    ConnectorSyncJob.id == row.sync_job_id,
                )
                .with_for_update(),
                "controlled discovery synchronization job lock failed",
            )
            if job is None:
                raise SyncWorkLedgerConflict(
                    "controlled discovery synchronization job is unavailable"
                )
            reservation = self._one(
                select(ConnectorSyncControlReservation)
                .where(
                    ConnectorSyncControlReservation.organization_id
                    == organization_id,
                    ConnectorSyncControlReservation.id == reservation_id,
                    ConnectorSyncControlReservation.state == "job",
                    ConnectorSyncControlReservation.planner_lease_id
                    == planner_lease_id,
                    ConnectorSyncControlReservation.released_at.is_(None),
                    ConnectorSyncControlReservation.expires_at
                    > func.clock_timestamp(),
                )
                .with_for_update(),
                "controlled discovery reservation lock failed",
            )
            if reservation is None:
                raise SyncWorkLedgerConflict(
                    "controlled discovery reservation is unavailable"
                )
        if reservation is not None:
            work_rows = self._all(
                select(ConnectorSyncFileWorkItem)
                .where(
                    ConnectorSyncFileWorkItem.organization_id == organization_id,
                    ConnectorSyncFileWorkItem.generation_id == generation_id,
                )
                .order_by(ConnectorSyncFileWorkItem.id)
                .with_for_update(),
                "controlled discovery work validation failed",
            )
            if (
                reservation.connector_id != row.connector_id
                or reservation.connector_scope_id != row.connector_scope_id
                or reservation.sync_job_id != row.sync_job_id
                or reservation.target_provider_revision_id != row.commit_object_id
                or reservation.target_profile_fingerprint
                != row.profile_fingerprint
            ):
                raise SyncWorkLedgerConflict(
                    "controlled discovery reservation attribution changed"
                )
            if len(work_rows) != 1 or row.items_registered != 1:
                raise SyncWorkLedgerConflict(
                    "controlled discovery target is not unique"
                )
            target = work_rows[0]
            if (
                target.connector_id != reservation.connector_id
                or target.connector_scope_id != reservation.connector_scope_id
                or target.source_key_hash != reservation.target_source_key_hash
                or target.provider_blob_id != reservation.target_provider_blob_id
                or target.provider_revision_id
                != reservation.target_provider_revision_id
                or target.profile_fingerprint
                != reservation.target_profile_fingerprint
            ):
                raise SyncWorkLedgerConflict(
                    "controlled discovery target attribution changed"
                )
            database_now = self._scalar(
                select(func.clock_timestamp()),
                "controlled discovery database time lookup failed",
            )
            if not isinstance(database_now, datetime):
                raise SyncWorkLedgerPersistenceError(
                    "controlled discovery database time is invalid"
                )
            reservation.state = "work_item"
            reservation.generation_id = row.id
            reservation.work_item_id = target.id
            reservation.planner_lease_id = None
            reservation.processor_lease_id = None
            reservation.handed_off_at = database_now
        if not row.discovery_complete:
            row.discovery_complete = True
            row.discovery_completed_at = now
            row.status = RepositoryGenerationStatus.PROCESSING.value
            row.updated_at = now
            self._flush("generation discovery could not be completed")
        elif reservation is not None:
            self._flush("controlled discovery handoff could not be completed")
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
                _generic_work_reservation_available(),
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

    def acquire_target_available(
        self,
        organization_id: UUID,
        connector_id: UUID,
        connector_scope_id: UUID,
        generation_id: UUID,
        work_item_id: UUID,
        reservation_owner: ControlReservationOwner,
        *,
        provider_key: str,
        profile_fingerprint: str,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> TargetedFileWorkClaimResult:
        """Recover, if necessary, and claim only one capability-owned item."""
        organization_id = _uuid("organization_id", organization_id)
        connector_id = _uuid("connector_id", connector_id)
        connector_scope_id = _uuid("connector_scope_id", connector_scope_id)
        generation_id = _uuid("generation_id", generation_id)
        work_item_id = _uuid("work_item_id", work_item_id)
        if not isinstance(reservation_owner, ControlReservationOwner):
            raise InvalidSyncWorkLedgerRequest("controlled reservation owner is invalid")
        provider_key = _code("provider_key", provider_key, 64)
        profile_fingerprint = _identifier(
            "profile_fingerprint", profile_fingerprint, 255
        )
        worker_id = _worker_id(worker_id)
        now = _aware("now", now)
        lease_duration = _lease_duration(lease_duration)

        reservation = self._one(
            select(ConnectorSyncControlReservation)
            .where(
                ConnectorSyncControlReservation.organization_id == organization_id,
                ConnectorSyncControlReservation.connector_id == connector_id,
                ConnectorSyncControlReservation.connector_scope_id
                == connector_scope_id,
                ConnectorSyncControlReservation.generation_id == generation_id,
                ConnectorSyncControlReservation.work_item_id == work_item_id,
                ConnectorSyncControlReservation.id
                == reservation_owner.reservation_id,
                ConnectorSyncControlReservation.owner_token_hash
                == reservation_owner.owner_token_hash,
                ConnectorSyncControlReservation.state == "work_item",
                ConnectorSyncControlReservation.released_at.is_(None),
                ConnectorSyncControlReservation.expires_at
                > func.clock_timestamp(),
            )
            .with_for_update(skip_locked=True),
            "controlled file-work reservation lock failed",
        )
        if reservation is None:
            return TargetedFileWorkClaimResult("reservation_unavailable")

        row = self._one(
            select(ConnectorSyncFileWorkItem)
            .join(
                ConnectorSyncGeneration,
                and_(
                    ConnectorSyncGeneration.organization_id
                    == ConnectorSyncFileWorkItem.organization_id,
                    ConnectorSyncGeneration.connector_id
                    == ConnectorSyncFileWorkItem.connector_id,
                    ConnectorSyncGeneration.connector_scope_id
                    == ConnectorSyncFileWorkItem.connector_scope_id,
                    ConnectorSyncGeneration.id
                    == ConnectorSyncFileWorkItem.generation_id,
                    ConnectorSyncGeneration.profile_fingerprint
                    == ConnectorSyncFileWorkItem.profile_fingerprint,
                ),
            )
            .join(
                ConnectorSyncJob,
                and_(
                    ConnectorSyncJob.organization_id
                    == ConnectorSyncGeneration.organization_id,
                    ConnectorSyncJob.connector_id
                    == ConnectorSyncGeneration.connector_id,
                    ConnectorSyncJob.connector_scope_id
                    == ConnectorSyncGeneration.connector_scope_id,
                    ConnectorSyncJob.id == ConnectorSyncGeneration.sync_job_id,
                ),
            )
            .join(
                ConnectorScope,
                and_(
                    ConnectorScope.organization_id
                    == ConnectorSyncGeneration.organization_id,
                    ConnectorScope.connector_id
                    == ConnectorSyncGeneration.connector_id,
                    ConnectorScope.id
                    == ConnectorSyncGeneration.connector_scope_id,
                    ConnectorScope.external_scope_key
                    == ConnectorSyncGeneration.repository_identity,
                ),
            )
            .where(
                ConnectorSyncFileWorkItem.organization_id == organization_id,
                ConnectorSyncFileWorkItem.connector_id == connector_id,
                ConnectorSyncFileWorkItem.connector_scope_id == connector_scope_id,
                ConnectorSyncFileWorkItem.generation_id == generation_id,
                ConnectorSyncFileWorkItem.id == work_item_id,
                ConnectorSyncGeneration.provider_key == provider_key,
                ConnectorSyncGeneration.profile_fingerprint == profile_fingerprint,
                ConnectorSyncGeneration.profile_fingerprint
                == reservation.target_profile_fingerprint,
                ConnectorSyncGeneration.commit_object_id
                == ConnectorSyncFileWorkItem.provider_revision_id,
                ConnectorSyncGeneration.commit_object_id
                == reservation.target_provider_revision_id,
                ConnectorSyncGeneration.status
                == RepositoryGenerationStatus.PROCESSING.value,
                ConnectorSyncGeneration.discovery_complete.is_(True),
                ConnectorSyncJob.status != "cancelled",
                ConnectorSyncJob.cancel_requested_at.is_(None),
            )
            .with_for_update(of=ConnectorSyncFileWorkItem, skip_locked=True),
            "controlled file-work target lock failed",
        )
        if row is None:
            return TargetedFileWorkClaimResult("target_unavailable")
        if (
            row.source_key_hash != reservation.target_source_key_hash
            or row.provider_blob_id != reservation.target_provider_blob_id
            or row.provider_revision_id != reservation.target_provider_revision_id
            or row.profile_fingerprint != reservation.target_profile_fingerprint
        ):
            return TargetedFileWorkClaimResult("target_mismatch")
        if row.cancel_requested_at is not None:
            if (
                row.status == FileWorkStatus.RUNNING.value
                and row.lease_expires_at is not None
                and row.lease_expires_at <= now
            ):
                _apply_terminal(row, FileWorkStatus.CANCELLED.value, now)
                self._release_work_reservation(reservation, now=now)
                self._flush("controlled cancelled file work recovery failed")
                return TargetedFileWorkClaimResult(
                    "recovered_cancelled", mutated=True
                )
            return TargetedFileWorkClaimResult("cancelled")
        if row.status == FileWorkStatus.RUNNING.value:
            if row.lease_expires_at is None or row.lease_expires_at > now:
                return TargetedFileWorkClaimResult("target_busy")
            if row.attempt_count >= row.max_attempts:
                row.last_error_category = "internal"
                row.last_error_code = "lease_expired"
                _apply_terminal(row, FileWorkStatus.FAILED.value, now)
                self._release_work_reservation(reservation, now=now)
                self._flush("controlled exhausted file work recovery failed")
                return TargetedFileWorkClaimResult("recovered_failed", mutated=True)
            row.status = FileWorkStatus.RETRY_WAIT.value
            row.next_attempt_at = now
            row.last_error_category = "internal"
            row.last_error_code = "lease_expired"
            _clear_lease(row)
            reservation.processor_lease_id = None
            row.updated_at = now
        elif row.status in {status.value for status in TERMINAL_FILE_WORK_STATUSES}:
            return TargetedFileWorkClaimResult(f"already_{row.status}")
        elif row.status not in {
            FileWorkStatus.PENDING.value,
            FileWorkStatus.RETRY_WAIT.value,
        }:
            return TargetedFileWorkClaimResult("target_unavailable")

        if row.next_attempt_at is None or row.next_attempt_at > now:
            return TargetedFileWorkClaimResult("retry_not_due")
        if row.attempt_count >= row.max_attempts:
            return TargetedFileWorkClaimResult("attempts_exhausted")
        lease = self._claim_locked_row(
            row,
            worker_id=worker_id,
            now=now,
            lease_duration=lease_duration,
            reservation_id=reservation.id,
        )
        reservation.processor_lease_id = lease.lease_id
        self._flush("controlled file work could not be acquired")
        return TargetedFileWorkClaimResult("acquired", lease, True)

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
                _generic_work_reservation_available(),
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

    def claim_next_available_fair(
        self,
        *,
        provider_key: str,
        profile_fingerprint: str,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> FileWorkLease | None:
        """Atomically claim from the least-recently-served eligible organization."""
        provider_key = _code("provider_key", provider_key, 64)
        profile_fingerprint = _identifier(
            "profile_fingerprint", profile_fingerprint, 255
        )
        worker_id = _worker_id(worker_id)
        now = _aware("now", now)
        lease_duration = _lease_duration(lease_duration)

        attempted_organizations: set[UUID] = set()
        for _ in range(MAX_FAIR_SELECTION_ATTEMPTS):
            selected = self._select_fair_organization(
                provider_key=provider_key,
                profile_fingerprint=profile_fingerprint,
                now=now,
                excluded=attempted_organizations,
            )
            if selected is None:
                return None
            organization_id, schedule = selected
            lease = self._claim_fair_organization_item(
                organization_id=organization_id,
                provider_key=provider_key,
                profile_fingerprint=profile_fingerprint,
                worker_id=worker_id,
                now=now,
                lease_duration=lease_duration,
            )
            if lease is None:
                attempted_organizations.add(organization_id)
                continue

            try:
                claim_sequence = self._session.scalar(
                    select(FAIR_CLAIM_SEQUENCE.next_value())
                )
            except SQLAlchemyError as exc:
                raise SyncWorkLedgerPersistenceError(
                    "fair organization sequence allocation failed"
                ) from exc
            if not isinstance(claim_sequence, int) or claim_sequence < 1:
                raise SyncWorkLedgerPersistenceError(
                    "fair organization sequence allocation was invalid"
                )
            if schedule is None:
                # The never-served selection and this write are separate SQL
                # statements. Under READ COMMITTED, another worker can create
                # the schedule after our selection snapshot. Advance that row
                # atomically instead of losing a valid, disjoint item claim.
                try:
                    self._session.execute(
                        insert(ConnectorSyncOrganizationClaimSchedule)
                        .values(
                            organization_id=organization_id,
                            last_claim_sequence=claim_sequence,
                            claim_count=1,
                            last_claimed_at=now,
                            created_at=now,
                            updated_at=now,
                        )
                        .on_conflict_do_update(
                            index_elements=[
                                ConnectorSyncOrganizationClaimSchedule.organization_id
                            ],
                            set_={
                                "last_claim_sequence": claim_sequence,
                                "claim_count": (
                                    ConnectorSyncOrganizationClaimSchedule.claim_count
                                    + 1
                                ),
                                "last_claimed_at": now,
                                "updated_at": now,
                            },
                        )
                    )
                except SQLAlchemyError as exc:
                    raise SyncWorkLedgerPersistenceError(
                        "fair organization claim could not be advanced"
                    ) from exc
            else:
                schedule.last_claim_sequence = claim_sequence
                schedule.claim_count += 1
                schedule.last_claimed_at = now
                schedule.updated_at = now
            self._flush("fair organization claim could not be advanced")
            return FileWorkLease(
                lease.organization_id,
                lease.connector_id,
                lease.connector_scope_id,
                lease.generation_id,
                lease.work_item_id,
                lease.worker_id,
                lease.lease_id,
                lease.fencing_token,
                lease.attempt_number,
                lease.max_attempts,
                lease.lease_expires_at,
                fairness_claim_sequence=claim_sequence,
            )
        return None

    def _select_fair_organization(
        self,
        *,
        provider_key: str,
        profile_fingerprint: str,
        now: datetime,
        excluded: set[UUID],
    ) -> tuple[UUID, ConnectorSyncOrganizationClaimSchedule | None] | None:
        excluded_ids = tuple(sorted(excluded))
        eligible_without_schedule = select(Organization.id).where(
            ~exists(
                select(1).where(
                    ConnectorSyncOrganizationClaimSchedule.organization_id
                    == Organization.id
                )
            ),
            _eligible_file_work_candidate(
                organization_id=Organization.id,
                provider_key=provider_key,
                profile_fingerprint=profile_fingerprint,
                now=now,
            ).is_not(None),
        )
        if excluded_ids:
            eligible_without_schedule = eligible_without_schedule.where(
                Organization.id.not_in(excluded_ids)
            )
        organization_id = self._scalar(
            eligible_without_schedule.order_by(Organization.id)
            .with_for_update(of=Organization, skip_locked=True)
            .limit(1),
            "never-served fair organization selection failed",
        )
        if organization_id is not None:
            return organization_id, None

        served = select(ConnectorSyncOrganizationClaimSchedule).where(
            _eligible_file_work_candidate(
                organization_id=ConnectorSyncOrganizationClaimSchedule.organization_id,
                provider_key=provider_key,
                profile_fingerprint=profile_fingerprint,
                now=now,
            ).is_not(None)
        )
        if excluded_ids:
            served = served.where(
                ConnectorSyncOrganizationClaimSchedule.organization_id.not_in(
                    excluded_ids
                )
            )
        schedule = self._one(
            served.order_by(
                ConnectorSyncOrganizationClaimSchedule.last_claim_sequence,
                ConnectorSyncOrganizationClaimSchedule.organization_id,
            )
            .with_for_update(
                of=ConnectorSyncOrganizationClaimSchedule,
                skip_locked=True,
            )
            .limit(1),
            "served fair organization selection failed",
        )
        if schedule is None:
            return None
        return schedule.organization_id, schedule

    def _claim_fair_organization_item(
        self,
        *,
        organization_id: UUID,
        provider_key: str,
        profile_fingerprint: str,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> FileWorkLease | None:
        statement = (
            select(ConnectorSyncFileWorkItem)
            .join(
                ConnectorSyncGeneration,
                (ConnectorSyncGeneration.organization_id == ConnectorSyncFileWorkItem.organization_id)
                & (ConnectorSyncGeneration.connector_id == ConnectorSyncFileWorkItem.connector_id)
                & (
                    ConnectorSyncGeneration.connector_scope_id
                    == ConnectorSyncFileWorkItem.connector_scope_id
                )
                & (ConnectorSyncGeneration.id == ConnectorSyncFileWorkItem.generation_id)
                & (
                    ConnectorSyncGeneration.profile_fingerprint
                    == ConnectorSyncFileWorkItem.profile_fingerprint
                ),
            )
            .join(
                ConnectorSyncJob,
                (ConnectorSyncJob.organization_id == ConnectorSyncGeneration.organization_id)
                & (ConnectorSyncJob.connector_id == ConnectorSyncGeneration.connector_id)
                & (ConnectorSyncJob.connector_scope_id == ConnectorSyncGeneration.connector_scope_id)
                & (ConnectorSyncJob.id == ConnectorSyncGeneration.sync_job_id),
            )
            .where(
                ConnectorSyncFileWorkItem.organization_id == organization_id,
                ConnectorSyncGeneration.provider_key == provider_key,
                ConnectorSyncGeneration.profile_fingerprint == profile_fingerprint,
                ConnectorSyncFileWorkItem.profile_fingerprint == profile_fingerprint,
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
                _generic_work_reservation_available(),
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
        return self._claim_one(
            statement,
            worker_id=worker_id,
            now=now,
            lease_duration=lease_duration,
        )

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
        return self._claim_locked_row(
            row,
            worker_id=worker_id,
            now=now,
            lease_duration=lease_duration,
        )

    def _claim_locked_row(
        self,
        row: ConnectorSyncFileWorkItem,
        *,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
        reservation_id: UUID | None = None,
    ) -> FileWorkLease:
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
        return _lease(row, reservation_id=reservation_id)

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
        return _lease(row, reservation_id=lease.reservation_id)

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
        self._release_work_reservation_for_lease(lease, now=now)
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
            self._release_work_reservation_for_lease(lease, now=now)
        elif retry_at is not None and row.attempt_count < row.max_attempts:
            row.status = FileWorkStatus.RETRY_WAIT.value
            row.next_attempt_at = retry_at
            row.terminal_at = None
            _clear_lease(row)
            self._retain_work_reservation_for_retry(lease)
            row.updated_at = now
        else:
            _apply_terminal(row, FileWorkStatus.FAILED.value, now, preserve_counters=True)
            self._release_work_reservation_for_lease(lease, now=now)
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
        # Target acquisition, heartbeats and terminal transitions all lock the
        # reservation before the work item. Cancellation must use the same
        # ordering or it can deadlock with a concurrent exact-target claim.
        reservation = self._lock_work_reservation(
            organization_id, generation_id, work_item_id
        )
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
            if reservation is not None:
                self._release_work_reservation(reservation, now=now)
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
        self._release_work_reservation_for_lease(lease, now=now)
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
                _generic_work_reservation_available(),
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
                _generic_work_reservation_available(),
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

    def promote_generation(
        self,
        request: GenerationPromotionRequest,
        *,
        now: datetime,
    ) -> GenerationPromotionResult:
        """Atomically validate, activate, and retire generation retrieval state."""
        if not isinstance(request, GenerationPromotionRequest):
            raise InvalidSyncWorkLedgerRequest("generation promotion request is invalid")
        now = _aware("now", now)

        scope = self._one(
            select(ConnectorScope)
            .where(
                ConnectorScope.organization_id == request.organization_id,
                ConnectorScope.connector_id == request.connector_id,
                ConnectorScope.id == request.connector_scope_id,
            )
            .with_for_update(),
            "generation promotion scope lock failed",
        )
        generation = self._locked_generation(
            request.organization_id, request.generation_id
        )
        if scope is None or generation is None:
            raise SyncWorkLedgerNotFound("generation promotion context was not found")
        if (
            scope.status != "active"
            or scope.scope_type != "repository"
            or scope.external_scope_key != request.repository_identity
            or not _generation_promotion_matches(generation, request)
        ):
            raise SyncWorkLedgerConflict("generation promotion attribution changed")

        job = self._one(
            select(ConnectorSyncJob)
            .where(
                ConnectorSyncJob.organization_id == request.organization_id,
                ConnectorSyncJob.connector_id == request.connector_id,
                ConnectorSyncJob.connector_scope_id == request.connector_scope_id,
                ConnectorSyncJob.id == request.sync_job_id,
            )
            .with_for_update(),
            "generation promotion job lock failed",
        )
        if (
            job is None
            or job.status != "succeeded"
            or job.cancel_requested_at is not None
        ):
            raise SyncWorkLedgerConflict("generation promotion job is not successful")

        activations = self._all(
            select(ConnectorSyncGenerationActivation)
            .where(
                ConnectorSyncGenerationActivation.organization_id
                == request.organization_id,
                ConnectorSyncGenerationActivation.connector_id
                == request.connector_id,
                ConnectorSyncGenerationActivation.connector_scope_id
                == request.connector_scope_id,
            )
            .order_by(
                ConnectorSyncGenerationActivation.activated_at,
                ConnectorSyncGenerationActivation.id,
            )
            .with_for_update(),
            "generation activation lock failed",
        )
        existing = next(
            (row for row in activations if row.generation_id == request.generation_id),
            None,
        )
        active = next(
            (
                row
                for row in activations
                if row.status == GenerationActivationStatus.ACTIVE.value
            ),
            None,
        )
        if existing is not None and existing.status != GenerationActivationStatus.ACTIVE.value:
            raise SyncWorkLedgerConflict("retired generation cannot be promoted")
        if active is not None and active is not existing:
            active_generation = self._locked_generation(
                request.organization_id, active.generation_id
            )
            if active_generation is None:
                raise SyncWorkLedgerConflict("active generation is unavailable")

        materialization_count, chunk_count = self._validate_promotion_projection(
            generation, allow_completed=existing is not None
        )
        if existing is not None:
            if not _activation_matches(existing, request):
                raise SyncWorkLedgerConflict("active generation attribution changed")
            if generation.manifest_schema_version == 2 and not generation.reconciliation_eligible:
                raise SyncWorkLedgerConflict(
                    "active generation reconciliation authority is unavailable"
                )
            return GenerationPromotionResult(
                _activation_view(existing),
                False,
                None,
                materialization_count,
                chunk_count,
            )

        newer = self._one(
            select(func.count(ConnectorSyncGeneration.id)).where(
                ConnectorSyncGeneration.organization_id == request.organization_id,
                ConnectorSyncGeneration.connector_id == request.connector_id,
                ConnectorSyncGeneration.connector_scope_id
                == request.connector_scope_id,
                ConnectorSyncGeneration.id != request.generation_id,
                or_(
                    ConnectorSyncGeneration.created_at > generation.created_at,
                    and_(
                        ConnectorSyncGeneration.created_at == generation.created_at,
                        ConnectorSyncGeneration.id > generation.id,
                    ),
                ),
            ),
            "newer generation lookup failed",
        )
        if newer:
            raise SyncWorkLedgerConflict("stale generation cannot be promoted")

        retired_generation_id = None
        if active is not None:
            active.status = GenerationActivationStatus.RETIRED.value
            active.retired_at = now
            active.updated_at = now
            retired_generation_id = active.generation_id

        activation = ConnectorSyncGenerationActivation(
            id=self._new_uuid("activation_id", self._activation_id_factory),
            organization_id=request.organization_id,
            connector_id=request.connector_id,
            connector_scope_id=request.connector_scope_id,
            generation_id=request.generation_id,
            repository_identity=request.repository_identity,
            commit_object_id=request.commit_object_id,
            profile_fingerprint=request.profile_fingerprint,
            status=GenerationActivationStatus.ACTIVE.value,
            activated_at=now,
            retired_at=None,
            created_at=now,
            updated_at=now,
        )
        self._session.add(activation)
        generation.status = RepositoryGenerationStatus.COMPLETED.value
        generation.terminal_at = now
        if generation.manifest_schema_version == 2:
            generation.reconciliation_eligible = True
            generation.reconciliation_eligible_at = now
        generation.updated_at = now
        self._flush("generation promotion failed")
        return GenerationPromotionResult(
            _activation_view(activation),
            True,
            retired_generation_id,
            materialization_count,
            chunk_count,
        )

    def project_citations_and_promote_generation(
        self,
        request: GenerationPromotionRequest,
        profile: GenerationCitationProjectionProfile,
        *,
        now: datetime,
    ) -> GenerationPromotionResult:
        """Project staged citations and activate without committing internally."""
        if not isinstance(request, GenerationPromotionRequest):
            raise InvalidSyncWorkLedgerRequest("generation promotion request is invalid")
        if not isinstance(profile, GenerationCitationProjectionProfile):
            raise InvalidSyncWorkLedgerRequest("generation projection profile is invalid")
        now = _aware("now", now)
        if profile.profile_fingerprint != request.profile_fingerprint:
            raise SyncWorkLedgerConflict("generation projection profile changed")

        # A savepoint makes the operation safe even if a caller catches a
        # validation conflict and later commits its surrounding transaction.
        with self._session.begin_nested():
            scope = self._one(
                select(ConnectorScope)
                .where(
                    ConnectorScope.organization_id == request.organization_id,
                    ConnectorScope.connector_id == request.connector_id,
                    ConnectorScope.id == request.connector_scope_id,
                )
                .with_for_update(),
                "generation projection scope lock failed",
            )
            existing = self._one(
                select(ConnectorSyncGenerationActivation)
                .where(
                    ConnectorSyncGenerationActivation.organization_id
                    == request.organization_id,
                    ConnectorSyncGenerationActivation.connector_id
                    == request.connector_id,
                    ConnectorSyncGenerationActivation.connector_scope_id
                    == request.connector_scope_id,
                    ConnectorSyncGenerationActivation.generation_id
                    == request.generation_id,
                    ConnectorSyncGenerationActivation.status
                    == GenerationActivationStatus.ACTIVE.value,
                )
                .with_for_update(),
                "generation activation replay lookup failed",
            )
            if existing is not None:
                return self.promote_generation(request, now=now)
            generation = self._locked_generation(
                request.organization_id, request.generation_id
            )
            if (
                scope is None
                or generation is None
                or scope.status != "active"
                or scope.scope_type != "repository"
                or scope.external_scope_key != request.repository_identity
                or not _generation_promotion_matches(generation, request)
            ):
                raise SyncWorkLedgerConflict("generation projection attribution changed")
            job = self._one(
                select(ConnectorSyncJob)
                .where(
                    ConnectorSyncJob.organization_id == request.organization_id,
                    ConnectorSyncJob.connector_id == request.connector_id,
                    ConnectorSyncJob.connector_scope_id == request.connector_scope_id,
                    ConnectorSyncJob.id == request.sync_job_id,
                )
                .with_for_update(),
                "generation projection job lock failed",
            )
            if job is None or job.status != "succeeded" or job.cancel_requested_at:
                raise SyncWorkLedgerConflict("generation promotion job is not successful")
            newer = self._one(
                select(ConnectorSyncGeneration.id)
                .where(
                    ConnectorSyncGeneration.organization_id
                    == request.organization_id,
                    ConnectorSyncGeneration.connector_id == request.connector_id,
                    ConnectorSyncGeneration.connector_scope_id
                    == request.connector_scope_id,
                    ConnectorSyncGeneration.id != request.generation_id,
                    or_(
                        ConnectorSyncGeneration.created_at > generation.created_at,
                        and_(
                            ConnectorSyncGeneration.created_at
                            == generation.created_at,
                            ConnectorSyncGeneration.id > generation.id,
                        ),
                    ),
                )
                .limit(1)
                .with_for_update(),
                "newer generation projection lookup failed",
            )
            if newer is not None:
                raise SyncWorkLedgerConflict("stale generation cannot be promoted")

            self._validate_promotion_projection(
                generation, allow_completed=False, require_citations=False
            )
            self._project_generation_citations(generation, profile, now)
            self._flush("generation citation projection failed")
            return self.promote_generation(request, now=now)

    def _project_generation_citations(
        self,
        generation: ConnectorSyncGeneration,
        profile: GenerationCitationProjectionProfile,
        now: datetime,
    ) -> None:
        materializations = self._all(
            select(ConnectorSyncFileMaterialization)
            .where(
                ConnectorSyncFileMaterialization.organization_id
                == generation.organization_id,
                ConnectorSyncFileMaterialization.generation_id == generation.id,
            )
            .order_by(ConnectorSyncFileMaterialization.source_item_key)
            .with_for_update(),
            "generation materialization projection lookup failed",
        )
        work_by_id = {
            row.id: row
            for row in self._all(
                select(ConnectorSyncFileWorkItem)
                .where(
                    ConnectorSyncFileWorkItem.organization_id
                    == generation.organization_id,
                    ConnectorSyncFileWorkItem.generation_id == generation.id,
                )
                .with_for_update(),
                "generation work projection lookup failed",
            )
        }
        repository_id = _github_repository_id(generation.repository_identity)
        for materialization in materializations:
            work = work_by_id.get(materialization.work_item_id)
            if (
                work is None
                or materialization.profile_fingerprint != profile.profile_fingerprint
                or materialization.embedding_model != profile.embedding_model
                or work.file_size_bytes is None
                or work.mime_type != materialization.mime_type
            ):
                raise SyncWorkLedgerConflict("generation projection profile changed")
            chunks = self._all(
                select(ConnectorSyncFileMaterializationChunk)
                .where(
                    ConnectorSyncFileMaterializationChunk.organization_id
                    == generation.organization_id,
                    ConnectorSyncFileMaterializationChunk.generation_id
                    == generation.id,
                    ConnectorSyncFileMaterializationChunk.materialization_id
                    == materialization.id,
                )
                .order_by(ConnectorSyncFileMaterializationChunk.chunk_index)
                .with_for_update(),
                "generation chunk projection lookup failed",
            )
            if len(chunks) != materialization.chunk_count:
                raise SyncWorkLedgerConflict("generation materialization chunks are incomplete")

            source = self._one(
                select(SourceItem)
                .where(
                    SourceItem.organization_id == generation.organization_id,
                    SourceItem.connector_id == generation.connector_id,
                    SourceItem.source_item_key == materialization.source_item_key,
                )
                .with_for_update(),
                "generation source projection lookup failed",
            )
            metadata = {
                "provider": "github",
                "repository_id": repository_id,
                "repository_identity": generation.repository_identity,
                "repository_path": materialization.repository_path,
                "blob_object_id": materialization.provider_blob_id,
                "snapshot_commit_id": generation.commit_object_id,
                "file_extension": PurePosixPath(materialization.repository_path).suffix.casefold(),
                "size_bytes": work.file_size_bytes,
            }
            restored = source is not None and source.status != "active"
            if source is None:
                source = SourceItem(
                    id=self._new_uuid("source_item_id", uuid4),
                    organization_id=generation.organization_id,
                    connector_id=generation.connector_id,
                    source_item_key=materialization.source_item_key,
                    parent_source_item_key=None,
                    source_item_type="file",
                    title=materialization.title,
                    source_url=None,
                    mime_type=materialization.mime_type,
                    source_checksum=materialization.content_checksum,
                    source_version=materialization.provider_blob_id,
                    size_bytes=work.file_size_bytes,
                    source_created_at=None,
                    source_modified_at=None,
                    first_seen_at=now,
                    last_seen_at=now,
                    status="active",
                    deleted_at=None,
                    source_metadata=metadata,
                    metadata_schema_version=1,
                )
                self._session.add(source)
                self._flush("generation source projection failed")
            else:
                if source.source_item_type != "file":
                    raise SyncWorkLedgerConflict("generation source type changed")
                # An active source can be authorized by another scope.  Its
                # mutable legacy projection remains authoritative there until
                # that scope is independently activated.  Activated ledger
                # retrieval binds the immutable staged version directly, so
                # changing the shared legacy source is neither required nor
                # safe here.
                if restored:
                    source.title = materialization.title
                    source.mime_type = materialization.mime_type
                    source.source_checksum = materialization.content_checksum
                    source.source_version = materialization.provider_blob_id
                    source.size_bytes = work.file_size_bytes
                    source.last_seen_at = now
                    source.status = "active"
                    source.deleted_at = None
                    source.source_metadata = metadata
                    source.metadata_schema_version = 1
                    source.updated_at = now

            memberships = self._all(
                select(SourceItemScopeMembership)
                .where(
                    SourceItemScopeMembership.organization_id
                    == generation.organization_id,
                    SourceItemScopeMembership.connector_id == generation.connector_id,
                    SourceItemScopeMembership.source_item_id == source.id,
                )
                .order_by(SourceItemScopeMembership.connector_scope_id)
                .with_for_update(),
                "generation membership projection lookup failed",
            )
            membership = next(
                (row for row in memberships if row.connector_scope_id == generation.connector_scope_id),
                None,
            )
            if membership is None:
                self._session.add(
                    SourceItemScopeMembership(
                        id=self._new_uuid("source_membership_id", uuid4),
                        organization_id=generation.organization_id,
                        connector_id=generation.connector_id,
                        source_item_id=source.id,
                        connector_scope_id=generation.connector_scope_id,
                        status="active",
                        first_discovered_at=now,
                        last_seen_at=now,
                        removed_at=None,
                    )
                )
            else:
                if membership.status != "active" or membership.removed_at is not None:
                    membership.status = "active"
                    membership.last_seen_at = now
                    membership.removed_at = None
                    membership.updated_at = now

            versions = self._all(
                select(DocumentVersion)
                .where(
                    DocumentVersion.organization_id == generation.organization_id,
                    DocumentVersion.source_item_id == source.id,
                )
                .order_by(DocumentVersion.version_number)
                .with_for_update(),
                "generation version projection lookup failed",
            )
            current = next((row for row in versions if row.is_current), None)
            matching_versions = [
                row
                for row in versions
                if row.provider_version_id == materialization.provider_blob_id
                and row.content_checksum == materialization.content_checksum
                and row.lifecycle == "available"
                and row.version_metadata.get("provider") == "github"
                and row.version_metadata.get("commit_object_id")
                == generation.commit_object_id
                and row.version_metadata.get("blob_object_id")
                == materialization.provider_blob_id
            ]
            if len(matching_versions) > 1:
                raise SyncWorkLedgerConflict("generation citation version is duplicated")
            version = matching_versions[0] if matching_versions else None
            if version is None:
                if restored and current is not None:
                    current.is_current = False
                version = DocumentVersion(
                    id=self._new_uuid("document_version_id", uuid4),
                    organization_id=generation.organization_id,
                    connector_id=generation.connector_id,
                    source_item_id=source.id,
                    version_number=1 + max((row.version_number for row in versions), default=0),
                    provider_version_id=materialization.provider_blob_id,
                    content_checksum=materialization.content_checksum,
                    checksum_algorithm="sha256",
                    source_modified_at=None,
                    source_size_bytes=work.file_size_bytes,
                    content_type=materialization.mime_type,
                    file_extension=PurePosixPath(materialization.repository_path).suffix.casefold(),
                    version_cause=("restored" if restored else "content_changed" if versions else "discovered"),
                    lifecycle="available",
                    is_current=current is None or restored,
                    discovered_at=now,
                    version_metadata={
                        "provider": "github",
                        "repository_id": repository_id,
                        "commit_object_id": generation.commit_object_id,
                        "blob_object_id": materialization.provider_blob_id,
                    },
                    metadata_schema_version=1,
                )
                self._session.add(version)
                self._flush("generation version projection failed")
            elif restored and not version.is_current:
                if current is not None:
                    current.is_current = False
                version.is_current = True

            document = self._one(
                select(Document)
                .where(
                    Document.organization_id == generation.organization_id,
                    Document.source_type == "github",
                    Document.source_document_key == materialization.source_item_key,
                )
                .with_for_update(),
                "generation document projection lookup failed",
            )
            if document is None:
                document = Document(
                    id=self._new_uuid("document_id", uuid4),
                    organization_id=generation.organization_id,
                    source_type="github",
                    source_document_key=materialization.source_item_key,
                    title=materialization.title,
                    source_url=None,
                    mime_type=materialization.mime_type,
                    checksum_latest=materialization.content_checksum,
                    status="ready",
                    source_created_at=None,
                    source_updated_at=None,
                    deleted_at=None,
                )
                self._session.add(document)
                self._flush("generation document projection failed")
            else:
                if restored:
                    document.title = materialization.title
                    document.mime_type = materialization.mime_type
                    document.checksum_latest = materialization.content_checksum
                    document.status = "ready"
                    document.deleted_at = None
                    document.updated_at = now

            links = self._all(
                select(DocumentVersionDocument)
                .where(
                    DocumentVersionDocument.organization_id == generation.organization_id,
                    or_(
                        DocumentVersionDocument.document_id == document.id,
                        DocumentVersionDocument.document_version_id == version.id,
                    ),
                )
                .with_for_update(),
                "generation citation link lookup failed",
            )
            version_link = next(
                (row for row in links if row.document_version_id == version.id), None
            )
            document_link = next(
                (row for row in links if row.document_id == document.id), None
            )
            if version_link is not None and version_link.document_id != document.id:
                raise SyncWorkLedgerConflict("generation citation document changed")
            if restored:
                for link in links:
                    self._session.delete(link)
                if links:
                    self._flush("generation citation link replacement failed")
                version_link = None
                document_link = None
            # The legacy one-to-one link denotes its current projection.  A
            # changed active source keeps that link intact for other scopes;
            # ledger retrieval resolves the immutable staged version without
            # rewriting the legacy pointer.  New/restored sources establish
            # the link normally.
            if version_link is None and document_link is None:
                self._session.add(
                    DocumentVersionDocument(
                        id=self._new_uuid("document_version_document_id", uuid4),
                        organization_id=generation.organization_id,
                        document_version_id=version.id,
                        document_id=document.id,
                        linked_at=now,
                    )
                )

            states = self._all(
                select(DocumentIndexingState)
                .where(
                    DocumentIndexingState.organization_id == generation.organization_id,
                    DocumentIndexingState.document_version_id == version.id,
                    DocumentIndexingState.profile_fingerprint == profile.profile_fingerprint,
                )
                .with_for_update(),
                "generation indexing projection lookup failed",
            )
            if len(states) > 1:
                raise SyncWorkLedgerConflict("generation indexing projection is duplicated")
            state = states[0] if states else None
            if state is None:
                self._session.add(
                    DocumentIndexingState(
                        id=self._new_uuid("indexing_state_id", uuid4),
                        organization_id=generation.organization_id,
                        document_version_id=version.id,
                        extraction_profile=profile.extraction_profile,
                        extraction_version=profile.extraction_version,
                        chunking_profile=profile.chunking_profile,
                        chunking_version=profile.chunking_version,
                        embedding_provider=profile.embedding_provider,
                        embedding_model=profile.embedding_model,
                        embedding_dimensions=profile.embedding_dimensions,
                        profile_fingerprint=profile.profile_fingerprint,
                        desired_generation=1,
                        indexed_generation=1,
                        status="indexed",
                        reason="new_version" if not versions else "content_changed",
                        attempt_count=0,
                        requested_at=now,
                        started_at=now,
                        completed_at=now,
                    )
                )
            elif (
                state.embedding_model != profile.embedding_model
                or state.embedding_dimensions != profile.embedding_dimensions
                or state.extraction_profile != profile.extraction_profile
                or state.extraction_version != profile.extraction_version
                or state.chunking_profile != profile.chunking_profile
                or state.chunking_version != profile.chunking_version
                or state.embedding_provider != profile.embedding_provider
            ):
                raise SyncWorkLedgerConflict("generation indexing profile changed")
            elif not (
                state.status == "indexed"
                and state.indexed_generation == state.desired_generation
                and state.last_error_category is None
                and state.last_error_code is None
                and state.next_retry_at is None
            ):
                state.desired_generation = max(1, state.desired_generation)
                state.indexed_generation = state.desired_generation
                state.status = "indexed"
                state.last_error_category = None
                state.last_error_code = None
                state.next_retry_at = None
                state.started_at = state.started_at or now
                state.completed_at = now
                state.updated_at = now

    def _validate_promotion_projection(
        self,
        generation: ConnectorSyncGeneration,
        *,
        allow_completed: bool,
        require_citations: bool = True,
    ) -> tuple[int, int]:
        allowed_statuses = {RepositoryGenerationStatus.PROCESSING.value}
        if allow_completed:
            allowed_statuses.add(RepositoryGenerationStatus.COMPLETED.value)
        if not generation.discovery_complete or generation.status not in allowed_statuses:
            raise SyncWorkLedgerConflict("generation is not promotion eligible")

        work_rows = self._all(
            select(ConnectorSyncFileWorkItem)
            .where(
                ConnectorSyncFileWorkItem.organization_id == generation.organization_id,
                ConnectorSyncFileWorkItem.generation_id == generation.id,
            )
            .order_by(ConnectorSyncFileWorkItem.id)
            .with_for_update(),
            "generation work validation failed",
        )
        if (
            len(work_rows) != generation.items_registered
            or any(row.status != FileWorkStatus.SUCCEEDED.value for row in work_rows)
            or len({row.source_key_hash for row in work_rows}) != len(work_rows)
            or len({row.repository_path for row in work_rows}) != len(work_rows)
        ):
            raise SyncWorkLedgerConflict("generation work is not completely successful")
        if generation.manifest_schema_version == 2:
            observations = self._all(
                select(ConnectorSyncGenerationObservation)
                .where(
                    ConnectorSyncGenerationObservation.organization_id
                    == generation.organization_id,
                    ConnectorSyncGenerationObservation.generation_id == generation.id,
                )
                .order_by(ConnectorSyncGenerationObservation.id)
                .with_for_update(),
                "generation observation validation failed",
            )
            observation_by_hash = {row.source_key_hash: row for row in observations}
            work_by_hash = {row.source_key_hash: row for row in work_rows}
            eligible_hashes = {
                row.source_key_hash
                for row in observations
                if row.disposition == GenerationObservationDisposition.ELIGIBLE.value
            }
            if (
                len(observations) != generation.items_discovered
                or len(observation_by_hash) != len(observations)
                or set(work_by_hash) != eligible_hashes
                or any(
                    not _work_matches_observation(work_by_hash[source_hash], observation)
                    for source_hash, observation in observation_by_hash.items()
                    if observation.disposition
                    == GenerationObservationDisposition.ELIGIBLE.value
                )
            ):
                raise SyncWorkLedgerConflict(
                    "generation observations are incomplete"
                )

        materializations = self._all(
            select(ConnectorSyncFileMaterialization)
            .where(
                ConnectorSyncFileMaterialization.organization_id
                == generation.organization_id,
                ConnectorSyncFileMaterialization.generation_id == generation.id,
            )
            .order_by(ConnectorSyncFileMaterialization.id)
            .with_for_update(),
            "generation materialization validation failed",
        )
        work_by_id = {row.id: row for row in work_rows}
        if len(materializations) != len(work_rows):
            raise SyncWorkLedgerConflict("generation materializations are incomplete")
        for materialization in materializations:
            work = work_by_id.get(materialization.work_item_id)
            if work is None or not _persisted_promotion_materialization_matches(
                materialization, generation, work
            ):
                raise SyncWorkLedgerConflict("generation materialization attribution changed")

        chunk_rows = self._all(
            select(ConnectorSyncFileMaterializationChunk)
            .where(
                ConnectorSyncFileMaterializationChunk.organization_id
                == generation.organization_id,
                ConnectorSyncFileMaterializationChunk.generation_id == generation.id,
            )
            .order_by(
                ConnectorSyncFileMaterializationChunk.materialization_id,
                ConnectorSyncFileMaterializationChunk.chunk_index,
            )
            .with_for_update(),
            "generation materialization chunk validation failed",
        )
        chunks_by_materialization: dict[UUID, list[ConnectorSyncFileMaterializationChunk]] = {}
        for chunk in chunk_rows:
            chunks_by_materialization.setdefault(chunk.materialization_id, []).append(chunk)
        if any(
            len(chunks_by_materialization.get(materialization.id, ()))
            != materialization.chunk_count
            or any(
                chunk.chunk_index != index
                or chunk.embedding_model != materialization.embedding_model
                for index, chunk in enumerate(
                    chunks_by_materialization.get(materialization.id, ())
                )
            )
            for materialization in materializations
        ):
            raise SyncWorkLedgerConflict("generation materialization chunks are incomplete")

        if not require_citations:
            return len(materializations), len(chunk_rows)

        projection_count = self._promotion_projection_count(generation)
        if projection_count != len(materializations):
            raise SyncWorkLedgerConflict("generation citation projection is incomplete")
        return len(materializations), len(chunk_rows)

    def _validate_reconciliation_projection(
        self, generation: ConnectorSyncGeneration
    ) -> None:
        """Validate an immutable completed generation with bounded Python state."""
        work_total, succeeded_total, distinct_work_hashes, distinct_work_paths = self._row(
            select(
                func.count(ConnectorSyncFileWorkItem.id),
                func.count(ConnectorSyncFileWorkItem.id).filter(
                    ConnectorSyncFileWorkItem.status == FileWorkStatus.SUCCEEDED.value
                ),
                func.count(func.distinct(ConnectorSyncFileWorkItem.source_key_hash)),
                func.count(func.distinct(ConnectorSyncFileWorkItem.repository_path)),
            ).where(
                ConnectorSyncFileWorkItem.organization_id == generation.organization_id,
                ConnectorSyncFileWorkItem.generation_id == generation.id,
            ),
            "generation work aggregate validation failed",
        )
        if (
            work_total != generation.items_registered
            or succeeded_total != work_total
            or distinct_work_hashes != work_total
            or distinct_work_paths != work_total
        ):
            raise SyncWorkLedgerConflict("generation work is not completely successful")

        observation_total, eligible_total, distinct_observation_hashes = self._row(
            select(
                func.count(ConnectorSyncGenerationObservation.id),
                func.count(ConnectorSyncGenerationObservation.id).filter(
                    ConnectorSyncGenerationObservation.disposition
                    == GenerationObservationDisposition.ELIGIBLE.value
                ),
                func.count(
                    func.distinct(ConnectorSyncGenerationObservation.source_key_hash)
                ),
            ).where(
                ConnectorSyncGenerationObservation.organization_id
                == generation.organization_id,
                ConnectorSyncGenerationObservation.generation_id == generation.id,
            ),
            "generation observation aggregate validation failed",
        )
        matched_observations = self._one(
            select(func.count(ConnectorSyncGenerationObservation.id))
            .select_from(ConnectorSyncGenerationObservation)
            .join(
                ConnectorSyncFileWorkItem,
                and_(
                    ConnectorSyncFileWorkItem.organization_id
                    == ConnectorSyncGenerationObservation.organization_id,
                    ConnectorSyncFileWorkItem.connector_id
                    == ConnectorSyncGenerationObservation.connector_id,
                    ConnectorSyncFileWorkItem.connector_scope_id
                    == ConnectorSyncGenerationObservation.connector_scope_id,
                    ConnectorSyncFileWorkItem.generation_id
                    == ConnectorSyncGenerationObservation.generation_id,
                    ConnectorSyncFileWorkItem.source_item_key
                    == ConnectorSyncGenerationObservation.source_item_key,
                    ConnectorSyncFileWorkItem.source_key_hash
                    == ConnectorSyncGenerationObservation.source_key_hash,
                    ConnectorSyncFileWorkItem.repository_path
                    == ConnectorSyncGenerationObservation.repository_path,
                    ConnectorSyncFileWorkItem.provider_blob_id
                    == ConnectorSyncGenerationObservation.provider_object_id,
                    ConnectorSyncFileWorkItem.provider_revision_id
                    == ConnectorSyncGenerationObservation.provider_revision_id,
                    ConnectorSyncFileWorkItem.profile_fingerprint
                    == ConnectorSyncGenerationObservation.profile_fingerprint,
                ),
            )
            .where(
                ConnectorSyncGenerationObservation.organization_id
                == generation.organization_id,
                ConnectorSyncGenerationObservation.generation_id == generation.id,
                ConnectorSyncGenerationObservation.disposition
                == GenerationObservationDisposition.ELIGIBLE.value,
            ),
            "generation observation work validation failed",
        )
        if (
            observation_total != generation.items_discovered
            or distinct_observation_hashes != observation_total
            or eligible_total != work_total
            or matched_observations != work_total
        ):
            raise SyncWorkLedgerConflict("generation observations are incomplete")

        materialization_total = self._one(
            select(func.count(ConnectorSyncFileMaterialization.id)).where(
                ConnectorSyncFileMaterialization.organization_id
                == generation.organization_id,
                ConnectorSyncFileMaterialization.generation_id == generation.id,
            ),
            "generation materialization aggregate validation failed",
        )
        matched_materializations = self._one(
            select(func.count(ConnectorSyncFileMaterialization.id))
            .select_from(ConnectorSyncFileMaterialization)
            .join(
                ConnectorSyncFileWorkItem,
                and_(
                    ConnectorSyncFileWorkItem.organization_id
                    == ConnectorSyncFileMaterialization.organization_id,
                    ConnectorSyncFileWorkItem.connector_id
                    == ConnectorSyncFileMaterialization.connector_id,
                    ConnectorSyncFileWorkItem.connector_scope_id
                    == ConnectorSyncFileMaterialization.connector_scope_id,
                    ConnectorSyncFileWorkItem.generation_id
                    == ConnectorSyncFileMaterialization.generation_id,
                    ConnectorSyncFileWorkItem.id
                    == ConnectorSyncFileMaterialization.work_item_id,
                    ConnectorSyncFileWorkItem.source_item_key
                    == ConnectorSyncFileMaterialization.source_item_key,
                    ConnectorSyncFileWorkItem.source_key_hash
                    == ConnectorSyncFileMaterialization.source_key_hash,
                    ConnectorSyncFileWorkItem.repository_path
                    == ConnectorSyncFileMaterialization.repository_path,
                    ConnectorSyncFileWorkItem.provider_blob_id
                    == ConnectorSyncFileMaterialization.provider_blob_id,
                    ConnectorSyncFileWorkItem.provider_revision_id
                    == ConnectorSyncFileMaterialization.provider_revision_id,
                    ConnectorSyncFileWorkItem.profile_fingerprint
                    == ConnectorSyncFileMaterialization.profile_fingerprint,
                ),
            )
            .where(
                ConnectorSyncFileMaterialization.organization_id
                == generation.organization_id,
                ConnectorSyncFileMaterialization.generation_id == generation.id,
                ConnectorSyncFileMaterialization.repository_identity
                == generation.repository_identity,
                ConnectorSyncFileMaterialization.branch_name == generation.branch_name,
                ConnectorSyncFileMaterialization.root_tree_object_id
                == generation.root_tree_object_id,
                ConnectorSyncFileMaterialization.profile_fingerprint
                == generation.profile_fingerprint,
                ConnectorSyncFileWorkItem.status == FileWorkStatus.SUCCEEDED.value,
            ),
            "generation materialization work validation failed",
        )
        if materialization_total != work_total or matched_materializations != work_total:
            raise SyncWorkLedgerConflict("generation materializations are incomplete")

        invalid_chunk_groups = self._one(
            select(func.count())
            .select_from(
                select(ConnectorSyncFileMaterialization.id)
                .outerjoin(
                    ConnectorSyncFileMaterializationChunk,
                    and_(
                        ConnectorSyncFileMaterializationChunk.organization_id
                        == ConnectorSyncFileMaterialization.organization_id,
                        ConnectorSyncFileMaterializationChunk.generation_id
                        == ConnectorSyncFileMaterialization.generation_id,
                        ConnectorSyncFileMaterializationChunk.materialization_id
                        == ConnectorSyncFileMaterialization.id,
                    ),
                )
                .where(
                    ConnectorSyncFileMaterialization.organization_id
                    == generation.organization_id,
                    ConnectorSyncFileMaterialization.generation_id == generation.id,
                )
                .group_by(
                    ConnectorSyncFileMaterialization.id,
                    ConnectorSyncFileMaterialization.chunk_count,
                )
                .having(
                    or_(
                        func.count(ConnectorSyncFileMaterializationChunk.id)
                        != ConnectorSyncFileMaterialization.chunk_count,
                        func.count(
                            func.distinct(
                                ConnectorSyncFileMaterializationChunk.chunk_index
                            )
                        )
                        != ConnectorSyncFileMaterialization.chunk_count,
                        func.min(ConnectorSyncFileMaterializationChunk.chunk_index) != 0,
                        func.max(ConnectorSyncFileMaterializationChunk.chunk_index)
                        != ConnectorSyncFileMaterialization.chunk_count - 1,
                    )
                )
                .subquery()
            ),
            "generation materialization chunk aggregate validation failed",
        )
        invalid_chunk_models = self._one(
            select(func.count(ConnectorSyncFileMaterializationChunk.id))
            .select_from(ConnectorSyncFileMaterializationChunk)
            .join(
                ConnectorSyncFileMaterialization,
                and_(
                    ConnectorSyncFileMaterialization.organization_id
                    == ConnectorSyncFileMaterializationChunk.organization_id,
                    ConnectorSyncFileMaterialization.generation_id
                    == ConnectorSyncFileMaterializationChunk.generation_id,
                    ConnectorSyncFileMaterialization.id
                    == ConnectorSyncFileMaterializationChunk.materialization_id,
                ),
            )
            .where(
                ConnectorSyncFileMaterializationChunk.organization_id
                == generation.organization_id,
                ConnectorSyncFileMaterializationChunk.generation_id == generation.id,
                ConnectorSyncFileMaterializationChunk.embedding_model
                != ConnectorSyncFileMaterialization.embedding_model,
            ),
            "generation materialization chunk model validation failed",
        )
        if invalid_chunk_groups or invalid_chunk_models:
            raise SyncWorkLedgerConflict("generation materialization chunks are incomplete")

        if self._promotion_projection_count(generation) != materialization_total:
            raise SyncWorkLedgerConflict("generation citation projection is incomplete")

    def _promotion_projection_count(self, generation: ConnectorSyncGeneration) -> int:
        # Manifest v1 promotions may already cite the sole immutable version
        # created for an unchanged Git blob at an earlier commit.  Manifest v2
        # projection creates a commit-exact version instead.  In both cases,
        # more than one eligible candidate is deliberately treated as a
        # conflict rather than resolved by ordering or a mutable current flag.
        exact_version = aliased(DocumentVersion)
        compatible_version = aliased(DocumentVersion)
        exact_version_count = (
            select(func.count(exact_version.id))
            .where(
                exact_version.organization_id == SourceItem.organization_id,
                exact_version.connector_id == SourceItem.connector_id,
                exact_version.source_item_id == SourceItem.id,
                exact_version.provider_version_id
                == ConnectorSyncFileMaterialization.provider_blob_id,
                exact_version.content_checksum
                == ConnectorSyncFileMaterialization.content_checksum,
                exact_version.lifecycle == "available",
                exact_version.version_metadata["provider"].as_string() == "github",
                exact_version.version_metadata["commit_object_id"].as_string()
                == generation.commit_object_id,
                exact_version.version_metadata["blob_object_id"].as_string()
                == ConnectorSyncFileMaterialization.provider_blob_id,
            )
            .correlate(SourceItem, ConnectorSyncFileMaterialization)
            .scalar_subquery()
        )
        compatible_version_count = (
            select(func.count(compatible_version.id))
            .where(
                compatible_version.organization_id == SourceItem.organization_id,
                compatible_version.connector_id == SourceItem.connector_id,
                compatible_version.source_item_id == SourceItem.id,
                compatible_version.provider_version_id
                == ConnectorSyncFileMaterialization.provider_blob_id,
                compatible_version.content_checksum
                == ConnectorSyncFileMaterialization.content_checksum,
                compatible_version.lifecycle == "available",
                compatible_version.version_metadata["provider"].as_string()
                == "github",
                compatible_version.version_metadata["blob_object_id"].as_string()
                == ConnectorSyncFileMaterialization.provider_blob_id,
            )
            .correlate(SourceItem, ConnectorSyncFileMaterialization)
            .scalar_subquery()
        )
        exact_version_match = and_(
            DocumentVersion.version_metadata["commit_object_id"].as_string()
            == generation.commit_object_id,
            exact_version_count == 1,
        )
        if generation.manifest_schema_version == 1:
            citation_version_match = or_(
                exact_version_match,
                and_(
                    exact_version_count == 0,
                    compatible_version_count == 1,
                ),
            )
        else:
            citation_version_match = exact_version_match

        return self._one(
            select(func.count(ConnectorSyncFileMaterialization.id))
            .select_from(ConnectorSyncFileMaterialization)
            .join(
                SourceItem,
                and_(
                    SourceItem.organization_id
                    == ConnectorSyncFileMaterialization.organization_id,
                    SourceItem.connector_id
                    == ConnectorSyncFileMaterialization.connector_id,
                    SourceItem.source_item_key
                    == ConnectorSyncFileMaterialization.source_item_key,
                ),
            )
            .join(
                SourceItemScopeMembership,
                and_(
                    SourceItemScopeMembership.organization_id
                    == SourceItem.organization_id,
                    SourceItemScopeMembership.connector_id == SourceItem.connector_id,
                    SourceItemScopeMembership.source_item_id == SourceItem.id,
                    SourceItemScopeMembership.connector_scope_id
                    == ConnectorSyncFileMaterialization.connector_scope_id,
                ),
            )
            .join(
                DocumentVersion,
                and_(
                    DocumentVersion.organization_id == SourceItem.organization_id,
                    DocumentVersion.connector_id == SourceItem.connector_id,
                    DocumentVersion.source_item_id == SourceItem.id,
                ),
            )
            .join(
                Document,
                and_(
                    Document.organization_id == DocumentVersion.organization_id,
                    Document.source_type == "github",
                    Document.source_document_key
                    == ConnectorSyncFileMaterialization.source_item_key,
                ),
            )
            .join(
                DocumentIndexingState,
                and_(
                    DocumentIndexingState.organization_id
                    == DocumentVersion.organization_id,
                    DocumentIndexingState.document_version_id == DocumentVersion.id,
                    DocumentIndexingState.profile_fingerprint
                    == ConnectorSyncFileMaterialization.profile_fingerprint,
                ),
            )
            .where(
                ConnectorSyncFileMaterialization.organization_id
                == generation.organization_id,
                ConnectorSyncFileMaterialization.generation_id == generation.id,
                SourceItem.status == "active",
                SourceItem.deleted_at.is_(None),
                SourceItemScopeMembership.status == "active",
                SourceItemScopeMembership.removed_at.is_(None),
                DocumentVersion.lifecycle == "available",
                DocumentVersion.provider_version_id
                == ConnectorSyncFileMaterialization.provider_blob_id,
                DocumentVersion.content_checksum
                == ConnectorSyncFileMaterialization.content_checksum,
                DocumentVersion.version_metadata["provider"].as_string() == "github",
                DocumentVersion.version_metadata["blob_object_id"].as_string()
                == ConnectorSyncFileMaterialization.provider_blob_id,
                citation_version_match,
                Document.status == "ready",
                Document.deleted_at.is_(None),
                DocumentIndexingState.status == "indexed",
                DocumentIndexingState.indexed_generation
                == DocumentIndexingState.desired_generation,
                DocumentIndexingState.embedding_model
                == ConnectorSyncFileMaterialization.embedding_model,
                DocumentIndexingState.embedding_dimensions == 1536,
            ),
            "generation citation projection validation failed",
        )

    def reconcile_generation(
        self,
        request: GenerationReconciliationRequest,
        *,
        now: datetime,
        limit: int = 100,
    ) -> GenerationReconciliationResult:
        """Retire one bounded batch absent from an active authoritative manifest."""
        if not isinstance(request, GenerationReconciliationRequest):
            raise InvalidSyncWorkLedgerRequest(
                "generation reconciliation request is invalid"
            )
        now = _aware("now", now)
        if isinstance(limit, bool) or not isinstance(limit, int) or not (
            1 <= limit <= MAX_RECONCILIATION_BATCH_SIZE
        ):
            raise InvalidSyncWorkLedgerRequest(
                f"reconciliation limit must be between 1 and {MAX_RECONCILIATION_BATCH_SIZE}"
            )

        scope = self._one(
            select(ConnectorScope)
            .where(
                ConnectorScope.organization_id == request.organization_id,
                ConnectorScope.connector_id == request.connector_id,
                ConnectorScope.id == request.connector_scope_id,
            )
            .with_for_update(),
            "generation reconciliation scope lock failed",
        )
        generation = self._locked_generation(
            request.organization_id, request.generation_id
        )
        if scope is None or generation is None:
            raise SyncWorkLedgerNotFound(
                "generation reconciliation context was not found"
            )
        if (
            scope.status != "active"
            or scope.scope_type != "repository"
            or scope.external_scope_key != request.repository_identity
            or not _generation_reconciliation_matches(generation, request)
            or generation.provider_key != "github"
            or generation.manifest_schema_version != 2
            or generation.status != RepositoryGenerationStatus.COMPLETED.value
            or not generation.discovery_complete
            or not generation.reconciliation_eligible
            or generation.reconciliation_eligible_at is None
        ):
            raise SyncWorkLedgerConflict(
                "generation reconciliation attribution changed"
            )
        if now < generation.reconciliation_eligible_at:
            raise SyncWorkLedgerConflict(
                "generation reconciliation time moved backward"
            )

        job = self._one(
            select(ConnectorSyncJob)
            .where(
                ConnectorSyncJob.organization_id == request.organization_id,
                ConnectorSyncJob.connector_id == request.connector_id,
                ConnectorSyncJob.connector_scope_id == request.connector_scope_id,
                ConnectorSyncJob.id == request.sync_job_id,
            )
            .with_for_update(),
            "generation reconciliation job lock failed",
        )
        if job is None or job.status != "succeeded" or job.cancel_requested_at is not None:
            raise SyncWorkLedgerConflict(
                "generation reconciliation job is not successful"
            )
        other_active_job = self._one(
            select(ConnectorSyncJob.id)
            .where(
                ConnectorSyncJob.organization_id == request.organization_id,
                ConnectorSyncJob.connector_id == request.connector_id,
                ConnectorSyncJob.connector_scope_id == request.connector_scope_id,
                ConnectorSyncJob.id != request.sync_job_id,
                ConnectorSyncJob.status.in_(("queued", "running", "retry_wait")),
            )
            .limit(1)
            .with_for_update(),
            "concurrent synchronization lookup failed",
        )
        if other_active_job is not None:
            raise SyncWorkLedgerConflict(
                "generation reconciliation conflicts with active synchronization"
            )
        newer_job = self._one(
            select(ConnectorSyncJob.id)
            .where(
                ConnectorSyncJob.organization_id == request.organization_id,
                ConnectorSyncJob.connector_id == request.connector_id,
                ConnectorSyncJob.connector_scope_id == request.connector_scope_id,
                ConnectorSyncJob.id != request.sync_job_id,
                or_(
                    ConnectorSyncJob.created_at > job.created_at,
                    and_(
                        ConnectorSyncJob.created_at == job.created_at,
                        ConnectorSyncJob.id > job.id,
                    ),
                ),
            )
            .limit(1),
            "newer synchronization lookup failed",
        )
        if newer_job is not None:
            raise SyncWorkLedgerConflict(
                "stale generation: newer synchronization prevents reconciliation"
            )

        activations = self._all(
            select(ConnectorSyncGenerationActivation)
            .where(
                ConnectorSyncGenerationActivation.organization_id
                == request.organization_id,
                ConnectorSyncGenerationActivation.connector_id == request.connector_id,
                ConnectorSyncGenerationActivation.connector_scope_id
                == request.connector_scope_id,
            )
            .order_by(
                ConnectorSyncGenerationActivation.activated_at,
                ConnectorSyncGenerationActivation.id,
            )
            .with_for_update(),
            "generation reconciliation activation lock failed",
        )
        active = [
            row
            for row in activations
            if row.status == GenerationActivationStatus.ACTIVE.value
        ]
        if len(active) != 1 or not _reconciliation_activation_matches(
            active[0], request
        ):
            raise SyncWorkLedgerConflict(
                "generation is not the active reconciliation authority"
            )
        newer = self._one(
            select(ConnectorSyncGeneration.id)
            .where(
                ConnectorSyncGeneration.organization_id == request.organization_id,
                ConnectorSyncGeneration.connector_id == request.connector_id,
                ConnectorSyncGeneration.connector_scope_id
                == request.connector_scope_id,
                ConnectorSyncGeneration.id != request.generation_id,
                or_(
                    ConnectorSyncGeneration.created_at > generation.created_at,
                    and_(
                        ConnectorSyncGeneration.created_at == generation.created_at,
                        ConnectorSyncGeneration.id > generation.id,
                    ),
                ),
            )
            .limit(1),
            "newer reconciliation generation lookup failed",
        )
        if newer is not None:
            raise SyncWorkLedgerConflict(
                "stale generation cannot reconcile lifecycle state"
            )

        if generation.reconciliation_completed_at is not None:
            return GenerationReconciliationResult(
                generation.id,
                True,
                True,
                0,
                0,
                0,
                generation.reconciled_membership_count,
                generation.reconciled_source_count,
                generation.reconciled_document_count,
            )
        if generation.reconciliation_started_at is None:
            # Promotion made this completed generation immutable to every
            # application write path.  Persist the expensive aggregate proof
            # with the first retirement batch; rollback removes the marker and
            # forces the next attempt to validate again.  Later batches still
            # recheck scope, job, activation, and newer-work authority above.
            self._validate_reconciliation_projection(generation)
            generation.reconciliation_started_at = now

        observation_exists = exists(
            select(ConnectorSyncGenerationObservation.id).where(
                ConnectorSyncGenerationObservation.organization_id
                == request.organization_id,
                ConnectorSyncGenerationObservation.generation_id
                == request.generation_id,
                ConnectorSyncGenerationObservation.source_item_key
                == SourceItem.source_item_key,
            )
        )
        candidate_ids = self._all(
            select(SourceItem.id)
            .join(
                SourceItemScopeMembership,
                and_(
                    SourceItemScopeMembership.organization_id
                    == SourceItem.organization_id,
                    SourceItemScopeMembership.connector_id == SourceItem.connector_id,
                    SourceItemScopeMembership.source_item_id == SourceItem.id,
                ),
            )
            .where(
                SourceItem.organization_id == request.organization_id,
                SourceItem.connector_id == request.connector_id,
                SourceItemScopeMembership.connector_scope_id
                == request.connector_scope_id,
                SourceItemScopeMembership.status == "active",
                SourceItemScopeMembership.removed_at.is_(None),
                SourceItem.source_item_type == "file",
                SourceItem.source_item_key.startswith(
                    f"{request.repository_identity}:path:"
                ),
                SourceItem.source_metadata["provider"].as_string() == "github",
                SourceItem.source_metadata["repository_identity"].as_string()
                == request.repository_identity,
                ~observation_exists,
            )
            .order_by(SourceItem.id)
            .limit(limit),
            "generation reconciliation candidate lookup failed",
        )

        memberships_retired = 0
        sources_retired = 0
        documents_retired = 0
        for source_item_id in candidate_ids:
            retired_source, retired_document = self._retire_absent_source(
                request, source_item_id, now
            )
            memberships_retired += 1
            sources_retired += int(retired_source)
            documents_retired += int(retired_document)

        generation.reconciled_membership_count += memberships_retired
        generation.reconciled_source_count += sources_retired
        generation.reconciled_document_count += documents_retired
        generation.updated_at = now
        self._flush("generation reconciliation batch failed")

        remaining = self._one(
            select(SourceItem.id)
            .join(
                SourceItemScopeMembership,
                and_(
                    SourceItemScopeMembership.organization_id
                    == SourceItem.organization_id,
                    SourceItemScopeMembership.connector_id == SourceItem.connector_id,
                    SourceItemScopeMembership.source_item_id == SourceItem.id,
                ),
            )
            .where(
                SourceItem.organization_id == request.organization_id,
                SourceItem.connector_id == request.connector_id,
                SourceItemScopeMembership.connector_scope_id
                == request.connector_scope_id,
                SourceItemScopeMembership.status == "active",
                SourceItemScopeMembership.removed_at.is_(None),
                SourceItem.source_item_type == "file",
                SourceItem.source_item_key.startswith(
                    f"{request.repository_identity}:path:"
                ),
                SourceItem.source_metadata["provider"].as_string() == "github",
                SourceItem.source_metadata["repository_identity"].as_string()
                == request.repository_identity,
                ~observation_exists,
            )
            .limit(1),
            "generation reconciliation completion lookup failed",
        )
        completed = remaining is None
        if completed:
            generation.reconciliation_completed_at = now
            self._flush("generation reconciliation completion failed")
        return GenerationReconciliationResult(
            generation.id,
            completed,
            False,
            memberships_retired,
            sources_retired,
            documents_retired,
            generation.reconciled_membership_count,
            generation.reconciled_source_count,
            generation.reconciled_document_count,
        )

    def _retire_absent_source(
        self,
        request: GenerationReconciliationRequest,
        source_item_id: UUID,
        now: datetime,
    ) -> tuple[bool, bool]:
        source = self._one(
            select(SourceItem)
            .where(
                SourceItem.organization_id == request.organization_id,
                SourceItem.connector_id == request.connector_id,
                SourceItem.id == source_item_id,
            )
            .with_for_update(),
            "reconciliation source lock failed",
        )
        membership = self._one(
            select(SourceItemScopeMembership)
            .where(
                SourceItemScopeMembership.organization_id == request.organization_id,
                SourceItemScopeMembership.connector_id == request.connector_id,
                SourceItemScopeMembership.connector_scope_id
                == request.connector_scope_id,
                SourceItemScopeMembership.source_item_id == source_item_id,
            )
            .with_for_update(),
            "reconciliation membership lock failed",
        )
        if source is None or membership is None:
            raise SyncWorkLedgerConflict("reconciliation source context disappeared")
        metadata = source.source_metadata
        if (
            membership.status != "active"
            or membership.removed_at is not None
            or source.source_item_type != "file"
            or source.status not in {"active", "unavailable"}
            or source.deleted_at is not None
            or not source.source_item_key.startswith(
                f"{request.repository_identity}:path:"
            )
            or metadata.get("provider") != "github"
            or metadata.get("repository_identity") != request.repository_identity
        ):
            raise SyncWorkLedgerConflict("reconciliation source attribution changed")
        observed = self._one(
            select(ConnectorSyncGenerationObservation.id)
            .where(
                ConnectorSyncGenerationObservation.organization_id
                == request.organization_id,
                ConnectorSyncGenerationObservation.generation_id
                == request.generation_id,
                ConnectorSyncGenerationObservation.source_item_key
                == source.source_item_key,
            )
            .limit(1),
            "reconciliation observation revalidation failed",
        )
        if observed is not None:
            raise SyncWorkLedgerConflict("observed source cannot be retired")

        current = self._one(
            select(DocumentVersion)
            .where(
                DocumentVersion.organization_id == request.organization_id,
                DocumentVersion.source_item_id == source.id,
                DocumentVersion.is_current.is_(True),
            )
            .with_for_update(),
            "reconciliation current version lock failed",
        )
        materialization = None
        document = None
        if current is not None:
            materialization = self._one(
                select(DocumentVersionDocument)
                .where(
                    DocumentVersionDocument.organization_id
                    == request.organization_id,
                    DocumentVersionDocument.document_version_id == current.id,
                )
                .with_for_update(),
                "reconciliation document link lock failed",
            )
            if materialization is not None:
                document = self._one(
                    select(Document)
                    .where(
                        Document.organization_id == request.organization_id,
                        Document.id == materialization.document_id,
                    )
                    .with_for_update(),
                    "reconciliation document lock failed",
                )

        membership.status = "removed"
        membership.removed_at = now
        membership.last_seen_at = now
        membership.updated_at = now
        self._flush("source membership retirement failed")
        other_membership = self._one(
            select(SourceItemScopeMembership.id)
            .where(
                SourceItemScopeMembership.organization_id == request.organization_id,
                SourceItemScopeMembership.connector_id == request.connector_id,
                SourceItemScopeMembership.source_item_id == source.id,
                SourceItemScopeMembership.status == "active",
                SourceItemScopeMembership.removed_at.is_(None),
            )
            .limit(1),
            "shared source membership lookup failed",
        )
        if other_membership is not None:
            return False, False

        source.status = "deleted"
        source.deleted_at = now
        source.updated_at = now
        if current is None or current.lifecycle != "deleted":
            next_number = self._one(
                select(func.coalesce(func.max(DocumentVersion.version_number), 0) + 1)
                .where(
                    DocumentVersion.organization_id == request.organization_id,
                    DocumentVersion.source_item_id == source.id,
                ),
                "reconciliation document version allocation failed",
            )
            if current is not None:
                current.is_current = False
            self._session.add(
                DocumentVersion(
                    id=self._new_uuid("document_version_id", uuid4),
                    organization_id=request.organization_id,
                    connector_id=request.connector_id,
                    source_item_id=source.id,
                    version_number=int(next_number),
                    provider_version_id=(
                        current.provider_version_id if current is not None else None
                    ),
                    content_checksum=None,
                    checksum_algorithm=None,
                    source_modified_at=None,
                    source_size_bytes=None,
                    content_type=None,
                    file_extension=None,
                    version_cause="tombstone",
                    lifecycle="deleted",
                    is_current=True,
                    discovered_at=now,
                    version_metadata={
                        "provider": "github",
                        "reason": "provider_deleted",
                        "generation_id": str(request.generation_id),
                    },
                    metadata_schema_version=1,
                )
            )
        document_retired = document is not None and document.deleted_at is None
        if document_retired:
            document.deleted_at = now
            document.updated_at = now
        self._flush("source lifecycle retirement failed")
        return True, document_retired

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
        self._release_work_reservation_for_lease(lease, now=now)
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
        if lease.reservation_id is not None:
            self._owned_work_reservation(lease)
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

    def _owned_work_reservation(
        self, lease: FileWorkLease
    ) -> ConnectorSyncControlReservation:
        reservation = self._one(
            select(ConnectorSyncControlReservation)
            .where(
                ConnectorSyncControlReservation.organization_id
                == lease.organization_id,
                ConnectorSyncControlReservation.connector_id == lease.connector_id,
                ConnectorSyncControlReservation.connector_scope_id
                == lease.connector_scope_id,
                ConnectorSyncControlReservation.generation_id
                == lease.generation_id,
                ConnectorSyncControlReservation.work_item_id == lease.work_item_id,
                ConnectorSyncControlReservation.id == lease.reservation_id,
                ConnectorSyncControlReservation.state == "work_item",
                ConnectorSyncControlReservation.processor_lease_id == lease.lease_id,
                ConnectorSyncControlReservation.released_at.is_(None),
                ConnectorSyncControlReservation.expires_at
                > func.clock_timestamp(),
            )
            .with_for_update(),
            "controlled file-work reservation validation failed",
        )
        if reservation is None:
            raise LostFileWorkLease(
                "controlled file-work reservation is no longer owned"
            )
        return reservation

    def _release_work_reservation_for_lease(
        self, lease: FileWorkLease, *, now: datetime
    ) -> None:
        if lease.reservation_id is None:
            return
        self._release_work_reservation(self._owned_work_reservation(lease), now=now)

    def _retain_work_reservation_for_retry(self, lease: FileWorkLease) -> None:
        if lease.reservation_id is None:
            return
        reservation = self._owned_work_reservation(lease)
        reservation.processor_lease_id = None

    def _lock_work_reservation(
        self,
        organization_id: UUID,
        generation_id: UUID,
        work_item_id: UUID,
    ) -> ConnectorSyncControlReservation | None:
        return self._one(
            select(ConnectorSyncControlReservation)
            .where(
                ConnectorSyncControlReservation.organization_id
                == organization_id,
                ConnectorSyncControlReservation.generation_id == generation_id,
                ConnectorSyncControlReservation.work_item_id == work_item_id,
                ConnectorSyncControlReservation.state == "work_item",
                ConnectorSyncControlReservation.released_at.is_(None),
            )
            .with_for_update(),
            "controlled file-work reservation lock failed",
        )

    @staticmethod
    def _release_work_reservation(
        reservation: ConnectorSyncControlReservation, *, now: datetime
    ) -> None:
        reservation.state = "released"
        reservation.planner_lease_id = None
        reservation.processor_lease_id = None
        reservation.released_at = func.clock_timestamp()

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

    def _row(self, statement, message: str):
        try:
            return self._session.execute(statement).one()
        except SQLAlchemyError as exc:
            raise SyncWorkLedgerPersistenceError(message) from exc

    def _scalar(self, statement, message: str):
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


def _eligible_file_work_candidate(
    *,
    organization_id,
    provider_key: str,
    profile_fingerprint: str,
    now: datetime,
):
    return (
        select(ConnectorSyncFileWorkItem.id)
        .select_from(ConnectorSyncFileWorkItem)
        .join(
            ConnectorSyncGeneration,
            (
                ConnectorSyncGeneration.organization_id
                == ConnectorSyncFileWorkItem.organization_id
            )
            & (
                ConnectorSyncGeneration.connector_id
                == ConnectorSyncFileWorkItem.connector_id
            )
            & (
                ConnectorSyncGeneration.connector_scope_id
                == ConnectorSyncFileWorkItem.connector_scope_id
            )
            & (
                ConnectorSyncGeneration.id
                == ConnectorSyncFileWorkItem.generation_id
            )
            & (
                ConnectorSyncGeneration.profile_fingerprint
                == ConnectorSyncFileWorkItem.profile_fingerprint
            ),
        )
        .join(
            ConnectorSyncJob,
            (ConnectorSyncJob.organization_id == ConnectorSyncGeneration.organization_id)
            & (ConnectorSyncJob.connector_id == ConnectorSyncGeneration.connector_id)
            & (
                ConnectorSyncJob.connector_scope_id
                == ConnectorSyncGeneration.connector_scope_id
            )
            & (ConnectorSyncJob.id == ConnectorSyncGeneration.sync_job_id),
        )
        .where(
            ConnectorSyncFileWorkItem.organization_id == organization_id,
            ConnectorSyncGeneration.provider_key == provider_key,
            ConnectorSyncGeneration.profile_fingerprint == profile_fingerprint,
            ConnectorSyncFileWorkItem.profile_fingerprint == profile_fingerprint,
            ConnectorSyncGeneration.status
            == RepositoryGenerationStatus.PROCESSING.value,
            ConnectorSyncGeneration.discovery_complete.is_(True),
            ConnectorSyncJob.status != "cancelled",
            ConnectorSyncJob.cancel_requested_at.is_(None),
            ConnectorSyncFileWorkItem.status.in_(
                (FileWorkStatus.PENDING.value, FileWorkStatus.RETRY_WAIT.value)
            ),
            ConnectorSyncFileWorkItem.next_attempt_at <= now,
            ConnectorSyncFileWorkItem.cancel_requested_at.is_(None),
            ConnectorSyncFileWorkItem.attempt_count
            < ConnectorSyncFileWorkItem.max_attempts,
            _generic_work_reservation_available(),
        )
        .limit(1)
        .scalar_subquery()
    )


def _generic_work_reservation_available():
    reservation_generation = aliased(ConnectorSyncGeneration)
    live_work = exists(
        select(ConnectorSyncControlReservation.id).where(
            ConnectorSyncControlReservation.organization_id
            == ConnectorSyncFileWorkItem.organization_id,
            ConnectorSyncControlReservation.generation_id
            == ConnectorSyncFileWorkItem.generation_id,
            ConnectorSyncControlReservation.work_item_id
            == ConnectorSyncFileWorkItem.id,
            ConnectorSyncControlReservation.state == "work_item",
            ConnectorSyncControlReservation.released_at.is_(None),
            ConnectorSyncControlReservation.expires_at > func.clock_timestamp(),
        )
    )
    live_job = exists(
        select(ConnectorSyncControlReservation.id)
        .select_from(ConnectorSyncControlReservation)
        .join(
            reservation_generation,
            and_(
                reservation_generation.organization_id
                == ConnectorSyncControlReservation.organization_id,
                reservation_generation.connector_id
                == ConnectorSyncControlReservation.connector_id,
                reservation_generation.connector_scope_id
                == ConnectorSyncControlReservation.connector_scope_id,
                reservation_generation.sync_job_id
                == ConnectorSyncControlReservation.sync_job_id,
            ),
        )
        .where(
            reservation_generation.organization_id
            == ConnectorSyncFileWorkItem.organization_id,
            reservation_generation.id
            == ConnectorSyncFileWorkItem.generation_id,
            ConnectorSyncControlReservation.state == "job",
            ConnectorSyncControlReservation.released_at.is_(None),
            ConnectorSyncControlReservation.expires_at > func.clock_timestamp(),
        )
    )
    return ~(live_work | live_job)


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
            "manifest_schema_version",
        )
    )


def _generation_promotion_matches(
    row: ConnectorSyncGeneration, request: GenerationPromotionRequest
) -> bool:
    return row.id == request.generation_id and all(
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


def _generation_reconciliation_matches(
    row: ConnectorSyncGeneration, request: GenerationReconciliationRequest
) -> bool:
    return row.id == request.generation_id and all(
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


def _persisted_promotion_materialization_matches(
    materialization: ConnectorSyncFileMaterialization,
    generation: ConnectorSyncGeneration,
    work: ConnectorSyncFileWorkItem,
) -> bool:
    return (
        materialization.organization_id == generation.organization_id
        and materialization.connector_id == generation.connector_id
        and materialization.connector_scope_id == generation.connector_scope_id
        and materialization.generation_id == generation.id
        and materialization.work_item_id == work.id
        and materialization.repository_identity == generation.repository_identity
        and materialization.branch_name == generation.branch_name
        and materialization.root_tree_object_id == generation.root_tree_object_id
        and materialization.source_item_key == work.source_item_key
        and materialization.source_key_hash == work.source_key_hash
        and materialization.repository_path == work.repository_path
        and materialization.provider_blob_id == work.provider_blob_id
        and materialization.provider_revision_id == generation.commit_object_id
        and materialization.provider_revision_id == work.provider_revision_id
        and materialization.profile_fingerprint == generation.profile_fingerprint
        and materialization.profile_fingerprint == work.profile_fingerprint
        and materialization.chunk_count > 0
    )


def _activation_matches(
    row: ConnectorSyncGenerationActivation, request: GenerationPromotionRequest
) -> bool:
    return (
        row.organization_id == request.organization_id
        and row.connector_id == request.connector_id
        and row.connector_scope_id == request.connector_scope_id
        and row.generation_id == request.generation_id
        and row.repository_identity == request.repository_identity
        and row.commit_object_id == request.commit_object_id
        and row.profile_fingerprint == request.profile_fingerprint
        and row.status == GenerationActivationStatus.ACTIVE.value
        and row.retired_at is None
    )


def _reconciliation_activation_matches(
    row: ConnectorSyncGenerationActivation,
    request: GenerationReconciliationRequest,
) -> bool:
    return (
        row.organization_id == request.organization_id
        and row.connector_id == request.connector_id
        and row.connector_scope_id == request.connector_scope_id
        and row.generation_id == request.generation_id
        and row.repository_identity == request.repository_identity
        and row.commit_object_id == request.commit_object_id
        and row.profile_fingerprint == request.profile_fingerprint
        and row.status == GenerationActivationStatus.ACTIVE.value
        and row.retired_at is None
    )


def _activation_view(
    row: ConnectorSyncGenerationActivation,
) -> GenerationActivationView:
    return GenerationActivationView(
        row.id,
        row.organization_id,
        row.connector_id,
        row.connector_scope_id,
        row.generation_id,
        row.repository_identity,
        row.commit_object_id,
        row.profile_fingerprint,
        GenerationActivationStatus(row.status),
        row.activated_at,
        row.retired_at,
        row.created_at,
        row.updated_at,
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


def _github_repository_id(repository_identity: str) -> int:
    prefix = "github:repository:"
    if not repository_identity.startswith(prefix):
        raise SyncWorkLedgerConflict("generation repository identity is invalid")
    value = repository_identity[len(prefix) :]
    if not value.isdigit() or int(value) < 1:
        raise SyncWorkLedgerConflict("generation repository identity is invalid")
    return int(value)


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


def _observation_row_matches(
    row: ConnectorSyncGenerationObservation,
    observation: GenerationSourceObservation,
) -> bool:
    return (
        row.source_item_key == observation.source_item_key
        and row.repository_path == observation.repository_path
        and row.provider_object_id == observation.provider_object_id
        and row.provider_revision_id == observation.provider_revision_id
        and row.profile_fingerprint == observation.profile_fingerprint
        and row.entry_type == observation.entry_type
        and row.disposition == observation.disposition.value
        and row.file_size_bytes == observation.file_size_bytes
    )


def _work_matches_observation(
    work: ConnectorSyncFileWorkItem,
    observation: ConnectorSyncGenerationObservation,
) -> bool:
    return (
        work.organization_id == observation.organization_id
        and work.connector_id == observation.connector_id
        and work.connector_scope_id == observation.connector_scope_id
        and work.generation_id == observation.generation_id
        and work.source_item_key == observation.source_item_key
        and work.source_key_hash == observation.source_key_hash
        and work.repository_path == observation.repository_path
        and work.provider_blob_id == observation.provider_object_id
        and work.provider_revision_id == observation.provider_revision_id
        and work.profile_fingerprint == observation.profile_fingerprint
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


def _lease(row, *, reservation_id: UUID | None = None) -> FileWorkLease:
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
        reservation_id=reservation_id,
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
        row.manifest_schema_version,
        row.reconciliation_started_at,
        row.reconciliation_completed_at,
        row.reconciled_membership_count,
        row.reconciled_source_count,
        row.reconciled_document_count,
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
