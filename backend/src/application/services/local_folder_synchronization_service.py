"""Secure bounded orchestration for incremental Local Folder synchronization."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path
from typing import Callable
from uuid import UUID, uuid4

from application.services.document_chunk_embedding_service import DocumentChunkEmbeddingPersistenceError
from application.services.local_document_indexing_service import (
    InvalidDocumentIndexingRequestError,
    LocalDocumentIndexingService,
    NonProgressingDocumentChunkPageError,
)
from application.services.local_document_ingestion_service import (
    InvalidLocalDocumentRequestError,
    LocalDocumentIngestionPersistenceError,
)
from domain.connectors.exceptions import ConnectorError
from domain.connectors.models import SourceItem as DiscoveredSourceItem, SyncCheckpoint
from domain.content_extraction.exceptions import ContentExtractionError
from domain.embeddings.exceptions import EmbeddingError
from infrastructure.connectors.local.connector import LocalFolderConnector
from infrastructure.db.models import (
    ConnectorSyncItem,
    ConnectorSyncRun,
    DocumentIndexingAttempt,
    DocumentIndexingState,
    SourceItem,
    SourceItemScopeMembership,
)
from infrastructure.repositories.connector_repository import (
    ConnectorRepository,
    ConnectorRepositoryConflict,
    ConnectorRepositoryPersistenceError,
    InvalidConnectorRepositoryRequest,
    _require_choice,
    _require_limit,
    _require_uuid,
)
from infrastructure.repositories.connector_scope_repository import ConnectorScopeRepository
from infrastructure.repositories.connector_sync_repository import ConnectorSyncRepository, SafeSyncError
from infrastructure.repositories.document_indexing_repository import DocumentIndexingRepository
from infrastructure.repositories.document_version_repository import DocumentVersionRepository
from infrastructure.repositories.source_item_repository import (
    MembershipReconciliationCursor,
    SourceItemRepository,
)

SYNC_MODES = frozenset({"initial", "incremental", "retry", "reconciliation"})
SYNC_TRIGGERS = frozenset({"manual", "scheduled", "retry", "system"})
INDEXING_FAILURES = (
    ContentExtractionError,
    EmbeddingError,
    InvalidDocumentIndexingRequestError,
    NonProgressingDocumentChunkPageError,
    InvalidLocalDocumentRequestError,
    LocalDocumentIngestionPersistenceError,
    DocumentChunkEmbeddingPersistenceError,
    ConnectorRepositoryConflict,
    ConnectorRepositoryPersistenceError,
    InvalidConnectorRepositoryRequest,
)


class InvalidLocalFolderSynchronizationRequest(ValueError):
    """Raised when tenant-scoped synchronization input is malformed."""


class LocalFolderSynchronizationUnavailable(RuntimeError):
    """Raised when the connector or scope cannot be synchronized."""


class UnsafeLocalFolderConfiguration(RuntimeError):
    """Raised when the persisted Local Folder boundary is unsafe."""


class LocalFolderDiscoveryError(RuntimeError):
    """Raised when complete Local Folder discovery cannot be established."""


class LocalFolderSourceReconciliationError(RuntimeError):
    """Raised when canonical source reconciliation cannot be persisted."""


class LocalFolderIndexingError(RuntimeError):
    """Raised when an immutable version cannot be indexed safely."""


class LocalFolderSynchronizationPersistenceError(RuntimeError):
    """Raised when run progress cannot be persisted safely."""


class NonProgressingLocalFolderSynchronization(RuntimeError):
    """Raised when persisted synchronization continuation cannot advance safely."""


@dataclass(frozen=True)
class LocalFolderSynchronizationRequest:
    organization_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    initiated_by_user_id: UUID | None = None
    sync_run_id: UUID | None = None
    mode: str = "incremental"
    trigger_type: str = "manual"
    batch_size: int = 100


@dataclass(frozen=True)
class LocalFolderSynchronizationResult:
    sync_run_id: UUID
    outcome: str
    files_discovered: int
    supported_files_considered: int
    unchanged_files: int
    new_source_items: int
    changed_source_items: int
    restored_source_items: int
    versions_created: int
    versions_indexed: int
    failed_items: int
    missing_memberships_reconciled: int
    batches_processed: int
    discovery_completed: bool
    reconciliation_completed: bool


@dataclass
class _BatchCounts:
    discovered: int = 0
    considered: int = 0
    unchanged: int = 0
    new: int = 0
    changed: int = 0
    restored: int = 0
    versions_created: int = 0
    versions_indexed: int = 0
    failed: int = 0
    reconciled: int = 0


class LocalFolderSynchronizationService:
    """Coordinate one caller-committed Local Folder synchronization batch."""

    def __init__(
        self,
        connector_repository: ConnectorRepository,
        scope_repository: ConnectorScopeRepository,
        source_repository: SourceItemRepository,
        sync_repository: ConnectorSyncRepository,
        version_repository: DocumentVersionRepository,
        indexing_repository: DocumentIndexingRepository,
        indexing_service: LocalDocumentIndexingService,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._connectors = connector_repository
        self._scopes = scope_repository
        self._sources = source_repository
        self._sync = sync_repository
        self._versions = version_repository
        self._indexing = indexing_repository
        self._indexing_service = indexing_service
        self._clock = clock

    def synchronize(self, request: LocalFolderSynchronizationRequest) -> LocalFolderSynchronizationResult:
        _validate_request(request)
        now = self._now()
        connector = self._connectors.lock_by_id(request.organization_id, request.connector_id)
        scope = self._scopes.lock_by_id(request.organization_id, request.connector_scope_id)
        root = _validate_configuration(connector, scope, request)
        provider = LocalFolderConnector(request.organization_id, request.connector_id, root)
        _validate_capabilities(connector.capabilities, provider)
        run = self._get_or_start_run(request, now)
        phase, discovery_cursor, reconciliation_cursor = self._load_progress(request, run)
        counts = _BatchCounts()

        if phase == "discovery":
            discovery_complete, next_key = self._process_discovery_batch(
                request, run, provider, discovery_cursor, now, counts
            )
            if not discovery_complete:
                self._persist_progress(request, run, "discovery", next_key, None, now)
                return _result(run.id, "running", counts, False, False)
            phase = "reconciliation"
            reconciliation_cursor = None

        reconciliation_complete, next_membership = self._reconcile_membership_batch(
            request, run, reconciliation_cursor, now, counts
        )
        if not reconciliation_complete:
            self._persist_progress(request, run, "reconciliation", None, next_membership, now)
            return _result(run.id, "running", counts, True, False)

        self._persist_progress(request, run, "completed", None, None, now)
        completed = self._sync.set_run_state(
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            run.id,
            status="completed",
            started_at=run.started_at,
            heartbeat_at=now,
            finished_at=now,
        )
        if completed is None:
            raise LocalFolderSynchronizationPersistenceError("synchronization run could not be completed")
        return _result(run.id, "completed", counts, True, True)

    def _get_or_start_run(self, request: LocalFolderSynchronizationRequest, now: datetime) -> ConnectorSyncRun:
        if request.sync_run_id is not None:
            run = self._sync.lock_run(
                request.organization_id,
                request.connector_id,
                request.connector_scope_id,
                request.sync_run_id,
            )
            if run is None or run.status != "running" or run.started_at is None:
                raise LocalFolderSynchronizationUnavailable("synchronization run is unavailable")
            return run
        run = ConnectorSyncRun(
            id=uuid4(),
            organization_id=request.organization_id,
            connector_id=request.connector_id,
            connector_scope_id=request.connector_scope_id,
            mode=request.mode,
            trigger_type=request.trigger_type,
            status="running",
            initiated_by_user_id=request.initiated_by_user_id,
            started_at=now,
            heartbeat_at=now,
            run_metadata={"orchestrator": "local_folder", "schema_version": 1},
        )
        try:
            return self._sync.add_run(
                request.organization_id, request.connector_id, request.connector_scope_id, run
            )
        except (ConnectorRepositoryConflict, ConnectorRepositoryPersistenceError) as exc:
            raise LocalFolderSynchronizationPersistenceError(
                "synchronization run could not be started"
            ) from exc

    def _load_progress(
        self, request: LocalFolderSynchronizationRequest, run: ConnectorSyncRun
    ) -> tuple[str, str | None, MembershipReconciliationCursor | None]:
        cursor = self._sync.get_active_cursor(
            request.organization_id, request.connector_id, request.connector_scope_id
        )
        if cursor is None or cursor.created_by_run_id != run.id:
            return "discovery", None, None
        value = cursor.safe_cursor
        if not isinstance(value, dict):
            raise NonProgressingLocalFolderSynchronization("synchronization progress is invalid")
        phase = value.get("phase")
        if phase == "discovery":
            after_key = value.get("after_key")
            if not isinstance(after_key, str) or not after_key:
                raise NonProgressingLocalFolderSynchronization("synchronization progress is invalid")
            return phase, after_key, None
        if phase == "reconciliation":
            seen = value.get("last_seen_at")
            membership_id = value.get("membership_id")
            try:
                parsed = MembershipReconciliationCursor(
                    datetime.fromisoformat(str(seen)), UUID(str(membership_id))
                )
            except (TypeError, ValueError) as exc:
                raise NonProgressingLocalFolderSynchronization(
                    "synchronization progress is invalid"
                ) from exc
            return phase, None, parsed
        if phase == "completed":
            raise NonProgressingLocalFolderSynchronization("synchronization run is already complete")
        raise NonProgressingLocalFolderSynchronization("synchronization progress is invalid")

    def _process_discovery_batch(
        self,
        request: LocalFolderSynchronizationRequest,
        run: ConnectorSyncRun,
        provider: LocalFolderConnector,
        after_key: str | None,
        observed_at: datetime,
        counts: _BatchCounts,
    ) -> tuple[bool, str | None]:
        try:
            discovered = list(
                islice(
                    provider.crawl(SyncCheckpoint(cursor=after_key, last_synced_at=run.started_at)),
                    request.batch_size + 1,
                )
            )
        except ConnectorError as exc:
            self._record_run_failure(request, run, "source_read", "discovery_failed", observed_at)
            raise LocalFolderDiscoveryError("Local Folder discovery failed") from exc
        batch = discovered[: request.batch_size]
        counts.discovered = len(batch)
        previous_key = after_key
        for item in batch:
            if previous_key is not None and item.external_id <= previous_key:
                raise NonProgressingLocalFolderSynchronization(
                    "Local Folder discovery did not make progress"
                )
            previous_key = item.external_id
            self._process_item(request, run, provider, item, observed_at, counts)
        has_more = len(discovered) > request.batch_size
        return not has_more, previous_key

    def _process_item(
        self,
        request: LocalFolderSynchronizationRequest,
        run: ConnectorSyncRun,
        provider: LocalFolderConnector,
        discovered: DiscoveredSourceItem,
        observed_at: datetime,
        counts: _BatchCounts,
    ) -> None:
        counts.considered += 1
        existing = self._sources.lock_by_key(
            request.organization_id, request.connector_id, discovered.external_id
        )
        previous_checksum = existing.source_checksum if existing is not None else None
        current_version = (
            self._versions.get_current(request.organization_id, existing.id)
            if existing is not None
            else None
        )
        unchanged = (
            existing is not None
            and previous_checksum == discovered.checksum
            and current_version is not None
            and current_version.content_checksum == discovered.checksum
        )
        restored = existing is not None and existing.status != "active"
        change_type = "new" if existing is None else "unchanged" if unchanged else "changed"
        prior_item = self._sync.get_item_by_key(
            request.organization_id, request.connector_id, run.id, discovered.external_id
        )
        if prior_item is not None and prior_item.processing_status in {"succeeded", "skipped"}:
            if prior_item.current_checksum != discovered.checksum:
                raise NonProgressingLocalFolderSynchronization(
                    "a completed synchronization item changed during the same run"
                )
            return

        source = self._persist_source(request, discovered, existing, observed_at)
        self._persist_membership(request, source.id, observed_at)
        sync_item = prior_item or ConnectorSyncItem(
            id=uuid4(),
            organization_id=request.organization_id,
            connector_id=request.connector_id,
            connector_scope_id=request.connector_scope_id,
            sync_run_id=run.id,
            source_item_id=source.id,
            source_item_key=discovered.external_id,
            change_type=change_type,
            processing_status="pending",
            previous_checksum=previous_checksum,
            current_checksum=discovered.checksum,
            attempt_count=0,
        )
        if prior_item is None:
            self._sync.add_item(
                request.organization_id,
                request.connector_id,
                request.connector_scope_id,
                run.id,
                sync_item,
            )

        if unchanged:
            self._finish_sync_item(request, run.id, sync_item, "skipped", observed_at)
            self._increment_run(request, run.id, items_discovered=1, items_unchanged=1, items_skipped=1)
            counts.unchanged += 1
            if restored:
                counts.restored += 1
            return

        sync_item = self._finish_sync_item(request, run.id, sync_item, "processing", observed_at)
        try:
            version = self._versions.create_current_version(
                request.organization_id,
                source.id,
                version_cause="discovered" if existing is None else "content_changed",
                lifecycle="available",
                discovered_at=observed_at,
                content_checksum=discovered.checksum,
                checksum_algorithm="sha256",
                source_modified_at=discovered.updated_at,
                source_size_bytes=_source_size(discovered),
                content_type=discovered.mime_type,
                file_extension=Path(discovered.external_id).suffix.lower(),
                metadata={"source_item_type": "file"},
            )
            profile = self._indexing_service.profile
            state = self._indexing.get_or_create_state(
                request.organization_id,
                version.id,
                DocumentIndexingState(
                    id=uuid4(),
                    organization_id=request.organization_id,
                    document_version_id=version.id,
                    extraction_profile=profile.extraction_profile,
                    extraction_version=profile.extraction_version,
                    chunking_profile=profile.chunking_profile,
                    chunking_version=profile.chunking_version,
                    embedding_provider=profile.embedding_provider,
                    embedding_model=profile.embedding_model,
                    embedding_dimensions=profile.embedding_dimensions,
                    profile_fingerprint=profile.fingerprint,
                    desired_generation=1,
                    indexed_generation=None,
                    status="pending",
                    reason="new_version" if existing is None else "content_changed",
                    attempt_count=0,
                    requested_at=observed_at,
                ),
            )
            attempt = self._indexing.allocate_attempt(
                request.organization_id,
                version.id,
                profile.fingerprint,
                trigger_type="sync",
                started_at=observed_at,
                sync_run_id=run.id,
                sync_item_id=sync_item.id,
            )
            indexed = self._indexing_service.index(
                request.organization_id,
                "local_folder",
                discovered.external_id,
                provider.resolve_content_path(discovered.external_id),
                source_url=None,
                mime_type=discovered.mime_type,
            )
            if indexed.content_checksum != discovered.checksum:
                raise InvalidDocumentIndexingRequestError("source content changed during indexing")
            completed_at = self._now()
            self._indexing.complete_attempt(
                request.organization_id,
                state.id,
                attempt.id,
                status="succeeded",
                completed_at=completed_at,
                retryable=False,
                summary={"chunks_indexed": indexed.chunks_embedded},
            )
            persisted = self._indexing.persist_controlled_state(
                request.organization_id,
                version.id,
                profile.fingerprint,
                status="indexed",
                desired_generation=state.desired_generation,
                indexed_generation=state.desired_generation,
                attempt_count=state.attempt_count,
                requested_at=state.requested_at,
                started_at=state.started_at,
                completed_at=completed_at,
                last_attempt_at=state.last_attempt_at,
            )
            if persisted is None:
                raise LocalFolderIndexingError("indexing state could not be completed")
            self._versions.replace_materialization(
                request.organization_id, source.id, version.id, indexed.document_id
            )
        except INDEXING_FAILURES + (ConnectorError,) as exc:
            self._fail_indexing(request, run, sync_item, locals().get("state"), locals().get("attempt"))
            raise LocalFolderIndexingError("Local Folder indexing failed") from exc

        self._finish_sync_item(request, run.id, sync_item, "succeeded", observed_at)
        counters = {"items_discovered": 1, "items_succeeded": 1}
        if existing is None:
            counters["items_new"] = 1
            counts.new += 1
        else:
            counters["items_changed"] = 1
            counts.changed += 1
            if restored:
                counts.restored += 1
        self._increment_run(request, run.id, **counters)
        counts.versions_created += 1
        counts.versions_indexed += 1

    def _persist_source(
        self,
        request: LocalFolderSynchronizationRequest,
        discovered: DiscoveredSourceItem,
        existing: SourceItem | None,
        observed_at: datetime,
    ) -> SourceItem:
        metadata = {
            "relative_path": discovered.external_id,
            "extension": Path(discovered.external_id).suffix.lower(),
            "size_bytes": _source_size(discovered),
        }
        if existing is None:
            source = SourceItem(
                id=uuid4(),
                organization_id=request.organization_id,
                connector_id=request.connector_id,
                source_item_key=discovered.external_id,
                source_item_type="file",
                title=discovered.title,
                source_url=None,
                mime_type=discovered.mime_type,
                source_checksum=discovered.checksum,
                size_bytes=_source_size(discovered),
                source_created_at=discovered.created_at,
                source_modified_at=discovered.updated_at,
                first_seen_at=observed_at,
                last_seen_at=observed_at,
                status="active",
                source_metadata=metadata,
                metadata_schema_version=1,
            )
            return self._sources.add(request.organization_id, request.connector_id, source)
        updated = self._sources.update_provider_state(
            request.organization_id,
            request.connector_id,
            existing.id,
            source_metadata=metadata,
            metadata_schema_version=1,
            last_seen_at=observed_at,
            source_checksum=discovered.checksum,
            size_bytes=_source_size(discovered),
            source_modified_at=discovered.updated_at,
        )
        if updated is None:
            raise LocalFolderSourceReconciliationError("source item could not be reconciled")
        if existing.status != "active":
            restored = self._sources.set_lifecycle(
                request.organization_id, request.connector_id, existing.id, "active"
            )
            if restored is None:
                raise LocalFolderSourceReconciliationError("source item could not be restored")
        return updated

    def _persist_membership(
        self, request: LocalFolderSynchronizationRequest, source_item_id: UUID, observed_at: datetime
    ) -> None:
        membership = self._sources.lock_membership(
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            source_item_id,
        )
        if membership is None:
            self._sources.add_membership(
                request.organization_id,
                request.connector_id,
                SourceItemScopeMembership(
                    id=uuid4(),
                    organization_id=request.organization_id,
                    connector_id=request.connector_id,
                    source_item_id=source_item_id,
                    connector_scope_id=request.connector_scope_id,
                    status="active",
                    first_discovered_at=observed_at,
                    last_seen_at=observed_at,
                ),
            )
            return
        if self._sources.reactivate_membership(
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            source_item_id,
            observed_at,
        ) is None:
            raise LocalFolderSourceReconciliationError("source membership could not be reconciled")

    def _reconcile_membership_batch(
        self,
        request: LocalFolderSynchronizationRequest,
        run: ConnectorSyncRun,
        cursor: MembershipReconciliationCursor | None,
        observed_at: datetime,
        counts: _BatchCounts,
    ) -> tuple[bool, MembershipReconciliationCursor | None]:
        page = self._sources.list_active_memberships_before(
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            run.started_at,
            limit=request.batch_size,
            cursor=cursor,
        )
        for membership in page.items:
            source = self._sources.lock_by_id(
                request.organization_id, request.connector_id, membership.source_item_id
            )
            locked = self._sources.lock_membership(
                request.organization_id,
                request.connector_id,
                request.connector_scope_id,
                membership.source_item_id,
            )
            if source is None or locked is None:
                continue
            if locked.status != "active" or locked.last_seen_at >= run.started_at:
                continue
            if self._sources.remove_membership(
                request.organization_id,
                request.connector_id,
                request.connector_scope_id,
                source.id,
                observed_at,
            ) is None:
                raise LocalFolderSourceReconciliationError(
                    "missing source membership could not be reconciled"
                )
            if not self._sources.has_active_membership(
                request.organization_id, request.connector_id, source.id
            ):
                if self._sources.set_lifecycle(
                    request.organization_id, request.connector_id, source.id, "unavailable"
                ) is None:
                    raise LocalFolderSourceReconciliationError(
                        "missing source item could not be reconciled"
                    )
            sync_item = self._sync.get_item_by_key(
                request.organization_id,
                request.connector_id,
                run.id,
                source.source_item_key,
            )
            if sync_item is None:
                sync_item = ConnectorSyncItem(
                    id=uuid4(),
                    organization_id=request.organization_id,
                    connector_id=request.connector_id,
                    connector_scope_id=request.connector_scope_id,
                    sync_run_id=run.id,
                    source_item_id=source.id,
                    source_item_key=source.source_item_key,
                    change_type="deleted",
                    processing_status="pending",
                    previous_checksum=source.source_checksum,
                    current_checksum=None,
                    attempt_count=0,
                )
                self._sync.add_item(
                    request.organization_id,
                    request.connector_id,
                    request.connector_scope_id,
                    run.id,
                    sync_item,
                )
            self._finish_sync_item(request, run.id, sync_item, "succeeded", observed_at)
            self._increment_run(request, run.id, items_deleted=1)
            counts.reconciled += 1
        return not page.has_more, page.next_cursor

    def _persist_progress(
        self,
        request: LocalFolderSynchronizationRequest,
        run: ConnectorSyncRun,
        phase: str,
        after_key: str | None,
        membership_cursor: MembershipReconciliationCursor | None,
        observed_at: datetime,
    ) -> None:
        current = self._sync.get_active_cursor(
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            lock=True,
        )
        safe_cursor: dict[str, object] = {"phase": phase}
        if after_key is not None:
            safe_cursor["after_key"] = after_key
        if membership_cursor is not None:
            safe_cursor["last_seen_at"] = membership_cursor.last_seen_at.isoformat()
            safe_cursor["membership_id"] = str(membership_cursor.membership_id)
        version = current.cursor_version + 1 if current is not None else 1
        self._sync.replace_active_cursor(
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            run.id,
            version=version,
            cursor_type="local_folder_progress",
            activated_at=observed_at,
            safe_cursor=safe_cursor,
        )
        if phase != "completed":
            running = self._sync.set_run_state(
                request.organization_id,
                request.connector_id,
                request.connector_scope_id,
                run.id,
                status="running",
                started_at=run.started_at,
                heartbeat_at=observed_at,
            )
            if running is None:
                raise LocalFolderSynchronizationPersistenceError(
                    "synchronization progress could not be persisted"
                )

    def _finish_sync_item(
        self,
        request: LocalFolderSynchronizationRequest,
        run_id: UUID,
        item: ConnectorSyncItem,
        status: str,
        observed_at: datetime,
    ) -> ConnectorSyncItem:
        started = item.started_at or observed_at
        finished = observed_at if status in {"succeeded", "skipped", "failed"} else None
        attempt_count = item.attempt_count + (1 if status == "processing" else 0)
        updated = self._sync.set_item_state(
            request.organization_id,
            request.connector_id,
            run_id,
            item.id,
            status=status,
            attempt_count=attempt_count,
            started_at=started,
            finished_at=finished,
            source_item_id=item.source_item_id,
        )
        if updated is None:
            raise LocalFolderSynchronizationPersistenceError(
                "synchronization item could not be persisted"
            )
        return updated

    def _increment_run(self, request: LocalFolderSynchronizationRequest, run_id: UUID, **values: int) -> None:
        if self._sync.increment_counters(
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            run_id,
            **values,
        ) is None:
            raise LocalFolderSynchronizationPersistenceError(
                "synchronization counters could not be persisted"
            )

    def _fail_indexing(
        self,
        request: LocalFolderSynchronizationRequest,
        run: ConnectorSyncRun,
        item: ConnectorSyncItem,
        state: DocumentIndexingState | None,
        attempt: DocumentIndexingAttempt | None,
    ) -> None:
        completed_at = self._now()
        if state is not None and attempt is not None:
            self._indexing.complete_attempt(
                request.organization_id,
                state.id,
                attempt.id,
                status="failed",
                completed_at=completed_at,
                retryable=True,
                error_category="embedding",
                error_code="indexing_failed",
                summary={},
            )
            self._indexing.persist_controlled_state(
                request.organization_id,
                state.document_version_id,
                state.profile_fingerprint,
                status="failed",
                desired_generation=state.desired_generation,
                indexed_generation=state.indexed_generation,
                attempt_count=state.attempt_count,
                requested_at=state.requested_at,
                started_at=state.started_at,
                completed_at=completed_at,
                last_attempt_at=state.last_attempt_at,
                error_category="embedding",
                error_code="indexing_failed",
            )
        self._finish_sync_item(request, run.id, item, "failed", completed_at)
        self._increment_run(request, run.id, items_discovered=1, items_failed=1)
        self._sync.add_error(
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            run.id,
            SafeSyncError(
                "embedding",
                "indexing_failed",
                "Local Folder indexing failed",
                True,
                max(item.attempt_count, 1),
                {},
                completed_at,
            ),
            item_id=item.id,
        )
        self._sync.set_run_state(
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            run.id,
            status="failed",
            started_at=run.started_at,
            heartbeat_at=completed_at,
            finished_at=completed_at,
            error_summary="Local Folder indexing failed",
        )

    def _record_run_failure(
        self,
        request: LocalFolderSynchronizationRequest,
        run: ConnectorSyncRun,
        category: str,
        code: str,
        observed_at: datetime,
    ) -> None:
        self._sync.add_error(
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            run.id,
            SafeSyncError(category, code, "Local Folder synchronization failed", True, 1, {}, observed_at),
        )
        self._sync.set_run_state(
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            run.id,
            status="failed",
            started_at=run.started_at,
            heartbeat_at=observed_at,
            finished_at=observed_at,
            error_summary="Local Folder synchronization failed",
        )

    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise InvalidLocalFolderSynchronizationRequest(
                "synchronization clock must return a timezone-aware datetime"
            )
        return value


def _validate_request(request: LocalFolderSynchronizationRequest) -> None:
    if not isinstance(request, LocalFolderSynchronizationRequest):
        raise InvalidLocalFolderSynchronizationRequest("synchronization request is invalid")
    try:
        _require_uuid("organization_id", request.organization_id)
        _require_uuid("connector_id", request.connector_id)
        _require_uuid("connector_scope_id", request.connector_scope_id)
        if request.initiated_by_user_id is not None:
            _require_uuid("initiated_by_user_id", request.initiated_by_user_id)
        if request.sync_run_id is not None:
            _require_uuid("sync_run_id", request.sync_run_id)
        _require_choice("mode", request.mode, SYNC_MODES)
        _require_choice("trigger_type", request.trigger_type, SYNC_TRIGGERS)
        _require_limit(request.batch_size)
    except InvalidConnectorRepositoryRequest as exc:
        raise InvalidLocalFolderSynchronizationRequest("synchronization request is invalid") from exc
    if request.sync_run_id is not None and request.initiated_by_user_id is not None:
        raise InvalidLocalFolderSynchronizationRequest("continuation request is invalid")


def _validate_configuration(connector, scope, request: LocalFolderSynchronizationRequest) -> Path:
    if connector is None or scope is None:
        raise LocalFolderSynchronizationUnavailable("connector scope is unavailable")
    if connector.organization_id != request.organization_id or connector.id != request.connector_id:
        raise LocalFolderSynchronizationUnavailable("connector scope is unavailable")
    if scope.organization_id != request.organization_id or scope.connector_id != request.connector_id:
        raise LocalFolderSynchronizationUnavailable("connector scope is unavailable")
    if connector.connector_type != "local_folder" or connector.status != "active":
        raise LocalFolderSynchronizationUnavailable("connector scope is unavailable")
    if scope.status != "active" or scope.scope_type != "folder" or scope.access_mode != "platform_managed":
        raise LocalFolderSynchronizationUnavailable("connector scope is unavailable")
    if connector.acl_support != "none" or connector.credential_status not in {"not_configured", "valid"}:
        raise LocalFolderSynchronizationUnavailable("connector scope is unavailable")
    if scope.safe_config.get("follow_symlinks", False) is not False:
        raise UnsafeLocalFolderConfiguration("Local Folder configuration is unsafe")
    raw_root = scope.external_scope_key
    if not isinstance(raw_root, str) or not raw_root.strip():
        raise UnsafeLocalFolderConfiguration("Local Folder configuration is unsafe")
    root = Path(raw_root)
    if not root.is_absolute() or ".." in root.parts or root.is_symlink():
        raise UnsafeLocalFolderConfiguration("Local Folder configuration is unsafe")
    try:
        resolved = root.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise UnsafeLocalFolderConfiguration("Local Folder configuration is unsafe") from exc
    if not resolved.is_dir():
        raise UnsafeLocalFolderConfiguration("Local Folder configuration is unsafe")
    return resolved


def _validate_capabilities(snapshot: dict[str, object], provider: LocalFolderConnector) -> None:
    if snapshot != asdict(provider.capabilities):
        raise LocalFolderSynchronizationUnavailable("connector capabilities are incompatible")


def _source_size(item: DiscoveredSourceItem) -> int:
    value = item.metadata.get("size_bytes")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LocalFolderSourceReconciliationError("source metadata is invalid")
    return value


def _result(
    run_id: UUID,
    outcome: str,
    counts: _BatchCounts,
    discovery_completed: bool,
    reconciliation_completed: bool,
) -> LocalFolderSynchronizationResult:
    return LocalFolderSynchronizationResult(
        sync_run_id=run_id,
        outcome=outcome,
        files_discovered=counts.discovered,
        supported_files_considered=counts.considered,
        unchanged_files=counts.unchanged,
        new_source_items=counts.new,
        changed_source_items=counts.changed,
        restored_source_items=counts.restored,
        versions_created=counts.versions_created,
        versions_indexed=counts.versions_indexed,
        failed_items=counts.failed,
        missing_memberships_reconciled=counts.reconciled,
        batches_processed=1,
        discovery_completed=discovery_completed,
        reconciliation_completed=reconciliation_completed,
    )
