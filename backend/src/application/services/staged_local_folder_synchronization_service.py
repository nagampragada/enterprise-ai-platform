"""Transaction-safe staged Local Folder synchronization orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
from itertools import islice
import json
import math
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from application.services.connector_sync_execution_service import ConnectorSyncExecutionService
from application.services.local_document_indexing_service import LocalDocumentIndexingProfile
from application.services.local_document_ingestion_service import _normalized_profile_name, _profile_hash
from domain.connectors.exceptions import ConnectorUnavailableError
from domain.connectors.models import SourceItem as DiscoveredSourceItem, SyncCheckpoint
from domain.content_chunking.chunker import ContentChunker
from domain.embeddings.models import EmbeddingRequest
from domain.embeddings.provider import EmbeddingProvider
from domain.embeddings.validation import validate_embedding_results
from infrastructure.connectors.local.connector import LocalFolderConnector
from infrastructure.content_extraction.registry import ContentExtractorRegistry
from infrastructure.db.models import (
    ConnectorSyncItem,
    ConnectorSyncRun,
    Document,
    DocumentChunk,
    DocumentIndexingState,
    SourceItem,
    SourceItemScopeMembership,
)
from infrastructure.repositories.connector_repository import ConnectorRepository
from infrastructure.repositories.connector_scope_repository import ConnectorScopeRepository
from infrastructure.repositories.connector_sync_job_repository import SyncJobLease
from infrastructure.repositories.connector_sync_repository import ConnectorSyncRepository
from infrastructure.repositories.document_chunk_repository import DocumentChunkRepository
from infrastructure.repositories.document_indexing_repository import DocumentIndexingRepository
from infrastructure.repositories.document_repository import DocumentRepository
from infrastructure.repositories.document_version_repository import DocumentVersionRepository
from infrastructure.repositories.source_item_repository import (
    MembershipReconciliationCursor,
    SourceItemRepository,
)

MAX_PREPARED_CHUNKS = 500


class InvalidStagedLocalFolderRequest(ValueError):
    """Raised when a staged synchronization contract is invalid."""


class StalePreparedLocalFolderItem(ConnectorUnavailableError):
    """Raised when a prepared artifact no longer matches persisted/source state."""


@dataclass(frozen=True)
class LocalFolderSynchronizationSnapshot:
    organization_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    sync_run_id: UUID
    run_started_at: datetime
    root_path: Path
    phase: str
    after_key: str | None
    reconciliation_cursor: MembershipReconciliationCursor | None
    profile: LocalDocumentIndexingProfile


@dataclass(frozen=True)
class LocalFolderDiscoveredEntry:
    source_item_key: str
    title: str
    mime_type: str | None
    checksum: str
    size_bytes: int
    source_created_at: datetime | None
    source_modified_at: datetime | None
    has_more: bool

    def __post_init__(self) -> None:
        if not self.source_item_key.strip() or Path(self.source_item_key).is_absolute():
            raise InvalidStagedLocalFolderRequest("discovered source identity is invalid")
        if ".." in Path(self.source_item_key).parts:
            raise InvalidStagedLocalFolderRequest("discovered source identity is invalid")
        if not self.title.strip() or self.size_bytes < 0:
            raise InvalidStagedLocalFolderRequest("discovered source metadata is invalid")
        if len(self.checksum) != 64 or any(value not in "0123456789abcdef" for value in self.checksum):
            raise InvalidStagedLocalFolderRequest("discovered source checksum is invalid")
        for value in (self.source_created_at, self.source_modified_at):
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise InvalidStagedLocalFolderRequest("discovered source timestamps are invalid")
        if not isinstance(self.has_more, bool):
            raise InvalidStagedLocalFolderRequest("discovery continuation is invalid")


@dataclass(frozen=True)
class LocalFolderItemSnapshot:
    source_item_id: UUID | None
    persisted_checksum: str | None
    current_version_checksum: str | None
    profile_complete: bool
    run_item_status: str | None
    run_item_checksum: str | None


@dataclass(frozen=True)
class PreparedLocalFolderChunk:
    chunk_index: int
    chunk_text: str
    content_hash: str
    embedding: tuple[float, ...]

    def __post_init__(self) -> None:
        if self.chunk_index < 0 or not self.chunk_text.strip():
            raise InvalidStagedLocalFolderRequest("prepared chunk is invalid")
        if len(self.content_hash) != 64 or any(
            value not in "0123456789abcdef" for value in self.content_hash
        ):
            raise InvalidStagedLocalFolderRequest("prepared chunk hash is invalid")
        if len(self.embedding) != 1536 or any(not math.isfinite(value) for value in self.embedding):
            raise InvalidStagedLocalFolderRequest("prepared embedding is invalid")


@dataclass(frozen=True)
class PreparedLocalFolderItem:
    entry: LocalFolderDiscoveredEntry
    expected_source_item_id: UUID | None
    expected_persisted_checksum: str | None
    outcome: str
    document_title: str | None
    document_mime_type: str | None
    chunks: tuple[PreparedLocalFolderChunk, ...]
    embedding_model: str | None

    def __post_init__(self) -> None:
        if self.outcome not in {"already_complete", "unchanged", "indexed"}:
            raise InvalidStagedLocalFolderRequest("prepared item outcome is invalid")
        if self.outcome == "indexed":
            if not self.chunks or self.embedding_model is None:
                raise InvalidStagedLocalFolderRequest("indexed item requires prepared artifacts")
            if len(self.chunks) > MAX_PREPARED_CHUNKS:
                raise InvalidStagedLocalFolderRequest("prepared item contains too many chunks")
        elif self.chunks or self.embedding_model is not None:
            raise InvalidStagedLocalFolderRequest("unchanged item cannot contain prepared artifacts")


@dataclass(frozen=True)
class LocalFolderPersistenceOutcome:
    outcome: str
    phase: str
    source_item_key: str | None


class LocalFolderPreparationService:
    """Perform bounded filesystem, extraction, chunking and embedding work without a session."""

    def __init__(
        self,
        extractor_registry: ContentExtractorRegistry,
        content_chunker: ContentChunker,
        embedding_provider: EmbeddingProvider,
    ) -> None:
        self._extractors = extractor_registry
        self._chunker = content_chunker
        self._embedding_provider = embedding_provider

    @property
    def profile(self) -> LocalDocumentIndexingProfile:
        extractor_signature = tuple(
            (extension, type(extractor).__module__, type(extractor).__qualname__)
            for extension, extractor in sorted(self._extractors.extractors.items())
        )
        chunker_type = type(self._chunker)
        ingestion = {
            "extraction_profile": "content_extraction",
            "extraction_version": _profile_hash(extractor_signature),
            "chunking_profile": _normalized_profile_name(chunker_type.__name__),
            "chunking_version": _profile_hash(
                {
                    "implementation": f"{chunker_type.__module__}.{chunker_type.__qualname__}",
                    "config": {"max_chunk_size": 2000, "overlap": 200, "minimum_preferred_size": 200},
                }
            ),
        }
        embedding = self._embedding_provider.profile
        values = {
            **ingestion,
            "embedding_provider": embedding.provider_name,
            "embedding_model": embedding.model_identifier,
            "embedding_dimensions": embedding.dimension,
        }
        fingerprint = hashlib.sha256(
            json.dumps(values, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return LocalDocumentIndexingProfile(**values, fingerprint=fingerprint)

    def discover_next(
        self, snapshot: LocalFolderSynchronizationSnapshot
    ) -> LocalFolderDiscoveredEntry | None:
        _require_phase(snapshot, "discovery")
        if snapshot.root_path.is_symlink():
            raise InvalidStagedLocalFolderRequest("Local Folder configuration is unsafe")
        provider = LocalFolderConnector(snapshot.organization_id, snapshot.connector_id, snapshot.root_path)
        discovered = list(
            islice(
                provider.crawl(
                    SyncCheckpoint(cursor=snapshot.after_key, last_synced_at=snapshot.run_started_at)
                ),
                2,
            )
        )
        if not discovered:
            return None
        item = discovered[0]
        size = item.metadata.get("size_bytes")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise InvalidStagedLocalFolderRequest("source metadata is invalid")
        return LocalFolderDiscoveredEntry(
            item.external_id,
            item.title,
            item.mime_type,
            item.checksum,
            size,
            item.created_at,
            item.updated_at,
            len(discovered) > 1,
        )

    def prepare_item(
        self,
        snapshot: LocalFolderSynchronizationSnapshot,
        item_snapshot: LocalFolderItemSnapshot,
        entry: LocalFolderDiscoveredEntry,
    ) -> PreparedLocalFolderItem:
        _require_phase(snapshot, "discovery")
        if (
            item_snapshot.run_item_status in {"succeeded", "skipped"}
            and item_snapshot.run_item_checksum == entry.checksum
        ):
            return _prepared_without_content(entry, item_snapshot, "already_complete")
        if (
            item_snapshot.persisted_checksum == entry.checksum
            and item_snapshot.current_version_checksum == entry.checksum
            and item_snapshot.profile_complete
        ):
            return _prepared_without_content(entry, item_snapshot, "unchanged")

        provider = LocalFolderConnector(snapshot.organization_id, snapshot.connector_id, snapshot.root_path)
        path = provider.resolve_content_path(entry.source_item_key)
        before = path.stat()
        before_checksum = _sha256_file(path)
        if before_checksum != entry.checksum:
            raise StalePreparedLocalFolderItem("source changed during preparation")
        extracted = self._extractors.extract(path)
        _require_unchanged_file(path, before, before_checksum)
        chunk_results = self._chunker.chunk(extracted.text)
        if not chunk_results or len(chunk_results) > MAX_PREPARED_CHUNKS:
            raise InvalidStagedLocalFolderRequest("prepared chunk count is invalid")
        profile = self._embedding_provider.profile
        prepared: list[PreparedLocalFolderChunk] = []
        batch_size = profile.max_batch_size or len(chunk_results)
        for start in range(0, len(chunk_results), batch_size):
            batch = chunk_results[start : start + batch_size]
            requests = tuple(
                EmbeddingRequest(input_index=index, text=chunk.content)
                for index, chunk in enumerate(batch)
            )
            results = validate_embedding_results(
                requests, self._embedding_provider.embed_batch(requests), profile
            )
            prepared.extend(
                PreparedLocalFolderChunk(
                    chunk.chunk_index,
                    chunk.content,
                    chunk.content_checksum,
                    result.vector,
                )
                for chunk, result in zip(batch, results, strict=True)
            )
        _require_unchanged_file(path, before, before_checksum)
        return PreparedLocalFolderItem(
            entry,
            item_snapshot.source_item_id,
            item_snapshot.persisted_checksum,
            "indexed",
            extracted.title or entry.title,
            entry.mime_type or extracted.mime_type,
            tuple(prepared),
            profile.model_identifier,
        )


class StagedLocalFolderSynchronizationService:
    """Load snapshots and persist prepared items in short caller-owned transactions."""

    def __init__(
        self,
        session: Session,
        execution_service: ConnectorSyncExecutionService,
        profile: LocalDocumentIndexingProfile,
    ) -> None:
        self._session = session
        self._execution = execution_service
        self._profile = profile
        self._connectors = ConnectorRepository(session)
        self._scopes = ConnectorScopeRepository(session)
        self._sources = SourceItemRepository(session)
        self._sync = ConnectorSyncRepository(session)
        self._versions = DocumentVersionRepository(session)
        self._indexing = DocumentIndexingRepository(session)
        self._documents = DocumentRepository(session)
        self._chunks = DocumentChunkRepository(session)

    def snapshot(
        self, lease: SyncJobLease, sync_run_id: UUID, *, worker_id: str, now: datetime
    ) -> LocalFolderSynchronizationSnapshot:
        self._execution.validate_attempt(lease, sync_run_id, worker_id=worker_id)
        connector = self._connectors.get_by_id(lease.organization_id, lease.connector_id)
        scope = self._scopes.get_by_id(lease.organization_id, lease.connector_scope_id)
        run = self._session.execute(
            select(ConnectorSyncRun).where(
                ConnectorSyncRun.organization_id == lease.organization_id,
                ConnectorSyncRun.connector_id == lease.connector_id,
                ConnectorSyncRun.connector_scope_id == lease.connector_scope_id,
                ConnectorSyncRun.id == sync_run_id,
                ConnectorSyncRun.status == "running",
            )
        ).scalar_one_or_none()
        if connector is None or scope is None or run is None:
            raise InvalidStagedLocalFolderRequest("synchronization context is unavailable")
        if (
            connector.connector_type != "local_folder"
            or connector.status != "active"
            or connector.acl_support != "none"
            or scope.connector_id != connector.id
            or scope.status != "active"
            or scope.scope_type != "folder"
            or scope.access_mode != "platform_managed"
            or scope.safe_config.get("follow_symlinks", False) is not False
        ):
            raise InvalidStagedLocalFolderRequest("synchronization context is unavailable")
        root = Path(scope.external_scope_key)
        if not root.is_absolute() or ".." in root.parts:
            raise InvalidStagedLocalFolderRequest("Local Folder configuration is unsafe")
        phase, after_key, reconciliation_cursor = self._load_progress(run)
        return LocalFolderSynchronizationSnapshot(
            lease.organization_id,
            lease.connector_id,
            lease.connector_scope_id,
            run.id,
            run.started_at,
            root,
            phase,
            after_key,
            reconciliation_cursor,
            self._profile,
        )

    def item_snapshot(
        self,
        lease: SyncJobLease,
        snapshot: LocalFolderSynchronizationSnapshot,
        entry: LocalFolderDiscoveredEntry,
        *,
        worker_id: str,
    ) -> LocalFolderItemSnapshot:
        self._execution.validate_attempt(lease, snapshot.sync_run_id, worker_id=worker_id)
        source = self._sources.get_by_key(
            snapshot.organization_id, snapshot.connector_id, entry.source_item_key
        )
        current = (
            self._versions.get_current(snapshot.organization_id, source.id) if source else None
        )
        state = (
            self._indexing.get_state(
                snapshot.organization_id, current.id, snapshot.profile.fingerprint
            )
            if current else None
        )
        run_item = self._sync.get_item_by_key(
            snapshot.organization_id,
            snapshot.connector_id,
            snapshot.sync_run_id,
            entry.source_item_key,
        )
        return LocalFolderItemSnapshot(
            source.id if source else None,
            source.source_checksum if source else None,
            current.content_checksum if current else None,
            bool(
                state
                and state.status == "indexed"
                and state.indexed_generation == state.desired_generation
            ),
            run_item.processing_status if run_item else None,
            run_item.current_checksum if run_item else None,
        )

    def persist_discovery(
        self,
        lease: SyncJobLease,
        snapshot: LocalFolderSynchronizationSnapshot,
        prepared: PreparedLocalFolderItem | None,
        *,
        worker_id: str,
        now: datetime,
    ) -> LocalFolderPersistenceOutcome:
        self._execution.validate_attempt(lease, snapshot.sync_run_id, worker_id=worker_id)
        self._require_active_context(snapshot)
        if prepared is None:
            self._persist_progress(snapshot, "reconciliation", None, None, now)
            return LocalFolderPersistenceOutcome("in_progress", "reconciliation", None)
        entry = prepared.entry
        source = self._sources.lock_by_key(
            snapshot.organization_id, snapshot.connector_id, entry.source_item_key
        )
        if source is None and prepared.expected_source_item_id is not None:
            raise StalePreparedLocalFolderItem("expected source identity disappeared")
        prior_item = self._sync.get_item_by_key(
            snapshot.organization_id,
            snapshot.connector_id,
            snapshot.sync_run_id,
            entry.source_item_key,
        )
        if prior_item and prior_item.processing_status in {"succeeded", "skipped"}:
            if prior_item.current_checksum != entry.checksum:
                raise StalePreparedLocalFolderItem("completed item checksum changed")
            self._persist_progress(
                snapshot,
                "discovery" if entry.has_more else "reconciliation",
                entry.source_item_key if entry.has_more else None,
                None,
                now,
            )
            return LocalFolderPersistenceOutcome("idempotent", "discovery", entry.source_item_key)
        if source is not None:
            if prepared.expected_source_item_id not in {None, source.id}:
                raise StalePreparedLocalFolderItem("source identity changed")
            if (
                source.source_checksum != prepared.expected_persisted_checksum
                and source.source_checksum != entry.checksum
            ):
                raise StalePreparedLocalFolderItem("persisted source changed")
        source, restored = self._persist_source(snapshot, entry, source, now)
        self._persist_membership(snapshot, source.id, now)
        item = prior_item or ConnectorSyncItem(
            id=uuid4(),
            organization_id=snapshot.organization_id,
            connector_id=snapshot.connector_id,
            connector_scope_id=snapshot.connector_scope_id,
            sync_run_id=snapshot.sync_run_id,
            source_item_id=source.id,
            source_item_key=entry.source_item_key,
            change_type=("new" if prepared.expected_source_item_id is None else "unchanged" if prepared.outcome != "indexed" else "changed"),
            processing_status="pending",
            previous_checksum=prepared.expected_persisted_checksum,
            current_checksum=entry.checksum,
            attempt_count=0,
        )
        if prior_item is None:
            self._sync.add_item(
                snapshot.organization_id,
                snapshot.connector_id,
                snapshot.connector_scope_id,
                snapshot.sync_run_id,
                item,
            )
        if prepared.outcome in {"already_complete", "unchanged"}:
            self._finish_item(snapshot, item, "skipped", now)
            self._sync.increment_counters(
                snapshot.organization_id,
                snapshot.connector_id,
                snapshot.connector_scope_id,
                snapshot.sync_run_id,
                items_discovered=1,
                items_unchanged=1,
                items_skipped=1,
            )
        else:
            self._persist_indexed(snapshot, source, item, prepared, restored, now)
        self._persist_progress(
            snapshot,
            "discovery" if entry.has_more else "reconciliation",
            entry.source_item_key if entry.has_more else None,
            None,
            now,
        )
        return LocalFolderPersistenceOutcome(
            "persisted", "discovery" if entry.has_more else "reconciliation", entry.source_item_key
        )

    def reconcile(
        self,
        lease: SyncJobLease,
        snapshot: LocalFolderSynchronizationSnapshot,
        *,
        worker_id: str,
        now: datetime,
        limit: int,
    ) -> LocalFolderPersistenceOutcome:
        self._execution.validate_attempt(lease, snapshot.sync_run_id, worker_id=worker_id)
        self._require_active_context(snapshot)
        _require_phase(snapshot, "reconciliation")
        page = self._sources.list_active_memberships_before(
            snapshot.organization_id,
            snapshot.connector_id,
            snapshot.connector_scope_id,
            snapshot.run_started_at,
            limit=limit,
            cursor=snapshot.reconciliation_cursor,
        )
        for membership in page.items:
            source = self._sources.lock_by_id(
                snapshot.organization_id, snapshot.connector_id, membership.source_item_id
            )
            locked = self._sources.lock_membership(
                snapshot.organization_id,
                snapshot.connector_id,
                snapshot.connector_scope_id,
                membership.source_item_id,
            )
            if source is None or locked is None:
                continue
            if locked.status != "active" or locked.last_seen_at >= snapshot.run_started_at:
                continue
            self._sources.remove_membership(
                snapshot.organization_id,
                snapshot.connector_id,
                snapshot.connector_scope_id,
                source.id,
                now,
            )
            if not self._sources.has_active_membership(
                snapshot.organization_id, snapshot.connector_id, source.id
            ):
                self._sources.set_lifecycle(
                    snapshot.organization_id, snapshot.connector_id, source.id, "unavailable"
                )
            item = self._sync.get_item_by_key(
                snapshot.organization_id,
                snapshot.connector_id,
                snapshot.sync_run_id,
                source.source_item_key,
            )
            if item is None:
                item = ConnectorSyncItem(
                    id=uuid4(), organization_id=snapshot.organization_id,
                    connector_id=snapshot.connector_id, connector_scope_id=snapshot.connector_scope_id,
                    sync_run_id=snapshot.sync_run_id, source_item_id=source.id,
                    source_item_key=source.source_item_key, change_type="deleted",
                    processing_status="pending", previous_checksum=source.source_checksum,
                    current_checksum=None, attempt_count=0,
                )
                self._sync.add_item(
                    snapshot.organization_id, snapshot.connector_id,
                    snapshot.connector_scope_id, snapshot.sync_run_id, item,
                )
                self._finish_item(snapshot, item, "succeeded", now)
                self._sync.increment_counters(
                    snapshot.organization_id, snapshot.connector_id,
                    snapshot.connector_scope_id, snapshot.sync_run_id, items_deleted=1,
                )
        if page.has_more:
            self._persist_progress(snapshot, "reconciliation", None, page.next_cursor, now)
            return LocalFolderPersistenceOutcome("in_progress", "reconciliation", None)
        self._persist_progress(snapshot, "completed", None, None, now)
        run = self._session.execute(
            select(ConnectorSyncRun).where(
                ConnectorSyncRun.organization_id == snapshot.organization_id,
                ConnectorSyncRun.id == snapshot.sync_run_id,
            )
        ).scalar_one()
        self._sync.set_run_state(
            snapshot.organization_id, snapshot.connector_id, snapshot.connector_scope_id,
            snapshot.sync_run_id, status="completed", started_at=run.started_at,
            heartbeat_at=now, finished_at=now,
        )
        self._execution.complete_success(lease, worker_id=worker_id)
        return LocalFolderPersistenceOutcome("completed", "completed", None)

    def _load_progress(
        self, run: ConnectorSyncRun
    ) -> tuple[str, str | None, MembershipReconciliationCursor | None]:
        cursor = self._sync.get_active_cursor(
            run.organization_id,
            run.connector_id,
            run.connector_scope_id,
        )
        if cursor is None or cursor.created_by_run_id != run.id:
            return "discovery", None, None
        value = cursor.safe_cursor
        if not isinstance(value, dict):
            raise InvalidStagedLocalFolderRequest("synchronization progress is invalid")
        phase = value.get("phase")
        if phase == "discovery":
            after_key = value.get("after_key")
            if not isinstance(after_key, str) or not after_key:
                raise InvalidStagedLocalFolderRequest("synchronization progress is invalid")
            return phase, after_key, None
        if phase == "reconciliation":
            if "membership_id" not in value:
                return phase, None, None
            try:
                return phase, None, MembershipReconciliationCursor(
                    datetime.fromisoformat(str(value["last_seen_at"])), UUID(str(value["membership_id"]))
                )
            except (TypeError, ValueError) as exc:
                raise InvalidStagedLocalFolderRequest("synchronization progress is invalid") from exc
        if phase == "completed":
            return phase, None, None
        raise InvalidStagedLocalFolderRequest("synchronization progress is invalid")

    def _persist_source(
        self, snapshot, entry, existing: SourceItem | None, now: datetime
    ) -> tuple[SourceItem, bool]:
        metadata = {"relative_path": entry.source_item_key, "extension": Path(entry.source_item_key).suffix.lower(), "size_bytes": entry.size_bytes}
        if existing is None:
            source = SourceItem(
                id=uuid4(), organization_id=snapshot.organization_id,
                connector_id=snapshot.connector_id, source_item_key=entry.source_item_key,
                source_item_type="file", title=entry.title, source_url=None,
                mime_type=entry.mime_type, source_checksum=entry.checksum,
                size_bytes=entry.size_bytes, source_created_at=entry.source_created_at,
                source_modified_at=entry.source_modified_at, first_seen_at=now,
                last_seen_at=now, status="active", source_metadata=metadata,
                metadata_schema_version=1,
            )
            return self._sources.add(snapshot.organization_id, snapshot.connector_id, source), False
        restored = existing.status != "active"
        source = self._sources.update_provider_state(
            snapshot.organization_id, snapshot.connector_id, existing.id,
            source_metadata=metadata, metadata_schema_version=1, last_seen_at=now,
            source_checksum=entry.checksum, size_bytes=entry.size_bytes,
            source_modified_at=entry.source_modified_at,
        )
        if source is None:
            raise StalePreparedLocalFolderItem("source could not be updated")
        if restored:
            self._sources.set_lifecycle(snapshot.organization_id, snapshot.connector_id, source.id, "active")
        return source, restored

    def _persist_membership(self, snapshot, source_id: UUID, now: datetime) -> None:
        membership = self._sources.lock_membership(
            snapshot.organization_id, snapshot.connector_id,
            snapshot.connector_scope_id, source_id,
        )
        if membership is None:
            self._sources.add_membership(
                snapshot.organization_id, snapshot.connector_id,
                SourceItemScopeMembership(
                    id=uuid4(), organization_id=snapshot.organization_id,
                    connector_id=snapshot.connector_id, source_item_id=source_id,
                    connector_scope_id=snapshot.connector_scope_id, status="active",
                    first_discovered_at=now, last_seen_at=now,
                ),
            )
        else:
            self._sources.reactivate_membership(
                snapshot.organization_id, snapshot.connector_id,
                snapshot.connector_scope_id, source_id, now,
            )

    def _persist_indexed(self, snapshot, source, item, prepared, restored, now) -> None:
        current = self._versions.get_current(snapshot.organization_id, source.id)
        if current is None or current.content_checksum != prepared.entry.checksum:
            current = self._versions.create_current_version(
                snapshot.organization_id, source.id,
                version_cause="discovered" if prepared.expected_source_item_id is None else "content_changed",
                lifecycle="available", discovered_at=now,
                content_checksum=prepared.entry.checksum, checksum_algorithm="sha256",
                source_modified_at=prepared.entry.source_modified_at,
                source_size_bytes=prepared.entry.size_bytes,
                content_type=prepared.document_mime_type,
                file_extension=Path(prepared.entry.source_item_key).suffix.lower(),
                metadata={"source_item_type": "file"},
            )
        state = self._indexing.get_or_create_state(
            snapshot.organization_id, current.id,
            DocumentIndexingState(
                id=uuid4(), organization_id=snapshot.organization_id,
                document_version_id=current.id,
                extraction_profile=snapshot.profile.extraction_profile,
                extraction_version=snapshot.profile.extraction_version,
                chunking_profile=snapshot.profile.chunking_profile,
                chunking_version=snapshot.profile.chunking_version,
                embedding_provider=snapshot.profile.embedding_provider,
                embedding_model=snapshot.profile.embedding_model,
                embedding_dimensions=snapshot.profile.embedding_dimensions,
                profile_fingerprint=snapshot.profile.fingerprint,
                desired_generation=1, indexed_generation=None, status="pending",
                reason="new_version" if prepared.expected_source_item_id is None else "content_changed",
                attempt_count=0, requested_at=now,
            ),
        )
        if state.status == "indexed" and state.indexed_generation == state.desired_generation:
            self._finish_item(snapshot, item, "skipped", now)
            self._sync.increment_counters(
                snapshot.organization_id, snapshot.connector_id,
                snapshot.connector_scope_id, snapshot.sync_run_id,
                items_discovered=1, items_unchanged=1, items_skipped=1,
            )
            return
        attempt = self._indexing.allocate_attempt(
            snapshot.organization_id, current.id, snapshot.profile.fingerprint,
            trigger_type="sync", started_at=now,
            sync_run_id=snapshot.sync_run_id, sync_item_id=item.id,
        )
        document = self._documents.get_by_source_identity(
            snapshot.organization_id, "local_folder", prepared.entry.source_item_key
        )
        if document is None:
            document = Document(
                id=uuid4(), organization_id=snapshot.organization_id,
                source_type="local_folder", source_document_key=prepared.entry.source_item_key,
                title=prepared.document_title or prepared.entry.title, source_url=None,
                mime_type=prepared.document_mime_type,
                checksum_latest=prepared.entry.checksum, status="ready",
                source_created_at=prepared.entry.source_created_at,
                source_updated_at=prepared.entry.source_modified_at,
            )
            self._documents.add(snapshot.organization_id, document)
            self._session.flush()
        else:
            self._documents.update(
                snapshot.organization_id, document.id,
                title=prepared.document_title, mime_type=prepared.document_mime_type,
                checksum_latest=prepared.entry.checksum, status="ready",
                source_created_at=prepared.entry.source_created_at,
                source_updated_at=prepared.entry.source_modified_at,
            )
        chunks = [
            DocumentChunk(
                id=uuid4(), organization_id=snapshot.organization_id,
                document_id=document.id, chunk_index=chunk.chunk_index,
                chunk_text=chunk.chunk_text, token_count=None,
                content_hash=chunk.content_hash, embedding=list(chunk.embedding),
                embedding_model=prepared.embedding_model,
            )
            for chunk in prepared.chunks
        ]
        self._chunks.replace_for_document(snapshot.organization_id, document.id, chunks)
        self._session.flush()
        self._indexing.complete_attempt(
            snapshot.organization_id, state.id, attempt.id,
            status="succeeded", completed_at=now, retryable=False,
            summary={"chunks_indexed": len(chunks)},
        )
        self._indexing.persist_controlled_state(
            snapshot.organization_id, current.id, snapshot.profile.fingerprint,
            status="indexed", desired_generation=state.desired_generation,
            indexed_generation=state.desired_generation, attempt_count=state.attempt_count,
            requested_at=state.requested_at, started_at=state.started_at,
            completed_at=now, last_attempt_at=state.last_attempt_at,
        )
        self._versions.replace_materialization(
            snapshot.organization_id, source.id, current.id, document.id
        )
        self._finish_item(snapshot, item, "succeeded", now)
        counters = {"items_discovered": 1, "items_succeeded": 1}
        counters["items_new" if prepared.expected_source_item_id is None else "items_changed"] = 1
        self._sync.increment_counters(
            snapshot.organization_id, snapshot.connector_id,
            snapshot.connector_scope_id, snapshot.sync_run_id, **counters,
        )

    def _finish_item(self, snapshot, item, status, now):
        started = item.started_at or now
        result = self._sync.set_item_state(
            snapshot.organization_id, snapshot.connector_id, snapshot.sync_run_id,
            item.id, status=status, attempt_count=item.attempt_count,
            started_at=started, finished_at=now, source_item_id=item.source_item_id,
        )
        if result is None:
            raise StalePreparedLocalFolderItem("sync item could not be completed")

    def _require_active_context(self, snapshot: LocalFolderSynchronizationSnapshot) -> None:
        connector = self._connectors.get_by_id(snapshot.organization_id, snapshot.connector_id)
        scope = self._scopes.get_by_id(snapshot.organization_id, snapshot.connector_scope_id)
        if (
            connector is None
            or connector.connector_type != "local_folder"
            or connector.status != "active"
            or connector.acl_support != "none"
            or scope is None
            or scope.connector_id != snapshot.connector_id
            or scope.status != "active"
            or scope.scope_type != "folder"
            or scope.access_mode != "platform_managed"
        ):
            raise InvalidStagedLocalFolderRequest("synchronization context is unavailable")

    def _persist_progress(self, snapshot, phase, after_key, membership_cursor, now):
        current = self._sync.get_active_cursor(
            snapshot.organization_id, snapshot.connector_id,
            snapshot.connector_scope_id, lock=True,
        )
        safe_cursor = {"phase": phase}
        if after_key is not None:
            safe_cursor["after_key"] = after_key
        if membership_cursor is not None:
            safe_cursor["last_seen_at"] = membership_cursor.last_seen_at.isoformat()
            safe_cursor["membership_id"] = str(membership_cursor.membership_id)
        self._sync.replace_active_cursor(
            snapshot.organization_id, snapshot.connector_id,
            snapshot.connector_scope_id, snapshot.sync_run_id,
            version=(current.cursor_version + 1 if current else 1),
            cursor_type="local_folder_progress", activated_at=now,
            safe_cursor=safe_cursor,
        )


def _prepared_without_content(entry, snapshot, outcome):
    return PreparedLocalFolderItem(
        entry, snapshot.source_item_id, snapshot.persisted_checksum,
        outcome, None, None, (), None,
    )


def _require_phase(snapshot, expected):
    if snapshot.phase != expected:
        raise InvalidStagedLocalFolderRequest("synchronization phase is invalid")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for data in iter(lambda: handle.read(8192), b""):
            digest.update(data)
    return digest.hexdigest()


def _require_unchanged_file(path: Path, before, checksum: str) -> None:
    after = path.stat()
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or checksum != _sha256_file(path)
    ):
        raise StalePreparedLocalFolderItem("source changed during preparation")