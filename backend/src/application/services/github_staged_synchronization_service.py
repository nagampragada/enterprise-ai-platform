"""Bounded, resumable and transaction-safe GitHub repository synchronization."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import tempfile
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from application.services.connector_sync_execution_service import ConnectorSyncExecutionService
from application.services.connector_sync_retry_policy import (
    FailureClassification,
    SyncFailureKind,
    classification_for,
    classify_exception,
)
from application.services.github_repository_content_service import (
    MAX_GITHUB_BLOB_BYTES,
    MAX_REPOSITORY_PATH_SEGMENTS,
    SUPPORTED_CONTENT_EXTENSIONS,
    GitHubRepositoryContentAuthorization,
    GitHubRepositoryContentConflict,
    GitHubRepositoryContentNotFound,
    GitHubRepositoryContentRejected,
    GitHubRepositoryContentService,
    GitHubRepositoryContentUnavailable,
    GitHubRepositoryEntry,
    GitHubRepositorySnapshot,
    GitHubTreeDescriptor,
)
from application.services.local_document_indexing_service import LocalDocumentIndexingProfile
from application.services.local_document_ingestion_service import _normalized_profile_name, _profile_hash
from domain.content_chunking.chunker import ContentChunker
from domain.embeddings.models import EmbeddingRequest
from domain.embeddings.provider import EmbeddingProvider
from domain.embeddings.validation import validate_embedding_results
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
from infrastructure.repositories.connector_sync_job_repository import (
    LostSyncJobLease,
    StaleSyncJobFence,
    SyncJobCancellationConflict,
    SyncJobLease,
)
from infrastructure.repositories.connector_sync_repository import ConnectorSyncRepository
from infrastructure.repositories.document_chunk_repository import DocumentChunkRepository
from infrastructure.repositories.document_indexing_repository import DocumentIndexingRepository
from infrastructure.repositories.document_repository import DocumentRepository
from infrastructure.repositories.document_version_repository import DocumentVersionRepository
from infrastructure.repositories.source_item_repository import SourceItemRepository


CURSOR_SCHEMA_VERSION = 1
CURSOR_TYPE = "github_repository_progress"
MAX_CURSOR_BYTES = 96 * 1024
MAX_PREPARED_CHUNKS = 500
MAX_SOURCE_IDENTITY_CHARACTERS = 1024

DEFAULT_MAX_TREE_REQUESTS = 25
DEFAULT_MAX_ENTRIES = 1_000
DEFAULT_MAX_FILES = 10
DEFAULT_MAX_DOWNLOAD_BYTES = 25 * 1024 * 1024
DEFAULT_MAX_CHUNKS = 500

HARD_MAX_TREE_REQUESTS = 25
HARD_MAX_ENTRIES = 1_000
HARD_MAX_FILES = 10
HARD_MAX_DOWNLOAD_BYTES = 25 * 1024 * 1024
HARD_MAX_CHUNKS = 500

MAX_RUN_ENTRIES = 100_000
MAX_RUN_FILES = 10_000
MAX_RUN_DOWNLOAD_BYTES = 10 * 1024 * 1024 * 1024
MAX_RUN_CHUNKS = 500_000

_MIME_TYPES = {
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".markdown": "text/markdown",
}


class InvalidGitHubStagedSynchronizationRequest(ValueError):
    """Raised for an invalid or corrupted staged synchronization contract."""


class GitHubSynchronizationBudgetExceeded(InvalidGitHubStagedSynchronizationRequest):
    """Non-retryable exhaustion of a finite per-run synchronization budget."""


class StalePreparedGitHubBatch(RuntimeError):
    """Raised when provider-free prepared work no longer matches durable state."""


@dataclass(frozen=True)
class GitHubSynchronizationLimits:
    max_tree_requests: int = DEFAULT_MAX_TREE_REQUESTS
    max_entries: int = DEFAULT_MAX_ENTRIES
    max_files: int = DEFAULT_MAX_FILES
    max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES
    max_chunks: int = DEFAULT_MAX_CHUNKS

    def __post_init__(self) -> None:
        for name, value, maximum in (
            ("max_tree_requests", self.max_tree_requests, HARD_MAX_TREE_REQUESTS),
            ("max_entries", self.max_entries, HARD_MAX_ENTRIES),
            ("max_files", self.max_files, HARD_MAX_FILES),
            ("max_download_bytes", self.max_download_bytes, HARD_MAX_DOWNLOAD_BYTES),
            ("max_chunks", self.max_chunks, HARD_MAX_CHUNKS),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
                raise InvalidGitHubStagedSynchronizationRequest(
                    f"{name} must be between 1 and {maximum}"
                )


@dataclass(frozen=True)
class GitHubTraversalFrame:
    tree_path: str
    tree_object_id: str
    next_entry_index: int


@dataclass(frozen=True)
class GitHubRunBudget:
    entries_examined: int = 0
    supported_files: int = 0
    downloaded_bytes: int = 0
    prepared_chunks: int = 0


@dataclass(frozen=True)
class GitHubTraversalCursor:
    snapshot: GitHubRepositorySnapshot
    frames: tuple[GitHubTraversalFrame, ...]
    totals: GitHubRunBudget
    scan_complete: bool = False

    def __post_init__(self) -> None:
        _validate_cursor(self)

    @classmethod
    def initial(cls, snapshot: GitHubRepositorySnapshot) -> GitHubTraversalCursor:
        return cls(
            snapshot,
            (GitHubTraversalFrame("", snapshot.root_tree_object_id, 0),),
            GitHubRunBudget(),
        )

    def to_safe_json(self) -> dict[str, object]:
        value: dict[str, object] = {
            "schema_version": CURSOR_SCHEMA_VERSION,
            "snapshot": {
                "repository_id": self.snapshot.repository_id,
                "canonical_repository_identity": self.snapshot.canonical_repository_identity,
                "default_branch_name": self.snapshot.default_branch_name,
                "commit_object_id": self.snapshot.commit_object_id,
                "root_tree_object_id": self.snapshot.root_tree_object_id,
            },
            "frames": [
                {
                    "tree_path": frame.tree_path,
                    "tree_object_id": frame.tree_object_id,
                    "next_entry_index": frame.next_entry_index,
                }
                for frame in self.frames
            ],
            "totals": {
                "entries_examined": self.totals.entries_examined,
                "supported_files": self.totals.supported_files,
                "downloaded_bytes": self.totals.downloaded_bytes,
                "prepared_chunks": self.totals.prepared_chunks,
            },
            "scan_complete": self.scan_complete,
        }
        _require_cursor_size(value)
        return value

    @classmethod
    def from_safe_json(
        cls,
        value: object,
        *,
        connector_id: UUID,
        scope_id: UUID,
    ) -> GitHubTraversalCursor:
        if not isinstance(value, dict):
            raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor is invalid")
        _require_cursor_size(value)
        if set(value) != {"schema_version", "snapshot", "frames", "totals", "scan_complete"}:
            raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor fields are invalid")
        if value["schema_version"] != CURSOR_SCHEMA_VERSION:
            raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor version is unsupported")
        raw_snapshot = _exact_dict(
            value["snapshot"],
            {
                "repository_id",
                "canonical_repository_identity",
                "default_branch_name",
                "commit_object_id",
                "root_tree_object_id",
            },
        )
        repository_id = _positive_int("repository_id", raw_snapshot["repository_id"])
        canonical = raw_snapshot["canonical_repository_identity"]
        if canonical != f"github:repository:{repository_id}":
            raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor repository is invalid")
        snapshot = GitHubRepositorySnapshot(
            connector_id,
            scope_id,
            repository_id,
            _nonblank("canonical_repository_identity", canonical, maximum=255),
            _nonblank("default_branch_name", raw_snapshot["default_branch_name"], maximum=255),
            _object_id(raw_snapshot["commit_object_id"]),
            _object_id(raw_snapshot["root_tree_object_id"]),
        )
        raw_frames = value["frames"]
        if not isinstance(raw_frames, list) or len(raw_frames) > MAX_REPOSITORY_PATH_SEGMENTS:
            raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor frame stack is invalid")
        frames = tuple(
            GitHubTraversalFrame(
                _repository_path(frame["tree_path"], allow_empty=True),
                _object_id(frame["tree_object_id"]),
                _nonnegative_int("next_entry_index", frame["next_entry_index"]),
            )
            for frame in (
                _exact_dict(item, {"tree_path", "tree_object_id", "next_entry_index"})
                for item in raw_frames
            )
        )
        raw_totals = _exact_dict(
            value["totals"],
            {"entries_examined", "supported_files", "downloaded_bytes", "prepared_chunks"},
        )
        totals = GitHubRunBudget(
            _bounded_total("entries_examined", raw_totals["entries_examined"], MAX_RUN_ENTRIES),
            _bounded_total("supported_files", raw_totals["supported_files"], MAX_RUN_FILES),
            _bounded_total("downloaded_bytes", raw_totals["downloaded_bytes"], MAX_RUN_DOWNLOAD_BYTES),
            _bounded_total("prepared_chunks", raw_totals["prepared_chunks"], MAX_RUN_CHUNKS),
        )
        if not isinstance(value["scan_complete"], bool):
            raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor completion is invalid")
        return cls(snapshot, frames, totals, value["scan_complete"])


@dataclass(frozen=True, repr=False)
class GitHubSynchronizationSnapshot:
    authorization: GitHubRepositoryContentAuthorization
    sync_run_id: UUID
    run_started_at: datetime
    cursor: GitHubTraversalCursor | None
    profile: LocalDocumentIndexingProfile


@dataclass(frozen=True)
class GitHubDiscoveredFile:
    entry: GitHubRepositoryEntry
    cursor_before: GitHubTraversalCursor
    cursor_after: GitHubTraversalCursor
    skip_reason: str | None

    @property
    def source_item_key(self) -> str:
        return _source_identity(self.entry.repository_id, self.entry.path)


@dataclass(frozen=True)
class GitHubDiscoveryBatch:
    files: tuple[GitHubDiscoveredFile, ...]
    cursor_after: GitHubTraversalCursor
    tree_requests: int
    entries_examined: int


@dataclass(frozen=True)
class GitHubItemSnapshot:
    source_item_id: UUID | None
    persisted_blob_id: str | None
    persisted_checksum: str | None
    current_provider_version_id: str | None
    profile_complete: bool
    run_item_status: str | None
    run_item_blob_id: str | None


@dataclass(frozen=True, repr=False)
class PreparedGitHubChunk:
    chunk_index: int
    chunk_text: str
    content_hash: str
    embedding: tuple[float, ...]

    def __post_init__(self) -> None:
        if self.chunk_index < 0 or not self.chunk_text.strip():
            raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub chunk is invalid")
        _sha256(self.content_hash)
        if len(self.embedding) != 1536 or any(not math.isfinite(value) for value in self.embedding):
            raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub embedding is invalid")


@dataclass(frozen=True, repr=False)
class PreparedGitHubFile:
    discovered: GitHubDiscoveredFile
    expected_source_item_id: UUID | None
    expected_blob_id: str | None
    outcome: str
    content_checksum: str | None
    title: str | None
    mime_type: str | None
    chunks: tuple[PreparedGitHubChunk, ...]
    embedding_model: str | None

    def __post_init__(self) -> None:
        if self.outcome not in {"already_complete", "unchanged", "skipped", "indexed"}:
            raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub outcome is invalid")
        if self.outcome == "indexed":
            if (
                self.content_checksum is None
                or not self.chunks
                or len(self.chunks) > MAX_PREPARED_CHUNKS
                or self.embedding_model is None
            ):
                raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub file is invalid")
            _sha256(self.content_checksum)
        elif self.content_checksum is not None or self.chunks or self.embedding_model is not None:
            raise InvalidGitHubStagedSynchronizationRequest("unindexed GitHub file has content")


@dataclass(frozen=True)
class PreparedGitHubBatch:
    files: tuple[PreparedGitHubFile, ...]
    cursor_after: GitHubTraversalCursor
    downloaded_bytes: int
    prepared_chunks: int


@dataclass(frozen=True)
class GitHubPersistenceOutcome:
    outcome: str
    files_persisted: int
    scan_complete: bool


def classify_github_synchronization_failure(error: BaseException) -> FailureClassification:
    """Map staged GitHub failures to fixed safe retry-policy classifications."""
    if isinstance(error, GitHubRepositoryContentUnavailable):
        return classification_for(SyncFailureKind.RETRYABLE_PROVIDER)
    if isinstance(error, (GitHubRepositoryContentNotFound, GitHubRepositoryContentConflict)):
        return classification_for(SyncFailureKind.AUTHORIZATION)
    if isinstance(error, GitHubRepositoryContentRejected):
        return classification_for(SyncFailureKind.PERMANENT_PROVIDER)
    if isinstance(error, (InvalidGitHubStagedSynchronizationRequest, StalePreparedGitHubBatch)):
        return classification_for(SyncFailureKind.VALIDATION)
    if isinstance(error, (LostSyncJobLease, StaleSyncJobFence, SyncJobCancellationConflict)):
        return classification_for(SyncFailureKind.CANCELLED)
    return classify_exception(error)


class GitHubSynchronizationPreparationService:
    """Perform GitHub and content processing with no open database transaction."""

    def __init__(
        self,
        content_service: GitHubRepositoryContentService,
        extractor_registry: ContentExtractorRegistry,
        content_chunker: ContentChunker,
        embedding_provider: EmbeddingProvider,
    ) -> None:
        self._content = content_service
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
        values = {
            "extraction_profile": "content_extraction",
            "extraction_version": _profile_hash(extractor_signature),
            "chunking_profile": _normalized_profile_name(chunker_type.__name__),
            "chunking_version": _profile_hash(
                {
                    "implementation": f"{chunker_type.__module__}.{chunker_type.__qualname__}",
                    "config": {"max_chunk_size": 2000, "overlap": 200, "minimum_preferred_size": 200},
                }
            ),
            "embedding_provider": self._embedding_provider.profile.provider_name,
            "embedding_model": self._embedding_provider.profile.model_identifier,
            "embedding_dimensions": self._embedding_provider.profile.dimension,
        }
        fingerprint = hashlib.sha256(
            json.dumps(values, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return LocalDocumentIndexingProfile(**values, fingerprint=fingerprint)

    def resolve_snapshot(
        self, authorization: GitHubRepositoryContentAuthorization
    ) -> GitHubTraversalCursor:
        return GitHubTraversalCursor.initial(
            self._content.resolve_default_branch_snapshot(authorization)
        )

    def discover_batch(
        self,
        authorization: GitHubRepositoryContentAuthorization,
        cursor: GitHubTraversalCursor,
        *,
        limits: GitHubSynchronizationLimits = GitHubSynchronizationLimits(),
    ) -> GitHubDiscoveryBatch:
        _validate_authorization_cursor(authorization, cursor)
        if cursor.scan_complete:
            return GitHubDiscoveryBatch((), cursor, 0, 0)
        work = cursor
        files: list[GitHubDiscoveredFile] = []
        pages: dict[tuple[str, str], tuple[GitHubRepositoryEntry, ...]] = {}
        tree_requests = 0
        entries_examined = 0
        declared_bytes = 0
        while work.frames and entries_examined < limits.max_entries:
            frame = work.frames[-1]
            cache_key = (frame.tree_path, frame.tree_object_id)
            entries = pages.get(cache_key)
            if entries is None:
                if tree_requests >= limits.max_tree_requests:
                    break
                descriptor = _tree_descriptor(work.snapshot, frame)
                page = self._content.list_tree(authorization, work.snapshot, descriptor)
                if page.tree != descriptor:
                    raise InvalidGitHubStagedSynchronizationRequest("GitHub tree response changed")
                entries = page.entries
                pages[cache_key] = entries
                tree_requests += 1
            if frame.next_entry_index > len(entries):
                raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor offset is invalid")
            if frame.next_entry_index == len(entries):
                remaining_frames = work.frames[:-1]
                work = replace(
                    work,
                    frames=remaining_frames,
                    scan_complete=not remaining_frames,
                )
                continue
            before = work
            entry = entries[frame.next_entry_index]
            advanced_frame = replace(frame, next_entry_index=frame.next_entry_index + 1)
            totals = replace(work.totals, entries_examined=work.totals.entries_examined + 1)
            if totals.entries_examined > MAX_RUN_ENTRIES:
                raise GitHubSynchronizationBudgetExceeded("GitHub run entry budget was exceeded")
            work = replace(work, frames=work.frames[:-1] + (advanced_frame,), totals=totals)
            entries_examined += 1
            if entry.entry_type == "tree":
                if len(work.frames) >= MAX_REPOSITORY_PATH_SEGMENTS:
                    raise GitHubSynchronizationBudgetExceeded("GitHub repository depth was exceeded")
                work = replace(
                    work,
                    frames=work.frames + (GitHubTraversalFrame(entry.path, entry.object_id, 0),),
                )
                continue
            extension = PurePosixPath(entry.path).suffix.casefold()
            if extension not in SUPPORTED_CONTENT_EXTENSIONS:
                continue
            skip_reason = None
            if len(_source_identity(entry.repository_id, entry.path)) > MAX_SOURCE_IDENTITY_CHARACTERS:
                skip_reason = "source_identity_too_long"
            elif entry.size_bytes is None:
                skip_reason = "missing_size"
            elif entry.size_bytes > MAX_GITHUB_BLOB_BYTES:
                skip_reason = "file_too_large"
            if len(files) >= limits.max_files:
                work = before
                break
            size = entry.size_bytes or 0
            if skip_reason is None and declared_bytes + size > limits.max_download_bytes:
                work = before
                break
            after = work
            files.append(GitHubDiscoveredFile(entry, before, after, skip_reason))
            declared_bytes += size if skip_reason is None else 0
        if not work.frames:
            work = replace(work, scan_complete=True)
        return GitHubDiscoveryBatch(tuple(files), work, tree_requests, entries_examined)

    def prepare_batch(
        self,
        authorization: GitHubRepositoryContentAuthorization,
        item_snapshots: tuple[GitHubItemSnapshot, ...],
        batch: GitHubDiscoveryBatch,
        *,
        limits: GitHubSynchronizationLimits = GitHubSynchronizationLimits(),
    ) -> PreparedGitHubBatch:
        if len(item_snapshots) != len(batch.files):
            raise InvalidGitHubStagedSynchronizationRequest("GitHub item snapshots are incomplete")
        prepared: list[PreparedGitHubFile] = []
        downloaded_bytes = 0
        prepared_chunks = 0
        cursor_after = batch.cursor_after if not batch.files else batch.files[0].cursor_before
        for discovered, snapshot in zip(batch.files, item_snapshots, strict=True):
            _validate_discovered_file(discovered, batch.cursor_after.snapshot)
            if discovered.cursor_before.totals.entries_examined < cursor_after.totals.entries_examined:
                raise InvalidGitHubStagedSynchronizationRequest("GitHub batch order is invalid")
            if discovered.skip_reason is not None:
                item = _prepared_without_content(discovered, snapshot, "skipped")
            elif (
                snapshot.run_item_status in {"succeeded", "skipped"}
                and snapshot.run_item_blob_id == discovered.entry.object_id
            ):
                item = _prepared_without_content(discovered, snapshot, "already_complete")
            elif (
                snapshot.persisted_blob_id == discovered.entry.object_id
                and snapshot.current_provider_version_id == discovered.entry.object_id
                and snapshot.profile_complete
            ):
                item = _prepared_without_content(discovered, snapshot, "unchanged")
            else:
                declared = discovered.entry.size_bytes
                if declared is None:
                    raise InvalidGitHubStagedSynchronizationRequest("GitHub file size is invalid")
                if downloaded_bytes + declared > limits.max_download_bytes and prepared:
                    break
                item = self._prepare_changed(authorization, discovered, snapshot)
                next_chunks = prepared_chunks + len(item.chunks)
                if next_chunks > limits.max_chunks:
                    if prepared:
                        break
                    raise GitHubSynchronizationBudgetExceeded("GitHub file chunk budget was exceeded")
                downloaded_bytes += declared
                prepared_chunks = next_chunks
            prepared.append(item)
            cursor_after = discovered.cursor_after
        position = batch.cursor_after if len(prepared) == len(batch.files) else cursor_after
        totals = position.totals
        next_totals = GitHubRunBudget(
            totals.entries_examined,
            totals.supported_files + len(prepared),
            totals.downloaded_bytes + downloaded_bytes,
            totals.prepared_chunks + prepared_chunks,
        )
        _enforce_run_totals(next_totals)
        cursor_after = replace(position, totals=next_totals)
        return PreparedGitHubBatch(tuple(prepared), cursor_after, downloaded_bytes, prepared_chunks)

    def _prepare_changed(
        self,
        authorization: GitHubRepositoryContentAuthorization,
        discovered: GitHubDiscoveredFile,
        item_snapshot: GitHubItemSnapshot,
    ) -> PreparedGitHubFile:
        raw = self._content.download_blob(authorization, discovered.cursor_after.snapshot, discovered.entry)
        extension = PurePosixPath(discovered.entry.path).suffix.casefold()
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(suffix=extension, delete=False) as handle:
                handle.write(raw.content)
                temporary_path = Path(handle.name)
            extracted = self._extractors.extract(temporary_path)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        chunk_results = self._chunker.chunk(extracted.text)
        if not chunk_results or len(chunk_results) > MAX_PREPARED_CHUNKS:
            raise GitHubSynchronizationBudgetExceeded("GitHub file chunk budget was exceeded")
        profile = self._embedding_provider.profile
        chunks: list[PreparedGitHubChunk] = []
        batch_size = profile.max_batch_size or len(chunk_results)
        for start in range(0, len(chunk_results), batch_size):
            group = chunk_results[start : start + batch_size]
            requests = tuple(
                EmbeddingRequest(input_index=index, text=chunk.content)
                for index, chunk in enumerate(group)
            )
            results = validate_embedding_results(
                requests, self._embedding_provider.embed_batch(requests), profile
            )
            chunks.extend(
                PreparedGitHubChunk(
                    chunk.chunk_index,
                    chunk.content,
                    chunk.content_checksum,
                    result.vector,
                )
                for chunk, result in zip(group, results, strict=True)
            )
        return PreparedGitHubFile(
            discovered,
            item_snapshot.source_item_id,
            item_snapshot.persisted_blob_id,
            "indexed",
            raw.sha256,
            extracted.title or PurePosixPath(discovered.entry.path).name,
            extracted.mime_type or _MIME_TYPES[extension],
            tuple(chunks),
            profile.model_identifier,
        )


class GitHubStagedSynchronizationService:
    """Load and persist GitHub sync state in short caller-owned transactions."""

    def __init__(
        self,
        session: Session,
        execution_service: ConnectorSyncExecutionService,
        content_service: GitHubRepositoryContentService,
        profile: LocalDocumentIndexingProfile,
    ) -> None:
        self._session = session
        self._execution = execution_service
        self._content = content_service
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
        self, lease: SyncJobLease, sync_run_id: UUID, *, worker_id: str
    ) -> GitHubSynchronizationSnapshot:
        self._execution.validate_attempt(lease, sync_run_id, worker_id=worker_id)
        run = self._session.execute(
            select(ConnectorSyncRun).where(
                ConnectorSyncRun.organization_id == lease.organization_id,
                ConnectorSyncRun.connector_id == lease.connector_id,
                ConnectorSyncRun.connector_scope_id == lease.connector_scope_id,
                ConnectorSyncRun.id == sync_run_id,
                ConnectorSyncRun.status == "running",
            )
        ).scalar_one_or_none()
        if run is None:
            raise InvalidGitHubStagedSynchronizationRequest("GitHub synchronization run is unavailable")
        authorization = self._content.authorize(
            lease.organization_id, lease.connector_id, lease.connector_scope_id
        )
        active = self._sync.get_active_cursor(
            lease.organization_id, lease.connector_id, lease.connector_scope_id
        )
        cursor = None
        if active is not None and active.created_by_run_id == run.id:
            if active.cursor_type != CURSOR_TYPE or active.safe_cursor is None:
                raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor is invalid")
            cursor = GitHubTraversalCursor.from_safe_json(
                active.safe_cursor,
                connector_id=lease.connector_id,
                scope_id=lease.connector_scope_id,
            )
            _validate_authorization_cursor(authorization, cursor)
        return GitHubSynchronizationSnapshot(
            authorization, run.id, run.started_at, cursor, self._profile
        )

    def item_snapshots(
        self,
        lease: SyncJobLease,
        snapshot: GitHubSynchronizationSnapshot,
        batch: GitHubDiscoveryBatch,
        *,
        worker_id: str,
    ) -> tuple[GitHubItemSnapshot, ...]:
        self._execution.validate_attempt(lease, snapshot.sync_run_id, worker_id=worker_id)
        self._require_context(snapshot)
        results: list[GitHubItemSnapshot] = []
        for discovered in batch.files:
            source = self._sources.get_by_key(
                lease.organization_id, lease.connector_id, discovered.source_item_key
            )
            current = self._versions.get_current(lease.organization_id, source.id) if source else None
            state = (
                self._indexing.get_state(
                    lease.organization_id, current.id, snapshot.profile.fingerprint
                )
                if current
                else None
            )
            item = self._sync.get_item_by_key(
                lease.organization_id,
                lease.connector_id,
                snapshot.sync_run_id,
                discovered.source_item_key,
            )
            results.append(
                GitHubItemSnapshot(
                    source.id if source else None,
                    source.source_version if source else None,
                    source.source_checksum if source else None,
                    current.provider_version_id if current else None,
                    bool(
                        state
                        and state.status == "indexed"
                        and state.indexed_generation == state.desired_generation
                    ),
                    item.processing_status if item else None,
                    item.current_checksum if item else None,
                )
            )
        return tuple(results)

    def pin_snapshot(
        self,
        lease: SyncJobLease,
        snapshot: GitHubSynchronizationSnapshot,
        cursor: GitHubTraversalCursor,
        *,
        worker_id: str,
        now: datetime,
    ) -> None:
        self._execution.validate_attempt(lease, snapshot.sync_run_id, worker_id=worker_id)
        self._require_context(snapshot)
        if snapshot.cursor is not None:
            if snapshot.cursor != cursor:
                raise StalePreparedGitHubBatch("GitHub snapshot was already pinned")
            return
        _validate_authorization_cursor(snapshot.authorization, cursor)
        if cursor != GitHubTraversalCursor.initial(cursor.snapshot):
            raise InvalidGitHubStagedSynchronizationRequest(
                "new GitHub snapshot cursor must be at the traversal root"
            )
        current = self._sync.get_active_cursor(
            lease.organization_id, lease.connector_id, lease.connector_scope_id, lock=True
        )
        if current is not None and current.created_by_run_id == snapshot.sync_run_id:
            raise StalePreparedGitHubBatch("GitHub snapshot was concurrently pinned")
        self._replace_cursor(snapshot, cursor, current, now)

    def persist_batch(
        self,
        lease: SyncJobLease,
        snapshot: GitHubSynchronizationSnapshot,
        prepared: PreparedGitHubBatch,
        *,
        worker_id: str,
        now: datetime,
    ) -> GitHubPersistenceOutcome:
        self._execution.validate_attempt(lease, snapshot.sync_run_id, worker_id=worker_id)
        self._require_context(snapshot)
        _validate_authorization_cursor(snapshot.authorization, prepared.cursor_after)
        current_row = self._sync.get_active_cursor(
            lease.organization_id, lease.connector_id, lease.connector_scope_id, lock=True
        )
        if (
            current_row is None
            or current_row.created_by_run_id != snapshot.sync_run_id
            or current_row.cursor_type != CURSOR_TYPE
            or current_row.safe_cursor is None
        ):
            raise StalePreparedGitHubBatch("GitHub synchronization cursor is unavailable")
        current = GitHubTraversalCursor.from_safe_json(
            current_row.safe_cursor,
            connector_id=lease.connector_id,
            scope_id=lease.connector_scope_id,
        )
        _require_forward_progress(current, prepared.cursor_after, bool(prepared.files))
        _validate_prepared_batch(current, prepared)
        for item in prepared.files:
            self._persist_file(snapshot, item, now)
        self._replace_cursor(snapshot, prepared.cursor_after, current_row, now)
        if prepared.cursor_after.scan_complete:
            run = self._sync.get_run(
                lease.organization_id,
                lease.connector_id,
                lease.connector_scope_id,
                snapshot.sync_run_id,
            )
            if run is None or run.status != "running" or run.started_at is None:
                raise StalePreparedGitHubBatch("GitHub synchronization run is unavailable")
            self._sync.set_run_state(
                lease.organization_id,
                lease.connector_id,
                lease.connector_scope_id,
                snapshot.sync_run_id,
                status="completed",
                started_at=run.started_at,
                heartbeat_at=now,
                finished_at=now,
            )
            self._execution.complete_success(lease, worker_id=worker_id)
        return GitHubPersistenceOutcome(
            "completed" if prepared.cursor_after.scan_complete else "in_progress",
            len(prepared.files),
            prepared.cursor_after.scan_complete,
        )

    def _persist_file(
        self,
        snapshot: GitHubSynchronizationSnapshot,
        prepared: PreparedGitHubFile,
        now: datetime,
    ) -> None:
        discovered = prepared.discovered
        entry = discovered.entry
        source = self._sources.lock_by_key(
            snapshot.authorization.organization_id,
            snapshot.authorization.connector_id,
            discovered.source_item_key,
        )
        if source is None and prepared.expected_source_item_id is not None:
            raise StalePreparedGitHubBatch("GitHub source identity disappeared")
        prior = self._sync.get_item_by_key(
            snapshot.authorization.organization_id,
            snapshot.authorization.connector_id,
            snapshot.sync_run_id,
            discovered.source_item_key,
        )
        if prior and prior.processing_status in {"succeeded", "skipped"}:
            if prior.current_checksum != entry.object_id:
                raise StalePreparedGitHubBatch("completed GitHub item revision changed")
            return
        if source is not None:
            if prepared.expected_source_item_id not in {None, source.id}:
                raise StalePreparedGitHubBatch("GitHub source identity changed")
            if source.source_version not in {prepared.expected_blob_id, entry.object_id}:
                raise StalePreparedGitHubBatch("persisted GitHub source revision changed")
        source, restored = self._persist_source(snapshot, prepared, source, now)
        self._persist_membership(snapshot, source.id, now)
        item = prior or ConnectorSyncItem(
            id=uuid4(),
            organization_id=snapshot.authorization.organization_id,
            connector_id=snapshot.authorization.connector_id,
            connector_scope_id=snapshot.authorization.scope_id,
            sync_run_id=snapshot.sync_run_id,
            source_item_id=source.id,
            source_item_key=discovered.source_item_key,
            change_type=(
                "new"
                if prepared.expected_source_item_id is None
                else "unchanged"
                if prepared.outcome in {"already_complete", "unchanged", "skipped"}
                else "changed"
            ),
            processing_status="pending",
            previous_checksum=prepared.expected_blob_id,
            current_checksum=entry.object_id,
            attempt_count=0,
        )
        if prior is None:
            self._sync.add_item(
                snapshot.authorization.organization_id,
                snapshot.authorization.connector_id,
                snapshot.authorization.scope_id,
                snapshot.sync_run_id,
                item,
            )
        if prepared.outcome != "indexed":
            self._finish_item(snapshot, item, "skipped", now)
            deltas = {"items_discovered": 1, "items_skipped": 1}
            if prepared.outcome in {"already_complete", "unchanged"}:
                deltas["items_unchanged"] = 1
            self._sync.increment_counters(
                snapshot.authorization.organization_id,
                snapshot.authorization.connector_id,
                snapshot.authorization.scope_id,
                snapshot.sync_run_id,
                **deltas,
            )
            return
        self._persist_indexed(snapshot, source, item, prepared, restored, now)

    def _persist_source(self, snapshot, prepared, existing: SourceItem | None, now):
        entry = prepared.discovered.entry
        metadata = {
            "provider": "github",
            "repository_id": entry.repository_id,
            "repository_identity": entry.canonical_repository_identity,
            "repository_path": entry.path,
            "blob_object_id": entry.object_id,
            "snapshot_commit_id": entry.commit_object_id,
            "file_extension": PurePosixPath(entry.path).suffix.casefold(),
            "size_bytes": entry.size_bytes,
        }
        checksum = prepared.content_checksum or (existing.source_checksum if existing else None)
        if existing is None:
            source = SourceItem(
                id=uuid4(),
                organization_id=snapshot.authorization.organization_id,
                connector_id=snapshot.authorization.connector_id,
                source_item_key=prepared.discovered.source_item_key,
                parent_source_item_key=None,
                source_item_type="file",
                title=PurePosixPath(entry.path).name,
                source_url=None,
                mime_type=_MIME_TYPES[PurePosixPath(entry.path).suffix.casefold()],
                source_checksum=checksum,
                source_version=entry.object_id,
                size_bytes=entry.size_bytes,
                source_created_at=None,
                source_modified_at=None,
                first_seen_at=now,
                last_seen_at=now,
                status="active",
                source_metadata=metadata,
                metadata_schema_version=1,
            )
            return self._sources.add(
                snapshot.authorization.organization_id,
                snapshot.authorization.connector_id,
                source,
            ), False
        restored = existing.status != "active"
        source = self._sources.update_provider_state(
            snapshot.authorization.organization_id,
            snapshot.authorization.connector_id,
            existing.id,
            source_metadata=metadata,
            metadata_schema_version=1,
            last_seen_at=now,
            source_checksum=checksum,
            source_version=entry.object_id,
            size_bytes=entry.size_bytes,
        )
        if source is None:
            raise StalePreparedGitHubBatch("GitHub source could not be updated")
        if restored:
            self._sources.set_lifecycle(
                snapshot.authorization.organization_id,
                snapshot.authorization.connector_id,
                source.id,
                "active",
            )
        return source, restored

    def _persist_membership(self, snapshot, source_id, now):
        membership = self._sources.lock_membership(
            snapshot.authorization.organization_id,
            snapshot.authorization.connector_id,
            snapshot.authorization.scope_id,
            source_id,
        )
        if membership is None:
            self._sources.add_membership(
                snapshot.authorization.organization_id,
                snapshot.authorization.connector_id,
                SourceItemScopeMembership(
                    id=uuid4(),
                    organization_id=snapshot.authorization.organization_id,
                    connector_id=snapshot.authorization.connector_id,
                    source_item_id=source_id,
                    connector_scope_id=snapshot.authorization.scope_id,
                    status="active",
                    first_discovered_at=now,
                    last_seen_at=now,
                ),
            )
        else:
            self._sources.reactivate_membership(
                snapshot.authorization.organization_id,
                snapshot.authorization.connector_id,
                snapshot.authorization.scope_id,
                source_id,
                now,
            )

    def _persist_indexed(self, snapshot, source, item, prepared, restored, now):
        entry = prepared.discovered.entry
        current = self._versions.get_current(snapshot.authorization.organization_id, source.id)
        if current is None or current.provider_version_id != entry.object_id:
            current = self._versions.create_current_version(
                snapshot.authorization.organization_id,
                source.id,
                version_cause=(
                    "discovered"
                    if prepared.expected_source_item_id is None
                    else "restored"
                    if restored
                    else "content_changed"
                ),
                lifecycle="available",
                discovered_at=now,
                provider_version_id=entry.object_id,
                content_checksum=prepared.content_checksum,
                checksum_algorithm="sha256",
                source_size_bytes=entry.size_bytes,
                content_type=prepared.mime_type,
                file_extension=PurePosixPath(entry.path).suffix.casefold(),
                metadata={
                    "provider": "github",
                    "repository_id": entry.repository_id,
                    "commit_object_id": entry.commit_object_id,
                    "blob_object_id": entry.object_id,
                },
            )
        state = self._indexing.get_or_create_state(
            snapshot.authorization.organization_id,
            current.id,
            DocumentIndexingState(
                id=uuid4(),
                organization_id=snapshot.authorization.organization_id,
                document_version_id=current.id,
                extraction_profile=snapshot.profile.extraction_profile,
                extraction_version=snapshot.profile.extraction_version,
                chunking_profile=snapshot.profile.chunking_profile,
                chunking_version=snapshot.profile.chunking_version,
                embedding_provider=snapshot.profile.embedding_provider,
                embedding_model=snapshot.profile.embedding_model,
                embedding_dimensions=snapshot.profile.embedding_dimensions,
                profile_fingerprint=snapshot.profile.fingerprint,
                desired_generation=1,
                indexed_generation=None,
                status="pending",
                reason="new_version" if prepared.expected_source_item_id is None else "content_changed",
                attempt_count=0,
                requested_at=now,
            ),
        )
        if state.status == "indexed" and state.indexed_generation == state.desired_generation:
            self._finish_item(snapshot, item, "skipped", now)
            self._sync.increment_counters(
                snapshot.authorization.organization_id,
                snapshot.authorization.connector_id,
                snapshot.authorization.scope_id,
                snapshot.sync_run_id,
                items_discovered=1,
                items_unchanged=1,
                items_skipped=1,
            )
            return
        attempt = self._indexing.allocate_attempt(
            snapshot.authorization.organization_id,
            current.id,
            snapshot.profile.fingerprint,
            trigger_type="sync",
            started_at=now,
            sync_run_id=snapshot.sync_run_id,
            sync_item_id=item.id,
        )
        document = self._documents.get_by_source_identity(
            snapshot.authorization.organization_id,
            "github",
            prepared.discovered.source_item_key,
        )
        if document is None:
            document = Document(
                id=uuid4(),
                organization_id=snapshot.authorization.organization_id,
                source_type="github",
                source_document_key=prepared.discovered.source_item_key,
                title=prepared.title or PurePosixPath(entry.path).name,
                source_url=None,
                mime_type=prepared.mime_type,
                checksum_latest=prepared.content_checksum,
                status="ready",
                source_created_at=None,
                source_updated_at=None,
            )
            self._documents.add(snapshot.authorization.organization_id, document)
            self._session.flush()
        else:
            if document.deleted_at is not None:
                self._documents.restore(snapshot.authorization.organization_id, document.id)
            self._documents.update(
                snapshot.authorization.organization_id,
                document.id,
                title=prepared.title,
                mime_type=prepared.mime_type,
                checksum_latest=prepared.content_checksum,
                status="ready",
            )
        chunks = [
            DocumentChunk(
                id=uuid4(),
                organization_id=snapshot.authorization.organization_id,
                document_id=document.id,
                chunk_index=chunk.chunk_index,
                chunk_text=chunk.chunk_text,
                token_count=None,
                content_hash=chunk.content_hash,
                embedding=list(chunk.embedding),
                embedding_model=prepared.embedding_model,
            )
            for chunk in prepared.chunks
        ]
        self._chunks.replace_for_document(
            snapshot.authorization.organization_id, document.id, chunks
        )
        self._session.flush()
        self._indexing.complete_attempt(
            snapshot.authorization.organization_id,
            state.id,
            attempt.id,
            status="succeeded",
            completed_at=now,
            retryable=False,
            summary={"chunks_indexed": len(chunks)},
        )
        self._indexing.persist_controlled_state(
            snapshot.authorization.organization_id,
            current.id,
            snapshot.profile.fingerprint,
            status="indexed",
            desired_generation=state.desired_generation,
            indexed_generation=state.desired_generation,
            attempt_count=state.attempt_count,
            requested_at=state.requested_at,
            started_at=state.started_at,
            completed_at=now,
            last_attempt_at=state.last_attempt_at,
        )
        self._versions.replace_materialization(
            snapshot.authorization.organization_id, source.id, current.id, document.id
        )
        self._finish_item(snapshot, item, "succeeded", now)
        counter = "items_new" if prepared.expected_source_item_id is None else "items_changed"
        self._sync.increment_counters(
            snapshot.authorization.organization_id,
            snapshot.authorization.connector_id,
            snapshot.authorization.scope_id,
            snapshot.sync_run_id,
            items_discovered=1,
            items_succeeded=1,
            **{counter: 1},
        )

    def _finish_item(self, snapshot, item, status, now):
        result = self._sync.set_item_state(
            snapshot.authorization.organization_id,
            snapshot.authorization.connector_id,
            snapshot.sync_run_id,
            item.id,
            status=status,
            attempt_count=item.attempt_count,
            started_at=item.started_at or now,
            finished_at=now,
            source_item_id=item.source_item_id,
        )
        if result is None:
            raise StalePreparedGitHubBatch("GitHub sync item could not be completed")

    def _require_context(self, snapshot: GitHubSynchronizationSnapshot) -> None:
        current = self._content.authorize(
            snapshot.authorization.organization_id,
            snapshot.authorization.connector_id,
            snapshot.authorization.scope_id,
        )
        if current != snapshot.authorization:
            raise StalePreparedGitHubBatch("GitHub synchronization authorization changed")
        connector = self._connectors.get_by_id(
            snapshot.authorization.organization_id, snapshot.authorization.connector_id
        )
        scope = self._scopes.get_by_id(
            snapshot.authorization.organization_id, snapshot.authorization.scope_id
        )
        if (
            connector is None
            or connector.connector_type != "github"
            or connector.status != "active"
            or scope is None
            or scope.connector_id != connector.id
            or scope.status != "active"
        ):
            raise InvalidGitHubStagedSynchronizationRequest("GitHub synchronization context is unavailable")

    def _replace_cursor(self, snapshot, cursor, current, now):
        self._sync.replace_active_cursor(
            snapshot.authorization.organization_id,
            snapshot.authorization.connector_id,
            snapshot.authorization.scope_id,
            snapshot.sync_run_id,
            version=(current.cursor_version + 1 if current else 1),
            cursor_type=CURSOR_TYPE,
            activated_at=now,
            safe_cursor=cursor.to_safe_json(),
        )


def _prepared_without_content(discovered, snapshot, outcome):
    return PreparedGitHubFile(
        discovered,
        snapshot.source_item_id,
        snapshot.persisted_blob_id,
        outcome,
        None,
        None,
        None,
        (),
        None,
    )


def _tree_descriptor(snapshot, frame):
    return GitHubTreeDescriptor(
        snapshot.connector_id,
        snapshot.scope_id,
        snapshot.repository_id,
        snapshot.canonical_repository_identity,
        snapshot.commit_object_id,
        snapshot.root_tree_object_id,
        frame.tree_path,
        frame.tree_object_id,
    )


def _validate_authorization_cursor(authorization, cursor):
    if (
        not isinstance(authorization, GitHubRepositoryContentAuthorization)
        or cursor.snapshot.connector_id != authorization.connector_id
        or cursor.snapshot.scope_id != authorization.scope_id
        or cursor.snapshot.repository_id != authorization.repository_id
        or cursor.snapshot.canonical_repository_identity
        != authorization.canonical_repository_identity
        or cursor.snapshot.default_branch_name != authorization.default_branch_name
    ):
        raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor authorization is invalid")


def _validate_cursor(cursor):
    snapshot = cursor.snapshot
    if not isinstance(snapshot, GitHubRepositorySnapshot):
        raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor snapshot is invalid")
    _object_id(snapshot.commit_object_id)
    _object_id(snapshot.root_tree_object_id)
    if snapshot.canonical_repository_identity != f"github:repository:{snapshot.repository_id}":
        raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor repository is invalid")
    if cursor.scan_complete != (len(cursor.frames) == 0):
        raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor completion is invalid")
    if len(cursor.frames) > MAX_REPOSITORY_PATH_SEGMENTS:
        raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor depth is invalid")
    for index, frame in enumerate(cursor.frames):
        _repository_path(frame.tree_path, allow_empty=index == 0)
        _object_id(frame.tree_object_id)
        _nonnegative_int("next_entry_index", frame.next_entry_index)
        if index == 0 and (
            frame.tree_path != "" or frame.tree_object_id != snapshot.root_tree_object_id
        ):
            raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor root frame is invalid")
        if index:
            parent = PurePosixPath(frame.tree_path).parent
            expected_parent = cursor.frames[index - 1].tree_path
            if ("" if str(parent) == "." else str(parent)) != expected_parent:
                raise InvalidGitHubStagedSynchronizationRequest(
                    "GitHub cursor frame ancestry is invalid"
                )
    _enforce_run_totals(cursor.totals)


def _require_forward_progress(current, target, has_files):
    if current.snapshot != target.snapshot or current.scan_complete:
        raise StalePreparedGitHubBatch("GitHub synchronization cursor is stale")
    if target.totals.entries_examined < current.totals.entries_examined:
        raise StalePreparedGitHubBatch("GitHub synchronization cursor moved backward")
    if has_files and target.totals.supported_files <= current.totals.supported_files:
        raise StalePreparedGitHubBatch("GitHub synchronization file progress is stale")
    for new, old in (
        (target.totals.supported_files, current.totals.supported_files),
        (target.totals.downloaded_bytes, current.totals.downloaded_bytes),
        (target.totals.prepared_chunks, current.totals.prepared_chunks),
    ):
        if new < old:
            raise StalePreparedGitHubBatch("GitHub synchronization budget moved backward")
    if target == current:
        raise StalePreparedGitHubBatch("GitHub synchronization made no progress")


def _validate_prepared_batch(current, prepared):
    if not isinstance(prepared, PreparedGitHubBatch) or len(prepared.files) > HARD_MAX_FILES:
        raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub batch is invalid")
    if (
        isinstance(prepared.downloaded_bytes, bool)
        or not isinstance(prepared.downloaded_bytes, int)
        or prepared.downloaded_bytes < 0
        or prepared.downloaded_bytes > HARD_MAX_DOWNLOAD_BYTES
        or isinstance(prepared.prepared_chunks, bool)
        or not isinstance(prepared.prepared_chunks, int)
        or prepared.prepared_chunks < 0
        or prepared.prepared_chunks > HARD_MAX_CHUNKS
    ):
        raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub batch costs are invalid")
    target = prepared.cursor_after
    if target.totals.supported_files - current.totals.supported_files != len(prepared.files):
        raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub file total is invalid")
    if target.totals.downloaded_bytes - current.totals.downloaded_bytes != prepared.downloaded_bytes:
        raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub byte total is invalid")
    if target.totals.prepared_chunks - current.totals.prepared_chunks != prepared.prepared_chunks:
        raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub chunk total is invalid")
    calculated_bytes = 0
    calculated_chunks = 0
    prior_entries = current.totals.entries_examined
    for item in prepared.files:
        if not isinstance(item, PreparedGitHubFile):
            raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub file is invalid")
        discovered = item.discovered
        _validate_discovered_file(discovered, current.snapshot)
        if discovered.cursor_before.totals.entries_examined < prior_entries:
            raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub order is invalid")
        prior_entries = discovered.cursor_after.totals.entries_examined
        if item.outcome == "indexed":
            if discovered.skip_reason is not None or discovered.entry.size_bytes is None:
                raise InvalidGitHubStagedSynchronizationRequest("indexed GitHub file is ineligible")
            calculated_bytes += discovered.entry.size_bytes
            calculated_chunks += len(item.chunks)
        elif (item.outcome == "skipped") != (discovered.skip_reason is not None):
            raise InvalidGitHubStagedSynchronizationRequest(
                "prepared GitHub skip outcome is invalid"
            )
    if calculated_bytes != prepared.downloaded_bytes or calculated_chunks != prepared.prepared_chunks:
        raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub costs do not match files")
    if prior_entries > target.totals.entries_examined:
        raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub cursor skipped backward")


def _validate_discovered_file(discovered, snapshot):
    if (
        not isinstance(discovered, GitHubDiscoveredFile)
        or discovered.entry.entry_type != "regular_blob"
        or discovered.cursor_before.snapshot != snapshot
        or discovered.cursor_after.snapshot != snapshot
        or discovered.cursor_after.totals.entries_examined
        != discovered.cursor_before.totals.entries_examined + 1
        or discovered.entry.connector_id != snapshot.connector_id
        or discovered.entry.scope_id != snapshot.scope_id
        or discovered.entry.repository_id != snapshot.repository_id
        or discovered.entry.canonical_repository_identity != snapshot.canonical_repository_identity
        or discovered.entry.commit_object_id != snapshot.commit_object_id
        or discovered.entry.root_tree_object_id != snapshot.root_tree_object_id
        or PurePosixPath(discovered.entry.path).suffix.casefold()
        not in SUPPORTED_CONTENT_EXTENSIONS
    ):
        raise InvalidGitHubStagedSynchronizationRequest("discovered GitHub file is invalid")
    entry = discovered.entry
    _object_id(entry.parent_tree_object_id)
    _object_id(entry.object_id)
    _repository_path(entry.path, allow_empty=False)
    if (
        not isinstance(entry.name, str)
        or not entry.name
        or "/" in entry.name
        or not (entry.path == entry.name or entry.path.endswith(f"/{entry.name}"))
        or entry.executable not in {True, False}
        or (
            entry.size_bytes is not None
            and (
                isinstance(entry.size_bytes, bool)
                or not isinstance(entry.size_bytes, int)
                or entry.size_bytes < 0
            )
        )
        or discovered.skip_reason
        not in {None, "source_identity_too_long", "missing_size", "file_too_large"}
    ):
        raise InvalidGitHubStagedSynchronizationRequest("discovered GitHub descriptor is invalid")
    expected_reason = None
    if len(_source_identity(entry.repository_id, entry.path)) > MAX_SOURCE_IDENTITY_CHARACTERS:
        expected_reason = "source_identity_too_long"
    elif entry.size_bytes is None:
        expected_reason = "missing_size"
    elif entry.size_bytes > MAX_GITHUB_BLOB_BYTES:
        expected_reason = "file_too_large"
    if discovered.skip_reason != expected_reason:
        raise InvalidGitHubStagedSynchronizationRequest("discovered GitHub eligibility is invalid")


def _enforce_run_totals(totals):
    for name, value, maximum in (
        ("entries", totals.entries_examined, MAX_RUN_ENTRIES),
        ("files", totals.supported_files, MAX_RUN_FILES),
        ("download bytes", totals.downloaded_bytes, MAX_RUN_DOWNLOAD_BYTES),
        ("chunks", totals.prepared_chunks, MAX_RUN_CHUNKS),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise InvalidGitHubStagedSynchronizationRequest("GitHub run totals are invalid")
        if value > maximum:
            raise GitHubSynchronizationBudgetExceeded(f"GitHub run {name} budget was exceeded")


def _source_identity(repository_id, path):
    return f"github:repository:{repository_id}:path:{path}"


def _require_cursor_size(value):
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor is not finite JSON") from exc
    if len(encoded) > MAX_CURSOR_BYTES:
        raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor is too large")


def _exact_dict(value, keys):
    if not isinstance(value, dict) or set(value) != keys:
        raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor fields are invalid")
    return value


def _positive_int(name, value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise InvalidGitHubStagedSynchronizationRequest(f"GitHub cursor {name} is invalid")
    return value


def _nonnegative_int(name, value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InvalidGitHubStagedSynchronizationRequest(f"GitHub cursor {name} is invalid")
    return value


def _bounded_total(name, value, maximum):
    value = _nonnegative_int(name, value)
    if value > maximum:
        raise GitHubSynchronizationBudgetExceeded(f"GitHub run {name} budget was exceeded")
    return value


def _nonblank(name, value, *, maximum):
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise InvalidGitHubStagedSynchronizationRequest(f"GitHub cursor {name} is invalid")
    return value


def _object_id(value):
    if (
        not isinstance(value, str)
        or len(value) not in {40, 64}
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor object ID is invalid")
    return value


def _sha256(value):
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise InvalidGitHubStagedSynchronizationRequest("prepared GitHub checksum is invalid")
    return value


def _repository_path(value, *, allow_empty):
    if not isinstance(value, str) or (not value and not allow_empty):
        raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor path is invalid")
    if value == "" and allow_empty:
        return value
    segments = value.split("/")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor path is invalid") from exc
    if (
        value.startswith("/")
        or value.endswith("/")
        or "\\" in value
        or len(encoded) > 1024
        or len(segments) > MAX_REPOSITORY_PATH_SEGMENTS
        or any(segment in {"", ".", ".."} for segment in segments)
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise InvalidGitHubStagedSynchronizationRequest("GitHub cursor path is invalid")
    return value
