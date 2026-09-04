"""Immutable contracts for repository generations and file-work execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import math
from uuid import UUID


DEFAULT_FILE_WORK_MAX_ATTEMPTS = 3
HARD_MAX_FILE_WORK_ATTEMPTS = 10
MAX_MANIFEST_BATCH_SIZE = 500
MAX_SOURCE_ITEM_KEY_LENGTH = 1024
MAX_REPOSITORY_PATH_LENGTH = 1024
MAX_PROVIDER_IDENTITY_LENGTH = 255
MAX_PROFILE_FINGERPRINT_LENGTH = 255
MAX_FILE_SIZE_BYTES = 1024 * 1024 * 1024
MAX_EXTRACTED_CHARACTERS = 100_000_000
MAX_CHUNK_COUNT = 100_000
MAX_EMBEDDING_BATCH_COUNT = 100_000
FILE_WORK_EMBEDDING_DIMENSION = 1536
MAX_CHUNK_TEXT_LENGTH = 1_000_000


class RepositoryGenerationStatus(StrEnum):
    DISCOVERING = "discovering"
    PROCESSING = "processing"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FileWorkStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    QUARANTINED = "quarantined"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_GENERATION_STATUSES = frozenset(
    {
        RepositoryGenerationStatus.COMPLETED,
        RepositoryGenerationStatus.COMPLETED_WITH_ERRORS,
        RepositoryGenerationStatus.FAILED,
        RepositoryGenerationStatus.CANCELLED,
    }
)
TERMINAL_FILE_WORK_STATUSES = frozenset(
    {
        FileWorkStatus.SUCCEEDED,
        FileWorkStatus.SKIPPED,
        FileWorkStatus.QUARANTINED,
        FileWorkStatus.FAILED,
        FileWorkStatus.CANCELLED,
    }
)


@dataclass(frozen=True)
class RepositoryGenerationRegistration:
    organization_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    sync_job_id: UUID
    provider_key: str
    repository_identity: str
    branch_name: str
    commit_object_id: str
    root_tree_object_id: str
    profile_fingerprint: str
    created_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "organization_id",
            "connector_id",
            "connector_scope_id",
            "sync_job_id",
        ):
            _require_uuid(name, getattr(self, name))
        _require_code("provider_key", self.provider_key, maximum=64)
        for name in (
            "repository_identity",
            "branch_name",
            "commit_object_id",
            "root_tree_object_id",
        ):
            _require_nonblank(name, getattr(self, name), MAX_PROVIDER_IDENTITY_LENGTH)
        _require_identifier(
            "profile_fingerprint", self.profile_fingerprint, MAX_PROFILE_FINGERPRINT_LENGTH
        )
        _require_aware("created_at", self.created_at)


@dataclass(frozen=True)
class FileWorkManifestEntry:
    source_item_key: str
    repository_path: str
    provider_blob_id: str
    provider_revision_id: str
    profile_fingerprint: str
    file_size_bytes: int | None = None
    file_extension: str | None = None
    mime_type: str | None = None
    max_attempts: int = DEFAULT_FILE_WORK_MAX_ATTEMPTS

    def __post_init__(self) -> None:
        _require_nonblank("source_item_key", self.source_item_key, MAX_SOURCE_ITEM_KEY_LENGTH)
        _require_nonblank("repository_path", self.repository_path, MAX_REPOSITORY_PATH_LENGTH)
        _require_nonblank(
            "provider_blob_id", self.provider_blob_id, MAX_PROVIDER_IDENTITY_LENGTH
        )
        _require_nonblank(
            "provider_revision_id", self.provider_revision_id, MAX_PROVIDER_IDENTITY_LENGTH
        )
        _require_identifier(
            "profile_fingerprint", self.profile_fingerprint, MAX_PROFILE_FINGERPRINT_LENGTH
        )
        if self.file_size_bytes is not None:
            _require_bounded_counter(
                "file_size_bytes", self.file_size_bytes, MAX_FILE_SIZE_BYTES
            )
        if self.file_extension is not None:
            _require_nonblank("file_extension", self.file_extension, 64)
        if self.mime_type is not None:
            _require_nonblank("mime_type", self.mime_type, 255)
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or not 1 <= self.max_attempts <= HARD_MAX_FILE_WORK_ATTEMPTS
        ):
            raise ValueError(
                f"max_attempts must be between 1 and {HARD_MAX_FILE_WORK_ATTEMPTS}"
            )


@dataclass(frozen=True)
class FileWorkCounters:
    downloaded_bytes: int = 0
    extracted_characters: int = 0
    chunk_count: int = 0
    embedding_batch_count: int = 0

    def __post_init__(self) -> None:
        _require_bounded_counter(
            "downloaded_bytes", self.downloaded_bytes, MAX_FILE_SIZE_BYTES
        )
        _require_bounded_counter(
            "extracted_characters", self.extracted_characters, MAX_EXTRACTED_CHARACTERS
        )
        _require_bounded_counter("chunk_count", self.chunk_count, MAX_CHUNK_COUNT)
        _require_bounded_counter(
            "embedding_batch_count", self.embedding_batch_count, MAX_EMBEDDING_BATCH_COUNT
        )


@dataclass(frozen=True, repr=False)
class FileWorkMaterializationChunk:
    chunk_index: int
    chunk_text: str
    content_hash: str
    embedding: tuple[float, ...]
    embedding_model: str

    def __post_init__(self) -> None:
        _require_bounded_counter("chunk_index", self.chunk_index, MAX_CHUNK_COUNT - 1)
        if (
            not isinstance(self.chunk_text, str)
            or not self.chunk_text.strip()
            or len(self.chunk_text) > MAX_CHUNK_TEXT_LENGTH
        ):
            raise ValueError("chunk_text must be nonblank and bounded")
        _require_nonblank("content_hash", self.content_hash, 128)
        _require_identifier("embedding_model", self.embedding_model, 255)
        if len(self.embedding) != FILE_WORK_EMBEDDING_DIMENSION or any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            for value in self.embedding
        ):
            raise ValueError(
                f"embedding must contain {FILE_WORK_EMBEDDING_DIMENSION} finite values"
            )


@dataclass(frozen=True, repr=False)
class FileWorkMaterialization:
    repository_identity: str
    branch_name: str
    root_tree_object_id: str
    source_item_key: str
    repository_path: str
    provider_blob_id: str
    provider_revision_id: str
    profile_fingerprint: str
    content_checksum: str
    title: str
    mime_type: str
    embedding_model: str
    chunks: tuple[FileWorkMaterializationChunk, ...]

    def __post_init__(self) -> None:
        for name in ("repository_identity", "branch_name", "root_tree_object_id"):
            _require_nonblank(name, getattr(self, name), MAX_PROVIDER_IDENTITY_LENGTH)
        _require_nonblank("source_item_key", self.source_item_key, MAX_SOURCE_ITEM_KEY_LENGTH)
        _require_nonblank("repository_path", self.repository_path, MAX_REPOSITORY_PATH_LENGTH)
        _require_nonblank("provider_blob_id", self.provider_blob_id, MAX_PROVIDER_IDENTITY_LENGTH)
        _require_nonblank(
            "provider_revision_id", self.provider_revision_id, MAX_PROVIDER_IDENTITY_LENGTH
        )
        _require_identifier(
            "profile_fingerprint", self.profile_fingerprint, MAX_PROFILE_FINGERPRINT_LENGTH
        )
        _require_nonblank("content_checksum", self.content_checksum, 128)
        _require_nonblank("title", self.title, 255)
        _require_nonblank("mime_type", self.mime_type, 255)
        _require_identifier("embedding_model", self.embedding_model, 255)
        if not isinstance(self.chunks, tuple) or not self.chunks:
            raise ValueError("materialization chunks must be a nonempty tuple")
        if len(self.chunks) > MAX_CHUNK_COUNT:
            raise ValueError("materialization chunk count exceeds its maximum")
        if any(
            not isinstance(chunk, FileWorkMaterializationChunk)
            or chunk.chunk_index != index
            or chunk.embedding_model != self.embedding_model
            for index, chunk in enumerate(self.chunks)
        ):
            raise ValueError("materialization chunks are inconsistent")


@dataclass(frozen=True)
class FileWorkMaterializationView:
    materialization_id: UUID
    organization_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    generation_id: UUID
    work_item_id: UUID
    provider_blob_id: str
    provider_revision_id: str
    profile_fingerprint: str
    chunk_count: int
    created_at: datetime


@dataclass(frozen=True)
class FileWorkLease:
    organization_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    generation_id: UUID
    work_item_id: UUID
    worker_id: str
    lease_id: UUID
    fencing_token: int
    attempt_number: int
    max_attempts: int
    lease_expires_at: datetime
    fairness_claim_sequence: int | None = None

    def __post_init__(self) -> None:
        value = self.fairness_claim_sequence
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value < 1
        ):
            raise ValueError("fairness_claim_sequence must be a positive integer")


@dataclass(frozen=True)
class RepositoryGenerationView:
    generation_id: UUID
    organization_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    sync_job_id: UUID
    provider_key: str
    repository_identity: str
    branch_name: str
    commit_object_id: str
    root_tree_object_id: str
    profile_fingerprint: str
    status: RepositoryGenerationStatus
    discovery_complete: bool
    discovery_completed_at: datetime | None
    reconciliation_eligible: bool
    reconciliation_eligible_at: datetime | None
    resync_required: bool
    resync_requested_at: datetime | None
    items_discovered: int
    items_registered: int
    declared_bytes: int
    created_at: datetime
    updated_at: datetime
    terminal_at: datetime | None


@dataclass(frozen=True)
class FileWorkItemView:
    work_item_id: UUID
    organization_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    generation_id: UUID
    source_item_key: str
    repository_path: str
    provider_blob_id: str
    provider_revision_id: str
    profile_fingerprint: str
    file_size_bytes: int | None
    file_extension: str | None
    mime_type: str | None
    status: FileWorkStatus
    attempt_count: int
    max_attempts: int
    next_attempt_at: datetime | None
    cancellation_requested: bool
    last_error_category: str | None
    last_error_code: str | None
    quarantine_reason_code: str | None
    counters: FileWorkCounters
    created_at: datetime
    updated_at: datetime
    terminal_at: datetime | None


@dataclass(frozen=True)
class ManifestRegistrationResult:
    generation_id: UUID
    created_count: int
    existing_count: int
    work_item_ids: tuple[UUID, ...]


@dataclass(frozen=True)
class GenerationBarrierSummary:
    generation_id: UUID
    discovery_complete: bool
    total_items: int
    pending_items: int
    running_items: int
    retry_wait_items: int
    succeeded_items: int
    skipped_items: int
    quarantined_items: int
    failed_items: int
    cancelled_items: int
    nonterminal_items: int
    barrier_open: bool


class GenerationActivationStatus(StrEnum):
    ACTIVE = "active"
    RETIRED = "retired"


@dataclass(frozen=True)
class GenerationPromotionRequest:
    organization_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    generation_id: UUID
    sync_job_id: UUID
    provider_key: str
    repository_identity: str
    branch_name: str
    commit_object_id: str
    root_tree_object_id: str
    profile_fingerprint: str

    def __post_init__(self) -> None:
        for name in (
            "organization_id",
            "connector_id",
            "connector_scope_id",
            "generation_id",
            "sync_job_id",
        ):
            _require_uuid(name, getattr(self, name))
        _require_code("provider_key", self.provider_key, maximum=64)
        for name in (
            "repository_identity",
            "branch_name",
            "commit_object_id",
            "root_tree_object_id",
        ):
            _require_nonblank(name, getattr(self, name), MAX_PROVIDER_IDENTITY_LENGTH)
        _require_identifier(
            "profile_fingerprint", self.profile_fingerprint, MAX_PROFILE_FINGERPRINT_LENGTH
        )


@dataclass(frozen=True)
class GenerationActivationView:
    activation_id: UUID
    organization_id: UUID
    connector_id: UUID
    connector_scope_id: UUID
    generation_id: UUID
    repository_identity: str
    commit_object_id: str
    profile_fingerprint: str
    status: GenerationActivationStatus
    activated_at: datetime
    retired_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class GenerationPromotionResult:
    activation: GenerationActivationView
    promoted: bool
    retired_generation_id: UUID | None
    materialization_count: int
    chunk_count: int


def _require_uuid(name: str, value: object) -> UUID:
    if not isinstance(value, UUID):
        raise ValueError(f"{name} must be a UUID")
    return value


def _require_aware(name: str, value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value


def _require_nonblank(name: str, value: object, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{name} must be normalized and nonblank")
    if len(value) > maximum:
        raise ValueError(f"{name} exceeds its maximum length")
    return value


def _require_code(name: str, value: object, *, maximum: int) -> str:
    result = _require_nonblank(name, value, maximum)
    if not result[0].isalpha() or result.lower() != result or any(
        not (character.isalnum() or character == "_") for character in result
    ):
        raise ValueError(f"{name} must be a normalized code")
    return result


def _require_identifier(name: str, value: object, maximum: int) -> str:
    result = _require_nonblank(name, value, maximum)
    if result.lower() != result or any(
        not (character.isalnum() or character in "._:/-") for character in result
    ):
        raise ValueError(f"{name} must be a normalized identifier")
    return result


def _require_bounded_counter(name: str, value: object, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= maximum
    ):
        raise ValueError(f"{name} must be between 0 and {maximum}")
    return value
