"""Fenced, one-file GitHub work-ledger materialization boundary."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import PurePosixPath

from application.services.github_repository_content_service import (
    GitHubRepositoryContentAuthorization,
    GitHubRepositoryContentService,
    GitHubRepositoryEntry,
    GitHubRepositorySnapshot,
)
from application.services.github_staged_synchronization_service import (
    GitHubItemSnapshot,
    InvalidGitHubStagedSynchronizationRequest,
    PreparedGitHubFile,
    StalePreparedGitHubBatch,
)
from application.services.github_sync_work_planning_service import (
    GITHUB_PLANNING_MIME_TYPES,
    GITHUB_PROVIDER_KEY,
)
from domain.connectors.sync_work_ledger import (
    FileWorkCounters,
    FileWorkItemView,
    FileWorkLease,
    FileWorkMaterialization,
    FileWorkMaterializationChunk,
    FileWorkMaterializationView,
    FileWorkStatus,
    RepositoryGenerationStatus,
    RepositoryGenerationView,
)
from application.services.local_document_indexing_service import (
    LocalDocumentIndexingProfile,
)
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    ConnectorSyncWorkLedgerRepository,
    SyncWorkLedgerNotFound,
)


GITHUB_SYNC_LEDGER_PROCESSING_ENVIRONMENT_VARIABLE = (
    "GITHUB_SYNC_LEDGER_PROCESSING_ENABLED"
)


@dataclass(frozen=True, repr=False)
class GitHubFileWorkContext:
    lease: FileWorkLease
    generation: RepositoryGenerationView
    work_item: FileWorkItemView
    authorization: GitHubRepositoryContentAuthorization
    snapshot: GitHubRepositorySnapshot
    entry: GitHubRepositoryEntry
    item_snapshot: GitHubItemSnapshot


@dataclass(frozen=True)
class GitHubFileWorkResult:
    work_item: FileWorkItemView
    outcome: str
    materialization: FileWorkMaterializationView | None


class GitHubSyncWorkProcessingService:
    """Validate and atomically persist one already-claimed GitHub work item."""

    def __init__(
        self,
        ledger: ConnectorSyncWorkLedgerRepository,
        content: GitHubRepositoryContentService,
        profile: LocalDocumentIndexingProfile,
    ) -> None:
        self._ledger = ledger
        self._content = content
        self._profile = profile

    def load_context(
        self,
        lease: FileWorkLease,
        *,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> GitHubFileWorkContext:
        renewed = self._ledger.heartbeat(
            lease,
            worker_id=worker_id,
            now=now,
            lease_duration=lease_duration,
        )
        generation, work_item = self._durable_context(renewed)
        authorization = self._content.authorize(
            renewed.organization_id,
            renewed.connector_id,
            renewed.connector_scope_id,
        )
        snapshot, entry = _pinned_provider_context(
            generation, work_item, authorization
        )
        existing = self._ledger.get_materialization(
            renewed.organization_id,
            renewed.generation_id,
            renewed.work_item_id,
        )
        if existing is not None and not _materialization_view_matches(
            existing, generation, work_item
        ):
            raise StalePreparedGitHubBatch(
                "staged GitHub file materialization changed"
            )
        item_snapshot = GitHubItemSnapshot(
            None,
            existing.provider_blob_id if existing is not None else None,
            None,
            existing.provider_blob_id if existing is not None else None,
            existing is not None,
            None,
            None,
        )
        return GitHubFileWorkContext(
            renewed,
            generation,
            work_item,
            authorization,
            snapshot,
            entry,
            item_snapshot,
        )

    def persist(
        self,
        lease: FileWorkLease,
        context: GitHubFileWorkContext,
        prepared: PreparedGitHubFile,
        *,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> GitHubFileWorkResult:
        renewed = self._ledger.heartbeat(
            lease,
            worker_id=worker_id,
            now=now,
            lease_duration=lease_duration,
        )
        generation, work_item = self._durable_context(renewed)
        if (
            not isinstance(context, GitHubFileWorkContext)
            or context.generation != generation
            or context.work_item.work_item_id != work_item.work_item_id
            or context.work_item.source_item_key != work_item.source_item_key
            or context.work_item.provider_blob_id != work_item.provider_blob_id
            or context.work_item.provider_revision_id != work_item.provider_revision_id
            or context.work_item.profile_fingerprint != work_item.profile_fingerprint
        ):
            raise StalePreparedGitHubBatch("GitHub file work context changed")
        current_authorization = self._content.authorize(
            renewed.organization_id,
            renewed.connector_id,
            renewed.connector_scope_id,
        )
        if current_authorization != context.authorization:
            raise StalePreparedGitHubBatch(
                "GitHub file work authorization changed"
            )
        expected_snapshot, expected_entry = _pinned_provider_context(
            generation, work_item, current_authorization
        )
        if (
            context.snapshot != expected_snapshot
            or context.entry != expected_entry
            or prepared.discovered.entry != expected_entry
        ):
            raise StalePreparedGitHubBatch("prepared GitHub file work is stale")
        counters = FileWorkCounters(
            downloaded_bytes=prepared.downloaded_bytes,
            extracted_characters=prepared.extracted_characters,
            chunk_count=len(prepared.chunks),
            embedding_batch_count=prepared.embedding_batch_count,
        )
        if prepared.outcome == "indexed":
            if (
                prepared.content_checksum is None
                or prepared.embedding_model is None
                or not prepared.chunks
            ):
                raise InvalidGitHubStagedSynchronizationRequest(
                    "prepared GitHub file materialization is incomplete"
                )
            materialization = FileWorkMaterialization(
                generation.repository_identity,
                generation.branch_name,
                generation.root_tree_object_id,
                work_item.source_item_key,
                work_item.repository_path,
                work_item.provider_blob_id,
                work_item.provider_revision_id,
                work_item.profile_fingerprint,
                prepared.content_checksum,
                prepared.title,
                prepared.mime_type,
                prepared.embedding_model,
                tuple(
                    FileWorkMaterializationChunk(
                        chunk.chunk_index,
                        chunk.chunk_text,
                        chunk.content_hash,
                        tuple(float(value) for value in chunk.embedding),
                        prepared.embedding_model,
                    )
                    for chunk in prepared.chunks
                ),
            )
            completed, persisted, _created = (
                self._ledger.stage_materialization_and_complete(
                    renewed,
                    worker_id=worker_id,
                    generation=generation,
                    work_item=work_item,
                    materialization=materialization,
                    counters=counters,
                    now=now,
                )
            )
            return GitHubFileWorkResult(completed, "succeeded", persisted)
        if prepared.outcome in {"already_complete", "unchanged"}:
            existing = self._ledger.get_materialization(
                renewed.organization_id,
                renewed.generation_id,
                renewed.work_item_id,
            )
            if existing is None or not _materialization_view_matches(
                existing, generation, work_item
            ):
                raise StalePreparedGitHubBatch(
                    "staged GitHub file materialization is unavailable"
                )
            completed = self._ledger.complete(
                renewed,
                worker_id=worker_id,
                outcome=FileWorkStatus.SKIPPED,
                counters=counters,
                now=now,
            )
            return GitHubFileWorkResult(completed, "skipped", existing)
        if prepared.outcome == "unsupported":
            completed = self._ledger.complete(
                renewed,
                worker_id=worker_id,
                outcome=FileWorkStatus.SKIPPED,
                counters=counters,
                now=now,
            )
            return GitHubFileWorkResult(completed, "skipped", None)
        raise InvalidGitHubStagedSynchronizationRequest(
            "prepared GitHub file outcome is invalid"
        )

    def _durable_context(
        self, lease: FileWorkLease
    ) -> tuple[RepositoryGenerationView, FileWorkItemView]:
        generation = self._ledger.get_generation(
            lease.organization_id, lease.generation_id
        )
        work_item = self._ledger.get_work_item(
            lease.organization_id, lease.generation_id, lease.work_item_id
        )
        if generation is None or work_item is None:
            raise SyncWorkLedgerNotFound("GitHub file work context was not found")
        if (
            generation.organization_id != lease.organization_id
            or generation.connector_id != lease.connector_id
            or generation.connector_scope_id != lease.connector_scope_id
            or generation.provider_key != GITHUB_PROVIDER_KEY
            or generation.profile_fingerprint != self._profile.fingerprint
            or generation.status is not RepositoryGenerationStatus.PROCESSING
            or not generation.discovery_complete
            or work_item.organization_id != lease.organization_id
            or work_item.connector_id != lease.connector_id
            or work_item.connector_scope_id != lease.connector_scope_id
            or work_item.generation_id != lease.generation_id
            or work_item.work_item_id != lease.work_item_id
            or work_item.status is not FileWorkStatus.RUNNING
            or work_item.attempt_count != lease.attempt_number
            or work_item.max_attempts != lease.max_attempts
            or work_item.cancellation_requested
            or work_item.profile_fingerprint != generation.profile_fingerprint
            or work_item.provider_revision_id != generation.commit_object_id
        ):
            raise InvalidGitHubStagedSynchronizationRequest(
                "GitHub file work attribution is invalid"
            )
        return generation, work_item


def _pinned_provider_context(
    generation: RepositoryGenerationView,
    work_item: FileWorkItemView,
    authorization: GitHubRepositoryContentAuthorization,
) -> tuple[GitHubRepositorySnapshot, GitHubRepositoryEntry]:
    extension = PurePosixPath(work_item.repository_path).suffix.casefold()
    expected_source_key = (
        f"github:repository:{authorization.repository_id}:path:{work_item.repository_path}"
    )
    if (
        authorization.organization_id != generation.organization_id
        or authorization.connector_id != generation.connector_id
        or authorization.scope_id != generation.connector_scope_id
        or authorization.canonical_repository_identity
        != generation.repository_identity
        or authorization.default_branch_name != generation.branch_name
        or work_item.source_item_key != expected_source_key
        or work_item.file_size_bytes is None
        or work_item.file_size_bytes < 0
        or extension not in GITHUB_PLANNING_MIME_TYPES
        or work_item.file_extension != extension
        or work_item.mime_type != GITHUB_PLANNING_MIME_TYPES[extension]
    ):
        raise InvalidGitHubStagedSynchronizationRequest(
            "GitHub file work provider attribution is invalid"
        )
    snapshot = GitHubRepositorySnapshot(
        generation.connector_id,
        generation.connector_scope_id,
        authorization.repository_id,
        generation.repository_identity,
        generation.branch_name,
        generation.commit_object_id,
        generation.root_tree_object_id,
    )
    entry = GitHubRepositoryEntry(
        generation.connector_id,
        generation.connector_scope_id,
        authorization.repository_id,
        generation.repository_identity,
        generation.commit_object_id,
        generation.root_tree_object_id,
        generation.root_tree_object_id,
        work_item.repository_path.rsplit("/", 1)[-1],
        work_item.repository_path,
        "regular_blob",
        work_item.provider_blob_id,
        work_item.file_size_bytes,
        False,
    )
    return snapshot, entry


def _materialization_view_matches(
    materialization: FileWorkMaterializationView,
    generation: RepositoryGenerationView,
    work_item: FileWorkItemView,
) -> bool:
    return (
        materialization.organization_id == generation.organization_id
        and materialization.connector_id == generation.connector_id
        and materialization.connector_scope_id == generation.connector_scope_id
        and materialization.generation_id == generation.generation_id
        and materialization.work_item_id == work_item.work_item_id
        and materialization.provider_blob_id == work_item.provider_blob_id
        and materialization.provider_revision_id == generation.commit_object_id
        and materialization.provider_revision_id == work_item.provider_revision_id
        and materialization.profile_fingerprint == generation.profile_fingerprint
        and materialization.profile_fingerprint == work_item.profile_fingerprint
        and materialization.chunk_count > 0
    )
