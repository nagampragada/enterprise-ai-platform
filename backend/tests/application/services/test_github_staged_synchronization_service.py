from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
import hashlib
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.services.github_repository_content_service import (
    GitHubBlobContent,
    MAX_GITHUB_BLOB_BYTES,
    GitHubRepositoryContentAuthorization,
    GitHubRepositoryEntry,
    GitHubRepositorySnapshot,
    GitHubTreePage,
)
from application.services.github_staged_synchronization_service import (
    CURSOR_SCHEMA_VERSION,
    MAX_CURSOR_BYTES,
    GitHubDiscoveredFile,
    GitHubDiscoveryBatch,
    GitHubItemSnapshot,
    GitHubRunBudget,
    GitHubStagedSynchronizationService,
    GitHubSynchronizationBudgetExceeded,
    GitHubSynchronizationLimits,
    GitHubSynchronizationPreparationService,
    GitHubSynchronizationSnapshot,
    GitHubTraversalCursor,
    GitHubTraversalFrame,
    InvalidGitHubStagedSynchronizationRequest,
    PreparedGitHubBatch,
    StalePreparedGitHubBatch,
    classify_github_synchronization_failure,
)
from application.services.connector_sync_retry_policy import SyncFailureKind
from application.services.github_repository_content_service import (
    GitHubRepositoryContentRejected,
    GitHubRepositoryContentUnavailable,
)
from infrastructure.repositories.connector_sync_job_repository import StaleSyncJobFence
from application.services.local_document_indexing_service import LocalDocumentIndexingProfile
from domain.content_chunking.models import ChunkResult
from domain.content_extraction.models import ExtractedContent
from domain.embeddings.models import EmbeddingProfile, EmbeddingResult


OBJECT = "a" * 40
ROOT = "b" * 40
CHILD = "c" * 40
BLOB = "d" * 40


def _authorization():
    return GitHubRepositoryContentAuthorization(
        uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), 11, 22, 33,
        "example", 44, "repo", "example/repo", "example",
        "github:repository:44", "main",
    )


def _snapshot(authorization=None):
    authorization = authorization or _authorization()
    return GitHubRepositorySnapshot(
        authorization.connector_id,
        authorization.scope_id,
        authorization.repository_id,
        authorization.canonical_repository_identity,
        authorization.default_branch_name,
        OBJECT,
        ROOT,
    )


def _entry(snapshot, name, kind="regular_blob", object_id=BLOB, size=5, parent=ROOT):
    return GitHubRepositoryEntry(
        snapshot.connector_id,
        snapshot.scope_id,
        snapshot.repository_id,
        snapshot.canonical_repository_identity,
        snapshot.commit_object_id,
        snapshot.root_tree_object_id,
        parent,
        name.rsplit("/", 1)[-1],
        name,
        kind,
        object_id,
        None if kind == "tree" else size,
        False,
    )


def _preparation(content=None):
    content = content or Mock()
    registry, chunker, provider = Mock(), Mock(), Mock()
    registry.extractors = {".txt": Mock()}
    provider.profile = EmbeddingProfile("fake", "fake", 1536, "fake:model:1536", 2)
    return (
        GitHubSynchronizationPreparationService(content, registry, chunker, provider),
        content,
        registry,
        chunker,
        provider,
    )


def _item_snapshot(*, blob=None, complete=False, status=None, run_blob=None):
    return GitHubItemSnapshot(
        uuid4() if blob else None,
        blob,
        "e" * 64 if blob else None,
        blob,
        complete,
        status,
        run_blob,
    )


def test_cursor_is_immutable_round_trips_and_contains_only_allowlisted_safe_state():
    authorization = _authorization()
    cursor = GitHubTraversalCursor.initial(_snapshot(authorization))
    encoded = cursor.to_safe_json()
    assert encoded["schema_version"] == CURSOR_SCHEMA_VERSION
    assert "token" not in repr(encoded).lower()
    assert GitHubTraversalCursor.from_safe_json(
        encoded,
        connector_id=authorization.connector_id,
        scope_id=authorization.scope_id,
    ) == cursor
    with pytest.raises(FrozenInstanceError):
        cursor.scan_complete = True  # type: ignore[misc]


@pytest.mark.parametrize(
    "mutation,error",
    (
        (lambda value: value.update(schema_version=999), "version"),
        (lambda value: value.update(extra=True), "fields"),
        (lambda value: value["frames"][0].update(tree_object_id="not-an-id"), "object ID"),
        (lambda value: value["frames"][0].update(tree_path="../escape"), "path"),
        (lambda value: value["totals"].update(entries_examined=100_001), "budget"),
    ),
)
def test_corrupted_cursor_is_rejected(mutation, error):
    authorization = _authorization()
    value = GitHubTraversalCursor.initial(_snapshot(authorization)).to_safe_json()
    mutation(value)
    with pytest.raises(InvalidGitHubStagedSynchronizationRequest, match=error):
        GitHubTraversalCursor.from_safe_json(
            value,
            connector_id=authorization.connector_id,
            scope_id=authorization.scope_id,
        )


def test_oversized_cursor_is_rejected_before_deserialization():
    authorization = _authorization()
    value = GitHubTraversalCursor.initial(_snapshot(authorization)).to_safe_json()
    value["padding"] = "x" * MAX_CURSOR_BYTES
    with pytest.raises(InvalidGitHubStagedSynchronizationRequest, match="too large"):
        GitHubTraversalCursor.from_safe_json(
            value,
            connector_id=authorization.connector_id,
            scope_id=authorization.scope_id,
        )


def test_nonrecursive_dfs_skips_unsupported_and_resumes_from_exact_pinned_tree():
    authorization = _authorization()
    snapshot = _snapshot(authorization)
    content = Mock()
    root_entries = (
        _entry(snapshot, "ignore.py"),
        _entry(snapshot, "docs", "tree", CHILD),
        _entry(snapshot, "root.txt", object_id="e" * 40),
    )
    child_entries = (_entry(snapshot, "docs/readme.md", parent=CHILD),)

    def list_tree(_authorization, _snapshot_value, tree):
        assert _snapshot_value.commit_object_id == OBJECT
        return GitHubTreePage(tree, child_entries if tree.object_id == CHILD else root_entries)

    content.list_tree.side_effect = list_tree
    service, *_ = _preparation(content)
    first = service.discover_batch(
        authorization,
        GitHubTraversalCursor.initial(snapshot),
        limits=GitHubSynchronizationLimits(max_files=1),
    )
    assert [item.entry.path for item in first.files] == ["docs/readme.md"]
    assert first.tree_requests == 2
    assert first.cursor_after.snapshot.commit_object_id == OBJECT
    second = service.discover_batch(
        authorization,
        first.cursor_after,
        limits=GitHubSynchronizationLimits(max_files=1),
    )
    assert [item.entry.path for item in second.files] == ["root.txt"]
    assert all(call.args[2].path in {"", "docs"} for call in content.list_tree.call_args_list)


def test_entry_and_tree_request_limits_return_resumable_progress_without_looping():
    authorization = _authorization()
    snapshot = _snapshot(authorization)
    content = Mock()
    tree = snapshot.root_tree()
    content.list_tree.return_value = GitHubTreePage(
        tree,
        tuple(_entry(snapshot, f"skip-{index}.bin") for index in range(3)),
    )
    service, *_ = _preparation(content)
    batch = service.discover_batch(
        authorization,
        GitHubTraversalCursor.initial(snapshot),
        limits=GitHubSynchronizationLimits(max_entries=2, max_tree_requests=1),
    )
    assert not batch.files and batch.entries_examined == 2 and batch.tree_requests == 1
    assert batch.cursor_after.frames[0].next_entry_index == 2
    assert content.list_tree.call_count == 1


def test_depth_and_total_run_budgets_fail_safely_and_finitely():
    authorization = _authorization()
    snapshot = _snapshot(authorization)
    frames = tuple(
        GitHubTraversalFrame("/".join(["d"] * index), ROOT if index == 0 else CHILD, 0)
        for index in range(65)
    )
    with pytest.raises(InvalidGitHubStagedSynchronizationRequest, match="depth"):
        GitHubTraversalCursor(snapshot, frames, GitHubRunBudget())
    with pytest.raises(GitHubSynchronizationBudgetExceeded, match="entries"):
        GitHubTraversalCursor(
            snapshot,
            (GitHubTraversalFrame("", ROOT, 0),),
            GitHubRunBudget(entries_examined=100_001),
        )


def test_unsupported_extension_never_downloads_extracts_or_embeds():
    authorization = _authorization()
    snapshot = _snapshot(authorization)
    content = Mock()
    tree = snapshot.root_tree()
    content.list_tree.return_value = GitHubTreePage(tree, (_entry(snapshot, "code.py"),))
    service, _, registry, chunker, provider = _preparation(content)
    discovered = service.discover_batch(authorization, GitHubTraversalCursor.initial(snapshot))
    prepared = service.prepare_batch(authorization, (), discovered)
    assert not prepared.files and prepared.cursor_after.scan_complete
    content.download_blob.assert_not_called()
    registry.extract.assert_not_called()
    chunker.chunk.assert_not_called()
    provider.embed_batch.assert_not_called()


def test_oversized_supported_file_is_seen_as_skipped_without_download():
    authorization = _authorization()
    snapshot = _snapshot(authorization)
    content = Mock()
    tree = snapshot.root_tree()
    content.list_tree.return_value = GitHubTreePage(
        tree, (_entry(snapshot, "large.txt", size=MAX_GITHUB_BLOB_BYTES + 1),)
    )
    service, _, registry, chunker, provider = _preparation(content)
    discovered = service.discover_batch(authorization, GitHubTraversalCursor.initial(snapshot))
    assert discovered.files[0].skip_reason == "file_too_large"
    prepared = service.prepare_batch(authorization, (_item_snapshot(),), discovered)
    assert prepared.files[0].outcome == "skipped"
    content.download_blob.assert_not_called()
    registry.extract.assert_not_called()
    chunker.chunk.assert_not_called()
    provider.embed_batch.assert_not_called()


def test_declared_byte_budget_stops_before_next_file_and_preserves_resume_cursor():
    authorization = _authorization()
    snapshot = _snapshot(authorization)
    content = Mock()
    tree = snapshot.root_tree()
    content.list_tree.return_value = GitHubTreePage(
        tree,
        (
            _entry(snapshot, "one.txt", object_id="1" * 40, size=10),
            _entry(snapshot, "two.txt", object_id="2" * 40, size=10),
        ),
    )
    service, *_ = _preparation(content)
    batch = service.discover_batch(
        authorization,
        GitHubTraversalCursor.initial(snapshot),
        limits=GitHubSynchronizationLimits(max_download_bytes=10),
    )
    assert [item.entry.path for item in batch.files] == ["one.txt"]
    assert batch.cursor_after.frames[0].next_entry_index == 1
    assert not batch.cursor_after.scan_complete


@pytest.mark.parametrize("suffix", ("txt", "TXT", "md", "MARKDOWN", "pdf", "DOCX"))
def test_supported_extension_policy_is_case_insensitive(suffix):
    authorization = _authorization()
    snapshot = _snapshot(authorization)
    content = Mock()
    tree = snapshot.root_tree()
    content.list_tree.return_value = GitHubTreePage(tree, (_entry(snapshot, f"file.{suffix}"),))
    service, *_ = _preparation(content)
    result = service.discover_batch(authorization, GitHubTraversalCursor.initial(snapshot))
    assert len(result.files) == 1


def test_unchanged_and_completed_items_skip_all_expensive_work():
    authorization = _authorization()
    snapshot = _snapshot(authorization)
    cursor = GitHubTraversalCursor.initial(snapshot)
    entry = _entry(snapshot, "file.txt")
    after = replace(cursor, totals=GitHubRunBudget(entries_examined=1))
    final = replace(cursor, totals=GitHubRunBudget(entries_examined=2))
    first = GitHubDiscoveredFile(entry, cursor, after, None)
    second = GitHubDiscoveredFile(entry, after, final, None)
    batch = GitHubDiscoveryBatch((first, second), final, 1, 2)
    service, content, registry, chunker, provider = _preparation()
    result = service.prepare_batch(
        authorization,
        (
            _item_snapshot(blob=BLOB, complete=True),
            _item_snapshot(blob=BLOB, status="succeeded", run_blob=BLOB),
        ),
        batch,
    )
    assert [item.outcome for item in result.files] == ["unchanged", "already_complete"]
    content.download_blob.assert_not_called()
    registry.extract.assert_not_called()
    chunker.chunk.assert_not_called()
    provider.embed_batch.assert_not_called()


def test_changed_file_downloads_extracts_chunks_and_embeds_without_raw_content_in_dto():
    authorization = _authorization()
    snapshot = _snapshot(authorization)
    cursor = GitHubTraversalCursor.initial(snapshot)
    entry = _entry(snapshot, "file.txt")
    after = replace(cursor, totals=GitHubRunBudget(entries_examined=1))
    discovered = GitHubDiscoveredFile(entry, cursor, after, None)
    raw = b"alpha"
    checksum = hashlib.sha256(raw).hexdigest()
    service, content, registry, chunker, provider = _preparation()
    content.download_blob.return_value = GitHubBlobContent(raw, len(raw), checksum)
    registry.extract.return_value = ExtractedContent("Alpha", "alpha", "text/plain")
    chunker.chunk.return_value = (ChunkResult(0, "alpha", checksum, 5, 0, 5),)
    provider.embed_batch.return_value = (
        EmbeddingResult(0, (1.0,) * 1536, "fake:model:1536", 1536),
    )
    result = service.prepare_batch(
        authorization,
        (_item_snapshot(),),
        GitHubDiscoveryBatch((discovered,), after, 1, 1),
    )
    assert result.files[0].outcome == "indexed"
    assert result.downloaded_bytes == 5 and result.prepared_chunks == 1
    assert "alpha" not in repr(result.files[0])
    assert content.download_blob.call_count == registry.extract.call_count == 1
    assert provider.embed_batch.call_count == 1


def test_invalid_embedding_and_chunk_overflow_do_not_loop():
    authorization = _authorization()
    snapshot = _snapshot(authorization)
    cursor = GitHubTraversalCursor.initial(snapshot)
    entry = _entry(snapshot, "file.txt")
    after = replace(cursor, totals=GitHubRunBudget(entries_examined=1))
    discovered = GitHubDiscoveredFile(entry, cursor, after, None)
    service, content, registry, chunker, provider = _preparation()
    checksum = hashlib.sha256(b"alpha").hexdigest()
    content.download_blob.return_value = GitHubBlobContent(b"alpha", 5, checksum)
    registry.extract.return_value = ExtractedContent(None, "alpha", "text/plain")
    chunker.chunk.return_value = (ChunkResult(0, "alpha", checksum, 5, 0, 5),)
    provider.embed_batch.return_value = (
        EmbeddingResult(0, (1.0,) * 1535, "fake:model:1536", 1536),
    )
    with pytest.raises(Exception, match="vector length"):
        service.prepare_batch(
            authorization,
            (_item_snapshot(),),
            GitHubDiscoveryBatch((discovered,), after, 1, 1),
        )
    assert content.download_blob.call_count == provider.embed_batch.call_count == 1


def test_more_than_500_file_chunks_fails_before_any_embedding_call():
    authorization = _authorization()
    snapshot = _snapshot(authorization)
    cursor = GitHubTraversalCursor.initial(snapshot)
    entry = _entry(snapshot, "file.txt")
    after = replace(cursor, totals=GitHubRunBudget(entries_examined=1))
    discovered = GitHubDiscoveredFile(entry, cursor, after, None)
    service, content, registry, chunker, provider = _preparation()
    content.download_blob.return_value = GitHubBlobContent(b"alpha", 5, hashlib.sha256(b"alpha").hexdigest())
    registry.extract.return_value = ExtractedContent(None, "alpha", "text/plain")
    checksum = hashlib.sha256(b"x").hexdigest()
    chunker.chunk.return_value = tuple(
        ChunkResult(index, "x", checksum, 1, 0, 1) for index in range(501)
    )
    with pytest.raises(GitHubSynchronizationBudgetExceeded, match="chunk"):
        service.prepare_batch(
            authorization,
            (_item_snapshot(),),
            GitHubDiscoveryBatch((discovered,), after, 1, 1),
        )
    assert content.download_blob.call_count == 1
    provider.embed_batch.assert_not_called()


def test_stale_lease_rejection_precedes_authorization_or_provider_work():
    session, execution, content = Mock(), Mock(), Mock()
    execution.validate_attempt.side_effect = RuntimeError("stale lease")
    service = GitHubStagedSynchronizationService(session, execution, content, Mock())
    with pytest.raises(RuntimeError, match="stale lease"):
        service.snapshot(Mock(), uuid4(), worker_id="worker")
    content.authorize.assert_not_called()


@pytest.mark.parametrize(
    ("error", "kind", "retryable"),
    (
        (GitHubRepositoryContentUnavailable("safe"), SyncFailureKind.RETRYABLE_PROVIDER, True),
        (GitHubRepositoryContentRejected("safe"), SyncFailureKind.PERMANENT_PROVIDER, False),
        (GitHubSynchronizationBudgetExceeded("safe"), SyncFailureKind.VALIDATION, False),
        (StaleSyncJobFence("safe"), SyncFailureKind.CANCELLED, False),
    ),
)
def test_failure_classification_is_fixed_safe_and_bounded(error, kind, retryable):
    result = classify_github_synchronization_failure(error)
    assert result.kind is kind and result.retryable is retryable
    assert "safe" not in result.error_code and "safe" not in result.error_category


def test_persistence_rejects_missing_or_stale_cursor_before_writes():
    authorization = _authorization()
    cursor = GitHubTraversalCursor.initial(_snapshot(authorization))
    profile = LocalDocumentIndexingProfile(
        "extract", "v1", "chunk", "v1", "fake", "fake:model:1536", 1536, "f" * 64
    )
    session, execution, content = Mock(), Mock(), Mock()
    service = GitHubStagedSynchronizationService(session, execution, content, profile)
    service._require_context = Mock()  # type: ignore[method-assign]
    service._sync = Mock()
    service._sync.get_active_cursor.return_value = None
    sync_snapshot = GitHubSynchronizationSnapshot(authorization, uuid4(), Mock(), cursor, profile)
    prepared = PreparedGitHubBatch((), replace(cursor, scan_complete=True, frames=()), 0, 0)
    with pytest.raises(StalePreparedGitHubBatch, match="cursor is unavailable"):
        service.persist_batch(Mock(), sync_snapshot, prepared, worker_id="worker", now=Mock())
