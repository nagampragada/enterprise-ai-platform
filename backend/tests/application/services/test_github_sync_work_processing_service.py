from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.services.github_repository_content_service import (
    GitHubRepositoryContentAuthorization,
    GitHubRepositorySnapshot,
)
from application.services.github_staged_synchronization_service import (
    GitHubDiscoveredFile,
    GitHubRunBudget,
    GitHubTraversalCursor,
    InvalidGitHubStagedSynchronizationRequest,
    PreparedGitHubChunk,
    PreparedGitHubFile,
    StalePreparedGitHubBatch,
)
from application.services.github_sync_work_processing_service import (
    GitHubSyncWorkProcessingService,
)
from application.services.local_document_indexing_service import (
    LocalDocumentIndexingProfile,
)
from domain.connectors.sync_work_ledger import (
    FileWorkCounters,
    FileWorkItemView,
    FileWorkLease,
    FileWorkMaterializationView,
    FileWorkStatus,
    RepositoryGenerationStatus,
    RepositoryGenerationView,
)


NOW = datetime(2026, 8, 31, 12, tzinfo=timezone.utc)
COMMIT = "a" * 40
TREE = "b" * 40
BLOB = "c" * 40
CONTENT = "alpha"
CHECKSUM = hashlib.sha256(CONTENT.encode()).hexdigest()


def _profile():
    return LocalDocumentIndexingProfile(
        "content_extraction", "e" * 64, "deterministic_text_chunker", "d" * 64,
        "fake", "fake:model:1536", 1536, "f" * 64,
    )


def _state():
    organization_id, connector_id, scope_id, job_id, generation_id, work_id = (
        uuid4() for _ in range(6)
    )
    lease = FileWorkLease(
        organization_id, connector_id, scope_id, generation_id, work_id,
        "worker-1", uuid4(), 1, 1, 3, NOW + timedelta(minutes=15),
    )
    generation = RepositoryGenerationView(
        generation_id, organization_id, connector_id, scope_id, job_id,
        "github", "github:repository:501", "main", COMMIT, TREE,
        _profile().fingerprint, RepositoryGenerationStatus.PROCESSING, True,
        NOW, False, None, False, None, 1, 1, 5, NOW, NOW, None,
    )
    work = FileWorkItemView(
        work_id, organization_id, connector_id, scope_id, generation_id,
        "github:repository:501:path:documents/file.md", "documents/file.md",
        BLOB, COMMIT, _profile().fingerprint, 5, ".md", "text/markdown",
        FileWorkStatus.RUNNING, 1, 3, None, False, None, None, None,
        FileWorkCounters(), NOW, NOW, None,
    )
    authorization = GitHubRepositoryContentAuthorization(
        organization_id, connector_id, scope_id, uuid4(), uuid4(), 11, 22, 33,
        "sandbox-org", 501, "repository", "sandbox-org/repository",
        "sandbox-org", "github:repository:501", "main",
    )
    return lease, generation, work, authorization


def _materialization_view(generation, work):
    return FileWorkMaterializationView(
        uuid4(), generation.organization_id, generation.connector_id,
        generation.connector_scope_id, generation.generation_id,
        work.work_item_id, work.provider_blob_id, work.provider_revision_id,
        work.profile_fingerprint, 1, NOW,
    )


def _service(*, generation=None, work=None, authorization=None, existing=None):
    lease, default_generation, default_work, default_authorization = _state()
    generation = generation or default_generation
    work = work or default_work
    authorization = authorization or default_authorization
    lease = replace(
        lease,
        organization_id=generation.organization_id,
        connector_id=generation.connector_id,
        connector_scope_id=generation.connector_scope_id,
        generation_id=generation.generation_id,
        work_item_id=work.work_item_id,
    )
    ledger, content = Mock(), Mock()
    ledger.heartbeat.return_value = lease
    ledger.get_generation.return_value = generation
    ledger.get_work_item.return_value = work
    ledger.get_materialization.return_value = existing
    content.authorize.return_value = authorization
    return GitHubSyncWorkProcessingService(ledger, content, _profile()), lease, ledger, content


def _prepared(context, *, blob=BLOB, outcome="indexed"):
    cursor = GitHubTraversalCursor.initial(context.snapshot, context.authorization)
    advanced = replace(cursor, totals=GitHubRunBudget(entries_examined=1))
    chunks = (
        (PreparedGitHubChunk(0, CONTENT, CHECKSUM, (1.0,) * 1536),)
        if outcome == "indexed" else ()
    )
    return PreparedGitHubFile(
        GitHubDiscoveredFile(replace(context.entry, object_id=blob), cursor, advanced, None),
        None, BLOB if outcome == "unchanged" else None, outcome,
        CHECKSUM if outcome == "indexed" else None, "File", "text/markdown",
        chunks, "fake:model:1536" if outcome == "indexed" else None, None,
        5 if outcome == "indexed" else 0, 5 if outcome == "indexed" else 0,
        1 if outcome == "indexed" else 0,
    )


def test_load_context_uses_only_the_exact_pinned_generation_and_blob():
    service, lease, ledger, _content = _service()
    context = service.load_context(
        lease, worker_id="worker-1", now=NOW,
        lease_duration=timedelta(minutes=15),
    )
    assert context.snapshot == GitHubRepositorySnapshot(
        lease.connector_id, lease.connector_scope_id, 501,
        "github:repository:501", "main", COMMIT, TREE,
    )
    assert context.entry.path == "documents/file.md"
    assert context.entry.object_id == BLOB
    assert context.entry.commit_object_id == COMMIT
    ledger.heartbeat.assert_called_once()
    ledger.get_materialization.assert_called_once()


@pytest.mark.parametrize(
    "mutation",
    (
        lambda generation, work: (replace(generation, provider_key="other"), work),
        lambda generation, work: (replace(generation, discovery_complete=False), work),
        lambda generation, work: (generation, replace(work, organization_id=uuid4())),
        lambda generation, work: (generation, replace(work, connector_id=uuid4())),
        lambda generation, work: (generation, replace(work, connector_scope_id=uuid4())),
        lambda generation, work: (generation, replace(work, generation_id=uuid4())),
        lambda generation, work: (generation, replace(work, provider_revision_id="d" * 40)),
        lambda generation, work: (generation, replace(work, profile_fingerprint="e" * 64)),
    ),
)
def test_load_context_fails_closed_on_generation_or_work_attribution_mismatch(mutation):
    lease, generation, work, authorization = _state()
    generation, work = mutation(generation, work)
    service, lease, _ledger, _content = _service(
        generation=generation, work=work, authorization=authorization
    )
    with pytest.raises(InvalidGitHubStagedSynchronizationRequest):
        service.load_context(
            lease, worker_id="worker-1", now=NOW,
            lease_duration=timedelta(minutes=15),
        )


def test_persist_stages_and_completes_through_one_atomic_repository_operation():
    service, lease, ledger, _content = _service()
    context = service.load_context(
        lease, worker_id="worker-1", now=NOW,
        lease_duration=timedelta(minutes=15),
    )
    completed = replace(context.work_item, status=FileWorkStatus.SUCCEEDED)
    persisted = _materialization_view(context.generation, context.work_item)
    ledger.stage_materialization_and_complete.return_value = (completed, persisted, True)
    result = service.persist(
        lease, context, _prepared(context), worker_id="worker-1", now=NOW,
        lease_duration=timedelta(minutes=15),
    )
    assert result.work_item.status is FileWorkStatus.SUCCEEDED
    assert result.materialization == persisted
    call = ledger.stage_materialization_and_complete.call_args
    assert call.kwargs["counters"] == FileWorkCounters(5, 5, 1, 1)
    assert call.kwargs["materialization"].provider_blob_id == BLOB
    assert call.kwargs["materialization"].chunks[0].content_hash == CHECKSUM
    ledger.complete.assert_not_called()


def test_replay_of_existing_staged_output_completes_without_republishing():
    lease, generation, work, authorization = _state()
    existing = _materialization_view(generation, work)
    service, lease, ledger, _content = _service(
        generation=generation, work=work, authorization=authorization, existing=existing
    )
    context = service.load_context(
        lease, worker_id="worker-1", now=NOW,
        lease_duration=timedelta(minutes=15),
    )
    ledger.complete.return_value = replace(context.work_item, status=FileWorkStatus.SKIPPED)
    result = service.persist(
        lease, context, _prepared(context, outcome="unchanged"),
        worker_id="worker-1", now=NOW, lease_duration=timedelta(minutes=15),
    )
    assert result.outcome == "skipped"
    assert result.materialization == existing
    ledger.stage_materialization_and_complete.assert_not_called()
    ledger.complete.assert_called_once()


def test_stale_prepared_blob_is_rejected_before_staging_or_completion():
    service, lease, ledger, _content = _service()
    context = service.load_context(
        lease, worker_id="worker-1", now=NOW,
        lease_duration=timedelta(minutes=15),
    )
    with pytest.raises(StalePreparedGitHubBatch):
        service.persist(
            lease, context, _prepared(context, blob="d" * 40),
            worker_id="worker-1", now=NOW,
            lease_duration=timedelta(minutes=15),
        )
    ledger.stage_materialization_and_complete.assert_not_called()
    ledger.complete.assert_not_called()
