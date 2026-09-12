from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.services.github_repository_content_service import (
    MAX_GITHUB_BLOB_BYTES,
    GitHubRepositoryContentAuthorization,
    GitHubRepositoryEntry,
    GitHubRepositorySnapshot,
)
from application.services.github_sync_work_planning_service import (
    GitHubSyncWorkPlanningService,
    InvalidGitHubSyncWorkPlanningRequest,
)
from domain.connectors.sync_work_ledger import DiscoveryRegistrationResult
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    ConnectorSyncWorkLedgerRepository,
)


NOW = datetime(2026, 8, 28, 12, tzinfo=timezone.utc)
COMMIT = "a" * 40
TREE = "b" * 40
PROFILE = "c" * 64


def _context():
    organization_id, connector_id, scope_id, job_id = (uuid4() for _ in range(4))
    authorization = GitHubRepositoryContentAuthorization(
        organization_id,
        connector_id,
        scope_id,
        uuid4(),
        uuid4(),
        101,
        202,
        303,
        "sandbox-org",
        404,
        "repository",
        "sandbox-org/repository",
        "sandbox-org",
        "github:repository:404",
        "main",
    )
    snapshot = GitHubRepositorySnapshot(
        connector_id,
        scope_id,
        404,
        "github:repository:404",
        "main",
        COMMIT,
        TREE,
    )
    return organization_id, connector_id, scope_id, job_id, authorization, snapshot


def _entry(snapshot, path, *, object_id=None, size=100, entry_type="regular_blob"):
    return GitHubRepositoryEntry(
        snapshot.connector_id,
        snapshot.scope_id,
        snapshot.repository_id,
        snapshot.canonical_repository_identity,
        snapshot.commit_object_id,
        snapshot.root_tree_object_id,
        snapshot.root_tree_object_id,
        path.rsplit("/", 1)[-1],
        path,
        entry_type,
        object_id or f"{abs(hash(path)) % (16**40):040x}",
        size,
        False,
    )


def _service():
    repository = Mock(spec=ConnectorSyncWorkLedgerRepository)
    generation_id = uuid4()
    repository.register_generation.return_value = (
        SimpleNamespace(generation_id=generation_id),
        True,
    )
    repository.register_discovery_batch.side_effect = (
        lambda _organization, _generation, observations, entries, now: (
        DiscoveryRegistrationResult(
            generation_id,
            len(observations),
            0,
            len(entries),
            0,
            tuple(uuid4() for _ in observations),
            tuple(uuid4() for _ in entries),
        )
    ))
    return GitHubSyncWorkPlanningService(repository), repository, generation_id


def _register(service, context, entries):
    organization_id, connector_id, scope_id, job_id, authorization, snapshot = context
    return service.register_manifest_batch(
        organization_id=organization_id,
        connector_id=connector_id,
        connector_scope_id=scope_id,
        sync_job_id=job_id,
        authorization=authorization,
        snapshot=snapshot,
        profile_fingerprint=PROFILE,
        entries=entries,
        now=NOW,
    )


def test_supported_manifest_entries_preserve_exact_pinned_attribution() -> None:
    context = _context()
    service, repository, generation_id = _service()
    snapshot = context[-1]
    result = _register(
        service,
        context,
        (
            _entry(snapshot, "documents/one.md", object_id="1" * 40, size=11),
            _entry(snapshot, "documents/two.PDF", object_id="2" * 40, size=22),
        ),
    )

    request = repository.register_generation.call_args.args[0]
    assert request.organization_id == context[0]
    assert request.connector_id == context[1]
    assert request.connector_scope_id == context[2]
    assert request.sync_job_id == context[3]
    assert request.repository_identity == "github:repository:404"
    assert request.branch_name == "main"
    assert request.commit_object_id == COMMIT
    assert request.root_tree_object_id == TREE
    assert request.profile_fingerprint == PROFILE
    observations = repository.register_discovery_batch.call_args.args[2]
    manifest = repository.register_discovery_batch.call_args.args[3]
    assert [row.repository_path for row in observations] == [
        "documents/one.md",
        "documents/two.PDF",
    ]
    assert [entry.repository_path for entry in manifest] == [
        "documents/one.md",
        "documents/two.PDF",
    ]
    assert [entry.file_extension for entry in manifest] == [".md", ".pdf"]
    assert all(entry.provider_revision_id == COMMIT for entry in manifest)
    assert all(entry.profile_fingerprint == PROFILE for entry in manifest)
    assert result.generation_id == generation_id
    assert (
        result.observed_count,
        result.created_observation_count,
        result.eligible_count,
        result.created_count,
        result.existing_count,
    ) == (2, 2, 2, 2, 0)


def test_unsupported_oversized_and_nonregular_entries_never_create_work() -> None:
    context = _context()
    service, repository, _generation_id = _service()
    snapshot = context[-1]
    result = _register(
        service,
        context,
        (
            _entry(snapshot, "code.py"),
            _entry(snapshot, "large.md", size=MAX_GITHUB_BLOB_BYTES + 1),
            _entry(snapshot, "link.md", entry_type="symlink"),
        ),
    )
    assert result.eligible_count == 0
    assert result.observed_count == 3
    observations = repository.register_discovery_batch.call_args.args[2]
    assert [row.disposition.value for row in observations] == [
        "unsupported_format",
        "oversized",
        "unsupported_object_type",
    ]
    assert repository.register_discovery_batch.call_args.args[3] == ()


def test_exact_duplicates_collapse_and_conflicting_source_identity_fails_closed() -> None:
    context = _context()
    snapshot = context[-1]
    service, repository, _generation_id = _service()
    entry = _entry(snapshot, "documents/repeated.md", object_id="1" * 40)
    result = _register(service, context, (entry, entry))
    assert result.eligible_count == 1
    assert len(repository.register_discovery_batch.call_args.args[2]) == 1
    assert len(repository.register_discovery_batch.call_args.args[3]) == 1

    conflicting = _entry(snapshot, entry.path, object_id="2" * 40)
    with pytest.raises(InvalidGitHubSyncWorkPlanningRequest, match="duplicated"):
        _register(service, context, (entry, conflicting))


def test_manifest_batch_limit_and_cross_tenant_attribution_fail_before_writes() -> None:
    context = _context()
    snapshot = context[-1]
    service, repository, _generation_id = _service()
    entries = tuple(_entry(snapshot, f"documents/{index:04d}.md") for index in range(501))
    with pytest.raises(InvalidGitHubSyncWorkPlanningRequest, match="500"):
        _register(service, context, entries)
    repository.register_generation.assert_not_called()

    foreign = list(context)
    foreign[0] = uuid4()
    with pytest.raises(InvalidGitHubSyncWorkPlanningRequest, match="attribution"):
        _register(service, tuple(foreign), (_entry(snapshot, "one.md"),))
    repository.register_generation.assert_not_called()


@pytest.mark.parametrize(
    "entry",
    (
        lambda snapshot: _entry(snapshot, "../escape.md"),
        lambda snapshot: _entry(snapshot, "documents\\escape.md"),
        lambda snapshot: _entry(snapshot, "documents/file.md", object_id="not-an-object"),
        lambda snapshot: _entry(snapshot, "documents/file.md", size=-1),
        lambda snapshot: _entry(snapshot, "documents/file.md", entry_type="unknown"),
    ),
)
def test_malformed_paths_objects_sizes_and_types_fail_before_writes(entry) -> None:
    context = _context()
    service, repository, _generation_id = _service()
    with pytest.raises(InvalidGitHubSyncWorkPlanningRequest):
        _register(service, context, (entry(context[-1]),))
    repository.register_generation.assert_not_called()


def test_completion_is_idempotently_bound_to_the_exact_generation() -> None:
    context = _context()
    service, repository, generation_id = _service()
    repository.mark_discovery_complete.return_value = SimpleNamespace(
        generation_id=generation_id,
        discovery_complete=True,
    )
    organization_id, connector_id, scope_id, job_id, authorization, snapshot = context
    result = service.mark_discovery_complete(
        organization_id=organization_id,
        connector_id=connector_id,
        connector_scope_id=scope_id,
        sync_job_id=job_id,
        authorization=authorization,
        snapshot=snapshot,
        profile_fingerprint=PROFILE,
        now=NOW,
    )
    assert result.discovery_complete is True
    repository.mark_discovery_complete.assert_called_once_with(
        organization_id,
        generation_id,
        now=NOW,
        reservation_id=None,
        planner_lease_id=None,
    )


def test_controlled_completion_forwards_reservation_and_planner_fence() -> None:
    context = _context()
    service, repository, generation_id = _service()
    reservation_id, planner_lease_id = uuid4(), uuid4()
    repository.mark_discovery_complete.return_value = SimpleNamespace(
        generation_id=generation_id,
        discovery_complete=True,
    )
    organization_id, connector_id, scope_id, job_id, authorization, snapshot = context

    service.mark_discovery_complete(
        organization_id=organization_id,
        connector_id=connector_id,
        connector_scope_id=scope_id,
        sync_job_id=job_id,
        authorization=authorization,
        snapshot=snapshot,
        profile_fingerprint=PROFILE,
        now=NOW,
        reservation_id=reservation_id,
        planner_lease_id=planner_lease_id,
    )

    repository.mark_discovery_complete.assert_called_once_with(
        organization_id,
        generation_id,
        now=NOW,
        reservation_id=reservation_id,
        planner_lease_id=planner_lease_id,
    )
