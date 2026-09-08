from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from application.services.github_staged_synchronization_service import (
    GitHubDiscoveryBatch,
    GitHubSynchronizationSnapshot,
    GitHubTraversalCursor,
)
from infrastructure.workers.connector_sync_worker_host import ConnectorWorkerSettings
from infrastructure.workers.github_sync_ledger_planner_host import (
    GitHubSyncLedgerPlannerHost,
    _ProfileOnlyEmbeddingProvider,
)
from infrastructure.workers.github_sync_ledger_planner_worker import (
    GitHubSyncLedgerPlannerWorker,
)
from infrastructure.workers.local_folder_sync_worker import LocalFolderWorkerResult


NOW = datetime(2026, 9, 7, tzinfo=timezone.utc)


def _settings() -> ConnectorWorkerSettings:
    return ConnectorWorkerSettings(
        "planner",
        timedelta(minutes=15),
        timedelta(minutes=1),
        timedelta(seconds=1),
        timedelta(minutes=5),
        10,
        True,
    )


def test_planner_execution_budget_defaults_and_bounds_are_strict() -> None:
    settings = ConnectorWorkerSettings.from_environment([], {})
    assert settings.planner_max_batches_per_execution == 5_000
    assert settings.planner_max_execution_duration == timedelta(minutes=20)

    for values in (
        {"GITHUB_LEDGER_PLANNER_MAX_BATCHES": "0"},
        {"GITHUB_LEDGER_PLANNER_MAX_BATCHES": "100001"},
        {"GITHUB_LEDGER_PLANNER_MAX_SECONDS": "0"},
        {"GITHUB_LEDGER_PLANNER_MAX_SECONDS": "3601"},
    ):
        try:
            ConnectorWorkerSettings.from_environment([], values)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid planner execution budget was accepted")


def test_disabled_planner_host_claims_nothing() -> None:
    sessions = Mock()
    execution = Mock()
    host = GitHubSyncLedgerPlannerHost(
        sessions,
        execution,
        None,
        _settings(),
        planning_enabled=False,
    )

    assert host.run() == 0
    sessions.assert_not_called()
    execution.assert_not_called()


def test_planner_profile_provider_cannot_embed() -> None:
    provider = _ProfileOnlyEmbeddingProvider()
    assert provider.profile.dimension == 1536
    try:
        provider.embed_batch(())
    except RuntimeError as error:
        assert str(error) == "planner cannot execute embedding operations"
    else:
        raise AssertionError("planner profile provider performed embedding")


def _acquired_attempt():
    lease = SimpleNamespace(
        organization_id=uuid4(),
        job_id=uuid4(),
        connector_id=uuid4(),
        connector_scope_id=uuid4(),
        attempt_number=1,
        lease_id=uuid4(),
        fencing_token=1,
        lease_expires_at=NOW + timedelta(minutes=15),
        mode="incremental",
        trigger_type="manual",
        max_attempts=3,
    )
    return SimpleNamespace(lease=lease, sync_run_id=uuid4())


def test_planner_host_stops_truthfully_at_execution_batch_budget() -> None:
    worker = Mock()
    worker.execute.return_value = LocalFolderWorkerResult(
        "in_progress", uuid4(), uuid4(), 1, 1
    )
    logger = Mock()
    host = GitHubSyncLedgerPlannerHost(
        Mock(),
        Mock(),
        worker,
        replace(_settings(), planner_max_batches_per_execution=1),
        planning_enabled=True,
        monotonic=lambda: 0.0,
        logger=logger,
    )
    host._recover_and_claim = Mock(return_value=_acquired_attempt())  # type: ignore[method-assign]

    assert host.run() == 0
    worker.execute.assert_called_once()
    assert "outcome=resumable stop_reason=batch_limit" in logger.info.call_args.args[0]


def test_planner_host_stops_before_another_batch_at_runtime_budget() -> None:
    worker = Mock()
    logger = Mock()
    monotonic = Mock(side_effect=(0.0, 61.0))
    host = GitHubSyncLedgerPlannerHost(
        Mock(),
        Mock(),
        worker,
        replace(
            _settings(), planner_max_execution_duration=timedelta(seconds=60)
        ),
        planning_enabled=True,
        monotonic=monotonic,
        logger=logger,
    )
    host._recover_and_claim = Mock(return_value=_acquired_attempt())  # type: ignore[method-assign]

    assert host.run() == 0
    worker.execute.assert_not_called()
    assert "outcome=resumable stop_reason=runtime_limit" in logger.info.call_args.args[0]


def test_planner_worker_routes_discovery_only_to_planning_persistence() -> None:
    authorization = Mock()
    snapshot_value = SimpleNamespace(
        connector_id=uuid4(),
        scope_id=uuid4(),
        repository_id=123,
        canonical_repository_identity="github:repository:123",
        default_branch_name="main",
        commit_object_id="a" * 40,
        root_tree_object_id="b" * 40,
    )
    cursor = Mock(spec=GitHubTraversalCursor)
    cursor.phase = "traversal"
    cursor.scan_complete = False
    cursor.snapshot = snapshot_value
    sync_snapshot = Mock(spec=GitHubSynchronizationSnapshot)
    sync_snapshot.cursor = cursor
    sync_snapshot.authorization = authorization
    batch = Mock(spec=GitHubDiscoveryBatch)
    preparation = Mock()
    preparation.discover_batch.return_value = batch
    staged = Mock()
    staged.snapshot.return_value = sync_snapshot
    staged.persist_planning_batch.return_value = SimpleNamespace(outcome="completed")
    worker = GitHubSyncLedgerPlannerWorker(
        Mock(),
        Mock(),
        lambda _session: staged,
        preparation,
        worker_id="planner",
        lease_duration=timedelta(minutes=15),
        heartbeat_interval=timedelta(minutes=1),
        heartbeat_shutdown_timeout=timedelta(minutes=5),
    )
    worker._read = lambda operation: operation(staged)  # type: ignore[method-assign]
    worker._write = lambda operation, *_args: operation(staged)  # type: ignore[method-assign]
    worker._progress = Mock()  # type: ignore[method-assign]
    heartbeat = Mock()
    lease = Mock()

    assert worker._continue(lease, uuid4(), heartbeat) == "completed"
    preparation.discover_batch.assert_called_once()
    staged.persist_planning_batch.assert_called_once()
    assert not hasattr(staged, "persist_batch") or staged.persist_batch.call_count == 0
    assert not hasattr(staged, "reconcile") or staged.reconcile.call_count == 0
