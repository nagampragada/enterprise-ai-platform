from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

import infrastructure.workers.github_sync_ledger_planner_host as planner_module
from app.config import GitHubPlannerClaimTarget
from application.services.connector_sync_execution_service import (
    AcquiredSyncAttempt,
    TargetedSyncAttemptResult,
)
from application.services.github_staged_synchronization_service import (
    GitHubDiscoveryBatch,
    GitHubSynchronizationSnapshot,
    GitHubTraversalCursor,
)
from infrastructure.workers.connector_sync_worker_host import ConnectorWorkerSettings
from infrastructure.workers.github_sync_ledger_planner_host import (
    GitHubSyncLedgerPlannerHost,
    _PlannerClaimResult,
    _ProfileOnlyEmbeddingProvider,
    compose_github_sync_ledger_planner_host,
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


def test_disabled_planner_main_with_valid_target_never_composes_or_opens_session(
    monkeypatch,
) -> None:
    acquired = _acquired_attempt()
    target = _target_for(acquired)
    sessions = Mock()
    compose = Mock(side_effect=AssertionError("provider composition was called"))
    monkeypatch.setattr(
        planner_module,
        "validate_github_planner_process_environment",
        Mock(
            return_value=SimpleNamespace(
                github_sync_ledger_planning_enabled=False,
                claim_target=target,
            )
        ),
    )
    monkeypatch.setattr(
        planner_module.ConnectorWorkerSettings,
        "from_environment",
        Mock(return_value=_settings()),
    )
    monkeypatch.setattr(planner_module, "SessionLocal", sessions)
    monkeypatch.setattr(planner_module, "compose_github_sync_ledger_planner_host", compose)
    monkeypatch.setattr(planner_module, "install_shutdown_signal_handlers", Mock())

    assert planner_module.main([]) == 0
    compose.assert_not_called()
    sessions.assert_not_called()


def test_enabled_planner_composition_threads_target_and_retry_dependency(
    monkeypatch,
) -> None:
    acquired = _acquired_attempt()
    target = _target_for(acquired)
    process_settings = SimpleNamespace(
        github_sync_ledger_planning_enabled=True,
        claim_target=target,
        secret_manager=Mock(),
        github=Mock(),
    )
    retry = Mock()
    retry_type = Mock(return_value=retry)
    preparation = Mock()
    preparation.profile = Mock()
    worker = Mock()
    random_uniform = Mock()
    monkeypatch.setattr(planner_module, "ConnectorSyncRetryPolicy", retry_type)
    monkeypatch.setattr(
        planner_module,
        "GoogleSecretManagerSecretStore",
        Mock(return_value=Mock()),
    )
    monkeypatch.setattr(planner_module, "GitHubAppRestClient", Mock(return_value=Mock()))
    monkeypatch.setattr(
        planner_module,
        "GitHubRepositoryContentService",
        Mock(return_value=Mock()),
    )
    monkeypatch.setattr(
        planner_module,
        "GitHubSynchronizationPreparationService",
        Mock(return_value=preparation),
    )
    monkeypatch.setattr(
        planner_module,
        "GitHubSyncLedgerPlannerWorker",
        Mock(return_value=worker),
    )

    host = compose_github_sync_ledger_planner_host(
        _settings(),
        process_settings=process_settings,
        session_factory=Mock(),
        random_uniform=random_uniform,
    )

    assert host._claim_target == target
    assert host._worker is worker
    retry_type.assert_called_once_with(random_uniform=random_uniform)


def test_enabled_planner_composes_real_internal_graph_with_external_substitutes(
    monkeypatch,
) -> None:
    acquired = _acquired_attempt()
    target = _target_for(acquired)
    process_settings = SimpleNamespace(
        github_sync_ledger_planning_enabled=True,
        claim_target=target,
        secret_manager=Mock(),
        github=Mock(),
    )
    secret_store = Mock()
    github_client = Mock()
    monkeypatch.setattr(
        planner_module,
        "GoogleSecretManagerSecretStore",
        Mock(return_value=secret_store),
    )
    client_type = Mock(return_value=github_client)
    monkeypatch.setattr(planner_module, "GitHubAppRestClient", client_type)
    sessions = Mock()

    host = compose_github_sync_ledger_planner_host(
        _settings(),
        process_settings=process_settings,
        session_factory=sessions,
        random_uniform=lambda lower, upper: lower,
    )

    assert host._claim_target == target
    assert isinstance(host._worker, GitHubSyncLedgerPlannerWorker)
    assert isinstance(
        host._worker._preparation,
        planner_module.GitHubSynchronizationPreparationService,
    )
    staged = host._worker._staged(Mock())
    assert isinstance(staged, planner_module.GitHubStagedSynchronizationService)
    assert staged._ledger_planning_enabled is True
    client_type.assert_called_once_with(process_settings.github, secret_store)
    sessions.assert_not_called()


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
    return AcquiredSyncAttempt(lease=lease, sync_run_id=uuid4())


def _target_for(acquired) -> GitHubPlannerClaimTarget:
    lease = acquired.lease
    return GitHubPlannerClaimTarget(
        lease.organization_id,
        lease.connector_id,
        lease.connector_scope_id,
        lease.job_id,
    )


def test_targeted_host_uses_only_exact_recovery_and_claim_without_fallback() -> None:
    acquired = _acquired_attempt()
    target = _target_for(acquired)
    session = Mock()
    sessions = Mock(return_value=session)
    execution = Mock()
    execution.recover_expired_target_github.return_value = ()
    execution.acquire_target_github.return_value = TargetedSyncAttemptResult(
        "not_eligible"
    )
    host = GitHubSyncLedgerPlannerHost(
        sessions,
        Mock(return_value=execution),
        Mock(),
        _settings(),
        planning_enabled=True,
        claim_target=target,
    )

    assert host.run() == 1
    execution.recover_expired_target_github.assert_called_once_with(
        target.organization_id,
        target.connector_id,
        target.connector_scope_id,
        target.sync_job_id,
    )
    execution.acquire_target_github.assert_called_once()
    execution.recover_expired_github.assert_not_called()
    execution.acquire_one_github.assert_not_called()
    session.commit.assert_called_once()
    session.rollback.assert_not_called()
    session.close.assert_called_once()


@pytest.mark.parametrize(
    ("outcome", "attempt"),
    (
        ("acquired", None),
        ("acquired", object()),
        ("completed", _acquired_attempt()),
        ("unknown", None),
    ),
)
def test_targeted_attempt_result_rejects_inconsistent_outcomes(outcome, attempt) -> None:
    with pytest.raises(
        ValueError,
        match="targeted synchronization attempt result is invalid",
    ):
        TargetedSyncAttemptResult(outcome, attempt)


@pytest.mark.parametrize(
    ("outcome", "attempt"),
    (("acquired", None), ("completed", _acquired_attempt()), ("unknown", None)),
)
def test_planner_claim_result_rejects_inconsistent_outcomes(outcome, attempt) -> None:
    with pytest.raises(ValueError, match="planner claim result is invalid"):
        _PlannerClaimResult(outcome, attempt)


def test_targeted_host_distinguishes_completed_target_from_empty_global_queue() -> None:
    acquired = _acquired_attempt()
    target = _target_for(acquired)
    targeted_execution = Mock()
    targeted_execution.recover_expired_target_github.return_value = ()
    targeted_execution.acquire_target_github.return_value = TargetedSyncAttemptResult(
        "completed"
    )
    targeted = GitHubSyncLedgerPlannerHost(
        Mock(return_value=Mock()),
        Mock(return_value=targeted_execution),
        Mock(),
        _settings(),
        planning_enabled=True,
        claim_target=target,
    )
    global_execution = Mock()
    global_execution.recover_expired_github.return_value = ()
    global_execution.acquire_one_github.return_value = None
    global_host = GitHubSyncLedgerPlannerHost(
        Mock(return_value=Mock()),
        Mock(return_value=global_execution),
        Mock(),
        _settings(),
        planning_enabled=True,
    )

    assert targeted.run() == 1
    assert global_host.run() == 0


@pytest.mark.parametrize(
    "outcome",
    (
        "completed",
        "cancelled",
        "failed",
        "attempts_exhausted",
        "owned_elsewhere",
        "retry_not_due",
        "not_eligible",
        "not_found_or_mismatched",
    ),
)
def test_targeted_host_reports_every_miss_without_processing(outcome) -> None:
    acquired = _acquired_attempt()
    target = _target_for(acquired)
    execution = Mock()
    execution.recover_expired_target_github.return_value = ()
    execution.acquire_target_github.return_value = TargetedSyncAttemptResult(outcome)
    worker = Mock()
    logger = Mock()
    host = GitHubSyncLedgerPlannerHost(
        Mock(return_value=Mock()),
        Mock(return_value=execution),
        worker,
        _settings(),
        planning_enabled=True,
        claim_target=target,
        logger=logger,
    )

    assert host.run() == 1
    worker.execute.assert_not_called()
    logger.error.assert_called_once_with(
        "event=github_ledger_planner_target_not_acquired outcome=%s "
        "organization_id=%s connector_id=%s connector_scope_id=%s job_id=%s",
        outcome,
        target.organization_id,
        target.connector_id,
        target.connector_scope_id,
        target.sync_job_id,
    )


def test_targeted_host_claims_once_and_drains_multiple_batches_for_same_job() -> None:
    acquired = _acquired_attempt()
    target = _target_for(acquired)
    execution = Mock()
    execution.recover_expired_target_github.return_value = ()
    execution.acquire_target_github.return_value = TargetedSyncAttemptResult(
        "acquired", acquired
    )
    worker = Mock()
    worker.execute.side_effect = (
        LocalFolderWorkerResult("in_progress", uuid4(), uuid4(), 1, 1),
        LocalFolderWorkerResult("completed", uuid4(), uuid4(), 1, 1),
    )
    host = GitHubSyncLedgerPlannerHost(
        Mock(return_value=Mock()),
        Mock(return_value=execution),
        worker,
        _settings(),
        planning_enabled=True,
        claim_target=target,
        monotonic=lambda: 0.0,
    )

    assert host.run() == 0
    execution.acquire_target_github.assert_called_once()
    execution.acquire_one_github.assert_not_called()
    assert worker.execute.call_count == 2
    assert all(call.args[0].job_id == target.sync_job_id for call in worker.execute.call_args_list)


@pytest.mark.parametrize(
    ("worker_outcome", "expected_exit"),
    (("cancelled", 0), ("lease_lost", 1), ("retry_scheduled", 1)),
)
def test_targeted_host_never_reacquires_after_terminal_attempt_outcome(
    worker_outcome,
    expected_exit,
) -> None:
    acquired = _acquired_attempt()
    target = _target_for(acquired)
    execution = Mock()
    execution.recover_expired_target_github.return_value = ()
    execution.acquire_target_github.return_value = TargetedSyncAttemptResult(
        "acquired", acquired
    )
    worker = Mock()
    worker.execute.return_value = LocalFolderWorkerResult(
        worker_outcome,
        target.sync_job_id,
        acquired.sync_run_id,
        1,
        1,
    )
    host = GitHubSyncLedgerPlannerHost(
        Mock(return_value=Mock()),
        Mock(return_value=execution),
        worker,
        _settings(),
        planning_enabled=True,
        claim_target=target,
        monotonic=lambda: 0.0,
    )

    assert host.run() == expected_exit
    execution.acquire_target_github.assert_called_once()
    execution.acquire_one_github.assert_not_called()
    worker.execute.assert_called_once()


def test_target_identity_mismatch_rolls_back_claim_transaction() -> None:
    acquired = _acquired_attempt()
    target = _target_for(acquired)
    acquired.lease.connector_scope_id = uuid4()
    session = Mock()
    execution = Mock()
    execution.recover_expired_target_github.return_value = ()
    execution.acquire_target_github.return_value = TargetedSyncAttemptResult(
        "acquired", acquired
    )
    host = GitHubSyncLedgerPlannerHost(
        Mock(return_value=session),
        Mock(return_value=execution),
        Mock(),
        _settings(),
        planning_enabled=True,
        claim_target=target,
    )

    with pytest.raises(RuntimeError, match="targeted planner claim identity mismatch"):
        host._recover_and_claim()
    session.commit.assert_not_called()
    session.rollback.assert_called_once()
    session.close.assert_called_once()


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
    host._recover_and_claim = Mock(  # type: ignore[method-assign]
        return_value=_PlannerClaimResult("acquired", _acquired_attempt())
    )

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
    host._recover_and_claim = Mock(  # type: ignore[method-assign]
        return_value=_PlannerClaimResult("acquired", _acquired_attempt())
    )

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
