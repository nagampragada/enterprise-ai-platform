import random
from datetime import timedelta
from unittest.mock import Mock
from uuid import uuid4

from application.services.connector_sync_execution_service import AcquiredRoutedSyncAttempt
from infrastructure.repositories.connector_sync_job_repository import SyncJobLease
import infrastructure.workers.connector_sync_worker_host as worker_host_module
from infrastructure.workers.connector_sync_worker_host import (
    ConnectorSyncWorkerHost,
    ConnectorWorkerSettings,
)
from infrastructure.workers.local_folder_sync_worker import (
    LocalFolderAttemptContext,
    LocalFolderWorkerResult,
)


def _settings():
    return ConnectorWorkerSettings(
        "worker-1", timedelta(minutes=5), timedelta(minutes=1),
        timedelta(seconds=1), timedelta(seconds=2), 10, True,
    )


def _acquired(connector_type):
    lease = SyncJobLease(
        uuid4(), uuid4(), uuid4(), uuid4(), "incremental", "scheduled", 1, 3,
        uuid4(), 1, __import__("datetime").datetime.now(__import__("datetime").UTC)
        + timedelta(minutes=5),
    )
    return AcquiredRoutedSyncAttempt(lease, uuid4(), connector_type)


def _host(connector_type):
    acquired = _acquired(connector_type)
    execution = Mock()
    execution.recover_expired_routed.return_value = ()
    execution.acquire_one_routed.return_value = acquired
    session = Mock()
    local, github = Mock(), Mock()
    local.attempt_context.return_value = LocalFolderAttemptContext(
        acquired.lease.organization_id, acquired.lease.job_id, acquired.lease.connector_id,
        acquired.lease.connector_scope_id, acquired.sync_run_id, 1, "worker-1",
        acquired.lease.lease_id, 1, acquired.lease.lease_expires_at,
        "incremental", "scheduled", 3,
    )
    result = LocalFolderWorkerResult("completed", acquired.lease.job_id,
                                     acquired.sync_run_id, 1, 1)
    local.execute.return_value = result
    github.execute.return_value = result
    host = ConnectorSyncWorkerHost(lambda: session, lambda _s: execution,
                                   local, github, _settings())
    return host, execution, local, github


def test_local_folder_routes_only_to_local_worker():
    host, execution, local, github = _host("local_folder")
    assert host.run_cycle() == "completed"
    local.execute.assert_called_once(); github.execute.assert_not_called()
    execution.acquire_one_routed.assert_called_once_with(
        worker_id="worker-1", lease_duration=timedelta(minutes=5),
    )


def test_github_routes_only_to_github_worker():
    host, _execution, local, github = _host("github")
    assert host.run_cycle() == "completed"
    github.execute.assert_called_once(); local.execute.assert_not_called()


def test_unsupported_persisted_type_fails_nonretryably_without_dispatch():
    host, execution, local, github = _host("future_provider")
    execution.fail_attempt.return_value.status = "failed"
    assert host.run_cycle() == "failed"
    execution.fail_attempt.assert_called_once()
    local.execute.assert_not_called(); github.execute.assert_not_called()


def test_one_shot_no_work_exits_successfully():
    execution = Mock()
    execution.recover_expired_routed.return_value = ()
    execution.acquire_one_routed.return_value = None
    session = Mock()
    host = ConnectorSyncWorkerHost(
        lambda: session,
        lambda _session: execution,
        Mock(),
        Mock(),
        _settings(),
    )

    assert host.run() == 0
    session.commit.assert_called_once()
    session.close.assert_called_once()


def test_legacy_host_excludes_github_claim_and_recovery_when_planner_owns_lane():
    execution = Mock()
    execution.recover_expired_local_folder.return_value = ()
    execution.acquire_one_local_folder.return_value = None
    session = Mock()
    host = ConnectorSyncWorkerHost(
        lambda: session,
        lambda _session: execution,
        Mock(),
        Mock(),
        _settings(),
        github_jobs_enabled=False,
    )

    assert host.run_cycle() == "no_work"
    execution.recover_expired_local_folder.assert_called_once_with(limit=10)
    execution.acquire_one_local_folder.assert_called_once_with(
        worker_id="worker-1", lease_duration=timedelta(minutes=5)
    )
    execution.recover_expired_routed.assert_not_called()
    execution.acquire_one_routed.assert_not_called()


def test_ledger_file_work_is_considered_only_after_legacy_queue_is_empty():
    execution = Mock()
    execution.recover_expired_routed.return_value = ()
    execution.acquire_one_routed.return_value = None
    file_work = Mock()
    file_work.execute_one.return_value = "completed"
    host = ConnectorSyncWorkerHost(
        lambda: Mock(),
        lambda _session: execution,
        Mock(),
        Mock(),
        _settings(),
        github_file_work_worker=file_work,
    )

    assert host.run_cycle() == "completed"
    file_work.execute_one.assert_called_once_with()


def test_legacy_queue_has_priority_over_ledger_file_work():
    host, _execution, _local, _github = _host("github")
    file_work = Mock()
    host._github_file_work = file_work

    assert host.run_cycle() == "completed"
    file_work.execute_one.assert_not_called()


def _patch_composition_dependencies(monkeypatch, captured):
    class RecordingRetryPolicy:
        def __init__(self, *, random_uniform):
            captured["random_uniform"] = random_uniform

    monkeypatch.setattr(worker_host_module, "ConnectorSyncRetryPolicy", RecordingRetryPolicy)
    for name in (
        "GoogleSecretManagerSecretStore",
        "GitHubAppRestClient",
        "OpenAIEmbeddingProvider",
        "create_default_content_extractor_registry",
        "DeterministicTextChunker",
        "LocalFolderPreparationService",
        "LocalFolderSyncWorker",
        "GitHubRepositoryContentService",
        "GitHubSynchronizationPreparationService",
        "GitHubStagedSynchronizationService",
        "GitHubSyncWorker",
        "GitHubSyncWorkProcessingService",
        "ConnectorSyncWorkLedgerRepository",
        "GitHubSyncWorkItemWorker",
    ):
        monkeypatch.setattr(worker_host_module, name, Mock())


def test_default_composition_supplies_system_seeded_retry_jitter(monkeypatch):
    captured = {}
    _patch_composition_dependencies(monkeypatch, captured)

    host = worker_host_module.compose_connector_sync_worker_host(
        _settings(), session_factory=Mock(), process_settings=Mock()
    )

    assert isinstance(host, ConnectorSyncWorkerHost)
    assert callable(captured["random_uniform"])
    assert isinstance(captured["random_uniform"].__self__, random.SystemRandom)


def test_composition_preserves_injected_deterministic_retry_jitter(monkeypatch):
    captured = {}
    _patch_composition_dependencies(monkeypatch, captured)
    deterministic_uniform = lambda low, high: low + ((high - low) / 2)

    worker_host_module.compose_connector_sync_worker_host(
        _settings(),
        session_factory=Mock(),
        process_settings=Mock(),
        random_uniform=deterministic_uniform,
    )

    assert captured["random_uniform"] is deterministic_uniform


def test_composition_propagates_disabled_and_enabled_ledger_planning(monkeypatch):
    captured = {}
    _patch_composition_dependencies(monkeypatch, captured)
    github_worker = worker_host_module.GitHubSyncWorker

    for enabled in (False, True):
        github_worker.reset_mock()
        process_settings = Mock()
        process_settings.github_sync_ledger_planning_enabled = enabled
        host = worker_host_module.compose_connector_sync_worker_host(
            _settings(), session_factory=Mock(), process_settings=process_settings
        )
        assert (
            github_worker.call_args.kwargs["ledger_planning_enabled"] is enabled
        )
        assert host._github_jobs_enabled is (not enabled)
        staged_factory = github_worker.call_args.args[2]
        staged_factory(Mock())
        assert (
            worker_host_module.GitHubStagedSynchronizationService.call_args.kwargs[
                "ledger_planning_enabled"
            ]
            is enabled
        )


def test_composition_only_constructs_ledger_processor_when_explicitly_enabled(monkeypatch):
    captured = {}
    _patch_composition_dependencies(monkeypatch, captured)

    disabled = Mock()
    disabled.github_sync_ledger_planning_enabled = False
    disabled.github_sync_ledger_processing_enabled = False
    worker_host_module.compose_connector_sync_worker_host(
        _settings(), session_factory=Mock(), process_settings=disabled
    )
    worker_host_module.GitHubSyncWorkItemWorker.assert_not_called()

    enabled = Mock()
    enabled.github_sync_ledger_planning_enabled = False
    enabled.github_sync_ledger_processing_enabled = True
    host = worker_host_module.compose_connector_sync_worker_host(
        _settings(), session_factory=Mock(), process_settings=enabled
    )
    worker_host_module.GitHubSyncWorkItemWorker.assert_called_once()
    assert host._github_file_work is worker_host_module.GitHubSyncWorkItemWorker.return_value
