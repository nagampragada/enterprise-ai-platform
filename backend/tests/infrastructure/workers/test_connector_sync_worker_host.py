from datetime import timedelta
from unittest.mock import Mock
from uuid import uuid4

from application.services.connector_sync_execution_service import AcquiredRoutedSyncAttempt
from infrastructure.repositories.connector_sync_job_repository import SyncJobLease
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
