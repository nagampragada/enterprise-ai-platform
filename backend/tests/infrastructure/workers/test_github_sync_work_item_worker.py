from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.services.connector_sync_retry_policy import ConnectorSyncRetryPolicy
from application.services.github_staged_synchronization_service import (
    InvalidGitHubStagedSynchronizationRequest,
)
from domain.connectors.sync_work_ledger import FileWorkLease, FileWorkStatus
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    FileWorkCancellationConflict,
)
import infrastructure.workers.github_sync_work_item_worker as worker_module
from infrastructure.workers.github_sync_work_item_worker import (
    GitHubSyncWorkItemWorker,
)


NOW = datetime(2026, 8, 31, 12, tzinfo=timezone.utc)


def _lease():
    return FileWorkLease(
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
        "worker-1",
        uuid4(),
        1,
        1,
        3,
        NOW + timedelta(minutes=15),
    )


def _worker(preparation=None):
    return GitHubSyncWorkItemWorker(
        Mock(),
        Mock(),
        preparation or Mock(),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        worker_id="worker-1",
        lease_duration=timedelta(minutes=15),
        heartbeat_interval=timedelta(minutes=1),
        heartbeat_shutdown_timeout=timedelta(seconds=2),
        recovery_limit=10,
        clock=lambda: NOW,
    )


class _Heartbeat:
    def __init__(self, *args, **kwargs):
        self.raise_if_failed = Mock()
        self.stop = Mock()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stop()


def test_no_claimed_item_performs_no_provider_or_persistence_work():
    preparation = Mock()
    worker = _worker(preparation)
    worker._recover_and_claim = Mock(return_value=None)

    assert worker.execute_one() == "no_work"
    preparation.prepare_file.assert_not_called()


def test_one_claimed_item_is_prepared_and_completed(monkeypatch):
    monkeypatch.setattr(worker_module, "FileWorkLeaseHeartbeat", _Heartbeat)
    preparation = Mock()
    service = Mock()
    lease = _lease()
    context = SimpleNamespace(
        authorization=Mock(),
        snapshot=Mock(),
        entry=Mock(),
        item_snapshot=Mock(),
    )
    prepared = Mock()
    service.load_context.return_value = context
    service.persist.return_value = SimpleNamespace(
        work_item=SimpleNamespace(status=FileWorkStatus.SUCCEEDED)
    )
    preparation.prepare_file.return_value = prepared
    worker = _worker(preparation)
    worker._recover_and_claim = Mock(return_value=lease)
    worker._transaction = lambda operation: operation(service)

    assert worker.execute_one() == "completed"
    preparation.prepare_file.assert_called_once()
    service.persist.assert_called_once()
    assert service.persist.call_args.args[:3] == (lease, context, prepared)


def test_cancellation_before_provider_work_is_acknowledged(monkeypatch):
    monkeypatch.setattr(worker_module, "FileWorkLeaseHeartbeat", _Heartbeat)
    preparation = Mock()
    service = Mock()
    lease = _lease()
    service.load_context.side_effect = FileWorkCancellationConflict("cancelled")
    worker = _worker(preparation)
    worker._recover_and_claim = Mock(return_value=lease)
    worker._transaction = lambda operation: operation(service)
    worker._cancel = Mock(return_value="cancelled")

    assert worker.execute_one() == "cancelled"
    worker._cancel.assert_called_once_with(lease)
    preparation.prepare_file.assert_not_called()


def test_retryable_provider_failure_uses_existing_failure_transition(monkeypatch):
    monkeypatch.setattr(worker_module, "FileWorkLeaseHeartbeat", _Heartbeat)
    preparation = Mock()
    service = Mock()
    lease = _lease()
    context = SimpleNamespace(
        authorization=Mock(),
        snapshot=Mock(),
        entry=Mock(),
        item_snapshot=Mock(),
    )
    service.load_context.return_value = context
    preparation.prepare_file.side_effect = TimeoutError("safe")
    worker = _worker(preparation)
    worker._recover_and_claim = Mock(return_value=lease)
    worker._transaction = lambda operation: operation(service)
    worker._fail = Mock(return_value="retry_scheduled")

    assert worker.execute_one() == "retry_scheduled"
    worker._fail.assert_called_once()
    service.persist.assert_not_called()


def test_transaction_rolls_back_and_closes_on_persistence_failure():
    session = Mock()
    service = Mock()
    worker = GitHubSyncWorkItemWorker(
        lambda: session,
        lambda _session: service,
        Mock(),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: low),
        worker_id="worker-1",
        lease_duration=timedelta(minutes=15),
        heartbeat_interval=timedelta(minutes=1),
        heartbeat_shutdown_timeout=timedelta(seconds=2),
        recovery_limit=10,
        clock=lambda: NOW,
    )

    with pytest.raises(RuntimeError, match="persist failed"):
        worker._transaction(lambda _service: (_ for _ in ()).throw(RuntimeError("persist failed")))

    session.commit.assert_not_called()
    session.rollback.assert_called_once()
    session.close.assert_called_once()


def test_retryable_failure_records_only_existing_safe_classification(monkeypatch):
    repository = Mock()
    repository.record_failure.return_value = SimpleNamespace(
        status=SimpleNamespace(value="retry_wait")
    )
    monkeypatch.setattr(
        worker_module,
        "ConnectorSyncWorkLedgerRepository",
        Mock(return_value=repository),
    )
    session = Mock()
    worker = _worker()
    worker._sessions = lambda: session

    assert worker._fail(_lease(), TimeoutError("unsafe provider detail")) == "retry_scheduled"

    kwargs = repository.record_failure.call_args.kwargs
    assert kwargs["error_category"] == "source_read"
    assert kwargs["error_code"] == "network_temporarily_unavailable"
    assert kwargs["retry_at"] == NOW + timedelta(seconds=15)
    assert kwargs["quarantine_reason_code"] is None
    assert "unsafe provider detail" not in repr(kwargs)


def test_permanent_validation_failure_is_quarantined_with_safe_code(monkeypatch):
    repository = Mock()
    repository.record_failure.return_value = SimpleNamespace(
        status=SimpleNamespace(value="quarantined")
    )
    monkeypatch.setattr(
        worker_module,
        "ConnectorSyncWorkLedgerRepository",
        Mock(return_value=repository),
    )
    session = Mock()
    worker = _worker()
    worker._sessions = lambda: session

    assert worker._fail(
        _lease(), InvalidGitHubStagedSynchronizationRequest("unsafe detail")
    ) == "quarantined"

    kwargs = repository.record_failure.call_args.kwargs
    assert kwargs["error_category"] == "configuration"
    assert kwargs["error_code"] == "request_invalid"
    assert kwargs["retry_at"] is None
    assert kwargs["quarantine_reason_code"] == "request_invalid"
    assert "unsafe detail" not in repr(kwargs)
