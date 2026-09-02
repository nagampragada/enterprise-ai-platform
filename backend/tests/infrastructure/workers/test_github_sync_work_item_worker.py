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
    FileWorkGracefulShutdownExpired,
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
        work_item=SimpleNamespace(
            status=FileWorkStatus.SUCCEEDED, counters=worker_module.FileWorkCounters()
        )
    )
    preparation.prepare_file.return_value = prepared
    worker = _worker(preparation)
    worker._recover_and_claim = Mock(return_value=lease)
    worker._transaction = lambda operation: operation(service)

    assert worker.execute_one() == "completed"
    preparation.prepare_file.assert_called_once()
    service.persist.assert_called_once()
    assert service.persist.call_args.args[:3] == (lease, context, prepared)


def test_heartbeat_remains_active_until_atomic_completion_returns(monkeypatch):
    events = []

    class RecordingHeartbeat:
        def __init__(self, *args, **kwargs):
            self.raise_if_failed = Mock()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            events.append("heartbeat_stopped")

    monkeypatch.setattr(worker_module, "FileWorkLeaseHeartbeat", RecordingHeartbeat)
    preparation = Mock()
    service = Mock()
    lease = _lease()
    service.load_context.return_value = SimpleNamespace(
        authorization=Mock(), snapshot=Mock(), entry=Mock(), item_snapshot=Mock()
    )

    def persist(*_args, **_kwargs):
        events.append("completion_committed")
        return SimpleNamespace(
            work_item=SimpleNamespace(
                status=FileWorkStatus.SUCCEEDED,
                counters=worker_module.FileWorkCounters(),
            )
        )

    service.persist.side_effect = persist
    preparation.prepare_file.return_value = Mock()
    worker = _worker(preparation)
    worker._recover_and_claim = Mock(return_value=lease)
    worker._transaction = lambda operation: operation(service)

    assert worker.execute_one() == "completed"
    assert events == ["completion_committed", "heartbeat_stopped"]


def test_detailed_result_reports_only_safe_identity_attempt_and_counters(monkeypatch):
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
    counters = worker_module.FileWorkCounters(86, 86, 1, 1)
    service.load_context.return_value = context
    service.persist.return_value = SimpleNamespace(
        work_item=SimpleNamespace(
            status=FileWorkStatus.SUCCEEDED, counters=counters
        )
    )
    preparation.prepare_file.return_value = Mock()
    worker = _worker(preparation)
    worker._recover_and_claim = Mock(return_value=lease)
    worker._transaction = lambda operation: operation(service)

    result = worker.execute_one_result(claim_allowed=lambda: True)

    assert result.outcome == "completed"
    assert result.work_item_id == lease.work_item_id
    assert result.attempt_number == lease.attempt_number
    assert result.counters == counters


def test_claim_gate_is_rechecked_inside_claim_transaction(monkeypatch):
    repository = Mock()
    monkeypatch.setattr(
        worker_module,
        "ConnectorSyncWorkLedgerRepository",
        Mock(return_value=repository),
    )
    session = Mock()
    worker = _worker()
    worker._sessions = lambda: session

    assert worker._recover_and_claim(lambda: False) is None

    repository.recover_expired_available.assert_called_once()
    repository.claim_next_available.assert_not_called()
    session.commit.assert_called_once()
    session.close.assert_called_once()


def test_dedicated_fair_claim_uses_only_fair_repository_path(monkeypatch):
    repository = Mock()
    repository.claim_next_available_fair.return_value = None
    monkeypatch.setattr(
        worker_module,
        "ConnectorSyncWorkLedgerRepository",
        Mock(return_value=repository),
    )
    session = Mock()
    worker = _worker()
    worker._sessions = lambda: session
    worker._organization_fair_claims = True

    assert worker._recover_and_claim() is None

    repository.claim_next_available_fair.assert_called_once()
    repository.claim_next_available.assert_not_called()
    session.commit.assert_called_once()
    session.close.assert_called_once()


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


def test_shutdown_grace_is_checked_after_context_before_provider_work(monkeypatch):
    monkeypatch.setattr(worker_module, "FileWorkLeaseHeartbeat", _Heartbeat)
    preparation = Mock()
    service = Mock()
    lease = _lease()
    service.load_context.return_value = SimpleNamespace(
        authorization=Mock(), snapshot=Mock(), entry=Mock(), item_snapshot=Mock()
    )
    worker = GitHubSyncWorkItemWorker(
        Mock(),
        Mock(),
        preparation,
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: low),
        worker_id="worker-1",
        lease_duration=timedelta(minutes=15),
        heartbeat_interval=timedelta(minutes=1),
        heartbeat_shutdown_timeout=timedelta(seconds=2),
        recovery_limit=10,
        clock=lambda: NOW,
        progress_check=lambda: (_ for _ in ()).throw(
            FileWorkGracefulShutdownExpired("safe")
        ),
    )
    worker._recover_and_claim = Mock(return_value=lease)
    worker._transaction = lambda operation: operation(service)
    worker._fail = Mock(return_value="retry_scheduled")

    result = worker.execute_one_result()

    assert result.outcome == "retry_scheduled"
    assert result.reason_code == "shutdown_grace_expired"
    preparation.prepare_file.assert_not_called()
    worker._fail.assert_called_once()
    assert isinstance(worker._fail.call_args.args[1], FileWorkGracefulShutdownExpired)


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


def test_retry_transition_failure_rolls_back_closes_and_propagates(monkeypatch):
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
    session.commit.side_effect = RuntimeError("commit failed")
    worker = _worker()
    worker._sessions = lambda: session

    with pytest.raises(RuntimeError, match="commit failed"):
        worker._fail(_lease(), TimeoutError("unsafe provider detail"))

    session.rollback.assert_called_once()
    session.close.assert_called_once()


def test_cancellation_transition_failure_rolls_back_closes_and_propagates(monkeypatch):
    repository = Mock()
    monkeypatch.setattr(
        worker_module,
        "ConnectorSyncWorkLedgerRepository",
        Mock(return_value=repository),
    )
    session = Mock()
    session.commit.side_effect = RuntimeError("commit failed")
    worker = _worker()
    worker._sessions = lambda: session

    with pytest.raises(RuntimeError, match="commit failed"):
        worker._cancel(_lease())

    session.rollback.assert_called_once()
    session.close.assert_called_once()


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


def test_expired_shutdown_grace_uses_bounded_internal_retry_transition(monkeypatch):
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

    assert worker._fail(
        _lease(), FileWorkGracefulShutdownExpired("unsafe detail")
    ) == "retry_scheduled"

    kwargs = repository.record_failure.call_args.kwargs
    assert kwargs["error_category"] == "internal"
    assert kwargs["error_code"] == "shutdown_grace_expired"
    assert kwargs["retry_at"] == NOW + timedelta(seconds=15)
    assert "unsafe detail" not in repr(kwargs)


def test_shutdown_retry_does_not_extend_shared_failure_kind_taxonomy():
    from application.services.connector_sync_retry_policy import SyncFailureKind

    assert "graceful_shutdown" not in {kind.value for kind in SyncFailureKind}
