from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

import pytest

from application.services.connector_sync_execution_service import AcquiredSyncAttempt
from domain.embeddings.exceptions import PermanentEmbeddingProviderError, RetryableEmbeddingProviderError
from infrastructure.repositories.connector_sync_job_repository import (
    LostSyncJobLease,
    SyncJobCancellationConflict,
    SyncJobLease,
)
from infrastructure.workers.local_folder_sync_worker import (
    InvalidLocalFolderWorkerConfiguration,
    LocalFolderAttemptContext,
    LocalFolderSyncWorker,
)

NOW = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)


class FakeSession:
    def __init__(self, events: list[str], *, fail_commit: bool = False) -> None:
        self.events = events
        self.fail_commit = fail_commit
        self.closed = False

    def commit(self) -> None:
        self.events.append("commit")
        if self.fail_commit:
            raise RuntimeError("controlled commit failure")

    def rollback(self) -> None:
        self.events.append("rollback")

    def close(self) -> None:
        self.events.append("close")
        self.closed = True


class SessionFactory:
    def __init__(self, *, fail_commit_at: int | None = None) -> None:
        self.events: list[str] = []
        self.sessions: list[FakeSession] = []
        self.fail_commit_at = fail_commit_at

    def __call__(self) -> FakeSession:
        index = len(self.sessions) + 1
        session = FakeSession(self.events, fail_commit=index == self.fail_commit_at)
        self.sessions.append(session)
        self.events.append(f"open:{index}")
        return session


def _lease() -> SyncJobLease:
    return SyncJobLease(
        uuid4(), uuid4(), uuid4(), uuid4(), "incremental", "manual",
        1, 3, uuid4(), 1, NOW + timedelta(minutes=5),
    )


def _context(lease: SyncJobLease | None = None) -> LocalFolderAttemptContext:
    lease = lease or _lease()
    return LocalFolderAttemptContext(
        lease.organization_id,
        lease.job_id,
        lease.connector_id,
        lease.connector_scope_id,
        uuid4(),
        lease.attempt_number,
        "worker-one",
        lease.lease_id,
        lease.fencing_token,
        lease.lease_expires_at,
        lease.mode,
        lease.trigger_type,
        lease.max_attempts,
    )


def _execution(lease: SyncJobLease | None = None) -> Mock:
    lease = lease or _lease()
    service = Mock()
    service.acquire_one.return_value = AcquiredSyncAttempt(lease, uuid4())
    service.heartbeat.return_value = lease
    service.validate_attempt.return_value = SimpleNamespace(cancellation_requested=False)
    service.complete_success.return_value = SimpleNamespace(status="succeeded")
    service.acknowledge_cancellation.return_value = SimpleNamespace(status="cancelled")
    service.fail_attempt.return_value = SimpleNamespace(status="failed")
    return service


def _worker(
    sessions: SessionFactory,
    execution: Mock,
    local: Mock,
    *,
    steps: int = 1,
) -> LocalFolderSyncWorker:
    worker = LocalFolderSyncWorker(
        sessions, lambda session: execution, lambda session: local,
        worker_id="worker-one", steps_per_invocation=steps, batch_size=1,
    )
    worker._validate_local_folder_dispatch = Mock()  # type: ignore[method-assign]
    return worker


def test_no_work_returns_safe_result_and_closes_session():
    sessions, execution, local = SessionFactory(), _execution(), Mock()
    execution.acquire_one.return_value = None
    result = _worker(sessions, execution, local).run_one(uuid4())
    assert result.outcome == "no_work" and result.job_id is None
    assert sessions.events == ["open:1", "rollback", "close"]
    local.synchronize.assert_not_called()


def test_claim_acquires_one_run_commits_closes_and_returns_scalar_immutable_context():
    sessions, lease, local = SessionFactory(), _lease(), Mock()
    execution = _execution(lease)
    context = _worker(sessions, execution, local).claim_one(lease.organization_id)
    assert context is not None
    execution.acquire_one.assert_called_once()
    assert sessions.events == ["open:1", "commit", "close"]
    assert all(
        isinstance(value, (UUID, int, str, datetime))
        for value in context.__dict__.values()
    )
    with pytest.raises(FrozenInstanceError):
        context.attempt_number = 2  # type: ignore[misc]


def test_incomplete_work_is_bounded_and_does_not_finalize_or_consume_retry():
    sessions, lease = SessionFactory(), _lease()
    execution, local = _execution(lease), Mock()
    local.synchronize.return_value = SimpleNamespace(sync_run_id=None, outcome="running")
    context = _context(lease)
    local.synchronize.return_value.sync_run_id = context.sync_run_id
    result = _worker(sessions, execution, local, steps=2).execute(context)
    assert result.outcome == "in_progress" and result.steps_processed == 2
    assert local.synchronize.call_count == 2
    execution.complete_success.assert_not_called()
    execution.fail_attempt.assert_not_called()
    assert all(session.closed for session in sessions.sessions)


def test_complete_work_finalizes_once_and_returns_no_lease_or_provider_data():
    sessions, lease = SessionFactory(), _lease()
    execution, local, context = _execution(lease), Mock(), _context(lease)
    local.synchronize.return_value = SimpleNamespace(
        sync_run_id=context.sync_run_id, outcome="completed"
    )
    result = _worker(sessions, execution, local).execute(context)
    assert result.outcome == "completed" and result.steps_processed == 1
    execution.complete_success.assert_called_once()
    assert not hasattr(result, "lease_id")
    assert not hasattr(result, "path")
    assert not hasattr(result, "content")


def test_cancellation_before_work_prevents_local_service_and_acknowledges():
    sessions, lease = SessionFactory(), _lease()
    execution, local = _execution(lease), Mock()
    execution.heartbeat.side_effect = SyncJobCancellationConflict("pending")
    result = _worker(sessions, execution, local).execute(_context(lease))
    assert result.outcome == "cancelled" and result.steps_processed == 0
    local.synchronize.assert_not_called()
    execution.acknowledge_cancellation.assert_called_once()
    assert all(session.closed for session in sessions.sessions)


def test_cancellation_at_precommit_barrier_rolls_back_and_prevents_success():
    sessions, lease = SessionFactory(), _lease()
    execution, local, context = _execution(lease), Mock(), _context(lease)
    local.synchronize.return_value = SimpleNamespace(
        sync_run_id=context.sync_run_id, outcome="running"
    )
    execution.heartbeat.side_effect = [lease, SyncJobCancellationConflict("pending")]
    result = _worker(sessions, execution, local).execute(context)
    assert result.outcome == "cancelled"
    execution.complete_success.assert_not_called()
    execution.acknowledge_cancellation.assert_called_once()
    assert "rollback" in sessions.events


def test_cancellation_before_success_finalization_rolls_back_and_acknowledges():
    sessions, lease = SessionFactory(), _lease()
    execution, local, context = _execution(lease), Mock(), _context(lease)
    local.synchronize.return_value = SimpleNamespace(
        sync_run_id=context.sync_run_id, outcome="completed"
    )
    execution.complete_success.side_effect = SyncJobCancellationConflict("pending")
    result = _worker(sessions, execution, local).execute(context)
    assert result.outcome == "cancelled"
    execution.acknowledge_cancellation.assert_called_once()
    assert "rollback" in sessions.events
    assert all(session.closed for session in sessions.sessions)


@pytest.mark.parametrize(
    ("error", "status", "outcome"),
    (
        (RetryableEmbeddingProviderError("safe"), "retry_wait", "retry_scheduled"),
        (PermanentEmbeddingProviderError("safe"), "failed", "failed"),
        (Exception("unknown"), "failed", "failed"),
    ),
)
def test_failure_classification_schedules_or_fails_without_immediate_retry(error, status, outcome):
    sessions, lease = SessionFactory(), _lease()
    execution, local = _execution(lease), Mock()
    local.synchronize.side_effect = error
    execution.fail_attempt.return_value = SimpleNamespace(status=status)
    result = _worker(sessions, execution, local).execute(_context(lease))
    assert result.outcome == outcome
    assert local.synchronize.call_count == 1
    execution.fail_attempt.assert_called_once()
    assert all(session.closed for session in sessions.sessions)


def test_lost_lease_never_mutates_outcome():
    sessions, lease = SessionFactory(), _lease()
    execution, local = _execution(lease), Mock()
    execution.heartbeat.side_effect = LostSyncJobLease("lost")
    result = _worker(sessions, execution, local).execute(_context(lease))
    assert result.outcome == "lost_lease"
    execution.fail_attempt.assert_not_called()
    execution.complete_success.assert_not_called()
    execution.acknowledge_cancellation.assert_not_called()


def test_unsupported_or_disabled_dispatch_is_permanent_and_cross_tenant_validation_is_lost():
    sessions, lease = SessionFactory(), _lease()
    execution, local = _execution(lease), Mock()
    worker = _worker(sessions, execution, local)
    worker._validate_local_folder_dispatch.side_effect = RuntimeError("unavailable")  # type: ignore[attr-defined]
    result = worker.execute(_context(lease))
    assert result.outcome == "failed"
    local.synchronize.assert_not_called()
    execution.fail_attempt.assert_called_once()

    sessions, execution = SessionFactory(), _execution(lease)
    execution.validate_attempt.side_effect = LostSyncJobLease("tenant mismatch")
    result = _worker(sessions, execution, local).execute(_context(lease))
    assert result.outcome == "lost_lease"
    execution.fail_attempt.assert_not_called()


@pytest.mark.parametrize(
    "kwargs",
    (
        {"steps_per_invocation": 0},
        {"steps_per_invocation": 11},
        {"lease_duration": timedelta(0)},
        {"heartbeat_target": timedelta(0)},
        {"heartbeat_target": timedelta(minutes=6)},
        {"batch_size": 0},
        {"batch_size": 101},
        {"worker_id": "bad worker"},
    ),
)
def test_invalid_worker_limits_are_rejected(kwargs):
    values = {"worker_id": "worker-one", **kwargs}
    with pytest.raises(InvalidLocalFolderWorkerConfiguration):
        LocalFolderSyncWorker(Mock(), Mock(), Mock(), **values)


def test_failed_commit_rolls_back_closes_and_never_reuses_failed_session():
    sessions, execution, local = SessionFactory(fail_commit_at=1), _execution(), Mock()
    with pytest.raises(RuntimeError):
        _worker(sessions, execution, local).claim_one(uuid4())
    assert sessions.events == ["open:1", "commit", "rollback", "close"]
    assert sessions.sessions[0].closed


def test_transaction_phases_use_distinct_closed_sessions():
    sessions, lease = SessionFactory(), _lease()
    execution, local, context = _execution(lease), Mock(), _context(lease)
    local.synchronize.return_value = SimpleNamespace(
        sync_run_id=context.sync_run_id, outcome="running"
    )
    _worker(sessions, execution, local).execute(context)
    assert len(sessions.sessions) == 2
    assert all(session.closed for session in sessions.sessions)
    assert sessions.events == [
        "open:1", "commit", "close", "open:2", "commit", "close"
    ]


def test_source_has_no_sleep_forever_loop_or_unbounded_step_loop():
    import inspect
    import infrastructure.workers.local_folder_sync_worker as module

    source = inspect.getsource(module)
    assert "sleep(" not in source
    assert "while True" not in source
    assert "range(self._steps_per_invocation)" in source