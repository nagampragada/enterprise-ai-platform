from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from infrastructure.repositories.connector_sync_job_repository import (
    SyncJobCancellationConflict,
    SyncJobLease,
)
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    SyncWorkLedgerPersistenceError,
)
from infrastructure.workers.github_sync_worker import GitHubSyncWorker


NOW = datetime(2026, 8, 28, 12, tzinfo=timezone.utc)


def _lease():
    return SyncJobLease(
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
        "incremental",
        "manual",
        1,
        3,
        uuid4(),
        1,
        NOW + timedelta(minutes=10),
    )


def _worker(*, enabled: bool):
    preparation = Mock()
    staged = Mock()
    cursor = SimpleNamespace(phase="traversal")
    snapshot = SimpleNamespace(cursor=cursor, authorization=Mock())
    batch = Mock()
    item_snapshots = (Mock(),)
    prepared = Mock()
    staged.snapshot.return_value = snapshot
    staged.plan_manifest_batch.return_value = Mock()
    staged.item_snapshots.return_value = item_snapshots
    staged.persist_batch.return_value = SimpleNamespace(outcome="in_progress")
    preparation.discover_batch.return_value = batch
    preparation.prepare_batch.return_value = prepared
    worker = GitHubSyncWorker(
        Mock(),
        Mock(),
        Mock(),
        preparation,
        worker_id="worker",
        lease_duration=timedelta(minutes=10),
        heartbeat_interval=timedelta(minutes=1),
        heartbeat_shutdown_timeout=timedelta(minutes=2),
        ledger_planning_enabled=enabled,
        clock=lambda: NOW,
    )
    worker._read = lambda operation: operation(staged)  # type: ignore[method-assign]
    worker._write = (  # type: ignore[method-assign]
        lambda operation, _lease, _run_id, _heartbeat: operation(staged)
    )
    return worker, preparation, staged, batch, prepared


def test_disabled_flag_preserves_legacy_path_without_planning_writes() -> None:
    worker, preparation, staged, batch, prepared = _worker(enabled=False)
    heartbeat = Mock()
    lease = _lease()

    assert worker._continue(lease, uuid4(), heartbeat) == "in_progress"

    staged.plan_manifest_batch.assert_not_called()
    staged.item_snapshots.assert_called_once()
    preparation.prepare_batch.assert_called_once()
    staged.persist_batch.assert_called_once()
    assert preparation.prepare_batch.call_args.args[2] is batch
    assert staged.persist_batch.call_args.args[2] is prepared


def test_enabled_flag_registers_shadow_manifest_before_legacy_preparation() -> None:
    worker, preparation, staged, batch, _prepared = _worker(enabled=True)
    heartbeat = Mock()
    lease = _lease()
    events: list[str] = []
    staged.plan_manifest_batch.side_effect = lambda *args, **kwargs: events.append("plan")
    preparation.prepare_batch.side_effect = lambda *args, **kwargs: (
        events.append("prepare") or Mock()
    )
    staged.persist_batch.side_effect = lambda *args, **kwargs: (
        events.append("persist") or SimpleNamespace(outcome="in_progress")
    )

    assert worker._continue(lease, uuid4(), heartbeat) == "in_progress"

    assert events == ["plan", "prepare", "persist"]
    staged.plan_manifest_batch.assert_called_once()
    assert staged.plan_manifest_batch.call_args.args[2] is batch
    preparation.prepare_batch.assert_called_once()


def test_failure_before_snapshot_pinning_creates_no_generation_or_cursor() -> None:
    worker, preparation, staged, _batch, _prepared = _worker(enabled=True)
    staged.snapshot.return_value = SimpleNamespace(cursor=None, authorization=Mock())
    preparation.resolve_snapshot.side_effect = RuntimeError("provider failed safely")

    with pytest.raises(RuntimeError, match="provider failed safely"):
        worker._continue(_lease(), uuid4(), Mock())

    staged.plan_manifest_batch.assert_not_called()
    staged.pin_snapshot.assert_not_called()
    staged.persist_batch.assert_not_called()


def test_committed_snapshot_pin_resumes_into_shadow_planning() -> None:
    worker, preparation, staged, _batch, _prepared = _worker(enabled=True)
    authorization = Mock()
    pinned_cursor = SimpleNamespace(phase="traversal")
    staged.snapshot.side_effect = (
        SimpleNamespace(cursor=None, authorization=authorization),
        SimpleNamespace(cursor=pinned_cursor, authorization=authorization),
    )
    preparation.resolve_snapshot.return_value = pinned_cursor
    lease = _lease()
    run_id = uuid4()

    assert worker._continue(lease, run_id, Mock()) == "in_progress"
    staged.pin_snapshot.assert_called_once()
    staged.plan_manifest_batch.assert_not_called()

    assert worker._continue(lease, run_id, Mock()) == "in_progress"
    staged.plan_manifest_batch.assert_called_once()
    staged.persist_batch.assert_called_once()


def test_planning_failure_prevents_provider_preparation_and_cursor_advance() -> None:
    worker, preparation, staged, _batch, _prepared = _worker(enabled=True)
    staged.plan_manifest_batch.side_effect = SyncWorkLedgerPersistenceError("safe")

    with pytest.raises(SyncWorkLedgerPersistenceError):
        worker._continue(_lease(), uuid4(), Mock())

    staged.item_snapshots.assert_not_called()
    preparation.prepare_batch.assert_not_called()
    staged.persist_batch.assert_not_called()


def test_failure_after_planning_replays_before_single_legacy_persistence() -> None:
    worker, _preparation, staged, _batch, _prepared = _worker(enabled=True)
    staged.item_snapshots.side_effect = (
        SyncWorkLedgerPersistenceError("safe"),
        (Mock(),),
    )
    lease = _lease()
    run_id = uuid4()

    with pytest.raises(SyncWorkLedgerPersistenceError):
        worker._continue(lease, run_id, Mock())
    assert worker._continue(lease, run_id, Mock()) == "in_progress"

    assert staged.plan_manifest_batch.call_count == 2
    staged.persist_batch.assert_called_once()


def test_reconciliation_resume_never_replans_or_processes_ledger_work() -> None:
    worker, preparation, staged, _batch, _prepared = _worker(enabled=True)
    staged.snapshot.return_value = SimpleNamespace(
        cursor=SimpleNamespace(phase="reconciliation"), authorization=Mock()
    )
    staged.reconcile.return_value = SimpleNamespace(outcome="in_progress")

    assert worker._continue(_lease(), uuid4(), Mock()) == "in_progress"

    staged.reconcile.assert_called_once()
    staged.plan_manifest_batch.assert_not_called()
    staged.item_snapshots.assert_not_called()
    preparation.discover_batch.assert_not_called()
    preparation.prepare_batch.assert_not_called()
    staged.persist_batch.assert_not_called()


def test_cancellation_after_durable_planning_prevents_legacy_processing() -> None:
    worker, preparation, staged, _batch, _prepared = _worker(enabled=True)
    heartbeat = Mock()
    heartbeat.raise_if_failed.side_effect = (
        None,
        None,
        SyncJobCancellationConflict("cancelled"),
    )

    with pytest.raises(SyncJobCancellationConflict):
        worker._continue(_lease(), uuid4(), heartbeat)

    staged.plan_manifest_batch.assert_called_once()
    staged.item_snapshots.assert_not_called()
    preparation.prepare_batch.assert_not_called()
    staged.persist_batch.assert_not_called()


def test_planning_persistence_failure_rolls_back_its_bounded_transaction() -> None:
    session = Mock()
    service = Mock()
    worker = GitHubSyncWorker(
        lambda: session,
        Mock(),
        lambda _session: service,
        Mock(),
        worker_id="worker",
        lease_duration=timedelta(minutes=10),
        heartbeat_interval=timedelta(minutes=1),
        heartbeat_shutdown_timeout=timedelta(minutes=2),
        ledger_planning_enabled=True,
        clock=lambda: NOW,
    )

    def fail(_service):
        raise SyncWorkLedgerPersistenceError("safe")

    with pytest.raises(SyncWorkLedgerPersistenceError):
        worker._read(fail)

    session.commit.assert_not_called()
    session.rollback.assert_called_once()
    session.close.assert_called_once()


def test_completion_failure_rolls_back_the_cursor_and_generation_transaction() -> None:
    session = Mock()
    execution = Mock()
    service = Mock()
    heartbeat = Mock()
    worker = GitHubSyncWorker(
        lambda: session,
        lambda _session: execution,
        lambda _session: service,
        Mock(),
        worker_id="worker",
        lease_duration=timedelta(minutes=10),
        heartbeat_interval=timedelta(minutes=1),
        heartbeat_shutdown_timeout=timedelta(minutes=2),
        ledger_planning_enabled=True,
        clock=lambda: NOW,
    )

    def fail(_service):
        raise SyncWorkLedgerPersistenceError("safe")

    lease = _lease()
    run_id = uuid4()
    with pytest.raises(SyncWorkLedgerPersistenceError):
        worker._write(fail, lease, run_id, heartbeat)

    heartbeat.stop.assert_called_once()
    execution.validate_attempt.assert_called_once()
    session.commit.assert_not_called()
    session.rollback.assert_called_once()
    session.close.assert_called_once()


def test_ledger_flag_must_be_a_real_boolean() -> None:
    try:
        _worker(enabled="true")  # type: ignore[arg-type]
    except ValueError as exc:
        assert str(exc) == "GitHub ledger-planning flag is invalid"
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("non-boolean ledger flag was accepted")
