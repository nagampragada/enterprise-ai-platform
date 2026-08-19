from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from application.services.connector_sync_execution_service import AcquiredSyncAttempt
from domain.embeddings.models import EmbeddingProfile
from infrastructure.repositories.connector_sync_job_repository import SyncJobLease
from infrastructure.workers.local_folder_sync_worker import (
    LocalFolderAttemptContext,
    LocalFolderWorkerResult,
)
from infrastructure.workers.local_folder_sync_worker_host import (
    DEFAULT_BACKOFF_MAX_SECONDS,
    DEFAULT_BACKOFF_MIN_SECONDS,
    DEFAULT_HEARTBEAT_SECONDS,
    DEFAULT_IDLE_SECONDS,
    DEFAULT_LEASE_SECONDS,
    DEFAULT_MAX_HOST_FAILURES,
    DEFAULT_RECOVERY_LIMIT,
    DEFAULT_SHUTDOWN_TIMEOUT_SECONDS,
    InvalidLocalFolderHostConfiguration,
    LocalFolderHostExitCode,
    LocalFolderSyncWorkerHost,
    LocalFolderWorkerHostSettings,
    compose_local_folder_sync_worker_host,
    install_shutdown_signal_handlers,
)

NOW = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)


class FakeSession:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.closed = False

    def commit(self) -> None:
        self.events.append("commit")

    def rollback(self) -> None:
        self.events.append("rollback")

    def close(self) -> None:
        self.events.append("close")
        self.closed = True


class SessionFactory:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.sessions: list[FakeSession] = []

    def __call__(self) -> FakeSession:
        session = FakeSession(self.events)
        self.sessions.append(session)
        self.events.append("open")
        return session


def _settings(*, one_shot: bool = True, maximum_failures: int = 3):
    return LocalFolderWorkerHostSettings(
        worker_id="host-one",
        idle_interval=timedelta(seconds=5),
        lease_duration=timedelta(minutes=15),
        heartbeat_interval=timedelta(minutes=1),
        maximum_consecutive_failures=maximum_failures,
        minimum_backoff=timedelta(seconds=1),
        maximum_backoff=timedelta(seconds=4),
        backoff_jitter=0.0,
        graceful_shutdown_timeout=timedelta(minutes=5),
        one_shot=one_shot,
        expired_recovery_limit=10,
    )


def _lease() -> SyncJobLease:
    return SyncJobLease(
        uuid4(), uuid4(), uuid4(), uuid4(), "incremental", "manual",
        1, 3, uuid4(), 1, NOW + timedelta(minutes=15),
    )


def _context(lease: SyncJobLease) -> LocalFolderAttemptContext:
    return LocalFolderAttemptContext(
        lease.organization_id, lease.job_id, lease.connector_id,
        lease.connector_scope_id, uuid4(), lease.attempt_number, "host-one",
        lease.lease_id, lease.fencing_token, lease.lease_expires_at,
        lease.mode, lease.trigger_type, lease.max_attempts,
    )


def _host(
    sessions: SessionFactory,
    execution: Mock,
    worker: Mock,
    *,
    settings=None,
    event=None,
    wait=None,
    monotonic=lambda: 0.0,
):
    return LocalFolderSyncWorkerHost(
        sessions,
        lambda session: execution,
        worker,
        settings or _settings(),
        shutdown_event=event,
        wait=wait,
        random_uniform=lambda low, high: high,
        monotonic=monotonic,
        logger=logging.getLogger("test.local_folder_host"),
    )


def test_settings_defaults_generate_safe_worker_id_and_are_bounded():
    settings = LocalFolderWorkerHostSettings.from_environment(
        [], environ={}, worker_id_factory=lambda: "local-folder-generated"
    )
    assert settings.worker_id == "local-folder-generated"
    assert settings.idle_interval.total_seconds() == DEFAULT_IDLE_SECONDS
    assert settings.lease_duration.total_seconds() == DEFAULT_LEASE_SECONDS
    assert settings.heartbeat_interval.total_seconds() == DEFAULT_HEARTBEAT_SECONDS
    assert settings.maximum_consecutive_failures == DEFAULT_MAX_HOST_FAILURES
    assert settings.minimum_backoff.total_seconds() == DEFAULT_BACKOFF_MIN_SECONDS
    assert settings.maximum_backoff.total_seconds() == DEFAULT_BACKOFF_MAX_SECONDS
    assert settings.graceful_shutdown_timeout.total_seconds() == DEFAULT_SHUTDOWN_TIMEOUT_SECONDS
    assert settings.expired_recovery_limit == DEFAULT_RECOVERY_LIMIT
    assert settings.one_shot is False


@pytest.mark.parametrize(
    "environment",
    (
        {"LOCAL_FOLDER_WORKER_IDLE_SECONDS": "0"},
        {"LOCAL_FOLDER_WORKER_LEASE_SECONDS": "-1"},
        {"LOCAL_FOLDER_WORKER_HEARTBEAT_SECONDS": "900"},
        {"LOCAL_FOLDER_WORKER_BACKOFF_MIN_SECONDS": "10", "LOCAL_FOLDER_WORKER_BACKOFF_MAX_SECONDS": "1"},
        {"LOCAL_FOLDER_WORKER_MAX_FAILURES": "0"},
        {"LOCAL_FOLDER_WORKER_MAX_FAILURES": "101"},
        {"LOCAL_FOLDER_WORKER_RECOVERY_LIMIT": "101"},
        {"LOCAL_FOLDER_WORKER_BACKOFF_JITTER": "1.1"},
        {"LOCAL_FOLDER_WORKER_SHUTDOWN_TIMEOUT_SECONDS": "3601"},
        {"LOCAL_FOLDER_WORKER_ID": "unsafe worker"},
        {"LOCAL_FOLDER_WORKER_IDLE_SECONDS": "nan"},
    ),
)
def test_settings_reject_invalid_values_and_unsafe_relationships(environment):
    with pytest.raises(InvalidLocalFolderHostConfiguration):
        LocalFolderWorkerHostSettings.from_environment([], environ=environment)


def test_one_shot_flag_uses_same_settings_parser():
    settings = LocalFolderWorkerHostSettings.from_environment(
        ["--once"], environ={}, worker_id_factory=lambda: "host-one"
    )
    assert settings.one_shot is True


def test_direct_settings_construction_is_strict():
    with pytest.raises(InvalidLocalFolderHostConfiguration):
        LocalFolderWorkerHostSettings(
            worker_id="host-one",
            idle_interval=timedelta(seconds=1),
            lease_duration=timedelta(seconds=60),
            heartbeat_interval=timedelta(seconds=60),
            maximum_consecutive_failures=3,
            minimum_backoff=timedelta(seconds=1),
            maximum_backoff=timedelta(seconds=2),
            backoff_jitter=0.0,
            graceful_shutdown_timeout=timedelta(seconds=30),
            one_shot=True,
            expired_recovery_limit=1,
        )


def test_one_shot_empty_queue_commits_recovery_and_exits_without_work():
    sessions, execution, worker = SessionFactory(), Mock(), Mock()
    execution.recover_expired_local_folder.return_value = ()
    execution.acquire_one_local_folder.return_value = None
    host = _host(sessions, execution, worker)
    assert host.run() == LocalFolderHostExitCode.NO_WORK
    assert sessions.events == ["open", "commit", "close"]
    worker.execute.assert_not_called()


def test_one_shot_executes_at_most_one_claimed_job_outside_claim_transaction():
    sessions, execution, worker = SessionFactory(), Mock(), Mock()
    lease = _lease()
    acquired = AcquiredSyncAttempt(lease, uuid4())
    context = _context(lease)
    execution.recover_expired_local_folder.return_value = ()
    execution.acquire_one_local_folder.return_value = acquired
    worker.attempt_context.return_value = context

    def execute(value):
        assert value == context
        assert all(session.closed for session in sessions.sessions)
        return LocalFolderWorkerResult("completed", value.job_id, value.sync_run_id, 1, 1)

    worker.execute.side_effect = execute
    host = _host(sessions, execution, worker)
    assert host.run() == LocalFolderHostExitCode.SUCCESS
    worker.execute.assert_called_once()


def test_continuous_empty_queue_waits_interruptibly_without_creating_work():
    sessions, execution, worker = SessionFactory(), Mock(), Mock()
    execution.recover_expired_local_folder.return_value = ()
    execution.acquire_one_local_folder.return_value = None
    waits = []

    def wait(seconds):
        waits.append(seconds)
        return True

    host = _host(sessions, execution, worker, settings=_settings(one_shot=False), wait=wait)
    assert host.run() == LocalFolderHostExitCode.SUCCESS
    assert waits == [5.0]
    worker.execute.assert_not_called()


def test_shutdown_during_work_stops_at_next_safe_boundary():
    sessions, execution, worker = SessionFactory(), Mock(), Mock()
    event = threading.Event()
    lease, context = _lease(), None
    context = _context(lease)
    execution.recover_expired_local_folder.return_value = ()
    execution.acquire_one_local_folder.return_value = AcquiredSyncAttempt(lease, context.sync_run_id)
    worker.attempt_context.return_value = context

    def execute(value):
        event.set()
        return LocalFolderWorkerResult("in_progress", value.job_id, value.sync_run_id, 1, 1)

    worker.execute.side_effect = execute
    result = _host(sessions, execution, worker, event=event).run_cycle()
    assert result.outcome == "shutdown" and not result.shutdown_timeout_exceeded
    worker.execute.assert_called_once()


def test_database_failures_use_capped_backoff_and_exit_at_maximum(caplog):
    sessions, execution, worker = SessionFactory(), Mock(), Mock()
    execution.recover_expired_local_folder.side_effect = RuntimeError("C:/secret/root")
    waits = []

    def wait(seconds):
        waits.append(seconds)
        return False

    caplog.set_level(logging.ERROR, logger="test.local_folder_host")
    host = _host(
        sessions,
        execution,
        worker,
        settings=_settings(one_shot=False, maximum_failures=3),
        wait=wait,
    )
    assert host.run() == LocalFolderHostExitCode.HOST_FAILURE
    assert waits == [1.0, 2.0]
    assert "C:/secret/root" not in caplog.text
    assert execution.acquire_one_local_folder.call_count == 0
    assert all(session.closed for session in sessions.sessions)


def test_successful_database_cycle_resets_consecutive_failure_count():
    sessions, execution, worker = SessionFactory(), Mock(), Mock()
    execution.recover_expired_local_folder.side_effect = (
        RuntimeError("first"),
        (),
        RuntimeError("second"),
        RuntimeError("third"),
    )
    execution.acquire_one_local_folder.return_value = None
    waits = []

    def wait(seconds):
        waits.append(seconds)
        return False

    host = _host(
        sessions,
        execution,
        worker,
        settings=_settings(one_shot=False, maximum_failures=2),
        wait=wait,
    )
    assert host.run() == LocalFolderHostExitCode.HOST_FAILURE
    assert waits == [1.0, 5.0, 1.0]


def test_infrastructure_backoff_is_capped():
    sessions, execution, worker = SessionFactory(), Mock(), Mock()
    execution.recover_expired_local_folder.side_effect = RuntimeError("database unavailable")
    waits = []
    host = _host(
        sessions,
        execution,
        worker,
        settings=_settings(one_shot=False, maximum_failures=5),
        wait=lambda seconds: waits.append(seconds) or False,
    )
    assert host.run() == LocalFolderHostExitCode.HOST_FAILURE
    assert waits == [1.0, 2.0, 4.0, 4.0]


def test_shutdown_timeout_is_reported_after_an_indivisible_step_returns():
    sessions, execution, worker = SessionFactory(), Mock(), Mock()
    event = threading.Event()
    lease = _lease()
    context = _context(lease)
    execution.recover_expired_local_folder.return_value = ()
    execution.acquire_one_local_folder.return_value = AcquiredSyncAttempt(lease, context.sync_run_id)
    worker.attempt_context.return_value = context

    def execute(value):
        event.set()
        return LocalFolderWorkerResult("in_progress", value.job_id, value.sync_run_id, 1, 1)

    worker.execute.side_effect = execute
    times = iter((0.0, 301.0))
    result = _host(
        sessions,
        execution,
        worker,
        event=event,
        monotonic=lambda: next(times),
    ).run_cycle()
    assert result.outcome == "shutdown" and result.shutdown_timeout_exceeded


def test_shutdown_after_claim_commit_does_not_start_provider_work():
    sessions, execution, worker = SessionFactory(), Mock(), Mock()
    event = threading.Event()
    lease = _lease()
    context = _context(lease)
    execution.recover_expired_local_folder.return_value = ()
    execution.acquire_one_local_folder.return_value = AcquiredSyncAttempt(lease, context.sync_run_id)

    def attempt_context(acquired):
        event.set()
        return context

    worker.attempt_context.side_effect = attempt_context
    result = _host(sessions, execution, worker, event=event).run_cycle()
    assert result.outcome == "shutdown"
    worker.execute.assert_not_called()
    assert all(session.closed for session in sessions.sessions)


def test_signal_handlers_only_set_the_shutdown_event():
    event = threading.Event()
    handlers = {}

    def register(signum, handler):
        handlers[signum] = handler

    with patch("infrastructure.workers.local_folder_sync_worker_host.signal.signal", register):
        install_shutdown_signal_handlers(event)
    assert handlers
    next(iter(handlers.values()))(0, None)
    assert event.is_set()


def test_composition_validates_provider_profile_without_calling_provider_or_database():
    provider = Mock()
    provider.profile = EmbeddingProfile("fake", "fake", 1536, "fake:1536", 64)
    session_factory = Mock()
    host = compose_local_folder_sync_worker_host(
        _settings(),
        session_factory=session_factory,
        embedding_provider_factory=lambda: provider,
        random_uniform=lambda low, high: high,
    )
    assert isinstance(host, LocalFolderSyncWorkerHost)
    provider.embed_batch.assert_not_called()
    session_factory.assert_not_called()


@pytest.mark.parametrize(
    ("outcome", "exit_code"),
    (
        ("retry_scheduled", LocalFolderHostExitCode.RETRY_SCHEDULED),
        ("failed", LocalFolderHostExitCode.TERMINAL_FAILURE),
        ("cancelled", LocalFolderHostExitCode.CANCELLED),
        ("lost_lease", LocalFolderHostExitCode.LOST_LEASE),
    ),
)
def test_one_shot_has_deterministic_job_outcome_exit_codes(outcome, exit_code):
    sessions, execution, worker = SessionFactory(), Mock(), Mock()
    lease, context = _lease(), None
    context = _context(lease)
    execution.recover_expired_local_folder.return_value = ()
    execution.acquire_one_local_folder.return_value = AcquiredSyncAttempt(lease, context.sync_run_id)
    worker.attempt_context.return_value = context
    worker.execute.return_value = LocalFolderWorkerResult(
        outcome, context.job_id, context.sync_run_id, 1, 1
    )
    assert _host(sessions, execution, worker).run() == exit_code


def test_recovery_and_claim_share_one_transaction_and_shutdown_prevents_claim():
    sessions, execution, worker = SessionFactory(), Mock(), Mock()
    event = threading.Event()
    execution.recover_expired_local_folder.side_effect = lambda **kwargs: event.set() or ()
    result = _host(sessions, execution, worker, event=event).run_cycle()
    assert result.outcome == "shutdown"
    execution.acquire_one_local_folder.assert_not_called()
    assert sessions.events == ["open", "commit", "close"]