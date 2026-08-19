from datetime import timedelta
import logging
import threading
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from application.services.connector_sync_schedule_service import DueScheduleResult
from infrastructure.workers.connector_sync_scheduler_host import (
    ConnectorSyncSchedulerHost,
    ConnectorSyncSchedulerHostSettings,
    InvalidSchedulerHostConfiguration,
    SchedulerHostExitCode,
    compose_connector_sync_scheduler_host,
    install_shutdown_signal_handlers,
)


class FakeSession:
    def __init__(self, events):
        self.events = events
        self.closed = False

    def commit(self): self.events.append("commit")
    def rollback(self): self.events.append("rollback")
    def close(self): self.events.append("close"); self.closed = True


class Sessions:
    def __init__(self): self.events=[]; self.sessions=[]
    def __call__(self):
        value=FakeSession(self.events); self.sessions.append(value); self.events.append("open"); return value


def _settings(*, once=True, failures=3):
    return ConnectorSyncSchedulerHostSettings(
        "scheduler-one", timedelta(seconds=5), failures, timedelta(seconds=1),
        timedelta(seconds=4), 0.0, timedelta(seconds=30), once,
    )


def _host(sessions, service, *, settings=None, event=None, wait=None, monotonic=lambda: 0.0):
    return ConnectorSyncSchedulerHost(
        sessions, lambda session: service, settings or _settings(),
        shutdown_event=event, wait=wait, random_uniform=lambda low, high: high,
        monotonic=monotonic,
        logger=logging.getLogger("test.scheduler"),
    )


def test_settings_defaults_generated_id_one_shot_and_invalid_values():
    settings = ConnectorSyncSchedulerHostSettings.from_environment(
        ["--once"], environ={}, scheduler_id_factory=lambda: "generated-scheduler"
    )
    assert settings.scheduler_id == "generated-scheduler" and settings.one_shot
    for environment in (
        {"CONNECTOR_SYNC_SCHEDULER_POLL_SECONDS": "0"},
        {"CONNECTOR_SYNC_SCHEDULER_MAX_FAILURES": "0"},
        {"CONNECTOR_SYNC_SCHEDULER_MAX_FAILURES": "101"},
        {"CONNECTOR_SYNC_SCHEDULER_BACKOFF_MIN_SECONDS": "10", "CONNECTOR_SYNC_SCHEDULER_BACKOFF_MAX_SECONDS": "1"},
        {"CONNECTOR_SYNC_SCHEDULER_BACKOFF_JITTER": "1.1"},
        {"CONNECTOR_SYNC_SCHEDULER_ID": "unsafe scheduler"},
    ):
        with pytest.raises(InvalidSchedulerHostConfiguration):
            ConnectorSyncSchedulerHostSettings.from_environment([], environ=environment)


def test_one_shot_no_work_and_at_most_one_due_schedule():
    sessions, service = Sessions(), Mock()
    service.process_one_due.return_value = DueScheduleResult("no_work", None, None, None, False, None)
    assert _host(sessions, service).run() == SchedulerHostExitCode.NO_WORK
    assert sessions.events == ["open", "commit", "close"]
    service.process_one_due.return_value = DueScheduleResult("enqueued", uuid4(), uuid4(), uuid4(), False, None)
    assert _host(sessions, service).run() == SchedulerHostExitCode.SUCCESS
    assert service.process_one_due.call_count == 2


def test_continuous_idle_wait_is_interruptible_and_has_no_open_session():
    sessions, service, waits = Sessions(), Mock(), []
    service.process_one_due.return_value = DueScheduleResult("no_work", None, None, None, False, None)
    def wait(seconds):
        assert all(item.closed for item in sessions.sessions); waits.append(seconds); return True
    assert _host(sessions, service, settings=_settings(once=False), wait=wait).run() == 0
    assert waits == [5.0]


def test_database_failures_backoff_cap_exit_and_reset_without_leaking_error(caplog):
    sessions, service, waits = Sessions(), Mock(), []
    service.process_one_due.side_effect = RuntimeError("postgresql://secret/path")
    caplog.set_level(logging.ERROR, logger="test.scheduler")
    host = _host(
        sessions, service, settings=_settings(once=False, failures=5),
        wait=lambda seconds: waits.append(seconds) or False,
    )
    assert host.run() == SchedulerHostExitCode.HOST_FAILURE
    assert waits == [1.0, 2.0, 4.0, 4.0]
    assert "secret" not in caplog.text and all(item.closed for item in sessions.sessions)

    sessions, service, waits = Sessions(), Mock(), []
    service.process_one_due.side_effect = (
        RuntimeError("one"), DueScheduleResult("no_work", None, None, None, False, None),
        RuntimeError("two"), RuntimeError("three"),
    )
    host = _host(
        sessions, service, settings=_settings(once=False, failures=2),
        wait=lambda seconds: waits.append(seconds) or False,
    )
    assert host.run() == SchedulerHostExitCode.HOST_FAILURE
    assert waits == [1.0, 5.0, 1.0]


def test_shutdown_and_signal_handler_do_no_database_work():
    sessions, service, event = Sessions(), Mock(), threading.Event()
    event.set()
    assert _host(sessions, service, event=event).run_cycle().outcome == "shutdown"
    assert not sessions.sessions and service.process_one_due.call_count == 0
    event.clear(); handlers = {}
    with patch(
        "infrastructure.workers.connector_sync_scheduler_host.signal.signal",
        lambda signum, handler: handlers.setdefault(signum, handler),
    ):
        install_shutdown_signal_handlers(event)
    next(iter(handlers.values()))(0, None)
    assert event.is_set() and not sessions.sessions


def test_shutdown_timeout_exits_nonzero_after_database_boundary():
    sessions, service, event = Sessions(), Mock(), threading.Event()
    service.process_one_due.side_effect = lambda: (
        event.set() or DueScheduleResult("no_work", None, None, None, False, None)
    )
    times = iter((0.0, 31.0))
    host = _host(
        sessions, service, settings=_settings(once=False), event=event,
        monotonic=lambda: next(times),
    )
    assert host.run() == SchedulerHostExitCode.HOST_FAILURE
    assert sessions.events == ["open", "commit", "close"]


def test_composition_opens_no_session_and_imports_no_worker_or_provider():
    sessions = Mock()
    host = compose_connector_sync_scheduler_host(_settings(), session_factory=sessions)
    assert isinstance(host, ConnectorSyncSchedulerHost)
    sessions.assert_not_called()
    import inspect
    import infrastructure.workers.connector_sync_scheduler_host as module
    source = inspect.getsource(module)
    assert "LocalFolderSyncWorker" not in source
    assert "EmbeddingProvider" not in source
    assert "openai" not in source.lower()
    assert "Path(" not in source