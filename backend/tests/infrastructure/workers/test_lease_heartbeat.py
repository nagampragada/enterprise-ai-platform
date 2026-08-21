from datetime import UTC, datetime, timedelta
import threading
from unittest.mock import Mock
from uuid import uuid4

import pytest

from infrastructure.repositories.connector_sync_job_repository import SyncJobLease
from infrastructure.workers.lease_heartbeat import LeaseHeartbeat, LeaseHeartbeatFailure


def _lease():
    return SyncJobLease(
        uuid4(), uuid4(), uuid4(), uuid4(), "incremental", "scheduled",
        1, 3, uuid4(), 1, datetime.now(UTC) + timedelta(minutes=5),
    )


class _Session:
    def __init__(self, owners):
        owners.append(threading.get_ident())
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1
    def close(self): self.closed = True


def test_heartbeat_uses_thread_owned_short_sessions_and_stops_deterministically():
    owners, sessions, renewed = [], [], threading.Event()
    def sessions_factory():
        session = _Session(owners); sessions.append(session); return session
    execution = Mock()
    execution.heartbeat.side_effect = lambda *_a, **_k: renewed.set()
    heartbeat = LeaseHeartbeat(
        sessions_factory, lambda _session: execution, _lease(), worker_id="worker-1",
        lease_duration=timedelta(seconds=1), interval=timedelta(milliseconds=10),
        shutdown_timeout=timedelta(seconds=1),
    )
    with heartbeat:
        assert renewed.wait(1)
    assert owners and all(owner != threading.get_ident() for owner in owners)
    assert all(session.commits == 1 and session.closed for session in sessions)


def test_heartbeat_rejection_is_observed_without_retry_loop():
    attempted = threading.Event()
    execution = Mock()
    def reject(*_args, **_kwargs):
        attempted.set(); raise RuntimeError("rejected")
    execution.heartbeat.side_effect = reject
    heartbeat = LeaseHeartbeat(
        lambda: _Session([]), lambda _session: execution, _lease(), worker_id="worker-1",
        lease_duration=timedelta(seconds=1), interval=timedelta(milliseconds=10),
        shutdown_timeout=timedelta(seconds=1),
    )
    heartbeat.__enter__()
    assert attempted.wait(1)
    with pytest.raises(LeaseHeartbeatFailure):
        heartbeat.stop()
    assert execution.heartbeat.call_count == 1


@pytest.mark.parametrize("interval", [0.5, 0.9])
def test_unsafe_renewal_margin_is_rejected(interval):
    with pytest.raises(ValueError, match="renewal margin"):
        LeaseHeartbeat(
            Mock(), Mock(), _lease(), worker_id="worker-1",
            lease_duration=timedelta(seconds=1), interval=timedelta(seconds=interval),
            shutdown_timeout=timedelta(seconds=1),
        )
