from __future__ import annotations

import os
import subprocess
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from application.services.connector_sync_execution_service import ConnectorSyncExecutionService
from application.services.connector_sync_retry_policy import ConnectorSyncRetryPolicy, SyncFailureKind
from infrastructure.db.models import ConnectorSyncJob, ConnectorSyncRun
from infrastructure.repositories.connector_sync_job_repository import (
    ConnectorSyncJobRepository,
    InvalidSyncJobTransition,
    LostSyncJobLease,
    StaleSyncJobFence,
    SyncJobCancellationConflict,
    SyncJobConflict,
    SyncJobNotFound,
)

ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
TEST_URL = "TEST_DATABASE_URL"
DEV_URL = "DATABASE_URL"
NOW = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
LEASE = timedelta(minutes=5)


def _identity(url: str):
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database


@pytest.fixture(scope="module")
def engine():
    url = os.environ[TEST_URL]
    development = os.environ.get(DEV_URL)
    if development and _identity(development) == _identity(url):
        raise RuntimeError("test database must differ from development database")
    reset = create_engine(url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    reset.dispose()
    environment = os.environ.copy()
    environment[DEV_URL] = url
    subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(INI), "upgrade", "head"],
        check=True,
        cwd=str(ROOT),
        env=environment,
    )
    value = create_engine(url, future=True)
    try:
        yield value
    finally:
        value.dispose()


@pytest.fixture
def session(engine):
    value = Session(engine, expire_on_commit=False)
    try:
        yield value
    finally:
        value.rollback()
        value.close()


def _exec(session: Session, sql: str, **params):
    return session.execute(text(sql), params)


def _setup(session: Session, name: str = "Alpha"):
    organization_id, connector_id, space_id, scope_id = (uuid.uuid4() for _ in range(4))
    _exec(
        session,
        "INSERT INTO organizations (id,name,slug) VALUES (:id,:name,:slug)",
        id=organization_id,
        name=name,
        slug=f"{name.lower()}-{organization_id}",
    )
    _exec(
        session,
        """INSERT INTO connectors
           (id,organization_id,connector_type,display_name,slug,status)
           VALUES (:id,:org,'local_folder',:name,:slug,'active')""",
        id=connector_id,
        org=organization_id,
        name=name,
        slug=f"connector-{connector_id}",
    )
    _exec(
        session,
        "INSERT INTO knowledge_spaces (id,organization_id,name,slug) VALUES (:id,:org,:name,:slug)",
        id=space_id,
        org=organization_id,
        name=name,
        slug=f"space-{space_id}",
    )
    _exec(
        session,
        """INSERT INTO connector_scopes
           (id,organization_id,connector_id,knowledge_space_id,display_name,slug,scope_type,
            external_scope_key,access_mode,status)
           VALUES (:id,:org,:connector,:space,:name,:slug,'folder',:key,'platform_managed','active')""",
        id=scope_id,
        org=organization_id,
        connector=connector_id,
        space=space_id,
        name=name,
        slug=f"scope-{scope_id}",
        key=f"C:/safe/{scope_id}",
    )
    session.commit()
    return organization_id, connector_id, scope_id


def _scope(session: Session, organization_id, connector_id, name: str):
    space_id, scope_id = uuid.uuid4(), uuid.uuid4()
    _exec(
        session,
        "INSERT INTO knowledge_spaces (id,organization_id,name,slug) VALUES (:id,:org,:name,:slug)",
        id=space_id,
        org=organization_id,
        name=name,
        slug=f"space-{space_id}",
    )
    _exec(
        session,
        """INSERT INTO connector_scopes
           (id,organization_id,connector_id,knowledge_space_id,display_name,slug,scope_type,
            external_scope_key,access_mode,status)
           VALUES (:id,:org,:connector,:space,:name,:slug,'folder',:key,'platform_managed','active')""",
        id=scope_id,
        org=organization_id,
        connector=connector_id,
        space=space_id,
        name=name,
        slug=f"scope-{scope_id}",
        key=f"C:/safe/{scope_id}",
    )
    session.commit()
    return scope_id


def _repo(session: Session) -> ConnectorSyncJobRepository:
    return ConnectorSyncJobRepository(session)


def _enqueue(session: Session, organization_id, connector_id, scope_id, *, maximum=3):
    result = _repo(session).enqueue_or_coalesce(
        organization_id,
        connector_id,
        scope_id,
        mode="incremental",
        trigger_type="manual",
        max_attempts=maximum,
        now=NOW,
    )
    return result


def _acquire(session: Session, organization_id, *, worker="worker-one", now=NOW):
    return _repo(session).acquire_next(
        organization_id,
        worker_id=worker,
        lease_duration=LEASE,
        now=now,
    )


def test_concurrent_enqueue_coalesces_to_one_nonterminal_job(engine):
    setup = Session(engine)
    organization_id, connector_id, scope_id = _setup(setup, "Coalesce")
    setup.close()
    barrier = threading.Barrier(2)
    results = []
    errors = []

    def enqueue(worker: int):
        value = Session(engine, expire_on_commit=False)
        try:
            barrier.wait()
            results.append(
                _repo(value).enqueue_or_coalesce(
                    organization_id,
                    connector_id,
                    scope_id,
                    mode="incremental",
                    trigger_type="manual",
                    now=NOW + timedelta(microseconds=worker),
                )
            )
            value.commit()
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)
            value.rollback()
        finally:
            value.close()

    threads = [threading.Thread(target=enqueue, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert len(results) == 2
    assert {result.job_id for result in results} == {results[0].job_id}
    assert sorted(result.coalesced for result in results) == [False, True]
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT count(*) FROM connector_sync_jobs WHERE organization_id=:org"),
            {"org": organization_id},
        ).scalar_one() == 1


def test_two_concurrent_acquirers_produce_one_lease_and_one_generation(engine):
    setup = Session(engine)
    organization_id, connector_id, scope_id = _setup(setup, "Acquire")
    _enqueue(setup, organization_id, connector_id, scope_id)
    setup.commit()
    setup.close()
    barrier = threading.Barrier(2)
    leases = []

    def acquire(worker: int):
        value = Session(engine, expire_on_commit=False)
        try:
            barrier.wait()
            leases.append(_acquire(value, organization_id, worker=f"worker-{worker}"))
            value.commit()
        finally:
            value.close()

    threads = [threading.Thread(target=acquire, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    winners = [lease for lease in leases if lease is not None]
    assert len(winners) == 1
    lease = winners[0]
    assert lease.attempt_number == lease.fencing_token == 1
    assert lease.lease_expires_at == NOW + LEASE


def test_heartbeat_uses_tenant_lease_worker_fence_expiration_and_cancellation(session):
    organization_id, connector_id, scope_id = _setup(session, "Heartbeat")
    _enqueue(session, organization_id, connector_id, scope_id)
    session.commit()
    lease = _acquire(session, organization_id)
    assert lease is not None
    session.commit()
    renewed = _repo(session).renew_heartbeat(
        lease,
        worker_id="worker-one",
        now=NOW + timedelta(minutes=1),
        lease_duration=LEASE,
    )
    assert renewed.lease_expires_at == NOW + timedelta(minutes=6)
    session.commit()
    for invalid, worker, now, error in (
        (lease.__class__(**{**lease.__dict__, "lease_id": uuid.uuid4()}), "worker-one", NOW, LostSyncJobLease),
        (lease.__class__(**{**lease.__dict__, "fencing_token": 2}), "worker-one", NOW, ValueError),
        (lease.__class__(**{**lease.__dict__, "organization_id": uuid.uuid4()}), "worker-one", NOW, LostSyncJobLease),
        (lease, "worker-other", NOW, LostSyncJobLease),
        (renewed, "worker-one", renewed.lease_expires_at, LostSyncJobLease),
    ):
        with pytest.raises(error):
            _repo(session).renew_heartbeat(invalid, worker_id=worker, now=now, lease_duration=LEASE)
    _repo(session).request_cancellation(organization_id, lease.job_id, now=NOW + timedelta(minutes=2))
    session.commit()
    with pytest.raises(SyncJobCancellationConflict):
        _repo(session).renew_heartbeat(
            renewed,
            worker_id="worker-one",
            now=NOW + timedelta(minutes=3),
            lease_duration=LEASE,
        )


def test_success_is_fenced_clears_lease_and_cannot_be_retried(session):
    organization_id, connector_id, scope_id = _setup(session, "Success")
    _enqueue(session, organization_id, connector_id, scope_id)
    session.commit()
    lease = _acquire(session, organization_id)
    assert lease is not None
    run = _repo(session).create_attempt_run(lease, worker_id="worker-one", now=NOW)
    session.commit()
    result = _repo(session).complete_success(
        lease, worker_id="worker-one", now=NOW + timedelta(minutes=1)
    )
    assert result.status == "succeeded" and result.completed_at is not None
    session.commit()
    row = session.get(ConnectorSyncJob, lease.job_id)
    assert row is not None and row.lease_id is None and row.next_attempt_at is None
    assert session.get(ConnectorSyncRun, run.id).status == "completed"
    with pytest.raises(LostSyncJobLease):
        _repo(session).complete_success(
            lease, worker_id="worker-one", now=NOW + timedelta(minutes=2)
        )
    assert _acquire(session, organization_id, now=NOW + timedelta(days=1)) is None


def test_retry_wait_eligibility_new_attempt_and_exhaustion(session):
    organization_id, connector_id, scope_id = _setup(session, "Retry")
    _enqueue(session, organization_id, connector_id, scope_id, maximum=2)
    session.commit()
    first = _acquire(session, organization_id)
    assert first is not None
    _repo(session).create_attempt_run(first, worker_id="worker-one", now=NOW)
    retry_at = NOW + timedelta(minutes=2)
    result = _repo(session).record_failure(
        first,
        worker_id="worker-one",
        now=NOW + timedelta(minutes=1),
        error_category="source_read",
        error_code="provider_temporarily_unavailable",
        retry_at=retry_at,
    )
    assert result.status == "retry_wait" and result.next_attempt_at == retry_at
    session.commit()
    assert _acquire(session, organization_id, now=retry_at - timedelta(seconds=1)) is None
    second = _acquire(session, organization_id, worker="worker-two", now=retry_at)
    assert second is not None and second.attempt_number == second.fencing_token == 2
    _repo(session).create_attempt_run(second, worker_id="worker-two", now=retry_at)
    exhausted = _repo(session).record_failure(
        second,
        worker_id="worker-two",
        now=retry_at + timedelta(minutes=1),
        error_category="source_read",
        error_code="provider_temporarily_unavailable",
        retry_at=None,
    )
    assert exhausted.status == "failed" and exhausted.next_attempt_at is None
    session.commit()
    assert _acquire(session, organization_id, now=retry_at + timedelta(days=1)) is None


@pytest.mark.parametrize(
    ("kind", "category"),
    (
        (SyncFailureKind.AUTHENTICATION, "authentication"),
        (SyncFailureKind.CONFIGURATION, "configuration"),
        (SyncFailureKind.VALIDATION, "configuration"),
        (SyncFailureKind.PERMANENT_PROVIDER, "source_read"),
        (SyncFailureKind.UNKNOWN_INTERNAL, "internal"),
    ),
)
def test_nonretryable_service_failures_are_terminal(session, kind, category):
    organization_id, connector_id, scope_id = _setup(session, f"Permanent-{kind.value}")
    _enqueue(session, organization_id, connector_id, scope_id)
    session.commit()
    lease = _acquire(session, organization_id)
    assert lease is not None
    _repo(session).create_attempt_run(lease, worker_id="worker-one", now=NOW)
    service = ConnectorSyncExecutionService(
        _repo(session),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=lambda: NOW + timedelta(minutes=1),
    )
    result = service.fail_attempt(lease, worker_id="worker-one", kind=kind)
    assert result.status == "failed" and result.last_error_category == category
    session.commit()
    assert _acquire(session, organization_id, now=NOW + timedelta(days=1)) is None


def test_queued_and_running_cancellation_are_distinct_and_idempotent(session):
    organization_id, connector_id, queued_scope = _setup(session, "Cancellation")
    queued = _enqueue(session, organization_id, connector_id, queued_scope)
    session.commit()
    cancelled = _repo(session).request_cancellation(
        organization_id, queued.job_id, now=NOW, reason_code="user_requested"
    )
    assert cancelled.status == "cancelled"
    assert _repo(session).request_cancellation(
        organization_id, queued.job_id, now=NOW + timedelta(seconds=1)
    ).status == "cancelled"
    running_scope = _scope(session, organization_id, connector_id, "running-cancel")
    running_job = _enqueue(session, organization_id, connector_id, running_scope)
    session.commit()
    lease = _acquire(session, organization_id)
    assert lease is not None and lease.job_id == running_job.job_id
    _repo(session).create_attempt_run(lease, worker_id="worker-one", now=NOW)
    requested = _repo(session).request_cancellation(
        organization_id, lease.job_id, now=NOW + timedelta(minutes=1)
    )
    assert requested.status == "running" and requested.cancellation_requested
    with pytest.raises(LostSyncJobLease):
        _repo(session).acknowledge_cancellation(
            lease.__class__(**{**lease.__dict__, "lease_id": uuid.uuid4()}),
            worker_id="worker-one",
            now=NOW + timedelta(minutes=2),
        )
    acknowledged = _repo(session).acknowledge_cancellation(
        lease, worker_id="worker-one", now=NOW + timedelta(minutes=2)
    )
    assert acknowledged.status == "cancelled"


def test_cancellation_and_success_race_has_one_terminal_winner(engine):
    setup = Session(engine, expire_on_commit=False)
    organization_id, connector_id, scope_id = _setup(setup, "CancelRace")
    job = _enqueue(setup, organization_id, connector_id, scope_id)
    setup.commit()
    lease = _acquire(setup, organization_id)
    assert lease is not None
    _repo(setup).create_attempt_run(lease, worker_id="worker-one", now=NOW)
    setup.commit()
    setup.close()
    barrier = threading.Barrier(2)
    outcomes = []

    def success():
        value = Session(engine)
        try:
            barrier.wait()
            _repo(value).complete_success(
                lease, worker_id="worker-one", now=NOW + timedelta(minutes=1)
            )
            value.commit()
            outcomes.append("succeeded")
        except (LostSyncJobLease, SyncJobCancellationConflict):
            value.rollback()
            outcomes.append("rejected")
        finally:
            value.close()

    def cancel():
        value = Session(engine)
        try:
            barrier.wait()
            _repo(value).request_cancellation(
                organization_id, job.job_id, now=NOW + timedelta(minutes=1)
            )
            result = _repo(value).acknowledge_cancellation(
                lease,
                worker_id="worker-one",
                now=NOW + timedelta(minutes=1, seconds=1),
            )
            value.commit()
            outcomes.append(result.status)
        except (InvalidSyncJobTransition, LostSyncJobLease, SyncJobCancellationConflict):
            value.rollback()
            outcomes.append("rejected")
        finally:
            value.close()

    threads = [threading.Thread(target=success), threading.Thread(target=cancel)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    with engine.connect() as connection:
        row = connection.execute(
            text("SELECT status,cancel_requested_at FROM connector_sync_jobs WHERE id=:id"),
            {"id": job.job_id},
        ).one()
    assert row.status in {"succeeded", "cancelled"}
    assert outcomes.count("rejected") == 1


def test_concurrent_expired_recovery_has_one_winner_and_stales_old_lease(engine):
    setup = Session(engine, expire_on_commit=False)
    organization_id, connector_id, scope_id = _setup(setup, "Recovery")
    _enqueue(setup, organization_id, connector_id, scope_id, maximum=2)
    setup.commit()
    lease = _acquire(setup, organization_id)
    assert lease is not None
    _repo(setup).create_attempt_run(lease, worker_id="worker-one", now=NOW)
    setup.commit()
    setup.close()
    recovery_time = lease.lease_expires_at
    barrier = threading.Barrier(2)
    counts = []

    def recover():
        value = Session(engine)
        try:
            barrier.wait()
            service = ConnectorSyncExecutionService(
                _repo(value),
                ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
                clock=lambda: recovery_time,
            )
            counts.append(len(service.recover_expired(limit=1, organization_id=organization_id)))
            value.commit()
        finally:
            value.close()

    threads = [threading.Thread(target=recover) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(counts) == [0, 1]
    stale = Session(engine)
    with pytest.raises(LostSyncJobLease):
        _repo(stale).renew_heartbeat(
            lease,
            worker_id="worker-one",
            now=recovery_time,
            lease_duration=LEASE,
        )
    replacement = _acquire(
        stale,
        organization_id,
        worker="worker-two",
        now=recovery_time + timedelta(minutes=1),
    )
    assert replacement is not None and replacement.fencing_token == 2
    with pytest.raises(StaleSyncJobFence):
        _repo(stale).renew_heartbeat(
            lease,
            worker_id="worker-one",
            now=recovery_time + timedelta(minutes=1),
            lease_duration=LEASE,
        )
    stale.close()


def test_expired_recovery_at_attempt_limit_is_terminal(session):
    organization_id, connector_id, scope_id = _setup(session, "RecoveryExhausted")
    _enqueue(session, organization_id, connector_id, scope_id, maximum=1)
    session.commit()
    lease = _acquire(session, organization_id)
    assert lease is not None
    _repo(session).create_attempt_run(lease, worker_id="worker-one", now=NOW)
    session.commit()
    service = ConnectorSyncExecutionService(
        _repo(session),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=lambda: lease.lease_expires_at,
    )
    recovered = service.recover_expired(limit=1, organization_id=organization_id)
    assert len(recovered) == 1
    assert recovered[0].status == "failed" and recovered[0].next_attempt_at is None


def test_expired_cancellation_request_is_terminal_and_never_retries(session):
    organization_id, connector_id, scope_id = _setup(session, "RecoveryCancelled")
    _enqueue(session, organization_id, connector_id, scope_id, maximum=3)
    session.commit()
    lease = _acquire(session, organization_id)
    assert lease is not None
    _repo(session).create_attempt_run(lease, worker_id="worker-one", now=NOW)
    _repo(session).request_cancellation(
        organization_id,
        lease.job_id,
        now=NOW + timedelta(minutes=1),
    )
    session.commit()
    service = ConnectorSyncExecutionService(
        _repo(session),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=lambda: lease.lease_expires_at,
    )
    recovered = service.recover_expired(limit=1, organization_id=organization_id)
    assert len(recovered) == 1
    assert recovered[0].status == "cancelled" and recovered[0].next_attempt_at is None
    session.commit()
    assert _acquire(session, organization_id, now=NOW + timedelta(days=1)) is None


def test_run_linkage_is_unique_tenant_safe_and_legacy_nullable(session):
    organization_id, connector_id, scope_id = _setup(session, "RunLink")
    _enqueue(session, organization_id, connector_id, scope_id)
    session.commit()
    lease = _acquire(session, organization_id)
    assert lease is not None
    run = _repo(session).create_attempt_run(lease, worker_id="worker-one", now=NOW)
    session.commit()
    assert run.sync_job_id == lease.job_id and run.job_attempt_number == 1
    with pytest.raises(SyncJobConflict):
        _repo(session).create_attempt_run(lease, worker_id="worker-one", now=NOW)
    session.rollback()
    legacy = ConnectorSyncRun(
        id=uuid.uuid4(),
        organization_id=organization_id,
        connector_id=connector_id,
        connector_scope_id=scope_id,
        mode="incremental",
        trigger_type="manual",
        status="queued",
        run_metadata={},
    )
    session.add(legacy)
    session.flush()
    assert legacy.sync_job_id is None and legacy.job_attempt_number is None


def test_tenant_reads_mutations_and_history_are_bounded(session):
    organization_id, connector_id, scope_id = _setup(session, "TenantA")
    other_org, _, _ = _setup(session, "TenantB")
    job = _enqueue(session, organization_id, connector_id, scope_id)
    second_scope = _scope(session, organization_id, connector_id, "history-two")
    _enqueue(session, organization_id, connector_id, second_scope)
    session.commit()
    assert _repo(session).get(other_org, job.job_id) is None
    with pytest.raises(SyncJobNotFound):
        _repo(session).request_cancellation(other_org, job.job_id, now=NOW)
    assert _repo(session).acquire_next(
        other_org, worker_id="worker-other", lease_duration=LEASE, now=NOW
    ) is None
    first_page = _repo(session).list_history(organization_id, limit=1)
    assert len(first_page.items) == 1 and first_page.has_more and first_page.next_cursor
    second_page = _repo(session).list_history(
        organization_id, limit=1, cursor=first_page.next_cursor
    )
    assert len(second_page.items) == 1
    assert not hasattr(first_page.items[0], "lease_id")
    assert not hasattr(first_page.items[0], "lease_owner")


def test_caller_rollback_restores_acquisition_heartbeat_retry_and_completion(engine):
    setup = Session(engine, expire_on_commit=False)
    organization_id, connector_id, scope_id = _setup(setup, "Rollback")
    job = _enqueue(setup, organization_id, connector_id, scope_id)
    setup.commit()
    lease = _acquire(setup, organization_id)
    assert lease is not None
    setup.rollback()
    queued = setup.get(ConnectorSyncJob, job.job_id)
    setup.refresh(queued)
    assert queued.status == "queued" and queued.attempt_count == queued.fencing_token == 0
    lease = _acquire(setup, organization_id)
    assert lease is not None
    _repo(setup).create_attempt_run(lease, worker_id="worker-one", now=NOW)
    setup.commit()
    _repo(setup).renew_heartbeat(
        lease,
        worker_id="worker-one",
        now=NOW + timedelta(minutes=1),
        lease_duration=LEASE,
    )
    setup.rollback()
    row = setup.get(ConnectorSyncJob, job.job_id)
    setup.refresh(row)
    assert row.heartbeat_at == NOW and row.lease_expires_at == NOW + LEASE
    _repo(setup).record_failure(
        lease,
        worker_id="worker-one",
        now=NOW + timedelta(minutes=1),
        error_category="source_read",
        error_code="provider_temporarily_unavailable",
        retry_at=NOW + timedelta(minutes=2),
    )
    setup.rollback()
    row = setup.get(ConnectorSyncJob, job.job_id)
    setup.refresh(row)
    assert row.status == "running" and row.lease_id == lease.lease_id
    _repo(setup).complete_success(
        lease, worker_id="worker-one", now=NOW + timedelta(minutes=1)
    )
    setup.rollback()
    row = setup.get(ConnectorSyncJob, job.job_id)
    setup.refresh(row)
    assert row.status == "running" and row.completed_at is None
    setup.close()


def test_committed_indexes_are_available_to_critical_query_shapes(session):
    organization_id, connector_id, scope_id = _setup(session, "Plans")
    _enqueue(session, organization_id, connector_id, scope_id)
    session.commit()
    session.execute(text("SET LOCAL enable_seqscan = off"))
    session.execute(text("SET LOCAL enable_bitmapscan = off"))
    plans = []
    for sql, params in (
        (
            """EXPLAIN SELECT id FROM connector_sync_jobs
               WHERE status IN ('queued','retry_wait') AND next_attempt_at <= :now
               ORDER BY status,priority,next_attempt_at,created_at,id LIMIT 1""",
            {"now": NOW},
        ),
        (
            """EXPLAIN SELECT id FROM connector_sync_jobs
               WHERE status='running' AND lease_expires_at <= :now
               ORDER BY lease_expires_at,id LIMIT 10""",
            {"now": NOW},
        ),
        (
            """EXPLAIN SELECT id FROM connector_sync_jobs
               WHERE organization_id=:org AND connector_scope_id=:scope
               ORDER BY created_at,id LIMIT 10""",
            {"org": organization_id, "scope": scope_id},
        ),
    ):
        plans.append("\n".join(session.execute(text(sql), params).scalars()))
    assert "ix_sync_jobs_ready" in plans[0]
    assert "ix_sync_jobs_expired_leases" in plans[1]
    assert "ix_sync_jobs_org_scope_created" in plans[2]