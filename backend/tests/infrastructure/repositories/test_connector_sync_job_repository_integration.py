from __future__ import annotations

import os
import subprocess
import threading
import time
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
from infrastructure.workers.lease_heartbeat import LeaseHeartbeat, LeaseHeartbeatFailure

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


@pytest.fixture(autouse=True)
def isolate_committed_test_state(engine):
    """Keep global claim tests independent despite their intentional commits."""
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM organizations"))


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


def _setup_github(session: Session, name: str = "GitHub"):
    organization_id, connector_id, scope_id = _setup(session, name)
    _exec(
        session,
        "UPDATE connectors SET connector_type='github' WHERE id=:id",
        id=connector_id,
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


def _acquire_local_folder(session: Session, *, worker="worker-one", now=NOW):
    return _repo(session).acquire_next_local_folder(
        worker_id=worker,
        lease_duration=LEASE,
        now=now,
    )


def _acquire_routed(session: Session, *, worker="worker-one", now=NOW):
    return _repo(session).acquire_next_routed(
        worker_id=worker,
        lease_duration=LEASE,
        now=now,
    )


def _service(session: Session, *, now=NOW) -> ConnectorSyncExecutionService:
    return ConnectorSyncExecutionService(
        _repo(session),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=lambda: now,
    )


def _target_claim(
    session: Session,
    organization_id,
    connector_id,
    scope_id,
    job_id,
    *,
    worker="targeted-planner",
    now=NOW,
):
    return _service(session, now=now).acquire_target_github(
        organization_id,
        connector_id,
        scope_id,
        job_id,
        worker_id=worker,
        lease_duration=LEASE,
    )


def test_targeted_github_claim_ignores_unrelated_eligible_jobs(session):
    organization_id, connector_id, target_scope = _setup_github(session, "TargetExact")
    unrelated_scope = _scope(session, organization_id, connector_id, "UnrelatedExact")
    unrelated = _enqueue(session, organization_id, connector_id, unrelated_scope)
    target = _enqueue(session, organization_id, connector_id, target_scope)
    session.commit()

    result = _target_claim(
        session,
        organization_id,
        connector_id,
        target_scope,
        target.job_id,
    )
    session.commit()

    assert result.outcome == "acquired"
    assert result.attempt is not None
    assert result.attempt.lease.job_id == target.job_id
    unrelated_state = _repo(session).get(organization_id, unrelated.job_id)
    assert unrelated_state is not None
    assert unrelated_state.status == "queued"
    assert unrelated_state.attempt_count == 0


@pytest.mark.parametrize(
    "mismatch",
    ("organization", "connector", "scope", "job"),
)
def test_targeted_github_claim_rejects_every_mismatched_identifier(session, mismatch):
    organization_id, connector_id, scope_id = _setup_github(session, f"Mismatch{mismatch}")
    job = _enqueue(session, organization_id, connector_id, scope_id)
    session.commit()
    identifiers = {
        "organization": organization_id,
        "connector": connector_id,
        "scope": scope_id,
        "job": job.job_id,
    }
    identifiers[mismatch] = uuid.uuid4()

    result = _target_claim(
        session,
        identifiers["organization"],
        identifiers["connector"],
        identifiers["scope"],
        identifiers["job"],
    )

    assert result.outcome == "not_found_or_mismatched"
    state = _repo(session).get(organization_id, job.job_id)
    assert state is not None
    assert state.status == "queued"
    assert state.attempt_count == 0


def test_targeted_github_claim_rejects_non_github_and_nonexistent_targets(session):
    organization_id, connector_id, scope_id = _setup(session, "NotGitHubTarget")
    job = _enqueue(session, organization_id, connector_id, scope_id)
    session.commit()

    wrong_type = _target_claim(
        session,
        organization_id,
        connector_id,
        scope_id,
        job.job_id,
    )
    missing = _target_claim(
        session,
        organization_id,
        connector_id,
        scope_id,
        uuid.uuid4(),
    )

    assert wrong_type.outcome == "not_found_or_mismatched"
    assert missing.outcome == "not_found_or_mismatched"
    state = _repo(session).get(organization_id, job.job_id)
    assert state is not None and state.status == "queued"


@pytest.mark.parametrize(
    ("sql_values", "expected_outcome"),
    (
        (
            {
                "status": "retry_wait",
                "attempt_count": 1,
                "fencing_token": 1,
                "next_attempt_at": NOW + timedelta(minutes=1),
            },
            "retry_not_due",
        ),
        ({"cancel_requested_at": NOW}, "cancelled"),
        (
            {"attempt_count": 3, "max_attempts": 3, "fencing_token": 3},
            "attempts_exhausted",
        ),
        (
            {
                "status": "succeeded",
                "attempt_count": 1,
                "fencing_token": 1,
                "next_attempt_at": None,
                "completed_at": NOW,
            },
            "completed",
        ),
        (
            {
                "status": "failed",
                "attempt_count": 1,
                "fencing_token": 1,
                "next_attempt_at": None,
                "completed_at": NOW,
            },
            "failed",
        ),
    ),
)
def test_targeted_github_claim_reports_ineligible_lifecycle_state(
    session,
    sql_values,
    expected_outcome,
):
    organization_id, connector_id, scope_id = _setup_github(
        session, f"Ineligible{expected_outcome}"
    )
    job = _enqueue(session, organization_id, connector_id, scope_id)
    session.commit()
    assignments = ", ".join(f"{name}=:{name}" for name in sql_values)
    _exec(
        session,
        f"UPDATE connector_sync_jobs SET {assignments} WHERE id=:job_id",
        job_id=job.job_id,
        **sql_values,
    )
    session.commit()

    result = _target_claim(
        session,
        organization_id,
        connector_id,
        scope_id,
        job.job_id,
    )

    assert result.outcome == expected_outcome
    assert result.attempt is None


def test_targeted_github_claim_accepts_due_retry_wait(session):
    organization_id, connector_id, scope_id = _setup_github(session, "DueRetryTarget")
    job = _enqueue(session, organization_id, connector_id, scope_id)
    _exec(
        session,
        """UPDATE connector_sync_jobs
           SET status='retry_wait', attempt_count=1, fencing_token=1
           WHERE id=:job_id""",
        job_id=job.job_id,
    )
    session.commit()

    result = _target_claim(
        session,
        organization_id,
        connector_id,
        scope_id,
        job.job_id,
    )

    assert result.outcome == "acquired"
    assert result.attempt is not None
    assert result.attempt.lease.attempt_number == 2


def test_targeted_github_claim_does_not_override_active_lease(session):
    organization_id, connector_id, scope_id = _setup_github(session, "LeasedTarget")
    job = _enqueue(session, organization_id, connector_id, scope_id)
    first = _target_claim(
        session,
        organization_id,
        connector_id,
        scope_id,
        job.job_id,
        worker="first-owner",
    )
    session.commit()

    second = _target_claim(
        session,
        organization_id,
        connector_id,
        scope_id,
        job.job_id,
        worker="second-owner",
    )

    assert first.outcome == "acquired"
    assert second.outcome == "owned_elsewhere"
    assert second.attempt is None


def test_targeted_recovery_changes_only_exact_expired_job_and_fences_stale_owner(session):
    organization_id, connector_id, target_scope = _setup_github(session, "TargetRecovery")
    unrelated_scope = _scope(session, organization_id, connector_id, "UnrelatedRecovery")
    target_job = _enqueue(session, organization_id, connector_id, target_scope)
    unrelated_job = _enqueue(session, organization_id, connector_id, unrelated_scope)
    target = _target_claim(
        session,
        organization_id,
        connector_id,
        target_scope,
        target_job.job_id,
        worker="stale-target",
    )
    unrelated = _target_claim(
        session,
        organization_id,
        connector_id,
        unrelated_scope,
        unrelated_job.job_id,
        worker="unrelated-owner",
    )
    session.commit()
    assert target.attempt is not None and unrelated.attempt is not None
    recovery_time = NOW + LEASE + timedelta(seconds=1)

    recovered = _service(session, now=recovery_time).recover_expired_target_github(
        organization_id,
        connector_id,
        target_scope,
        target_job.job_id,
    )
    session.commit()

    assert len(recovered) == 1
    assert recovered[0].job_id == target_job.job_id
    target_state = _repo(session).get(organization_id, target_job.job_id)
    unrelated_state = _repo(session).get(organization_id, unrelated_job.job_id)
    assert target_state is not None and target_state.status == "retry_wait"
    assert unrelated_state is not None and unrelated_state.status == "running"
    with pytest.raises(LostSyncJobLease):
        _service(session, now=recovery_time).heartbeat(
            target.attempt.lease,
            worker_id="stale-target",
            lease_duration=LEASE,
        )


def test_mismatched_target_recovery_leaves_expired_job_unchanged(session):
    organization_id, connector_id, scope_id = _setup_github(session, "MismatchedRecovery")
    job = _enqueue(session, organization_id, connector_id, scope_id)
    acquired = _target_claim(
        session,
        organization_id,
        connector_id,
        scope_id,
        job.job_id,
    )
    session.commit()
    assert acquired.attempt is not None
    recovery_time = NOW + LEASE + timedelta(seconds=1)

    recovered = _service(session, now=recovery_time).recover_expired_target_github(
        organization_id,
        connector_id,
        uuid.uuid4(),
        job.job_id,
    )

    assert recovered == ()
    state = _repo(session).get(organization_id, job.job_id)
    assert state is not None
    assert state.status == "running"
    assert state.last_error_code is None


@pytest.mark.parametrize(
    ("maximum", "request_cancellation", "expected_status"),
    ((3, True, "cancelled"), (1, False, "failed")),
)
def test_targeted_recovery_preserves_cancellation_and_attempt_limit_semantics(
    session,
    maximum,
    request_cancellation,
    expected_status,
):
    organization_id, connector_id, scope_id = _setup_github(
        session, f"Recovery{expected_status}"
    )
    job = _enqueue(
        session,
        organization_id,
        connector_id,
        scope_id,
        maximum=maximum,
    )
    acquired = _target_claim(
        session,
        organization_id,
        connector_id,
        scope_id,
        job.job_id,
    )
    assert acquired.attempt is not None
    if request_cancellation:
        _exec(
            session,
            "UPDATE connector_sync_jobs SET cancel_requested_at=:now WHERE id=:job_id",
            now=NOW,
            job_id=job.job_id,
        )
    session.commit()
    recovery_time = NOW + LEASE + timedelta(seconds=1)

    recovered = _service(session, now=recovery_time).recover_expired_target_github(
        organization_id,
        connector_id,
        scope_id,
        job.job_id,
    )
    session.commit()

    assert len(recovered) == 1
    assert recovered[0].status == expected_status
    state = _repo(session).get(organization_id, job.job_id)
    assert state is not None
    assert state.status == expected_status
    assert state.next_attempt_at is None


def test_targeted_claim_and_attempt_run_roll_back_together(engine):
    setup = Session(engine, expire_on_commit=False)
    organization_id, connector_id, scope_id = _setup_github(setup, "TargetRollback")
    job = _enqueue(setup, organization_id, connector_id, scope_id)
    setup.commit()
    result = _target_claim(
        setup,
        organization_id,
        connector_id,
        scope_id,
        job.job_id,
    )
    assert result.outcome == "acquired"
    setup.rollback()
    setup.close()

    verify = Session(engine, expire_on_commit=False)
    try:
        state = _repo(verify).get(organization_id, job.job_id)
        runs = verify.scalars(
            select(ConnectorSyncRun).where(ConnectorSyncRun.sync_job_id == job.job_id)
        ).all()
        assert state is not None
        assert state.status == "queued"
        assert state.attempt_count == 0
        assert runs == []
    finally:
        verify.close()


def test_targeted_attempt_run_failure_cannot_leave_committed_claim(engine):
    setup = Session(engine, expire_on_commit=False)
    organization_id, connector_id, scope_id = _setup_github(
        setup, "TargetRunFailure"
    )
    job = _enqueue(setup, organization_id, connector_id, scope_id)
    existing_run = ConnectorSyncRun(
        id=uuid.uuid4(),
        organization_id=organization_id,
        connector_id=connector_id,
        connector_scope_id=scope_id,
        sync_job_id=job.job_id,
        job_attempt_number=1,
        mode="incremental",
        trigger_type="manual",
        status="completed",
        started_at=NOW,
        heartbeat_at=NOW,
        finished_at=NOW,
        run_metadata={"fixture": "attempt_conflict"},
    )
    setup.add(existing_run)
    setup.commit()
    existing_run_id = existing_run.id

    with pytest.raises(SyncJobConflict, match="attempt run already exists"):
        _target_claim(
            setup,
            organization_id,
            connector_id,
            scope_id,
            job.job_id,
        )
    setup.rollback()
    setup.close()

    verify = Session(engine, expire_on_commit=False)
    try:
        state = _repo(verify).get(organization_id, job.job_id)
        runs = verify.scalars(
            select(ConnectorSyncRun).where(ConnectorSyncRun.sync_job_id == job.job_id)
        ).all()
        assert state is not None
        assert state.status == "queued"
        assert state.attempt_count == 0
        assert [run.id for run in runs] == [existing_run_id]
    finally:
        verify.close()


def test_targeted_recovery_rolls_back_without_partial_lifecycle_mutation(engine):
    setup = Session(engine, expire_on_commit=False)
    organization_id, connector_id, scope_id = _setup_github(setup, "RecoveryRollback")
    job = _enqueue(setup, organization_id, connector_id, scope_id)
    acquired = _target_claim(
        setup,
        organization_id,
        connector_id,
        scope_id,
        job.job_id,
    )
    setup.commit()
    assert acquired.attempt is not None
    recovery_time = NOW + LEASE + timedelta(seconds=1)

    recovered = _service(setup, now=recovery_time).recover_expired_target_github(
        organization_id,
        connector_id,
        scope_id,
        job.job_id,
    )
    assert len(recovered) == 1
    setup.rollback()
    setup.close()

    verify = Session(engine, expire_on_commit=False)
    try:
        state = _repo(verify).get(organization_id, job.job_id)
        assert state is not None
        assert state.status == "running"
        assert state.last_error_code is None
        assert state.attempt_count == acquired.attempt.lease.attempt_number
    finally:
        verify.close()


def test_concurrent_targeted_claim_has_exactly_one_winner(engine):
    setup = Session(engine, expire_on_commit=False)
    organization_id, connector_id, scope_id = _setup_github(
        setup, "TargetConcurrency"
    )
    job = _enqueue(setup, organization_id, connector_id, scope_id)
    setup.commit()
    setup.close()
    barrier = threading.Barrier(2)
    results, errors = [], []

    def claim(worker):
        value = Session(engine, expire_on_commit=False)
        try:
            barrier.wait()
            results.append(
                _target_claim(
                    value,
                    organization_id,
                    connector_id,
                    scope_id,
                    job.job_id,
                    worker=worker,
                )
            )
            value.commit()
        except Exception as error:  # pragma: no cover - asserted below
            errors.append(error)
            value.rollback()
        finally:
            value.close()

    threads = [
        threading.Thread(target=claim, args=(f"target-{index}",))
        for index in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    assert sum(result.outcome == "acquired" for result in results) == 1
    assert sum(result.outcome in {"owned_elsewhere", "not_eligible"} for result in results) == 1


def test_concurrent_targeted_and_global_claim_have_one_owner(engine):
    setup = Session(engine, expire_on_commit=False)
    organization_id, connector_id, scope_id = _setup_github(
        setup, "TargetGlobalRace"
    )
    job = _enqueue(setup, organization_id, connector_id, scope_id)
    setup.commit()
    setup.close()
    barrier = threading.Barrier(2)
    results = {}
    errors = []

    def claim_targeted():
        session = Session(engine, expire_on_commit=False)
        try:
            barrier.wait()
            results["targeted"] = _target_claim(
                session,
                organization_id,
                connector_id,
                scope_id,
                job.job_id,
                worker="targeted-racer",
            )
            session.commit()
        except Exception as error:
            errors.append(error)
            session.rollback()
        finally:
            session.close()

    def claim_global():
        session = Session(engine, expire_on_commit=False)
        try:
            barrier.wait()
            results["global"] = _service(session).acquire_one_github(
                worker_id="global-racer",
                lease_duration=LEASE,
            )
            session.commit()
        except Exception as error:
            errors.append(error)
            session.rollback()
        finally:
            session.close()

    threads = [
        threading.Thread(target=claim_targeted),
        threading.Thread(target=claim_global),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    targeted = results["targeted"]
    global_attempt = results["global"]
    winners = int(targeted.outcome == "acquired") + int(global_attempt is not None)
    assert winners == 1
    verify = Session(engine, expire_on_commit=False)
    try:
        runs = verify.scalars(
            select(ConnectorSyncRun).where(ConnectorSyncRun.sync_job_id == job.job_id)
        ).all()
        assert len(runs) == 1
    finally:
        verify.close()


def test_global_consumer_winning_target_does_not_make_targeted_claim_fall_back(session):
    organization_id, connector_id, target_scope = _setup_github(session, "CompetingTarget")
    unrelated_scope = _scope(session, organization_id, connector_id, "CompetingUnrelated")
    target = _repo(session).enqueue_or_coalesce(
        organization_id,
        connector_id,
        target_scope,
        mode="incremental",
        trigger_type="manual",
        priority=1,
        now=NOW,
    )
    unrelated = _repo(session).enqueue_or_coalesce(
        organization_id,
        connector_id,
        unrelated_scope,
        mode="incremental",
        trigger_type="manual",
        priority=100,
        now=NOW,
    )
    session.commit()
    global_winner = _service(session).acquire_one_github(
        worker_id="global-consumer",
        lease_duration=LEASE,
    )
    session.commit()
    assert global_winner is not None and global_winner.lease.job_id == target.job_id

    targeted = _target_claim(
        session,
        organization_id,
        connector_id,
        target_scope,
        target.job_id,
    )

    assert targeted.outcome == "owned_elsewhere"
    unrelated_state = _repo(session).get(organization_id, unrelated.job_id)
    assert unrelated_state is not None
    assert unrelated_state.status == "queued"
    assert unrelated_state.attempt_count == 0


def test_routed_claim_has_one_concurrent_winner_and_uses_persisted_type(engine):
    setup = Session(engine, expire_on_commit=False)
    organization_id, connector_id, scope_id = _setup(setup, "RoutedClaim")
    setup.execute(
        text("UPDATE connectors SET connector_type='github' WHERE id=:id"),
        {"id": connector_id},
    )
    job = _enqueue(setup, organization_id, connector_id, scope_id)
    setup.commit()
    setup.close()
    barrier = threading.Barrier(2)
    results, errors = [], []

    def claim(worker):
        value = Session(engine, expire_on_commit=False)
        try:
            barrier.wait()
            results.append(_acquire_routed(value, worker=worker))
            value.commit()
        except Exception as error:  # pragma: no cover - asserted below
            errors.append(error)
            value.rollback()
        finally:
            value.close()

    threads = [threading.Thread(target=claim, args=(f"routed-{index}",)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    assert winners[0].lease.job_id == job.job_id
    assert winners[0].connector_type == "github"


def test_local_and_routed_claims_share_the_same_skip_locked_mechanism(engine):
    setup = Session(engine, expire_on_commit=False)
    organization_id, connector_id, scope_id = _setup(setup, "SharedClaim")
    job = _enqueue(setup, organization_id, connector_id, scope_id)
    setup.commit()
    setup.close()
    barrier = threading.Barrier(2)
    results, errors = [], []

    def claim(local, worker):
        value = Session(engine, expire_on_commit=False)
        try:
            barrier.wait()
            result = (
                _acquire_local_folder(value, worker=worker)
                if local
                else _acquire_routed(value, worker=worker)
            )
            results.append(result)
            value.commit()
        except Exception as error:  # pragma: no cover - asserted below
            errors.append(error)
            value.rollback()
        finally:
            value.close()

    threads = [
        threading.Thread(target=claim, args=(True, "legacy-local")),
        threading.Thread(target=claim, args=(False, "routed-host")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    assert all(not thread.is_alive() for thread in threads)
    assert not errors
    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    winner_lease = winners[0].lease if hasattr(winners[0], "lease") else winners[0]
    assert winner_lease.job_id == job.job_id


def test_independent_heartbeat_sessions_commit_close_and_observe_cancellation(engine):
    setup = Session(engine, expire_on_commit=False)
    organization_id, connector_id, scope_id = _setup(setup, "HeartbeatThread")
    _enqueue(setup, organization_id, connector_id, scope_id)
    setup.commit()
    lease = _acquire(setup, organization_id, worker="heartbeat-owner")
    assert lease is not None
    _repo(setup).create_attempt_run(lease, worker_id="heartbeat-owner", now=NOW)
    setup.commit()
    setup.close()

    owners, opened, renewed = [], [], threading.Event()

    class TrackingSession(Session):
        def __init__(self):
            super().__init__(engine, expire_on_commit=False)
            self.was_committed = False
            self.was_rolled_back = False
            self.was_closed = False
            owners.append(threading.get_ident())
            opened.append(self)

        def commit(self):
            super().commit()
            self.was_committed = True

        def rollback(self):
            super().rollback()
            self.was_rolled_back = True

        def close(self):
            super().close()
            self.was_closed = True

    class ObservedExecution:
        def __init__(self, session):
            self._service = ConnectorSyncExecutionService(
                _repo(session),
                ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high),
                clock=lambda: NOW + timedelta(minutes=1),
            )

        def heartbeat(self, *args, **kwargs):
            result = self._service.heartbeat(*args, **kwargs)
            renewed.set()
            return result

    heartbeat = LeaseHeartbeat(
        TrackingSession,
        ObservedExecution,
        lease,
        worker_id="heartbeat-owner",
        lease_duration=LEASE,
        interval=timedelta(milliseconds=20),
        shutdown_timeout=timedelta(seconds=2),
    )
    heartbeat.__enter__()
    assert renewed.wait(2)
    cancellation = Session(engine, expire_on_commit=False)
    _repo(cancellation).request_cancellation(
        organization_id,
        lease.job_id,
        now=NOW + timedelta(minutes=2),
    )
    cancellation.commit()
    cancellation.close()
    for _ in range(200):
        try:
            heartbeat.raise_if_failed()
        except LeaseHeartbeatFailure:
            break
        time.sleep(0.01)
    with pytest.raises(LeaseHeartbeatFailure) as failure:
        heartbeat.stop()
    assert isinstance(failure.value.__cause__, SyncJobCancellationConflict)
    assert owners and all(owner != threading.get_ident() for owner in owners)
    assert any(item.was_committed for item in opened)
    assert any(item.was_rolled_back for item in opened)
    assert all(item.was_closed for item in opened)


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


def test_internal_local_folder_claim_selects_across_tenants_and_excludes_other_types(session):
    first_org, first_connector, first_scope = _setup(session, "GlobalA")
    second_org, second_connector, second_scope = _setup(session, "GlobalB")
    other_org, other_connector, other_scope = _setup(session, "GlobalOther")
    _exec(
        session,
        "UPDATE connectors SET connector_type='google_drive' WHERE id=:id",
        id=other_connector,
    )
    _enqueue(session, first_org, first_connector, first_scope)
    _enqueue(session, second_org, second_connector, second_scope)
    _enqueue(session, other_org, other_connector, other_scope)
    session.commit()

    leases = []
    for worker in ("global-one", "global-two"):
        lease = _acquire_local_folder(session, worker=worker)
        assert lease is not None
        leases.append(lease)
        session.commit()

    assert {lease.organization_id for lease in leases} == {first_org, second_org}
    assert _acquire_local_folder(session, worker="global-three") is None
    other_job = session.scalar(
        select(ConnectorSyncJob).where(ConnectorSyncJob.organization_id == other_org)
    )
    assert other_job is not None and other_job.status == "queued" and other_job.attempt_count == 0


def test_two_internal_hosts_cannot_claim_the_same_local_folder_job(engine):
    setup = Session(engine)
    organization_id, connector_id, scope_id = _setup(setup, "GlobalRace")
    _enqueue(setup, organization_id, connector_id, scope_id)
    setup.commit()
    setup.close()
    barrier = threading.Barrier(2)
    leases = []

    def acquire(worker: int):
        value = Session(engine, expire_on_commit=False)
        try:
            barrier.wait()
            leases.append(_acquire_local_folder(value, worker=f"global-{worker}"))
            value.commit()
        finally:
            value.close()

    threads = [threading.Thread(target=acquire, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len([lease for lease in leases if lease is not None]) == 1


def test_two_internal_hosts_can_claim_different_cross_tenant_jobs(engine):
    setup = Session(engine)
    first_org, first_connector, first_scope = _setup(setup, "GlobalParallelA")
    second_org, second_connector, second_scope = _setup(setup, "GlobalParallelB")
    _enqueue(setup, first_org, first_connector, first_scope)
    _enqueue(setup, second_org, second_connector, second_scope)
    setup.commit()
    setup.close()
    barrier = threading.Barrier(2)
    leases = []

    def acquire(worker: int):
        value = Session(engine, expire_on_commit=False)
        try:
            barrier.wait()
            leases.append(_acquire_local_folder(value, worker=f"parallel-{worker}"))
            value.commit()
        finally:
            value.close()

    threads = [threading.Thread(target=acquire, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(leases) == 2 and all(lease is not None for lease in leases)
    assert {lease.organization_id for lease in leases if lease is not None} == {
        first_org,
        second_org,
    }
    assert len({lease.job_id for lease in leases if lease is not None}) == 2


def test_internal_claim_excludes_future_and_terminal_jobs(session):
    organization_id, connector_id, future_scope = _setup(session, "GlobalEligibility")
    terminal_scope = _scope(session, organization_id, connector_id, "terminal")
    eligible_scope = _scope(session, organization_id, connector_id, "eligible")
    _repo(session).enqueue_or_coalesce(
        organization_id,
        connector_id,
        future_scope,
        mode="incremental",
        trigger_type="manual",
        now=NOW + timedelta(hours=1),
    )
    terminal = _enqueue(session, organization_id, connector_id, terminal_scope)
    eligible = _enqueue(session, organization_id, connector_id, eligible_scope)
    _repo(session).request_cancellation(organization_id, terminal.job_id, now=NOW)
    session.commit()
    lease = _acquire_local_folder(session)
    assert lease is not None and lease.job_id == eligible.job_id
    session.commit()
    assert _acquire_local_folder(session) is None


def test_internal_local_folder_recovery_excludes_other_types_and_fences_old_owner(session):
    _exec(session, "DELETE FROM connector_sync_runs")
    _exec(session, "DELETE FROM connector_sync_jobs")
    session.commit()
    local_org, local_connector, local_scope = _setup(session, "RecoverLocal")
    other_org, other_connector, other_scope = _setup(session, "RecoverOther")
    _enqueue(session, local_org, local_connector, local_scope, maximum=2)
    _enqueue(session, other_org, other_connector, other_scope, maximum=2)
    session.commit()
    local_lease = _acquire(session, local_org, worker="local-owner")
    other_lease = _acquire(session, other_org, worker="other-owner")
    assert local_lease is not None and other_lease is not None
    _repo(session).create_attempt_run(local_lease, worker_id="local-owner", now=NOW)
    _repo(session).create_attempt_run(other_lease, worker_id="other-owner", now=NOW)
    _exec(
        session,
        "UPDATE connectors SET connector_type='google_drive' WHERE id=:id",
        id=other_connector,
    )
    session.commit()
    service = ConnectorSyncExecutionService(
        _repo(session),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=lambda: local_lease.lease_expires_at,
    )
    recovered = service.recover_expired_local_folder(limit=10)
    assert len(recovered) == 1 and recovered[0].job_id == local_lease.job_id
    session.commit()
    local_job = session.get(ConnectorSyncJob, local_lease.job_id)
    other_job = session.get(ConnectorSyncJob, other_lease.job_id)
    assert local_job is not None and local_job.status == "retry_wait"
    assert other_job is not None and other_job.status == "running"
    with pytest.raises(LostSyncJobLease):
        _repo(session).complete_success(
            local_lease,
            worker_id="local-owner",
            now=local_lease.lease_expires_at,
        )


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


def test_connector_operational_reads_are_tenant_qualified_newest_first_and_bounded(session):
    organization_id, connector_id, scope_id = _setup(session, "Operations")
    other_org, other_connector, other_scope = _setup(session, "OperationsOther")
    first = _enqueue(session, organization_id, connector_id, scope_id)
    second_scope = _scope(session, organization_id, connector_id, "operations-two")
    second = _enqueue(session, organization_id, connector_id, second_scope)
    foreign = _enqueue(session, other_org, other_connector, other_scope)
    for attempt in range(1, 26):
        session.add(
            ConnectorSyncRun(
                id=uuid.uuid4(),
                organization_id=organization_id,
                connector_id=connector_id,
                connector_scope_id=scope_id,
                mode="incremental",
                trigger_type="manual",
                status="queued",
                run_metadata={},
                sync_job_id=first.job_id,
                job_attempt_number=attempt,
            )
        )
    session.commit()

    page_one = _repo(session).list_connector_history_page(
        organization_id, connector_id, page=1, page_size=1
    )
    page_two = _repo(session).list_connector_history_page(
        organization_id, connector_id, page=2, page_size=1
    )
    assert len(page_one.items) == len(page_two.items) == 1
    assert page_one.has_next and not page_two.has_next
    assert {page_one.items[0].job_id, page_two.items[0].job_id} == {
        first.job_id,
        second.job_id,
    }
    expected_newest = max((first.job_id, second.job_id), key=str)
    assert page_one.items[0].job_id == expected_newest
    assert _repo(session).get_for_connector(
        organization_id, connector_id, foreign.job_id
    ) is None
    assert _repo(session).get_for_connector(
        other_org, other_connector, foreign.job_id
    ).job_id == foreign.job_id

    runs = _repo(session).list_attempt_runs(
        organization_id, connector_id, first.job_id, limit=20
    )
    assert len(runs) == 20
    assert [run.attempt_number for run in runs] == list(range(25, 5, -1))
    assert _repo(session).list_attempt_runs(
        other_org, other_connector, first.job_id, limit=20
    ) == ()
    assert not hasattr(runs[0], "heartbeat_at")
    assert not hasattr(runs[0], "run_metadata")


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
