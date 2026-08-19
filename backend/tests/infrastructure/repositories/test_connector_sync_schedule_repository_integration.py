from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess
import threading
import uuid

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from application.services.connector_sync_schedule_service import (
    ConnectorSyncScheduleService,
    SyncScheduleNotFound,
)
from infrastructure.db.models import ConnectorSyncJob, ConnectorSyncSchedule

ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
NOW = datetime(2026, 8, 24, 17, 20, tzinfo=timezone.utc)


class Clock:
    def __init__(self, value=NOW):
        self.value = value

    def __call__(self):
        return self.value


def _identity(url: str):
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database


@pytest.fixture(scope="module")
def engine():
    url = os.environ["TEST_DATABASE_URL"]
    development = os.environ.get("DATABASE_URL")
    if development and _identity(development) == _identity(url):
        raise RuntimeError("test database must differ from development database")
    reset = create_engine(url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    reset.dispose()
    environment = os.environ.copy()
    environment["DATABASE_URL"] = url
    subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(INI), "upgrade", "head"],
        check=True, cwd=str(ROOT), env=environment,
    )
    value = create_engine(url, future=True)
    try:
        yield value
    finally:
        value.dispose()


@pytest.fixture(autouse=True)
def clean(engine):
    with engine.begin() as connection:
        for table in (
            "connector_sync_schedules", "connector_sync_runs", "connector_sync_jobs",
            "connector_scopes", "connectors", "knowledge_spaces", "user_roles", "users",
            "organization_settings", "organizations",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


@pytest.fixture
def factory(engine):
    return sessionmaker(bind=engine, class_=Session, autoflush=False, expire_on_commit=False)


def _setup(factory, name: str):
    organization_id, user_id, connector_id, space_id, scope_id = [uuid.uuid4() for _ in range(5)]
    session = factory()
    session.execute(
        text("INSERT INTO organizations(id,name,slug) VALUES (:id,:name,:slug)"),
        {"id": organization_id, "name": name, "slug": f"{name.lower()}-{organization_id}"},
    )
    session.execute(
        text("""INSERT INTO users(id,organization_id,email,normalized_email,password_hash,display_name)
                VALUES (:id,:org,:email,:email,'hash',:name)"""),
        {"id": user_id, "org": organization_id, "email": f"{user_id}@example.com", "name": name},
    )
    session.execute(
        text("""INSERT INTO connectors(id,organization_id,connector_type,display_name,slug,status)
                VALUES (:id,:org,'local_folder',:name,:slug,'active')"""),
        {"id": connector_id, "org": organization_id, "name": name, "slug": f"connector-{connector_id}"},
    )
    session.execute(
        text("INSERT INTO knowledge_spaces(id,organization_id,name,slug) VALUES (:id,:org,:name,:slug)"),
        {"id": space_id, "org": organization_id, "name": name, "slug": f"space-{space_id}"},
    )
    session.execute(
        text("""INSERT INTO connector_scopes
                (id,organization_id,connector_id,knowledge_space_id,display_name,slug,scope_type,
                 external_scope_key,access_mode,status)
                VALUES (:id,:org,:connector,:space,:name,:slug,'folder',:key,'platform_managed','active')"""),
        {"id": scope_id, "org": organization_id, "connector": connector_id, "space": space_id,
         "name": name, "slug": f"scope-{scope_id}", "key": f"C:/safe/{scope_id}"},
    )
    session.commit()
    session.close()
    return organization_id, user_id, connector_id, scope_id


def _service(session, clock):
    return ConnectorSyncScheduleService(session, clock=clock)


def test_create_replace_pause_resume_delete_and_tenant_concealment(factory):
    clock = Clock()
    organization_id, user_id, connector_id, scope_id = _setup(factory, "Lifecycle")
    other_org, _, _, _ = _setup(factory, "Other")
    session = factory()
    service = _service(session, clock)
    created = service.create_or_replace(
        organization_id, user_id, connector_id, scope_id, interval_seconds=3600
    )
    assert created.next_run_at == NOW + timedelta(hours=1)
    schedule_id = created.schedule_id
    session.commit()
    clock.value = NOW + timedelta(minutes=10)
    replaced = service.create_or_replace(
        organization_id, user_id, connector_id, scope_id, interval_seconds=7200,
        first_run_at=clock.value + timedelta(hours=1),
    )
    session.commit()
    assert replaced.schedule_id == schedule_id and replaced.interval_seconds == 7200
    assert service.pause(organization_id, connector_id, scope_id).status == "paused"
    session.commit()
    clock.value = NOW + timedelta(hours=5, minutes=20)
    resumed = service.resume(organization_id, connector_id, scope_id)
    session.commit()
    assert resumed.status == "active" and resumed.next_run_at > clock.value
    with pytest.raises(SyncScheduleNotFound):
        service.get(other_org, connector_id, scope_id)
    service.delete(organization_id, connector_id, scope_id)
    session.commit()
    with pytest.raises(SyncScheduleNotFound):
        service.get(organization_id, connector_id, scope_id)
    session.close()


def test_due_order_future_paused_coalescing_and_invalid_resource_pause(factory):
    clock = Clock()
    first, second, future = (_setup(factory, name) for name in ("DueA", "DueB", "Future"))
    session = factory()
    service = _service(session, clock)
    for values, due in (
        (first, NOW - timedelta(hours=2)),
        (second, NOW - timedelta(hours=1)),
        (future, NOW + timedelta(hours=1)),
    ):
        service.create_or_replace(
            values[0], values[1], values[2], values[3], interval_seconds=3600,
            first_run_at=max(due, NOW),
        )
        session.execute(
            text("UPDATE connector_sync_schedules SET next_run_at=:due WHERE organization_id=:org"),
            {"due": due, "org": values[0]},
        )
    service.pause(second[0], second[2], second[3])
    session.commit()
    first_result = service.process_one_due()
    session.commit()
    assert first_result.outcome == "enqueued" and first_result.organization_id == first[0]
    assert first_result.next_run_at == NOW + timedelta(hours=1)
    assert service.process_one_due().outcome == "no_work"
    session.rollback()
    session.execute(
        text("UPDATE connector_sync_schedules SET next_run_at=:now WHERE organization_id=:org"),
        {"now": NOW, "org": first[0]},
    )
    session.commit()
    coalesced = service.process_one_due()
    session.commit()
    assert coalesced.outcome == "coalesced" and coalesced.job_id == first_result.job_id
    job = session.get(ConnectorSyncJob, first_result.job_id)
    assert job.status == "queued" and job.attempt_count == 0 and job.trigger_type == "scheduled"
    session.execute(
        text("UPDATE connector_sync_schedules SET next_run_at=:now WHERE organization_id=:org"),
        {"now": NOW, "org": future[0]},
    )
    session.execute(text("UPDATE connectors SET status='paused' WHERE id=:id"), {"id": future[2]})
    session.commit()
    invalid = service.process_one_due()
    session.commit()
    assert invalid.outcome == "paused"
    schedule = session.scalar(
        select(ConnectorSyncSchedule).where(ConnectorSyncSchedule.organization_id == future[0])
    )
    assert schedule.status == "paused" and schedule.pause_reason_code == "connector_inactive"
    session.close()


def test_concurrent_schedulers_claim_different_rows_with_skip_locked(factory):
    clock = Clock()
    rows = [_setup(factory, f"Concurrent{index}") for index in range(2)]
    setup = factory()
    service = _service(setup, clock)
    for organization_id, user_id, connector_id, scope_id in rows:
        service.create_or_replace(
            organization_id, user_id, connector_id, scope_id,
            interval_seconds=3600, first_run_at=NOW,
        )
    setup.commit()
    setup.close()
    barrier = threading.Barrier(2)
    results, errors = [], []

    def process():
        session = factory()
        try:
            barrier.wait()
            results.append(_service(session, clock).process_one_due())
            session.commit()
        except Exception as exc:
            errors.append(exc)
            session.rollback()
        finally:
            session.close()

    threads = [threading.Thread(target=process) for _ in range(2)]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    assert not errors
    assert {result.organization_id for result in results} == {rows[0][0], rows[1][0]}
    assert all(result.outcome == "enqueued" for result in results)


def test_two_schedulers_cannot_process_the_same_due_schedule(factory):
    clock = Clock()
    organization_id, user_id, connector_id, scope_id = _setup(factory, "SingleWinner")
    setup = factory()
    _service(setup, clock).create_or_replace(
        organization_id, user_id, connector_id, scope_id,
        interval_seconds=3600, first_run_at=NOW,
    )
    setup.commit(); setup.close()
    barrier = threading.Barrier(2)
    results = []

    def process():
        session = factory()
        try:
            barrier.wait()
            results.append(_service(session, clock).process_one_due())
            session.commit()
        finally:
            session.close()

    threads = [threading.Thread(target=process) for _ in range(2)]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    assert sorted(result.outcome for result in results) == ["enqueued", "no_work"]
    verify = factory()
    assert verify.scalar(select(func.count()).select_from(ConnectorSyncJob)) == 1
    verify.close()


def test_job_enqueue_and_schedule_advance_roll_back_together(factory):
    clock = Clock()
    organization_id, user_id, connector_id, scope_id = _setup(factory, "Rollback")
    session = factory()
    service = _service(session, clock)
    service.create_or_replace(
        organization_id, user_id, connector_id, scope_id,
        interval_seconds=3600, first_run_at=NOW,
    )
    session.commit()
    assert service.process_one_due().outcome == "enqueued"
    session.rollback()
    session.close()
    verify = factory()
    schedule = verify.scalar(select(ConnectorSyncSchedule))
    assert schedule.next_run_at == NOW and schedule.last_job_id is None
    assert verify.scalar(select(func.count()).select_from(ConnectorSyncJob)) == 0
    verify.close()