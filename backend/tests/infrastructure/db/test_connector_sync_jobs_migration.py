from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from infrastructure.db import models as db_models  # noqa: F401
from infrastructure.db.base import Base
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
PROJECT_VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
TEST_DATABASE_URL_ENV_VAR = "TEST_DATABASE_URL"
DATABASE_URL_ENV_VAR = "DATABASE_URL"
PRIOR_REVISION = "20260822_000013"
NOW = datetime(2026, 8, 23, 12, tzinfo=timezone.utc)
JOB_COLUMNS = [
    "id", "organization_id", "connector_id", "connector_scope_id", "mode", "trigger_type",
    "status", "requested_by_user_id", "priority", "attempt_count", "max_attempts",
    "next_attempt_at", "lease_owner", "lease_id", "fencing_token", "lease_acquired_at",
    "lease_expires_at", "heartbeat_at", "cancel_requested_at", "cancel_requested_by_user_id",
    "cancel_reason_code", "completed_at", "last_error_category", "last_error_code",
    "last_error_summary", "created_at", "updated_at",
]


def _identity(url: str) -> tuple[str, str | None, int | None, str | None]:
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def _upgrade(url: str) -> None:
    environment = os.environ.copy()
    environment[DATABASE_URL_ENV_VAR] = url
    subprocess.run(
        [str(PROJECT_VENV_PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", "head"],
        check=True,
        cwd=str(PROJECT_ROOT),
        env=environment,
    )


def _config() -> Config:
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", _required(TEST_DATABASE_URL_ENV_VAR))
    return config


@pytest.fixture(scope="module")
def engine():
    test_url = _required(TEST_DATABASE_URL_ENV_VAR)
    development_url = os.environ.get(DATABASE_URL_ENV_VAR)
    if development_url and _identity(development_url) == _identity(test_url):
        raise RuntimeError("TEST_DATABASE_URL must differ from DATABASE_URL")
    reset = create_engine(test_url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    reset.dispose()
    _upgrade(test_url)
    value = create_engine(test_url, future=True)
    try:
        yield value
    finally:
        value.dispose()


@pytest.fixture(autouse=True)
def clean(engine):
    with engine.begin() as connection:
        for table in (
            "source_acl_entries", "source_acl_snapshots", "external_group_memberships",
            "external_directory_states", "user_external_identity_links", "external_principals",
            "document_indexing_attempts", "document_indexing_states", "document_version_documents",
            "document_versions", "connector_sync_cursors", "connector_sync_errors",
            "connector_sync_items", "connector_sync_runs", "connector_sync_jobs",
            "source_item_scope_memberships", "source_items", "connector_scopes", "connectors",
            "audit_events", "knowledge_space_user_grants", "knowledge_space_team_grants",
            "knowledge_space_department_grants", "knowledge_space_organization_grants",
            "knowledge_spaces", "team_memberships", "department_memberships", "teams",
            "departments", "document_chunks", "documents", "authentication_sessions", "user_roles",
            "users", "organization_settings", "organizations", "industries",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


def _execute(engine, sql: str, **params):
    with engine.begin() as connection:
        return connection.execute(text(sql), params)


def _count(engine, table: str) -> int:
    with engine.connect() as connection:
        return connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()


def _organization(engine, name: str) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(
        engine,
        "INSERT INTO organizations (id, name, slug) VALUES (:id, :name, :slug)",
        id=value,
        name=name,
        slug=f"{name.lower()}-{value}",
    )
    return value


def _user(engine, organization_id: uuid.UUID, email: str) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(
        engine,
        """INSERT INTO users
           (id, organization_id, email, normalized_email, password_hash, display_name)
           VALUES (:id, :org, :email, :email, 'hash', :email)""",
        id=value,
        org=organization_id,
        email=email,
    )
    return value


def _connector(engine, organization_id: uuid.UUID, slug: str) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(
        engine,
        """INSERT INTO connectors
           (id, organization_id, connector_type, display_name, slug)
           VALUES (:id, :org, 'local_folder', :slug, :slug)""",
        id=value,
        org=organization_id,
        slug=slug,
    )
    return value


def _scope(engine, organization_id: uuid.UUID, connector_id: uuid.UUID, slug: str) -> uuid.UUID:
    space_id, scope_id = uuid.uuid4(), uuid.uuid4()
    _execute(
        engine,
        "INSERT INTO knowledge_spaces (id, organization_id, name, slug) VALUES (:id, :org, :slug, :slug)",
        id=space_id,
        org=organization_id,
        slug=f"space-{slug}",
    )
    _execute(
        engine,
        """INSERT INTO connector_scopes
           (id, organization_id, connector_id, knowledge_space_id, display_name, slug,
            scope_type, external_scope_key, access_mode)
           VALUES (:id, :org, :connector, :space, :slug, :slug, 'folder', :key, 'platform_managed')""",
        id=scope_id,
        org=organization_id,
        connector=connector_id,
        space=space_id,
        slug=slug,
        key=f"C:/safe/{slug}",
    )
    return scope_id


def _setup(engine, name: str = "Alpha") -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    organization_id = _organization(engine, name)
    connector_id = _connector(engine, organization_id, f"connector-{name.lower()}")
    scope_id = _scope(engine, organization_id, connector_id, f"scope-{name.lower()}")
    return organization_id, connector_id, scope_id


def _job(engine, organization_id, connector_id, scope_id, **overrides) -> uuid.UUID:
    values = {
        "id": uuid.uuid4(),
        "organization_id": organization_id,
        "connector_id": connector_id,
        "connector_scope_id": scope_id,
        "mode": "incremental",
        "trigger_type": "manual",
        "status": "queued",
        "requested_by_user_id": None,
        "priority": 100,
        "attempt_count": 0,
        "max_attempts": 3,
        "next_attempt_at": NOW,
        "lease_owner": None,
        "lease_id": None,
        "fencing_token": 0,
        "lease_acquired_at": None,
        "lease_expires_at": None,
        "heartbeat_at": None,
        "cancel_requested_at": None,
        "cancel_requested_by_user_id": None,
        "cancel_reason_code": None,
        "completed_at": None,
        "last_error_category": None,
        "last_error_code": None,
        "last_error_summary": None,
        "created_at": NOW,
        "updated_at": NOW,
    }
    values.update(overrides)
    columns = ", ".join(values)
    parameters = ", ".join(f":{name}" for name in values)
    _execute(engine, f"INSERT INTO connector_sync_jobs ({columns}) VALUES ({parameters})", **values)
    return values["id"]


def _running_values(attempt: int = 1, *, acquired_at: datetime = NOW) -> dict[str, object]:
    return {
        "status": "running",
        "attempt_count": attempt,
        "next_attempt_at": None,
        "lease_owner": f"worker-{attempt}",
        "lease_id": uuid.uuid4(),
        "fencing_token": attempt,
        "lease_acquired_at": acquired_at,
        "lease_expires_at": acquired_at + timedelta(minutes=5),
        "heartbeat_at": acquired_at,
    }


def _run(engine, organization_id, connector_id, scope_id, job_id, attempt, *, status="running"):
    value = uuid.uuid4()
    finished_at = NOW + timedelta(minutes=1) if status != "running" else None
    _execute(
        engine,
        """INSERT INTO connector_sync_runs
           (id, organization_id, connector_id, connector_scope_id, mode, trigger_type, status,
            started_at, finished_at, sync_job_id, job_attempt_number)
           VALUES (:id, :org, :connector, :scope, :mode, :trigger, :status,
                   :started, :finished, :job, :attempt)""",
        id=value,
        org=organization_id,
        connector=connector_id,
        scope=scope_id,
        mode="incremental" if attempt == 1 else "retry",
        trigger="manual" if attempt == 1 else "retry",
        status=status,
        started=NOW,
        finished=finished_at,
        job=job_id,
        attempt=attempt,
    )
    return value


def test_schema_reflection_matches_orm_and_operational_contract(engine):
    inspector = inspect(engine)
    assert "connector_sync_jobs" in inspector.get_table_names(schema="public")
    reflected = inspector.get_columns("connector_sync_jobs", schema="public")
    model_columns = list(Base.metadata.tables["connector_sync_jobs"].columns)
    assert [column["name"] for column in reflected] == JOB_COLUMNS
    assert [column.name for column in model_columns] == JOB_COLUMNS
    for model_column, database_column in zip(model_columns, reflected, strict=True):
        assert model_column.type._type_affinity is database_column["type"]._type_affinity
        if hasattr(model_column.type, "length"):
            assert model_column.type.length == database_column["type"].length
        if hasattr(model_column.type, "timezone"):
            assert model_column.type.timezone == database_column["type"].timezone
        assert model_column.nullable == database_column["nullable"]
        assert (model_column.server_default is None) == (database_column["default"] is None)
    defaults = {column["name"]: column["default"] for column in reflected}
    assert "queued" in defaults["status"]
    assert defaults["priority"] == "100"
    assert defaults["attempt_count"] == "0"
    assert defaults["max_attempts"] == "3"
    assert defaults["next_attempt_at"] == "now()"
    assert defaults["fencing_token"] == "0"
    assert defaults["created_at"] == "now()"
    assert defaults["updated_at"] == "now()"

    table = Base.metadata.tables["connector_sync_jobs"]
    model_constraint_names = {constraint.name for constraint in table.constraints if constraint.name}
    database_constraint_names = {
        inspector.get_pk_constraint("connector_sync_jobs", schema="public")["name"],
        *(item["name"] for item in inspector.get_unique_constraints("connector_sync_jobs", schema="public")),
        *(item["name"] for item in inspector.get_check_constraints("connector_sync_jobs", schema="public")),
        *(item["name"] for item in inspector.get_foreign_keys("connector_sync_jobs", schema="public")),
    }
    assert model_constraint_names == database_constraint_names
    assert inspector.get_pk_constraint("connector_sync_jobs", schema="public")["constrained_columns"] == ["id"]
    unique_constraints = {
        item["name"]: item["column_names"]
        for item in inspector.get_unique_constraints("connector_sync_jobs", schema="public")
    }
    assert unique_constraints == {
        "uq_sync_jobs_organization_id_id": ["organization_id", "id"],
        "uq_sync_jobs_scope_id": ["organization_id", "connector_id", "connector_scope_id", "id"],
        "uq_sync_jobs_lease_id": ["lease_id"],
    }

    foreign_keys = {
        item["name"]: (
            item["constrained_columns"], item["referred_table"], item["referred_columns"],
            item["options"]["ondelete"],
        )
        for item in inspector.get_foreign_keys("connector_sync_jobs", schema="public")
    }
    assert foreign_keys == {
        "fk_sync_jobs_organization": (
            ["organization_id"], "organizations", ["id"], "CASCADE"
        ),
        "fk_sync_jobs_connector_tenant": (
            ["organization_id", "connector_id"], "connectors",
            ["organization_id", "id"], "CASCADE",
        ),
        "fk_sync_jobs_scope_tenant": (
            ["organization_id", "connector_id", "connector_scope_id"], "connector_scopes",
            ["organization_id", "connector_id", "id"], "CASCADE",
        ),
        "fk_sync_jobs_requester_tenant": (
            ["organization_id", "requested_by_user_id"],
            "users",
            ["organization_id", "id"],
            "SET NULL (requested_by_user_id)",
        ),
        "fk_sync_jobs_cancel_requester_tenant": (
            ["organization_id", "cancel_requested_by_user_id"],
            "users",
            ["organization_id", "id"],
            "SET NULL (cancel_requested_by_user_id)",
        ),
    }

    indexes = {item["name"]: item for item in inspector.get_indexes("connector_sync_jobs", schema="public")}
    assert indexes["uq_sync_jobs_org_scope_nonterminal"]["unique"] is True
    assert indexes["uq_sync_jobs_org_scope_nonterminal"]["column_names"] == [
        "organization_id", "connector_scope_id"
    ]
    assert indexes["ix_sync_jobs_ready"]["column_names"] == [
        "status", "priority", "next_attempt_at", "created_at", "id"
    ]
    assert indexes["ix_sync_jobs_expired_leases"]["column_names"] == ["status", "lease_expires_at"]
    assert indexes["ix_sync_jobs_cancellation_requests"]["column_names"] == [
        "status", "cancel_requested_at"
    ]
    assert indexes["ix_sync_jobs_org_scope_created"]["column_names"] == [
        "organization_id", "connector_scope_id", "created_at", "id"
    ]
    assert indexes["ix_sync_jobs_org_connector_created"]["column_names"] == [
        "organization_id", "connector_id", "created_at", "id"
    ]
    assert all(
        indexes[name].get("dialect_options", {}).get("postgresql_where") is not None
        for name in (
            "uq_sync_jobs_org_scope_nonterminal",
            "ix_sync_jobs_expired_leases",
            "ix_sync_jobs_cancellation_requests",
        )
    )
    predicates = {
        name: str(indexes[name]["dialect_options"]["postgresql_where"])
        for name in (
            "uq_sync_jobs_org_scope_nonterminal",
            "ix_sync_jobs_expired_leases",
            "ix_sync_jobs_cancellation_requests",
        )
    }
    assert all(value in predicates["uq_sync_jobs_org_scope_nonterminal"] for value in (
        "queued", "running", "retry_wait"
    ))
    assert "running" in predicates["ix_sync_jobs_expired_leases"]
    assert all(value in predicates["ix_sync_jobs_cancellation_requests"] for value in (
        "cancel_requested_at", "completed_at"
    ))

    run_columns = inspector.get_columns("connector_sync_runs", schema="public")
    assert [item["name"] for item in run_columns][-2:] == ["sync_job_id", "job_attempt_number"]
    assert [column.name for column in Base.metadata.tables["connector_sync_runs"].columns] == [
        item["name"] for item in run_columns
    ]
    run_foreign_keys = {
        item["name"]: item for item in inspector.get_foreign_keys("connector_sync_runs", schema="public")
    }
    assert run_foreign_keys["fk_sync_runs_job_tenant"]["constrained_columns"] == [
        "organization_id", "connector_id", "connector_scope_id", "sync_job_id"
    ]
    assert run_foreign_keys["fk_sync_runs_job_tenant"]["options"]["ondelete"] == "CASCADE"
    run_constraint_names = {
        *(item["name"] for item in inspector.get_unique_constraints("connector_sync_runs", schema="public")),
        *(item["name"] for item in inspector.get_check_constraints("connector_sync_runs", schema="public")),
        *(item["name"] for item in inspector.get_foreign_keys("connector_sync_runs", schema="public")),
        inspector.get_pk_constraint("connector_sync_runs", schema="public")["name"],
    }
    assert run_constraint_names == {
        constraint.name
        for constraint in Base.metadata.tables["connector_sync_runs"].constraints
        if constraint.name
    }


def test_valid_queue_lease_heartbeat_cancellation_and_terminal_outcomes(engine):
    organization_id, connector_id, first_scope = _setup(engine)
    requester = _user(engine, organization_id, "requester@example.com")
    manual_job = _job(
        engine, organization_id, connector_id, first_scope, requested_by_user_id=requester
    )
    scheduled_scope = _scope(engine, organization_id, connector_id, "scheduled")
    scheduled_job = _job(
        engine,
        organization_id,
        connector_id,
        scheduled_scope,
        trigger_type="scheduled",
        next_attempt_at=NOW + timedelta(hours=1),
        priority=200,
    )

    lease_id = uuid.uuid4()
    _execute(
        engine,
        """UPDATE connector_sync_jobs SET status='running', attempt_count=1, next_attempt_at=NULL,
           lease_owner='worker-primary', lease_id=:lease, fencing_token=1, lease_acquired_at=:now,
           lease_expires_at=:expires, heartbeat_at=:now WHERE id=:id""",
        id=manual_job,
        lease=lease_id,
        now=NOW,
        expires=NOW + timedelta(minutes=5),
    )
    heartbeat = NOW + timedelta(minutes=2)
    _execute(
        engine,
        "UPDATE connector_sync_jobs SET heartbeat_at=:heartbeat, lease_expires_at=:expires WHERE id=:id",
        id=manual_job,
        heartbeat=heartbeat,
        expires=heartbeat + timedelta(minutes=5),
    )
    _execute(
        engine,
        """UPDATE connector_sync_jobs SET cancel_requested_at=:requested,
           cancel_requested_by_user_id=:user, cancel_reason_code='user_requested' WHERE id=:id""",
        id=manual_job,
        requested=heartbeat,
        user=requester,
    )
    with engine.connect() as connection:
        row = connection.execute(
            text("SELECT status, cancel_requested_at, completed_at FROM connector_sync_jobs WHERE id=:id"),
            {"id": manual_job},
        ).one()
        assert row.status == "running" and row.cancel_requested_at == heartbeat and row.completed_at is None
    _execute(
        engine,
        """UPDATE connector_sync_jobs SET status='cancelled', lease_owner=NULL, lease_id=NULL,
           lease_acquired_at=NULL, lease_expires_at=NULL, heartbeat_at=NULL, completed_at=:completed
           WHERE id=:id""",
        id=manual_job,
        completed=heartbeat + timedelta(seconds=1),
    )

    running = _running_values()
    assignments = ", ".join(f"{name}=:{name}" for name in running)
    _execute(
        engine,
        f"UPDATE connector_sync_jobs SET {assignments} WHERE id=:id",
        id=scheduled_job,
        **running,
    )
    _execute(
        engine,
        """UPDATE connector_sync_jobs SET status='succeeded', lease_owner=NULL, lease_id=NULL,
           lease_acquired_at=NULL, lease_expires_at=NULL, heartbeat_at=NULL, completed_at=:completed
           WHERE id=:id""",
        id=scheduled_job,
        completed=NOW + timedelta(minutes=6),
    )

    cancelled_scope = _scope(engine, organization_id, connector_id, "cancel-before-run")
    cancelled_job = _job(
        engine,
        organization_id,
        connector_id,
        cancelled_scope,
        cancel_requested_at=NOW + timedelta(seconds=1),
        cancel_reason_code="no_longer_needed",
    )
    _execute(
        engine,
        "UPDATE connector_sync_jobs SET status='cancelled', next_attempt_at=NULL, completed_at=:done WHERE id=:id",
        id=cancelled_job,
        done=NOW + timedelta(seconds=2),
    )
    assert _count(engine, "connector_sync_jobs") == 3


def test_retry_backoff_recovery_fencing_and_distinct_run_history(engine):
    organization_id, connector_id, scope_id = _setup(engine)
    job_id = _job(engine, organization_id, connector_id, scope_id, max_attempts=2)
    running = _running_values()
    _execute(
        engine,
        """UPDATE connector_sync_jobs SET status=:status, attempt_count=:attempt_count,
           next_attempt_at=:next_attempt_at, lease_owner=:lease_owner, lease_id=:lease_id,
           fencing_token=:fencing_token, lease_acquired_at=:lease_acquired_at,
           lease_expires_at=:lease_expires_at, heartbeat_at=:heartbeat_at WHERE id=:id""",
        id=job_id,
        **running,
    )
    first_run = _run(engine, organization_id, connector_id, scope_id, job_id, 1)
    _execute(
        engine,
        "UPDATE connector_sync_runs SET status='failed', finished_at=:done WHERE id=:id",
        id=first_run,
        done=NOW + timedelta(minutes=1),
    )
    _execute(
        engine,
        """UPDATE connector_sync_jobs SET status='retry_wait', next_attempt_at=:retry_at,
           lease_owner=NULL, lease_id=NULL, lease_acquired_at=NULL, lease_expires_at=NULL,
           heartbeat_at=NULL, last_error_category='source_read', last_error_code='source_unavailable',
           last_error_summary='Temporary source read failure' WHERE id=:id""",
        id=job_id,
        retry_at=NOW + timedelta(minutes=10),
    )
    recovered_at = NOW + timedelta(minutes=11)
    recovered_lease = uuid.uuid4()
    _execute(
        engine,
        """UPDATE connector_sync_jobs SET status='running', attempt_count=2, next_attempt_at=NULL,
           lease_owner='worker-recovery', lease_id=:lease, fencing_token=2,
           lease_acquired_at=:acquired, lease_expires_at=:expires, heartbeat_at=:acquired
           WHERE id=:id""",
        id=job_id,
        lease=recovered_lease,
        acquired=recovered_at,
        expires=recovered_at + timedelta(minutes=5),
    )
    second_run = _run(engine, organization_id, connector_id, scope_id, job_id, 2)
    assert second_run != first_run
    _execute(
        engine,
        "UPDATE connector_sync_runs SET status='failed', finished_at=:done WHERE id=:id",
        id=second_run,
        done=recovered_at + timedelta(minutes=1),
    )
    _execute(
        engine,
        """UPDATE connector_sync_jobs SET status='failed', lease_owner=NULL, lease_id=NULL,
           lease_acquired_at=NULL, lease_expires_at=NULL, heartbeat_at=NULL, completed_at=:done
           WHERE id=:id""",
        id=job_id,
        done=recovered_at + timedelta(minutes=1),
    )
    with engine.connect() as connection:
        attempts = connection.execute(
            text("""SELECT job_attempt_number FROM connector_sync_runs
                     WHERE organization_id=:org AND sync_job_id=:job ORDER BY job_attempt_number"""),
            {"org": organization_id, "job": job_id},
        ).scalars().all()
        job = connection.execute(
            text("SELECT status, attempt_count, fencing_token, next_attempt_at FROM connector_sync_jobs WHERE id=:id"),
            {"id": job_id},
        ).one()
    assert attempts == [1, 2]
    assert job == ("failed", 2, 2, None)


@pytest.mark.parametrize(
    "overrides",
    (
        {"status": "unknown"},
        {"mode": "retry"},
        {"trigger_type": "retry"},
        {"attempt_count": -1, "fencing_token": -1},
        {"max_attempts": 0},
        {"attempt_count": 4, "fencing_token": 4, "max_attempts": 3},
        {"attempt_count": 0, "fencing_token": 1},
        {"fencing_token": -1},
        {"status": "succeeded", "attempt_count": 1, "fencing_token": 1, "next_attempt_at": None},
        {"status": "queued", "completed_at": NOW + timedelta(seconds=1)},
        {"last_error_category": "unsafe", "last_error_code": "failure"},
        {"last_error_category": "internal"},
        {"last_error_category": "internal", "last_error_code": "Bad-Code"},
        {"last_error_summary": "summary without controlled code"},
    ),
)
def test_invalid_identity_counts_lifecycle_completion_and_error_fields_are_rejected(engine, overrides):
    organization_id, connector_id, scope_id = _setup(engine)
    with pytest.raises(IntegrityError):
        _job(engine, organization_id, connector_id, scope_id, **overrides)


@pytest.mark.parametrize(
    "overrides",
    (
        {"status": "running", "attempt_count": 1, "fencing_token": 1, "next_attempt_at": None},
        {**_running_values(), "lease_owner": "   "},
        {**_running_values(), "lease_expires_at": NOW},
        {**_running_values(), "heartbeat_at": NOW - timedelta(seconds=1)},
        {**_running_values(), "heartbeat_at": NOW + timedelta(minutes=5)},
        {
            **_running_values(),
            "status": "succeeded",
            "completed_at": NOW + timedelta(minutes=1),
        },
    ),
)
def test_invalid_lease_combinations_and_timestamp_ordering_are_rejected(engine, overrides):
    organization_id, connector_id, scope_id = _setup(engine)
    with pytest.raises(IntegrityError):
        _job(engine, organization_id, connector_id, scope_id, **overrides)


@pytest.mark.parametrize(
    "overrides",
    (
        {"cancel_requested_by_user_id": uuid.uuid4()},
        {"cancel_reason_code": "user requested"},
        {"cancel_requested_at": NOW - timedelta(seconds=1)},
        {"status": "cancelled", "next_attempt_at": None, "completed_at": NOW + timedelta(seconds=1)},
        {
            "status": "cancelled", "next_attempt_at": None,
            "cancel_requested_at": NOW + timedelta(seconds=2),
            "completed_at": NOW + timedelta(seconds=1),
        },
        {"status": "retry_wait", "attempt_count": 1, "fencing_token": 1, "next_attempt_at": None},
        {"status": "retry_wait", "attempt_count": 0, "fencing_token": 0},
        {"status": "retry_wait", "attempt_count": 3, "fencing_token": 3, "max_attempts": 3},
        {"status": "failed", "attempt_count": 1, "fencing_token": 1, "next_attempt_at": None},
    ),
)
def test_invalid_cancellation_retry_and_terminal_combinations_are_rejected(engine, overrides):
    organization_id, connector_id, scope_id = _setup(engine)
    with pytest.raises(IntegrityError):
        _job(engine, organization_id, connector_id, scope_id, **overrides)


def test_tenant_boundaries_duplicate_policy_and_attempt_linkage(engine):
    organization_id, connector_id, scope_id = _setup(engine)
    other_org, other_connector, other_scope = _setup(engine, "Beta")
    with pytest.raises(IntegrityError):
        _job(engine, organization_id, other_connector, other_scope)

    job_id = _job(engine, organization_id, connector_id, scope_id)
    with pytest.raises(IntegrityError):
        _job(engine, organization_id, connector_id, scope_id)
    terminal_id = _job(
        engine,
        organization_id,
        connector_id,
        _scope(engine, organization_id, connector_id, "terminal"),
        status="cancelled",
        next_attempt_at=None,
        cancel_requested_at=NOW,
        completed_at=NOW,
    )
    assert terminal_id

    _run(engine, organization_id, connector_id, scope_id, job_id, 1, status="failed")
    with pytest.raises(IntegrityError):
        _run(engine, organization_id, connector_id, scope_id, job_id, 1, status="failed")
    with pytest.raises(IntegrityError):
        _run(engine, other_org, other_connector, other_scope, job_id, 2, status="failed")
    for linked_job, attempt_number in ((None, 1), (job_id, None), (job_id, 0)):
        with pytest.raises(IntegrityError):
            _execute(
                engine,
                """INSERT INTO connector_sync_runs
                   (id, organization_id, connector_id, connector_scope_id, mode, trigger_type,
                    sync_job_id, job_attempt_number)
                   VALUES (:id, :org, :connector, :scope, 'incremental', 'manual', :job, :attempt)""",
                id=uuid.uuid4(),
                org=organization_id,
                connector=connector_id,
                scope=scope_id,
                job=linked_job,
                attempt=attempt_number,
            )

    shared_lease = uuid.uuid4()
    first_lease_scope = _scope(engine, organization_id, connector_id, "lease-one")
    second_lease_scope = _scope(engine, organization_id, connector_id, "lease-two")
    _job(engine, organization_id, connector_id, first_lease_scope, **{
        **_running_values(), "lease_id": shared_lease,
    })
    with pytest.raises(IntegrityError):
        _job(engine, organization_id, connector_id, second_lease_scope, **{
            **_running_values(), "lease_id": shared_lease,
        })


def test_conditional_claim_and_expired_recovery_have_single_winners(engine):
    organization_id, connector_id, scope_id = _setup(engine)
    job_id = _job(engine, organization_id, connector_id, scope_id)
    first_lease = uuid.uuid4()
    claim_sql = """UPDATE connector_sync_jobs
        SET status='running', attempt_count=attempt_count + 1, fencing_token=fencing_token + 1,
            next_attempt_at=NULL, lease_owner=:owner, lease_id=:lease, lease_acquired_at=:now,
            lease_expires_at=:expires, heartbeat_at=:now
        WHERE id=:id AND status IN ('queued', 'retry_wait') AND next_attempt_at <= :now
          AND attempt_count < max_attempts
        RETURNING fencing_token"""
    with engine.begin() as connection:
        first = connection.execute(
            text(claim_sql),
            {
                "id": job_id, "owner": "worker-one", "lease": first_lease,
                "now": NOW, "expires": NOW + timedelta(minutes=1),
            },
        ).scalar_one_or_none()
        second = connection.execute(
            text(claim_sql),
            {
                "id": job_id, "owner": "worker-two", "lease": uuid.uuid4(),
                "now": NOW, "expires": NOW + timedelta(minutes=1),
            },
        ).scalar_one_or_none()
    assert first == 1 and second is None

    recovery_time = NOW + timedelta(minutes=2)
    recovery_sql = """UPDATE connector_sync_jobs
        SET attempt_count=attempt_count + 1, fencing_token=fencing_token + 1,
            lease_owner=:owner, lease_id=:new_lease, lease_acquired_at=:now,
            lease_expires_at=:expires, heartbeat_at=:now
        WHERE id=:id AND status='running' AND lease_id=:old_lease AND fencing_token=:old_fence
          AND lease_expires_at <= :now AND attempt_count < max_attempts
        RETURNING fencing_token"""
    with engine.begin() as connection:
        recovered = connection.execute(
            text(recovery_sql),
            {
                "id": job_id, "owner": "worker-recovery", "new_lease": uuid.uuid4(),
                "old_lease": first_lease, "old_fence": 1, "now": recovery_time,
                "expires": recovery_time + timedelta(minutes=1),
            },
        ).scalar_one_or_none()
        stale_recovery = connection.execute(
            text(recovery_sql),
            {
                "id": job_id, "owner": "worker-late", "new_lease": uuid.uuid4(),
                "old_lease": first_lease, "old_fence": 1, "now": recovery_time,
                "expires": recovery_time + timedelta(minutes=1),
            },
        ).scalar_one_or_none()
    assert recovered == 2 and stale_recovery is None


def test_user_deletion_cascades_and_audit_retention_remain_deliberate(engine):
    organization_id, connector_id, scope_id = _setup(engine)
    requester = _user(engine, organization_id, "requester@example.com")
    job_id = _job(
        engine,
        organization_id,
        connector_id,
        scope_id,
        requested_by_user_id=requester,
        cancel_requested_at=NOW,
        cancel_requested_by_user_id=requester,
        cancel_reason_code="user_requested",
    )
    run_id = _run(engine, organization_id, connector_id, scope_id, job_id, 1, status="failed")
    _execute(engine, "DELETE FROM users WHERE id=:id", id=requester)
    with engine.connect() as connection:
        attribution = connection.execute(
            text("""SELECT requested_by_user_id, cancel_requested_by_user_id
                     FROM connector_sync_jobs WHERE id=:id"""),
            {"id": job_id},
        ).one()
    assert attribution == (None, None)

    audit_id = uuid.uuid4()
    _execute(
        engine,
        """INSERT INTO audit_events
           (id, organization_id, occurred_at, actor_type, actor_reference, action,
            resource_type, resource_id, outcome)
           VALUES (:id, :org, :now, 'system', 'connector-worker', 'connector.sync.requested',
                   'connector_sync_job', :job, 'success')""",
        id=audit_id,
        org=organization_id,
        now=NOW,
        job=job_id,
    )
    with pytest.raises(IntegrityError):
        _execute(engine, "DELETE FROM organizations WHERE id=:id", id=organization_id)
    _execute(engine, "DELETE FROM audit_events WHERE id=:id", id=audit_id)
    _execute(engine, "DELETE FROM connector_scopes WHERE id=:id", id=scope_id)
    assert _count(engine, "connector_sync_jobs") == 0
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT count(*) FROM connector_sync_runs WHERE id=:id"), {"id": run_id}
        ).scalar_one() == 0


def test_downgrade_removes_only_execution_control_and_reupgrade_succeeds(engine):
    command.downgrade(_config(), PRIOR_REVISION)
    inspector = inspect(engine)
    tables = set(inspector.get_table_names(schema="public"))
    assert "connector_sync_jobs" not in tables
    assert {
        "connector_sync_runs", "connector_sync_items", "connector_sync_errors",
        "connector_sync_cursors", "document_versions", "external_principals", "audit_events",
    }.issubset(tables)
    assert "sync_job_id" not in {
        column["name"] for column in inspector.get_columns("connector_sync_runs", schema="public")
    }
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT 1 FROM pg_extension WHERE extname='vector'")
        ).scalar_one() == 1
    command.upgrade(_config(), "head")
    inspector = inspect(engine)
    assert "connector_sync_jobs" in inspector.get_table_names(schema="public")
    assert {"sync_job_id", "job_attempt_number"}.issubset(
        {column["name"] for column in inspector.get_columns("connector_sync_runs", schema="public")}
    )