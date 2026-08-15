from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timezone
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
PRIOR_REVISION = "20260816_000007"


def _database_identity(database_url: str) -> tuple[str, str | None, int | None, str | None]:
    url = make_url(database_url)
    return url.drivername, url.host, url.port, url.database


def _required_url(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def _upgrade(database_url: str, revision: str = "head") -> None:
    environment = os.environ.copy()
    environment[DATABASE_URL_ENV_VAR] = database_url
    subprocess.run(
        [str(PROJECT_VENV_PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", revision],
        check=True,
        cwd=str(PROJECT_ROOT),
        env=environment,
    )


def _config() -> Config:
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", _required_url(TEST_DATABASE_URL_ENV_VAR))
    return config


@pytest.fixture(scope="module")
def engine():
    test_url = _required_url(TEST_DATABASE_URL_ENV_VAR)
    development_url = os.environ.get(DATABASE_URL_ENV_VAR)
    if development_url and _database_identity(development_url) == _database_identity(test_url):
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
            "audit_events",
            "knowledge_space_user_grants",
            "knowledge_space_team_grants",
            "knowledge_space_department_grants",
            "knowledge_space_organization_grants",
            "knowledge_spaces",
            "team_memberships",
            "department_memberships",
            "teams",
            "departments",
            "document_chunks",
            "documents",
            "authentication_sessions",
            "user_roles",
            "users",
            "organization_settings",
            "organizations",
            "industries",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


def _execute(engine, statement: str, **params):
    with engine.begin() as connection:
        return connection.execute(text(statement), params)


def _organization(engine, name: str) -> uuid.UUID:
    organization_id = uuid.uuid4()
    _execute(
        engine,
        "INSERT INTO organizations (id, name, slug) VALUES (:id, :name, :slug)",
        id=organization_id,
        name=name,
        slug=f"{name.lower()}-{organization_id}",
    )
    return organization_id


def _user(engine, organization_id: uuid.UUID, email: str) -> uuid.UUID:
    user_id = uuid.uuid4()
    _execute(
        engine,
        """
        INSERT INTO users (id, organization_id, email, normalized_email, password_hash, display_name)
        VALUES (:id, :organization_id, :email, :normalized_email, 'argon2id$test', :display_name)
        """,
        id=user_id,
        organization_id=organization_id,
        email=email,
        normalized_email=email.lower(),
        display_name=email.split("@")[0],
    )
    return user_id


def _event(
    engine,
    organization_id: uuid.UUID,
    *,
    actor_type: str = "system",
    actor_user_id: uuid.UUID | None = None,
    actor_reference: str | None = "migration-test",
    action: str = "knowledge_space.grant.created",
    resource_type: str = "knowledge_space_grant",
    resource_id: uuid.UUID | None = None,
    outcome: str = "success",
    reason: str | None = None,
    request_id: str | None = None,
    correlation_id: uuid.UUID | None = None,
    change_summary: object | None = None,
    context: object | None = None,
    schema_version: int = 1,
) -> uuid.UUID:
    event_id = uuid.uuid4()
    _execute(
        engine,
        """
        INSERT INTO audit_events (
            id, organization_id, occurred_at, actor_type, actor_user_id, actor_reference,
            action, resource_type, resource_id, outcome, reason, request_id, correlation_id,
            change_summary, context, schema_version
        )
        VALUES (
            :id, :organization_id, :occurred_at, :actor_type, :actor_user_id, :actor_reference,
            :action, :resource_type, :resource_id, :outcome, :reason, :request_id, :correlation_id,
            CAST(:change_summary AS jsonb), CAST(:context AS jsonb), :schema_version
        )
        """,
        id=event_id,
        organization_id=organization_id,
        occurred_at=datetime(2026, 8, 17, tzinfo=timezone.utc),
        actor_type=actor_type,
        actor_user_id=actor_user_id,
        actor_reference=actor_reference,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id or uuid.uuid4(),
        outcome=outcome,
        reason=reason,
        request_id=request_id,
        correlation_id=correlation_id,
        change_summary='{}' if change_summary is None else change_summary,
        context='{}' if context is None else context,
        schema_version=schema_version,
    )
    return event_id


def _count(engine, table: str) -> int:
    with engine.connect() as connection:
        return connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()


def test_upgrade_creates_audit_event_schema_and_matches_metadata(engine):
    inspector = inspect(engine)
    assert "audit_events" in inspector.get_table_names(schema="public")
    assert {"organizations", "users", "departments", "teams", "knowledge_spaces", "documents", "document_chunks"}.issubset(
        set(inspector.get_table_names(schema="public"))
    )
    assert engine.connect().execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")).scalar_one() == 1

    expected_columns = {
        "id", "organization_id", "occurred_at", "actor_type", "actor_user_id", "actor_reference",
        "action", "resource_type", "resource_id", "outcome", "reason", "request_id", "correlation_id",
        "change_summary", "context", "schema_version",
    }
    reflected_columns = inspector.get_columns("audit_events", schema="public")
    assert {column["name"] for column in reflected_columns} == expected_columns
    assert "updated_at" not in expected_columns
    assert "deleted_at" not in expected_columns
    assert list(Base.metadata.tables["audit_events"].columns.keys()) == [column["name"] for column in reflected_columns]
    columns_by_name = {column["name"]: column for column in reflected_columns}
    assert str(columns_by_name["id"]["type"]).upper() == "UUID"
    assert columns_by_name["occurred_at"]["type"].timezone is True
    assert str(columns_by_name["change_summary"]["type"]).upper() == "JSONB"
    assert str(columns_by_name["context"]["type"]).upper() == "JSONB"
    assert str(columns_by_name["schema_version"]["type"]).upper() == "SMALLINT"
    assert all(columns_by_name[name]["default"] is not None for name in ("occurred_at", "change_summary", "context", "schema_version"))
    assert inspector.get_pk_constraint("audit_events", schema="public")["name"] == "pk_audit_events"
    assert all(not column["nullable"] for column in reflected_columns if column["name"] in {"id", "organization_id", "occurred_at", "actor_type", "action", "resource_type", "resource_id", "outcome", "change_summary", "context", "schema_version"})
    assert {item["name"] for item in inspector.get_foreign_keys("audit_events", schema="public")} == {
        "fk_audit_events_organization_id_organizations",
        "fk_audit_events_actor_user_tenant",
    }
    model_constraint_names = {constraint.name for constraint in Base.metadata.tables["audit_events"].constraints if constraint.name}
    database_constraint_names = {
        inspector.get_pk_constraint("audit_events", schema="public")["name"],
        *(item["name"] for item in inspector.get_check_constraints("audit_events", schema="public")),
        *(item["name"] for item in inspector.get_foreign_keys("audit_events", schema="public")),
    }
    assert model_constraint_names == database_constraint_names
    assert {
        "ix_audit_events_organization_id_occurred_at",
        "ix_audit_events_org_actor_occurred",
        "ix_audit_events_org_resource_occurred",
        "ix_audit_events_org_action_occurred",
        "ix_audit_events_org_correlation",
    }.issubset({item["name"] for item in inspector.get_indexes("audit_events", schema="public")})


def test_valid_actor_rows_and_defaults(engine):
    organization_id = _organization(engine, "Alpha")
    user_id = _user(engine, organization_id, "user@example.com")
    _event(engine, organization_id, actor_type="user", actor_user_id=user_id, actor_reference="User One")
    _event(engine, organization_id, actor_type="system", actor_reference="scheduler")
    _event(engine, organization_id, actor_type="service", actor_reference="connector-sync")
    default_event_id = uuid.uuid4()
    _execute(
        engine,
        """
        INSERT INTO audit_events (
            id, organization_id, actor_type, actor_reference, action, resource_type, resource_id, outcome
        )
        VALUES (:id, :organization_id, 'system', 'migration-test', 'audit.event.created', 'audit_event', :resource_id, 'success')
        """,
        id=default_event_id,
        organization_id=organization_id,
        resource_id=uuid.uuid4(),
    )
    with engine.connect() as connection:
        row = connection.execute(text("SELECT change_summary, context, schema_version FROM audit_events WHERE id = :id"), {"id": default_event_id}).one()
    assert row.change_summary == {}
    assert row.context == {}
    assert row.schema_version == 1


def test_actor_validation_and_tenant_isolation(engine):
    org_a, org_b = _organization(engine, "Beta"), _organization(engine, "Gamma")
    user_a = _user(engine, org_a, "a@example.com")
    user_b = _user(engine, org_b, "b@example.com")
    with pytest.raises(IntegrityError):
        _event(engine, org_a, actor_type="user", actor_user_id=None)
    for actor_type in ("system", "service"):
        with pytest.raises(IntegrityError):
            _event(engine, org_a, actor_type=actor_type, actor_user_id=user_a, actor_reference=actor_type)
        with pytest.raises(IntegrityError):
            _event(engine, org_a, actor_type=actor_type, actor_reference="   ")
    with pytest.raises(IntegrityError):
        _event(engine, org_a, actor_type="user", actor_user_id=user_b)
    with pytest.raises(IntegrityError):
        _event(engine, org_a, actor_type="human", actor_reference="unknown")


def test_field_and_json_validation(engine):
    organization_id = _organization(engine, "Delta")
    with pytest.raises(IntegrityError):
        _event(engine, organization_id, action="  ")
    with pytest.raises(IntegrityError):
        _event(engine, organization_id, resource_type="  ")
    with pytest.raises(IntegrityError):
        _event(engine, organization_id, outcome="pending")
    with pytest.raises(IntegrityError):
        _event(engine, organization_id, reason="  ")
    with pytest.raises(IntegrityError):
        _event(engine, organization_id, request_id="  ")
    with pytest.raises(IntegrityError):
        _event(engine, organization_id, actor_reference="  ")
    for schema_version in (0, -1):
        with pytest.raises(IntegrityError):
            _event(engine, organization_id, schema_version=schema_version)
    for value in ("[]", '"value"', "null"):
        with pytest.raises(IntegrityError):
            _event(engine, organization_id, change_summary=value)
        with pytest.raises(IntegrityError):
            _event(engine, organization_id, context=value)
    _event(engine, organization_id, change_summary='{"changed_fields": ["outcome"]}', context='{"route": "/api/v1/audit-events"}')


def test_request_and_correlation_identifiers_are_not_unique(engine):
    organization_id = _organization(engine, "Epsilon")
    correlation_id = uuid.uuid4()
    _event(engine, organization_id, request_id="request-1", correlation_id=correlation_id)
    _event(engine, organization_id, request_id="request-1", correlation_id=correlation_id)
    assert _count(engine, "audit_events") == 2


def test_retention_restricts_actor_and_organization_deletion(engine):
    organization_id = _organization(engine, "Zeta")
    actor_id = _user(engine, organization_id, "actor@example.com")
    unrelated_id = _user(engine, organization_id, "unrelated@example.com")
    event_id = _event(engine, organization_id, actor_type="user", actor_user_id=actor_id)
    _execute(engine, "DELETE FROM users WHERE id = :id", id=unrelated_id)
    assert _count(engine, "audit_events") == 1
    with pytest.raises(IntegrityError):
        _execute(engine, "DELETE FROM users WHERE id = :id", id=actor_id)
    with pytest.raises(IntegrityError):
        _execute(engine, "DELETE FROM organizations WHERE id = :id", id=organization_id)
    _execute(engine, "DELETE FROM audit_events WHERE id = :id", id=event_id)
    _execute(engine, "DELETE FROM users WHERE id = :id", id=actor_id)
    _execute(engine, "DELETE FROM organizations WHERE id = :id", id=organization_id)
    assert _count(engine, "organizations") == 0


def test_historical_target_has_no_foreign_key(engine):
    organization_id = _organization(engine, "Eta")
    department_id = uuid.uuid4()
    _execute(
        engine,
        "INSERT INTO departments (id, organization_id, name, slug) VALUES (:id, :organization_id, 'Engineering', 'engineering')",
        id=department_id,
        organization_id=organization_id,
    )
    event_id = _event(engine, organization_id, resource_type="department", resource_id=department_id)
    _execute(engine, "DELETE FROM departments WHERE id = :id", id=department_id)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT resource_id FROM audit_events WHERE id = :id"), {"id": event_id}).scalar_one() == department_id


def test_downgrade_removes_only_audit_events_and_reupgrade_succeeds(engine):
    command.downgrade(_config(), PRIOR_REVISION)
    tables = set(inspect(engine).get_table_names(schema="public"))
    assert "audit_events" not in tables
    assert {"organizations", "users", "departments", "teams", "knowledge_spaces", "documents", "document_chunks"}.issubset(tables)
    command.upgrade(_config(), "head")
    assert "audit_events" in inspect(engine).get_table_names(schema="public")
