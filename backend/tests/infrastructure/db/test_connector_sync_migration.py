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
PRIOR_REVISION = "20260819_000010"
NOW = datetime(2026, 8, 20, tzinfo=timezone.utc)


def _identity(url: str):
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
    subprocess.run([str(PROJECT_VENV_PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", "head"], check=True, cwd=str(PROJECT_ROOT), env=environment)


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
            "connector_sync_cursors", "connector_sync_errors", "connector_sync_items", "connector_sync_runs",
            "source_item_scope_memberships", "source_items", "connector_scopes", "connectors", "audit_events",
            "knowledge_space_user_grants", "knowledge_space_team_grants", "knowledge_space_department_grants",
            "knowledge_space_organization_grants", "knowledge_spaces", "team_memberships", "department_memberships",
            "teams", "departments", "document_chunks", "documents", "authentication_sessions", "user_roles",
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
    _execute(engine, "INSERT INTO organizations (id, name, slug) VALUES (:id, :name, :slug)", id=value, name=name, slug=f"{name.lower()}-{value}")
    return value


def _connector(engine, organization_id: uuid.UUID, slug: str) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(engine, "INSERT INTO connectors (id, organization_id, connector_type, display_name, slug) VALUES (:id, :organization_id, 'local_folder', :name, :slug)", id=value, organization_id=organization_id, name=slug, slug=slug)
    return value


def _scope(engine, organization_id, connector_id, slug):
    space_id, scope_id = uuid.uuid4(), uuid.uuid4()
    _execute(engine, "INSERT INTO knowledge_spaces (id, organization_id, name, slug) VALUES (:id, :org, :name, :slug)", id=space_id, org=organization_id, name=slug, slug=f"space-{slug}")
    _execute(engine, "INSERT INTO connector_scopes (id, organization_id, connector_id, knowledge_space_id, display_name, slug, scope_type, external_scope_key, access_mode) VALUES (:id, :org, :connector, :space, :name, :slug, 'folder', :key, 'platform_managed')", id=scope_id, org=organization_id, connector=connector_id, space=space_id, name=slug, slug=slug, key=f"/{slug}")
    return scope_id


def _source_item(engine, organization_id, connector_id, key="item"):
    value = uuid.uuid4()
    _execute(engine, "INSERT INTO source_items (id, organization_id, connector_id, source_item_key, source_item_type, title, first_seen_at, last_seen_at) VALUES (:id, :org, :connector, :key, 'file', :key, :now, :now)", id=value, org=organization_id, connector=connector_id, key=key, now=NOW)
    return value


def _run(engine, organization_id, connector_id, scope_id, *, status="queued", started=None, finished=None, cancel=None, parent=None, metadata="{}", counter=0):
    value = uuid.uuid4()
    _execute(engine, """
        INSERT INTO connector_sync_runs (
            id, organization_id, connector_id, connector_scope_id, parent_run_id, mode, trigger_type,
            status, started_at, cancel_requested_at, finished_at, run_metadata, items_failed
        ) VALUES (
            :id, :org, :connector, :scope, :parent, 'incremental', 'manual', :status,
            :started, :cancel, :finished, CAST(:metadata AS jsonb), :counter
        )
    """, id=value, org=organization_id, connector=connector_id, scope=scope_id, parent=parent, status=status, started=started, cancel=cancel, finished=finished, metadata=metadata, counter=counter)
    return value


def _item(engine, organization_id, connector_id, scope_id, run_id, key="item", *, source_item_id=None, status="pending", started=None, finished=None, attempt=0):
    value = uuid.uuid4()
    _execute(engine, """
        INSERT INTO connector_sync_items (
            id, organization_id, connector_id, connector_scope_id, sync_run_id, source_item_id,
            source_item_key, change_type, processing_status, attempt_count, started_at, finished_at
        ) VALUES (:id, :org, :connector, :scope, :run, :source, :key, 'unknown', :status, :attempt, :started, :finished)
    """, id=value, org=organization_id, connector=connector_id, scope=scope_id, run=run_id, source=source_item_id, key=key, status=status, attempt=attempt, started=started, finished=finished)
    return value


def _error(engine, organization_id, connector_id, scope_id, run_id, *, item_id=None, code="source_read_failed", details="{}", occurred=NOW, retry_after=None, resolved=None):
    value = uuid.uuid4()
    _execute(engine, """
        INSERT INTO connector_sync_errors (
            id, organization_id, connector_id, connector_scope_id, sync_run_id, sync_item_id,
            error_category, error_code, message, retryable, attempt_number, details,
            retry_after_at, resolved_at, occurred_at
        ) VALUES (:id, :org, :connector, :scope, :run, :item, 'source_read', :code,
            'Safe provider read summary', true, 1, CAST(:details AS jsonb), :retry_after, :resolved, :occurred)
    """, id=value, org=organization_id, connector=connector_id, scope=scope_id, run=run_id, item=item_id, code=code, details=details, retry_after=retry_after, resolved=resolved, occurred=occurred)
    return value


def _cursor(engine, organization_id, connector_id, scope_id, run_id, version, *, state="active", safe='{"page": 1}', secret=None, activated=NOW, retired=None, cursor_type="page_token"):
    value = uuid.uuid4()
    _execute(engine, """
        INSERT INTO connector_sync_cursors (
            id, organization_id, connector_id, connector_scope_id, created_by_run_id,
            cursor_version, cursor_type, state, safe_cursor, secret_reference, activated_at, retired_at
        ) VALUES (:id, :org, :connector, :scope, :run, :version, :type, :state,
            CAST(:safe AS jsonb), :secret, :activated, :retired)
    """, id=value, org=organization_id, connector=connector_id, scope=scope_id, run=run_id, version=version, type=cursor_type, state=state, safe=safe, secret=secret, activated=activated, retired=retired)
    return value


def _setup(engine, name="Alpha"):
    org = _organization(engine, name)
    connector = _connector(engine, org, f"connector-{name.lower()}")
    scope = _scope(engine, org, connector, f"scope-{name.lower()}")
    return org, connector, scope


def test_schema_matches_orm_constraints_and_indexes(engine):
    inspector = inspect(engine)
    tables = {"connector_sync_runs", "connector_sync_items", "connector_sync_errors", "connector_sync_cursors"}
    assert tables.issubset(inspector.get_table_names(schema="public"))
    assert {"connectors", "connector_scopes", "source_items", "audit_events", "documents", "document_chunks"}.issubset(inspector.get_table_names(schema="public"))
    assert engine.connect().execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")).scalar_one() == 1
    for table in tables:
        reflected = inspector.get_columns(table, schema="public")
        model_columns = list(Base.metadata.tables[table].columns)
        assert [column.name for column in model_columns] == [column["name"] for column in reflected]
        for model_column, database_column in zip(model_columns, reflected, strict=True):
            assert model_column.type._type_affinity is database_column["type"]._type_affinity
            if hasattr(model_column.type, "length"):
                assert model_column.type.length == database_column["type"].length
            if hasattr(model_column.type, "timezone"):
                assert model_column.type.timezone == database_column["type"].timezone
            assert model_column.nullable == database_column["nullable"]
            assert (model_column.server_default is None) == (database_column["default"] is None)
        assert inspector.get_pk_constraint(table, schema="public")["name"] == f"pk_{table}"
        model_names = {constraint.name for constraint in Base.metadata.tables[table].constraints if constraint.name}
        database_names = {
            inspector.get_pk_constraint(table, schema="public")["name"],
            *(item["name"] for item in inspector.get_unique_constraints(table, schema="public")),
            *(item["name"] for item in inspector.get_check_constraints(table, schema="public")),
            *(item["name"] for item in inspector.get_foreign_keys(table, schema="public")),
        }
        assert model_names == database_names
        assert {index.name for index in Base.metadata.tables[table].indexes}.issubset(
            {item["name"] for item in inspector.get_indexes(table, schema="public")}
        )


def test_run_states_counters_parent_and_active_uniqueness(engine):
    org, connector, scope = _setup(engine)
    queued_one = _run(engine, org, connector, scope)
    assert _run(engine, org, connector, scope)
    running = _run(engine, org, connector, scope, status="running", started=NOW)
    with pytest.raises(IntegrityError):
        _run(engine, org, connector, scope, status="cancelling", started=NOW, cancel=NOW)
    for kwargs in (
        {"status": "queued", "started": NOW},
        {"status": "running"},
        {"status": "cancelling", "started": NOW},
        {"status": "completed", "started": NOW},
        {"status": "failed", "started": NOW, "finished": NOW - timedelta(seconds=1)},
        {"counter": -1},
        {"metadata": "[]"},
    ):
        with pytest.raises(IntegrityError):
            _run(engine, org, connector, scope, **kwargs)
    with pytest.raises(IntegrityError):
        _execute(engine, "UPDATE connector_sync_runs SET parent_run_id = id WHERE id = :id", id=queued_one)
    other_org, other_connector, other_scope = _setup(engine, "Beta")
    foreign_parent = _run(engine, other_org, other_connector, other_scope)
    with pytest.raises(IntegrityError):
        _run(engine, org, connector, scope, parent=foreign_parent)
    child = _run(engine, org, connector, scope, parent=queued_one)
    _execute(engine, "DELETE FROM connector_sync_runs WHERE id = :id", id=queued_one)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT parent_run_id FROM connector_sync_runs WHERE id = :id"), {"id": child}).scalar_one() is None
    _execute(engine, "DELETE FROM connector_sync_runs WHERE id = :id", id=running)


def test_items_enforce_run_source_identity_and_state(engine):
    org, connector, scope = _setup(engine)
    run = _run(engine, org, connector, scope)
    source = _source_item(engine, org, connector)
    item = _item(engine, org, connector, scope, run, source_item_id=source)
    with pytest.raises(IntegrityError):
        _item(engine, org, connector, scope, run)
    for kwargs in (
        {"key": "other", "status": "processing"},
        {"key": "other", "status": "succeeded", "started": NOW},
        {"key": "other", "status": "succeeded", "started": NOW, "finished": NOW - timedelta(seconds=1)},
        {"key": "other", "attempt": -1},
    ):
        with pytest.raises(IntegrityError):
            _item(engine, org, connector, scope, run, **kwargs)
    other_org, other_connector, other_scope = _setup(engine, "Gamma")
    other_source = _source_item(engine, other_org, other_connector)
    with pytest.raises(IntegrityError):
        _item(engine, org, connector, scope, run, key="foreign", source_item_id=other_source)
    with pytest.raises(IntegrityError):
        _item(engine, org, other_connector, other_scope, run, key="wrong-run")
    _execute(engine, "DELETE FROM source_items WHERE id = :id", id=source)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT source_item_id FROM connector_sync_items WHERE id = :id"), {"id": item}).scalar_one() is None


def test_errors_are_safe_repeatable_and_survive_item_deletion(engine):
    org, connector, scope = _setup(engine)
    run = _run(engine, org, connector, scope)
    item = _item(engine, org, connector, scope, run)
    first = _error(engine, org, connector, scope, run, item_id=item)
    _error(engine, org, connector, scope, run, item_id=item)
    assert _count(engine, "connector_sync_errors") == 2
    other_run = _run(engine, org, connector, scope)
    other_item = _item(engine, org, connector, scope, other_run, key="other")
    with pytest.raises(IntegrityError):
        _error(engine, org, connector, scope, run, item_id=other_item)
    for kwargs in ({"code": "Bad-Code"}, {"details": "[]"}, {"retry_after": NOW - timedelta(seconds=1)}, {"resolved": NOW - timedelta(seconds=1)}):
        with pytest.raises(IntegrityError):
            _error(engine, org, connector, scope, run, **kwargs)
    _execute(engine, "DELETE FROM connector_sync_items WHERE id = :id", id=item)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT sync_item_id FROM connector_sync_errors WHERE id = :id"), {"id": first}).scalar_one() is None
    assert _count(engine, "connector_sync_errors") == 2


def test_cursor_storage_version_state_scope_and_retention(engine):
    org, connector, scope = _setup(engine)
    run = _run(engine, org, connector, scope)
    _cursor(engine, org, connector, scope, run, 1)
    with pytest.raises(IntegrityError):
        _cursor(engine, org, connector, scope, run, 2)
    with pytest.raises(IntegrityError):
        _cursor(engine, org, connector, scope, run, 1, state="superseded", retired=NOW)
    for kwargs in (
        {"version": 2, "safe": None, "secret": None},
        {"version": 2, "safe": "{}", "secret": "vault://cursor"},
        {"version": 0, "state": "superseded", "retired": NOW},
        {"version": 2, "safe": "[]", "state": "superseded", "retired": NOW},
        {"version": 2, "safe": None, "secret": "   ", "state": "superseded", "retired": NOW},
        {"version": 2, "state": "superseded"},
        {"version": 2, "state": "invalid", "retired": NOW - timedelta(seconds=1)},
    ):
        with pytest.raises(IntegrityError):
            _cursor(engine, org, connector, scope, run, **kwargs)
    _cursor(engine, org, connector, scope, run, 2, state="superseded", safe=None, secret="vault://cursor", retired=NOW)
    other_org, other_connector, other_scope = _setup(engine, "Delta")
    with pytest.raises(IntegrityError):
        _cursor(engine, other_org, other_connector, other_scope, run, 1)
    with pytest.raises(IntegrityError):
        _execute(engine, "DELETE FROM connector_sync_runs WHERE id = :id", id=run)


def test_operational_cascades_and_cursor_retention(engine):
    org, connector, scope = _setup(engine)
    run = _run(engine, org, connector, scope)
    item = _item(engine, org, connector, scope, run)
    _error(engine, org, connector, scope, run, item_id=item)
    _execute(engine, "DELETE FROM connector_scopes WHERE id = :id", id=scope)
    assert all(_count(engine, table) == 0 for table in ("connector_sync_runs", "connector_sync_items", "connector_sync_errors"))

    scope = _scope(engine, org, connector, "second")
    run = _run(engine, org, connector, scope)
    _cursor(engine, org, connector, scope, run, 1)
    with pytest.raises(IntegrityError):
        _execute(engine, "DELETE FROM connectors WHERE id = :id", id=connector)
    with pytest.raises(IntegrityError):
        _execute(engine, "DELETE FROM organizations WHERE id = :id", id=org)
    _execute(engine, "DELETE FROM connector_sync_cursors")
    _execute(engine, "DELETE FROM organizations WHERE id = :id", id=org)
    assert _count(engine, "connector_sync_runs") == 0


def test_downgrade_removes_only_sync_tables_and_reupgrade_succeeds(engine):
    command.downgrade(_config(), PRIOR_REVISION)
    tables = set(inspect(engine).get_table_names(schema="public"))
    assert not {"connector_sync_runs", "connector_sync_items", "connector_sync_errors", "connector_sync_cursors"}.intersection(tables)
    assert {"connectors", "connector_scopes", "source_items", "audit_events", "documents", "document_chunks"}.issubset(tables)
    command.upgrade(_config(), "head")
    assert {"connector_sync_runs", "connector_sync_items", "connector_sync_errors", "connector_sync_cursors"}.issubset(inspect(engine).get_table_names(schema="public"))
