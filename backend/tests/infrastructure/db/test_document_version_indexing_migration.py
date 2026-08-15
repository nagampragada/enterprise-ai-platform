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
PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
TEST_DATABASE_URL_ENV_VAR = "TEST_DATABASE_URL"
DATABASE_URL_ENV_VAR = "DATABASE_URL"
PRIOR_REVISION = "20260820_000011"
NOW = datetime(2026, 8, 21, tzinfo=timezone.utc)
NEW_TABLES = {"document_versions", "document_version_documents", "document_indexing_states", "document_indexing_attempts"}


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
    subprocess.run([str(PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", "head"], check=True, cwd=str(PROJECT_ROOT), env=environment)


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
            "document_indexing_attempts", "document_indexing_states", "document_version_documents", "document_versions",
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


def _setup(engine, name="Alpha"):
    org, connector, space, scope, source = (uuid.uuid4() for _ in range(5))
    _execute(engine, "INSERT INTO organizations (id, name, slug) VALUES (:id, :name, :slug)", id=org, name=name, slug=f"{name.lower()}-{org}")
    _execute(engine, "INSERT INTO connectors (id, organization_id, connector_type, display_name, slug) VALUES (:id, :org, 'local_folder', :name, :slug)", id=connector, org=org, name=name, slug=f"connector-{str(connector)[:8]}")
    _execute(engine, "INSERT INTO knowledge_spaces (id, organization_id, name, slug) VALUES (:id, :org, :name, :slug)", id=space, org=org, name=name, slug=f"space-{str(space)[:8]}")
    _execute(engine, "INSERT INTO connector_scopes (id, organization_id, connector_id, knowledge_space_id, display_name, slug, scope_type, external_scope_key, access_mode) VALUES (:id, :org, :connector, :space, :name, :slug, 'folder', :key, 'platform_managed')", id=scope, org=org, connector=connector, space=space, name=name, slug=f"scope-{str(scope)[:8]}", key=f"/{scope}")
    _execute(engine, "INSERT INTO source_items (id, organization_id, connector_id, source_item_key, source_item_type, title, first_seen_at, last_seen_at) VALUES (:id, :org, :connector, :key, 'file', :key, :now, :now)", id=source, org=org, connector=connector, key=f"file-{source}.txt", now=NOW)
    return org, connector, scope, source


def _version(engine, org, connector, source, number, *, current=False, checksum="abc", algorithm="sha256", cause="discovered", lifecycle="available", size=10, metadata="{}"):
    value = uuid.uuid4()
    _execute(engine, """
        INSERT INTO document_versions (
            id, organization_id, connector_id, source_item_id, version_number,
            content_checksum, checksum_algorithm, source_size_bytes, version_cause,
            lifecycle, is_current, discovered_at, metadata
        ) VALUES (
            :id, :org, :connector, :source, :number, :checksum, :algorithm, :size,
            :cause, :lifecycle, :current, :now, CAST(:metadata AS jsonb)
        )
    """, id=value, org=org, connector=connector, source=source, number=number, checksum=checksum, algorithm=algorithm, size=size, cause=cause, lifecycle=lifecycle, current=current, now=NOW, metadata=metadata)
    return value


def _document(engine, org, key):
    value = uuid.uuid4()
    _execute(engine, "INSERT INTO documents (id, organization_id, source_type, source_document_key, title) VALUES (:id, :org, 'manual_upload', :key, :key)", id=value, org=org, key=key)
    return value


def _state(engine, org, version, fingerprint="profile-a", *, status="pending", reason="new_version", desired=1, indexed=None, started=None, completed=None, retry=None, error_category=None, error_code=None, dimensions=1536):
    value = uuid.uuid4()
    _execute(engine, """
        INSERT INTO document_indexing_states (
            id, organization_id, document_version_id, extraction_profile, extraction_version,
            chunking_profile, chunking_version, embedding_provider, embedding_model,
            embedding_dimensions, profile_fingerprint, desired_generation, indexed_generation,
            status, reason, last_error_category, last_error_code, requested_at,
            started_at, completed_at, next_retry_at
        ) VALUES (
            :id, :org, :version, 'default_extractor', 'v1', 'deterministic_text', 'v1',
            'openai', 'text-embedding-3-small', :dimensions, :fingerprint, :desired,
            :indexed, :status, :reason, :error_category, :error_code, :now,
            :started, :completed, :retry
        )
    """, id=value, org=org, version=version, dimensions=dimensions, fingerprint=fingerprint, desired=desired, indexed=indexed, status=status, reason=reason, error_category=error_category, error_code=error_code, now=NOW, started=started, completed=completed, retry=retry)
    return value


def _sync(engine, org, connector, scope):
    run, item = uuid.uuid4(), uuid.uuid4()
    _execute(engine, "INSERT INTO connector_sync_runs (id, organization_id, connector_id, connector_scope_id, mode, trigger_type) VALUES (:id, :org, :connector, :scope, 'incremental', 'manual')", id=run, org=org, connector=connector, scope=scope)
    _execute(engine, "INSERT INTO connector_sync_items (id, organization_id, connector_id, connector_scope_id, sync_run_id, source_item_key, change_type) VALUES (:id, :org, :connector, :scope, :run, :key, 'changed')", id=item, org=org, connector=connector, scope=scope, run=run, key=f"item-{item}")
    return run, item


def _attempt(engine, org, state, number, *, run=None, item=None, status="running", completed=None, error_category=None, error_code=None, summary="{}"):
    value = uuid.uuid4()
    _execute(engine, """
        INSERT INTO document_indexing_attempts (
            id, organization_id, indexing_state_id, connector_sync_run_id,
            connector_sync_item_id, attempt_number, trigger_type, status, started_at,
            completed_at, error_category, error_code, summary
        ) VALUES (
            :id, :org, :state, :run, :item, :number, 'sync', :status, :now,
            :completed, :error_category, :error_code, CAST(:summary AS jsonb)
        )
    """, id=value, org=org, state=state, run=run, item=item, number=number, status=status, now=NOW, completed=completed, error_category=error_category, error_code=error_code, summary=summary)
    return value


def test_schema_matches_orm_and_prior_objects_remain(engine):
    inspector = inspect(engine)
    assert NEW_TABLES.issubset(inspector.get_table_names(schema="public"))
    assert {"organizations", "users", "documents", "document_chunks", "knowledge_spaces", "connectors", "source_items", "connector_sync_runs", "audit_events"}.issubset(inspector.get_table_names(schema="public"))
    assert engine.connect().execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")).scalar_one() == 1
    for table in NEW_TABLES:
        reflected = inspector.get_columns(table, schema="public")
        model_columns = list(Base.metadata.tables[table].columns)
        assert [column.name for column in model_columns] == [column["name"] for column in reflected]
        for model, database in zip(model_columns, reflected, strict=True):
            assert model.type._type_affinity is database["type"]._type_affinity
            assert model.nullable == database["nullable"]
            assert (model.server_default is None) == (database["default"] is None)
        model_names = {constraint.name for constraint in Base.metadata.tables[table].constraints if constraint.name}
        database_names = {
            inspector.get_pk_constraint(table, schema="public")["name"],
            *(item["name"] for item in inspector.get_unique_constraints(table, schema="public")),
            *(item["name"] for item in inspector.get_check_constraints(table, schema="public")),
            *(item["name"] for item in inspector.get_foreign_keys(table, schema="public")),
        }
        assert model_names == database_names
        assert {index.name for index in Base.metadata.tables[table].indexes}.issubset({item["name"] for item in inspector.get_indexes(table, schema="public")})


def test_version_identity_current_replacement_and_validation(engine):
    org, connector, _, source = _setup(engine)
    first = _version(engine, org, connector, source, 1, current=True)
    with pytest.raises(IntegrityError):
        _version(engine, org, connector, source, 1)
    with pytest.raises(IntegrityError):
        _version(engine, org, connector, source, 2, current=True)
    with engine.begin() as connection:
        connection.execute(text("UPDATE document_versions SET is_current = false WHERE id = :id"), {"id": first})
        second = uuid.uuid4()
        connection.execute(text("INSERT INTO document_versions (id, organization_id, connector_id, source_item_id, version_number, content_checksum, checksum_algorithm, source_size_bytes, version_cause, lifecycle, is_current, discovered_at) VALUES (:id, :org, :connector, :source, 2, 'def', 'sha256', 20, 'content_changed', 'available', true, :now)"), {"id": second, "org": org, "connector": connector, "source": source, "now": NOW})
    assert _count(engine, "document_versions") == 2
    other_source = uuid.uuid4()
    _execute(engine, "INSERT INTO source_items (id, organization_id, connector_id, source_item_key, source_item_type, title, first_seen_at, last_seen_at) VALUES (:id, :org, :connector, :key, 'file', :key, :now, :now)", id=other_source, org=org, connector=connector, key=f"other-{other_source}", now=NOW)
    assert _version(engine, org, connector, other_source, 1)
    for kwargs in (
        {"number": 0}, {"number": 3, "checksum": "abc", "algorithm": None},
        {"number": 3, "checksum": None, "algorithm": "sha256"}, {"number": 3, "size": -1},
        {"number": 3, "metadata": "[]"},
        {"number": 3, "cause": "tombstone", "lifecycle": "available", "checksum": None, "algorithm": None, "size": None},
        {"number": 3, "cause": "tombstone", "lifecycle": "deleted", "checksum": "abc", "algorithm": "sha256", "size": 1},
    ):
        number = kwargs.pop("number")
        with pytest.raises(IntegrityError):
            _version(engine, org, connector, source, number, **kwargs)
    assert _version(engine, org, connector, source, 3, cause="tombstone", lifecycle="deleted", checksum=None, algorithm=None, size=None)


def test_cross_tenant_version_and_document_materialization_mapping(engine):
    org_a, connector_a, _, source_a = _setup(engine, "Beta")
    org_b, connector_b, _, source_b = _setup(engine, "Gamma")
    version = _version(engine, org_a, connector_a, source_a, 1)
    with pytest.raises(IntegrityError):
        _version(engine, org_a, connector_b, source_b, 1)
    assert version and _count(engine, "document_version_documents") == 0
    document_a = _document(engine, org_a, "manual-a")
    document_b = _document(engine, org_b, "manual-b")
    _execute(engine, "INSERT INTO document_version_documents (id, organization_id, document_version_id, document_id) VALUES (:id, :org, :version, :document)", id=uuid.uuid4(), org=org_a, version=version, document=document_a)
    with pytest.raises(IntegrityError):
        _execute(engine, "INSERT INTO document_version_documents (id, organization_id, document_version_id, document_id) VALUES (:id, :org, :version, :document)", id=uuid.uuid4(), org=org_a, version=version, document=document_b)
    other_version = _version(engine, org_a, connector_a, source_a, 2)
    with pytest.raises(IntegrityError):
        _execute(engine, "INSERT INTO document_version_documents (id, organization_id, document_version_id, document_id) VALUES (:id, :org, :version, :document)", id=uuid.uuid4(), org=org_a, version=other_version, document=document_a)


def test_indexing_states_support_profiles_and_enforce_status_generation(engine):
    org, connector, _, source = _setup(engine, "Delta")
    version = _version(engine, org, connector, source, 1)
    _state(engine, org, version, "profile-a")
    with pytest.raises(IntegrityError):
        _state(engine, org, version, "profile-a")
    assert _state(engine, org, version, "profile-b", reason="profile_changed")
    invalid = (
        {"fingerprint": "Bad Profile"}, {"fingerprint": "profile-c", "dimensions": 0},
        {"fingerprint": "profile-c", "desired": 0}, {"fingerprint": "profile-c", "desired": 1, "indexed": 2},
        {"fingerprint": "profile-c", "status": "processing"},
        {"fingerprint": "profile-c", "status": "pending", "started": NOW},
        {"fingerprint": "profile-c", "status": "indexed", "desired": 2, "indexed": 1, "completed": NOW},
        {"fingerprint": "profile-c", "status": "indexed", "desired": 1, "indexed": 1},
        {"fingerprint": "profile-c", "status": "indexed", "desired": 1, "indexed": 1, "completed": NOW, "retry": NOW},
        {"fingerprint": "profile-c", "status": "failed", "completed": NOW},
        {"fingerprint": "profile-c", "status": "processing", "started": NOW, "retry": NOW},
    )
    for kwargs in invalid:
        with pytest.raises(IntegrityError):
            _state(engine, org, version, **kwargs)
    assert _state(engine, org, version, "profile-indexed", status="indexed", desired=2, indexed=2, started=NOW, completed=NOW)
    assert _state(engine, org, version, "profile-failed", status="failed", completed=NOW, retry=NOW + timedelta(hours=1), error_category="embedding", error_code="provider_timeout")


def test_attempt_history_validation_and_sync_attribution(engine):
    org, connector, scope, source = _setup(engine, "Epsilon")
    version = _version(engine, org, connector, source, 1)
    state = _state(engine, org, version)
    run, item = _sync(engine, org, connector, scope)
    attempt = _attempt(engine, org, state, 1, run=run, item=item)
    with pytest.raises(IntegrityError):
        _attempt(engine, org, state, 1)
    for kwargs in (
        {"number": 0}, {"number": 2, "status": "succeeded"},
        {"number": 2, "status": "running", "completed": NOW},
        {"number": 2, "status": "succeeded", "completed": NOW, "error_category": "embedding", "error_code": "bad"},
        {"number": 2, "status": "failed", "completed": NOW},
        {"number": 2, "summary": "[]"}, {"number": 2, "item": item},
    ):
        number = kwargs.pop("number")
        with pytest.raises(IntegrityError):
            _attempt(engine, org, state, number, **kwargs)
    assert _attempt(engine, org, state, 2, status="failed", completed=NOW, error_category="embedding", error_code="provider_timeout")
    other_run, other_item = _sync(engine, org, connector, scope)
    with pytest.raises(IntegrityError):
        _attempt(engine, org, state, 3, run=run, item=other_item)
    _execute(engine, "DELETE FROM connector_sync_items WHERE id = :id", id=item)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT connector_sync_item_id FROM document_indexing_attempts WHERE id = :id"), {"id": attempt}).scalar_one() is None
    _execute(engine, "DELETE FROM connector_sync_runs WHERE id = :id", id=run)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT connector_sync_run_id FROM document_indexing_attempts WHERE id = :id"), {"id": attempt}).scalar_one() is None


def test_source_item_purge_cascades_version_state_attempt_and_mapping(engine):
    org, connector, _, source = _setup(engine, "Zeta")
    version = _version(engine, org, connector, source, 1)
    document = _document(engine, org, "mapped")
    _execute(engine, "INSERT INTO document_version_documents (id, organization_id, document_version_id, document_id) VALUES (:id, :org, :version, :document)", id=uuid.uuid4(), org=org, version=version, document=document)
    state = _state(engine, org, version)
    _attempt(engine, org, state, 1)
    _execute(engine, "DELETE FROM source_items WHERE id = :id", id=source)
    assert all(_count(engine, table) == 0 for table in NEW_TABLES)
    assert _count(engine, "documents") == 1


def test_downgrade_removes_only_new_slice_and_reupgrade_succeeds(engine):
    command.downgrade(_config(), PRIOR_REVISION)
    inspector = inspect(engine)
    tables = set(inspector.get_table_names(schema="public"))
    assert not NEW_TABLES.intersection(tables)
    assert {"organizations", "documents", "document_chunks", "connectors", "source_items", "connector_sync_runs", "audit_events"}.issubset(tables)
    assert "uq_sync_runs_organization_id_id" not in {item["name"] for item in inspector.get_unique_constraints("connector_sync_runs", schema="public")}
    command.upgrade(_config(), "head")
    assert NEW_TABLES.issubset(inspect(engine).get_table_names(schema="public"))
