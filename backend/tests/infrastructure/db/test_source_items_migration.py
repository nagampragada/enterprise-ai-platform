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
PRIOR_REVISION = "20260818_000009"
SCOPE_CONNECTOR_KEY = "uq_connector_scopes_org_connector_id"


def _database_identity(database_url: str) -> tuple[str, str | None, int | None, str | None]:
    url = make_url(database_url)
    return url.drivername, url.host, url.port, url.database


def _required_url(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def _upgrade(database_url: str) -> None:
    environment = os.environ.copy()
    environment[DATABASE_URL_ENV_VAR] = database_url
    subprocess.run(
        [str(PROJECT_VENV_PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", "head"],
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
            "source_item_scope_memberships", "source_items", "connector_scopes", "connectors", "audit_events",
            "knowledge_space_user_grants", "knowledge_space_team_grants", "knowledge_space_department_grants",
            "knowledge_space_organization_grants", "knowledge_spaces", "team_memberships",
            "department_memberships", "teams", "departments", "document_chunks", "documents",
            "authentication_sessions", "user_roles", "users", "organization_settings", "organizations", "industries",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


def _execute(engine, statement: str, **params):
    with engine.begin() as connection:
        return connection.execute(text(statement), params)


def _organization(engine, name: str) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(engine, "INSERT INTO organizations (id, name, slug) VALUES (:id, :name, :slug)", id=value, name=name, slug=f"{name.lower()}-{value}")
    return value


def _connector(engine, organization_id: uuid.UUID, slug: str) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(
        engine,
        "INSERT INTO connectors (id, organization_id, connector_type, display_name, slug) VALUES (:id, :organization_id, 'local_folder', :name, :slug)",
        id=value, organization_id=organization_id, name=slug.title(), slug=slug,
    )
    return value


def _space(engine, organization_id: uuid.UUID, slug: str) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(engine, "INSERT INTO knowledge_spaces (id, organization_id, name, slug) VALUES (:id, :organization_id, :name, :slug)", id=value, organization_id=organization_id, name=slug.title(), slug=slug)
    return value


def _scope(engine, organization_id: uuid.UUID, connector_id: uuid.UUID, space_id: uuid.UUID, slug: str) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(
        engine,
        """
        INSERT INTO connector_scopes (
            id, organization_id, connector_id, knowledge_space_id, display_name, slug,
            scope_type, external_scope_key, access_mode
        ) VALUES (
            :id, :organization_id, :connector_id, :space_id, :name, :slug,
            'folder', :external_key, 'platform_managed'
        )
        """,
        id=value, organization_id=organization_id, connector_id=connector_id, space_id=space_id,
        name=slug.title(), slug=slug, external_key=f"/{slug}",
    )
    return value


def _item(
    engine,
    organization_id: uuid.UUID,
    connector_id: uuid.UUID,
    source_item_key: str,
    *,
    parent_source_item_key: str | None = None,
    source_item_type: str = "file",
    title: str = "Readme",
    source_url: str | None = None,
    mime_type: str | None = None,
    source_checksum: str | None = None,
    source_version: str | None = None,
    size_bytes: int | None = None,
    first_seen_at: datetime | None = None,
    last_seen_at: datetime | None = None,
    status: str = "active",
    deleted_at: datetime | None = None,
    metadata: str = "{}",
    metadata_schema_version: int = 1,
) -> uuid.UUID:
    value = uuid.uuid4()
    first_seen = first_seen_at or datetime(2026, 8, 19, tzinfo=timezone.utc)
    _execute(
        engine,
        """
        INSERT INTO source_items (
            id, organization_id, connector_id, source_item_key, parent_source_item_key,
            source_item_type, title, source_url, mime_type, source_checksum, source_version,
            size_bytes, first_seen_at, last_seen_at, status, deleted_at, metadata,
            metadata_schema_version
        ) VALUES (
            :id, :organization_id, :connector_id, :source_item_key, :parent_source_item_key,
            :source_item_type, :title, :source_url, :mime_type, :source_checksum, :source_version,
            :size_bytes, :first_seen_at, :last_seen_at, :status, :deleted_at,
            CAST(:metadata AS jsonb), :metadata_schema_version
        )
        """,
        id=value, organization_id=organization_id, connector_id=connector_id,
        source_item_key=source_item_key, parent_source_item_key=parent_source_item_key,
        source_item_type=source_item_type, title=title, source_url=source_url, mime_type=mime_type,
        source_checksum=source_checksum, source_version=source_version, size_bytes=size_bytes,
        first_seen_at=first_seen, last_seen_at=last_seen_at or first_seen, status=status,
        deleted_at=deleted_at, metadata=metadata, metadata_schema_version=metadata_schema_version,
    )
    return value


def _membership(
    engine,
    organization_id: uuid.UUID,
    connector_id: uuid.UUID,
    source_item_id: uuid.UUID,
    scope_id: uuid.UUID,
    *,
    status: str = "active",
    first_discovered_at: datetime | None = None,
    last_seen_at: datetime | None = None,
    removed_at: datetime | None = None,
) -> uuid.UUID:
    value = uuid.uuid4()
    first_seen = first_discovered_at or datetime(2026, 8, 19, tzinfo=timezone.utc)
    _execute(
        engine,
        """
        INSERT INTO source_item_scope_memberships (
            id, organization_id, connector_id, source_item_id, connector_scope_id,
            status, first_discovered_at, last_seen_at, removed_at
        ) VALUES (
            :id, :organization_id, :connector_id, :source_item_id, :scope_id,
            :status, :first_seen, :last_seen, :removed_at
        )
        """,
        id=value, organization_id=organization_id, connector_id=connector_id,
        source_item_id=source_item_id, scope_id=scope_id, status=status,
        first_seen=first_seen, last_seen=last_seen_at or first_seen, removed_at=removed_at,
    )
    return value


def _count(engine, table: str) -> int:
    with engine.connect() as connection:
        return connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()


def test_schema_matches_models_candidate_keys_and_indexes(engine):
    inspector = inspect(engine)
    assert {"source_items", "source_item_scope_memberships"}.issubset(inspector.get_table_names(schema="public"))
    assert {"connectors", "connector_scopes", "knowledge_spaces", "audit_events", "documents", "document_chunks"}.issubset(inspector.get_table_names(schema="public"))
    assert engine.connect().execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")).scalar_one() == 1
    assert SCOPE_CONNECTOR_KEY in {item["name"] for item in inspector.get_unique_constraints("connector_scopes", schema="public")}

    expected_columns = {
        "source_items": ["id", "organization_id", "connector_id", "source_item_key", "parent_source_item_key", "source_item_type", "title", "source_url", "mime_type", "source_checksum", "source_version", "size_bytes", "source_created_at", "source_modified_at", "first_seen_at", "last_seen_at", "status", "deleted_at", "metadata", "metadata_schema_version", "created_at", "updated_at"],
        "source_item_scope_memberships": ["id", "organization_id", "connector_id", "source_item_id", "connector_scope_id", "status", "first_discovered_at", "last_seen_at", "removed_at", "created_at", "updated_at"],
    }
    expected_indexes = {
        "source_items": {"ix_source_items_org_connector_status", "ix_source_items_org_connector_type", "ix_source_items_org_connector_seen"},
        "source_item_scope_memberships": {"ix_source_scope_memberships_org_scope_status", "ix_source_scope_memberships_org_item_status"},
    }
    nullable_columns = {
        "source_items": {"parent_source_item_key", "source_url", "mime_type", "source_checksum", "source_version", "size_bytes", "source_created_at", "source_modified_at", "deleted_at"},
        "source_item_scope_memberships": {"removed_at"},
    }
    defaulted_columns = {
        "source_items": {"status", "metadata", "metadata_schema_version", "created_at", "updated_at"},
        "source_item_scope_memberships": {"status", "created_at", "updated_at"},
    }
    for table, columns in expected_columns.items():
        reflected = inspector.get_columns(table, schema="public")
        assert [column["name"] for column in reflected] == columns
        assert list(Base.metadata.tables[table].columns.keys()) == columns
        assert inspector.get_pk_constraint(table, schema="public")["name"] == f"pk_{table}"
        assert {column["name"] for column in reflected if column["nullable"]} == nullable_columns[table]
        assert defaulted_columns[table].issubset({column["name"] for column in reflected if column["default"] is not None})
        assert all(column["type"].timezone is True for column in reflected if column["name"] in {"source_created_at", "source_modified_at", "first_seen_at", "last_seen_at", "deleted_at", "first_discovered_at", "removed_at", "created_at", "updated_at"})
        model_names = {constraint.name for constraint in Base.metadata.tables[table].constraints if constraint.name}
        database_names = {
            inspector.get_pk_constraint(table, schema="public")["name"],
            *(item["name"] for item in inspector.get_unique_constraints(table, schema="public")),
            *(item["name"] for item in inspector.get_check_constraints(table, schema="public")),
            *(item["name"] for item in inspector.get_foreign_keys(table, schema="public")),
        }
        assert model_names == database_names
        assert expected_indexes[table].issubset({item["name"] for item in inspector.get_indexes(table, schema="public")})
    source_columns = {column["name"]: column for column in inspector.get_columns("source_items", schema="public")}
    assert str(source_columns["size_bytes"]["type"]).upper() == "BIGINT"
    assert str(source_columns["metadata"]["type"]).upper() == "JSONB"
    assert str(source_columns["metadata_schema_version"]["type"]).upper() == "SMALLINT"
    assert source_columns["metadata"]["default"] is not None
    assert source_columns["metadata_schema_version"]["default"] is not None
    assert not {"sync_run_id", "document_id", "acl", "permissions"}.intersection(source_columns)


def test_canonical_identity_is_connector_wide_case_sensitive(engine):
    org_a, org_b = _organization(engine, "Alpha"), _organization(engine, "Beta")
    connector_a_one = _connector(engine, org_a, "a-one")
    connector_a_two = _connector(engine, org_a, "a-two")
    connector_b = _connector(engine, org_b, "b-one")
    _item(engine, org_a, connector_a_one, "Docs/Readme.md")
    with pytest.raises(IntegrityError):
        _item(engine, org_a, connector_a_one, "Docs/Readme.md")
    assert _item(engine, org_a, connector_a_one, "docs/readme.md")
    assert _item(engine, org_a, connector_a_two, "Docs/Readme.md")
    assert _item(engine, org_b, connector_b, "Docs/Readme.md")
    assert _item(engine, org_a, connector_a_one, "child", parent_source_item_key="provider-parent-not-yet-seen")


def test_source_item_validation_and_tenant_ownership(engine):
    org_a, org_b = _organization(engine, "Gamma"), _organization(engine, "Delta")
    connector_a, connector_b = _connector(engine, org_a, "a"), _connector(engine, org_b, "b")
    assert _item(engine, org_a, connector_a, "file", source_item_type="file", size_bytes=0)
    assert _item(engine, org_a, connector_a, "folder", source_item_type="folder")
    assert _item(engine, org_a, connector_a, "unavailable", status="unavailable")
    assert _item(engine, org_a, connector_a, "deleted", status="deleted", deleted_at=datetime.now(timezone.utc))
    invalid_cases = (
        {"source_item_key": "   "}, {"source_item_key": "self", "parent_source_item_key": "self"},
        {"source_item_key": "type", "source_item_type": "Bad-Type"},
        {"source_item_key": "title", "title": "   "}, {"source_item_key": "url", "source_url": "   "},
        {"source_item_key": "mime", "mime_type": "   "}, {"source_item_key": "checksum", "source_checksum": "   "},
        {"source_item_key": "version", "source_version": "   "}, {"source_item_key": "parent", "parent_source_item_key": "   "},
        {"source_item_key": "size", "size_bytes": -1}, {"source_item_key": "status", "status": "changed"},
        {"source_item_key": "missing-deleted", "status": "deleted"},
        {"source_item_key": "unexpected-deleted", "deleted_at": datetime.now(timezone.utc)},
        {"source_item_key": "metadata", "metadata": "[]"},
        {"source_item_key": "metadata-version", "metadata_schema_version": 0},
    )
    for params in invalid_cases:
        key = params.pop("source_item_key")
        with pytest.raises(IntegrityError):
            _item(engine, org_a, connector_a, key, **params)
    first_seen = datetime(2026, 8, 19, tzinfo=timezone.utc)
    with pytest.raises(IntegrityError):
        _item(engine, org_a, connector_a, "seen-order", first_seen_at=first_seen, last_seen_at=first_seen - timedelta(seconds=1))
    with pytest.raises(IntegrityError):
        _item(engine, org_a, connector_b, "cross-tenant")


def test_scope_membership_deduplicates_and_enforces_connector(engine):
    org_a, org_b = _organization(engine, "Epsilon"), _organization(engine, "Zeta")
    connector_a = _connector(engine, org_a, "a")
    connector_a_two = _connector(engine, org_a, "a-two")
    connector_b = _connector(engine, org_b, "b")
    space_a, space_b = _space(engine, org_a, "space-a"), _space(engine, org_b, "space-b")
    scope_a_one = _scope(engine, org_a, connector_a, space_a, "one")
    scope_a_two = _scope(engine, org_a, connector_a, space_a, "two")
    scope_a_other_connector = _scope(engine, org_a, connector_a_two, space_a, "other")
    scope_b = _scope(engine, org_b, connector_b, space_b, "foreign")
    item_a = _item(engine, org_a, connector_a, "item")
    membership_id = _membership(engine, org_a, connector_a, item_a, scope_a_one)
    _membership(engine, org_a, connector_a, item_a, scope_a_two)
    with pytest.raises(IntegrityError):
        _membership(engine, org_a, connector_a, item_a, scope_a_one)
    with pytest.raises(IntegrityError):
        _membership(engine, org_a, connector_a, item_a, scope_a_other_connector)
    with pytest.raises(IntegrityError):
        _membership(engine, org_b, connector_b, item_a, scope_b)
    with pytest.raises(IntegrityError):
        _membership(engine, org_a, connector_a, item_a, scope_a_one, status="invalid")
    first_seen = datetime(2026, 8, 19, tzinfo=timezone.utc)
    with pytest.raises(IntegrityError):
        _membership(engine, org_a, connector_a, item_a, scope_a_one, first_discovered_at=first_seen, last_seen_at=first_seen - timedelta(seconds=1))
    with pytest.raises(IntegrityError):
        _membership(engine, org_a, connector_a, item_a, scope_a_one, status="removed")
    with pytest.raises(IntegrityError):
        _membership(engine, org_a, connector_a, item_a, scope_a_one, removed_at=first_seen)
    _execute(
        engine,
        "UPDATE source_item_scope_memberships SET status = 'removed', removed_at = :removed_at WHERE id = :id",
        id=membership_id,
        removed_at=first_seen,
    )
    assert _count(engine, "source_item_scope_memberships") == 2
    _execute(
        engine,
        "UPDATE source_item_scope_memberships SET status = 'active', removed_at = NULL WHERE id = :id",
        id=membership_id,
    )
    assert _count(engine, "source_item_scope_memberships") == 2


def test_referential_actions_preserve_canonical_item_semantics(engine):
    org = _organization(engine, "Eta")
    connector = _connector(engine, org, "connector")
    space = _space(engine, org, "space")
    scope_one = _scope(engine, org, connector, space, "one")
    scope_two = _scope(engine, org, connector, space, "two")
    item = _item(engine, org, connector, "item")
    _membership(engine, org, connector, item, scope_one)
    _membership(engine, org, connector, item, scope_two)
    _execute(engine, "DELETE FROM connector_scopes WHERE id = :id", id=scope_one)
    assert _count(engine, "source_items") == 1 and _count(engine, "source_item_scope_memberships") == 1
    _execute(engine, "DELETE FROM source_items WHERE id = :id", id=item)
    assert _count(engine, "source_item_scope_memberships") == 0

    item = _item(engine, org, connector, "item-two")
    _membership(engine, org, connector, item, scope_two)
    _execute(engine, "DELETE FROM connectors WHERE id = :id", id=connector)
    assert _count(engine, "source_items") == 0 and _count(engine, "source_item_scope_memberships") == 0

    connector = _connector(engine, org, "connector-two")
    scope = _scope(engine, org, connector, space, "three")
    item = _item(engine, org, connector, "item-three")
    _membership(engine, org, connector, item, scope)
    _execute(engine, "DELETE FROM organizations WHERE id = :id", id=org)
    assert _count(engine, "source_items") == 0 and _count(engine, "source_item_scope_memberships") == 0

    retained_org = _organization(engine, "Retained")
    _execute(
        engine,
        "INSERT INTO audit_events (id, organization_id, actor_type, actor_reference, action, resource_type, resource_id, outcome) VALUES (:id, :organization_id, 'system', 'test', 'organization.deleted', 'organization', :organization_id, 'denied')",
        id=uuid.uuid4(), organization_id=retained_org,
    )
    with pytest.raises(IntegrityError):
        _execute(engine, "DELETE FROM organizations WHERE id = :id", id=retained_org)


def test_downgrade_removes_only_source_tables_and_scope_key_then_reupgrades(engine):
    command.downgrade(_config(), PRIOR_REVISION)
    inspector = inspect(engine)
    tables = set(inspector.get_table_names(schema="public"))
    assert not {"source_items", "source_item_scope_memberships"}.intersection(tables)
    assert {"connectors", "connector_scopes", "knowledge_spaces", "audit_events", "documents", "document_chunks"}.issubset(tables)
    assert SCOPE_CONNECTOR_KEY not in {item["name"] for item in inspector.get_unique_constraints("connector_scopes", schema="public")}
    command.upgrade(_config(), "head")
    inspector = inspect(engine)
    assert {"source_items", "source_item_scope_memberships"}.issubset(inspector.get_table_names(schema="public"))
    assert SCOPE_CONNECTOR_KEY in {item["name"] for item in inspector.get_unique_constraints("connector_scopes", schema="public")}
