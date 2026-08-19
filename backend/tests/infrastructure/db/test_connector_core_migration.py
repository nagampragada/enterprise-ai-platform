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
PRIOR_REVISION = "20260817_000008"


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
            "connector_scopes", "connectors", "audit_events",
            "knowledge_space_user_grants", "knowledge_space_team_grants",
            "knowledge_space_department_grants", "knowledge_space_organization_grants",
            "knowledge_spaces", "team_memberships", "department_memberships", "teams", "departments",
            "document_chunks", "documents", "authentication_sessions", "user_roles", "users",
            "organization_settings", "organizations", "industries",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


def _execute(engine, statement: str, **params):
    with engine.begin() as connection:
        return connection.execute(text(statement), params)


def _organization(engine, name: str) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(engine, "INSERT INTO organizations (id, name, slug) VALUES (:id, :name, :slug)", id=value, name=name, slug=f"{name.lower()}-{value}")
    return value


def _user(engine, organization_id: uuid.UUID, email: str) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(
        engine,
        "INSERT INTO users (id, organization_id, email, normalized_email, password_hash, display_name) VALUES (:id, :organization_id, :email, :email, 'argon2id$test', :display_name)",
        id=value, organization_id=organization_id, email=email, display_name=email.split("@")[0],
    )
    return value


def _space(engine, organization_id: uuid.UUID, slug: str, status: str = "active") -> uuid.UUID:
    value = uuid.uuid4()
    _execute(
        engine,
        "INSERT INTO knowledge_spaces (id, organization_id, name, slug, status) VALUES (:id, :organization_id, :name, :slug, :status)",
        id=value, organization_id=organization_id, name=slug.title(), slug=slug, status=status,
    )
    return value


def _connector(
    engine,
    organization_id: uuid.UUID,
    slug: str,
    *,
    connector_type: str = "local_folder",
    display_name: str = "Local Folder",
    status: str = "active",
    acl_support: str = "none",
    capabilities: str = "{}",
    safe_config: str = "{}",
    config_schema_version: int = 1,
    created_by_user_id: uuid.UUID | None = None,
    archived_at: datetime | None = None,
) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(
        engine,
        """
        INSERT INTO connectors (
            id, organization_id, connector_type, display_name, slug, status, acl_support,
            capabilities, safe_config, config_schema_version,
            created_by_user_id, created_at, archived_at
        ) VALUES (
            :id, :organization_id, :connector_type, :display_name, :slug, :status, :acl_support,
            CAST(:capabilities AS jsonb), CAST(:safe_config AS jsonb), :config_schema_version,
            :created_by_user_id,
            :created_at, :archived_at
        )
        """,
        id=value, organization_id=organization_id, connector_type=connector_type,
        display_name=display_name, slug=slug, status=status, acl_support=acl_support,
        capabilities=capabilities, safe_config=safe_config, config_schema_version=config_schema_version,
        created_by_user_id=created_by_user_id,
        created_at=datetime(2026, 8, 18, tzinfo=timezone.utc), archived_at=archived_at,
    )
    return value


def _scope(
    engine,
    organization_id: uuid.UUID,
    connector_id: uuid.UUID,
    knowledge_space_id: uuid.UUID | None,
    slug: str,
    *,
    display_name: str = "Company Documents",
    scope_type: str = "folder",
    external_scope_key: str = "/mounted/company-documents",
    access_mode: str = "platform_managed",
    status: str = "active",
    safe_config: str = "{}",
    config_schema_version: int = 1,
    created_by_user_id: uuid.UUID | None = None,
    removed_at: datetime | None = None,
) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(
        engine,
        """
        INSERT INTO connector_scopes (
            id, organization_id, connector_id, knowledge_space_id, display_name, slug,
            scope_type, external_scope_key, access_mode, status, safe_config,
            config_schema_version, created_by_user_id, removed_at
        ) VALUES (
            :id, :organization_id, :connector_id, :knowledge_space_id, :display_name, :slug,
            :scope_type, :external_scope_key, :access_mode, :status, CAST(:safe_config AS jsonb),
            :config_schema_version, :created_by_user_id, :removed_at
        )
        """,
        id=value, organization_id=organization_id, connector_id=connector_id,
        knowledge_space_id=knowledge_space_id, display_name=display_name, slug=slug,
        scope_type=scope_type, external_scope_key=external_scope_key, access_mode=access_mode,
        status=status, safe_config=safe_config, config_schema_version=config_schema_version,
        created_by_user_id=created_by_user_id, removed_at=removed_at,
    )
    return value


def _count(engine, table: str) -> int:
    with engine.connect() as connection:
        return connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()


def test_schema_matches_models_and_required_indexes(engine):
    inspector = inspect(engine)
    assert {"connectors", "connector_scopes"}.issubset(inspector.get_table_names(schema="public"))
    assert {"organizations", "users", "knowledge_spaces", "audit_events", "documents", "document_chunks"}.issubset(inspector.get_table_names(schema="public"))
    assert engine.connect().execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")).scalar_one() == 1
    expected_columns = {
        "connectors": ["id", "organization_id", "connector_type", "display_name", "slug", "status", "acl_support", "capabilities", "safe_config", "config_schema_version", "created_by_user_id", "last_validated_at", "created_at", "updated_at", "archived_at"],
        "connector_scopes": ["id", "organization_id", "connector_id", "knowledge_space_id", "display_name", "slug", "scope_type", "external_scope_key", "access_mode", "status", "safe_config", "config_schema_version", "created_by_user_id", "last_validated_at", "created_at", "updated_at", "removed_at"],
    }
    expected_indexes = {
        "connectors": {"ix_connectors_organization_id_status", "ix_connectors_org_type_status"},
        "connector_scopes": {"ix_connector_scopes_org_connector_status", "ix_connector_scopes_org_space_status", "ix_connector_scopes_org_access_status"},
    }
    nullable_columns = {
        "connectors": {"created_by_user_id", "last_validated_at", "archived_at"},
        "connector_scopes": {"created_by_user_id", "last_validated_at", "removed_at"},
    }
    defaulted_columns = {
        "connectors": {"status", "acl_support", "capabilities", "safe_config", "config_schema_version", "created_at", "updated_at"},
        "connector_scopes": {"status", "safe_config", "config_schema_version", "created_at", "updated_at"},
    }
    for table, columns in expected_columns.items():
        reflected = inspector.get_columns(table, schema="public")
        assert [column["name"] for column in reflected] == columns
        assert list(Base.metadata.tables[table].columns.keys()) == columns
        assert inspector.get_pk_constraint(table, schema="public")["name"] == f"pk_{table}"
        assert {column["name"] for column in reflected if column["nullable"]} == nullable_columns[table]
        assert defaulted_columns[table].issubset({column["name"] for column in reflected if column["default"] is not None})
        assert str(next(column for column in reflected if column["name"] == "safe_config")["type"]).upper() == "JSONB"
        assert str(next(column for column in reflected if column["name"] == "config_schema_version")["type"]).upper() == "SMALLINT"
        assert all(column["type"].timezone is True for column in reflected if column["name"] in {"last_validated_at", "created_at", "updated_at", "archived_at", "removed_at"})
        model_names = {constraint.name for constraint in Base.metadata.tables[table].constraints if constraint.name}
        database_names = {
            inspector.get_pk_constraint(table, schema="public")["name"],
            *(item["name"] for item in inspector.get_unique_constraints(table, schema="public")),
            *(item["name"] for item in inspector.get_check_constraints(table, schema="public")),
            *(item["name"] for item in inspector.get_foreign_keys(table, schema="public")),
        }
        assert model_names == database_names
        assert expected_indexes[table].issubset({item["name"] for item in inspector.get_indexes(table, schema="public")})


def test_connector_validation_and_tenant_uniqueness(engine):
    org_a, org_b = _organization(engine, "Alpha"), _organization(engine, "Beta")
    creator_a, creator_b = _user(engine, org_a, "a@example.com"), _user(engine, org_b, "b@example.com")
    assert _connector(engine, org_a, "local-documents", created_by_user_id=creator_a, capabilities='{"supports_permissions": false}', safe_config='{"root_path": "/mounted/company-documents"}')
    assert _connector(engine, org_a, "acl-source", connector_type="google_drive", acl_support="complete")
    assert _connector(engine, org_b, "local-documents")
    invalid_cases = (
        {"display_name": "   "}, {"connector_type": "Bad-Type"}, {"slug": "Bad_Slug"},
        {"status": "invalid"}, {"acl_support": "full"},
        {"capabilities": "[]"}, {"safe_config": "[]"}, {"config_schema_version": 0},
        {"status": "archived"},
        {"archived_at": datetime.now(timezone.utc)},
        {"created_by_user_id": creator_b},
    )
    for index, overrides in enumerate(invalid_cases):
        slug = overrides.pop("slug", f"invalid-{index}")
        with pytest.raises(IntegrityError):
            _connector(engine, org_a, slug, **overrides)
    with pytest.raises(IntegrityError):
        _connector(engine, org_a, "local-documents")


def test_scope_validation_uniqueness_and_tenant_isolation(engine):
    org_a, org_b = _organization(engine, "Gamma"), _organization(engine, "Delta")
    creator_a, creator_b = _user(engine, org_a, "a@example.com"), _user(engine, org_b, "b@example.com")
    space_a, space_b = _space(engine, org_a, "space-a"), _space(engine, org_b, "space-b")
    connector_a = _connector(engine, org_a, "connector-a")
    connector_a_two = _connector(engine, org_a, "connector-b")
    connector_b = _connector(engine, org_b, "connector-c")
    assert _scope(engine, org_a, connector_a, space_a, "company-documents", created_by_user_id=creator_a)
    assert _scope(engine, org_a, connector_a_two, space_a, "other", external_scope_key="/mounted/company-documents")
    invalid_cases = (
        {"knowledge_space_id": None}, {"display_name": "   "}, {"slug": "Bad_Slug"},
        {"access_mode": "public"}, {"status": "archived"},
        {"scope_type": "Bad-Type"}, {"external_scope_key": "   "}, {"safe_config": "[]"},
        {"config_schema_version": 0}, {"status": "removed"},
        {"removed_at": datetime.now(timezone.utc)}, {"created_by_user_id": creator_b},
    )
    for index, overrides in enumerate(invalid_cases):
        params = {"knowledge_space_id": space_a, **overrides}
        external_scope_key = params.pop("external_scope_key", f"key-{index}")
        slug = params.pop("slug", f"invalid-{index}")
        with pytest.raises(IntegrityError):
            _scope(engine, org_a, connector_a, params.pop("knowledge_space_id"), slug, external_scope_key=external_scope_key, **params)
    with pytest.raises(IntegrityError):
        _scope(engine, org_a, connector_a, space_a, "company-documents", external_scope_key="other")
    with pytest.raises(IntegrityError):
        _scope(engine, org_a, connector_a, space_a, "different", external_scope_key="/mounted/company-documents")
    with pytest.raises(IntegrityError):
        _scope(engine, org_a, connector_b, space_a, "cross-connector", external_scope_key="cross-connector")
    with pytest.raises(IntegrityError):
        _scope(engine, org_a, connector_a, space_b, "cross-space", external_scope_key="cross-space")


def test_referential_actions_and_audit_retention(engine):
    org = _organization(engine, "Epsilon")
    creator = _user(engine, org, "creator@example.com")
    space = _space(engine, org, "space")
    unrelated_space = _space(engine, org, "unrelated")
    connector = _connector(engine, org, "connector", created_by_user_id=creator)
    scope = _scope(engine, org, connector, space, "scope", created_by_user_id=creator)
    _execute(engine, "DELETE FROM users WHERE id = :id", id=creator)
    with engine.connect() as connection:
        row = connection.execute(text("SELECT created_by_user_id FROM connectors WHERE id = :id"), {"id": connector}).one()
        scope_row = connection.execute(text("SELECT created_by_user_id FROM connector_scopes WHERE id = :id"), {"id": scope}).one()
    assert row.created_by_user_id is None and scope_row.created_by_user_id is None
    with pytest.raises(IntegrityError):
        _execute(engine, "DELETE FROM knowledge_spaces WHERE id = :id", id=space)
    _execute(engine, "DELETE FROM knowledge_spaces WHERE id = :id", id=unrelated_space)
    _execute(engine, "DELETE FROM connectors WHERE id = :id", id=connector)
    assert _count(engine, "connector_scopes") == 0

    connector = _connector(engine, org, "connector-two")
    _scope(engine, org, connector, space, "scope-two")
    _execute(engine, "DELETE FROM organizations WHERE id = :id", id=org)
    assert _count(engine, "connectors") == 0 and _count(engine, "connector_scopes") == 0

    retained_org = _organization(engine, "Retained")
    _execute(
        engine,
        "INSERT INTO audit_events (id, organization_id, actor_type, actor_reference, action, resource_type, resource_id, outcome) VALUES (:id, :organization_id, 'system', 'test', 'organization.deleted', 'organization', :organization_id, 'denied')",
        id=uuid.uuid4(), organization_id=retained_org,
    )
    with pytest.raises(IntegrityError):
        _execute(engine, "DELETE FROM organizations WHERE id = :id", id=retained_org)


def test_cross_row_security_rules_are_intentionally_service_invariants(engine):
    org = _organization(engine, "Zeta")
    inactive_space = _space(engine, org, "inactive-space", status="inactive")
    draft_connector = _connector(engine, org, "draft-connector", status="draft", acl_support="none")
    _scope(engine, org, draft_connector, inactive_space, "source-acl", access_mode="source_acl", status="active")
    _scope(engine, org, draft_connector, inactive_space, "hybrid", external_scope_key="hybrid", access_mode="hybrid", status="active")
    assert _count(engine, "connector_scopes") == 2


def test_downgrade_removes_only_connector_core_and_reupgrade_succeeds(engine):
    command.downgrade(_config(), PRIOR_REVISION)
    tables = set(inspect(engine).get_table_names(schema="public"))
    assert not {"connectors", "connector_scopes"}.intersection(tables)
    assert {"organizations", "users", "knowledge_spaces", "audit_events", "documents", "document_chunks"}.issubset(tables)
    command.upgrade(_config(), "head")
    assert {"connectors", "connector_scopes"}.issubset(inspect(engine).get_table_names(schema="public"))
