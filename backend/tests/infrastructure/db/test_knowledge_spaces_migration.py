from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from infrastructure.db.base import Base
from infrastructure.db import models as db_models  # noqa: F401
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PROJECT_VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
TEST_DATABASE_URL_ENV_VAR = "TEST_DATABASE_URL"
DATABASE_URL_ENV_VAR = "DATABASE_URL"
PRIOR_REVISION = "20260815_000006"

GRANT_TARGETS = {
    "knowledge_space_department_grants": "department_id",
    "knowledge_space_team_grants": "team_id",
    "knowledge_space_user_grants": "user_id",
}


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


def _department(engine, organization_id: uuid.UUID, slug: str) -> uuid.UUID:
    department_id = uuid.uuid4()
    _execute(
        engine,
        "INSERT INTO departments (id, organization_id, name, slug) VALUES (:id, :organization_id, :name, :slug)",
        id=department_id,
        organization_id=organization_id,
        name=slug.title(),
        slug=slug,
    )
    return department_id


def _team(engine, organization_id: uuid.UUID, slug: str) -> uuid.UUID:
    team_id = uuid.uuid4()
    _execute(
        engine,
        "INSERT INTO teams (id, organization_id, name, slug) VALUES (:id, :organization_id, :name, :slug)",
        id=team_id,
        organization_id=organization_id,
        name=slug.title(),
        slug=slug,
    )
    return team_id


def _space(engine, organization_id: uuid.UUID, slug: str, *, status: str = "active", archived_at=None) -> uuid.UUID:
    knowledge_space_id = uuid.uuid4()
    _execute(
        engine,
        """
        INSERT INTO knowledge_spaces (id, organization_id, name, slug, status, archived_at)
        VALUES (:id, :organization_id, :name, :slug, :status, :archived_at)
        """,
        id=knowledge_space_id,
        organization_id=organization_id,
        name=slug.replace("-", " ").title(),
        slug=slug,
        status=status,
        archived_at=archived_at,
    )
    return knowledge_space_id


def _grant(
    engine,
    table: str,
    organization_id: uuid.UUID,
    knowledge_space_id: uuid.UUID,
    *,
    target_id: uuid.UUID | None = None,
    permission_level: str = "viewer",
    granted_at: datetime | None = None,
    expires_at: datetime | None = None,
    revoked_at: datetime | None = None,
    granted_by_user_id: uuid.UUID | None = None,
) -> uuid.UUID:
    target_column = GRANT_TARGETS.get(table)
    columns = ["id", "organization_id", "knowledge_space_id"]
    values = [":id", ":organization_id", ":knowledge_space_id"]
    params: dict[str, object] = {
        "id": uuid.uuid4(),
        "organization_id": organization_id,
        "knowledge_space_id": knowledge_space_id,
        "permission_level": permission_level,
        "granted_at": granted_at or datetime(2026, 8, 16, tzinfo=timezone.utc),
        "expires_at": expires_at,
        "revoked_at": revoked_at,
        "granted_by_user_id": granted_by_user_id,
    }
    if target_column is not None:
        columns.append(target_column)
        values.append(":target_id")
        params["target_id"] = target_id
    columns.extend(["permission_level", "granted_by_user_id", "granted_at", "expires_at", "revoked_at"])
    values.extend([":permission_level", ":granted_by_user_id", ":granted_at", ":expires_at", ":revoked_at"])
    _execute(
        engine,
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})",
        **params,
    )
    return params["id"]  # type: ignore[return-value]


def _count(engine, table: str) -> int:
    with engine.connect() as connection:
        return connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()


def test_upgrade_creates_typed_grant_schema_and_matches_metadata(engine):
    inspector = inspect(engine)
    expected_tables = {
        "knowledge_spaces",
        "knowledge_space_organization_grants",
        "knowledge_space_department_grants",
        "knowledge_space_team_grants",
        "knowledge_space_user_grants",
    }
    assert expected_tables.issubset(set(inspector.get_table_names(schema="public")))
    assert {"organizations", "users", "departments", "teams", "documents", "document_chunks"}.issubset(
        set(inspector.get_table_names(schema="public"))
    )
    assert engine.connect().execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")).scalar_one() == 1

    expected_columns = {
        "knowledge_spaces": {"id", "organization_id", "name", "slug", "description", "status", "created_at", "updated_at", "archived_at"},
        "knowledge_space_organization_grants": {"id", "organization_id", "knowledge_space_id", "permission_level", "granted_by_user_id", "granted_at", "expires_at", "revoked_at", "reason", "created_at", "updated_at"},
        "knowledge_space_department_grants": {"id", "organization_id", "knowledge_space_id", "department_id", "permission_level", "granted_by_user_id", "granted_at", "expires_at", "revoked_at", "reason", "created_at", "updated_at"},
        "knowledge_space_team_grants": {"id", "organization_id", "knowledge_space_id", "team_id", "permission_level", "granted_by_user_id", "granted_at", "expires_at", "revoked_at", "reason", "created_at", "updated_at"},
        "knowledge_space_user_grants": {"id", "organization_id", "knowledge_space_id", "user_id", "permission_level", "granted_by_user_id", "granted_at", "expires_at", "revoked_at", "reason", "created_at", "updated_at"},
    }
    expected_foreign_keys = {
        "knowledge_spaces": {"fk_knowledge_spaces_organization_id_organizations"},
        "knowledge_space_organization_grants": {"fk_ks_organization_grants_organization", "fk_ks_organization_grants_space_tenant", "fk_ks_organization_grants_creator"},
        "knowledge_space_department_grants": {"fk_ks_department_grants_organization", "fk_ks_department_grants_space_tenant", "fk_ks_department_grants_department_tenant", "fk_ks_department_grants_creator"},
        "knowledge_space_team_grants": {"fk_ks_team_grants_organization", "fk_ks_team_grants_space_tenant", "fk_ks_team_grants_team_tenant", "fk_ks_team_grants_creator"},
        "knowledge_space_user_grants": {"fk_ks_user_grants_organization", "fk_ks_user_grants_space_tenant", "fk_ks_user_grants_user_tenant", "fk_ks_user_grants_creator"},
    }
    for table, columns in expected_columns.items():
        reflected_columns = inspector.get_columns(table, schema="public")
        assert {column["name"] for column in reflected_columns} == columns
        assert list(Base.metadata.tables[table].columns.keys()) == [column["name"] for column in inspector.get_columns(table, schema="public")]
        assert inspector.get_pk_constraint(table, schema="public")["name"] == f"pk_{table}"
        assert all(not column["nullable"] for column in reflected_columns if column["name"] in {"id", "organization_id", "knowledge_space_id"})
        assert {item["name"] for item in inspector.get_foreign_keys(table, schema="public")} == expected_foreign_keys[table]
        model_constraint_names = {constraint.name for constraint in Base.metadata.tables[table].constraints if constraint.name}
        database_constraint_names = {
            inspector.get_pk_constraint(table, schema="public")["name"],
            *(item["name"] for item in inspector.get_unique_constraints(table, schema="public")),
            *(item["name"] for item in inspector.get_check_constraints(table, schema="public")),
            *(item["name"] for item in inspector.get_foreign_keys(table, schema="public")),
        }
        assert model_constraint_names == database_constraint_names

    assert {"uq_knowledge_spaces_organization_id_id", "uq_knowledge_spaces_organization_id_slug"}.issubset(
        {item["name"] for item in inspector.get_unique_constraints("knowledge_spaces", schema="public")}
    )
    for table, unique_name in (
        ("knowledge_space_organization_grants", "uq_ks_organization_grants_space"),
        ("knowledge_space_department_grants", "uq_ks_department_grants_space_department"),
        ("knowledge_space_team_grants", "uq_ks_team_grants_space_team"),
        ("knowledge_space_user_grants", "uq_ks_user_grants_space_user"),
    ):
        assert unique_name in {item["name"] for item in inspector.get_unique_constraints(table, schema="public")}
    assert "ix_knowledge_spaces_organization_id_status" in {item["name"] for item in inspector.get_indexes("knowledge_spaces", schema="public")}
    assert "ix_ks_department_grants_org_department" in {item["name"] for item in inspector.get_indexes("knowledge_space_department_grants", schema="public")}
    assert "ix_ks_team_grants_org_team" in {item["name"] for item in inspector.get_indexes("knowledge_space_team_grants", schema="public")}
    assert "ix_ks_user_grants_org_user" in {item["name"] for item in inspector.get_indexes("knowledge_space_user_grants", schema="public")}


def test_knowledge_space_lifecycle_and_tenant_slug_constraints(engine):
    org_a, org_b = _organization(engine, "Alpha"), _organization(engine, "Beta")
    assert _space(engine, org_a, "engineering")
    assert _space(engine, org_a, "inactive-space", status="inactive")
    assert _space(engine, org_a, "archived-space", status="archived", archived_at=datetime.now(timezone.utc))
    assert _space(engine, org_b, "engineering")
    with pytest.raises(IntegrityError):
        _space(engine, org_a, "engineering")
    with pytest.raises(IntegrityError):
        _execute(engine, "INSERT INTO knowledge_spaces (id, organization_id, name, slug) VALUES (:id, :organization_id, '   ', 'blank')", id=uuid.uuid4(), organization_id=org_a)
    with pytest.raises(IntegrityError):
        _space(engine, org_a, "Bad_Slug")
    with pytest.raises(IntegrityError):
        _space(engine, org_a, "bad-status", status="public")
    with pytest.raises(IntegrityError):
        _space(engine, org_a, "archive-missing", status="archived")
    with pytest.raises(IntegrityError):
        _space(engine, org_a, "archive-unexpected", archived_at=datetime.now(timezone.utc))


def test_organization_grants_enforce_tenant_uniqueness_and_lifecycle(engine):
    org_a, org_b = _organization(engine, "Gamma"), _organization(engine, "Delta")
    space_a, space_b = _space(engine, org_a, "space-a"), _space(engine, org_b, "space-b")
    granted_at = datetime(2026, 8, 16, tzinfo=timezone.utc)
    _grant(engine, "knowledge_space_organization_grants", org_a, space_a, permission_level="manager", granted_at=granted_at)
    with pytest.raises(IntegrityError):
        _grant(engine, "knowledge_space_organization_grants", org_b, space_a)
    with pytest.raises(IntegrityError):
        _grant(engine, "knowledge_space_organization_grants", org_a, space_a)
    with pytest.raises(IntegrityError):
        _grant(engine, "knowledge_space_organization_grants", org_b, space_b, permission_level="owner")
    with pytest.raises(IntegrityError):
        _grant(engine, "knowledge_space_organization_grants", org_b, space_b, granted_at=granted_at, expires_at=granted_at)
    with pytest.raises(IntegrityError):
        _grant(engine, "knowledge_space_organization_grants", org_b, space_b, granted_at=granted_at, revoked_at=granted_at - timedelta(seconds=1))
    _grant(engine, "knowledge_space_organization_grants", org_b, space_b, granted_at=granted_at, expires_at=granted_at + timedelta(days=1), revoked_at=granted_at)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT revoked_at FROM knowledge_space_organization_grants WHERE organization_id = :organization_id"), {"organization_id": org_b}).scalar_one() == granted_at


@pytest.mark.parametrize(
    ("table", "target_factory"),
    (
        ("knowledge_space_department_grants", _department),
        ("knowledge_space_team_grants", _team),
        ("knowledge_space_user_grants", lambda engine, organization_id, slug: _user(engine, organization_id, f"{slug}@example.com")),
    ),
)
def test_typed_grants_enforce_tenant_uniqueness_and_multiple_spaces(engine, table, target_factory):
    org_a, org_b = _organization(engine, f"{table}-a"), _organization(engine, f"{table}-b")
    target_a = target_factory(engine, org_a, "target-a")
    target_b = target_factory(engine, org_b, "target-b")
    space_a_one, space_a_two = _space(engine, org_a, "space-one"), _space(engine, org_a, "space-two")
    space_b = _space(engine, org_b, "space-three")
    _grant(engine, table, org_a, space_a_one, target_id=target_a)
    _grant(engine, table, org_a, space_a_two, target_id=target_a, permission_level="contributor")
    with pytest.raises(IntegrityError):
        _grant(engine, table, org_a, space_a_one, target_id=target_a)
    with pytest.raises(IntegrityError):
        _grant(engine, table, org_a, space_b, target_id=target_a)
    with pytest.raises(IntegrityError):
        _grant(engine, table, org_a, space_a_one, target_id=target_b)


def test_grant_deletion_actions_and_creator_tenant_safety(engine):
    org_a, org_b = _organization(engine, "Epsilon"), _organization(engine, "Zeta")
    creator = _user(engine, org_a, "creator@example.com")
    target = _user(engine, org_a, "target@example.com")
    foreign_creator = _user(engine, org_b, "foreign@example.com")
    department = _department(engine, org_a, "engineering")
    team = _team(engine, org_a, "platform")
    space_one, space_two = _space(engine, org_a, "space-one"), _space(engine, org_a, "space-two")
    _grant(engine, "knowledge_space_organization_grants", org_a, space_one, granted_by_user_id=creator)
    _grant(engine, "knowledge_space_department_grants", org_a, space_one, target_id=department, granted_by_user_id=creator)
    _grant(engine, "knowledge_space_team_grants", org_a, space_one, target_id=team, granted_by_user_id=creator)
    _grant(engine, "knowledge_space_user_grants", org_a, space_one, target_id=target, granted_by_user_id=creator)
    with pytest.raises(IntegrityError):
        _grant(engine, "knowledge_space_organization_grants", org_a, space_two, granted_by_user_id=foreign_creator)

    _execute(engine, "DELETE FROM users WHERE id = :id", id=creator)
    for table in ("knowledge_space_organization_grants", "knowledge_space_department_grants", "knowledge_space_team_grants", "knowledge_space_user_grants"):
        with engine.connect() as connection:
            assert connection.execute(text(f"SELECT granted_by_user_id FROM {table}")).scalar_one() is None

    _execute(engine, "DELETE FROM users WHERE id = :id", id=target)
    assert _count(engine, "knowledge_space_user_grants") == 0
    assert _count(engine, "knowledge_space_department_grants") == 1
    assert _count(engine, "knowledge_space_team_grants") == 1

    _execute(engine, "DELETE FROM departments WHERE id = :id", id=department)
    assert _count(engine, "knowledge_space_department_grants") == 0
    assert _count(engine, "knowledge_spaces") == 2
    _execute(engine, "DELETE FROM teams WHERE id = :id", id=team)
    assert _count(engine, "knowledge_space_team_grants") == 0

    _execute(engine, "DELETE FROM knowledge_spaces WHERE id = :id", id=space_one)
    assert _count(engine, "knowledge_space_organization_grants") == 0
    assert _count(engine, "knowledge_spaces") == 1

    new_user = _user(engine, org_a, "new-user@example.com")
    _grant(engine, "knowledge_space_user_grants", org_a, space_two, target_id=new_user)
    _execute(engine, "DELETE FROM organizations WHERE id = :id", id=org_a)
    for table in (
        "knowledge_spaces",
        "knowledge_space_organization_grants",
        "knowledge_space_department_grants",
        "knowledge_space_team_grants",
        "knowledge_space_user_grants",
    ):
        assert _count(engine, table) == 0


def test_downgrade_removes_only_knowledge_space_tables_and_reupgrade_succeeds(engine):
    command.downgrade(_config(), PRIOR_REVISION)
    tables = set(inspect(engine).get_table_names(schema="public"))
    assert not {
        "knowledge_spaces",
        "knowledge_space_organization_grants",
        "knowledge_space_department_grants",
        "knowledge_space_team_grants",
        "knowledge_space_user_grants",
    }.intersection(tables)
    assert {"organizations", "users", "departments", "teams", "documents", "document_chunks"}.issubset(tables)
    command.upgrade(_config(), "head")
    assert {
        "knowledge_spaces",
        "knowledge_space_organization_grants",
        "knowledge_space_department_grants",
        "knowledge_space_team_grants",
        "knowledge_space_user_grants",
    }.issubset(set(inspect(engine).get_table_names(schema="public")))