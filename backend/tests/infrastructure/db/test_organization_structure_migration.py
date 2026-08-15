from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PROJECT_VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
TEST_DATABASE_URL_ENV_VAR = "TEST_DATABASE_URL"
DATABASE_URL_ENV_VAR = "DATABASE_URL"
PRIOR_REVISION = "20260814_000005"
REVISION = "20260815_000006"


def _database_identity(database_url: str) -> tuple[str, str | None, int | None, str | None]:
    url = make_url(database_url)
    return url.drivername, url.host, url.port, url.database


def _required_url(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def _config() -> Config:
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", _required_url(TEST_DATABASE_URL_ENV_VAR))
    return config


def _upgrade(database_url: str, revision: str = "head") -> None:
    environment = os.environ.copy()
    environment[DATABASE_URL_ENV_VAR] = database_url
    subprocess.run(
        [str(PROJECT_VENV_PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", revision],
        check=True,
        cwd=str(PROJECT_ROOT),
        env=environment,
    )


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
        connection.execute(text("DELETE FROM team_memberships"))
        connection.execute(text("DELETE FROM department_memberships"))
        connection.execute(text("DELETE FROM teams"))
        connection.execute(text("DELETE FROM departments"))
        connection.execute(text("DELETE FROM document_chunks"))
        connection.execute(text("DELETE FROM documents"))
        connection.execute(text("DELETE FROM authentication_sessions"))
        connection.execute(text("DELETE FROM user_roles"))
        connection.execute(text("DELETE FROM users"))
        connection.execute(text("DELETE FROM organization_settings"))
        connection.execute(text("DELETE FROM organizations"))
        connection.execute(text("DELETE FROM industries"))


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
        """
        INSERT INTO users (id, organization_id, email, normalized_email, password_hash, display_name)
        VALUES (:id, :organization_id, :email, :normalized_email, 'argon2id$test', :display_name)
        """,
        id=value,
        organization_id=organization_id,
        email=email,
        normalized_email=email.lower(),
        display_name=email.split("@")[0],
    )
    return value


def _department(engine, organization_id: uuid.UUID, slug: str, *, parent_id=None, status="active", archived_at=None) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(
        engine,
        """
        INSERT INTO departments (id, organization_id, parent_department_id, name, slug, status, archived_at)
        VALUES (:id, :organization_id, :parent_id, :name, :slug, :status, :archived_at)
        """,
        id=value,
        organization_id=organization_id,
        parent_id=parent_id,
        name=slug.replace("-", " ").title(),
        slug=slug,
        status=status,
        archived_at=archived_at,
    )
    return value


def _team(engine, organization_id: uuid.UUID, slug: str) -> uuid.UUID:
    value = uuid.uuid4()
    _execute(
        engine,
        "INSERT INTO teams (id, organization_id, name, slug) VALUES (:id, :organization_id, :name, :slug)",
        id=value, organization_id=organization_id, name=slug.title(), slug=slug,
    )
    return value


def _membership(engine, table: str, organization_id, entity_id, user_id, *, responsibility="member", status="active", effective=None, expires=None, revoked=None, creator=None):
    _execute(
        engine,
        f"""
        INSERT INTO {table} (id, organization_id, {('department' if table.startswith('department') else 'team')}_id, user_id, responsibility, status, effective_from, expires_at, revoked_at, created_by_user_id)
        VALUES (:id, :organization_id, :entity_id, :user_id, :responsibility, :status, :effective, :expires, :revoked, :creator)
        """,
        id=uuid.uuid4(), organization_id=organization_id, entity_id=entity_id, user_id=user_id,
        responsibility=responsibility, status=status,
        effective=effective or datetime(2026, 1, 1, tzinfo=timezone.utc), expires=expires, revoked=revoked, creator=creator,
    )


def test_structure_constraints_indexes_and_mapper_contract(engine):
    inspector = inspect(engine)
    expected = {"departments", "teams", "department_memberships", "team_memberships"}
    assert expected.issubset(set(inspector.get_table_names(schema="public")))
    assert "documents" in inspector.get_table_names(schema="public")
    assert "document_chunks" in inspector.get_table_names(schema="public")
    assert engine.connect().execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")).scalar_one() == 1
    for table in expected:
        assert "id" in {column["name"] for column in inspector.get_columns(table, schema="public")}
        assert inspector.get_pk_constraint(table, schema="public")["name"].startswith("pk_")
    assert {"uq_departments_organization_id_id", "uq_departments_organization_id_slug"}.issubset({item["name"] for item in inspector.get_unique_constraints("departments", schema="public")})
    assert {"uq_teams_organization_id_id", "uq_teams_organization_id_slug"}.issubset({item["name"] for item in inspector.get_unique_constraints("teams", schema="public")})
    assert "ix_departments_organization_id_status" in {item["name"] for item in inspector.get_indexes("departments", schema="public")}
    assert "ix_departments_organization_id_parent_department_id" in {item["name"] for item in inspector.get_indexes("departments", schema="public")}
    assert "ix_teams_organization_id_status" in {item["name"] for item in inspector.get_indexes("teams", schema="public")}
    department_membership_indexes = {item["name"] for item in inspector.get_indexes("department_memberships", schema="public")}
    assert {"ix_department_memberships_organization_id_user_id_status", "ix_department_memberships_organization_id_department_id_status"}.issubset(department_membership_indexes)
    team_membership_indexes = {item["name"] for item in inspector.get_indexes("team_memberships", schema="public")}
    assert {"ix_team_memberships_organization_id_user_id_status", "ix_team_memberships_organization_id_team_id_status"}.issubset(team_membership_indexes)


def test_optional_structure_hierarchy_slug_and_tenant_isolation(engine):
    org_a, org_b = _organization(engine, "Alpha"), _organization(engine, "Beta")
    user_a, user_b = _user(engine, org_a, "a@example.com"), _user(engine, org_b, "b@example.com")
    root = _department(engine, org_a, "engineering")
    child = _department(engine, org_a, "platform", parent_id=root)
    assert root and child
    assert _department(engine, org_b, "engineering")
    assert _team(engine, org_a, "platform-team")
    assert _team(engine, org_b, "platform-team")
    with pytest.raises(IntegrityError):
        _department(engine, org_b, "cross-parent", parent_id=root)
    with pytest.raises(IntegrityError):
        _department(engine, org_a, "engineering")
    with pytest.raises(IntegrityError):
        _department(engine, org_a, "invalid_slug")
    with pytest.raises(IntegrityError):
        _execute(
            engine,
            "INSERT INTO departments (id, organization_id, name, slug) VALUES (:id, :organization_id, '   ', 'blank-name')",
            id=uuid.uuid4(),
            organization_id=org_a,
        )
    with pytest.raises(IntegrityError):
        _execute(
            engine,
            "INSERT INTO teams (id, organization_id, name, slug, status) VALUES (:id, :organization_id, 'Team', 'bad-status', 'bad')",
            id=uuid.uuid4(),
            organization_id=org_a,
        )
    with pytest.raises(IntegrityError):
        _execute(
            engine,
            "INSERT INTO teams (id, organization_id, name, slug, status) VALUES (:id, :organization_id, 'Team', 'missing-archive', 'archived')",
            id=uuid.uuid4(),
            organization_id=org_a,
        )
    with pytest.raises(IntegrityError):
        _department(engine, org_a, "self-parent", parent_id=uuid.uuid4())
    with pytest.raises(IntegrityError):
        _membership(engine, "department_memberships", org_b, root, user_b)
    with pytest.raises(IntegrityError):
        _membership(engine, "team_memberships", org_b, _team(engine, org_a, "another-team"), user_b)


def test_self_parent_membership_temporal_and_responsibility_constraints(engine):
    org = _organization(engine, "Gamma")
    user = _user(engine, org, "user@example.com")
    department = _department(engine, org, "engineering")
    team = _team(engine, org, "platform")
    with pytest.raises(IntegrityError):
        _execute(engine, "UPDATE departments SET parent_department_id = id WHERE id = :id", id=department)
    _membership(engine, "department_memberships", org, department, user, responsibility="manager")
    _membership(engine, "team_memberships", org, team, user, responsibility="owner")
    with pytest.raises(IntegrityError):
        _membership(engine, "department_memberships", org, department, user)
    with pytest.raises(IntegrityError):
        _membership(engine, "team_memberships", org, team, user)
    with pytest.raises(IntegrityError):
        _membership(engine, "department_memberships", org, department, user, responsibility="owner")
    with pytest.raises(IntegrityError):
        _membership(engine, "team_memberships", org, team, user, responsibility="bad")
    future = datetime(2026, 1, 1, tzinfo=timezone.utc) - timedelta(days=1)
    with pytest.raises(IntegrityError):
        _membership(engine, "department_memberships", org, department, uuid.uuid4(), expires=future)


def test_parent_deletion_is_restricted_and_revocation_is_consistent(engine):
    org = _organization(engine, "Delta")
    user = _user(engine, org, "user@example.com")
    parent = _department(engine, org, "parent")
    child = _department(engine, org, "child", parent_id=parent)
    team = _team(engine, org, "team")
    with pytest.raises(IntegrityError):
        _execute(engine, "DELETE FROM departments WHERE id = :id", id=parent)
    with pytest.raises(IntegrityError):
        _membership(engine, "department_memberships", org, child, user, status="revoked")
    with pytest.raises(IntegrityError):
        _membership(engine, "team_memberships", org, team, user, status="active", revoked=datetime.now(timezone.utc))
    _membership(engine, "department_memberships", org, child, user, status="revoked", revoked=datetime.now(timezone.utc))


def test_user_and_organization_deletion_actions_preserve_tenant_integrity(engine):
    org = _organization(engine, "Epsilon")
    creator = _user(engine, org, "creator@example.com")
    member = _user(engine, org, "member@example.com")
    department = _department(engine, org, "engineering")
    team = _team(engine, org, "platform")
    _membership(engine, "department_memberships", org, department, member, creator=creator)
    _membership(engine, "team_memberships", org, team, member, creator=creator)

    _execute(engine, "DELETE FROM users WHERE id = :id", id=creator)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT created_by_user_id FROM department_memberships")).scalar_one() is None
        assert connection.execute(text("SELECT created_by_user_id FROM team_memberships")).scalar_one() is None

    _execute(engine, "DELETE FROM users WHERE id = :id", id=member)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM department_memberships")).scalar_one() == 0
        assert connection.execute(text("SELECT count(*) FROM team_memberships")).scalar_one() == 0

    owner = _user(engine, org, "owner@example.com")
    _membership(engine, "department_memberships", org, department, owner)
    _membership(engine, "team_memberships", org, team, owner)
    _execute(engine, "DELETE FROM organizations WHERE id = :id", id=org)
    with engine.connect() as connection:
        for table in ("departments", "teams", "department_memberships", "team_memberships"):
            assert connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one() == 0


def test_downgrade_removes_only_structure_tables_and_reupgrade_succeeds(engine):
    config = _config()
    command.downgrade(config, PRIOR_REVISION)
    tables = set(inspect(engine).get_table_names(schema="public"))
    assert not {"departments", "teams", "department_memberships", "team_memberships"}.intersection(tables)
    assert {"organizations", "users", "documents", "document_chunks"}.issubset(tables)
    command.upgrade(config, "head")
    assert {"departments", "teams", "department_memberships", "team_memberships"}.issubset(set(inspect(engine).get_table_names(schema="public")))
