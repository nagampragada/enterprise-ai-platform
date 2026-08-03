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
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine import make_url

from infrastructure.db.base import Base
from infrastructure.db import models as db_models  # noqa: F401


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PROJECT_VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
TEST_DATABASE_URL_ENV_VAR = "TEST_DATABASE_URL"
DATABASE_URL_ENV_VAR = "DATABASE_URL"
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"


def _database_identity(database_url: str) -> tuple[str, str | None, int | None, str | None]:
    url = make_url(database_url)
    return url.drivername, url.host, url.port, url.database


def _required_env_url(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set for identity migration tests")
    return value


def _run_alembic_upgrade(database_url: str) -> None:
    environment = os.environ.copy()
    environment[DATABASE_URL_ENV_VAR] = database_url
    subprocess.run(
        [str(PROJECT_VENV_PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", "head"],
        check=True,
        cwd=str(PROJECT_ROOT),
        env=environment,
    )


def _alembic_config() -> Config:
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", _required_env_url(TEST_DATABASE_URL_ENV_VAR))
    return config


@pytest.fixture(scope="module")
def migrated_engine():
    test_database_url = _required_env_url(TEST_DATABASE_URL_ENV_VAR)
    development_database_url = os.environ.get(DATABASE_URL_ENV_VAR)
    if development_database_url and _database_identity(development_database_url) == _database_identity(test_database_url):
        raise RuntimeError("TEST_DATABASE_URL must point to a different database than DATABASE_URL")

    # Reset schemas explicitly so each test starts from a known baseline.
    # This avoids stale alembic_version tables in non-public schemas.
    # Module scope keeps this expensive step to one-time setup per test file.
    reset_engine = create_engine(test_database_url, future=True)

    with reset_engine.begin() as conn:
        current_user = conn.execute(text("SELECT current_user")).scalar_one()
        if current_user and current_user != "public":
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{current_user}" CASCADE'))
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))

    reset_engine.dispose()

    _run_alembic_upgrade(test_database_url)

    engine = create_engine(test_database_url, future=True)

    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(autouse=True)
def _clean_test_data(migrated_engine):
    # Keep seeded roles, but clear test-created rows before each test.
    with migrated_engine.begin() as conn:
        conn.execute(text("SET search_path TO public"))
        conn.execute(text("DELETE FROM authentication_sessions"))
        conn.execute(text("DELETE FROM user_roles"))
        conn.execute(text("DELETE FROM users"))
        conn.execute(text("DELETE FROM organization_settings"))
        conn.execute(text("DELETE FROM organizations"))
        conn.execute(text("DELETE FROM industries"))


def _table_names(engine) -> set[str]:
    with engine.connect() as conn:
        conn.execute(text("SET search_path TO public"))
        return set(inspect(conn).get_table_names(schema="public"))


def _fetch_all(engine, sql: str, **params):
    with engine.connect() as conn:
        conn.execute(text("SET search_path TO public"))
        return conn.execute(text(sql), params).all()


def _execute(engine, sql: str, **params) -> None:
    with engine.begin() as conn:
        conn.execute(text("SET search_path TO public"))
        conn.execute(text(sql), params)


def _create_organization(engine, *, name: str, slug: str) -> uuid.UUID:
    organization_id = uuid.uuid4()
    _execute(
        engine,
        """
        INSERT INTO organizations (id, name, slug)
        VALUES (:id, :name, :slug)
        """,
        id=organization_id,
        name=name,
        slug=slug,
    )
    return organization_id


def _create_user(
    engine,
    *,
    organization_id: uuid.UUID,
    email: str,
    normalized_email: str,
    display_name: str = "User",
    status: str = "active",
) -> uuid.UUID:
    user_id = uuid.uuid4()
    _execute(
        engine,
        """
        INSERT INTO users (
            id,
            organization_id,
            email,
            normalized_email,
            password_hash,
            display_name,
            status
        )
        VALUES (
            :id,
            :organization_id,
            :email,
            :normalized_email,
            :password_hash,
            :display_name,
            :status
        )
        """,
        id=user_id,
        organization_id=organization_id,
        email=email,
        normalized_email=normalized_email,
        password_hash="argon2id$demo-hash",
        display_name=display_name,
        status=status,
    )
    return user_id


def _seed_role_id(engine, role_name: str) -> uuid.UUID:
    with engine.connect() as conn:
        conn.execute(text("SET search_path TO public"))
        row = conn.execute(text("SELECT id FROM roles WHERE name = :name"), {"name": role_name}).one()
        return row.id


def test_upgrade_creates_all_identity_tables_and_seed_roles(migrated_engine) -> None:
    table_names = _table_names(migrated_engine)
    assert {
        "alembic_version",
        "industries",
        "organizations",
        "organization_settings",
        "roles",
        "users",
        "user_roles",
        "authentication_sessions",
    }.issubset(table_names)

    seed_roles = _fetch_all(
        migrated_engine,
        "SELECT name, is_system_role FROM roles ORDER BY name",
    )
    assert [(row.name, row.is_system_role) for row in seed_roles] == [
        ("employee", True),
        ("organization_admin", True),
    ]

    inspector = inspect(migrated_engine)
    organization_settings_checks = [
        str(constraint.get("sqltext", "")).lower()
        for constraint in inspector.get_check_constraints("organization_settings", schema="public")
    ]

    def _has_check(*fragments: str) -> bool:
        return any(all(fragment in check_sql for fragment in fragments) for check_sql in organization_settings_checks)

    def _has_retention_range_check() -> bool:
        for check_sql in organization_settings_checks:
            if "retention_days" not in check_sql:
                continue

            between_style = "between" in check_sql and "1" in check_sql and "3650" in check_sql
            bounds_style = ">=" in check_sql and "<=" in check_sql and "1" in check_sql and "3650" in check_sql
            if between_style or bounds_style:
                return True
        return False

    assert _has_check("default_locale", "btrim", "<>")
    assert _has_check("timezone", "btrim", "<>")
    assert _has_retention_range_check()
    assert _has_check("ai_model_name", "is null", "btrim", "<>")

    role_unique_constraints = {constraint["name"] for constraint in inspector.get_unique_constraints("roles", schema="public")}
    assert "uq_roles_name" in role_unique_constraints

    users_unique_constraints = {constraint["name"] for constraint in inspector.get_unique_constraints("users", schema="public")}
    assert {
        "uq_users_organization_id_normalized_email",
        "uq_users_organization_id_id",
    }.issubset(users_unique_constraints)

    auth_indexes = {index["name"] for index in inspector.get_indexes("authentication_sessions", schema="public")}
    assert {
        "ix_authentication_sessions_org_user_active",
        "ix_authentication_sessions_expires_at",
    }.issubset(auth_indexes)


def test_model_metadata_matches_migrated_schema(migrated_engine) -> None:
    inspector = inspect(migrated_engine)
    expected_tables = [
        "organization_settings",
        "roles",
        "users",
        "user_roles",
        "authentication_sessions",
    ]

    for table_name in expected_tables:
        assert table_name in Base.metadata.tables
        model_columns = list(Base.metadata.tables[table_name].columns.keys())
        db_columns = [column["name"] for column in inspector.get_columns(table_name, schema="public")]
        assert model_columns == db_columns


def test_roles_are_unique_and_reject_blank_names(migrated_engine) -> None:
    organization_id = _create_organization(migrated_engine, name="Acme", slug="acme")
    assert organization_id

    with pytest.raises(IntegrityError):
        _execute(
            migrated_engine,
            """
            INSERT INTO roles (id, name, is_system_role)
            VALUES (:id, :name, :is_system_role)
            """,
            id=uuid.uuid4(),
            name="employee",
            is_system_role=True,
        )

    with pytest.raises(IntegrityError):
        _execute(
            migrated_engine,
            """
            INSERT INTO roles (id, name, is_system_role)
            VALUES (:id, :name, :is_system_role)
            """,
            id=uuid.uuid4(),
            name="",
            is_system_role=True,
        )


def test_user_email_is_unique_per_organization(migrated_engine) -> None:
    org_a = _create_organization(migrated_engine, name="Org A", slug="org-a")
    org_b = _create_organization(migrated_engine, name="Org B", slug="org-b")

    _create_user(
        migrated_engine,
        organization_id=org_a,
        email="Alice@Example.com",
        normalized_email="alice@example.com",
        display_name="Alice A",
    )

    with pytest.raises(IntegrityError):
        _create_user(
            migrated_engine,
            organization_id=org_a,
            email="ALICE@EXAMPLE.COM",
            normalized_email="alice@example.com",
            display_name="Alice Duplicate",
        )

    _create_user(
        migrated_engine,
        organization_id=org_b,
        email="ALICE@EXAMPLE.COM",
        normalized_email="alice@example.com",
        display_name="Alice B",
    )


def test_cross_tenant_user_roles_and_sessions_fail(migrated_engine) -> None:
    org_a = _create_organization(migrated_engine, name="Org A", slug="org-a-cross")
    org_b = _create_organization(migrated_engine, name="Org B", slug="org-b-cross")
    user_a = _create_user(
        migrated_engine,
        organization_id=org_a,
        email="user-a@example.com",
        normalized_email="user-a@example.com",
        display_name="User A",
    )
    user_b = _create_user(
        migrated_engine,
        organization_id=org_b,
        email="user-b@example.com",
        normalized_email="user-b@example.com",
        display_name="User B",
    )
    employee_role_id = _seed_role_id(migrated_engine, "employee")

    with pytest.raises(IntegrityError):
        _execute(
            migrated_engine,
            """
            INSERT INTO user_roles (id, organization_id, user_id, role_id, assigned_at)
            VALUES (:id, :organization_id, :user_id, :role_id, :assigned_at)
            """,
            id=uuid.uuid4(),
            organization_id=org_b,
            user_id=user_a,
            role_id=employee_role_id,
            assigned_at=datetime.now(timezone.utc),
        )

    with pytest.raises(IntegrityError):
        _execute(
            migrated_engine,
            """
            INSERT INTO authentication_sessions (
                id,
                organization_id,
                user_id,
                refresh_token_hash,
                created_at,
                expires_at
            )
            VALUES (
                :id,
                :organization_id,
                :user_id,
                :refresh_token_hash,
                :created_at,
                :expires_at
            )
            """,
            id=uuid.uuid4(),
            organization_id=org_b,
            user_id=user_a,
            refresh_token_hash=b"token-a",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

    _execute(
        migrated_engine,
        """
        INSERT INTO user_roles (id, organization_id, user_id, role_id, assigned_at)
        VALUES (:id, :organization_id, :user_id, :role_id, :assigned_at)
        """,
        id=uuid.uuid4(),
        organization_id=org_a,
        user_id=user_a,
        role_id=employee_role_id,
        assigned_at=datetime.now(timezone.utc),
    )

    _execute(
        migrated_engine,
        """
        INSERT INTO authentication_sessions (
            id,
            organization_id,
            user_id,
            refresh_token_hash,
            created_at,
            expires_at
        )
        VALUES (
            :id,
            :organization_id,
            :user_id,
            :refresh_token_hash,
            :created_at,
            :expires_at
        )
        """,
        id=uuid.uuid4(),
        organization_id=org_b,
        user_id=user_b,
        refresh_token_hash=b"token-b",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )


def test_user_and_session_constraints_fail(migrated_engine) -> None:
    organization_id = _create_organization(migrated_engine, name="Org C", slug="org-c")
    user_id = _create_user(
        migrated_engine,
        organization_id=organization_id,
        email="user-c@example.com",
        normalized_email="user-c@example.com",
        display_name="User C",
    )

    with pytest.raises(IntegrityError):
        _execute(
            migrated_engine,
            """
            INSERT INTO users (
                id,
                organization_id,
                email,
                normalized_email,
                password_hash,
                display_name,
                status
            ) VALUES (
                :id,
                :organization_id,
                :email,
                :normalized_email,
                :password_hash,
                :display_name,
                :status
            )
            """,
            id=uuid.uuid4(),
            organization_id=organization_id,
            email="bad@example.com",
            normalized_email="bad@example.com",
            password_hash="argon2id$demo-hash",
            display_name="Bad User",
            status="pending",
        )

    _execute(
        migrated_engine,
        """
        INSERT INTO authentication_sessions (
            id,
            organization_id,
            user_id,
            refresh_token_hash,
            created_at,
            expires_at
        )
        VALUES (
            :id,
            :organization_id,
            :user_id,
            :refresh_token_hash,
            :created_at,
            :expires_at
        )
        """,
        id=uuid.uuid4(),
        organization_id=organization_id,
        user_id=user_id,
        refresh_token_hash=b"duplicate-token",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    with pytest.raises(IntegrityError):
        _execute(
            migrated_engine,
            """
            INSERT INTO authentication_sessions (
                id,
                organization_id,
                user_id,
                refresh_token_hash,
                created_at,
                expires_at
            )
            VALUES (
                :id,
                :organization_id,
                :user_id,
                :refresh_token_hash,
                :created_at,
                :expires_at
            )
            """,
            id=uuid.uuid4(),
            organization_id=organization_id,
            user_id=user_id,
            refresh_token_hash=b"duplicate-token",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

    with pytest.raises(IntegrityError):
        _execute(
            migrated_engine,
            """
            INSERT INTO authentication_sessions (
                id,
                organization_id,
                user_id,
                refresh_token_hash,
                created_at,
                expires_at
            )
            VALUES (
                :id,
                :organization_id,
                :user_id,
                :refresh_token_hash,
                :created_at,
                :expires_at
            )
            """,
            id=uuid.uuid4(),
            organization_id=organization_id,
            user_id=user_id,
            refresh_token_hash=b"duplicate-token",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=2),
        )

    with pytest.raises(IntegrityError):
        _execute(
            migrated_engine,
            """
            INSERT INTO authentication_sessions (
                id,
                organization_id,
                user_id,
                refresh_token_hash,
                created_at,
                expires_at
            )
            VALUES (
                :id,
                :organization_id,
                :user_id,
                :refresh_token_hash,
                :created_at,
                :expires_at
            )
            """,
            id=uuid.uuid4(),
            organization_id=organization_id,
            user_id=user_id,
            refresh_token_hash=b"expired-token",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        )


def test_downgrade_removes_identity_tables_and_preserves_base_tables(migrated_engine) -> None:
    config = _alembic_config()
    command.downgrade(config, "20260802_000001")

    remaining_tables = _table_names(migrated_engine)
    assert remaining_tables == {"alembic_version", "industries", "organizations"}

    command.upgrade(config, "head")