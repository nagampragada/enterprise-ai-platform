from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PROJECT_VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
TEST_DATABASE_URL_ENV_VAR = "TEST_DATABASE_URL"
DATABASE_URL_ENV_VAR = "DATABASE_URL"
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
PRIOR_REVISION = "20260814_000003"


def _database_identity(database_url: str) -> tuple[str, str | None, int | None, str | None]:
    url = make_url(database_url)
    return url.drivername, url.host, url.port, url.database


def _required_env_url(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set for pgvector migration tests")
    return value


def _run_alembic_upgrade(database_url: str, revision: str) -> None:
    environment = os.environ.copy()
    environment[DATABASE_URL_ENV_VAR] = database_url
    subprocess.run(
        [str(PROJECT_VENV_PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", revision],
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

    reset_engine = create_engine(test_database_url, future=True)
    with reset_engine.begin() as conn:
        current_user = conn.execute(text("SELECT current_user")).scalar_one()
        if current_user and current_user != "public":
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{current_user}" CASCADE'))
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.execute(text("DROP EXTENSION IF EXISTS vector"))
    reset_engine.dispose()

    _run_alembic_upgrade(test_database_url, PRIOR_REVISION)
    engine = create_engine(test_database_url, future=True)
    try:
        yield engine
    finally:
        engine.dispose()


def _extension_version(engine) -> str | None:
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        ).scalar_one_or_none()


def test_vector_extension_is_absent_before_new_migration(migrated_engine) -> None:
    assert _extension_version(migrated_engine) is None


def test_upgrade_enables_vector_and_preserves_existing_tables(migrated_engine) -> None:
    config = _alembic_config()
    command.upgrade(config, "head")

    extension_version = _extension_version(migrated_engine)
    assert extension_version is not None
    assert extension_version.strip() != ""

    tables = set(inspect(migrated_engine).get_table_names(schema="public"))
    assert {"industries", "organizations", "users", "authentication_sessions", "documents"}.issubset(tables)


def test_reapplying_upgrade_is_idempotent(migrated_engine) -> None:
    config = _alembic_config()
    command.upgrade(config, "head")
    version_before = _extension_version(migrated_engine)

    command.upgrade(config, "head")

    assert _extension_version(migrated_engine) == version_before


def test_downgrade_retains_shared_vector_extension(migrated_engine) -> None:
    config = _alembic_config()
    command.upgrade(config, "head")
    command.downgrade(config, PRIOR_REVISION)

    assert _extension_version(migrated_engine) is not None
    assert "documents" in inspect(migrated_engine).get_table_names(schema="public")

    command.upgrade(config, "head")