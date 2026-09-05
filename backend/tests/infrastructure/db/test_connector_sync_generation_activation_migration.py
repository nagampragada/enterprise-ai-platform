from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import CheckConstraint, create_engine, inspect, text
from sqlalchemy.engine import make_url

import infrastructure.db.health as db_health
from infrastructure.db import models as db_models  # noqa: F401
from infrastructure.db.base import Base


ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
REVISION = "20260905_000024"
PRIOR_REVISION = "20260902_000022"
TABLE = "connector_sync_generation_activations"


def _identity(url: str) -> tuple[object, ...]:
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database, value.query


def _config(url: str) -> Config:
    value = Config(str(INI))
    value.set_main_option("script_location", str(ROOT / "alembic"))
    value.set_main_option("sqlalchemy.url", url)
    return value


@pytest.fixture(scope="module")
def engine():
    url = os.environ["TEST_DATABASE_URL"]
    development = os.environ.get("DATABASE_URL")
    if development and _identity(development) == _identity(url):
        raise RuntimeError("test database must differ from development database")
    reset = create_engine(url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    reset.dispose()
    environment = os.environ.copy()
    environment["DATABASE_URL"] = url
    subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(INI), "upgrade", "head"],
        check=True,
        cwd=str(ROOT),
        env=environment,
    )
    value = create_engine(url, future=True)
    try:
        yield value
    finally:
        value.dispose()


def test_activation_schema_matches_models_and_is_single_head(engine) -> None:
    inspector = inspect(engine)
    assert TABLE in inspector.get_table_names(schema="public")
    reflected = inspector.get_columns(TABLE, schema="public")
    model = list(Base.metadata.tables[TABLE].columns)
    assert [column["name"] for column in reflected] == [column.name for column in model]
    for model_column, database_column in zip(model, reflected, strict=True):
        assert model_column.type._type_affinity is database_column["type"]._type_affinity
        assert model_column.nullable == database_column["nullable"]
    assert inspector.get_pk_constraint(TABLE)["name"] == (
        "pk_connector_sync_generation_activations"
    )
    assert {item["name"] for item in inspector.get_foreign_keys(TABLE)} == {
        "fk_sync_generation_activations_scope_tenant",
        "fk_sync_generation_activations_generation_tenant",
    }
    assert {item["name"] for item in inspector.get_unique_constraints(TABLE)} == {
        "uq_sync_generation_activations_generation"
    }
    checks = inspector.get_check_constraints(TABLE)
    assert len(checks) == 7
    assert len(
        {
            constraint.name
            for constraint in Base.metadata.tables[TABLE].constraints
            if isinstance(constraint, CheckConstraint)
        }
    ) == 7
    definitions = " ".join(item["sqltext"] for item in checks)
    for required in (
        "active", "retired", "repository_identity", "commit_object_id",
        "profile_fingerprint", "retired_at", "activated_at", "updated_at", "created_at",
    ):
        assert required in definitions
    indexes = {item["name"]: item for item in inspector.get_indexes(TABLE)}
    assert indexes["uq_sync_generation_activations_active_scope"]["unique"] is True
    assert indexes["uq_sync_generation_activations_active_scope"]["dialect_options"][
        "postgresql_where"
    ] is not None
    assert indexes["ix_sync_generation_activations_scope_history"]["column_names"] == [
        "organization_id",
        "connector_scope_id",
        "activated_at",
        "id",
    ]
    with engine.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == REVISION
        assert connection.execute(text("SELECT count(*) FROM alembic_version")).scalar_one() == 1
        assert connection.execute(text(f"SELECT count(*) FROM {TABLE}")).scalar_one() == 0


def test_activation_revision_downgrade_upgrade_preserves_predecessor(engine, monkeypatch) -> None:
    url = os.environ["TEST_DATABASE_URL"]
    engine.dispose()
    command.downgrade(_config(url), PRIOR_REVISION)
    downgraded = create_engine(url, future=True)
    try:
        assert TABLE not in inspect(downgraded).get_table_names(schema="public")
        assert "connector_sync_generations" in inspect(downgraded).get_table_names(schema="public")
        monkeypatch.setattr(db_health, "engine", downgraded)
        health = db_health.check_database_connection()
        assert health.healthy is True
        assert health.schema_compatible is False
        assert health.schema_current is False
        assert health.migration_required is False
    finally:
        downgraded.dispose()
    command.upgrade(_config(url), REVISION)
    upgraded = create_engine(url, future=True)
    try:
        assert TABLE in inspect(upgraded).get_table_names(schema="public")
        monkeypatch.setattr(db_health, "engine", upgraded)
        health = db_health.check_database_connection()
        assert health.schema_compatible is True
        assert health.schema_current is True
        assert health.migration_required is False
    finally:
        upgraded.dispose()
