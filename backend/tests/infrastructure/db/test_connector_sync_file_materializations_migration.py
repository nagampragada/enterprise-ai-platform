from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import configure_mappers

import infrastructure.db.health as db_health
from infrastructure.db import models as db_models  # noqa: F401
from infrastructure.db.base import Base


ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
TEST_URL = "TEST_DATABASE_URL"
DEV_URL = "DATABASE_URL"
REVISION = "20260911_000025"
PRIOR_REVISION = "20260828_000020"


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
    url = os.environ[TEST_URL]
    development = os.environ.get(DEV_URL)
    if development and _identity(development) == _identity(url):
        raise RuntimeError("test database must differ from development database")
    reset = create_engine(url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    reset.dispose()
    environment = os.environ.copy()
    environment[DEV_URL] = url
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


def test_materialization_schema_matches_models_and_single_head(engine) -> None:
    configure_mappers()
    inspector = inspect(engine)
    expected = {
        "connector_sync_file_materializations": {
            "indexes": {"ix_sync_file_materializations_generation"},
            "foreign_keys": {
                "fk_sync_file_materializations_generation_tenant",
                "fk_sync_file_materializations_work_tenant",
            },
        },
        "connector_sync_file_materialization_chunks": {
            "indexes": {"ix_sync_file_materialization_chunks_parent"},
            "foreign_keys": {"fk_sync_file_materialization_chunks_parent"},
        },
    }
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one() == REVISION
        assert connection.execute(
            text("SELECT count(*) FROM alembic_version")
        ).scalar_one() == 1
    assert expected.keys() <= set(inspector.get_table_names(schema="public"))
    for table, contract in expected.items():
        assert [
            column["name"] for column in inspector.get_columns(table, schema="public")
        ] == list(Base.metadata.tables[table].columns.keys())
        assert contract["indexes"] <= {
            index["name"] for index in inspector.get_indexes(table, schema="public")
        }
        assert contract["foreign_keys"] == {
            key["name"]
            for key in inspector.get_foreign_keys(table, schema="public")
        }
        model_constraints = {
            constraint.name
            for constraint in Base.metadata.tables[table].constraints
            if constraint.name
        }
        reflected_constraints = {
            inspector.get_pk_constraint(table, schema="public")["name"],
            *(
                item["name"]
                for item in inspector.get_unique_constraints(table, schema="public")
            ),
            *(
                item["name"]
                for item in inspector.get_check_constraints(table, schema="public")
            ),
            *(
                item["name"]
                for item in inspector.get_foreign_keys(table, schema="public")
            ),
        }
        assert model_constraints == reflected_constraints

    chunks = Base.metadata.tables["connector_sync_file_materialization_chunks"]
    assert str(chunks.c.embedding.type) == "VECTOR(1536)"
    assert not {
        "source_item_id",
        "document_id",
        "document_version_id",
        "indexing_state_id",
    }.intersection(Base.metadata.tables["connector_sync_file_materializations"].columns)


def test_materialization_revision_downgrades_without_removing_phase1_ledger(
    engine,
    monkeypatch,
) -> None:
    url = os.environ[TEST_URL]
    engine.dispose()
    command.downgrade(_config(url), PRIOR_REVISION)
    downgraded = create_engine(url, future=True)
    try:
        tables = set(inspect(downgraded).get_table_names(schema="public"))
        assert "connector_sync_file_materializations" not in tables
        assert "connector_sync_file_materialization_chunks" not in tables
        assert "connector_sync_generations" in tables
        assert "connector_sync_file_work_items" in tables
        with downgraded.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == PRIOR_REVISION
        monkeypatch.setattr(db_health, "engine", downgraded)
        compatibility = db_health.check_database_connection()
        assert compatibility.healthy is True
        assert compatibility.schema_compatible is False
        assert compatibility.schema_current is False
        assert compatibility.migration_required is False
    finally:
        downgraded.dispose()
    command.upgrade(_config(url), REVISION)
