from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

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
TEST_URL = "TEST_DATABASE_URL"
DEV_URL = "DATABASE_URL"
REVISION = "20260902_000022"
PRIOR_REVISION = "20260831_000021"
HEAD_REVISION = "20260904_000023"
TABLE = "connector_sync_organization_claim_schedules"
SEQUENCE = "connector_sync_org_fair_claim_seq"


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


def test_fairness_schema_matches_models_and_is_the_single_head(engine) -> None:
    inspector = inspect(engine)
    assert TABLE in inspector.get_table_names(schema="public")
    reflected = inspector.get_columns(TABLE, schema="public")
    model = list(Base.metadata.tables[TABLE].columns)
    assert [column["name"] for column in reflected] == [column.name for column in model]
    for model_column, database_column in zip(model, reflected, strict=True):
        assert model_column.type._type_affinity is database_column["type"]._type_affinity
        assert model_column.nullable == database_column["nullable"]
        if hasattr(model_column.type, "timezone"):
            assert model_column.type.timezone == database_column["type"].timezone
    primary_key = inspector.get_pk_constraint(TABLE, schema="public")
    assert primary_key["name"] == "pk_connector_sync_org_claim_schedules"
    assert primary_key["constrained_columns"] == ["organization_id"]
    foreign_keys = inspector.get_foreign_keys(TABLE, schema="public")
    assert len(foreign_keys) == 1
    assert foreign_keys[0]["name"] == "fk_sync_org_claim_schedules_organization"
    assert foreign_keys[0]["constrained_columns"] == ["organization_id"]
    assert foreign_keys[0]["referred_table"] == "organizations"
    assert foreign_keys[0]["referred_columns"] == ["id"]
    assert foreign_keys[0]["options"] == {"ondelete": "CASCADE"}
    uniques = inspector.get_unique_constraints(TABLE, schema="public")
    assert len(uniques) == 1
    assert uniques[0]["name"] == "uq_sync_org_claim_schedules_sequence"
    assert uniques[0]["column_names"] == ["last_claim_sequence"]
    checks = inspector.get_check_constraints(TABLE)
    assert len(checks) == 4
    assert all(
        item["name"].startswith("ck_connector_sync_organization_claim_schedules_")
        for item in checks
    )
    assert {item["sqltext"] for item in checks} == {
        str(constraint.sqltext)
        for constraint in Base.metadata.tables[TABLE].constraints
        if isinstance(constraint, CheckConstraint)
    }
    indexes = {item["name"]: item for item in inspector.get_indexes(TABLE)}
    assert indexes["ix_sync_org_claim_schedules_fair_order"]["column_names"] == [
        "last_claim_sequence",
        "organization_id",
    ]
    assert indexes["ix_sync_org_claim_schedules_fair_order"]["unique"] is False
    work_indexes = {
        item["name"]: item
        for item in inspector.get_indexes("connector_sync_file_work_items")
    }
    assert work_indexes["ix_sync_file_work_fair_eligible"]["column_names"] == [
        "organization_id",
        "profile_fingerprint",
        "next_attempt_at",
        "generation_id",
        "id",
    ]
    assert (
        work_indexes["ix_sync_file_work_fair_eligible"]["dialect_options"][
            "postgresql_where"
        ]
        is not None
    )
    fair_predicate = str(
        work_indexes["ix_sync_file_work_fair_eligible"]["dialect_options"][
            "postgresql_where"
        ]
    )
    assert "cancel_requested_at IS NULL" in fair_predicate
    assert "attempt_count < max_attempts" in fair_predicate
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one() == HEAD_REVISION
        assert connection.execute(
            text("SELECT count(*) FROM alembic_version")
        ).scalar_one() == 1
        assert connection.execute(
            text("SELECT count(*) FROM pg_class WHERE relkind='S' AND relname=:name"),
            {"name": SEQUENCE},
        ).scalar_one() == 1
        assert connection.execute(
            text("SELECT data_type FROM pg_sequences WHERE sequencename=:name"),
            {"name": SEQUENCE},
        ).scalar_one() == "bigint"
        assert connection.execute(text(f"SELECT count(*) FROM {TABLE}")).scalar_one() == 0


def test_fairness_revision_downgrade_and_upgrade_are_isolated(engine, monkeypatch) -> None:
    url = os.environ[TEST_URL]
    organization_id = uuid4()
    now = datetime(2026, 9, 2, 12, tzinfo=timezone.utc)
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO organizations (id,name,slug) VALUES (:id,:name,:slug)"),
            {
                "id": organization_id,
                "name": "Fairness downgrade",
                "slug": f"fairness-downgrade-{organization_id}",
            },
        )
        sequence = connection.execute(
            text("SELECT nextval('connector_sync_org_fair_claim_seq')")
        ).scalar_one()
        connection.execute(
            text(
                f"""INSERT INTO {TABLE}
                (organization_id,last_claim_sequence,claim_count,last_claimed_at,created_at,updated_at)
                VALUES (:organization_id,:sequence,1,:now,:now,:now)"""
            ),
            {"organization_id": organization_id, "sequence": sequence, "now": now},
        )
    engine.dispose()
    command.downgrade(_config(url), PRIOR_REVISION)
    downgraded = create_engine(url, future=True)
    try:
        inspector = inspect(downgraded)
        assert TABLE not in inspector.get_table_names(schema="public")
        assert "connector_sync_file_work_items" in inspector.get_table_names(
            schema="public"
        )
        assert "ix_sync_file_work_fair_eligible" not in {
            item["name"]
            for item in inspector.get_indexes("connector_sync_file_work_items")
        }
        with downgraded.connect() as connection:
            assert connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one() == PRIOR_REVISION
            assert connection.execute(
                text("SELECT count(*) FROM pg_class WHERE relkind='S' AND relname=:name"),
                {"name": SEQUENCE},
            ).scalar_one() == 0
            assert connection.execute(
                text("SELECT count(*) FROM organizations WHERE id=:id"),
                {"id": organization_id},
            ).scalar_one() == 1
        monkeypatch.setattr(db_health, "engine", downgraded)
        compatibility = db_health.check_database_connection()
        assert compatibility.healthy is True
        assert compatibility.schema_compatible is False
        assert compatibility.schema_current is False
        assert compatibility.migration_required is False
    finally:
        downgraded.dispose()
    command.upgrade(_config(url), "head")
