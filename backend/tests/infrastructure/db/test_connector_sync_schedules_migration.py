from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from infrastructure.db import models as db_models  # noqa: F401
from infrastructure.db.base import Base

ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
PRIOR = "20260823_000014"
TABLE = "connector_sync_schedules"
COLUMNS = [
    "id", "organization_id", "connector_id", "connector_scope_id", "status",
    "interval_seconds", "next_run_at", "last_due_at", "last_enqueued_at", "last_job_id",
    "pause_reason_code", "paused_at", "created_by_user_id", "created_at", "updated_at",
]


def _identity(url: str):
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database


def _config() -> Config:
    config = Config(str(INI))
    config.set_main_option("sqlalchemy.url", os.environ["TEST_DATABASE_URL"])
    return config


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
        check=True, cwd=str(ROOT), env=environment,
    )
    value = create_engine(url, future=True)
    try:
        yield value
    finally:
        value.dispose()


def test_schedule_schema_matches_orm_and_tenant_contract(engine):
    inspector = inspect(engine)
    assert len(Base.metadata.tables) == 42
    assert len(inspector.get_table_names(schema="public")) == 43
    reflected = inspector.get_columns(TABLE, schema="public")
    model = list(Base.metadata.tables[TABLE].columns)
    assert [item["name"] for item in reflected] == COLUMNS
    assert [item.name for item in model] == COLUMNS
    for model_column, database_column in zip(model, reflected, strict=True):
        assert model_column.type._type_affinity is database_column["type"]._type_affinity
        assert model_column.nullable == database_column["nullable"]
        if hasattr(model_column.type, "timezone"):
            assert model_column.type.timezone == database_column["type"].timezone
    uniques = {
        item["name"]: item["column_names"]
        for item in inspector.get_unique_constraints(TABLE, schema="public")
    }
    assert uniques == {
        "uq_sync_schedules_organization_id_id": ["organization_id", "id"],
        "uq_sync_schedules_scope": ["organization_id", "connector_id", "connector_scope_id"],
    }
    foreign_keys = {
        item["name"]: (item["constrained_columns"], item["referred_table"], item["options"]["ondelete"])
        for item in inspector.get_foreign_keys(TABLE, schema="public")
    }
    assert foreign_keys == {
        "fk_sync_schedules_organization": (["organization_id"], "organizations", "CASCADE"),
        "fk_sync_schedules_scope_tenant": (
            ["organization_id", "connector_id", "connector_scope_id"], "connector_scopes", "CASCADE"
        ),
        "fk_sync_schedules_last_job_tenant": (
            ["organization_id", "connector_id", "connector_scope_id", "last_job_id"],
            "connector_sync_jobs", "SET NULL (last_job_id)",
        ),
        "fk_sync_schedules_creator_tenant": (
            ["organization_id", "created_by_user_id"], "users", "SET NULL (created_by_user_id)"
        ),
    }
    indexes = {item["name"]: item for item in inspector.get_indexes(TABLE, schema="public")}
    assert indexes["ix_sync_schedules_due"]["column_names"] == ["next_run_at", "id"]
    assert indexes["ix_sync_schedules_due"]["dialect_options"]["postgresql_where"] is not None
    assert indexes["ix_sync_schedules_org_scope"]["column_names"] == [
        "organization_id", "connector_scope_id"
    ]
    assert indexes["ix_sync_schedules_status_next"]["column_names"] == ["status", "next_run_at"]
    assert indexes["ix_sync_schedules_last_job"]["column_names"] == ["organization_id", "last_job_id"]


def test_schedule_constraints_and_delete_actions(engine):
    with engine.begin() as connection:
        org, user, connector, space, scope = [__import__("uuid").uuid4() for _ in range(5)]
        connection.execute(text("INSERT INTO organizations(id,name,slug) VALUES (:id,'Org',:slug)"), {"id": org, "slug": str(org)})
        connection.execute(text("""INSERT INTO users(id,organization_id,email,normalized_email,password_hash,display_name)
            VALUES (:id,:org,:email,:email,'hash','Admin')"""), {"id": user, "org": org, "email": f"{user}@example.com"})
        connection.execute(text("""INSERT INTO connectors(id,organization_id,connector_type,display_name,slug,status)
            VALUES (:id,:org,'local_folder','Local',:slug,'active')"""), {"id": connector, "org": org, "slug": str(connector)})
        connection.execute(text("INSERT INTO knowledge_spaces(id,organization_id,name,slug) VALUES (:id,:org,'Space',:slug)"), {"id": space, "org": org, "slug": str(space)})
        connection.execute(text("""INSERT INTO connector_scopes
            (id,organization_id,connector_id,knowledge_space_id,display_name,slug,scope_type,external_scope_key,access_mode,status)
            VALUES (:id,:org,:connector,:space,'Scope',:slug,'folder','C:/safe','platform_managed','active')"""),
            {"id": scope, "org": org, "connector": connector, "space": space, "slug": str(scope)})
        values = {"id": __import__("uuid").uuid4(), "org": org, "connector": connector, "scope": scope, "user": user}
        connection.execute(text("""INSERT INTO connector_sync_schedules
            (id,organization_id,connector_id,connector_scope_id,status,interval_seconds,next_run_at,created_by_user_id)
            VALUES (:id,:org,:connector,:scope,'active',900,CURRENT_TIMESTAMP,:user)"""), values)
        connection.execute(text("DELETE FROM users WHERE id=:user"), values)
        assert connection.execute(text("SELECT created_by_user_id FROM connector_sync_schedules WHERE id=:id"), values).scalar_one() is None
        job_id = __import__("uuid").uuid4()
        connection.execute(text("""INSERT INTO connector_sync_jobs
            (id,organization_id,connector_id,connector_scope_id,mode,trigger_type,status,next_attempt_at)
            VALUES (:job,:org,:connector,:scope,'incremental','scheduled','queued',CURRENT_TIMESTAMP)"""),
            {**values, "job": job_id})
        connection.execute(
            text("UPDATE connector_sync_schedules SET last_job_id=:job,last_due_at=CURRENT_TIMESTAMP,last_enqueued_at=CURRENT_TIMESTAMP WHERE id=:id"),
            {**values, "job": job_id},
        )
        connection.execute(text("DELETE FROM connector_sync_jobs WHERE id=:job"), {"job": job_id})
        assert connection.execute(text("SELECT last_job_id FROM connector_sync_schedules WHERE id=:id"), values).scalar_one() is None
        connection.execute(text("DELETE FROM connector_scopes WHERE id=:scope"), values)
        assert connection.execute(text("SELECT count(*) FROM connector_sync_schedules")).scalar_one() == 0


@pytest.mark.parametrize(
    ("status", "interval", "paused_at", "reason"),
    (
        ("active", 899, None, None),
        ("active", 2_592_001, None, None),
        ("invalid", 900, None, None),
        ("active", 900, "now", "administrator_paused"),
        ("paused", 900, None, None),
        ("paused", 900, "now", "unsafe reason"),
    ),
)
def test_schedule_interval_status_and_pause_constraints(engine, status, interval, paused_at, reason):
    with engine.begin() as connection:
        org, connector, space, scope = [__import__("uuid").uuid4() for _ in range(4)]
        connection.execute(text("INSERT INTO organizations(id,name,slug) VALUES (:id,'Org',:slug)"), {"id": org, "slug": str(org)})
        connection.execute(text("""INSERT INTO connectors(id,organization_id,connector_type,display_name,slug)
            VALUES (:id,:org,'local_folder','Local',:slug)"""), {"id": connector, "org": org, "slug": str(connector)})
        connection.execute(text("INSERT INTO knowledge_spaces(id,organization_id,name,slug) VALUES (:id,:org,'Space',:slug)"), {"id": space, "org": org, "slug": str(space)})
        connection.execute(text("""INSERT INTO connector_scopes
            (id,organization_id,connector_id,knowledge_space_id,display_name,slug,scope_type,external_scope_key,access_mode)
            VALUES (:id,:org,:connector,:space,'Scope',:slug,'folder','C:/safe','platform_managed')"""),
            {"id": scope, "org": org, "connector": connector, "space": space, "slug": str(scope)})
        with pytest.raises(IntegrityError):
            with connection.begin_nested():
                connection.execute(text("""INSERT INTO connector_sync_schedules
                    (id,organization_id,connector_id,connector_scope_id,status,interval_seconds,next_run_at,paused_at,pause_reason_code)
                    VALUES (:id,:org,:connector,:scope,:status,:interval,CURRENT_TIMESTAMP,
                        CASE WHEN :paused='now' THEN CURRENT_TIMESTAMP ELSE NULL END,:reason)"""),
                    {"id": __import__("uuid").uuid4(), "org": org, "connector": connector,
                     "scope": scope, "status": status, "interval": interval,
                     "paused": paused_at, "reason": reason})


def test_downgrade_removes_only_schedule_table_and_reupgrade_succeeds(engine):
    before = set(inspect(engine).get_table_names(schema="public"))
    command.downgrade(_config(), "20260824_000015")
    at_schedule_head = set(inspect(engine).get_table_names(schema="public"))
    assert before - at_schedule_head == {
        "connector_credentials", "oauth_authorization_transactions", "github_app_installations"
    }
    command.downgrade(_config(), PRIOR)
    after = set(inspect(engine).get_table_names(schema="public"))
    assert at_schedule_head - after == {TABLE}
    assert "connector_sync_jobs" in after and len(after) == 39
    command.upgrade(_config(), "head")
    assert TABLE in inspect(engine).get_table_names(schema="public")
