from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import CheckConstraint, create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.dialects.postgresql import dialect

import infrastructure.db.health as db_health
from infrastructure.db import models as db_models  # noqa: F401
from infrastructure.db.base import Base


ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
REVISION = "20260911_000025"
PRIOR_REVISION = "20260905_000024"
TABLE = "connector_sync_control_reservations"


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


def test_control_reservation_schema_matches_model_and_single_head(engine):
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

    assert inspector.get_pk_constraint(TABLE)["name"] == (
        "pk_connector_sync_control_reservations"
    )
    assert {item["name"] for item in inspector.get_foreign_keys(TABLE)} == {
        "fk_sync_control_reservations_organization",
        "fk_sync_control_reservations_creator_tenant",
        "fk_sync_control_reservations_job_tenant",
        "fk_sync_control_reservations_work_item_tenant",
    }
    assert {item["name"] for item in inspector.get_unique_constraints(TABLE)} == {
        "uq_sync_control_reservations_job",
        "uq_sync_control_reservations_owner_token_hash",
        "uq_sync_control_reservations_work_item",
    }
    checks = {item["name"]: item["sqltext"] for item in inspector.get_check_constraints(TABLE)}
    preparer = dialect().identifier_preparer
    model_checks = {
        preparer.format_constraint(constraint)
        for constraint in Base.metadata.tables[TABLE].constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert len(checks) == len(model_checks) == 11
    assert set(checks) == model_checks
    normalized = {name: value.casefold() for name, value in checks.items()}
    expected_fragments = {
        "state_valid": ("state", "job", "work_item", "released"),
        "owner_token_hash_valid": ("owner_token_hash", "0-9a-f", "64"),
        "target_source_ke": ("target_source_key_hash", "0-9a-f", "64"),
        "provider_identit": (
            "target_provider_blob_id",
            "target_provider_revision_id",
            "0-9a-f",
            "40",
            "64",
        ),
        "target_profile_f": ("target_profile_fingerprint", "0-9", "a-z"),
        "expiry_bounded": ("expires_at", "created_at", "00:05:00", "02:00:00"),
        "lifecycle_consistent": ("generation_id", "work_item_id", "handed_off_at", "released_at"),
        "planner_lease_st": ("planner_lease_id", "processor_lease_id", "job"),
        "processor_lease_": ("processor_lease_id", "planner_lease_id", "work_item"),
        "handoff_after_created": ("handed_off_at", "created_at"),
        "release_after_created": ("released_at", "created_at"),
    }
    for suffix, fragments in expected_fragments.items():
        definition = next(
            value for name, value in normalized.items() if suffix in name
        )
        assert all(fragment in definition for fragment in fragments)
    indexes = {item["name"]: item for item in inspector.get_indexes(TABLE)}
    assert set(indexes) >= {
        "ix_sync_control_reservations_job_live",
        "ix_sync_control_reservations_work_live",
    }
    assert all(
        indexes[name]["dialect_options"]["postgresql_where"] is not None
        for name in (
            "ix_sync_control_reservations_job_live",
            "ix_sync_control_reservations_work_live",
        )
    )
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == REVISION
        assert connection.scalar(text("SELECT count(*) FROM alembic_version")) == 1
        assert connection.scalar(text(f"SELECT count(*) FROM {TABLE}")) == 0


def test_control_reservation_upgrade_downgrade_upgrade_is_additive(
    engine, monkeypatch
):
    url = os.environ["TEST_DATABASE_URL"]
    identifiers = tuple(uuid.uuid4() for _ in range(7))
    organization_id, user_id, connector_id, space_id, scope_id, job_id, reservation_id = (
        identifiers
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO organizations (id,name,slug) "
                "VALUES (:id,'Reservation Transition',:slug)"
            ),
            {"id": organization_id, "slug": f"reservation-{organization_id}"},
        )
        connection.execute(
            text(
                "INSERT INTO users "
                "(id,organization_id,email,normalized_email,password_hash,display_name) "
                "VALUES (:id,:org,:email,:email,'hash','Reservation Creator')"
            ),
            {
                "id": user_id,
                "org": organization_id,
                "email": f"{user_id}@example.test",
            },
        )
        connection.execute(
            text(
                "INSERT INTO connectors "
                "(id,organization_id,connector_type,display_name,slug,status) "
                "VALUES (:id,:org,'github','Reservation Connector',:slug,'active')"
            ),
            {
                "id": connector_id,
                "org": organization_id,
                "slug": f"connector-{connector_id}",
            },
        )
        connection.execute(
            text(
                "INSERT INTO knowledge_spaces (id,organization_id,name,slug) "
                "VALUES (:id,:org,'Reservation Space',:slug)"
            ),
            {
                "id": space_id,
                "org": organization_id,
                "slug": f"space-{space_id}",
            },
        )
        connection.execute(
            text(
                "INSERT INTO connector_scopes "
                "(id,organization_id,connector_id,knowledge_space_id,display_name,"
                "slug,scope_type,external_scope_key,access_mode,status) VALUES "
                "(:id,:org,:connector,:space,'Reservation Scope',:slug,'repository',"
                ":key,'platform_managed','active')"
            ),
            {
                "id": scope_id,
                "org": organization_id,
                "connector": connector_id,
                "space": space_id,
                "slug": f"scope-{scope_id}",
                "key": f"github:repository:{scope_id.int}",
            },
        )
        connection.execute(
            text(
                "INSERT INTO connector_sync_jobs "
                "(id,organization_id,connector_id,connector_scope_id,mode,trigger_type,"
                "status,attempt_count,fencing_token,next_attempt_at,created_at,updated_at) "
                "VALUES (:id,:org,:connector,:scope,'incremental','manual','queued',"
                "0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
            ),
            {
                "id": job_id,
                "org": organization_id,
                "connector": connector_id,
                "scope": scope_id,
            },
        )
        connection.execute(
            text(
                f"INSERT INTO {TABLE} "
                "(id,organization_id,connector_id,connector_scope_id,sync_job_id,"
                "created_by_user_id,owner_token_hash,target_source_key_hash,"
                "target_provider_blob_id,target_provider_revision_id,"
                "target_profile_fingerprint,state,created_at,expires_at) VALUES "
                "(:id,:org,:connector,:scope,:job,:user,:token_hash,:source_hash,"
                ":blob,:revision,'test:profile','job',CURRENT_TIMESTAMP,"
                "CURRENT_TIMESTAMP + interval '1 hour')"
            ),
            {
                "id": reservation_id,
                "org": organization_id,
                "connector": connector_id,
                "scope": scope_id,
                "job": job_id,
                "user": user_id,
                "token_hash": "a" * 64,
                "source_hash": "b" * 64,
                "blob": "c" * 40,
                "revision": "d" * 40,
            },
        )
    engine.dispose()
    command.downgrade(_config(url), PRIOR_REVISION)
    downgraded = create_engine(url, future=True)
    try:
        inspector = inspect(downgraded)
        assert TABLE not in inspector.get_table_names(schema="public")
        assert "connector_sync_jobs" in inspector.get_table_names(schema="public")
        assert "connector_sync_file_work_items" in inspector.get_table_names(
            schema="public"
        )
        monkeypatch.setattr(db_health, "engine", downgraded)
        health = db_health.check_database_connection()
        assert health.healthy is True
        assert health.schema_compatible is True
        assert health.schema_current is False
        assert health.migration_required is True
        with downgraded.connect() as connection:
            assert connection.scalar(
                text("SELECT count(*) FROM connector_sync_jobs WHERE id=:id"),
                {"id": job_id},
            ) == 1
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
        with upgraded.connect() as connection:
            assert connection.scalar(text(f"SELECT count(*) FROM {TABLE}")) == 0
            assert connection.scalar(
                text("SELECT count(*) FROM connector_sync_jobs WHERE id=:id"),
                {"id": job_id},
            ) == 1
    finally:
        upgraded.dispose()
