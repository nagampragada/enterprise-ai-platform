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
from sqlalchemy.orm import Session

import infrastructure.db.health as db_health
from infrastructure.db import models as db_models  # noqa: F401
from infrastructure.db.base import Base
from infrastructure.repositories.permission_aware_document_chunk_search_repository import (
    PermissionAwareDocumentChunkSearchRepository,
)


ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
REVISION = "20260905_000024"
PRIOR_REVISION = "20260904_000023"
OBSERVATIONS = "connector_sync_generation_observations"
GENERATIONS = "connector_sync_generations"
NEW_GENERATION_COLUMNS = {
    "manifest_schema_version",
    "reconciliation_started_at",
    "reconciliation_completed_at",
    "reconciled_membership_count",
    "reconciled_source_count",
    "reconciled_document_count",
}


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


def test_reconciliation_schema_matches_models_and_has_one_head(engine) -> None:
    inspector = inspect(engine)
    assert OBSERVATIONS in inspector.get_table_names(schema="public")
    reflected = inspector.get_columns(OBSERVATIONS, schema="public")
    model = list(Base.metadata.tables[OBSERVATIONS].columns)
    assert [column["name"] for column in reflected] == [column.name for column in model]
    for model_column, database_column in zip(model, reflected, strict=True):
        assert model_column.type._type_affinity is database_column["type"]._type_affinity
        assert model_column.nullable == database_column["nullable"]
    assert inspector.get_pk_constraint(OBSERVATIONS)["name"] == (
        "pk_connector_sync_generation_observations"
    )
    assert {item["name"] for item in inspector.get_foreign_keys(OBSERVATIONS)} == {
        "fk_sync_generation_observations_generation_tenant"
    }
    assert {item["name"] for item in inspector.get_unique_constraints(OBSERVATIONS)} == {
        "uq_sync_generation_observations_generation_id",
        "uq_sync_generation_observations_source",
    }
    model_checks = {
        constraint.name
        for constraint in Base.metadata.tables[OBSERVATIONS].constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert len(model_checks) == len(inspector.get_check_constraints(OBSERVATIONS)) == 11
    assert {
        item["name"] for item in inspector.get_indexes(OBSERVATIONS)
    } >= {"ix_sync_generation_observations_scope_path"}

    generation_columns = {
        item["name"] for item in inspector.get_columns(GENERATIONS, schema="public")
    }
    assert NEW_GENERATION_COLUMNS <= generation_columns
    definitions = " ".join(
        item["sqltext"] for item in inspector.get_check_constraints(GENERATIONS)
    )
    for required in (
        "manifest_schema_version",
        "reconciliation_eligible",
        "reconciliation_started_at",
        "reconciliation_completed_at",
        "reconciliation_eligible_at",
        "reconciled_membership_count",
    ):
        assert required in definitions
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == REVISION
        assert connection.scalar(text("SELECT count(*) FROM alembic_version")) == 1
        assert connection.scalar(text(f"SELECT count(*) FROM {OBSERVATIONS}")) == 0


def test_reconciliation_revision_schema_round_trip_preserves_transition_compatibility(
    engine, monkeypatch
) -> None:
    url = os.environ["TEST_DATABASE_URL"]
    engine.dispose()
    command.downgrade(_config(url), PRIOR_REVISION)
    downgraded = create_engine(url, future=True)
    try:
        inspector = inspect(downgraded)
        assert OBSERVATIONS not in inspector.get_table_names(schema="public")
        assert NEW_GENERATION_COLUMNS.isdisjoint(
            {item["name"] for item in inspector.get_columns(GENERATIONS)}
        )
        monkeypatch.setattr(db_health, "engine", downgraded)
        health = db_health.check_database_connection()
        assert health.healthy is True
        assert health.schema_compatible is True
        assert health.schema_current is False
        assert health.migration_required is True
        with downgraded.begin() as connection:
            organization_id, user_id, connector_id, space_id, scope_id, job_id, generation_id = (
                uuid.uuid4() for _ in range(7)
            )
            connection.execute(
                text(
                    "INSERT INTO organizations (id,name,slug) "
                    "VALUES (:id,'Transition','transition')"
                ),
                {"id": organization_id},
            )
            connection.execute(
                text(
                    "INSERT INTO users "
                    "(id,organization_id,email,normalized_email,password_hash,display_name) "
                    "VALUES (:id,:org,:email,:email,'hash','Transition')"
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
                    "VALUES (:id,:org,'github','Transition','transition','active')"
                ),
                {"id": connector_id, "org": organization_id},
            )
            connection.execute(
                text(
                    "INSERT INTO knowledge_spaces (id,organization_id,name,slug) "
                    "VALUES (:id,:org,'Transition','transition')"
                ),
                {"id": space_id, "org": organization_id},
            )
            connection.execute(
                text(
                    "INSERT INTO connector_scopes "
                    "(id,organization_id,connector_id,knowledge_space_id,display_name,"
                    "slug,scope_type,external_scope_key,access_mode,status) VALUES "
                    "(:id,:org,:connector,:space,'Transition','transition','repository',"
                    "'github:repository:123','platform_managed','active')"
                ),
                {
                    "id": scope_id,
                    "org": organization_id,
                    "connector": connector_id,
                    "space": space_id,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO connector_sync_jobs "
                    "(id,organization_id,connector_id,connector_scope_id,mode,trigger_type,"
                    "status,attempt_count,fencing_token,next_attempt_at,completed_at,"
                    "created_at,updated_at) "
                    "VALUES (:id,:org,:connector,:scope,'incremental','manual','succeeded',"
                    "1,1,NULL,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
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
                    "INSERT INTO connector_sync_generations "
                    "(id,organization_id,connector_id,connector_scope_id,sync_job_id,"
                    "provider_key,repository_identity,branch_name,commit_object_id,"
                    "root_tree_object_id,profile_fingerprint,status,discovery_complete,"
                    "discovery_completed_at,reconciliation_eligible,resync_required,"
                    "items_discovered,items_registered,declared_bytes,created_at,updated_at,"
                    "terminal_at) VALUES (:id,:org,:connector,:scope,:job,'github',"
                    "'github:repository:123','main',:commit,:tree,'test:profile',"
                    "'completed',true,CURRENT_TIMESTAMP,false,false,0,0,0,"
                    "CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"
                ),
                {
                    "id": generation_id,
                    "org": organization_id,
                    "connector": connector_id,
                    "scope": scope_id,
                    "job": job_id,
                    "commit": "a" * 40,
                    "tree": "b" * 40,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO connector_sync_generation_activations "
                    "(id,organization_id,connector_id,connector_scope_id,generation_id,"
                    "repository_identity,commit_object_id,profile_fingerprint,status,"
                    "activated_at,created_at,updated_at) VALUES "
                    "(:id,:org,:connector,:scope,:generation,'github:repository:123',"
                    ":commit,'test:profile','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,"
                    "CURRENT_TIMESTAMP)"
                ),
                {
                    "id": uuid.uuid4(),
                    "org": organization_id,
                    "connector": connector_id,
                    "scope": scope_id,
                    "generation": generation_id,
                    "commit": "a" * 40,
                },
            )
        with Session(downgraded) as session:
            assert PermissionAwareDocumentChunkSearchRepository(session).search(
                organization_id,
                user_id,
                [0.0] * 1536,
                "test:model:1536",
                10,
                source_item_types=("file",),
            ) == ()
    finally:
        downgraded.dispose()
    command.upgrade(_config(url), REVISION)
    upgraded = create_engine(url, future=True)
    try:
        assert OBSERVATIONS in inspect(upgraded).get_table_names(schema="public")
        monkeypatch.setattr(db_health, "engine", upgraded)
        health = db_health.check_database_connection()
        assert health.schema_compatible is True
        assert health.schema_current is True
        assert health.migration_required is False
        with upgraded.connect() as connection:
            # This is the exact predecessor-era generation projection.  Extra
            # Slice 5 columns and the observation table must not break code
            # deployed before the migration.
            predecessor_rows = connection.execute(
                text(
                    "SELECT id,organization_id,connector_id,connector_scope_id,"
                    "sync_job_id,provider_key,repository_identity,branch_name,"
                    "commit_object_id,root_tree_object_id,profile_fingerprint,status,"
                    "discovery_complete,discovery_completed_at,reconciliation_eligible,"
                    "reconciliation_eligible_at,resync_required,resync_requested_at,"
                    "items_discovered,items_registered,declared_bytes,created_at,"
                    "updated_at,terminal_at FROM connector_sync_generations"
                )
            ).all()
            assert len(predecessor_rows) == 1
            assert connection.execute(
                text(
                    "SELECT manifest_schema_version,reconciliation_eligible,"
                    "reconciliation_started_at,reconciliation_completed_at,"
                    "reconciled_membership_count,reconciled_source_count,"
                    "reconciled_document_count FROM connector_sync_generations "
                    "WHERE id=:id"
                ),
                {"id": generation_id},
            ).one() == (1, False, None, None, 0, 0, 0)
            assert connection.scalar(
                text(
                    "SELECT count(*) FROM connector_sync_generation_observations "
                    "WHERE generation_id=:id"
                ),
                {"id": generation_id},
            ) == 0
        with Session(upgraded) as session:
            assert PermissionAwareDocumentChunkSearchRepository(session).search(
                organization_id,
                user_id,
                [0.0] * 1536,
                "test:model:1536",
                10,
                source_item_types=("file",),
            ) == ()
    finally:
        upgraded.dispose()
