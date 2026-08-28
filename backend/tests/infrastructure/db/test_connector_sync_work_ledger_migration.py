from __future__ import annotations

import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import configure_mappers

from infrastructure.db import models as db_models  # noqa: F401
from infrastructure.db.base import Base


ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
TEST_URL = "TEST_DATABASE_URL"
DEV_URL = "DATABASE_URL"
REVISION = "20260828_000020"
PRIOR_REVISION = "20260828_000019"


def _identity(url: str) -> tuple[object, ...]:
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database, value.query


def _config(url: str) -> Config:
    value = Config(str(INI))
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


def test_mapper_configuration_and_migration_head(engine) -> None:
    configure_mappers()
    with engine.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == REVISION


def test_schema_matches_models_and_contains_only_control_plane_metadata(engine) -> None:
    inspector = inspect(engine)
    expected = {
        "connector_sync_generations": {
            "indexes": {
                "ix_sync_generations_org_scope_created",
                "ix_sync_generations_org_scope_terminal",
                "ix_sync_generations_follow_up",
            },
            "foreign_keys": {
                "fk_sync_generations_organization",
                "fk_sync_generations_connector_tenant",
                "fk_sync_generations_scope_tenant",
                "fk_sync_generations_job_tenant",
            },
        },
        "connector_sync_file_work_items": {
            "indexes": {
                "ix_sync_file_work_claimable",
                "ix_sync_file_work_expired",
                "ix_sync_file_work_generation_barrier",
                "ix_sync_file_work_scope_terminal",
            },
            "foreign_keys": {"fk_sync_file_work_generation_tenant"},
        },
    }
    assert expected.keys() <= set(inspector.get_table_names(schema="public"))
    for table, contract in expected.items():
        reflected_columns = [
            column["name"] for column in inspector.get_columns(table, schema="public")
        ]
        assert reflected_columns == list(Base.metadata.tables[table].columns.keys())
        assert contract["indexes"] <= {
            index["name"] for index in inspector.get_indexes(table, schema="public")
        }
        assert contract["foreign_keys"] == {
            key["name"] for key in inspector.get_foreign_keys(table, schema="public")
        }
        model_constraints = {
            constraint.name
            for constraint in Base.metadata.tables[table].constraints
            if constraint.name
        }
        reflected_constraints = {
            inspector.get_pk_constraint(table, schema="public")["name"],
            *(item["name"] for item in inspector.get_unique_constraints(table, schema="public")),
            *(item["name"] for item in inspector.get_check_constraints(table, schema="public")),
            *(item["name"] for item in inspector.get_foreign_keys(table, schema="public")),
        }
        assert model_constraints == reflected_constraints

    work_columns = set(Base.metadata.tables["connector_sync_file_work_items"].columns)
    assert not work_columns.intersection(
        {"raw_bytes", "raw_content", "extracted_text", "chunk_text", "embedding", "vector"}
    )


def test_tenant_qualified_generation_foreign_keys_reject_mixed_tenants(engine) -> None:
    organization_one, organization_two = uuid4(), uuid4()
    connector_id, space_id, scope_id, job_id, generation_id = (
        uuid4() for _ in range(5)
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO organizations (id,name,slug) VALUES "
                "(:one,'One',:one_slug),(:two,'Two',:two_slug)"
            ),
            {
                "one": organization_one,
                "two": organization_two,
                "one_slug": f"one-{organization_one}",
                "two_slug": f"two-{organization_two}",
            },
        )
        connection.execute(
            text(
                """INSERT INTO connectors
                   (id,organization_id,connector_type,display_name,slug,status)
                   VALUES (:id,:org,'github','GitHub',:slug,'active')"""
            ),
            {"id": connector_id, "org": organization_one, "slug": f"connector-{connector_id}"},
        )
        connection.execute(
            text(
                "INSERT INTO knowledge_spaces (id,organization_id,name,slug) "
                "VALUES (:id,:org,'Space',:slug)"
            ),
            {"id": space_id, "org": organization_one, "slug": f"space-{space_id}"},
        )
        connection.execute(
            text(
                """INSERT INTO connector_scopes
                   (id,organization_id,connector_id,knowledge_space_id,display_name,slug,
                    scope_type,external_scope_key,access_mode,status)
                   VALUES (:id,:org,:connector,:space,'Scope',:slug,'repository',:key,
                           'platform_managed','active')"""
            ),
            {
                "id": scope_id,
                "org": organization_one,
                "connector": connector_id,
                "space": space_id,
                "slug": f"scope-{scope_id}",
                "key": f"github:repository:{scope_id.int}",
            },
        )
        connection.execute(
            text(
                """INSERT INTO connector_sync_jobs
                   (id,organization_id,connector_id,connector_scope_id,mode,trigger_type,status)
                   VALUES (:id,:org,:connector,:scope,'initial','manual','queued')"""
            ),
            {
                "id": job_id,
                "org": organization_one,
                "connector": connector_id,
                "scope": scope_id,
            },
        )

    with pytest.raises(IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    """INSERT INTO connector_sync_generations
                       (id,organization_id,connector_id,connector_scope_id,sync_job_id,
                        provider_key,repository_identity,branch_name,commit_object_id,
                        root_tree_object_id,profile_fingerprint,created_at,updated_at)
                       VALUES (:id,:wrong_org,:connector,:scope,:job,'github',
                               'github:repository:123','main',:commit,:tree,:profile,now(),now())"""
                ),
                {
                    "id": generation_id,
                    "wrong_org": organization_two,
                    "connector": connector_id,
                    "scope": scope_id,
                    "job": job_id,
                    "commit": "a" * 40,
                    "tree": "b" * 40,
                    "profile": "github:extract-v1:chunk-v2:embed-v1",
                },
            )


def test_revision_downgrades_cleanly_and_reupgrades(engine) -> None:
    url = os.environ[TEST_URL]
    engine.dispose()
    command.downgrade(_config(url), PRIOR_REVISION)
    downgraded = create_engine(url, future=True)
    try:
        tables = set(inspect(downgraded).get_table_names(schema="public"))
        assert "connector_sync_generations" not in tables
        assert "connector_sync_file_work_items" not in tables
        with downgraded.connect() as connection:
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == PRIOR_REVISION
    finally:
        downgraded.dispose()
    command.upgrade(_config(url), REVISION)
    upgraded = create_engine(url, future=True)
    try:
        assert {
            "connector_sync_generations",
            "connector_sync_file_work_items",
        } <= set(inspect(upgraded).get_table_names(schema="public"))
    finally:
        upgraded.dispose()
