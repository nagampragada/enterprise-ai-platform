from __future__ import annotations

import os
import subprocess
import uuid
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
        raise RuntimeError(f"{name} must be set for document migration tests")
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
    with migrated_engine.begin() as conn:
        conn.execute(text("SET search_path TO public"))
        conn.execute(text("DELETE FROM documents"))
        conn.execute(text("DELETE FROM organizations"))
        conn.execute(text("DELETE FROM industries"))


def _create_organization(engine, *, name: str, slug: str) -> uuid.UUID:
    organization_id = uuid.uuid4()
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO organizations (id, name, slug) VALUES (:id, :name, :slug)"),
            {"id": organization_id, "name": name, "slug": slug},
        )
    return organization_id


def _insert_document(engine, *, organization_id: uuid.UUID, source_key: str, title: str = "Policy") -> uuid.UUID:
    document_id = uuid.uuid4()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO documents (id, organization_id, source_type, source_document_key, title)
                VALUES (:id, :organization_id, 'local_folder', :source_document_key, :title)
                """
            ),
            {
                "id": document_id,
                "organization_id": organization_id,
                "source_document_key": source_key,
                "title": title,
            },
        )
    return document_id


def test_upgrade_creates_documents_table_columns_constraints_and_indexes(migrated_engine) -> None:
    inspector = inspect(migrated_engine)
    assert "documents" in inspector.get_table_names(schema="public")
    assert [column["name"] for column in inspector.get_columns("documents", schema="public")] == [
        "id",
        "organization_id",
        "source_type",
        "source_document_key",
        "title",
        "source_url",
        "mime_type",
        "checksum_latest",
        "status",
        "source_created_at",
        "source_updated_at",
        "created_at",
        "updated_at",
        "deleted_at",
    ]

    constraints = {constraint["name"] for constraint in inspector.get_unique_constraints("documents", schema="public")}
    assert "uq_documents_organization_id_source_type_source_document_key" in constraints

    foreign_keys = {foreign_key["name"] for foreign_key in inspector.get_foreign_keys("documents", schema="public")}
    assert "fk_documents_organization_id_organizations" in foreign_keys

    indexes = {index["name"] for index in inspector.get_indexes("documents", schema="public")}
    assert {
        "ix_documents_organization_id_status",
        "ix_documents_organization_id_source_type",
        "ix_documents_organization_id_deleted_at",
    }.issubset(indexes)


def test_model_metadata_matches_documents_schema(migrated_engine) -> None:
    inspector = inspect(migrated_engine)
    assert list(Base.metadata.tables["documents"].columns.keys()) == [
        column["name"] for column in inspector.get_columns("documents", schema="public")
    ]


def test_same_source_identity_is_allowed_in_different_organizations(migrated_engine) -> None:
    organization_a = _create_organization(migrated_engine, name="Alpha", slug="alpha")
    organization_b = _create_organization(migrated_engine, name="Beta", slug="beta")

    _insert_document(migrated_engine, organization_id=organization_a, source_key="shared-key")
    _insert_document(migrated_engine, organization_id=organization_b, source_key="shared-key")


def test_duplicate_source_identity_is_rejected_within_one_organization(migrated_engine) -> None:
    organization_id = _create_organization(migrated_engine, name="Gamma", slug="gamma")
    _insert_document(migrated_engine, organization_id=organization_id, source_key="duplicate-key")

    with pytest.raises(IntegrityError):
        _insert_document(migrated_engine, organization_id=organization_id, source_key="duplicate-key")


def test_invalid_organization_reference_is_rejected(migrated_engine) -> None:
    with pytest.raises(IntegrityError):
        _insert_document(migrated_engine, organization_id=uuid.uuid4(), source_key="orphan-key")


def test_downgrade_removes_documents_table_and_upgrade_restores_it(migrated_engine) -> None:
    config = _alembic_config()
    command.downgrade(config, "20260802_000002")
    assert "documents" not in inspect(migrated_engine).get_table_names(schema="public")
    command.upgrade(config, "head")
    assert "documents" in inspect(migrated_engine).get_table_names(schema="public")