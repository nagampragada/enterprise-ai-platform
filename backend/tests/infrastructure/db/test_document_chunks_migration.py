from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from infrastructure.db import models as db_models  # noqa: F401
from infrastructure.db.base import Base


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PROJECT_VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
TEST_DATABASE_URL_ENV_VAR = "TEST_DATABASE_URL"
DATABASE_URL_ENV_VAR = "DATABASE_URL"
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
PRIOR_REVISION = "20260814_000004"


def _database_identity(database_url: str) -> tuple[str, str | None, int | None, str | None]:
    url = make_url(database_url)
    return url.drivername, url.host, url.port, url.database


def _required_env_url(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set for document chunk migration tests")
    return value


def _alembic_config() -> Config:
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", _required_env_url(TEST_DATABASE_URL_ENV_VAR))
    return config


def _run_alembic_upgrade(database_url: str, revision: str) -> None:
    environment = os.environ.copy()
    environment[DATABASE_URL_ENV_VAR] = database_url
    subprocess.run(
        [str(PROJECT_VENV_PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", revision],
        check=True,
        cwd=str(PROJECT_ROOT),
        env=environment,
    )


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

    _run_alembic_upgrade(test_database_url, PRIOR_REVISION)
    command.upgrade(_alembic_config(), "head")
    engine = create_engine(test_database_url, future=True)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(autouse=True)
def _clean_test_data(migrated_engine):
    with migrated_engine.begin() as conn:
        conn.execute(text("SET search_path TO public"))
        conn.execute(text("DELETE FROM document_chunks"))
        conn.execute(text("DELETE FROM documents"))
        conn.execute(text("DELETE FROM organizations"))
        conn.execute(text("DELETE FROM industries"))


@pytest.fixture()
def db_session(migrated_engine):
    session = sessionmaker(bind=migrated_engine, autoflush=False, expire_on_commit=False, class_=Session)()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


def _create_organization(session: Session, *, name: str, slug: str) -> uuid.UUID:
    organization_id = uuid.uuid4()
    session.execute(
        text("INSERT INTO organizations (id, name, slug) VALUES (:id, :name, :slug)"),
        {"id": organization_id, "name": name, "slug": slug},
    )
    session.commit()
    return organization_id


def _create_document(session: Session, organization_id: uuid.UUID, *, key: str = "document.txt") -> uuid.UUID:
    document_id = uuid.uuid4()
    session.execute(
        text(
            """
            INSERT INTO documents (id, organization_id, source_type, source_document_key, title)
            VALUES (:id, :organization_id, 'local_folder', :key, 'Document')
            """
        ),
        {"id": document_id, "organization_id": organization_id, "key": key},
    )
    session.commit()
    return document_id


def _insert_chunk(
    session: Session,
    organization_id: uuid.UUID,
    document_id: uuid.UUID,
    *,
    chunk_index: int = 0,
    chunk_text: str = "Extracted text",
    token_count: int | None = 3,
    content_hash: str = "hash-0",
    embedding: str | None = None,
    embedding_model: str | None = None,
    chunk_id: uuid.UUID | None = None,
) -> uuid.UUID:
    chunk_id = chunk_id or uuid.uuid4()
    try:
        session.execute(
            text(
                """
                INSERT INTO document_chunks (
                    id, organization_id, document_id, chunk_index, chunk_text,
                    token_count, content_hash, embedding, embedding_model
                ) VALUES (
                    :id, :organization_id, :document_id, :chunk_index, :chunk_text,
                    :token_count, :content_hash,
                    CAST(:embedding AS vector),
                    :embedding_model
                )
                """
            ),
            {
                "id": chunk_id,
                "organization_id": organization_id,
                "document_id": document_id,
                "chunk_index": chunk_index,
                "chunk_text": chunk_text,
                "token_count": token_count,
                "content_hash": content_hash,
                "embedding": embedding,
                "embedding_model": embedding_model,
            },
        )
        session.commit()
    except SQLAlchemyError:
        session.rollback()
        raise
    return chunk_id


def _zero_vector(dimension: int = 1536) -> str:
    return "[" + ",".join("0" for _ in range(dimension)) + "]"


def test_upgrade_creates_chunk_table_columns_type_constraints_and_indexes(migrated_engine) -> None:
    config = _alembic_config()
    command.downgrade(config, PRIOR_REVISION)
    assert "document_chunks" not in inspect(migrated_engine).get_table_names(schema="public")
    command.upgrade(config, "head")
    inspector = inspect(migrated_engine)
    assert "document_chunks" in inspector.get_table_names(schema="public")
    assert [column["name"] for column in inspector.get_columns("document_chunks", schema="public")] == [
        "id", "organization_id", "document_id", "chunk_index", "chunk_text", "token_count",
        "content_hash", "embedding", "embedding_model", "created_at", "updated_at",
    ]
    embedding_column = next(column for column in inspector.get_columns("document_chunks", schema="public") if column["name"] == "embedding")
    assert str(embedding_column["type"]).lower() == "vector(1536)"
    assert {"uq_document_chunks_organization_id_document_id_chunk_index"}.issubset(
        {constraint["name"] for constraint in inspector.get_unique_constraints("document_chunks", schema="public")}
    )
    assert {"ix_document_chunks_organization_id_document_id"}.issubset(
        {index["name"] for index in inspector.get_indexes("document_chunks", schema="public")}
    )
    assert "uq_documents_organization_id_id" in {
        constraint["name"] for constraint in inspector.get_unique_constraints("documents", schema="public")
    }


def test_model_metadata_matches_chunk_schema(migrated_engine) -> None:
    inspector = inspect(migrated_engine)
    assert list(Base.metadata.tables["document_chunks"].columns.keys()) == [
        column["name"] for column in inspector.get_columns("document_chunks", schema="public")
    ]


def test_valid_ordered_chunks_and_duplicate_index_rules(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Alpha", slug="alpha")
    document_id = _create_document(db_session, organization_id)
    _insert_chunk(db_session, organization_id, document_id, chunk_index=0)
    _insert_chunk(db_session, organization_id, document_id, chunk_index=1, content_hash="hash-1")
    with pytest.raises(IntegrityError):
        _insert_chunk(db_session, organization_id, document_id, chunk_index=1, content_hash="hash-duplicate")


def test_chunk_index_can_repeat_for_other_documents_and_organizations(db_session: Session) -> None:
    organization_a = _create_organization(db_session, name="Beta", slug="beta")
    organization_b = _create_organization(db_session, name="Gamma", slug="gamma")
    document_a = _create_document(db_session, organization_a, key="a.txt")
    document_a_two = _create_document(db_session, organization_a, key="b.txt")
    document_b = _create_document(db_session, organization_b, key="a.txt")
    _insert_chunk(db_session, organization_a, document_a)
    _insert_chunk(db_session, organization_a, document_a_two)
    _insert_chunk(db_session, organization_b, document_b)


def test_tenant_and_document_foreign_keys_reject_invalid_references(db_session: Session) -> None:
    organization_a = _create_organization(db_session, name="Delta", slug="delta")
    organization_b = _create_organization(db_session, name="Epsilon", slug="epsilon")
    document_a = _create_document(db_session, organization_a)
    cases = [
        {"organization_id": uuid.uuid4(), "document_id": document_a},
        {"organization_id": organization_a, "document_id": uuid.uuid4()},
        {"organization_id": organization_b, "document_id": document_a},
    ]
    for case in cases:
        with pytest.raises(IntegrityError):
            _insert_chunk(db_session, case["organization_id"], case["document_id"])


@pytest.mark.parametrize(
    ("field", "value"),
    [("chunk_index", -1), ("token_count", -1), ("chunk_text", ""), ("chunk_text", "   "), ("content_hash", "")],
)
def test_chunk_value_constraints_reject_invalid_values(db_session: Session, field: str, value) -> None:
    organization_id = _create_organization(db_session, name=f"{field}-{value!s}", slug=f"{field}-{uuid.uuid4()}")
    document_id = _create_document(db_session, organization_id, key=f"{field}.txt")
    with pytest.raises(IntegrityError):
        _insert_chunk(db_session, organization_id, document_id, **{field: value})


@pytest.mark.parametrize(
    ("embedding", "embedding_model"),
    [(_zero_vector(1536), None), (None, "model-without-vector"), (_zero_vector(1536), " ")],
)
def test_embedding_and_model_must_be_populated_together(db_session: Session, embedding: str | None, embedding_model: str | None) -> None:
    organization_id = _create_organization(db_session, name="Embedding", slug=f"embedding-{uuid.uuid4()}")
    document_id = _create_document(db_session, organization_id, key=f"{uuid.uuid4()}.txt")
    with pytest.raises(IntegrityError):
        _insert_chunk(
            db_session,
            organization_id,
            document_id,
            embedding=embedding,
            embedding_model=embedding_model,
        )


def test_valid_1536_vector_is_accepted_and_wrong_dimension_rejected(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Vectors", slug="vectors")
    document_id = _create_document(db_session, organization_id)
    _insert_chunk(
        db_session,
        organization_id,
        document_id,
        embedding=_zero_vector(),
        embedding_model="text-embedding-3-small",
    )
    other_document_id = _create_document(db_session, organization_id, key="other.txt")
    with pytest.raises(SQLAlchemyError):
        _insert_chunk(
            db_session,
            organization_id,
            other_document_id,
            embedding=_zero_vector(2),
            embedding_model="text-embedding-3-small",
        )


def test_deleting_document_cascades_to_chunks(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Cascade", slug="cascade")
    document_id = _create_document(db_session, organization_id)
    _insert_chunk(db_session, organization_id, document_id)
    db_session.execute(text("DELETE FROM documents WHERE organization_id = :organization_id AND id = :id"), {"organization_id": organization_id, "id": document_id})
    db_session.commit()
    count = db_session.execute(text("SELECT count(*) FROM document_chunks WHERE document_id = :id"), {"id": document_id}).scalar_one()
    assert count == 0


def test_downgrade_removes_chunks_and_supporting_key_but_keeps_vector_extension(migrated_engine) -> None:
    config = _alembic_config()
    command.upgrade(config, "head")
    command.downgrade(config, PRIOR_REVISION)
    inspector = inspect(migrated_engine)
    assert "document_chunks" not in inspector.get_table_names(schema="public")
    assert "uq_documents_organization_id_id" not in {
        constraint["name"] for constraint in inspector.get_unique_constraints("documents", schema="public")
    }
    with migrated_engine.connect() as connection:
        assert connection.execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")).scalar_one() == 1
    command.upgrade(config, "head")