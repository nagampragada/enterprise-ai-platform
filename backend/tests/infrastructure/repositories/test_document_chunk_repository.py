from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path
from uuid import UUID
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from infrastructure.db.models import Document, DocumentChunk, Organization
from infrastructure.repositories.document_chunk_repository import (
    EMBEDDING_DIMENSION,
    MAX_CHUNK_LIST_LIMIT,
    DocumentChunkRepository,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PROJECT_VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
TEST_DATABASE_URL_ENV_VAR = "TEST_DATABASE_URL"
DATABASE_URL_ENV_VAR = "DATABASE_URL"
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
DEFAULT_TEST_DATABASE_URL = (
    "postgresql+psycopg://enterprise_ai_platform:enterprise_ai_platform@127.0.0.1:15432/"
    "enterprise_ai_platform_test"
)


def _database_identity(database_url: str) -> tuple[str, str | None, int | None, str | None]:
    url = make_url(database_url)
    return url.drivername, url.host, url.port, url.database


def _test_database_url() -> str:
    return os.getenv(TEST_DATABASE_URL_ENV_VAR, DEFAULT_TEST_DATABASE_URL)


def _run_alembic_upgrade(database_url: str) -> None:
    environment = os.environ.copy()
    environment[DATABASE_URL_ENV_VAR] = database_url
    subprocess.run(
        [str(PROJECT_VENV_PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", "head"],
        check=True,
        cwd=str(PROJECT_ROOT),
        env=environment,
    )


@pytest.fixture(scope="module")
def migrated_engine():
    test_database_url = _test_database_url()
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
def _clean_test_data(migrated_engine) -> None:
    with migrated_engine.begin() as conn:
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


def _create_organization(session: Session, *, name: str, slug: str) -> UUID:
    organization_id = uuid.uuid4()
    session.add(Organization(id=organization_id, name=name, slug=slug, status="active"))
    session.flush()
    return organization_id


def _create_document(session: Session, organization_id: UUID, *, key: str = "document.txt") -> Document:
    document = Document(
        id=uuid.uuid4(),
        organization_id=organization_id,
        source_type="local_folder",
        source_document_key=key,
        title="Document",
    )
    session.add(document)
    session.flush()
    return document


def _build_chunk(organization_id: UUID, document_id: UUID, chunk_index: int, *, text_value: str | None = None) -> DocumentChunk:
    return DocumentChunk(
        id=uuid.uuid4(),
        organization_id=organization_id,
        document_id=document_id,
        chunk_index=chunk_index,
        chunk_text=text_value or f"chunk {chunk_index}",
        token_count=2,
        content_hash=f"hash-{chunk_index}-{uuid.uuid4()}",
    )


def _vector() -> list[float]:
    return [0.25] * EMBEDDING_DIMENSION


def test_add_many_and_get_are_tenant_document_scoped(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Acme", slug="acme")
    other_organization_id = _create_organization(db_session, name="Beta", slug="beta")
    document = _create_document(db_session, organization_id)
    repository = DocumentChunkRepository(db_session)
    chunks = [_build_chunk(organization_id, document.id, index) for index in range(2)]

    repository.add_many(organization_id, document.id, chunks)
    db_session.commit()

    assert repository.get_by_id(organization_id, document.id, chunks[0].id).id == chunks[0].id
    assert repository.get_by_id(other_organization_id, document.id, chunks[0].id) is None
    assert repository.get_by_id(organization_id, uuid.uuid4(), chunks[0].id) is None


@pytest.mark.parametrize("mismatch", ["organization", "document"])
def test_add_many_rejects_mixed_context_before_writes(db_session: Session, mismatch: str) -> None:
    organization_id = _create_organization(db_session, name="Gamma", slug="gamma")
    other_organization_id = _create_organization(db_session, name="Delta", slug="delta")
    document = _create_document(db_session, organization_id)
    other_document = _create_document(db_session, organization_id, key="other.txt")
    if mismatch == "organization":
        invalid = _build_chunk(other_organization_id, document.id, 1)
    else:
        invalid = _build_chunk(organization_id, other_document.id, 1)
    repository = DocumentChunkRepository(db_session)

    with pytest.raises(ValueError):
        repository.add_many(organization_id, document.id, [_build_chunk(organization_id, document.id, 0), invalid])

    assert db_session.execute(text("SELECT count(*) FROM document_chunks")).scalar_one() == 0


def test_duplicate_input_indexes_are_rejected_before_writes(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Epsilon", slug="epsilon")
    document = _create_document(db_session, organization_id)
    repository = DocumentChunkRepository(db_session)

    with pytest.raises(ValueError):
        repository.add_many(organization_id, document.id, [_build_chunk(organization_id, document.id, 0), _build_chunk(organization_id, document.id, 0)])

    assert db_session.execute(text("SELECT count(*) FROM document_chunks")).scalar_one() == 0


def test_listing_is_ordered_and_bounded(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Zeta", slug="zeta")
    document = _create_document(db_session, organization_id)
    repository = DocumentChunkRepository(db_session)
    chunks = [_build_chunk(organization_id, document.id, index) for index in range(MAX_CHUNK_LIST_LIMIT + 3)]
    repository.add_many(organization_id, document.id, reversed(chunks))
    db_session.commit()

    found = repository.list_for_document(organization_id, document.id, limit=MAX_CHUNK_LIST_LIMIT + 10)
    assert len(found) == MAX_CHUNK_LIST_LIMIT
    assert [chunk.chunk_index for chunk in found] == list(range(MAX_CHUNK_LIST_LIMIT))


def test_keyset_pages_cover_sparse_chunks_without_duplicates(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Page Org", slug="page-org")
    document = _create_document(db_session, organization_id)
    other_document = _create_document(db_session, organization_id, key="other.txt")
    other_organization = _create_organization(db_session, name="Other Page Org", slug="other-page-org")
    other_tenant_document = _create_document(db_session, other_organization, key="other-tenant.txt")
    repository = DocumentChunkRepository(db_session)
    chunks = [_build_chunk(organization_id, document.id, index) for index in (0, 3, 9, 20)]
    repository.add_many(organization_id, document.id, chunks)
    repository.add_many(organization_id, other_document.id, [_build_chunk(organization_id, other_document.id, 3)])
    repository.add_many(other_organization, other_tenant_document.id, [_build_chunk(other_organization, other_tenant_document.id, 3)])
    db_session.commit()

    first = repository.list_page_for_document(organization_id, document.id, limit=2)
    middle = repository.list_page_for_document(
        organization_id, document.id, limit=2, after_chunk_index=first.next_after_chunk_index
    )
    final = repository.list_page_for_document(organization_id, document.id, limit=2, after_chunk_index=20)
    assert [chunk.chunk_index for chunk in first.items] == [0, 3]
    assert first.has_more is True and first.next_after_chunk_index == 3
    assert [chunk.chunk_index for chunk in middle.items] == [9, 20]
    assert middle.has_more is False and middle.next_after_chunk_index is None
    assert final.items == ()
    assert final.has_more is False and final.next_after_chunk_index is None


def test_keyset_page_limit_validation_and_transaction_ownership(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Limit Org", slug="limit-org")
    document = _create_document(db_session, organization_id)
    repository = DocumentChunkRepository(db_session)
    repository.add_many(organization_id, document.id, [_build_chunk(organization_id, document.id, 0)])
    db_session.commit()
    for limit in (0, -1, MAX_CHUNK_LIST_LIMIT + 1, True):
        with pytest.raises(ValueError):
            repository.list_page_for_document(organization_id, document.id, limit=limit)
    for cursor in (-1, True):
        with pytest.raises(ValueError):
            repository.list_page_for_document(organization_id, document.id, limit=1, after_chunk_index=cursor)
    repository.commit = Mock()
    repository.rollback = Mock()
    repository.list_page_for_document(organization_id, document.id, limit=1)
    repository.commit.assert_not_called()
    repository.rollback.assert_not_called()


def test_delete_is_scoped_to_tenant_and_document(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Eta", slug="eta")
    other_organization_id = _create_organization(db_session, name="Theta", slug="theta")
    document = _create_document(db_session, organization_id)
    other_document = _create_document(db_session, organization_id, key="other.txt")
    other_tenant_document = _create_document(db_session, other_organization_id, key="tenant.txt")
    repository = DocumentChunkRepository(db_session)
    repository.add_many(organization_id, document.id, [_build_chunk(organization_id, document.id, 0)])
    repository.add_many(organization_id, other_document.id, [_build_chunk(organization_id, other_document.id, 0)])
    repository.add_many(other_organization_id, other_tenant_document.id, [_build_chunk(other_organization_id, other_tenant_document.id, 0)])
    db_session.commit()

    assert repository.delete_for_document(organization_id, document.id) == 1
    assert repository.delete_for_document(other_organization_id, document.id) == 0
    db_session.commit()
    assert repository.list_for_document(organization_id, other_document.id)
    assert repository.list_for_document(other_organization_id, other_tenant_document.id)


def test_replace_validates_before_delete_and_is_rollbackable(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Iota", slug="iota")
    document = _create_document(db_session, organization_id)
    repository = DocumentChunkRepository(db_session)
    old_chunks = [_build_chunk(organization_id, document.id, index) for index in range(2)]
    repository.add_many(organization_id, document.id, old_chunks)
    db_session.commit()

    invalid = _build_chunk(organization_id, document.id, 0)
    with pytest.raises(ValueError):
        repository.replace_for_document(organization_id, document.id, [_build_chunk(organization_id, document.id, 0), invalid])
    assert [chunk.chunk_index for chunk in repository.list_for_document(organization_id, document.id)] == [0, 1]

    replacement = [_build_chunk(organization_id, document.id, 4), _build_chunk(organization_id, document.id, 5)]
    repository.replace_for_document(organization_id, document.id, replacement)
    db_session.rollback()
    assert [chunk.chunk_index for chunk in repository.list_for_document(organization_id, document.id)] == [0, 1]

    repository.replace_for_document(organization_id, document.id, replacement)
    db_session.commit()
    assert [chunk.chunk_index for chunk in repository.list_for_document(organization_id, document.id)] == [4, 5]


def test_set_and_clear_embedding_are_scoped_and_transaction_owned(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Kappa", slug="kappa")
    other_organization_id = _create_organization(db_session, name="Lambda", slug="lambda")
    document = _create_document(db_session, organization_id)
    chunk = _build_chunk(organization_id, document.id, 0)
    repository = DocumentChunkRepository(db_session)
    repository.add_many(organization_id, document.id, [chunk])
    db_session.commit()

    assert repository.set_embedding(other_organization_id, document.id, chunk.id, "model", _vector()) is False
    assert repository.set_embedding(organization_id, document.id, chunk.id, "  model  ", _vector()) is True
    db_session.refresh(chunk)
    assert chunk.embedding_model == "model"
    assert repository.clear_embedding(organization_id, document.id, chunk.id) is True
    db_session.commit()
    db_session.refresh(chunk)
    assert chunk.embedding is None
    assert chunk.embedding_model is None


@pytest.mark.parametrize(
    "embedding",
    [[], [0.0] * (EMBEDDING_DIMENSION - 1), [0.0] * (EMBEDDING_DIMENSION + 1), [float("nan")] * EMBEDDING_DIMENSION, [float("inf")] * EMBEDDING_DIMENSION, [True] * EMBEDDING_DIMENSION, ["0"] * EMBEDDING_DIMENSION],
)
def test_embedding_validation_rejects_invalid_values(db_session: Session, embedding) -> None:
    organization_id = _create_organization(db_session, name="Mu", slug=f"mu-{uuid.uuid4()}")
    document = _create_document(db_session, organization_id, key=f"{uuid.uuid4()}.txt")
    chunk = _build_chunk(organization_id, document.id, 0)
    repository = DocumentChunkRepository(db_session)
    repository.add_many(organization_id, document.id, [chunk])
    db_session.commit()

    with pytest.raises(ValueError):
        repository.set_embedding(organization_id, document.id, chunk.id, "model", embedding)


def test_blank_embedding_model_is_rejected(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Nu", slug="nu")
    document = _create_document(db_session, organization_id)
    chunk = _build_chunk(organization_id, document.id, 0)
    repository = DocumentChunkRepository(db_session)
    repository.add_many(organization_id, document.id, [chunk])
    db_session.commit()

    with pytest.raises(ValueError):
        repository.set_embedding(organization_id, document.id, chunk.id, " ", _vector())