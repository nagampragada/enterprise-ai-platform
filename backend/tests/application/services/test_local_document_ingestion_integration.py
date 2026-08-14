from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from application.services.local_document_ingestion_service import (
    LocalDocumentIngestionRequest,
    LocalDocumentIngestionService,
)
from infrastructure.content_chunking.text_chunker import DeterministicTextChunker
from infrastructure.content_extraction.registry import create_default_content_extractor_registry
from infrastructure.db.models import Document, Organization
from infrastructure.repositories.document_chunk_repository import DocumentChunkRepository
from infrastructure.repositories.document_repository import DocumentRepository


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PROJECT_VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
DEFAULT_TEST_DATABASE_URL = (
    "postgresql+psycopg://enterprise_ai_platform:enterprise_ai_platform@127.0.0.1:15432/"
    "enterprise_ai_platform_test"
)


def _database_identity(database_url: str) -> tuple[str, str | None, int | None, str | None]:
    url = make_url(database_url)
    return url.drivername, url.host, url.port, url.database


def _run_upgrade(database_url: str) -> None:
    environment = os.environ.copy()
    environment["DATABASE_URL"] = database_url
    subprocess.run(
        [str(PROJECT_VENV_PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", "head"],
        check=True,
        cwd=str(PROJECT_ROOT),
        env=environment,
    )


@pytest.fixture(scope="module")
def engine():
    test_url = os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)
    development_url = os.environ.get("DATABASE_URL")
    if development_url and _database_identity(development_url) == _database_identity(test_url):
        raise RuntimeError("TEST_DATABASE_URL must differ from DATABASE_URL")
    reset = create_engine(test_url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    reset.dispose()
    _run_upgrade(test_url)
    value = create_engine(test_url, future=True)
    try:
        yield value
    finally:
        value.dispose()


@pytest.fixture()
def db_session(engine):
    session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)()
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM documents"))
        connection.execute(text("DELETE FROM organizations"))
        connection.execute(text("DELETE FROM industries"))
    try:
        yield session
    finally:
        session.rollback()
        session.close()


def _organization(session: Session) -> uuid.UUID:
    organization_id = uuid.uuid4()
    session.add(Organization(id=organization_id, name="Integration", slug=f"integration-{organization_id}"))
    session.flush()
    return organization_id


def _service(session: Session) -> LocalDocumentIngestionService:
    return LocalDocumentIngestionService(
        create_default_content_extractor_registry(),
        DeterministicTextChunker(),
        DocumentRepository(session),
        DocumentChunkRepository(session),
    )


def _request(organization_id: uuid.UUID, path: Path) -> LocalDocumentIngestionRequest:
    return LocalDocumentIngestionRequest(organization_id, "local_folder", path.name, path)


def test_new_document_and_chunks_persist_after_caller_commit(db_session: Session, tmp_path: Path) -> None:
    organization_id = _organization(db_session)
    path = tmp_path / "new.txt"
    path.write_text("first paragraph\n\nsecond paragraph", encoding="utf-8")

    summary = _service(db_session).ingest(_request(organization_id, path))
    db_session.commit()

    document = DocumentRepository(db_session).get_by_id(organization_id, summary.document_id)
    chunks = DocumentChunkRepository(db_session).list_for_document(organization_id, summary.document_id)
    assert document is not None
    assert document.status == "ready"
    assert len(chunks) == summary.chunk_count
    assert all(chunk.embedding is None and chunk.embedding_model is None for chunk in chunks)


def test_changed_file_replaces_chunks_atomically_after_commit(db_session: Session, tmp_path: Path) -> None:
    organization_id = _organization(db_session)
    path = tmp_path / "changed.txt"
    path.write_text("old content", encoding="utf-8")
    first = _service(db_session).ingest(_request(organization_id, path))
    db_session.commit()
    path.write_text("new content that is different", encoding="utf-8")

    second = _service(db_session).ingest(_request(organization_id, path))
    db_session.commit()

    chunks = DocumentChunkRepository(db_session).list_for_document(organization_id, first.document_id)
    assert second.outcome == "updated"
    assert [chunk.chunk_text for chunk in chunks] == ["new content that is different"]


def test_caller_rollback_removes_pending_new_document_and_chunks(db_session: Session, tmp_path: Path) -> None:
    organization_id = _organization(db_session)
    path = tmp_path / "rollback.txt"
    path.write_text("pending content", encoding="utf-8")

    summary = _service(db_session).ingest(_request(organization_id, path))
    db_session.rollback()

    assert DocumentRepository(db_session).get_by_id(organization_id, summary.document_id) is None
