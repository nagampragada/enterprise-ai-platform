from __future__ import annotations

import hashlib
import os
import subprocess
import uuid
from pathlib import Path
from typing import Sequence
from uuid import UUID

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from application.services.document_chunk_embedding_service import DocumentChunkEmbeddingService
from application.services.local_document_indexing_service import LocalDocumentIndexingService
from domain.embeddings.exceptions import RetryableEmbeddingProviderError
from domain.embeddings.models import EmbeddingProfile, EmbeddingRequest, EmbeddingResult
from domain.embeddings.provider import EmbeddingProvider
from infrastructure.content_chunking.text_chunker import DeterministicTextChunker
from infrastructure.content_extraction.registry import create_default_content_extractor_registry
from infrastructure.db.models import Document, DocumentChunk, Organization
from infrastructure.repositories.document_chunk_repository import DocumentChunkRepository
from infrastructure.repositories.document_repository import DocumentRepository
from application.services.local_document_ingestion_service import LocalDocumentIngestionService


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PROJECT_VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
DEFAULT_TEST_DATABASE_URL = (
    "postgresql+psycopg://enterprise_ai_platform:enterprise_ai_platform@127.0.0.1:15432/"
    "enterprise_ai_platform_test"
)
DIMENSION = 1536
MODEL_IDENTIFIER = "integration-fake:1536"


class DeterministicFakeEmbeddingProvider(EmbeddingProvider):
    def __init__(self, *, fail_on_batch: int | None = None) -> None:
        self.fail_on_batch = fail_on_batch
        self.batch_calls: list[tuple[int, ...]] = []

    @property
    def profile(self) -> EmbeddingProfile:
        return EmbeddingProfile("integration-fake", "integration-fake", DIMENSION, MODEL_IDENTIFIER, 128)

    def embed_batch(self, requests: Sequence[EmbeddingRequest]) -> tuple[EmbeddingResult, ...]:
        batch_number = len(self.batch_calls) + 1
        self.batch_calls.append(tuple(request.input_index for request in requests))
        if self.fail_on_batch == batch_number:
            raise RetryableEmbeddingProviderError("controlled integration failure")
        return tuple(
            EmbeddingResult(request.input_index, (float(request.input_index + 1),) * DIMENSION, MODEL_IDENTIFIER, DIMENSION)
            for request in requests
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
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM document_chunks"))
        connection.execute(text("DELETE FROM documents"))
        connection.execute(text("DELETE FROM organizations"))
        connection.execute(text("DELETE FROM industries"))
    session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


def _organization(session: Session, name: str) -> UUID:
    organization_id = uuid.uuid4()
    session.add(Organization(id=organization_id, name=name, slug=f"{name.lower()}-{organization_id}"))
    session.flush()
    return organization_id


def _coordinator(session: Session, provider: DeterministicFakeEmbeddingProvider) -> LocalDocumentIndexingService:
    chunk_repository = DocumentChunkRepository(session)
    return LocalDocumentIndexingService(
        LocalDocumentIngestionService(
            create_default_content_extractor_registry(),
            DeterministicTextChunker(),
            DocumentRepository(session),
            chunk_repository,
        ),
        chunk_repository,
        DocumentChunkEmbeddingService(provider, chunk_repository),
    )


def _write_large_source(path: Path, marker: str) -> bytes:
    payload = ((marker + "\n") * 300_000).encode("utf-8")
    path.write_bytes(payload)
    return payload


def _chunk_rows(session: Session, organization_id: UUID, document_id: UUID) -> list[DocumentChunk]:
    return list(
        session.scalars(
            select(DocumentChunk)
            .where(DocumentChunk.organization_id == organization_id, DocumentChunk.document_id == document_id)
            .order_by(DocumentChunk.chunk_index)
        ).all()
    )


def test_real_indexing_autoflush_false_paginates_and_commits_all_chunks(db_session: Session, tmp_path: Path) -> None:
    organization_id = _organization(db_session, "Target")
    other_organization_id = _organization(db_session, "Other")
    target_path = tmp_path / "large.txt"
    other_path = tmp_path / "other.txt"
    payload = _write_large_source(target_path, "target")
    other_payload = _write_large_source(other_path, "other")
    provider = DeterministicFakeEmbeddingProvider()
    coordinator = _coordinator(db_session, provider)

    first = coordinator.index(organization_id, "local_folder", "large.txt", target_path, page_size=500)
    other = coordinator.index(other_organization_id, "local_folder", "other.txt", other_path, page_size=500)
    db_session.commit()

    rows = _chunk_rows(db_session, organization_id, first.document_id)
    other_rows = _chunk_rows(db_session, other_organization_id, other.document_id)
    document = DocumentRepository(db_session).get_by_id(organization_id, first.document_id)
    assert document is not None
    assert document.checksum_latest == hashlib.sha256(payload).hexdigest()
    assert first.chunks_seen == first.chunks_embedded == len(rows)
    assert len(rows) >= 1200
    assert [row.chunk_index for row in rows] == list(range(len(rows)))
    assert all(row.embedding is not None and len(row.embedding) == DIMENSION for row in rows)
    assert all(row.embedding_model == MODEL_IDENTIFIER for row in rows)
    assert rows[-1].id in first.embedded_chunk_ids
    assert len(provider.batch_calls) > 1
    assert all(row.organization_id == organization_id for row in rows)
    assert all(row.organization_id == other_organization_id for row in other_rows)
    assert not hasattr(first, "chunk_text") and not hasattr(first, "embedding")
    initial_count = len(rows)
    initial_calls = len(provider.batch_calls)

    unchanged = coordinator.index(organization_id, "local_folder", "large.txt", target_path, page_size=500)
    db_session.commit()
    unchanged_rows = _chunk_rows(db_session, organization_id, first.document_id)
    assert unchanged.ingestion_outcome == "unchanged"
    assert unchanged.chunks_seen == initial_count
    assert unchanged.chunks_skipped == initial_count
    assert unchanged.chunks_embedded == 0
    assert len(unchanged_rows) == initial_count
    assert len(provider.batch_calls) == initial_calls
    assert len(_chunk_rows(db_session, other_organization_id, other.document_id)) == len(other_rows)
    assert DocumentRepository(db_session).get_by_id(other_organization_id, other.document_id).checksum_latest == hashlib.sha256(other_payload).hexdigest()


def test_later_embedding_failure_rolls_back_all_changes_and_preserves_other_tenant(db_session: Session, tmp_path: Path) -> None:
    organization_id = _organization(db_session, "Target")
    other_organization_id = _organization(db_session, "Other")
    path = tmp_path / "rollback.txt"
    other_path = tmp_path / "other.txt"
    _write_large_source(path, "original")
    _write_large_source(other_path, "other")
    initial_provider = DeterministicFakeEmbeddingProvider()
    initial = _coordinator(db_session, initial_provider)
    initial_summary = initial.index(organization_id, "local_folder", "rollback.txt", path, page_size=500)
    other_summary = initial.index(other_organization_id, "local_folder", "other.txt", other_path, page_size=500)
    db_session.commit()
    committed_document = DocumentRepository(db_session).get_by_id(organization_id, initial_summary.document_id)
    committed_rows = _chunk_rows(db_session, organization_id, initial_summary.document_id)
    other_rows_before = _chunk_rows(db_session, other_organization_id, other_summary.document_id)
    committed_checksum = committed_document.checksum_latest

    _write_large_source(path, "changed")
    failing_provider = DeterministicFakeEmbeddingProvider(fail_on_batch=2)
    failing = _coordinator(db_session, failing_provider)
    with pytest.raises(RetryableEmbeddingProviderError):
        failing.index(organization_id, "local_folder", "rollback.txt", path, page_size=500)
    assert len(failing_provider.batch_calls) == 2
    db_session.rollback()

    verification_document = DocumentRepository(db_session).get_by_id(organization_id, initial_summary.document_id)
    verification_rows = _chunk_rows(db_session, organization_id, initial_summary.document_id)
    assert verification_document.checksum_latest == committed_checksum
    assert [row.content_hash for row in verification_rows] == [row.content_hash for row in committed_rows]
    assert all(row.embedding_model == MODEL_IDENTIFIER for row in verification_rows)
    assert [row.content_hash for row in _chunk_rows(db_session, other_organization_id, other_summary.document_id)] == [row.content_hash for row in other_rows_before]
