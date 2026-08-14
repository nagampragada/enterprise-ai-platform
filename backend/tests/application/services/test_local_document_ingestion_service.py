from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.services.local_document_ingestion_service import (
    InvalidLocalDocumentRequestError,
    LocalDocumentIngestionRequest,
    LocalDocumentIngestionService,
)
from domain.content_chunking.models import ChunkResult
from domain.content_extraction.exceptions import ContentParseError, UnsupportedContentTypeError
from infrastructure.db.models import Document, DocumentChunk


def _chunk(index: int, content: str = "text") -> ChunkResult:
    return ChunkResult(index, content, "a" * 64, len(content), index, index + len(content))


def _build(tmp_path: Path, *, existing: Document | None = None, chunks=None):
    organization_id, document_id = uuid4(), existing.id if existing else uuid4()
    path = tmp_path / "document.txt"
    path.write_bytes(b"source bytes")
    request = LocalDocumentIngestionRequest(organization_id, "local_folder", "document.txt", path)
    registry = Mock()
    registry.extract.return_value = SimpleNamespace(text="source text", title="Title", mime_type="text/plain")
    chunker = Mock()
    chunker.chunk.return_value = tuple(chunks or [_chunk(0)])
    documents = Mock()
    documents.get_by_source_identity.return_value = existing
    documents.add.side_effect = lambda organization_id, document: document
    documents.update.return_value = existing or Document(
        id=document_id,
        organization_id=organization_id,
        source_type="local_folder",
        source_document_key="document.txt",
        title="Title",
        status="ready",
    )
    chunks_repo = Mock()
    chunks_repo.list_for_document.return_value = list(chunks or [_chunk(0)])
    return request, registry, chunker, documents, chunks_repo


@pytest.mark.parametrize(
    "field",
    ["source_type", "source_document_key"],
)
def test_blank_source_identity_rejected_before_extraction(tmp_path: Path, field: str):
    request, registry, chunker, documents, chunks_repo = _build(tmp_path)
    values = {
        "organization_id": request.organization_id,
        "source_type": request.source_type,
        "source_document_key": request.source_document_key,
        "path": request.path,
    }
    values[field] = " "
    invalid = request.__class__(**values)
    with pytest.raises(InvalidLocalDocumentRequestError):
        LocalDocumentIngestionService(registry, chunker, documents, chunks_repo).ingest(invalid)
    registry.extract.assert_not_called()


def test_missing_path_and_directory_are_rejected_before_extraction(tmp_path: Path):
    request, registry, chunker, documents, chunks_repo = _build(tmp_path)
    service = LocalDocumentIngestionService(registry, chunker, documents, chunks_repo)
    for path in (tmp_path / "missing.txt", tmp_path):
        invalid = request.__class__(request.organization_id, request.source_type, request.source_document_key, path)
        with pytest.raises(InvalidLocalDocumentRequestError):
            service.ingest(invalid)
    registry.extract.assert_not_called()


def test_unsupported_extension_propagates_from_registry(tmp_path: Path):
    request, registry, chunker, documents, chunks_repo = _build(tmp_path)
    registry.extract.side_effect = UnsupportedContentTypeError("unsupported")
    with pytest.raises(UnsupportedContentTypeError):
        LocalDocumentIngestionService(registry, chunker, documents, chunks_repo).ingest(request)
    chunks_repo.replace_for_document.assert_not_called()


def test_naive_source_timestamp_is_rejected(tmp_path: Path):
    request, registry, chunker, documents, chunks_repo = _build(tmp_path)
    invalid = request.__class__(
        request.organization_id,
        request.source_type,
        request.source_document_key,
        request.path,
        source_updated_at=datetime(2026, 8, 14),
    )
    with pytest.raises(InvalidLocalDocumentRequestError):
        LocalDocumentIngestionService(registry, chunker, documents, chunks_repo).ingest(invalid)
    registry.extract.assert_not_called()


def test_new_document_passes_tenant_to_repositories_and_creates_unembedded_chunks(tmp_path: Path):
    request, registry, chunker, documents, chunks_repo = _build(tmp_path)
    summary = LocalDocumentIngestionService(registry, chunker, documents, chunks_repo).ingest(request)
    assert summary.outcome == "created"
    assert summary.chunk_count == 1
    assert summary.embedding_required is True
    documents.add.assert_called_once()
    chunks_repo.replace_for_document.assert_called_once_with(request.organization_id, documents.add.call_args.args[1].id, [pytest.approx(chunks_repo.replace_for_document.call_args.args[2][0])])
    chunk = chunks_repo.replace_for_document.call_args.args[2][0]
    assert chunk.organization_id == request.organization_id
    assert chunk.embedding is None and chunk.embedding_model is None
    assert summary.content_checksum == hashlib.sha256(b"source bytes").hexdigest()
    assert not hasattr(summary, "text")
    assert not hasattr(summary, "vector")


def test_unchanged_document_skips_extraction_and_preserves_chunks(tmp_path: Path):
    existing = Document(id=uuid4(), organization_id=uuid4(), source_type="local_folder", source_document_key="document.txt", title="Old", status="ready", checksum_latest="c")
    request, registry, chunker, documents, chunks_repo = _build(tmp_path, existing=existing)
    import application.services.local_document_ingestion_service as module
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(module, "_sha256_file", lambda path: "c")
        summary = LocalDocumentIngestionService(registry, chunker, documents, chunks_repo).ingest(request)
    assert summary.outcome == "unchanged"
    registry.extract.assert_not_called()
    chunks_repo.replace_for_document.assert_not_called()


def test_changed_document_extracts_and_replaces_chunks(tmp_path: Path):
    existing = Document(id=uuid4(), organization_id=uuid4(), source_type="local_folder", source_document_key="document.txt", title="Old", status="ready", checksum_latest="old")
    request, registry, chunker, documents, chunks_repo = _build(tmp_path, existing=existing)
    summary = LocalDocumentIngestionService(registry, chunker, documents, chunks_repo).ingest(request)
    assert summary.outcome == "updated"
    registry.extract.assert_called_once_with(request.path)
    chunks_repo.replace_for_document.assert_called_once()


def test_zero_chunks_are_rejected_before_replacement(tmp_path: Path):
    request, registry, chunker, documents, chunks_repo = _build(tmp_path)
    chunker.chunk.return_value = ()
    with pytest.raises(InvalidLocalDocumentRequestError):
        LocalDocumentIngestionService(registry, chunker, documents, chunks_repo).ingest(request)
    chunks_repo.replace_for_document.assert_not_called()


def test_repository_failure_propagates_and_service_does_not_manage_transactions(tmp_path: Path):
    request, registry, chunker, documents, chunks_repo = _build(tmp_path)
    documents.add.side_effect = RuntimeError("database failure")
    documents.commit = Mock()
    documents.rollback = Mock()
    with pytest.raises(RuntimeError, match="database failure"):
        LocalDocumentIngestionService(registry, chunker, documents, chunks_repo).ingest(request)
    documents.commit.assert_not_called()
    documents.rollback.assert_not_called()


def test_extraction_and_chunking_failures_precede_replacement(tmp_path: Path):
    request, registry, chunker, documents, chunks_repo = _build(tmp_path)
    registry.extract.side_effect = ContentParseError("bad")
    with pytest.raises(ContentParseError):
        LocalDocumentIngestionService(registry, chunker, documents, chunks_repo).ingest(request)
    chunks_repo.replace_for_document.assert_not_called()

    registry.extract.side_effect = None
    chunker.chunk.side_effect = RuntimeError("chunking")
    with pytest.raises(RuntimeError):
        LocalDocumentIngestionService(registry, chunker, documents, chunks_repo).ingest(request)
    chunks_repo.replace_for_document.assert_not_called()


def test_soft_deleted_unchanged_document_is_restored_without_rechunking(tmp_path: Path):
    existing = Document(
        id=uuid4(),
        organization_id=uuid4(),
        source_type="local_folder",
        source_document_key="document.txt",
        title="Old",
        status="ready",
        checksum_latest="c",
        deleted_at=datetime.now(timezone.utc),
    )
    request, registry, chunker, documents, chunks_repo = _build(tmp_path, existing=existing)
    import application.services.local_document_ingestion_service as module
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(module, "_sha256_file", lambda path: "c")
        summary = LocalDocumentIngestionService(registry, chunker, documents, chunks_repo).ingest(request)
    assert summary.outcome == "restored"
    documents.restore.assert_called_once_with(request.organization_id, existing.id)
    chunks_repo.replace_for_document.assert_not_called()