from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.services.document_chunk_embedding_service import DocumentChunkEmbeddingSummary
from application.services.local_document_indexing_service import (
    InvalidDocumentIndexingRequestError,
    LocalDocumentIndexingService,
    NonProgressingDocumentChunkPageError,
)
from application.services.local_document_ingestion_service import LocalDocumentIngestionSummary
from infrastructure.db.models import DocumentChunk
from infrastructure.repositories.document_chunk_repository import DocumentChunkPage


def _chunk(organization_id, document_id, index, text="text"):
    return DocumentChunk(
        id=uuid4(), organization_id=organization_id, document_id=document_id, chunk_index=index,
        chunk_text=text, token_count=None, content_hash=f"hash-{index}",
    )


def _ingestion_summary(organization_id, document_id):
    return LocalDocumentIngestionSummary(
        organization_id, document_id, "local_folder", "file.txt", "created", "checksum", 1200, True, True,
    )


def _embedding_summary(organization_id, document_id, chunks, embedded=True):
    return DocumentChunkEmbeddingSummary(
        organization_id, document_id, "fake:model:1536", len(chunks), 0 if embedded else len(chunks),
        len(chunks) if embedded else 0, 1 if embedded else 0, tuple(chunk.id for chunk in chunks) if embedded else (),
    )


def _service(pages, page_size=2):
    organization_id, document_id = uuid4(), uuid4()
    ingestion = Mock()
    ingestion.ingest.return_value = _ingestion_summary(organization_id, document_id)
    repository = Mock()
    if callable(pages):
        repository.list_page_for_document.side_effect = pages
    else:
        repository.list_page_for_document.side_effect = iter(pages)
    embedding = Mock()
    embedding.embed_chunks.side_effect = lambda org, doc, chunks: _embedding_summary(org, doc, chunks)
    return LocalDocumentIndexingService(ingestion, repository, embedding), organization_id, document_id, ingestion, repository, embedding


def test_indexes_all_pages_and_aggregates_results():
    organization_id, document_id = uuid4(), uuid4()
    chunks = [_chunk(organization_id, document_id, index) for index in range(1200)]
    pages = [
        DocumentChunkPage(tuple(chunks[start : start + 500]), 500, start + 500 < 1200, start + 499 if start + 500 < 1200 else None)
        for start in (0, 500, 1000)
    ]
    service, _, _, ingestion, repository, embedding = _service(pages)
    ingestion.ingest.return_value = _ingestion_summary(organization_id, document_id)

    summary = service.index(organization_id, "local_folder", "file.txt", Path("file.txt"), page_size=500)

    assert summary.chunks_seen == 1200
    assert summary.chunks_embedded == 1200
    assert len(summary.embedded_chunk_ids) == 1200
    assert repository.list_page_for_document.call_args_list[1].kwargs["after_chunk_index"] == 499
    assert repository.list_page_for_document.call_args_list[2].kwargs["after_chunk_index"] == 999
    assert embedding.embed_chunks.call_count == 3


def test_empty_page_and_sparse_indexes_are_supported():
    organization_id, document_id = uuid4(), uuid4()
    chunks = [_chunk(organization_id, document_id, index) for index in (0, 10)]
    pages = [DocumentChunkPage(tuple(chunks), 500, False, None)]
    service, _, _, ingestion, _, _ = _service(pages)
    ingestion.ingest.return_value = _ingestion_summary(organization_id, document_id)
    summary = service.index(organization_id, "local_folder", "file.txt", Path("file.txt"))
    assert summary.chunks_seen == 2

    empty_pages = [DocumentChunkPage((), 500, False, None)]
    service, _, _, ingestion, _, embedding = _service(empty_pages)
    ingestion.ingest.return_value = _ingestion_summary(organization_id, document_id)
    summary = service.index(organization_id, "local_folder", "empty.txt", Path("empty.txt"))
    assert summary.chunks_seen == 0
    embedding.embed_chunks.assert_not_called()


def test_nonprogressing_cursor_stops_safely():
    organization_id, document_id = uuid4(), uuid4()
    chunk = _chunk(organization_id, document_id, 0)
    def pages(_organization_id, _document_id, *, after_chunk_index, limit):
        return DocumentChunkPage((chunk,), 1, True, 0)
    service, _, _, ingestion, _, _ = _service(pages, page_size=1)
    ingestion.ingest.return_value = _ingestion_summary(organization_id, document_id)
    with pytest.raises(NonProgressingDocumentChunkPageError):
        service.index(organization_id, "local_folder", "file.txt", Path("file.txt"), page_size=1)


@pytest.mark.parametrize("page_size", [0, -1, 501, True])
def test_invalid_page_size_rejected_before_collaborators(page_size):
    service, organization_id, _, ingestion, repository, _ = _service([])
    with pytest.raises(InvalidDocumentIndexingRequestError):
        service.index(organization_id, "local_folder", "file.txt", Path("file.txt"), page_size=page_size)
    ingestion.ingest.assert_not_called()
    repository.list_page_for_document.assert_not_called()


def test_provider_failure_prevents_later_pages_and_transactions_are_caller_owned():
    organization_id, document_id = uuid4(), uuid4()
    chunks = [_chunk(organization_id, document_id, index) for index in range(2)]
    def pages(_organization_id, _document_id, *, after_chunk_index, limit):
        if after_chunk_index is None:
            return DocumentChunkPage(tuple(chunks[:1]), 1, True, 0)
        return DocumentChunkPage(tuple(chunks[1:]), 1, False, None)

    service, _, _, ingestion, repository, embedding = _service(pages)
    ingestion.ingest.return_value = _ingestion_summary(organization_id, document_id)
    embedding.embed_chunks.side_effect = RuntimeError("provider failure")
    embedding.commit = Mock()
    embedding.rollback = Mock()
    with pytest.raises(RuntimeError, match="provider failure"):
        service.index(organization_id, "local_folder", "file.txt", Path("file.txt"))
    assert repository.list_page_for_document.call_count == 1
    embedding.commit.assert_not_called()
    embedding.rollback.assert_not_called()