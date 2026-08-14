"""Application coordinator for local document indexing."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from application.services.document_chunk_embedding_service import (
    DocumentChunkEmbeddingService,
    DocumentChunkEmbeddingSummary,
)
from application.services.local_document_ingestion_service import (
    LocalDocumentIngestionRequest,
    LocalDocumentIngestionService,
    LocalDocumentIngestionSummary,
)
from infrastructure.db.models import DocumentChunk
from infrastructure.repositories.document_chunk_repository import DocumentChunkPage, DocumentChunkRepository


class InvalidDocumentIndexingRequestError(ValueError):
    """Raised when indexing coordinator input or pagination is invalid."""


class NonProgressingDocumentChunkPageError(RuntimeError):
    """Raised when pagination cannot make safe forward progress."""


@dataclass(frozen=True)
class LocalDocumentIndexingSummary:
    """Immutable indexing summary without source text or vectors."""

    organization_id: UUID
    document_id: UUID
    source_type: str
    source_document_key: str
    ingestion_outcome: str
    content_checksum: str
    chunks_seen: int
    chunks_embedded: int
    chunks_skipped: int
    provider_batches: int
    embedded_chunk_ids: tuple[UUID, ...]


class LocalDocumentIndexingService:
    """Coordinate local ingestion, complete paginated retrieval, and embedding."""

    def __init__(
        self,
        ingestion_service: LocalDocumentIngestionService,
        chunk_repository: DocumentChunkRepository,
        embedding_service: DocumentChunkEmbeddingService,
    ) -> None:
        self._ingestion_service = ingestion_service
        self._chunk_repository = chunk_repository
        self._embedding_service = embedding_service

    def index(
        self,
        organization_id: UUID,
        source_type: str,
        source_document_key: str,
        path: Path,
        *,
        page_size: int = 500,
        source_url: str | None = None,
        mime_type: str | None = None,
    ) -> LocalDocumentIndexingSummary:
        _validate_input(organization_id, source_type, source_document_key, page_size)
        ingestion = self._ingestion_service.ingest(
            LocalDocumentIngestionRequest(
                organization_id=organization_id,
                source_type=source_type,
                source_document_key=source_document_key,
                path=path,
                source_url=source_url,
                mime_type=mime_type,
            )
        )

        embedded_ids: list[UUID] = []
        chunks_embedded = 0
        chunks_skipped = 0
        provider_batches = 0
        chunks_seen = 0
        for page in self._iter_pages(organization_id, ingestion.document_id, page_size):
            chunks_seen += len(page.items)
            if not page.items:
                continue
            result = self._embedding_service.embed_chunks(organization_id, ingestion.document_id, page.items)
            embedded_ids.extend(result.embedded_chunk_ids)
            chunks_embedded += result.embedded_chunks
            chunks_skipped += result.skipped_chunks
            provider_batches += result.provider_batches

        return LocalDocumentIndexingSummary(
            organization_id=organization_id,
            document_id=ingestion.document_id,
            source_type=ingestion.source_type,
            source_document_key=ingestion.source_document_key,
            ingestion_outcome=ingestion.outcome,
            content_checksum=ingestion.content_checksum,
            chunks_seen=chunks_seen,
            chunks_embedded=chunks_embedded,
            chunks_skipped=chunks_skipped,
            provider_batches=provider_batches,
            embedded_chunk_ids=tuple(embedded_ids),
        )

    def _iter_pages(self, organization_id: UUID, document_id: UUID, page_size: int):
        cursor: int | None = None
        seen_cursors: set[int] = set()
        while True:
            page = self._chunk_repository.list_page_for_document(
                organization_id,
                document_id,
                limit=page_size,
                after_chunk_index=cursor,
            )
            yield page
            if not page.has_more:
                return
            next_cursor = page.next_after_chunk_index
            if next_cursor is None or not page.items or next_cursor in seen_cursors:
                raise NonProgressingDocumentChunkPageError("chunk pagination did not make progress")
            if cursor is not None and next_cursor <= cursor:
                raise NonProgressingDocumentChunkPageError("chunk pagination cursor did not advance")
            seen_cursors.add(next_cursor)
            cursor = next_cursor


def _validate_input(organization_id: UUID, source_type: str, source_document_key: str, page_size: int) -> None:
    if organization_id is None:
        raise InvalidDocumentIndexingRequestError("organization_id is required")
    if not isinstance(source_type, str) or not source_type.strip():
        raise InvalidDocumentIndexingRequestError("source_type must not be blank")
    if not isinstance(source_document_key, str) or not source_document_key.strip():
        raise InvalidDocumentIndexingRequestError("source_document_key must not be blank")
    if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1 or page_size > 500:
        raise InvalidDocumentIndexingRequestError("page_size must be between 1 and 500")