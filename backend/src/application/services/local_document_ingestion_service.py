"""Application service for local document extraction and chunk persistence."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid4

from domain.content_chunking.chunker import ContentChunker
from domain.content_chunking.models import ChunkResult
from infrastructure.content_extraction.registry import ContentExtractorRegistry
from infrastructure.db.models import Document, DocumentChunk
from infrastructure.repositories.document_chunk_repository import DocumentChunkRepository
from infrastructure.repositories.document_repository import DocumentRepository


class InvalidLocalDocumentRequestError(ValueError):
    """Raised when local document ingestion input is invalid."""


class LocalDocumentIngestionPersistenceError(RuntimeError):
    """Raised when a document or chunk persistence operation reports no update."""


@dataclass(frozen=True)
class LocalDocumentIngestionRequest:
    """Explicit tenant-scoped local document ingestion input."""

    organization_id: UUID
    source_type: str
    source_document_key: str
    path: Path
    source_url: str | None = None
    mime_type: str | None = None
    source_created_at: datetime | None = None
    source_updated_at: datetime | None = None


@dataclass(frozen=True)
class LocalDocumentIngestionSummary:
    """Immutable result summary without source text or vectors."""

    organization_id: UUID
    document_id: UUID
    source_type: str
    source_document_key: str
    outcome: str
    content_checksum: str
    chunk_count: int
    chunks_replaced: bool
    embedding_required: bool


class LocalDocumentIngestionService:
    """Coordinates one local file through extraction, chunking, and persistence."""

    def __init__(
        self,
        extractor_registry: ContentExtractorRegistry,
        content_chunker: ContentChunker,
        document_repository: DocumentRepository,
        document_chunk_repository: DocumentChunkRepository,
    ) -> None:
        self._extractor_registry = extractor_registry
        self._content_chunker = content_chunker
        self._document_repository = document_repository
        self._document_chunk_repository = document_chunk_repository

    def ingest(self, request: LocalDocumentIngestionRequest) -> LocalDocumentIngestionSummary:
        _validate_request(request)
        content_checksum = _sha256_file(request.path)
        existing = self._document_repository.get_by_source_identity(
            organization_id=request.organization_id,
            source_type=request.source_type,
            source_document_key=request.source_document_key,
        )

        if existing is not None and existing.checksum_latest == content_checksum:
            restored = existing.deleted_at is not None
            if restored:
                restored_document = self._document_repository.restore(request.organization_id, existing.id)
                if restored_document is None:
                    raise LocalDocumentIngestionPersistenceError("document could not be restored")
            self._update_document_metadata(request, existing.id, content_checksum)
            chunk_count = len(
                self._document_chunk_repository.list_for_document(request.organization_id, existing.id)
            )
            return _summary(request, existing.id, "restored" if restored else "unchanged", content_checksum, chunk_count, False)

        extracted = self._extractor_registry.extract(request.path)
        chunk_results = self._content_chunker.chunk(extracted.text)
        if not chunk_results:
            raise InvalidLocalDocumentRequestError("document extraction produced zero chunks")

        if existing is None:
            document = Document(
                id=uuid4(),
                organization_id=request.organization_id,
                source_type=request.source_type,
                source_document_key=request.source_document_key,
                title=extracted.title or request.path.stem,
                source_url=request.source_url,
                mime_type=request.mime_type or extracted.mime_type,
                checksum_latest=content_checksum,
                status="ready",
                source_created_at=request.source_created_at,
                source_updated_at=request.source_updated_at,
            )
            self._document_repository.add(request.organization_id, document)
            outcome = "created"
        else:
            updated = self._update_document_metadata(request, existing.id, content_checksum, title=extracted.title)
            if updated is None:
                raise LocalDocumentIngestionPersistenceError("document could not be updated")
            if existing.deleted_at is not None:
                restored_document = self._document_repository.restore(request.organization_id, existing.id)
                if restored_document is None:
                    raise LocalDocumentIngestionPersistenceError("document could not be restored")
                outcome = "restored"
            else:
                outcome = "updated"
            document = existing

        chunks = _build_chunks(request.organization_id, document.id, chunk_results)
        self._document_chunk_repository.replace_for_document(request.organization_id, document.id, chunks)
        return _summary(request, document.id, outcome, content_checksum, len(chunks), True)

    def _update_document_metadata(
        self,
        request: LocalDocumentIngestionRequest,
        document_id: UUID,
        checksum: str,
        *,
        title: str | None = None,
    ) -> Document | None:
        return self._document_repository.update(
            organization_id=request.organization_id,
            document_id=document_id,
            title=title,
            source_url=request.source_url,
            mime_type=request.mime_type,
            checksum_latest=checksum,
            status="ready",
            source_created_at=request.source_created_at,
            source_updated_at=request.source_updated_at,
        )


def _validate_request(request: LocalDocumentIngestionRequest) -> None:
    if request.organization_id is None:
        raise InvalidLocalDocumentRequestError("organization_id is required")
    if not request.source_type.strip():
        raise InvalidLocalDocumentRequestError("source_type must not be blank")
    if not request.source_document_key.strip():
        raise InvalidLocalDocumentRequestError("source_document_key must not be blank")
    if not request.path.exists() or not request.path.is_file():
        raise InvalidLocalDocumentRequestError("path must be an existing regular file")
    for timestamp in (request.source_created_at, request.source_updated_at):
        if timestamp is not None and (timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None):
            raise InvalidLocalDocumentRequestError("source timestamps must be timezone-aware")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for data in iter(lambda: file_handle.read(8192), b""):
            digest.update(data)
    return digest.hexdigest()


def _build_chunks(organization_id: UUID, document_id: UUID, results: tuple[ChunkResult, ...]) -> list[DocumentChunk]:
    return [
        DocumentChunk(
            id=uuid4(),
            organization_id=organization_id,
            document_id=document_id,
            chunk_index=result.chunk_index,
            chunk_text=result.content,
            token_count=result.token_count,
            content_hash=result.content_checksum,
            embedding=None,
            embedding_model=None,
        )
        for result in results
    ]


def _summary(
    request: LocalDocumentIngestionRequest,
    document_id: UUID,
    outcome: str,
    checksum: str,
    chunk_count: int,
    chunks_replaced: bool,
) -> LocalDocumentIngestionSummary:
    return LocalDocumentIngestionSummary(
        organization_id=request.organization_id,
        document_id=document_id,
        source_type=request.source_type,
        source_document_key=request.source_document_key,
        outcome=outcome,
        content_checksum=checksum,
        chunk_count=chunk_count,
        chunks_replaced=chunks_replaced,
        embedding_required=True,
    )