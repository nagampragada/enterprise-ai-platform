"""Application service for coordinating chunk embedding and persistence."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable
from uuid import UUID

from domain.embeddings.exceptions import InvalidEmbeddingInputError, InvalidEmbeddingResultError
from domain.embeddings.models import EmbeddingRequest, EmbeddingResult
from domain.embeddings.provider import EmbeddingProvider
from domain.embeddings.validation import validate_embedding_results
from infrastructure.db.models import DocumentChunk
from infrastructure.repositories.document_chunk_repository import DocumentChunkRepository


class InvalidDocumentChunkEmbeddingBatchError(ValueError):
    """Raised when supplied chunks cannot be safely embedded as one batch."""


class DocumentChunkEmbeddingPersistenceError(RuntimeError):
    """Raised when a validated embedding cannot be persisted."""


@dataclass(frozen=True)
class DocumentChunkEmbeddingSummary:
    """Immutable summary of one embedding coordination operation."""

    organization_id: UUID
    document_id: UUID
    model_identifier: str
    total_chunks: int
    skipped_chunks: int
    embedded_chunks: int
    provider_batches: int
    embedded_chunk_ids: tuple[UUID, ...]


class DocumentChunkEmbeddingService:
    """Coordinates provider batches and tenant-scoped chunk updates."""

    def __init__(
        self,
        embedding_provider: EmbeddingProvider,
        document_chunk_repository: DocumentChunkRepository,
    ) -> None:
        self._embedding_provider = embedding_provider
        self._document_chunk_repository = document_chunk_repository

    def embed_chunks(
        self,
        organization_id: UUID,
        document_id: UUID,
        chunks: Iterable[DocumentChunk],
    ) -> DocumentChunkEmbeddingSummary:
        """Embed supplied chunks and leave transaction completion to the caller."""
        chunk_batch = sorted(tuple(chunks), key=lambda chunk: chunk.chunk_index)
        profile = self._embedding_provider.profile
        _validate_chunk_batch(organization_id, document_id, chunk_batch)

        to_embed: list[DocumentChunk] = []
        skipped_count = 0
        for chunk in chunk_batch:
            has_embedding = chunk.embedding is not None
            has_model = chunk.embedding_model is not None
            if has_embedding != has_model:
                raise InvalidDocumentChunkEmbeddingBatchError(
                    f"chunk {chunk.id} has an incomplete embedding state"
                )
            if has_embedding and chunk.embedding_model == profile.model_identifier:
                skipped_count += 1
            else:
                to_embed.append(chunk)

        provider_results: list[tuple[DocumentChunk, EmbeddingResult]] = []
        provider_batch_count = 0
        if to_embed:
            batch_size = profile.max_batch_size or len(to_embed)
            for batch_start in range(0, len(to_embed), batch_size):
                chunk_batch_to_embed = to_embed[batch_start : batch_start + batch_size]
                requests = tuple(
                    EmbeddingRequest(input_index=index, text=chunk.chunk_text)
                    for index, chunk in enumerate(chunk_batch_to_embed)
                )
                results = self._embedding_provider.embed_batch(requests)
                ordered_results = validate_embedding_results(requests, results, profile)
                provider_results.extend(zip(chunk_batch_to_embed, ordered_results, strict=True))
                provider_batch_count += 1

        embedded_ids: list[UUID] = []
        for chunk, result in provider_results:
            updated = self._document_chunk_repository.set_embedding(
                organization_id=organization_id,
                document_id=document_id,
                chunk_id=chunk.id,
                embedding_model=result.model_identifier,
                embedding=result.vector,
            )
            if not updated:
                raise DocumentChunkEmbeddingPersistenceError(
                    f"chunk {chunk.id} could not be updated for the requested tenant and document"
                )
            embedded_ids.append(chunk.id)

        return DocumentChunkEmbeddingSummary(
            organization_id=organization_id,
            document_id=document_id,
            model_identifier=profile.model_identifier,
            total_chunks=len(chunk_batch),
            skipped_chunks=skipped_count,
            embedded_chunks=len(embedded_ids),
            provider_batches=provider_batch_count,
            embedded_chunk_ids=tuple(embedded_ids),
        )


def _validate_chunk_batch(
    organization_id: UUID,
    document_id: UUID,
    chunks: tuple[DocumentChunk, ...],
) -> None:
    if not chunks:
        raise InvalidDocumentChunkEmbeddingBatchError("chunk batch must not be empty")

    chunk_ids: set[UUID] = set()
    chunk_indexes: set[int] = set()
    for chunk in chunks:
        if chunk.id is None:
            raise InvalidDocumentChunkEmbeddingBatchError("every chunk must have an ID")
        if chunk.organization_id != organization_id:
            raise InvalidDocumentChunkEmbeddingBatchError("chunk organization does not match requested organization")
        if chunk.document_id != document_id:
            raise InvalidDocumentChunkEmbeddingBatchError("chunk document does not match requested document")
        if chunk.id in chunk_ids:
            raise InvalidDocumentChunkEmbeddingBatchError("chunk IDs must be unique")
        if chunk.chunk_index in chunk_indexes:
            raise InvalidDocumentChunkEmbeddingBatchError("chunk indexes must be unique")
        if not chunk.chunk_text.strip():
            raise InvalidDocumentChunkEmbeddingBatchError("chunk content must not be blank")
        chunk_ids.add(chunk.id)
        chunk_indexes.add(chunk.chunk_index)