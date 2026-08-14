"""Document chunk repository implementation using SQLAlchemy 2.x."""

from __future__ import annotations

import math
from dataclasses import dataclass
from collections.abc import Iterable, Sequence
from uuid import UUID

from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from infrastructure.db.models import DocumentChunk


MAX_CHUNK_LIST_LIMIT = 500
EMBEDDING_DIMENSION = 1536


@dataclass(frozen=True)
class DocumentChunkPage:
    """Immutable keyset page for one tenant/document."""

    items: tuple[DocumentChunk, ...]
    limit: int
    has_more: bool
    next_after_chunk_index: int | None


class DocumentChunkRepository:
    """Repository for tenant- and document-scoped chunk persistence operations."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def add_many(
        self,
        organization_id: UUID,
        document_id: UUID,
        chunks: Iterable[DocumentChunk],
    ) -> None:
        """Add validated chunks without committing the caller's transaction."""
        chunk_batch = list(chunks)
        _validate_chunk_batch(organization_id, document_id, chunk_batch)
        self._session.add_all(chunk_batch)

    def get_by_id(
        self,
        organization_id: UUID,
        document_id: UUID,
        chunk_id: UUID,
    ) -> DocumentChunk | None:
        """Return a chunk only with complete tenant and document context."""
        statement = select(DocumentChunk).where(
            DocumentChunk.organization_id == organization_id,
            DocumentChunk.document_id == document_id,
            DocumentChunk.id == chunk_id,
        )
        return self._session.execute(statement).scalar_one_or_none()

    def list_for_document(
        self,
        organization_id: UUID,
        document_id: UUID,
        *,
        limit: int = MAX_CHUNK_LIST_LIMIT,
    ) -> list[DocumentChunk]:
        """Return chunks in deterministic chunk order for one tenant document."""
        if limit <= 0:
            raise ValueError("limit must be greater than zero")

        statement = (
            select(DocumentChunk)
            .where(
                DocumentChunk.organization_id == organization_id,
                DocumentChunk.document_id == document_id,
            )
            .order_by(DocumentChunk.chunk_index.asc(), DocumentChunk.id.asc())
            .limit(min(limit, MAX_CHUNK_LIST_LIMIT))
        )
        return list(self._session.execute(statement).scalars().all())

    def list_page_for_document(
        self,
        organization_id: UUID,
        document_id: UUID,
        *,
        limit: int,
        after_chunk_index: int | None = None,
    ) -> DocumentChunkPage:
        """Return one bounded keyset page ordered by chunk index."""
        _validate_page_arguments(limit, after_chunk_index)
        statement = select(DocumentChunk).where(
            DocumentChunk.organization_id == organization_id,
            DocumentChunk.document_id == document_id,
        )
        if after_chunk_index is not None:
            statement = statement.where(DocumentChunk.chunk_index > after_chunk_index)
        statement = statement.order_by(DocumentChunk.chunk_index.asc()).limit(limit + 1)
        rows = list(self._session.execute(statement).scalars().all())
        has_more = len(rows) > limit
        items = tuple(rows[:limit])
        return DocumentChunkPage(
            items=items,
            limit=limit,
            has_more=has_more,
            next_after_chunk_index=items[-1].chunk_index if has_more else None,
        )

    def delete_for_document(self, organization_id: UUID, document_id: UUID) -> int:
        """Delete all chunks for one tenant document without committing."""
        statement = delete(DocumentChunk).where(
            DocumentChunk.organization_id == organization_id,
            DocumentChunk.document_id == document_id,
        )
        result = self._session.execute(statement)
        return int(result.rowcount or 0)

    def replace_for_document(
        self,
        organization_id: UUID,
        document_id: UUID,
        chunks: Iterable[DocumentChunk],
    ) -> None:
        """Replace chunks in the caller-owned transaction.

        The complete replacement batch is validated before existing chunks are
        deleted. The caller must commit or roll back the transaction.
        """
        chunk_batch = list(chunks)
        _validate_chunk_batch(organization_id, document_id, chunk_batch)
        self.delete_for_document(organization_id, document_id)
        self._session.add_all(chunk_batch)

    def set_embedding(
        self,
        organization_id: UUID,
        document_id: UUID,
        chunk_id: UUID,
        embedding_model: str,
        embedding: Sequence[float],
    ) -> bool:
        """Set a validated embedding on one tenant/document chunk."""
        normalized_model = _validate_embedding(embedding_model, embedding)
        statement = (
            update(DocumentChunk)
            .where(
                DocumentChunk.organization_id == organization_id,
                DocumentChunk.document_id == document_id,
                DocumentChunk.id == chunk_id,
            )
            .values(embedding_model=normalized_model, embedding=[float(value) for value in embedding])
        )
        result = self._session.execute(statement)
        return bool(result.rowcount)

    def clear_embedding(self, organization_id: UUID, document_id: UUID, chunk_id: UUID) -> bool:
        """Clear the embedding and its model identity together."""
        statement = (
            update(DocumentChunk)
            .where(
                DocumentChunk.organization_id == organization_id,
                DocumentChunk.document_id == document_id,
                DocumentChunk.id == chunk_id,
            )
            .values(embedding=None, embedding_model=None)
        )
        result = self._session.execute(statement)
        return bool(result.rowcount)


def _validate_chunk_batch(
    organization_id: UUID,
    document_id: UUID,
    chunks: list[DocumentChunk],
) -> None:
    indexes: set[int] = set()
    for chunk in chunks:
        if chunk.organization_id != organization_id:
            raise ValueError("chunk organization does not match repository organization")
        if chunk.document_id != document_id:
            raise ValueError("chunk document does not match repository document")
        if chunk.chunk_index in indexes:
            raise ValueError("duplicate chunk index in replacement batch")
        indexes.add(chunk.chunk_index)


def _validate_embedding(embedding_model: str, embedding: Sequence[float]) -> str:
    if not isinstance(embedding_model, str) or not embedding_model.strip():
        raise ValueError("embedding_model must be a nonblank string")
    if isinstance(embedding, (str, bytes)) or len(embedding) != EMBEDDING_DIMENSION:
        raise ValueError(f"embedding must contain exactly {EMBEDDING_DIMENSION} numeric values")

    for value in embedding:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("embedding values must be numeric and not boolean")
        if not math.isfinite(float(value)):
            raise ValueError("embedding values must be finite")
    return embedding_model.strip()


def _validate_page_arguments(limit: int, after_chunk_index: int | None) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be an integer greater than zero")
    if limit > MAX_CHUNK_LIST_LIMIT:
        raise ValueError(f"limit must not exceed {MAX_CHUNK_LIST_LIMIT}")
    if after_chunk_index is not None and (
        isinstance(after_chunk_index, bool)
        or not isinstance(after_chunk_index, int)
        or after_chunk_index < 0
    ):
        raise ValueError("after_chunk_index must be None or a nonnegative integer")