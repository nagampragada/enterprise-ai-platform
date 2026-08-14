"""Document API response schemas."""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, ConfigDict


class DocumentIngestionResponse(BaseModel):
    """Safe summary of a completed document indexing operation."""

    model_config = ConfigDict(extra="forbid", strict=True)

    document_id: UUID
    source_type: str
    source_document_key: str
    ingestion_outcome: str
    chunks_seen: int
    chunks_embedded: int
    chunks_skipped: int
    provider_batches: int