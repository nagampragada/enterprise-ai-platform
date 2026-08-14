"""Authenticated document ingestion routes."""

from __future__ import annotations

import tempfile
import unicodedata
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status

from app.api.v1.documents.schemas import DocumentIngestionResponse
from app.dependencies import CurrentUser, get_current_user, get_db_session, get_local_document_indexing_service
from application.services.document_chunk_embedding_service import DocumentChunkEmbeddingPersistenceError
from application.services.local_document_indexing_service import (
    InvalidDocumentIndexingRequestError,
    LocalDocumentIndexingService,
    NonProgressingDocumentChunkPageError,
)
from application.services.local_document_ingestion_service import (
    InvalidLocalDocumentRequestError,
    LocalDocumentIngestionPersistenceError,
)
from domain.content_extraction.exceptions import (
    ContentParseError,
    ContentReadError,
    ContentTooLargeError,
    EncryptedContentError,
    UnsupportedContentTypeError,
)
from domain.embeddings.exceptions import (
    EmbeddingProviderAuthenticationError,
    PermanentEmbeddingProviderError,
    RetryableEmbeddingProviderError,
)
from sqlalchemy.orm import Session


documents_router = APIRouter(prefix="/documents", tags=["documents"])

MAX_UPLOAD_SIZE_BYTES = 25 * 1024 * 1024
UPLOAD_CHUNK_SIZE_BYTES = 1024 * 1024
SUPPORTED_EXTENSIONS = {".txt", ".md", ".markdown", ".docx", ".pdf"}


@documents_router.post("/ingest", response_model=DocumentIngestionResponse)
async def ingest_document(
    file: UploadFile = File(...),
    current_user: CurrentUser = Depends(get_current_user),
    db_session: Session = Depends(get_db_session),
    indexing_service: LocalDocumentIndexingService = Depends(get_local_document_indexing_service),
) -> DocumentIngestionResponse:
    safe_filename = _validate_filename(file.filename)
    suffix = Path(safe_filename).suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        raise HTTPException(status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, detail="Unsupported document type")

    try:
        with tempfile.TemporaryDirectory(prefix="enterprise-ai-upload-") as temporary_directory:
            temporary_path = Path(temporary_directory) / f"upload{suffix}"
            size = 0
            with temporary_path.open("wb") as target:
                while True:
                    data = await file.read(UPLOAD_CHUNK_SIZE_BYTES)
                    if not data:
                        break
                    size += len(data)
                    if size > MAX_UPLOAD_SIZE_BYTES:
                        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="Uploaded document is too large")
                    target.write(data)

            summary = indexing_service.index(
                organization_id=current_user.organization_id,
                source_type="manual_upload",
                source_document_key=safe_filename,
                path=temporary_path,
                mime_type=file.content_type,
            )
            db_session.commit()
    except HTTPException:
        db_session.rollback()
        raise
    except (EmbeddingProviderAuthenticationError, RetryableEmbeddingProviderError) as exc:
        db_session.rollback()
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Embedding provider unavailable") from exc
    except PermanentEmbeddingProviderError as exc:
        db_session.rollback()
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Embedding provider rejected the document") from exc
    except UnsupportedContentTypeError as exc:
        db_session.rollback()
        raise HTTPException(status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, detail="Unsupported document type") from exc
    except (ContentTooLargeError,) as exc:
        db_session.rollback()
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="Uploaded document is too large") from exc
    except (ContentParseError, EncryptedContentError, ContentReadError, InvalidLocalDocumentRequestError) as exc:
        db_session.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Document could not be processed") from exc
    except InvalidDocumentIndexingRequestError as exc:
        db_session.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Document indexing failed") from exc
    except (
        NonProgressingDocumentChunkPageError,
        LocalDocumentIngestionPersistenceError,
        DocumentChunkEmbeddingPersistenceError,
    ) as exc:
        db_session.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error") from exc
    except Exception as exc:
        db_session.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error") from exc

    return DocumentIngestionResponse(
        document_id=summary.document_id,
        source_type=summary.source_type,
        source_document_key=summary.source_document_key,
        ingestion_outcome=summary.ingestion_outcome,
        chunks_seen=summary.chunks_seen,
        chunks_embedded=summary.chunks_embedded,
        chunks_skipped=summary.chunks_skipped,
        provider_batches=summary.provider_batches,
    )


def _validate_filename(filename: str | None) -> str:
    if not filename:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Filename is required")
    normalized = unicodedata.normalize("NFKC", filename).strip()
    if not normalized or normalized in {".", ".."} or any(character in normalized for character in ("/", "\\", "\x00")):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid filename")
    if any(ord(character) < 32 or ord(character) == 127 for character in normalized):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid filename")
    return normalized