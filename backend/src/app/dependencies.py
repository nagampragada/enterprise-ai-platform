"""Application dependency wiring for API routes."""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from uuid import UUID

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from application.services.authentication_service import AuthenticationService
from application.services.connector_management_service import ConnectorManagementService
from application.services.connector_sync_schedule_service import ConnectorSyncScheduleService
from application.services.document_chunk_embedding_service import DocumentChunkEmbeddingService
from application.services.local_document_indexing_service import LocalDocumentIndexingService
from application.services.local_document_ingestion_service import LocalDocumentIngestionService
from domain.embeddings.exceptions import EmbeddingProviderAuthenticationError
from infrastructure.content_chunking.text_chunker import DeterministicTextChunker
from infrastructure.content_extraction.registry import create_default_content_extractor_registry
from infrastructure.embeddings.openai import OpenAIEmbeddingProvider
from infrastructure.repositories.document_chunk_repository import DocumentChunkRepository
from infrastructure.repositories.document_repository import DocumentRepository
from infrastructure.db.session import SessionLocal
from infrastructure.repositories.authentication_session_repository import AuthenticationSessionRepository
from infrastructure.repositories.user_repository import UserRepository
from infrastructure.security.tokens import decode_access_token


AUTHENTICATION_ERROR_DETAIL = "Invalid or expired access token"


@dataclass(frozen=True)
class CurrentUser:
    user_id: UUID
    organization_id: UUID
    email: str
    display_name: str


@dataclass(frozen=True)
class ConnectorAdministrator:
    user_id: UUID
    organization_id: UUID


def get_db_session() -> Generator[Session, None, None]:
    """Provide a request-scoped SQLAlchemy session."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def get_authentication_service(db_session: Session = Depends(get_db_session)) -> AuthenticationService:
    """Build an AuthenticationService from request-scoped repositories."""
    user_repository = UserRepository(db_session)
    authentication_session_repository = AuthenticationSessionRepository(db_session)
    return AuthenticationService(
        user_repository=user_repository,
        authentication_session_repository=authentication_session_repository,
    )


def get_connector_management_service(
    db_session: Session = Depends(get_db_session),
) -> ConnectorManagementService:
    return ConnectorManagementService(db_session)


def get_connector_sync_schedule_service(
    db_session: Session = Depends(get_db_session),
) -> ConnectorSyncScheduleService:
    return ConnectorSyncScheduleService(db_session)


def get_local_document_indexing_service(
    db_session: Session = Depends(get_db_session),
) -> LocalDocumentIndexingService:
    """Build the authenticated local-document indexing composition."""
    try:
        embedding_provider = OpenAIEmbeddingProvider()
    except EmbeddingProviderAuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Embedding provider is unavailable",
        ) from exc

    chunk_repository = DocumentChunkRepository(db_session)
    return LocalDocumentIndexingService(
        ingestion_service=LocalDocumentIngestionService(
            extractor_registry=create_default_content_extractor_registry(),
            content_chunker=DeterministicTextChunker(),
            document_repository=DocumentRepository(db_session),
            document_chunk_repository=chunk_repository,
        ),
        chunk_repository=chunk_repository,
        embedding_service=DocumentChunkEmbeddingService(
            embedding_provider=embedding_provider,
            document_chunk_repository=chunk_repository,
        ),
    )


def get_current_user(
    authorization: str | None = Header(default=None),
    db_session: Session = Depends(get_db_session),
) -> CurrentUser:
    if authorization is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=AUTHENTICATION_ERROR_DETAIL,
        )

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=AUTHENTICATION_ERROR_DETAIL,
        )

    payload = decode_access_token(token.strip())
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=AUTHENTICATION_ERROR_DETAIL,
        )

    user_repository = UserRepository(db_session)
    try:
        user = user_repository.get_by_id(
            organization_id=payload.organization_id,
            user_id=payload.user_id,
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        )

    if user is None or user.status != "active":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=AUTHENTICATION_ERROR_DETAIL,
        )

    return CurrentUser(
        user_id=user.id,
        organization_id=user.organization_id,
        email=user.email,
        display_name=user.display_name,
    )


def get_connector_administrator(
    current_user: CurrentUser = Depends(get_current_user),
    db_session: Session = Depends(get_db_session),
) -> ConnectorAdministrator:
    try:
        authorized = UserRepository(db_session).is_active_organization_admin(
            current_user.organization_id,
            current_user.user_id,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        ) from exc
    if not authorized:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Connector administration is forbidden",
        )
    return ConnectorAdministrator(current_user.user_id, current_user.organization_id)
