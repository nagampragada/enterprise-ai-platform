"""Application dependency wiring for API routes."""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from uuid import UUID

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from application.services.authentication_service import AuthenticationService
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
