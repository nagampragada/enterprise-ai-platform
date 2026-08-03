"""Application dependency wiring for API routes."""

from __future__ import annotations

from collections.abc import Generator

from fastapi import Depends
from sqlalchemy.orm import Session

from application.services.authentication_service import AuthenticationService
from infrastructure.db.session import SessionLocal
from infrastructure.repositories.authentication_session_repository import AuthenticationSessionRepository
from infrastructure.repositories.user_repository import UserRepository


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
