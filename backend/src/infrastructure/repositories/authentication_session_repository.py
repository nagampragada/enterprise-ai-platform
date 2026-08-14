"""Authentication session repository implementation using SQLAlchemy 2.x."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from infrastructure.db.models import AuthenticationSession


class AuthenticationSessionRepository:
    """Repository for authentication session persistence operations."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get_by_id(self, organization_id: UUID, session_id: UUID) -> AuthenticationSession | None:
        """Return an authentication session by tenant and session identifier."""
        statement = select(AuthenticationSession).where(
            AuthenticationSession.organization_id == organization_id,
            AuthenticationSession.id == session_id,
        )
        return self._session.execute(statement).scalar_one_or_none()

    def get_by_refresh_token_hash(self, refresh_token_hash: str) -> AuthenticationSession | None:
        """Return an authentication session by globally unique refresh token hash."""
        statement = select(AuthenticationSession).where(
            AuthenticationSession.refresh_token_hash == refresh_token_hash.encode("utf-8"),
        )
        return self._session.execute(statement).scalar_one_or_none()

    def add(self, authentication_session: AuthenticationSession) -> AuthenticationSession:
        """Add a new authentication session entity to the active session."""
        self._session.add(authentication_session)
        return authentication_session

    def revoke(
        self,
        organization_id: UUID,
        user_id: UUID,
        session_id: UUID,
        revoked_at: datetime,
    ) -> AuthenticationSession | None:
        """Revoke a tenant-scoped authentication session."""
        statement = (
            update(AuthenticationSession)
            .where(
                AuthenticationSession.organization_id == organization_id,
                AuthenticationSession.user_id == user_id,
                AuthenticationSession.id == session_id,
            )
            .values(revoked_at=revoked_at)
            .returning(AuthenticationSession)
        )
        return self._session.execute(statement).scalar_one_or_none()

    def revoke_all_for_user(
        self,
        organization_id: UUID,
        user_id: UUID,
        revoked_at: datetime,
    ) -> int:
        """Revoke all active sessions for a tenant-scoped user and return count."""
        statement = (
            update(AuthenticationSession)
            .where(
                AuthenticationSession.organization_id == organization_id,
                AuthenticationSession.user_id == user_id,
                AuthenticationSession.revoked_at.is_(None),
            )
            .values(revoked_at=revoked_at)
        )
        result = self._session.execute(statement)
        return int(result.rowcount or 0)

    def update_last_used(
        self,
        organization_id: UUID,
        session_id: UUID,
        last_used_at: datetime,
    ) -> AuthenticationSession | None:
        """Update last-used timestamp for a tenant-scoped authentication session."""
        statement = (
            update(AuthenticationSession)
            .where(
                AuthenticationSession.organization_id == organization_id,
                AuthenticationSession.id == session_id,
            )
            .values(last_used_at=last_used_at)
            .returning(AuthenticationSession)
        )
        return self._session.execute(statement).scalar_one_or_none()

    def delete_expired(self, expires_before: datetime) -> int:
        """Delete expired sessions and return count."""
        statement = delete(AuthenticationSession).where(
            AuthenticationSession.expires_at < expires_before,
        )
        result = self._session.execute(statement)
        return int(result.rowcount or 0)
