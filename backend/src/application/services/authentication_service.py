"""Application service for authentication flows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from app.config import get_settings
from infrastructure.db.models import AuthenticationSession
from infrastructure.repositories.authentication_session_repository import AuthenticationSessionRepository
from infrastructure.repositories.user_repository import UserRepository
from infrastructure.security.passwords import verify_password
from infrastructure.security.tokens import (
    create_access_token,
    generate_refresh_token,
    hash_refresh_token,
)

_REFRESH_SESSION_LIFETIME = timedelta(days=30)


@dataclass(frozen=True)
class AuthenticationTokens:
    access_token: str
    refresh_token: str
    expires_in_seconds: int


@dataclass(frozen=True)
class AuthenticatedUser:
    user_id: UUID
    organization_id: UUID
    email: str
    display_name: str


@dataclass(frozen=True)
class LoginResult:
    user: AuthenticatedUser
    tokens: AuthenticationTokens


class AuthenticationService:
    """Handles authentication workflows using repository and token utilities."""

    def __init__(
        self,
        user_repository: UserRepository,
        authentication_session_repository: AuthenticationSessionRepository,
    ) -> None:
        self._user_repository = user_repository
        self._authentication_session_repository = authentication_session_repository

    def login(
        self,
        organization_id: UUID,
        email: str,
        password: str,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ) -> LoginResult | None:
        normalized_email = email.strip().lower()
        user = self._user_repository.get_by_normalized_email(
            organization_id=organization_id,
            normalized_email=normalized_email,
        )
        if user is None:
            return None

        try:
            password_matches = verify_password(password, user.password_hash)
        except ValueError:
            return None

        if not password_matches:
            return None

        if user.status != "active":
            return None

        now = _utc_now()
        access_expires_delta = _access_token_expires_delta()
        access_token = create_access_token(
            user_id=user.id,
            organization_id=user.organization_id,
            expires_delta=access_expires_delta,
        )
        refresh_token = generate_refresh_token()
        refresh_token_hash = hash_refresh_token(refresh_token)

        session = AuthenticationSession(
            id=uuid4(),
            organization_id=user.organization_id,
            user_id=user.id,
            refresh_token_hash=refresh_token_hash.encode("utf-8"),
            created_at=now,
            expires_at=now + _REFRESH_SESSION_LIFETIME,
            revoked_at=None,
            last_used_at=now,
            ip_address=ip_address,
            user_agent=user_agent,
        )
        self._authentication_session_repository.add(session)
        self._user_repository.update_last_login(
            organization_id=user.organization_id,
            user_id=user.id,
            last_login_at=now,
        )

        return LoginResult(
            user=AuthenticatedUser(
                user_id=user.id,
                organization_id=user.organization_id,
                email=user.email,
                display_name=user.display_name,
            ),
            tokens=AuthenticationTokens(
                access_token=access_token,
                refresh_token=refresh_token,
                expires_in_seconds=int(access_expires_delta.total_seconds()),
            ),
        )

    def refresh(self, refresh_token: str) -> AuthenticationTokens | None:
        try:
            refresh_token_hash = hash_refresh_token(refresh_token)
        except ValueError:
            return None

        session = self._authentication_session_repository.get_by_refresh_token_hash(refresh_token_hash)
        if session is None:
            return None

        now = _utc_now()
        if session.revoked_at is not None:
            return None

        if session.expires_at <= now:
            return None

        user = self._user_repository.get_by_id(
            organization_id=session.organization_id,
            user_id=session.user_id,
        )
        if user is None or user.status != "active":
            return None

        rotated_refresh_token = generate_refresh_token()
        session.refresh_token_hash = hash_refresh_token(rotated_refresh_token).encode("utf-8")
        session.last_used_at = now

        access_expires_delta = _access_token_expires_delta()
        access_token = create_access_token(
            user_id=user.id,
            organization_id=user.organization_id,
            expires_delta=access_expires_delta,
        )

        return AuthenticationTokens(
            access_token=access_token,
            refresh_token=rotated_refresh_token,
            expires_in_seconds=int(access_expires_delta.total_seconds()),
        )

    def logout(self, organization_id: UUID, user_id: UUID, session_id: UUID, revoked_at: datetime) -> bool:
        revoked_session = self._authentication_session_repository.revoke(
            organization_id=organization_id,
            user_id=user_id,
            session_id=session_id,
            revoked_at=revoked_at,
        )
        return revoked_session is not None

    def logout_all(self, organization_id: UUID, user_id: UUID, revoked_at: datetime) -> int:
        return self._authentication_session_repository.revoke_all_for_user(
            organization_id=organization_id,
            user_id=user_id,
            revoked_at=revoked_at,
        )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _access_token_expires_delta() -> timedelta:
    settings = get_settings()
    return timedelta(minutes=settings.access_token_lifetime_minutes)
