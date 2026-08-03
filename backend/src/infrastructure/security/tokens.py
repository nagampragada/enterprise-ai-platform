"""Token utilities for JWT access tokens and hashed refresh tokens."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

import jwt
from jwt.exceptions import DecodeError, ExpiredSignatureError, InvalidTokenError

from app.config import get_settings


@dataclass(frozen=True)
class AccessTokenPayload:
    user_id: UUID
    organization_id: UUID
    issued_at: datetime
    expires_at: datetime


def create_access_token(
    user_id: UUID,
    organization_id: UUID,
    expires_delta: timedelta | None = None,
) -> str:
    settings = get_settings()
    now = datetime.now(timezone.utc)
    expires_in = expires_delta or timedelta(minutes=settings.access_token_lifetime_minutes)
    expires_at = now + expires_in

    payload = {
        "sub": str(user_id),
        "organization_id": str(organization_id),
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
        "type": "access",
    }

    return jwt.encode(payload, settings.jwt_secret_key, algorithm="HS256")


def decode_access_token(token: str) -> AccessTokenPayload | None:
    if not token:
        return None

    settings = get_settings()
    try:
        decoded = jwt.decode(token, settings.jwt_secret_key, algorithms=["HS256"])
    except (ExpiredSignatureError, DecodeError, InvalidTokenError):
        return None

    if decoded.get("type") != "access":
        return None

    try:
        user_id = UUID(decoded["sub"])
        organization_id = UUID(decoded["organization_id"])
        issued_at = datetime.fromtimestamp(int(decoded["iat"]), tz=timezone.utc)
        expires_at = datetime.fromtimestamp(int(decoded["exp"]), tz=timezone.utc)
    except (KeyError, TypeError, ValueError, OverflowError):
        return None

    return AccessTokenPayload(
        user_id=user_id,
        organization_id=organization_id,
        issued_at=issued_at,
        expires_at=expires_at,
    )


def generate_refresh_token() -> str:
    # token_urlsafe uses cryptographically secure randomness from the OS CSPRNG.
    return secrets.token_urlsafe(48)


def hash_refresh_token(refresh_token: str) -> str:
    if not refresh_token:
        raise ValueError("refresh token must not be empty")

    # HMAC-SHA-256 with a server-side secret protects against precomputed hash attacks
    # if database contents are leaked, while remaining fast for high-throughput token checks.
    settings = get_settings()
    digest = hmac.new(
        settings.refresh_token_hash_secret.encode("utf-8"),
        refresh_token.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest


def verify_refresh_token_hash(refresh_token: str, refresh_token_hash: str) -> bool:
    if not refresh_token or not refresh_token_hash:
        return False

    # Reject malformed hashes instead of raising from invalid input.
    if len(refresh_token_hash) != 64:
        return False

    try:
        int(refresh_token_hash, 16)
    except ValueError:
        return False

    expected_hash = hash_refresh_token(refresh_token)
    return hmac.compare_digest(expected_hash, refresh_token_hash)
