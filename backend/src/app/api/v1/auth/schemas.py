"""Pydantic schemas for authentication API contracts."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator


class LoginRequest(BaseModel):
    """Request payload for user login."""

    model_config = ConfigDict(extra="forbid")

    organization_id: UUID = Field(description="Organization identifier for tenant-scoped authentication.")
    email: EmailStr = Field(description="User email address.")
    password: str = Field(description="User plain-text password.")

    @field_validator("email", mode="before")
    @classmethod
    def _strip_email(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("password")
    @classmethod
    def _reject_blank_password(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("password must not be blank")
        return value


class RefreshRequest(BaseModel):
    """Request payload for token refresh."""

    model_config = ConfigDict(extra="forbid", strict=True)

    refresh_token: str = Field(description="Refresh token issued at login.")

    @field_validator("refresh_token")
    @classmethod
    def _reject_blank_token(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("refresh_token must not be blank")
        return value


class LogoutRequest(BaseModel):
    """Request payload for revoking one session."""

    model_config = ConfigDict(extra="forbid")

    session_id: UUID = Field(description="Authentication session identifier to revoke.")


class LogoutAllRequest(BaseModel):
    """Request payload for revoking all user sessions."""

    model_config = ConfigDict(extra="forbid")

    user_id: UUID = Field(description="User identifier whose sessions should be revoked.")


class AuthenticationTokensResponse(BaseModel):
    """Response payload containing issued authentication tokens."""

    model_config = ConfigDict(extra="forbid", strict=True)

    access_token: str = Field(description="Signed JWT access token.")
    refresh_token: str = Field(description="Opaque refresh token.")
    token_type: Literal["bearer"] = Field(default="bearer", description="Token type for Authorization header usage.")
    expires_in_seconds: int = Field(gt=0, description="Access-token lifetime in seconds.")


class AuthenticatedUserResponse(BaseModel):
    """Response payload for authenticated user profile information."""

    model_config = ConfigDict(extra="forbid", strict=True)

    user_id: UUID = Field(description="Authenticated user identifier.")
    organization_id: UUID = Field(description="Tenant organization identifier.")
    email: EmailStr = Field(description="Authenticated user email address.")
    display_name: str = Field(description="Display name shown to end users.")


class LoginResponse(BaseModel):
    """Response payload for successful login."""

    model_config = ConfigDict(extra="forbid", strict=True)

    user: AuthenticatedUserResponse = Field(description="Authenticated user details.")
    tokens: AuthenticationTokensResponse = Field(description="Issued access and refresh tokens.")


class MessageResponse(BaseModel):
    """Simple message response payload."""

    model_config = ConfigDict(extra="forbid", strict=True)

    message: str = Field(description="Human-readable status message.")
