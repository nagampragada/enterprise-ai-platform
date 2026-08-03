"""Authentication API routes."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.api.v1.auth.schemas import (
    AuthenticationTokensResponse,
    LoginRequest,
    LoginResponse,
    LogoutRequest,
    MessageResponse,
    RefreshRequest,
)
from app.dependencies import get_authentication_service, get_db_session
from application.services.authentication_service import AuthenticationService


auth_router = APIRouter(prefix="/auth", tags=["auth"])


@auth_router.post("/login", response_model=LoginResponse)
def login(
    payload: LoginRequest,
    request: Request,
    db_session: Session = Depends(get_db_session),
    authentication_service: AuthenticationService = Depends(get_authentication_service),
) -> LoginResponse:
    client_ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")

    try:
        result = authentication_service.login(
            organization_id=payload.organization_id,
            email=payload.email,
            password=payload.password,
            ip_address=client_ip,
            user_agent=user_agent,
        )
        if result is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password",
            )

        db_session.commit()
    except HTTPException:
        raise
    except Exception:
        db_session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        )

    return LoginResponse(
        user={
            "user_id": result.user.user_id,
            "organization_id": result.user.organization_id,
            "email": result.user.email,
            "display_name": result.user.display_name,
        },
        tokens={
            "access_token": result.tokens.access_token,
            "refresh_token": result.tokens.refresh_token,
            "token_type": "bearer",
            "expires_in_seconds": result.tokens.expires_in_seconds,
        },
    )


@auth_router.post("/refresh", response_model=AuthenticationTokensResponse)
def refresh(
    payload: RefreshRequest,
    db_session: Session = Depends(get_db_session),
    authentication_service: AuthenticationService = Depends(get_authentication_service),
) -> AuthenticationTokensResponse:
    try:
        result = authentication_service.refresh(payload.refresh_token)
        if result is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid refresh token",
            )

        db_session.commit()
    except HTTPException:
        raise
    except Exception:
        db_session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        )

    return AuthenticationTokensResponse(
        access_token=result.access_token,
        refresh_token=result.refresh_token,
        token_type="bearer",
        expires_in_seconds=result.expires_in_seconds,
    )


@auth_router.post("/logout", response_model=MessageResponse)
def logout(
    organization_id: UUID,
    payload: LogoutRequest,
    db_session: Session = Depends(get_db_session),
    authentication_service: AuthenticationService = Depends(get_authentication_service),
) -> MessageResponse:
    try:
        was_revoked = authentication_service.logout(
            organization_id=organization_id,
            session_id=payload.session_id,
            revoked_at=datetime.now(timezone.utc),
        )
        if not was_revoked:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Session not found",
            )

        db_session.commit()
    except HTTPException:
        raise
    except Exception:
        db_session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        )

    return MessageResponse(message="Logged out successfully")
