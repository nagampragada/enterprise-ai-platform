"""Authentication API routes."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.api.v1.auth.schemas import (
    AuthenticatedUserResponse,
    AuthenticationTokensResponse,
    LoginRequest,
    LoginResponse,
    LogoutAllRequest,
    LogoutRequest,
    MessageResponse,
    RefreshRequest,
)
from app.dependencies import CurrentUser, get_authentication_service, get_current_user, get_db_session
from application.services.authentication_service import AuthenticationService


auth_router = APIRouter(prefix="/auth", tags=["auth"])


@auth_router.get("/me", response_model=AuthenticatedUserResponse)
def me(current_user: CurrentUser = Depends(get_current_user)) -> AuthenticatedUserResponse:
    return AuthenticatedUserResponse(
        user_id=current_user.user_id,
        organization_id=current_user.organization_id,
        email=current_user.email,
        display_name=current_user.display_name,
    )


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


@auth_router.post("/logout-all", response_model=MessageResponse)
def logout_all(
    organization_id: UUID,
    payload: LogoutAllRequest,
    db_session: Session = Depends(get_db_session),
    authentication_service: AuthenticationService = Depends(get_authentication_service),
) -> MessageResponse:
    try:
        affected_count = authentication_service.logout_all(
            organization_id=organization_id,
            user_id=payload.user_id,
            revoked_at=datetime.now(timezone.utc),
        )
        if affected_count > 0:
            db_session.commit()
    except Exception:
        db_session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error",
        )

    return MessageResponse(message="Logged out from all sessions")
