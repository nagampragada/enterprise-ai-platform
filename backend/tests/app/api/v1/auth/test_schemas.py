from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

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


def test_login_request_valid_succeeds() -> None:
    organization_id = uuid4()

    model = LoginRequest(
        organization_id=organization_id,
        email="user@example.com",
        password="correct-horse-battery-staple",
    )

    assert model.organization_id == organization_id
    assert model.email == "user@example.com"


def test_login_request_email_whitespace_is_trimmed() -> None:
    model = LoginRequest(
        organization_id=uuid4(),
        email="  user@example.com  ",
        password="password",
    )

    assert model.email == "user@example.com"


def test_login_request_invalid_email_fails() -> None:
    with pytest.raises(ValidationError):
        LoginRequest(
            organization_id=uuid4(),
            email="not-an-email",
            password="password",
        )


def test_login_request_blank_password_fails() -> None:
    with pytest.raises(ValidationError):
        LoginRequest(
            organization_id=uuid4(),
            email="user@example.com",
            password="   ",
        )


def test_login_request_extra_fields_fail() -> None:
    with pytest.raises(ValidationError):
        LoginRequest(
            organization_id=uuid4(),
            email="user@example.com",
            password="password",
            unexpected="value",
        )


def test_refresh_request_valid_token_succeeds() -> None:
    model = RefreshRequest(refresh_token="refresh-token")

    assert model.refresh_token == "refresh-token"


def test_refresh_request_blank_token_fails() -> None:
    with pytest.raises(ValidationError):
        RefreshRequest(refresh_token="   ")


def test_refresh_request_extra_fields_fail() -> None:
    with pytest.raises(ValidationError):
        RefreshRequest(refresh_token="refresh-token", extra_field="x")


def test_logout_request_valid_uuid_succeeds() -> None:
    session_id = uuid4()

    model = LogoutRequest(session_id=session_id)

    assert model.session_id == session_id


def test_logout_request_invalid_uuid_fails() -> None:
    with pytest.raises(ValidationError):
        LogoutRequest(session_id="not-a-uuid")


def test_logout_all_request_valid_uuid_succeeds() -> None:
    user_id = uuid4()

    model = LogoutAllRequest(user_id=user_id)

    assert model.user_id == user_id


def test_logout_all_request_invalid_uuid_fails() -> None:
    with pytest.raises(ValidationError):
        LogoutAllRequest(user_id="not-a-uuid")


def test_authentication_tokens_response_valid_succeeds() -> None:
    model = AuthenticationTokensResponse(
        access_token="access",
        refresh_token="refresh",
        token_type="bearer",
        expires_in_seconds=900,
    )

    assert model.expires_in_seconds == 900


def test_authentication_tokens_response_token_type_defaults_to_bearer() -> None:
    model = AuthenticationTokensResponse(
        access_token="access",
        refresh_token="refresh",
        expires_in_seconds=900,
    )

    assert model.token_type == "bearer"


def test_authentication_tokens_response_non_bearer_token_type_fails() -> None:
    with pytest.raises(ValidationError):
        AuthenticationTokensResponse(
            access_token="access",
            refresh_token="refresh",
            token_type="basic",
            expires_in_seconds=900,
        )


def test_authentication_tokens_response_non_positive_expiry_fails() -> None:
    with pytest.raises(ValidationError):
        AuthenticationTokensResponse(
            access_token="access",
            refresh_token="refresh",
            expires_in_seconds=0,
        )


def test_authenticated_user_response_valid_succeeds() -> None:
    model = AuthenticatedUserResponse(
        user_id=uuid4(),
        organization_id=uuid4(),
        email="user@example.com",
        display_name="Example User",
    )

    assert model.display_name == "Example User"


def test_authenticated_user_response_invalid_email_fails() -> None:
    with pytest.raises(ValidationError):
        AuthenticatedUserResponse(
            user_id=uuid4(),
            organization_id=uuid4(),
            email="invalid",
            display_name="Example User",
        )


def test_authenticated_user_response_blank_display_name_currently_allowed() -> None:
    # Current schema does not include non-blank validation for display_name.
    model = AuthenticatedUserResponse(
        user_id=uuid4(),
        organization_id=uuid4(),
        email="user@example.com",
        display_name="   ",
    )

    assert model.display_name == "   "


def test_login_response_nested_models_validate() -> None:
    model = LoginResponse(
        user={
            "user_id": uuid4(),
            "organization_id": uuid4(),
            "email": "user@example.com",
            "display_name": "Example User",
        },
        tokens={
            "access_token": "access",
            "refresh_token": "refresh",
            "expires_in_seconds": 600,
        },
    )

    assert model.tokens.token_type == "bearer"
    assert model.user.email == "user@example.com"


def test_message_response_valid_message_succeeds() -> None:
    model = MessageResponse(message="Logged out")

    assert model.message == "Logged out"


def test_message_response_blank_message_currently_allowed() -> None:
    # Current schema does not include non-blank validation for message.
    model = MessageResponse(message="   ")

    assert model.message == "   "
