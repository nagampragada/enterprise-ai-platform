from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from app.dependencies import get_authentication_service, get_db_session
from app.main import app
from application.services.authentication_service import (
    AuthenticatedUser,
    AuthenticationTokens,
    LoginResult,
)


@dataclass
class FakeSession:
    commit_calls: int = 0
    rollback_calls: int = 0
    close_calls: int = 0

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.close_calls += 1


def _override_dependencies(service_mock: Mock, fake_session: FakeSession) -> None:
    def _db_session_override():
        try:
            yield fake_session
        finally:
            fake_session.close()

    def _authentication_service_override():
        return service_mock

    app.dependency_overrides[get_db_session] = _db_session_override
    app.dependency_overrides[get_authentication_service] = _authentication_service_override


def _build_valid_payload() -> dict[str, str]:
    return {
        "organization_id": str(uuid4()),
        "email": "user@example.com",
        "password": "password",
    }


def test_successful_login_returns_200_and_schema() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    organization_id = uuid4()
    user_id = uuid4()
    service_mock.login.return_value = LoginResult(
        user=AuthenticatedUser(
            user_id=user_id,
            organization_id=organization_id,
            email="user@example.com",
            display_name="Example User",
        ),
        tokens=AuthenticationTokens(
            access_token="access-token",
            refresh_token="refresh-token",
            expires_in_seconds=900,
        ),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/auth/login",
            json={
                "organization_id": str(organization_id),
                "email": "user@example.com",
                "password": "password",
            },
            headers={"user-agent": "pytest-agent"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "user": {
            "user_id": str(user_id),
            "organization_id": str(organization_id),
            "email": "user@example.com",
            "display_name": "Example User",
        },
        "tokens": {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "token_type": "bearer",
            "expires_in_seconds": 900,
        },
    }
    assert fake_session.commit_calls == 1
    assert fake_session.rollback_calls == 0

    service_mock.login.assert_called_once()
    call_kwargs = service_mock.login.call_args.kwargs
    assert call_kwargs["organization_id"] == organization_id
    assert call_kwargs["email"] == "user@example.com"
    assert call_kwargs["password"] == "password"
    assert call_kwargs["ip_address"] == "testclient"
    assert call_kwargs["user_agent"] == "pytest-agent"


def test_invalid_credentials_return_401_with_generic_message() -> None:
    service_mock = Mock()
    service_mock.login.return_value = None
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/login", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid email or password"}
    assert fake_session.commit_calls == 0
    assert fake_session.rollback_calls == 0


def test_request_validation_rejects_extra_request_fields() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    payload = _build_valid_payload()
    payload["unexpected"] = "value"

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/login", json=payload)

    app.dependency_overrides.clear()

    assert response.status_code == 422
    service_mock.login.assert_not_called()
    assert fake_session.commit_calls == 0


def test_request_validation_rejects_invalid_email() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    payload = _build_valid_payload()
    payload["email"] = "not-an-email"

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/login", json=payload)

    app.dependency_overrides.clear()

    assert response.status_code == 422
    service_mock.login.assert_not_called()
    assert fake_session.commit_calls == 0


def test_request_validation_rejects_blank_password() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    payload = _build_valid_payload()
    payload["password"] = "   "

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/login", json=payload)

    app.dependency_overrides.clear()

    assert response.status_code == 422
    service_mock.login.assert_not_called()
    assert fake_session.commit_calls == 0


def test_internal_exception_returns_safe_500_without_leaking_credentials() -> None:
    service_mock = Mock()
    service_mock.login.side_effect = RuntimeError(
        "database failed for postgresql://user:secret-pass@127.0.0.1:5432/db"
    )
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/login", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    body = response.json()
    assert body == {"detail": "Internal server error"}
    assert "secret-pass" not in str(body)
    assert "postgresql://" not in str(body)
    assert fake_session.commit_calls == 0
    assert fake_session.rollback_calls == 1


def test_openapi_schema_for_login_references_login_request() -> None:
    with TestClient(app) as client:
        response = client.get("/openapi.json")

    assert response.status_code == 200
    schema_ref = response.json()["paths"]["/api/v1/auth/login"]["post"]["requestBody"]["content"]["application/json"]["schema"]["$ref"]
    assert schema_ref == "#/components/schemas/LoginRequest"


def test_database_session_closed_after_successful_request() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    organization_id = uuid4()
    service_mock.login.return_value = LoginResult(
        user=AuthenticatedUser(
            user_id=uuid4(),
            organization_id=organization_id,
            email="user@example.com",
            display_name="Example User",
        ),
        tokens=AuthenticationTokens(
            access_token="access-token",
            refresh_token="refresh-token",
            expires_in_seconds=900,
        ),
    )

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/login", json={
            "organization_id": str(organization_id),
            "email": "user@example.com",
            "password": "password",
        })

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert fake_session.close_calls == 1


def test_database_session_closed_after_internal_error_request() -> None:
    service_mock = Mock()
    service_mock.login.side_effect = RuntimeError("unexpected failure")
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/login", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert fake_session.close_calls == 1
