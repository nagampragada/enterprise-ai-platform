from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import Mock

from fastapi.testclient import TestClient

from app.dependencies import get_authentication_service, get_db_session
from app.main import app
from application.services.authentication_service import AuthenticationTokens


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
        "refresh_token": "presented-refresh-token",
    }


def test_successful_refresh_returns_200_and_schema() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.refresh.return_value = AuthenticationTokens(
        access_token="new-access-token",
        refresh_token="new-refresh-token",
        expires_in_seconds=900,
    )

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "access_token": "new-access-token",
        "refresh_token": "new-refresh-token",
        "token_type": "bearer",
        "expires_in_seconds": 900,
    }
    assert fake_session.commit_calls == 1
    assert fake_session.rollback_calls == 0


def test_token_type_is_bearer() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.refresh.return_value = AuthenticationTokens(
        access_token="new-access-token",
        refresh_token="new-refresh-token",
        expires_in_seconds=900,
    )

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["token_type"] == "bearer"


def test_service_receives_presented_refresh_token() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.refresh.return_value = AuthenticationTokens(
        access_token="new-access-token",
        refresh_token="new-refresh-token",
        expires_in_seconds=900,
    )

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    service_mock.refresh.assert_called_once_with("presented-refresh-token")


def test_invalid_refresh_token_returns_401() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.refresh.return_value = None

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 401


def test_401_uses_exact_generic_message() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.refresh.return_value = None

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid refresh token"}


def test_blank_refresh_token_returns_422() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json={"refresh_token": "   "})

    app.dependency_overrides.clear()

    assert response.status_code == 422
    service_mock.refresh.assert_not_called()


def test_extra_fields_return_422() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    payload = _build_valid_payload()
    payload["extra"] = "value"

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json=payload)

    app.dependency_overrides.clear()

    assert response.status_code == 422
    service_mock.refresh.assert_not_called()


def test_successful_refresh_commits_once() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.refresh.return_value = AuthenticationTokens(
        access_token="new-access-token",
        refresh_token="new-refresh-token",
        expires_in_seconds=900,
    )

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert fake_session.commit_calls == 1


def test_invalid_refresh_does_not_commit() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.refresh.return_value = None

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 401
    assert fake_session.commit_calls == 0


def test_unexpected_exception_rolls_back() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.refresh.side_effect = RuntimeError("unexpected refresh failure")

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert fake_session.rollback_calls == 1


def test_unexpected_exception_returns_safe_500() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.refresh.side_effect = RuntimeError("boom")

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}


def test_error_response_does_not_contain_presented_token_or_exception_text() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    presented_token = "presented-refresh-token"
    service_mock.refresh.side_effect = RuntimeError(
        "failed for token presented-refresh-token and url postgresql://user:secret@127.0.0.1/db"
    )

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json={"refresh_token": presented_token})

    app.dependency_overrides.clear()

    assert response.status_code == 500
    body = response.json()
    assert body == {"detail": "Internal server error"}
    assert presented_token not in str(body)
    assert "postgresql://" not in str(body)
    assert "secret" not in str(body)


def test_session_closes_after_success() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.refresh.return_value = AuthenticationTokens(
        access_token="new-access-token",
        refresh_token="new-refresh-token",
        expires_in_seconds=900,
    )

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert fake_session.close_calls == 1


def test_session_closes_after_internal_error() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.refresh.side_effect = RuntimeError("unexpected failure")

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/refresh", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert fake_session.close_calls == 1
