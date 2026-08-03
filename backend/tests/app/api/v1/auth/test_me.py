from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.dependencies import get_db_session
from app.main import app
from infrastructure.security.tokens import AccessTokenPayload


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


class TrackingUserRepository:
    def __init__(self, user_to_return: object | None = None, side_effect: Exception | None = None) -> None:
        self.user_to_return = user_to_return
        self.side_effect = side_effect
        self.calls: list[tuple[object, object]] = []

    def get_by_id(self, organization_id, user_id):
        self.calls.append((organization_id, user_id))
        if self.side_effect is not None:
            raise self.side_effect
        return self.user_to_return


def _override_db_session(fake_session: FakeSession) -> None:
    def _db_session_override():
        try:
            yield fake_session
        finally:
            fake_session.close()

    app.dependency_overrides[get_db_session] = _db_session_override


def _valid_payload(user_id, organization_id) -> AccessTokenPayload:
    now = datetime.now(timezone.utc)
    return AccessTokenPayload(
        user_id=user_id,
        organization_id=organization_id,
        issued_at=now,
        expires_at=now + timedelta(minutes=5),
    )


@pytest.fixture(autouse=True)
def _clear_overrides_after_test():
    yield
    app.dependency_overrides.clear()


def test_valid_bearer_token_returns_200(monkeypatch: pytest.MonkeyPatch) -> None:
    token_user_id = uuid4()
    token_organization_id = uuid4()
    fake_session = FakeSession()
    _override_db_session(fake_session)

    user = SimpleNamespace(
        id=token_user_id,
        organization_id=token_organization_id,
        email="user@example.com",
        display_name="Example User",
        status="active",
    )
    tracking_repo = TrackingUserRepository(user_to_return=user)

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: _valid_payload(token_user_id, token_organization_id))
    monkeypatch.setattr("app.dependencies.UserRepository", lambda session: tracking_repo)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer valid-token"})

    assert response.status_code == 200


def test_response_matches_authenticated_user_response(monkeypatch: pytest.MonkeyPatch) -> None:
    token_user_id = uuid4()
    token_organization_id = uuid4()
    fake_session = FakeSession()
    _override_db_session(fake_session)

    user = SimpleNamespace(
        id=token_user_id,
        organization_id=token_organization_id,
        email="user@example.com",
        display_name="Example User",
        status="active",
    )
    tracking_repo = TrackingUserRepository(user_to_return=user)

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: _valid_payload(token_user_id, token_organization_id))
    monkeypatch.setattr("app.dependencies.UserRepository", lambda session: tracking_repo)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer valid-token"})

    assert response.status_code == 200
    assert response.json() == {
        "user_id": str(token_user_id),
        "organization_id": str(token_organization_id),
        "email": "user@example.com",
        "display_name": "Example User",
    }


def test_user_lookup_uses_identity_from_decoded_token(monkeypatch: pytest.MonkeyPatch) -> None:
    token_user_id = uuid4()
    token_organization_id = uuid4()
    fake_session = FakeSession()
    _override_db_session(fake_session)

    user = SimpleNamespace(
        id=token_user_id,
        organization_id=token_organization_id,
        email="user@example.com",
        display_name="Example User",
        status="active",
    )
    tracking_repo = TrackingUserRepository(user_to_return=user)

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: _valid_payload(token_user_id, token_organization_id))
    monkeypatch.setattr("app.dependencies.UserRepository", lambda session: tracking_repo)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer valid-token"})

    assert response.status_code == 200
    assert tracking_repo.calls == [(token_organization_id, token_user_id)]


def test_missing_authorization_header_returns_401(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_session = FakeSession()
    _override_db_session(fake_session)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me")

    assert response.status_code == 401


def test_wrong_auth_scheme_returns_401(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_session = FakeSession()
    _override_db_session(fake_session)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Basic abc"})

    assert response.status_code == 401


def test_empty_bearer_token_returns_401(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_session = FakeSession()
    _override_db_session(fake_session)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer   "})

    assert response.status_code == 401


def test_malformed_token_returns_401(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_session = FakeSession()
    _override_db_session(fake_session)

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: None)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer malformed"})

    assert response.status_code == 401


def test_invalid_or_expired_decoded_token_returns_401(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_session = FakeSession()
    _override_db_session(fake_session)

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: None)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer expired"})

    assert response.status_code == 401


def test_missing_user_returns_401(monkeypatch: pytest.MonkeyPatch) -> None:
    token_user_id = uuid4()
    token_organization_id = uuid4()
    fake_session = FakeSession()
    _override_db_session(fake_session)

    tracking_repo = TrackingUserRepository(user_to_return=None)

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: _valid_payload(token_user_id, token_organization_id))
    monkeypatch.setattr("app.dependencies.UserRepository", lambda session: tracking_repo)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer valid-token"})

    assert response.status_code == 401


def test_inactive_user_returns_401(monkeypatch: pytest.MonkeyPatch) -> None:
    token_user_id = uuid4()
    token_organization_id = uuid4()
    fake_session = FakeSession()
    _override_db_session(fake_session)

    user = SimpleNamespace(
        id=token_user_id,
        organization_id=token_organization_id,
        email="user@example.com",
        display_name="Example User",
        status="disabled",
    )
    tracking_repo = TrackingUserRepository(user_to_return=user)

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: _valid_payload(token_user_id, token_organization_id))
    monkeypatch.setattr("app.dependencies.UserRepository", lambda session: tracking_repo)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer valid-token"})

    assert response.status_code == 401


def test_generic_auth_error_message_is_exact(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_session = FakeSession()
    _override_db_session(fake_session)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me")

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or expired access token"}


def test_client_cannot_override_identity_with_query_or_body(monkeypatch: pytest.MonkeyPatch) -> None:
    token_user_id = uuid4()
    token_organization_id = uuid4()
    fake_session = FakeSession()
    _override_db_session(fake_session)

    user = SimpleNamespace(
        id=token_user_id,
        organization_id=token_organization_id,
        email="user@example.com",
        display_name="Example User",
        status="active",
    )
    tracking_repo = TrackingUserRepository(user_to_return=user)

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: _valid_payload(token_user_id, token_organization_id))
    monkeypatch.setattr("app.dependencies.UserRepository", lambda session: tracking_repo)

    with TestClient(app) as client:
        response = client.request(
            "GET",
            "/api/v1/auth/me",
            headers={"Authorization": "Bearer valid-token"},
            params={"organization_id": str(uuid4()), "user_id": str(uuid4())},
            json={"organization_id": str(uuid4()), "user_id": str(uuid4())},
        )

    assert response.status_code == 200
    assert tracking_repo.calls == [(token_organization_id, token_user_id)]


def test_endpoint_does_not_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    token_user_id = uuid4()
    token_organization_id = uuid4()
    fake_session = FakeSession()
    _override_db_session(fake_session)

    user = SimpleNamespace(
        id=token_user_id,
        organization_id=token_organization_id,
        email="user@example.com",
        display_name="Example User",
        status="active",
    )
    tracking_repo = TrackingUserRepository(user_to_return=user)

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: _valid_payload(token_user_id, token_organization_id))
    monkeypatch.setattr("app.dependencies.UserRepository", lambda session: tracking_repo)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer valid-token"})

    assert response.status_code == 200
    assert fake_session.commit_calls == 0


def test_unexpected_repository_exception_returns_safe_500(monkeypatch: pytest.MonkeyPatch) -> None:
    token_user_id = uuid4()
    token_organization_id = uuid4()
    fake_session = FakeSession()
    _override_db_session(fake_session)

    tracking_repo = TrackingUserRepository(side_effect=RuntimeError("db read failure"))

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: _valid_payload(token_user_id, token_organization_id))
    monkeypatch.setattr("app.dependencies.UserRepository", lambda session: tracking_repo)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer valid-token"})

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}


def test_error_response_does_not_expose_token_exception_or_db_url(monkeypatch: pytest.MonkeyPatch) -> None:
    token_user_id = uuid4()
    token_organization_id = uuid4()
    fake_session = FakeSession()
    _override_db_session(fake_session)

    tracking_repo = TrackingUserRepository(
        side_effect=RuntimeError("db failed for postgresql://user:secret@127.0.0.1:5432/db token=abc")
    )

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: _valid_payload(token_user_id, token_organization_id))
    monkeypatch.setattr("app.dependencies.UserRepository", lambda session: tracking_repo)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer highly-sensitive-token"})

    assert response.status_code == 500
    body = response.json()
    assert body == {"detail": "Internal server error"}
    assert "postgresql://" not in str(body)
    assert "secret" not in str(body)
    assert "token" not in str(body)
    assert "db failed" not in str(body)


def test_session_closes_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    token_user_id = uuid4()
    token_organization_id = uuid4()
    fake_session = FakeSession()
    _override_db_session(fake_session)

    user = SimpleNamespace(
        id=token_user_id,
        organization_id=token_organization_id,
        email="user@example.com",
        display_name="Example User",
        status="active",
    )
    tracking_repo = TrackingUserRepository(user_to_return=user)

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: _valid_payload(token_user_id, token_organization_id))
    monkeypatch.setattr("app.dependencies.UserRepository", lambda session: tracking_repo)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer valid-token"})

    assert response.status_code == 200
    assert fake_session.close_calls == 1


def test_session_closes_after_internal_error(monkeypatch: pytest.MonkeyPatch) -> None:
    token_user_id = uuid4()
    token_organization_id = uuid4()
    fake_session = FakeSession()
    _override_db_session(fake_session)

    tracking_repo = TrackingUserRepository(side_effect=RuntimeError("unexpected db error"))

    monkeypatch.setattr("app.dependencies.decode_access_token", lambda token: _valid_payload(token_user_id, token_organization_id))
    monkeypatch.setattr("app.dependencies.UserRepository", lambda session: tracking_repo)

    with TestClient(app) as client:
        response = client.get("/api/v1/auth/me", headers={"Authorization": "Bearer valid-token"})

    assert response.status_code == 500
    assert fake_session.close_calls == 1
