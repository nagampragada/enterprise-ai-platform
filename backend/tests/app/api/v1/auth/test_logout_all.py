from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from unittest.mock import Mock
from uuid import uuid4

from fastapi.testclient import TestClient

from app.dependencies import get_authentication_service, get_db_session
from app.main import app


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
        "user_id": str(uuid4()),
    }


def _build_valid_query() -> dict[str, str]:
    return {
        "organization_id": str(uuid4()),
    }


def test_affected_sessions_returns_200() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.return_value = 2

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout-all", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200


def test_zero_affected_sessions_still_returns_200() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.return_value = 0

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout-all", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200


def test_response_message_is_exact() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.return_value = 1

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout-all", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {"message": "Logged out from all sessions"}


def test_service_receives_organization_id() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.return_value = 1
    organization_id = uuid4()

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/auth/logout-all",
            params={"organization_id": str(organization_id)},
            json=_build_valid_payload(),
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert service_mock.logout_all.call_args.kwargs["organization_id"] == organization_id


def test_service_receives_user_id() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.return_value = 1
    user_id = uuid4()

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/auth/logout-all",
            params=_build_valid_query(),
            json={"user_id": str(user_id)},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert service_mock.logout_all.call_args.kwargs["user_id"] == user_id


def test_revoked_at_is_timezone_aware_utc() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.return_value = 1

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout-all", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    revoked_at = service_mock.logout_all.call_args.kwargs["revoked_at"]
    assert revoked_at.tzinfo == timezone.utc


def test_affected_sessions_commits_once() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.return_value = 3

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout-all", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert fake_session.commit_calls == 1


def test_zero_affected_sessions_does_not_commit() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.return_value = 0

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout-all", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert fake_session.commit_calls == 0


def test_invalid_organization_id_returns_422() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/auth/logout-all",
            params={"organization_id": "not-a-uuid"},
            json=_build_valid_payload(),
        )

    app.dependency_overrides.clear()

    assert response.status_code == 422
    service_mock.logout_all.assert_not_called()


def test_invalid_user_id_returns_422() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/auth/logout-all",
            params=_build_valid_query(),
            json={"user_id": "not-a-uuid"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 422
    service_mock.logout_all.assert_not_called()


def test_extra_request_fields_return_422() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    payload = _build_valid_payload()
    payload["extra"] = "value"

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout-all", params=_build_valid_query(), json=payload)

    app.dependency_overrides.clear()

    assert response.status_code == 422
    service_mock.logout_all.assert_not_called()


def test_openapi_request_body_references_logout_all_request() -> None:
    with TestClient(app) as client:
        openapi = client.get("/openapi.json")

    assert openapi.status_code == 200
    logout_all_post = openapi.json()["paths"]["/api/v1/auth/logout-all"]["post"]
    request_schema = logout_all_post["requestBody"]["content"]["application/json"]["schema"]
    assert request_schema == {"$ref": "#/components/schemas/LogoutAllRequest"}


def test_unexpected_exception_rolls_back() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.side_effect = RuntimeError("unexpected failure")

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout-all", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert fake_session.rollback_calls == 1


def test_unexpected_exception_returns_safe_500() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.side_effect = RuntimeError("boom")

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout-all", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}


def test_error_response_does_not_expose_exception_text_or_db_url() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.side_effect = RuntimeError(
        "db failed for postgresql://user:secret@127.0.0.1:5432/db"
    )

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout-all", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    body = response.json()
    assert body == {"detail": "Internal server error"}
    assert "postgresql://" not in str(body)
    assert "secret" not in str(body)
    assert "db failed" not in str(body)


def test_session_closes_after_success() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.return_value = 1

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout-all", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert fake_session.close_calls == 1


def test_session_closes_after_internal_error() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout_all.side_effect = RuntimeError("unexpected failure")

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout-all", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert fake_session.close_calls == 1
