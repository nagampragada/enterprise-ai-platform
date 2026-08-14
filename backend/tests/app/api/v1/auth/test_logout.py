from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from unittest.mock import Mock
from uuid import uuid4

from fastapi.testclient import TestClient

from app.dependencies import CurrentUser, get_authentication_service, get_current_user, get_db_session
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
    app.dependency_overrides[get_current_user] = lambda: CurrentUser(
        user_id=AUTHENTICATED_USER_ID,
        organization_id=AUTHENTICATED_ORGANIZATION_ID,
        email="user@example.com",
        display_name="Example User",
    )


AUTHENTICATED_USER_ID = uuid4()
AUTHENTICATED_ORGANIZATION_ID = uuid4()


def _build_valid_payload() -> dict[str, str]:
    return {
        "session_id": str(uuid4()),
    }


def _build_valid_query() -> dict[str, str]:
    return {
        "organization_id": str(uuid4()),
    }


def test_successful_logout_returns_http_200() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.return_value = True

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200


def test_successful_logout_response_message_is_exact() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.return_value = True

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {"message": "Logged out successfully"}


def test_service_receives_authenticated_organization_and_user_ids() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.return_value = True
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/auth/logout",
            json=_build_valid_payload(),
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert service_mock.logout.call_args.kwargs["organization_id"] == AUTHENTICATED_ORGANIZATION_ID
    assert service_mock.logout.call_args.kwargs["user_id"] == AUTHENTICATED_USER_ID


def test_service_receives_payload_session_id() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.return_value = True
    session_id = uuid4()

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/auth/logout",
            params=_build_valid_query(),
            json={"session_id": str(session_id)},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert service_mock.logout.call_args.kwargs["session_id"] == session_id


def test_revoked_at_is_timezone_aware_utc() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.return_value = True

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    revoked_at = service_mock.logout.call_args.kwargs["revoked_at"]
    assert revoked_at.tzinfo == timezone.utc


def test_successful_logout_commits_exactly_once() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.return_value = True

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert fake_session.commit_calls == 1


def test_missing_session_returns_http_404() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.return_value = False

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 404


def test_404_detail_is_exact_session_not_found() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.return_value = False

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "Session not found"}


def test_missing_session_does_not_commit() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.return_value = False

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 404
    assert fake_session.commit_calls == 0


def test_query_organization_id_does_not_control_logout() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/auth/logout",
            params={"organization_id": "not-a-uuid"},
            json=_build_valid_payload(),
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert service_mock.logout.call_args.kwargs["organization_id"] == AUTHENTICATED_ORGANIZATION_ID


def test_invalid_session_id_returns_422() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/auth/logout",
            params=_build_valid_query(),
            json={"session_id": "not-a-uuid"},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 422
    service_mock.logout.assert_not_called()


def test_extra_request_fields_return_422() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    payload = _build_valid_payload()
    payload["extra"] = "value"

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=payload)

    app.dependency_overrides.clear()

    assert response.status_code == 422
    service_mock.logout.assert_not_called()


def test_unexpected_service_exception_causes_rollback() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.side_effect = RuntimeError("unexpected logout failure")

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert fake_session.rollback_calls == 1


def test_unexpected_exception_returns_safe_500() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.side_effect = RuntimeError("boom")

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}


def test_error_response_does_not_expose_exception_text_or_database_url() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.side_effect = RuntimeError(
        "db failed for postgresql://user:secret@127.0.0.1:5432/db"
    )

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    body = response.json()
    assert body == {"detail": "Internal server error"}
    assert "postgresql://" not in str(body)
    assert "secret" not in str(body)
    assert "db failed" not in str(body)


def test_session_closes_after_successful_request() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.return_value = True

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert fake_session.close_calls == 1


def test_session_closes_after_internal_error_request() -> None:
    service_mock = Mock()
    fake_session = FakeSession()
    _override_dependencies(service_mock=service_mock, fake_session=fake_session)

    service_mock.logout.side_effect = RuntimeError("unexpected failure")

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", params=_build_valid_query(), json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert fake_session.close_calls == 1


def test_openapi_logout_request_body_references_logout_request_schema() -> None:
    with TestClient(app) as client:
        openapi = client.get("/openapi.json")

    assert openapi.status_code == 200
    logout_post = openapi.json()["paths"]["/api/v1/auth/logout"]["post"]
    request_schema = logout_post["requestBody"]["content"]["application/json"]["schema"]
    assert request_schema == {"$ref": "#/components/schemas/LogoutRequest"}


def test_unauthenticated_logout_returns_401() -> None:
    service_mock = Mock()
    fake_session = FakeSession()

    def _db_session_override():
        try:
            yield fake_session
        finally:
            fake_session.close()

    app.dependency_overrides[get_db_session] = _db_session_override
    app.dependency_overrides[get_authentication_service] = lambda: service_mock

    with TestClient(app) as client:
        response = client.post("/api/v1/auth/logout", json=_build_valid_payload())

    app.dependency_overrides.clear()

    assert response.status_code == 401
    service_mock.logout.assert_not_called()
