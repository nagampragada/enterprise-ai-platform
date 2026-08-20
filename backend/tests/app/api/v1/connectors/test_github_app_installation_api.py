from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient

from app.config import GitHubAppSettings
from app.dependencies import (
    ConnectorAdministrator,
    get_connector_administrator,
    get_db_session,
    get_github_app_installation_service,
    get_github_app_settings,
    get_secret_store,
)
from app.main import app, configure_github_app
from application.ports.secret_store import SecretReference, SecretValue
from application.services.github_app_installation_service import (
    GitHubInstallationInitiation,
    GitHubInstallationStatus,
    GitHubSetupRedirect,
)


NOW = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
STATE = "s" * 64


class Session:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def setup(service=None, admin=None):
    service = service or Mock()
    session = Session()
    admin = admin or ConnectorAdministrator(uuid4(), uuid4())

    def db():
        yield session

    app.dependency_overrides[get_db_session] = db
    app.dependency_overrides[get_connector_administrator] = lambda: admin
    app.dependency_overrides[get_github_app_installation_service] = lambda: service
    return TestClient(app), service, session, admin


@pytest.fixture(autouse=True)
def clear():
    yield
    app.dependency_overrides.clear()
    for name in ("secret_store", "github_app_settings"):
        if hasattr(app.state, name):
            delattr(app.state, name)


def installation_status(connected=True):
    return GitHubInstallationStatus(
        connected,
        "fake-org" if connected else None,
        "Organization" if connected else None,
        "99" if connected else None,
        "selected" if connected else None,
        "active" if connected else "revoked",
        NOW if connected else None,
        NOW if connected else None,
        NOW if connected else None,
    )


def test_admin_management_routes_remain_authenticated_and_redacted():
    client, service, session, _ = setup()
    connector = uuid4()
    service.initiate.return_value = GitHubInstallationInitiation(
        "https://github.com/apps/fake/installations/new?state=opaque",
        NOW,
    )
    response = client.post(f"/api/v1/connectors/{connector}/github/installation")
    assert response.status_code == 200
    assert set(response.json()) == {"installation_url", "expires_at"}
    service.status.return_value = installation_status()
    response = client.get(f"/api/v1/connectors/{connector}/github/installation")
    assert response.status_code == 200
    service.disconnect.return_value = installation_status(False)
    assert client.delete(f"/api/v1/connectors/{connector}/github/installation").status_code == 200
    assert session.commits == 2


def test_public_setup_redirect_and_callback_require_no_bearer_header():
    client, service, session, _ = setup()
    authorization_url = (
        "https://github.com/login/oauth/authorize?client_id=Iv1.test&state=opaque"
    )
    service.complete_setup.return_value = GitHubSetupRedirect(authorization_url)
    setup_response = client.get(
        "/api/v1/connectors/github/setup",
        params={"state": STATE, "installation_id": "77", "setup_action": "install"},
        follow_redirects=False,
    )
    assert setup_response.status_code == 303
    assert setup_response.headers["location"] == authorization_url
    service.complete_setup.assert_called_once_with(
        state=STATE, installation_id=77, setup_action="install"
    )

    service.complete_callback.return_value = installation_status()
    callback_response = client.get(
        "/api/v1/connectors/github/callback",
        params={"state": STATE, "code": "temporary-code"},
    )
    assert callback_response.status_code == 200
    assert callback_response.json() == {"status": "connected"}
    service.complete_callback.assert_called_once_with(state=STATE, code="temporary-code")
    assert session.commits == 2


@pytest.mark.parametrize(
    "params",
    (
        {"installation_id": "77", "setup_action": "install"},
        {"state": "short", "installation_id": "77", "setup_action": "install"},
        {"state": STATE, "installation_id": "0", "setup_action": "install"},
        {"state": STATE, "installation_id": "-1", "setup_action": "install"},
        {"state": STATE, "installation_id": "not-an-integer", "setup_action": "install"},
        {"state": STATE, "installation_id": str(2**63), "setup_action": "install"},
        {"state": STATE, "installation_id": "77", "setup_action": "update"},
    ),
)
def test_setup_rejects_missing_malformed_oversized_and_unallowed_query(params):
    client, service, _, _ = setup()
    response = client.get(
        "/api/v1/connectors/github/setup", params=params, follow_redirects=False
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Connector request is invalid"}
    service.complete_setup.assert_not_called()


def test_callback_validation_old_post_completion_and_public_error_are_safe():
    client, service, session, _ = setup()
    assert client.get("/api/v1/connectors/github/callback").status_code == 422
    assert client.get(
        "/api/v1/connectors/github/callback", params={"state": "short", "code": "x"}
    ).status_code == 422
    old = client.post(
        f"/api/v1/connectors/{uuid4()}/github/installation/complete",
        json={"state": STATE, "code": "temporary-code", "installation_id": 77},
    )
    assert old.status_code == 404

    service.complete_callback.side_effect = RuntimeError(
        "FAKE token private key provider payload"
    )
    response = client.get(
        "/api/v1/connectors/github/callback",
        params={"state": STATE, "code": "temporary-code"},
    )
    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}
    assert not any(
        value in response.text.lower()
        for value in ("fake", "token", "private key", STATE.lower(), "temporary-code")
    )
    assert session.rollbacks == 1


def test_client_supplied_redirect_parameters_are_never_used():
    client, service, _, _ = setup()
    fixed = "https://github.com/login/oauth/authorize?fixed=true"
    service.complete_setup.return_value = GitHubSetupRedirect(fixed)
    response = client.get(
        "/api/v1/connectors/github/setup",
        params={
            "state": STATE,
            "installation_id": "77",
            "setup_action": "install",
            "next": "https://evil.test",
            "return_to": "https://evil.test/steal",
        },
        follow_redirects=False,
    )
    assert response.headers["location"] == fixed
    assert "evil.test" not in response.headers["location"]


def test_runtime_composition_requires_and_accepts_injected_secret_store():
    request = Request(
        {
            "type": "http", "app": app, "headers": [], "method": "GET", "path": "/",
            "query_string": b"", "server": ("test", 443), "client": ("test", 1),
            "scheme": "https",
        }
    )
    with pytest.raises(HTTPException) as missing:
        get_secret_store(request)
    assert missing.value.status_code == 503

    class Store:
        def store(self, value):
            return SecretReference("opaque")

        def retrieve(self, reference):
            return SecretValue("hidden")

        def delete(self, reference):
            pass

    settings = GitHubAppSettings(
        123,
        "app-slug",
        "Iv1.client-id",
        SecretReference("fake://opaque-client"),
        SecretReference("fake://opaque-key"),
        "https://platform.test/api/v1/connectors/github/callback",
        "https://platform.test/api/v1/connectors/github/setup",
    )
    store = Store()
    configure_github_app(app, store, settings=settings)
    assert get_secret_store(request) is store
    assert get_github_app_settings(request) is settings
