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
    get_github_repository_discovery_service,
    get_github_app_settings,
    get_secret_store,
)
from app.main import app, configure_github_app
from application.ports.secret_store import SecretReference, SecretValue
from application.ports.github_app import (
    GitHubProviderRateLimitError,
    GitHubProviderUnavailableError,
    GitHubRepository,
    GitHubRepositoryPage,
)
from application.services.github_app_installation_service import (
    GitHubInstallationInitiation,
    GitHubInstallationStatus,
    GitHubSetupRedirect,
)
from application.services.github_repository_discovery_service import (
    GitHubRepositoryDiscoveryConflict,
    GitHubRepositoryDiscoveryContext,
    GitHubRepositoryDiscoveryNotFound,
    GitHubRepositoryDiscoveryRejected,
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


def setup(service=None, admin=None, discovery=None):
    service = service or Mock()
    discovery = discovery or Mock()
    session = Session()
    admin = admin or ConnectorAdministrator(uuid4(), uuid4())

    def db():
        yield session

    app.dependency_overrides[get_db_session] = db
    app.dependency_overrides[get_connector_administrator] = lambda: admin
    app.dependency_overrides[get_github_app_installation_service] = lambda: service
    app.dependency_overrides[get_github_repository_discovery_service] = lambda: discovery
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


def test_repository_discovery_defaults_are_redacted_and_end_db_work_before_provider_io():
    discovery = Mock()
    client, _, session, admin = setup(discovery=discovery)
    connector_id = uuid4()
    context = GitHubRepositoryDiscoveryContext(77, 99, "fake-org")
    discovery.prepare.return_value = context

    def result(*args, **kwargs):
        assert session.rollbacks == 1
        return GitHubRepositoryPage(
            (
                GitHubRepository(
                    501,
                    "docs",
                    "fake-org/docs",
                    "fake-org",
                    True,
                    "private",
                    False,
                    False,
                    "main",
                    "https://github.com/fake-org/docs",
                    NOW,
                ),
            ),
            1,
            50,
            False,
            1,
        )

    discovery.discover.side_effect = result
    response = client.get(f"/api/v1/connectors/{connector_id}/github/repositories")
    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "repository_id": 501,
                "name": "docs",
                "full_name": "fake-org/docs",
                "owner_login": "fake-org",
                "private": True,
                "visibility": "private",
                "archived": False,
                "disabled": False,
                "default_branch": "main",
                "html_url": "https://github.com/fake-org/docs",
                "updated_at": "2026-08-20T12:00:00Z",
            }
        ],
        "page": 1,
        "page_size": 50,
        "has_next": False,
        "total_count": 1,
    }
    discovery.prepare.assert_called_once_with(admin.organization_id, connector_id)
    discovery.discover.assert_called_once_with(context, page=1, page_size=50)
    assert "77" not in response.text and "token" not in response.text.lower()


def test_repository_discovery_rejects_unauthenticated_requests_before_service_use():
    session = Session()
    discovery = Mock()

    def db():
        yield session

    app.dependency_overrides[get_db_session] = db
    app.dependency_overrides[get_github_repository_discovery_service] = lambda: discovery
    response = TestClient(app).get(
        f"/api/v1/connectors/{uuid4()}/github/repositories"
    )
    assert response.status_code == 401
    discovery.prepare.assert_not_called()


@pytest.mark.parametrize(
    "params",
    (
        {"page": "0"},
        {"page": "1001"},
        {"page": "not-an-int"},
        {"page_size": "0"},
        {"page_size": "101"},
        {"owner": "other-org"},
        {"installation_id": "77"},
        {"token": "attacker-controlled"},
        {"all": "true"},
        {"sort": "updated"},
    ),
)
def test_repository_discovery_rejects_invalid_or_arbitrary_query_controls(params):
    discovery = Mock()
    client, _, _, _ = setup(discovery=discovery)
    response = client.get(
        f"/api/v1/connectors/{uuid4()}/github/repositories", params=params
    )
    assert response.status_code == 422
    assert response.json()["detail"] in {
        "Connector request is invalid",
        "GitHub repository discovery request is invalid",
    }
    discovery.prepare.assert_not_called()
    discovery.discover.assert_not_called()


@pytest.mark.parametrize(
    ("error", "status_code", "detail"),
    (
        (GitHubRepositoryDiscoveryNotFound(), 404, "Resource not found"),
        (GitHubRepositoryDiscoveryRejected(), 422, "GitHub repository discovery request is invalid"),
        (GitHubRepositoryDiscoveryConflict(), 409, "Resource state conflict"),
        (GitHubProviderUnavailableError("provider secret"), 502, "GitHub provider request failed"),
        (GitHubProviderRateLimitError("provider secret"), 503, "GitHub provider is temporarily unavailable"),
    ),
)
def test_repository_discovery_error_contract_is_fixed_and_redacted(error, status_code, detail):
    discovery = Mock()
    discovery.prepare.side_effect = error
    client, _, session, _ = setup(discovery=discovery)
    response = client.get(f"/api/v1/connectors/{uuid4()}/github/repositories")
    assert response.status_code == status_code
    assert response.json() == {"detail": detail}
    assert "secret" not in response.text.lower()
    assert session.rollbacks == 1


def test_repository_discovery_openapi_exposes_only_platform_fields():
    schema = app.openapi()
    operation = schema["paths"][
        "/api/v1/connectors/{connector_id}/github/repositories"
    ]["get"]
    assert {parameter["name"] for parameter in operation["parameters"]} == {
        "connector_id",
        "page",
        "page_size",
        "authorization",
    }
    repository_fields = set(
        schema["components"]["schemas"]["GitHubRepositoryResponse"]["properties"]
    )
    assert repository_fields == {
        "repository_id",
        "name",
        "full_name",
        "owner_login",
        "private",
        "visibility",
        "archived",
        "disabled",
        "default_branch",
        "html_url",
        "updated_at",
    }
    rendered = str(operation).lower()
    assert all(term not in rendered for term in ("installation_id", "app_id", "private_key", "access_token"))
