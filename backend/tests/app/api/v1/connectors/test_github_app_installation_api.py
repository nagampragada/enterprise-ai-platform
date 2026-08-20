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
    get_github_repository_scope_service,
    get_github_repository_selection_service,
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
from application.services.github_repository_selection_service import (
    GitHubRepositoryScopePage,
    GitHubRepositoryScopeView,
    GitHubRepositorySelectionContext,
    GitHubRepositorySelectionNotFound,
    GitHubRepositorySelectionRejected,
)
from infrastructure.repositories.connector_scope_repository import ConnectorScopePageCursor


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


def setup(service=None, admin=None, discovery=None, selection=None, scopes=None):
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
    if selection is not None:
        app.dependency_overrides[get_github_repository_selection_service] = lambda: selection
    if scopes is not None:
        app.dependency_overrides[get_github_repository_scope_service] = lambda: scopes
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


def repository_scope_view(scope_id=None, status="active"):
    return GitHubRepositoryScopeView(
        scope_id or uuid4(), uuid4(), uuid4(), 501, "docs", "fake-org/docs",
        "fake-org", True, "private", False, False, "main", status, NOW, NOW,
    )


def test_repository_selection_request_is_minimal_and_staged_between_transactions():
    selection=Mock();scopes=Mock();client,_,session,admin=setup(selection=selection,scopes=scopes)
    connector_id,space_id=uuid4(),uuid4()
    context=GitHubRepositorySelectionContext(
        admin.organization_id,connector_id,space_id,uuid4(),77,123,99,"fake-org",501
    )
    repo=GitHubRepository(501,"docs","fake-org/docs","fake-org",True,"private",
        False,False,"main","https://github.com/fake-org/docs",NOW)
    view=repository_scope_view();view=GitHubRepositoryScopeView(
        view.scope_id,connector_id,space_id,view.repository_id,view.repository_name,
        view.repository_full_name,view.owner_login,view.private,view.visibility,
        view.archived,view.disabled,view.default_branch,view.status,view.created_at,view.updated_at)
    selection.prepare.return_value=context
    def verify(value):
        assert value is context and session.rollbacks==1 and session.commits==0
        return repo
    selection.verify.side_effect=verify;selection.persist.return_value=view
    response=client.post(f"/api/v1/connectors/{connector_id}/github/repository-scopes",
        json={"repository_id":501,"knowledge_space_id":str(space_id)})
    assert response.status_code==200 and response.json()["scope_id"]==str(view.scope_id)
    selection.prepare.assert_called_once_with(admin.organization_id,connector_id,space_id,501)
    selection.persist.assert_called_once_with(context,repo,admin.user_id)
    assert session.rollbacks==1 and session.commits==1
    rejected=client.post(f"/api/v1/connectors/{connector_id}/github/repository-scopes",
        json={"repository_id":501,"knowledge_space_id":str(space_id),"owner_login":"spoof"})
    assert rejected.status_code==422 and rejected.json()=={"detail":"Connector request is invalid"}


def test_repository_scope_list_and_delete_are_provider_free_and_idempotent_routes():
    selection=Mock();scopes=Mock();client,_,session,admin=setup(selection=selection,scopes=scopes)
    connector_id=uuid4();first=repository_scope_view();second=repository_scope_view(status="removed")
    cursor=ConnectorScopePageCursor(first.created_at,first.scope_id)
    scopes.list.return_value=GitHubRepositoryScopePage((first,),1,True,cursor)
    response=client.get(f"/api/v1/connectors/{connector_id}/github/repository-scopes?limit=1")
    assert response.status_code==200 and response.json()["has_more"] is True
    assert "safe_config" not in response.text and "installation" not in response.text
    scopes.deselect.return_value=second
    deleted=client.delete(f"/api/v1/connectors/{connector_id}/github/repository-scopes/{second.scope_id}")
    assert deleted.status_code==200 and deleted.json()["status"]=="removed"
    scopes.deselect.assert_called_once_with(admin.organization_id,connector_id,second.scope_id)
    assert session.commits==1
    selection.create_repository_access_token.assert_not_called()


@pytest.mark.parametrize(("error","status_code","detail"),(
    (GitHubRepositorySelectionNotFound("unsafe tenant"),404,"Resource not found"),
    (GitHubRepositorySelectionRejected("unsafe provider response"),422,"GitHub repository selection request is invalid"),
))
def test_repository_selection_errors_are_fixed_and_redacted(error,status_code,detail):
    selection=Mock();client,_,session,_=setup(selection=selection)
    selection.prepare.side_effect=error
    response=client.post(f"/api/v1/connectors/{uuid4()}/github/repository-scopes",
        json={"repository_id":501,"knowledge_space_id":str(uuid4())})
    assert response.status_code==status_code and response.json()=={"detail":detail}
    assert "unsafe" not in response.text and session.rollbacks==1


def test_repository_scope_openapi_is_immutable_and_secret_free():
    schema=app.openapi();operation=schema["paths"]["/api/v1/connectors/{connector_id}/github/repository-scopes"]["post"]
    serialized=str(operation).lower()
    for forbidden in ("token","private_key","secret_reference","installation_id","app_id","safe_config"):
        assert forbidden not in serialized


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
