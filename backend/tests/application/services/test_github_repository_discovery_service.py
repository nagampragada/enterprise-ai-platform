from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.ports.github_app import (
    GitHubInstallationAccessToken,
    GitHubRepository,
    GitHubRepositoryPage,
)
from application.services.github_repository_discovery_service import (
    GitHubRepositoryDiscoveryConflict,
    GitHubRepositoryDiscoveryContext,
    GitHubRepositoryDiscoveryNotFound,
    GitHubRepositoryDiscoveryRejected,
    GitHubRepositoryDiscoveryService,
)
from infrastructure.repositories.connector_credential_repository import CredentialMetadata
from infrastructure.repositories.github_app_installation_repository import GitHubInstallationView


NOW = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)


def _service():
    client = Mock(app_id=123)
    value = GitHubRepositoryDiscoveryService(Mock(), client)
    value._connectors = Mock()
    value._credentials = Mock()
    value._installations = Mock()
    credential_id = uuid4()
    value._connectors.get_by_id.return_value = SimpleNamespace(
        connector_type="github", status="active"
    )
    value._credentials.get.return_value = CredentialMetadata(
        credential_id,
        uuid4(),
        "github",
        "app_installation",
        "active",
        "77",
        "fake-org",
        ("contents:read", "metadata:read"),
        None,
        NOW,
        None,
        NOW,
        NOW,
    )
    value._installations.get.return_value = GitHubInstallationView(
        uuid4(),
        credential_id,
        123,
        77,
        99,
        "fake-org",
        "Organization",
        "selected",
        "connected",
        NOW,
        NOW,
        NOW,
        None,
        NOW,
        NOW,
    )
    return value, client


def test_prepare_uses_tenant_scoped_lookups_and_returns_only_copied_primitives():
    value, _ = _service()
    organization_id, connector_id = uuid4(), uuid4()
    context = value.prepare(organization_id, connector_id)
    assert context == GitHubRepositoryDiscoveryContext(77, 99, "fake-org")
    assert "77" not in repr(context) and "fake-org" not in repr(context)
    value._connectors.get_by_id.assert_called_once_with(organization_id, connector_id)
    value._credentials.get.assert_called_once_with(organization_id, connector_id)
    value._installations.get.assert_called_once_with(organization_id, connector_id)


@pytest.mark.parametrize(
    ("target", "replacement", "error"),
    (
        ("connector", None, GitHubRepositoryDiscoveryNotFound),
        ("credential", None, GitHubRepositoryDiscoveryNotFound),
        ("installation", None, GitHubRepositoryDiscoveryNotFound),
        ("connector_type", "local_folder", GitHubRepositoryDiscoveryRejected),
        ("connector_status", "draft", GitHubRepositoryDiscoveryConflict),
        ("credential_status", "revoked", GitHubRepositoryDiscoveryConflict),
        ("provider_key", "gitlab", GitHubRepositoryDiscoveryConflict),
        ("auth_scheme", "oauth2", GitHubRepositoryDiscoveryConflict),
        ("external_subject", "78", GitHubRepositoryDiscoveryConflict),
        ("granted_scopes", ("metadata:read",), GitHubRepositoryDiscoveryConflict),
        ("installation_status", "disconnected", GitHubRepositoryDiscoveryConflict),
        ("account_type", "User", GitHubRepositoryDiscoveryConflict),
        ("app_id", 456, GitHubRepositoryDiscoveryConflict),
        ("account_login", "unsafe/login", GitHubRepositoryDiscoveryConflict),
    ),
)
def test_prepare_rejects_missing_cross_tenant_or_inconsistent_binding(
    target, replacement, error
):
    value, _ = _service()
    if target == "connector":
        value._connectors.get_by_id.return_value = replacement
    elif target == "credential":
        value._credentials.get.return_value = replacement
    elif target == "installation":
        value._installations.get.return_value = replacement
    elif target == "connector_type":
        value._connectors.get_by_id.return_value.connector_type = replacement
    elif target == "connector_status":
        value._connectors.get_by_id.return_value.status = replacement
    elif target in {"credential_status", "provider_key", "auth_scheme", "external_subject", "granted_scopes"}:
        name = target.removeprefix("credential_")
        current = value._credentials.get.return_value
        value._credentials.get.return_value = SimpleNamespace(
            **{**current.__dict__, name: replacement}
        )
    else:
        names = {
            "installation_status": "status",
            "app_id": "github_app_id",
        }
        current = value._installations.get.return_value
        value._installations.get.return_value = SimpleNamespace(
            **{**current.__dict__, names.get(target, target): replacement}
        )
    with pytest.raises(error):
        value.prepare(uuid4(), uuid4())


def test_prepare_rejects_credential_bound_to_a_different_installation_row():
    value, _ = _service()
    current = value._installations.get.return_value
    value._installations.get.return_value = SimpleNamespace(
        **{**current.__dict__, "credential_id": uuid4()}
    )
    with pytest.raises(GitHubRepositoryDiscoveryConflict):
        value.prepare(uuid4(), uuid4())


@pytest.mark.parametrize(("page", "page_size"), ((0, 50), (1001, 50), (True, 50), (1, 0), (1, 101), (1, True)))
def test_pagination_is_rejected_before_any_token_is_created(page, page_size):
    value, client = _service()
    with pytest.raises(GitHubRepositoryDiscoveryRejected):
        value.discover(
            GitHubRepositoryDiscoveryContext(77, 99, "fake-org"),
            page=page,
            page_size=page_size,
        )
    client.create_installation_access_token.assert_not_called()


def test_discover_uses_one_request_scoped_token_and_returns_no_credentials():
    value, client = _service()
    token = GitHubInstallationAccessToken("ghs_temporary", NOW + timedelta(hours=1))
    result = GitHubRepositoryPage(
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
        2,
        25,
        False,
        26,
    )
    client.create_installation_access_token.return_value = token
    client.list_installation_repositories.return_value = result
    assert value.discover(
        GitHubRepositoryDiscoveryContext(77, 99, "fake-org"),
        page=2,
        page_size=25,
    ) is result
    client.create_installation_access_token.assert_called_once_with(77)
    client.list_installation_repositories.assert_called_once_with(
        token,
        page=2,
        page_size=25,
        account_id=99,
        account_login="fake-org",
    )
    assert "ghs_temporary" not in repr(result)
