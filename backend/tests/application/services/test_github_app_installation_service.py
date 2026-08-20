from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import urlencode
from uuid import uuid4

import pytest

from application.ports.github_app import GitHubInstallation, GitHubUser, GitHubUserAccessToken
from application.ports.secret_store import SecretReference, SecretValue
from application.services.github_app_installation_service import (
    GitHubAppInstallationService,
    GitHubInstallationRejected,
)
from application.services.oauth_authorization_service import (
    LockedOAuthAuthorization,
    OAuthAuthorizationPreparation,
)
from infrastructure.repositories.connector_credential_repository import CredentialMetadata


NOW = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
STATE = "s" * 64
AUTHORIZATION_QUERY = {
    "client_id": "Iv1.client-id",
    "redirect_uri": "https://platform.test/api/v1/connectors/github/callback",
    "state": STATE,
    "code_challenge": "c" * 43,
    "code_challenge_method": "S256",
}


class Store:
    def delete(self, ref):
        pass


def service():
    client = Mock()
    client.app_id = 123
    client.web_base_url = "https://github.test"
    client.client_id = "Iv1.client-id"
    client.callback_url = "https://platform.test/api/v1/connectors/github/callback"
    value = GitHubAppInstallationService(Mock(), Store(), client, clock=lambda: NOW)
    value._connectors = Mock()
    value._credentials = Mock()
    value._bindings = Mock()
    value._oauth = Mock()
    value._credential_lifecycle = Mock()
    value._connectors.get_by_id.return_value = SimpleNamespace(
        connector_type="github", status="draft"
    )
    value._connectors.lock_by_id.return_value = SimpleNamespace(
        connector_type="github", status="draft"
    )
    return value, client


def installation(**kw):
    values = dict(
        installation_id=77,
        app_id=123,
        account_id=99,
        account_login="fake-org",
        account_type="Organization",
        repository_selection="selected",
        permissions=(("contents", "read"), ("metadata", "read")),
        created_at=NOW - timedelta(days=1),
        updated_at=NOW,
    )
    values.update(kw)
    return GitHubInstallation(**values)


def locked(*, candidate=77, setup_completed_at=NOW, **kw):
    values = dict(
        transaction_id=uuid4(),
        organization_id=uuid4(),
        connector_id=uuid4(),
        initiating_user_id=uuid4(),
        provider_key="github",
        callback_identifier="github_app_installation",
        pkce_verifier_secret_reference=SecretReference("opaque-pkce"),
        expires_at=NOW + timedelta(minutes=5),
        provider_candidate_installation_id=candidate,
        provider_setup_completed_at=setup_completed_at,
    )
    values.update(kw)
    return LockedOAuthAuthorization(**values)


def arrange_completion(value, client, *, user_installation=None, app_installation=None):
    transaction = locked()
    value._oauth.lock.return_value = transaction
    value._oauth.retrieve_pkce_verifier.return_value = SecretValue("v" * 64)
    client.exchange_authorization_code.return_value = GitHubUserAccessToken("ghu_hidden")
    client.get_authenticated_user.return_value = GitHubUser(55, "installer")
    client.list_user_installations.return_value = (user_installation or installation(),)
    client.verify_installation.return_value = app_installation or installation()
    credential = CredentialMetadata(
        uuid4(), transaction.connector_id, "github", "app_installation", "active",
        "77", "fake-org", ("contents:read", "metadata:read"), None, NOW, None, NOW, NOW,
    )
    value._credential_lifecycle.bind.return_value = credential
    value._credentials.get.return_value = credential
    value._bindings.get.return_value = SimpleNamespace(
        status="connected",
        account_login="fake-org",
        account_type="Organization",
        account_id=99,
        repository_selection="selected",
        provider_created_at=NOW,
        provider_updated_at=NOW,
        last_verified_at=NOW,
    )
    return transaction


def authorization_url(**overrides):
    query = {**AUTHORIZATION_QUERY, **overrides}
    return f"https://github.test/login/oauth/authorize?{urlencode(query)}"


def test_initiation_uses_pkce_and_supports_explicit_preconnection_states():
    value, client = service()
    expiry = NOW + timedelta(minutes=10)
    value._oauth.prepare.return_value = OAuthAuthorizationPreparation(
        uuid4(), STATE, "c" * 43, expiry
    )
    client.build_installation_url.return_value = "https://github.test/install"
    result = value.initiate(uuid4(), uuid4(), uuid4())
    assert result.installation_url == "https://github.test/install"
    assert result.expires_at == expiry
    assert value._oauth.prepare.call_args.kwargs["use_pkce"] is True
    assert "draft" in value._oauth.prepare.call_args.kwargs["allowed_connector_statuses"]
    client.build_authorization_url.assert_not_called()


def test_setup_correlates_once_then_redirects_to_exact_trusted_github_host():
    value, client = service()
    pending = locked(candidate=None, setup_completed_at=None)
    correlated = locked(
        transaction_id=pending.transaction_id,
        organization_id=pending.organization_id,
        connector_id=pending.connector_id,
        initiating_user_id=pending.initiating_user_id,
    )
    value._oauth.lock.return_value = pending
    value._oauth.complete_provider_setup.return_value = correlated
    value._oauth.retrieve_pkce_challenge.return_value = "c" * 43
    client.build_authorization_url.return_value = authorization_url()
    result = value.complete_setup(state=STATE, installation_id=77, setup_action="install")
    assert result.authorization_url == authorization_url()
    value._oauth.complete_provider_setup.assert_called_once_with(
        pending, candidate_installation_id=77
    )


@pytest.mark.parametrize(
    "url",
    (
        "https://evil.test/login/oauth/authorize?" + urlencode(AUTHORIZATION_QUERY),
        "https://github.test.evil/login/oauth/authorize?" + urlencode(AUTHORIZATION_QUERY),
        "https://github.test/login/oauth/authorize?" + urlencode({**AUTHORIZATION_QUERY, "next": "https://evil.test"}),
        "https://github.test/login/oauth/authorize?" + urlencode({**AUTHORIZATION_QUERY, "redirect_uri": "https://evil.test/callback"}),
        "https://github.test/login/oauth/authorize?" + urlencode({**AUTHORIZATION_QUERY, "state": "x" * 64}),
        "https://github.test/other?" + urlencode(AUTHORIZATION_QUERY),
    ),
)
def test_setup_rejects_host_spoofing_and_open_redirect_parameters(url):
    value, client = service()
    pending = locked(candidate=None, setup_completed_at=None)
    value._oauth.lock.return_value = pending
    value._oauth.complete_provider_setup.return_value = locked()
    value._oauth.retrieve_pkce_challenge.return_value = "c" * 43
    client.build_authorization_url.return_value = url
    with pytest.raises(GitHubInstallationRejected):
        value.complete_setup(state=STATE, installation_id=77, setup_action="install")


def test_setup_replay_candidate_replacement_and_invalid_inputs_are_rejected():
    value, client = service()
    value._oauth.lock.return_value = locked()
    with pytest.raises(GitHubInstallationRejected):
        value.complete_setup(state=STATE, installation_id=88, setup_action="install")
    for candidate in (-1, 0, 9_223_372_036_854_775_808):
        with pytest.raises(GitHubInstallationRejected):
            value.complete_setup(
                state=STATE, installation_id=candidate, setup_action="install"
            )
    with pytest.raises(GitHubInstallationRejected):
        value.complete_setup(state=STATE, installation_id=77, setup_action="update")
    value._oauth.complete_provider_setup.assert_not_called()
    client.build_authorization_url.assert_not_called()


def test_callback_proves_user_access_then_app_identity_and_consumes_last():
    value, client = service()
    transaction = arrange_completion(value, client)
    result = value.complete_callback(state=STATE, code="temporary-code")
    assert result.connected
    client.exchange_authorization_code.assert_called_once_with("temporary-code", "v" * 64)
    client.get_authenticated_user.assert_called_once()
    client.list_user_installations.assert_called_once()
    client.verify_installation.assert_called_once_with(77)
    value._oauth.delete_pkce_verifier.assert_called_once_with(transaction)
    value._oauth.consume_locked.assert_called_once_with(transaction)
    assert value._credential_lifecycle.bind.call_args.kwargs["secret_reference"] is None
    assert "ghu_hidden" not in repr(value._credential_lifecycle.bind.call_args)


@pytest.mark.parametrize(
    "changes",
    (
        {"candidate": None},
        {"setup_completed_at": None},
        {"provider_key": "other"},
        {"callback_identifier": "other_callback"},
        {"initiating_user_id": None},
    ),
)
def test_callback_requires_setup_and_exact_state_context(changes):
    value, client = service()
    value._oauth.lock.return_value = locked(**changes)
    with pytest.raises(GitHubInstallationRejected):
        value.complete_callback(state=STATE, code="code")
    client.exchange_authorization_code.assert_not_called()


def test_candidate_absent_or_duplicated_in_user_installations_is_rejected():
    for accessible in (
        (installation(installation_id=88),),
        (installation(), installation()),
    ):
        value, client = service()
        arrange_completion(value, client)
        client.list_user_installations.return_value = accessible
        with pytest.raises(GitHubInstallationRejected):
            value.complete_callback(state=STATE, code="code")
        client.verify_installation.assert_not_called()
        value._credential_lifecycle.bind.assert_not_called()
        value._oauth.delete_pkce_verifier.assert_called_once()


def test_wrong_user_app_personal_permissions_and_metadata_disagreement_are_rejected():
    cases = (
        RuntimeError("wrong authenticated user"),
        installation(app_id=999),
        installation(account_type="User"),
        installation(permissions=(("contents", "write"), ("metadata", "read"))),
        installation(account_id=100),
    )
    for case in cases:
        value, client = service()
        if isinstance(case, Exception):
            arrange_completion(value, client)
            client.get_authenticated_user.side_effect = case
        elif case.account_id == 100:
            arrange_completion(value, client, app_installation=case)
        else:
            arrange_completion(value, client, user_installation=case)
        with pytest.raises((GitHubInstallationRejected, RuntimeError)):
            value.complete_callback(state=STATE, code="code")
        value._credential_lifecycle.bind.assert_not_called()


def test_callback_failures_delete_pkce_but_do_not_consume_or_activate():
    value, client = service()
    arrange_completion(value, client)
    client.exchange_authorization_code.side_effect = RuntimeError("provider failed")
    with pytest.raises(RuntimeError):
        value.complete_callback(state=STATE, code="code")
    value._oauth.delete_pkce_verifier.assert_called_once()
    value._oauth.consume_locked.assert_not_called()
    value._credential_lifecycle.bind.assert_not_called()

    value, client = service()
    arrange_completion(value, client)
    value._credential_lifecycle.bind.side_effect = RuntimeError("database failed")
    with pytest.raises(RuntimeError):
        value.complete_callback(state=STATE, code="code")
    value._oauth.consume_locked.assert_not_called()
    value._connectors.update_validation.assert_not_called()


def test_disconnect_is_idempotent_and_never_deletes_app_key():
    value, _ = service()
    row = SimpleNamespace(status="connected")
    credential = SimpleNamespace(status="active")
    value._bindings.lock.return_value = row
    value._credentials.lock.return_value = credential
    assert not value.disconnect(uuid4(), uuid4()).connected
    credential.status = "revoked"
    row.status = "disconnected"
    value.disconnect(uuid4(), uuid4())
    value._credential_lifecycle.disconnect.assert_called_once()
    value._bindings.disconnect.assert_called_once()
