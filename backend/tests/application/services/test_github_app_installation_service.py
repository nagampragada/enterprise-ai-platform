from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.ports.github_app import GitHubInstallation,GitHubUser,GitHubUserAccessToken
from application.ports.secret_store import SecretReference,SecretValue
from application.services.github_app_installation_service import GitHubAppInstallationService,GitHubInstallationRejected
from application.services.oauth_authorization_service import LockedOAuthAuthorization,OAuthAuthorizationPreparation
from infrastructure.repositories.connector_credential_repository import CredentialMetadata

NOW=datetime(2026,8,20,12,tzinfo=timezone.utc)
STATE="s"*64

class Store:
    def delete(self,ref):pass

def service():
    client=Mock();client.app_id=123
    value=GitHubAppInstallationService(Mock(),Store(),client,clock=lambda:NOW)
    value._connectors=Mock();value._credentials=Mock();value._bindings=Mock();value._oauth=Mock();value._credential_lifecycle=Mock()
    value._connectors.get_by_id.return_value=SimpleNamespace(connector_type="github",status="draft")
    value._connectors.lock_by_id.return_value=SimpleNamespace(connector_type="github",status="draft")
    return value,client

def installation(**kw):
    values=dict(installation_id=77,app_id=123,account_id=99,account_login="fake-org",account_type="Organization",
        repository_selection="selected",permissions=(("contents","read"),("metadata","read")),
        created_at=NOW-timedelta(days=1),updated_at=NOW)
    values.update(kw);return GitHubInstallation(**values)

def locked(org,connector,user,**kw):
    values=dict(transaction_id=uuid4(),organization_id=org,connector_id=connector,initiating_user_id=user,
        provider_key="github",callback_identifier="github_app_installation",
        pkce_verifier_secret_reference=SecretReference("opaque-pkce"),expires_at=NOW+timedelta(minutes=5))
    values.update(kw);return LockedOAuthAuthorization(**values)

def arrange_completion(value,client,org,connector,user,*,user_installation=None,app_installation=None):
    value._oauth.lock.return_value=locked(org,connector,user);value._oauth.retrieve_pkce_verifier.return_value=SecretValue("v"*64)
    client.exchange_authorization_code.return_value=GitHubUserAccessToken("ghu_hidden")
    client.get_authenticated_user.return_value=GitHubUser(55,"installer")
    client.list_user_installations.return_value=(user_installation or installation(),)
    client.verify_installation.return_value=app_installation or installation()
    credential=CredentialMetadata(uuid4(),connector,"github","app_installation","active","77","fake-org",("contents:read","metadata:read"),None,NOW,None,NOW,NOW)
    value._credential_lifecycle.bind.return_value=credential;value._credentials.get.return_value=credential
    value._bindings.get.return_value=SimpleNamespace(status="connected",account_login="fake-org",account_type="Organization",account_id=99,
        repository_selection="selected",provider_created_at=NOW,provider_updated_at=NOW,last_verified_at=NOW)

def test_initiation_uses_pkce_and_supports_explicit_preconnection_states():
    value,client=service();expiry=NOW+timedelta(minutes=10)
    value._oauth.prepare.return_value=OAuthAuthorizationPreparation(uuid4(),STATE,"c"*43,expiry)
    client.build_installation_url.return_value="https://github.test/install"
    client.build_authorization_url.return_value="https://github.test/oauth"
    result=value.initiate(uuid4(),uuid4(),uuid4())
    assert result.authorization_url.endswith("oauth") and result.expires_at==expiry
    assert value._oauth.prepare.call_args.kwargs["use_pkce"] is True
    assert "draft" in value._oauth.prepare.call_args.kwargs["allowed_connector_statuses"]

def test_completion_proves_user_access_then_app_identity_and_consumes_last():
    value,client=service();org,connector,user=uuid4(),uuid4(),uuid4();arrange_completion(value,client,org,connector,user)
    result=value.complete(org,connector,user,state=STATE,code="temporary-code",installation_id=77)
    assert result.connected
    client.exchange_authorization_code.assert_called_once_with("temporary-code","v"*64)
    client.get_authenticated_user.assert_called_once();client.list_user_installations.assert_called_once()
    client.verify_installation.assert_called_once_with(77)
    value._oauth.delete_pkce_verifier.assert_called_once();value._oauth.consume_locked.assert_called_once()
    assert value._credential_lifecycle.bind.call_args.kwargs["secret_reference"] is None
    assert "ghu_hidden" not in repr(value._credential_lifecycle.bind.call_args)

@pytest.mark.parametrize("mismatch",["user","tenant","connector","provider"])
def test_state_context_mismatch_fails_before_code_exchange(mismatch):
    value,client=service();org,connector,user=uuid4(),uuid4(),uuid4();values={}
    if mismatch=="user":values["initiating_user_id"]=uuid4()
    if mismatch=="tenant":values["organization_id"]=uuid4()
    if mismatch=="connector":values["connector_id"]=uuid4()
    if mismatch=="provider":values["provider_key"]="other"
    value._oauth.lock.return_value=locked(org,connector,user,**values)
    with pytest.raises(GitHubInstallationRejected):value.complete(org,connector,user,state=STATE,code="code",installation_id=77)
    client.exchange_authorization_code.assert_not_called()

def test_app_owned_installation_absent_from_user_list_is_rejected():
    value,client=service();org,connector,user=uuid4(),uuid4(),uuid4();arrange_completion(value,client,org,connector,user)
    client.list_user_installations.return_value=(installation(installation_id=88),)
    with pytest.raises(GitHubInstallationRejected):value.complete(org,connector,user,state=STATE,code="code",installation_id=77)
    client.verify_installation.assert_not_called();value._credential_lifecycle.bind.assert_not_called()
    value._oauth.delete_pkce_verifier.assert_called_once()

def test_wrong_app_personal_account_and_metadata_disagreement_are_rejected():
    for user_item,app_item in ((installation(app_id=999),installation()),
        (installation(account_type="User"),installation(account_type="User")),
        (installation(permissions=(("contents","write"),("metadata","read"))),installation()),
        (installation(),installation(account_id=100))):
        value,client=service();org,connector,user=uuid4(),uuid4(),uuid4()
        arrange_completion(value,client,org,connector,user,user_installation=user_item,app_installation=app_item)
        with pytest.raises(GitHubInstallationRejected):value.complete(org,connector,user,state=STATE,code="code",installation_id=77)
        value._credential_lifecycle.bind.assert_not_called()

def test_code_exchange_or_binding_failure_does_not_consume_or_activate():
    value,client=service();org,connector,user=uuid4(),uuid4(),uuid4();arrange_completion(value,client,org,connector,user)
    client.exchange_authorization_code.side_effect=RuntimeError("provider failed")
    with pytest.raises(RuntimeError):value.complete(org,connector,user,state=STATE,code="code",installation_id=77)
    value._oauth.consume_locked.assert_not_called();value._credential_lifecycle.bind.assert_not_called()
    value._oauth.delete_pkce_verifier.assert_not_called()
    client.exchange_authorization_code.side_effect=None
    value._credential_lifecycle.bind.side_effect=RuntimeError("database failed")
    with pytest.raises(RuntimeError):value.complete(org,connector,user,state=STATE,code="code",installation_id=77)
    value._oauth.consume_locked.assert_not_called();value._connectors.update_validation.assert_not_called()

def test_disconnect_is_idempotent_and_never_deletes_app_key():
    value,_=service();row=SimpleNamespace(status="connected");credential=SimpleNamespace(status="active")
    value._bindings.lock.return_value=row;value._credentials.lock.return_value=credential
    assert not value.disconnect(uuid4(),uuid4()).connected
    credential.status="revoked";row.status="disconnected";value.disconnect(uuid4(),uuid4())
    value._credential_lifecycle.disconnect.assert_called_once();value._bindings.disconnect.assert_called_once()
