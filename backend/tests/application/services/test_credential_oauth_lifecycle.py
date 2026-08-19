from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4
import hashlib,pytest
from application.ports.secret_store import SecretNotFound,SecretReference,SecretValue
from application.services.oauth_authorization_service import OAuthAuthorizationRejected,OAuthAuthorizationService
from application.services.connector_credential_service import ConnectorCredentialService
from infrastructure.repositories.connector_credential_repository import CredentialMetadata,CredentialReplacement

NOW=datetime(2026,8,25,12,tzinfo=timezone.utc)
STATE="s"*64;VERIFIER="v"*64
class FakeStore:
    def __init__(self):self.values={};self.deleted=[];self.fail_delete=False
    def store(self,secret):self.values["opaque-ref"]=secret.value;return SecretReference("opaque-ref")
    def retrieve(self,reference):
        if reference.value not in self.values:raise SecretNotFound("secret was not found")
        return SecretValue(self.values[reference.value])
    def delete(self,reference):
        self.deleted.append(reference.value)
        if self.fail_delete:raise SecretNotFound("secret was not found")
        self.values.pop(reference.value,None)

def _oauth(store):
    service=OAuthAuthorizationService(Mock(),store,clock=lambda:NOW,state_factory=lambda:STATE,verifier_factory=lambda:VERIFIER)
    service._connectors=Mock();service._connectors.get_by_id.return_value=SimpleNamespace(status="active",connector_type="github")
    service._users=Mock();service._users.get_by_id.return_value=SimpleNamespace(status="active")
    service._transactions=Mock();return service
def test_prepare_persists_only_digest_and_reference_and_returns_no_verifier():
    store=FakeStore();service=_oauth(store);row=SimpleNamespace(id=uuid4(),expires_at=NOW+timedelta(minutes=10))
    service._transactions.create.return_value=row
    result=service.prepare(uuid4(),uuid4(),uuid4(),provider_key="github",callback_identifier="github_callback")
    kwargs=service._transactions.create.call_args.kwargs
    assert kwargs["state_hash"]==hashlib.sha256(STATE.encode()).digest()
    assert kwargs["pkce_reference"]=="opaque-ref" and VERIFIER not in repr(kwargs)
    assert result.state==STATE and result.pkce_challenge and not hasattr(result,"pkce_verifier")
    assert STATE not in repr(result) and VERIFIER not in repr(result)
def test_prepare_failure_deletes_verifier_once_without_masking_original():
    store=FakeStore();service=_oauth(store);service._transactions.create.side_effect=RuntimeError("database failed")
    with pytest.raises(RuntimeError,match="database failed"):service.prepare(uuid4(),uuid4(),uuid4(),provider_key="github",callback_identifier="github_callback")
    assert store.deleted==["opaque-ref"]
def test_consume_hashes_state_and_replay_or_expiry_fails_closed():
    service=_oauth(FakeStore());row=SimpleNamespace(id=uuid4(),organization_id=uuid4(),connector_id=uuid4(),initiating_user_id=uuid4(),provider_key="github",callback_identifier="github_callback",pkce_verifier_secret_reference="opaque",status="pending",expires_at=NOW+timedelta(minutes=1))
    service._transactions.lock_by_state_hash.return_value=row;service._transactions.consume.return_value=row
    result=service.consume(STATE);assert result.pkce_verifier_available
    service._transactions.lock_by_state_hash.return_value=None
    with pytest.raises(OAuthAuthorizationRejected,match="unavailable"):service.consume(STATE)
    assert STATE not in str(OAuthAuthorizationRejected("OAuth authorization transaction is unavailable"))
def test_credential_replacement_and_disconnect_are_fail_closed_on_delete_failure():
    store=FakeStore();store.fail_delete=True;service=ConnectorCredentialService(Mock(),store,clock=lambda:NOW)
    service._connectors=Mock();service._connectors.lock_by_id.return_value=SimpleNamespace(connector_type="github")
    metadata=CredentialMetadata(uuid4(),uuid4(),"github","oauth2","active",None,None,(),None,None,None,NOW,NOW)
    service._credentials=Mock();service._credentials.replace.return_value=CredentialReplacement(metadata,"old-ref")
    assert service.bind(uuid4(),metadata.connector_id,uuid4(),provider_key="github",auth_scheme="oauth2",secret_reference=SecretReference("new-ref"))==metadata
    row=SimpleNamespace(secret_reference="new-ref");service._credentials.lock.return_value=row
    revoked=CredentialMetadata(metadata.credential_id,metadata.connector_id,"github","oauth2","revoked",None,None,(),None,None,NOW,NOW,NOW)
    service._credentials.set_status.return_value=revoked
    assert service.disconnect(uuid4(),metadata.connector_id).status=="revoked"
    assert service._credentials.set_status.call_args.kwargs["status"]=="revoked"
    assert store.deleted==["old-ref","new-ref"]