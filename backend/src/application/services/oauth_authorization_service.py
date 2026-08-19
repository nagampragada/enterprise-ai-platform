"""Provider-neutral OAuth authorization preparation and single-use consumption."""
from __future__ import annotations
import base64,hashlib,secrets
from dataclasses import dataclass
from datetime import UTC,datetime,timedelta
from typing import Callable
from uuid import UUID
from application.ports.secret_store import SecretReference,SecretStore,SecretStoreError,SecretValue
from infrastructure.repositories.connector_repository import ConnectorRepository
from infrastructure.repositories.oauth_authorization_transaction_repository import OAuthAuthorizationTransactionRepository
from infrastructure.repositories.user_repository import UserRepository

DEFAULT_LIFETIME=timedelta(minutes=10);MAX_LIFETIME=timedelta(minutes=20)
class InvalidOAuthAuthorization(ValueError):pass
class OAuthAuthorizationRejected(RuntimeError):pass

@dataclass(frozen=True,repr=False)
class OAuthAuthorizationPreparation:
    transaction_id:UUID;state:str;pkce_challenge:str|None;expires_at:datetime

@dataclass(frozen=True)
class ConsumedOAuthAuthorization:
    transaction_id:UUID;organization_id:UUID;connector_id:UUID;initiating_user_id:UUID|None
    provider_key:str;callback_identifier:str;pkce_verifier_available:bool

class OAuthAuthorizationService:
    def __init__(self,session,secret_store:SecretStore,*,clock:Callable[[],datetime]=lambda:datetime.now(UTC),
        state_factory:Callable[[],str]=lambda:secrets.token_urlsafe(48),
        verifier_factory:Callable[[],str]=lambda:secrets.token_urlsafe(64))->None:
        self._connectors=ConnectorRepository(session);self._users=UserRepository(session)
        self._transactions=OAuthAuthorizationTransactionRepository(session);self._secrets=secret_store
        self._clock=clock;self._state_factory=state_factory;self._verifier_factory=verifier_factory
    def prepare(self,organization_id:UUID,connector_id:UUID,initiating_user_id:UUID,*,provider_key:str,
        callback_identifier:str,use_pkce:bool=True,lifetime:timedelta=DEFAULT_LIFETIME)->OAuthAuthorizationPreparation:
        now=_aware(self._clock());lifetime=_lifetime(lifetime)
        connector=self._connectors.get_by_id(organization_id,connector_id)
        user=self._users.get_by_id(organization_id,initiating_user_id)
        if connector is None or connector.status!="active" or connector.connector_type!=provider_key or user is None or user.status!="active":
            raise OAuthAuthorizationRejected("OAuth authorization cannot be prepared")
        state=_random(self._state_factory(),"state");state_hash=hashlib.sha256(state.encode()).digest()
        reference=None;challenge=None
        if use_pkce:
            verifier=_random(self._verifier_factory(),"PKCE verifier")
            reference=self._secrets.store(SecretValue(verifier))
            challenge=base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode("ascii")
        try:
            row=self._transactions.create(organization_id,connector_id,initiating_user_id,
                provider_key=provider_key,state_hash=state_hash,
                pkce_reference=reference.value if reference else None,callback_identifier=callback_identifier,
                created_at=now,expires_at=now+lifetime)
        except Exception:
            if reference is not None:
                try:self._secrets.delete(reference)
                except SecretStoreError:pass
            raise
        return OAuthAuthorizationPreparation(row.id,state,challenge,row.expires_at)
    def consume(self,state:str)->ConsumedOAuthAuthorization:
        now=_aware(self._clock());digest=hashlib.sha256(_random(state,"state").encode()).digest()
        row=self._transactions.lock_by_state_hash(digest)
        if row is None or row.status!="pending" or row.expires_at<=now:
            raise OAuthAuthorizationRejected("OAuth authorization transaction is unavailable")
        consumed=self._transactions.consume(row,now=now)
        return ConsumedOAuthAuthorization(consumed.id,consumed.organization_id,consumed.connector_id,
            consumed.initiating_user_id,consumed.provider_key,consumed.callback_identifier,
            consumed.pkce_verifier_secret_reference is not None)
def _aware(v):
    if not isinstance(v,datetime) or v.tzinfo is None or v.utcoffset() is None:raise InvalidOAuthAuthorization("clock is invalid")
    return v
def _lifetime(v):
    if not isinstance(v,timedelta) or v<=timedelta(0) or v>MAX_LIFETIME:raise InvalidOAuthAuthorization("OAuth lifetime is invalid")
    return v
def _random(v,name):
    if not isinstance(v,str) or len(v)<43:raise InvalidOAuthAuthorization(f"{name} generation failed")
    return v