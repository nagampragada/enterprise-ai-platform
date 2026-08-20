"""Secure GitHub App installation initiation, verification, status, and disconnect."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Callable
from uuid import UUID

from application.ports.github_app import GitHubAppClient, GitHubInstallation
from application.ports.secret_store import SecretStore
from application.services.connector_credential_service import ConnectorCredentialService
from application.services.oauth_authorization_service import OAuthAuthorizationRejected, OAuthAuthorizationService
from infrastructure.repositories.connector_credential_repository import ConnectorCredentialRepository
from infrastructure.repositories.connector_repository import ConnectorRepository
from infrastructure.repositories.github_app_installation_repository import GitHubAppInstallationRepository

CALLBACK_IDENTIFIER="github_app_installation"
PERMISSIONS=("contents:read","metadata:read")
AUTHORITATIVE_PERMISSIONS=(("contents","read"),("metadata","read"))
AUTHORIZABLE_CONNECTOR_STATUSES=("draft","validating","active","auth_failed")

class GitHubInstallationNotFound(RuntimeError): pass
class GitHubInstallationRejected(RuntimeError): pass
class GitHubInstallationConflict(RuntimeError): pass

@dataclass(frozen=True)
class GitHubInstallationInitiation:
    installation_url:str
    authorization_url:str
    expires_at:datetime

@dataclass(frozen=True)
class GitHubInstallationStatus:
    connected:bool;account_login:str|None;account_type:str|None;external_account_id:str|None
    repository_selection:str|None;credential_status:str|None;provider_created_at:datetime|None
    provider_updated_at:datetime|None;last_verified_at:datetime|None


class GitHubAppInstallationService:
    def __init__(self,session,secret_store:SecretStore,client:GitHubAppClient,*,
        clock:Callable[[],datetime]=lambda:datetime.now(UTC))->None:
        self._session=session;self._connectors=ConnectorRepository(session)
        self._credentials=ConnectorCredentialRepository(session)
        self._bindings=GitHubAppInstallationRepository(session)
        self._oauth=OAuthAuthorizationService(session,secret_store,clock=clock)
        self._credential_lifecycle=ConnectorCredentialService(session,secret_store,clock=clock)
        self._client=client;self._clock=clock

    def initiate(self,organization_id:UUID,connector_id:UUID,user_id:UUID)->GitHubInstallationInitiation:
        connector=self._connectors.get_by_id(organization_id,connector_id)
        if connector is None:raise GitHubInstallationNotFound("GitHub connector was not found")
        if connector.connector_type!="github":raise GitHubInstallationRejected("GitHub installation is unavailable")
        if connector.status not in AUTHORIZABLE_CONNECTOR_STATUSES:raise GitHubInstallationConflict("GitHub connector state is ineligible")
        prepared=self._oauth.prepare(organization_id,connector_id,user_id,provider_key="github",
            callback_identifier=CALLBACK_IDENTIFIER,use_pkce=True,
            allowed_connector_statuses=AUTHORIZABLE_CONNECTOR_STATUSES)
        if prepared.pkce_challenge is None:raise GitHubInstallationConflict("GitHub authorization is unavailable")
        return GitHubInstallationInitiation(self._client.build_installation_url(prepared.state),
            self._client.build_authorization_url(prepared.state,prepared.pkce_challenge),prepared.expires_at)

    def complete(self,organization_id:UUID,connector_id:UUID,user_id:UUID,*,state:str,
        code:str,installation_id:int)->GitHubInstallationStatus:
        try:locked=self._oauth.lock(state)
        except OAuthAuthorizationRejected as exc:raise GitHubInstallationRejected("GitHub installation callback is invalid") from exc
        if (locked.organization_id!=organization_id or locked.connector_id!=connector_id
            or locked.initiating_user_id!=user_id or locked.provider_key!="github"
            or locked.callback_identifier!=CALLBACK_IDENTIFIER):
            raise GitHubInstallationRejected("GitHub installation callback is invalid")
        connector=self._connectors.lock_by_id(organization_id,connector_id)
        if connector is None:raise GitHubInstallationNotFound("GitHub connector was not found")
        if connector.connector_type!="github" or connector.status not in AUTHORIZABLE_CONNECTOR_STATUSES:
            raise GitHubInstallationRejected("GitHub installation callback is invalid")
        try:verifier=self._oauth.retrieve_pkce_verifier(locked)
        except OAuthAuthorizationRejected as exc:raise GitHubInstallationRejected("GitHub installation callback is invalid") from exc
        token=self._client.exchange_authorization_code(code,verifier.value)
        try:
            self._client.get_authenticated_user(token)
            accessible=tuple(item for item in self._client.list_user_installations(token)
                if item.installation_id==installation_id)
            if len(accessible)!=1:raise GitHubInstallationRejected("GitHub installation callback is invalid")
            user_installation=accessible[0]
            if (user_installation.app_id!=self._client.app_id or user_installation.account_type!="Organization"
                or user_installation.permissions!=AUTHORITATIVE_PERMISSIONS):
                raise GitHubInstallationRejected("GitHub installation callback is invalid")
            app_installation=self._client.verify_installation(installation_id)
            if (not _same_installation(user_installation,app_installation)
                or app_installation.account_type!="Organization" or app_installation.permissions!=AUTHORITATIVE_PERMISSIONS):
                raise GitHubInstallationRejected("GitHub installation callback is invalid")
        finally:
            try:self._oauth.delete_pkce_verifier(locked)
            except OAuthAuthorizationRejected as exc:raise GitHubInstallationRejected("GitHub installation callback is invalid") from exc
        credential=self._credential_lifecycle.bind(organization_id,connector_id,user_id,provider_key="github",
            auth_scheme="app_installation",secret_reference=None,external_subject=str(app_installation.installation_id),
            display_label=app_installation.account_login,granted_scopes=PERMISSIONS)
        now=_aware(self._clock())
        self._bindings.bind(organization_id,connector_id,credential.credential_id,app_installation,now=now)
        self._credential_lifecycle.validation_succeeded(organization_id,connector_id)
        self._connectors.update_validation(organization_id,connector_id,status="active",validated_at=now)
        try:self._oauth.consume_locked(locked)
        except OAuthAuthorizationRejected as exc:raise GitHubInstallationConflict("GitHub installation callback conflicted") from exc
        return self.status(organization_id,connector_id)

    def status(self,organization_id:UUID,connector_id:UUID)->GitHubInstallationStatus:
        connector=self._connectors.get_by_id(organization_id,connector_id)
        if connector is None:raise GitHubInstallationNotFound("GitHub connector was not found")
        if connector.connector_type!="github":raise GitHubInstallationRejected("GitHub installation is unavailable")
        binding=self._bindings.get(organization_id,connector_id);credential=self._credentials.get(organization_id,connector_id)
        if binding is None or binding.status!="connected":
            return GitHubInstallationStatus(False,None,None,None,None,credential.status if credential else None,None,None,None)
        return GitHubInstallationStatus(True,binding.account_login,binding.account_type,str(binding.account_id),binding.repository_selection,
            credential.status if credential else None,binding.provider_created_at,binding.provider_updated_at,binding.last_verified_at)

    def disconnect(self,organization_id:UUID,connector_id:UUID)->GitHubInstallationStatus:
        connector=self._connectors.lock_by_id(organization_id,connector_id)
        if connector is None:raise GitHubInstallationNotFound("GitHub connector was not found")
        if connector.connector_type!="github":raise GitHubInstallationRejected("GitHub installation is unavailable")
        binding=self._bindings.lock(organization_id,connector_id);credential=self._credentials.lock(organization_id,connector_id)
        now=_aware(self._clock())
        if credential is not None and credential.status!="revoked":self._credential_lifecycle.disconnect(organization_id,connector_id)
        if binding is not None and binding.status!="disconnected":self._bindings.disconnect(binding,now=now)
        return GitHubInstallationStatus(False,None,None,None,None,"revoked" if credential else None,None,None,None)


def _same_installation(left:GitHubInstallation,right:GitHubInstallation)->bool:
    return (left.installation_id,left.app_id,left.account_id,left.account_login,left.account_type,
        left.repository_selection,left.permissions,left.created_at,left.updated_at)==(right.installation_id,right.app_id,
        right.account_id,right.account_login,right.account_type,right.repository_selection,right.permissions,right.created_at,right.updated_at)

def _aware(value:datetime)->datetime:
    if not isinstance(value,datetime) or value.tzinfo is None or value.utcoffset() is None:raise GitHubInstallationConflict("clock is invalid")
    return value
