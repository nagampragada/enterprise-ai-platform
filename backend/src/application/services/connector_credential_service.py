"""Provider-neutral connector credential lifecycle with fail-closed secret cleanup."""
from __future__ import annotations
from datetime import UTC,datetime
from typing import Callable
from uuid import UUID
from application.ports.secret_store import SecretReference,SecretStore,SecretStoreError
from infrastructure.repositories.connector_credential_repository import ConnectorCredentialRepository,CredentialMetadata
from infrastructure.repositories.connector_repository import ConnectorRepository

class CredentialLifecycleNotFound(RuntimeError):pass
class CredentialLifecycleConflict(RuntimeError):pass

class ConnectorCredentialService:
    def __init__(self,session,secret_store:SecretStore,*,clock:Callable[[],datetime]=lambda:datetime.now(UTC))->None:
        self._connectors=ConnectorRepository(session);self._credentials=ConnectorCredentialRepository(session)
        self._secrets=secret_store;self._clock=clock
    def bind(self,organization_id:UUID,connector_id:UUID,creator_user_id:UUID,*,provider_key:str,
        auth_scheme:str,secret_reference:SecretReference|None,external_subject:str|None=None,
        display_label:str|None=None,granted_scopes:tuple[str,...]=(),expires_at:datetime|None=None)->CredentialMetadata:
        connector=self._connectors.lock_by_id(organization_id,connector_id)
        if connector is None:raise CredentialLifecycleNotFound("connector was not found")
        if connector.connector_type!=provider_key:raise CredentialLifecycleConflict("credential provider is incompatible")
        result=self._credentials.replace(organization_id,connector_id,provider_key=provider_key,
            auth_scheme=auth_scheme,secret_reference=secret_reference.value if secret_reference else None,external_subject=external_subject,
            display_label=display_label,granted_scopes=granted_scopes,expires_at=expires_at,
            created_by_user_id=creator_user_id,now=_aware(self._clock()))
        if result.previous_secret_reference and (secret_reference is None or result.previous_secret_reference!=secret_reference.value):
            self._delete_best_effort(result.previous_secret_reference)
        return result.metadata
    def metadata(self,organization_id:UUID,connector_id:UUID)->CredentialMetadata:
        value=self._credentials.get(organization_id,connector_id)
        if value is None:raise CredentialLifecycleNotFound("credential binding was not found")
        return value
    def validation_succeeded(self,organization_id:UUID,connector_id:UUID)->CredentialMetadata:
        return self._status(organization_id,connector_id,"active")
    def mark_invalid(self,organization_id:UUID,connector_id:UUID)->CredentialMetadata:
        return self._status(organization_id,connector_id,"invalid")
    def mark_expired(self,organization_id:UUID,connector_id:UUID)->CredentialMetadata:
        return self._status(organization_id,connector_id,"expired")
    def revoke(self,organization_id:UUID,connector_id:UUID)->CredentialMetadata:
        return self._status(organization_id,connector_id,"revoked")
    def disconnect(self,organization_id:UUID,connector_id:UUID)->CredentialMetadata:
        row=self._credentials.lock(organization_id,connector_id)
        if row is None:raise CredentialLifecycleNotFound("credential binding was not found")
        metadata=self._credentials.set_status(row,status="revoked",now=_aware(self._clock()))
        if row.secret_reference:self._delete_best_effort(row.secret_reference)
        return metadata
    def remove_revoked(self,organization_id:UUID,connector_id:UUID)->None:
        row=self._credentials.lock(organization_id,connector_id)
        if row is None:raise CredentialLifecycleNotFound("credential binding was not found")
        reference=row.secret_reference;self._credentials.remove(row)
        if reference:self._delete_best_effort(reference)
    def _status(self,organization_id,connector_id,status):
        row=self._credentials.lock(organization_id,connector_id)
        if row is None:raise CredentialLifecycleNotFound("credential binding was not found")
        return self._credentials.set_status(row,status=status,now=_aware(self._clock()))
    def _delete_best_effort(self,value):
        try:self._secrets.delete(SecretReference(value))
        except SecretStoreError:pass
def _aware(v):
    if not isinstance(v,datetime) or v.tzinfo is None or v.utcoffset() is None:raise CredentialLifecycleConflict("clock is invalid")
    return v
