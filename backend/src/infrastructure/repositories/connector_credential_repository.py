"""Tenant-safe connector credential binding persistence."""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
import json, re
from uuid import UUID, uuid4
from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session
from infrastructure.db.models import ConnectorCredential

_CODE=re.compile(r"^[a-z][a-z0-9_]*$")
SCHEMES=frozenset({"oauth2","api_token","service_account","app_installation"})

class InvalidCredentialRequest(ValueError): pass
class CredentialConflict(RuntimeError): pass
class CredentialPersistenceError(RuntimeError): pass

@dataclass(frozen=True)
class CredentialMetadata:
    credential_id:UUID; connector_id:UUID; provider_key:str; auth_scheme:str; status:str
    external_subject:str|None; display_label:str|None; granted_scopes:tuple[str,...]
    expires_at:datetime|None; validated_at:datetime|None; revoked_at:datetime|None
    created_at:datetime; updated_at:datetime

@dataclass(frozen=True,repr=False)
class CredentialReplacement:
    metadata:CredentialMetadata; previous_secret_reference:str|None

class ConnectorCredentialRepository:
    def __init__(self,session:Session)->None:self._session=session
    def replace(self,organization_id:UUID,connector_id:UUID,*,provider_key:str,auth_scheme:str,
        secret_reference:str,external_subject:str|None,display_label:str|None,
        granted_scopes:tuple[str,...],expires_at:datetime|None,created_by_user_id:UUID|None,
        now:datetime)->CredentialReplacement:
        _uuid(organization_id);_uuid(connector_id);provider_key=_code(provider_key);auth_scheme=_choice(auth_scheme,SCHEMES)
        secret_reference=_reference(secret_reference);scopes=_scopes(granted_scopes);now=_aware(now)
        if expires_at is not None: expires_at=_aware(expires_at)
        if expires_at is not None and expires_at<=now: raise InvalidCredentialRequest("credential expiry is invalid")
        current=self.lock(organization_id,connector_id);previous=current.secret_reference if current else None
        if current is None:
            current=ConnectorCredential(id=uuid4(),organization_id=organization_id,connector_id=connector_id,
                provider_key=provider_key,auth_scheme=auth_scheme,secret_reference=secret_reference,status="active",
                external_subject=_optional(external_subject),display_label=_optional(display_label),
                granted_scopes=list(scopes),expires_at=expires_at,validated_at=None,revoked_at=None,
                created_by_user_id=created_by_user_id,schema_version=1,created_at=now,updated_at=now)
            self._session.add(current);self._flush("credential binding could not be created")
        else:
            current.provider_key=provider_key;current.auth_scheme=auth_scheme;current.secret_reference=secret_reference
            current.status="active";current.external_subject=_optional(external_subject);current.display_label=_optional(display_label)
            current.granted_scopes=list(scopes);current.expires_at=expires_at;current.validated_at=None
            current.revoked_at=None;current.created_by_user_id=created_by_user_id;current.updated_at=now
            self._flush("credential binding could not be replaced")
        return CredentialReplacement(_metadata(current),previous)
    def get(self,organization_id:UUID,connector_id:UUID)->CredentialMetadata|None:
        row=self._one(select(ConnectorCredential).where(ConnectorCredential.organization_id==_uuid(organization_id),ConnectorCredential.connector_id==_uuid(connector_id)))
        return _metadata(row) if row else None
    def lock(self,organization_id:UUID,connector_id:UUID)->ConnectorCredential|None:
        return self._one(select(ConnectorCredential).where(ConnectorCredential.organization_id==_uuid(organization_id),ConnectorCredential.connector_id==_uuid(connector_id)).with_for_update())
    def set_status(self,row:ConnectorCredential,*,status:str,now:datetime)->CredentialMetadata:
        if status not in {"active","expired","revoked","invalid"}:raise InvalidCredentialRequest("credential status is invalid")
        now=_aware(now);row.status=status;row.revoked_at=now if status=="revoked" else None
        if status=="active":row.validated_at=now
        row.updated_at=now;self._flush("credential lifecycle could not be persisted");return _metadata(row)
    def remove(self,row:ConnectorCredential)->None:
        if row.status!="revoked":raise CredentialConflict("credential must be revoked before removal")
        try:self._session.execute(delete(ConnectorCredential).where(ConnectorCredential.id==row.id))
        except SQLAlchemyError as exc:raise CredentialPersistenceError("credential binding could not be removed") from exc
    def _one(self,q):
        try:return self._session.execute(q).scalar_one_or_none()
        except SQLAlchemyError as exc:raise CredentialPersistenceError("credential binding could not be read") from exc
    def _flush(self,message):
        try:self._session.flush()
        except IntegrityError as exc:raise CredentialConflict(message) from exc
        except SQLAlchemyError as exc:raise CredentialPersistenceError(message) from exc

def _metadata(r):return CredentialMetadata(r.id,r.connector_id,r.provider_key,r.auth_scheme,r.status,r.external_subject,r.display_label,tuple(r.granted_scopes),r.expires_at,r.validated_at,r.revoked_at,r.created_at,r.updated_at)
def _uuid(v):
    if not isinstance(v,UUID):raise InvalidCredentialRequest("identifier is invalid")
    return v
def _aware(v):
    if not isinstance(v,datetime) or v.tzinfo is None or v.utcoffset() is None:raise InvalidCredentialRequest("timestamp is invalid")
    return v
def _code(v):
    if not isinstance(v,str) or not _CODE.fullmatch(v):raise InvalidCredentialRequest("code is invalid")
    return v
def _choice(v,c):
    if v not in c:raise InvalidCredentialRequest("choice is invalid")
    return v
def _reference(v):
    if not isinstance(v,str) or not v.strip() or len(v)>1024:raise InvalidCredentialRequest("secret reference is invalid")
    return v
def _optional(v):
    if v is not None and (not isinstance(v,str) or not v.strip() or len(v)>255):raise InvalidCredentialRequest("safe label is invalid")
    return v.strip() if v else None
def _scopes(v):
    if not isinstance(v,tuple) or len(v)>100 or any(not isinstance(x,str) or not x.strip() or len(x)>255 for x in v):raise InvalidCredentialRequest("granted scopes are invalid")
    result=tuple(dict.fromkeys(x.strip() for x in v))
    if len(json.dumps(result))>32768:raise InvalidCredentialRequest("granted scopes are invalid")
    return result