"""Tenant-safe authoritative GitHub App installation binding persistence."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID,uuid4
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError,SQLAlchemyError
from sqlalchemy.orm import Session
from application.ports.github_app import GitHubInstallation
from infrastructure.db.models import GitHubAppInstallation

class GitHubInstallationConflict(RuntimeError): pass
class GitHubInstallationPersistenceError(RuntimeError): pass

@dataclass(frozen=True)
class GitHubInstallationView:
    connector_id:UUID; credential_id:UUID; github_app_id:int; github_installation_id:int
    account_id:int; account_login:str; account_type:str; repository_selection:str; status:str
    provider_created_at:datetime; provider_updated_at:datetime; last_verified_at:datetime
    disconnected_at:datetime|None; created_at:datetime; updated_at:datetime

class GitHubAppInstallationRepository:
    def __init__(self,session:Session)->None:self._session=session
    def get(self,organization_id:UUID,connector_id:UUID)->GitHubInstallationView|None:
        row=self._one(select(GitHubAppInstallation).where(GitHubAppInstallation.organization_id==organization_id,GitHubAppInstallation.connector_id==connector_id))
        return _view(row) if row else None
    def lock(self,organization_id:UUID,connector_id:UUID)->GitHubAppInstallation|None:
        return self._one(select(GitHubAppInstallation).where(GitHubAppInstallation.organization_id==organization_id,GitHubAppInstallation.connector_id==connector_id).with_for_update())
    def bind(self,organization_id:UUID,connector_id:UUID,credential_id:UUID,installation:GitHubInstallation,*,now:datetime)->GitHubInstallationView:
        row=self.lock(organization_id,connector_id)
        if row is None:
            row=GitHubAppInstallation(id=uuid4(),organization_id=organization_id,connector_id=connector_id,credential_id=credential_id,
                github_app_id=installation.app_id,github_installation_id=installation.installation_id,account_id=installation.account_id,
                account_login=installation.account_login,account_type=installation.account_type,repository_selection=installation.repository_selection,
                status="connected",provider_created_at=installation.created_at,provider_updated_at=installation.updated_at,
                last_verified_at=now,disconnected_at=None,created_at=now,updated_at=now);self._session.add(row)
        else:
            row.credential_id=credential_id;row.github_app_id=installation.app_id;row.github_installation_id=installation.installation_id
            row.account_id=installation.account_id;row.account_login=installation.account_login;row.account_type=installation.account_type
            row.repository_selection=installation.repository_selection;row.status="connected";row.provider_created_at=installation.created_at
            row.provider_updated_at=installation.updated_at;row.last_verified_at=now;row.disconnected_at=None;row.updated_at=now
        self._flush();return _view(row)
    def disconnect(self,row:GitHubAppInstallation,*,now:datetime)->GitHubInstallationView:
        row.status="disconnected";row.disconnected_at=now;row.updated_at=now;self._flush();return _view(row)
    def _one(self,q):
        try:return self._session.execute(q).scalar_one_or_none()
        except SQLAlchemyError as exc:raise GitHubInstallationPersistenceError("GitHub installation could not be read") from exc
    def _flush(self):
        try:self._session.flush()
        except IntegrityError as exc:raise GitHubInstallationConflict("GitHub installation binding conflicted") from exc
        except SQLAlchemyError as exc:raise GitHubInstallationPersistenceError("GitHub installation could not be persisted") from exc
def _view(r):return GitHubInstallationView(r.connector_id,r.credential_id,r.github_app_id,r.github_installation_id,r.account_id,r.account_login,r.account_type,r.repository_selection,r.status,r.provider_created_at,r.provider_updated_at,r.last_verified_at,r.disconnected_at,r.created_at,r.updated_at)
