"""Short-lived single-use OAuth authorization transaction persistence."""
from __future__ import annotations
from datetime import datetime
import re
from uuid import UUID,uuid4
from sqlalchemy import select,update
from sqlalchemy.exc import IntegrityError,SQLAlchemyError
from sqlalchemy.orm import Session
from infrastructure.db.models import OAuthAuthorizationTransaction

_CODE=re.compile(r"^[a-z][a-z0-9_]*$")
class InvalidOAuthTransactionRequest(ValueError):pass
class OAuthTransactionConflict(RuntimeError):pass
class OAuthTransactionPersistenceError(RuntimeError):pass

class OAuthAuthorizationTransactionRepository:
    def __init__(self,session:Session)->None:self._session=session
    def create(self,organization_id:UUID,connector_id:UUID,initiating_user_id:UUID,*,provider_key:str,
        state_hash:bytes,pkce_reference:str|None,callback_identifier:str,created_at:datetime,
        expires_at:datetime)->OAuthAuthorizationTransaction:
        _uuid(organization_id);_uuid(connector_id);_uuid(initiating_user_id);_code(provider_key);_code(callback_identifier)
        _hash(state_hash);created_at=_aware(created_at);expires_at=_aware(expires_at)
        row=OAuthAuthorizationTransaction(id=uuid4(),organization_id=organization_id,connector_id=connector_id,
            initiating_user_id=initiating_user_id,provider_key=provider_key,state_hash=state_hash,
            pkce_verifier_secret_reference=_reference(pkce_reference),callback_identifier=callback_identifier,
            status="pending",created_at=created_at,expires_at=expires_at,consumed_at=None,failure_code=None,schema_version=1)
        self._session.add(row);self._flush("OAuth authorization transaction could not be created");return row
    def lock_by_state_hash(self,state_hash:bytes)->OAuthAuthorizationTransaction|None:
        return self._one(select(OAuthAuthorizationTransaction).where(OAuthAuthorizationTransaction.state_hash==_hash(state_hash)).with_for_update())
    def consume(self,row:OAuthAuthorizationTransaction,*,now:datetime)->OAuthAuthorizationTransaction:
        now=_aware(now);result=self._updated(update(OAuthAuthorizationTransaction).where(
            OAuthAuthorizationTransaction.id==row.id,OAuthAuthorizationTransaction.status=="pending",
            OAuthAuthorizationTransaction.expires_at>now).values(status="consumed",consumed_at=now).returning(OAuthAuthorizationTransaction))
        if result is None:raise OAuthTransactionConflict("OAuth authorization transaction is unavailable")
        return result
    def mark_failed(self,row,*,failure_code:str)->None:
        _code(failure_code);self._transition(row,"failed",failure_code=failure_code)
    def mark_expired(self,row)->None:self._transition(row,"expired",failure_code=None)
    def expire_stale(self,*,now:datetime,limit:int)->int:
        now=_aware(now)
        if isinstance(limit,bool) or not isinstance(limit,int) or not 1<=limit<=100:raise InvalidOAuthTransactionRequest("limit is invalid")
        ids=list(self._session.execute(select(OAuthAuthorizationTransaction.id).where(
            OAuthAuthorizationTransaction.status=="pending",OAuthAuthorizationTransaction.expires_at<=now
        ).order_by(OAuthAuthorizationTransaction.expires_at,OAuthAuthorizationTransaction.id).with_for_update(skip_locked=True).limit(limit)).scalars())
        if ids:self._session.execute(update(OAuthAuthorizationTransaction).where(OAuthAuthorizationTransaction.id.in_(ids)).values(status="expired"))
        return len(ids)
    def _transition(self,row,status,**values):
        result=self._updated(update(OAuthAuthorizationTransaction).where(OAuthAuthorizationTransaction.id==row.id,
            OAuthAuthorizationTransaction.status=="pending").values(status=status,**values).returning(OAuthAuthorizationTransaction))
        if result is None:raise OAuthTransactionConflict("OAuth authorization transaction is unavailable")
    def _one(self,q):
        try:return self._session.execute(q).scalar_one_or_none()
        except SQLAlchemyError as exc:raise OAuthTransactionPersistenceError("OAuth authorization transaction could not be read") from exc
    def _updated(self,q):
        try:return self._session.execute(q).scalar_one_or_none()
        except IntegrityError as exc:raise OAuthTransactionConflict("OAuth authorization transaction conflicted") from exc
        except SQLAlchemyError as exc:raise OAuthTransactionPersistenceError("OAuth authorization transaction could not be persisted") from exc
    def _flush(self,message):
        try:self._session.flush()
        except IntegrityError as exc:raise OAuthTransactionConflict(message) from exc
        except SQLAlchemyError as exc:raise OAuthTransactionPersistenceError(message) from exc
def _uuid(v):
    if not isinstance(v,UUID):raise InvalidOAuthTransactionRequest("identifier is invalid")
def _aware(v):
    if not isinstance(v,datetime) or v.tzinfo is None or v.utcoffset() is None:raise InvalidOAuthTransactionRequest("timestamp is invalid")
    return v
def _code(v):
    if not isinstance(v,str) or not _CODE.fullmatch(v):raise InvalidOAuthTransactionRequest("code is invalid")
    return v
def _hash(v):
    if not isinstance(v,bytes) or len(v)!=32:raise InvalidOAuthTransactionRequest("state digest is invalid")
    return v
def _reference(v):
    if v is not None and (not isinstance(v,str) or not v.strip() or len(v)>1024):raise InvalidOAuthTransactionRequest("secret reference is invalid")
    return v