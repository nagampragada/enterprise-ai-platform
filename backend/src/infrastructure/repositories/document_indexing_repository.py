"""Tenant-safe durable document indexing state and attempt persistence."""
from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID,uuid4
from sqlalchemy import and_,func,or_,select,update
from sqlalchemy.exc import IntegrityError,SQLAlchemyError
from sqlalchemy.orm import Session
from infrastructure.db.models import DocumentVersion,DocumentIndexingState,DocumentIndexingAttempt
from infrastructure.repositories.connector_repository import ConnectorRepositoryConflict,ConnectorRepositoryPersistenceError,InvalidConnectorRepositoryRequest,_require_aware,_require_choice,_require_code,_require_json_object,_require_limit,_require_positive_integer,_require_uuid
from infrastructure.repositories.document_version_repository import _require_safe_json
MAX_INDEXING_PAGE_LIMIT=100
STATE_STATUSES=frozenset({"pending","processing","indexed","stale","failed","cancelled"});STATE_REASONS=frozenset({"new_version","content_changed","profile_changed","embedding_model_changed","manual_backfill","repair"});ATTEMPT_TRIGGERS=frozenset({"sync","retry","manual_backfill","scheduled_backfill","repair"});ATTEMPT_STATUSES=frozenset({"running","succeeded","failed","cancelled"})
_PROFILE_IDENTIFIER=re.compile(r"^[a-z0-9][a-z0-9._:/-]*$")
@dataclass(frozen=True)
class IndexingWorkPageCursor:requested_at:datetime;state_id:UUID
@dataclass(frozen=True)
class IndexingWorkPage:items:tuple[DocumentIndexingState,...];limit:int;has_more:bool;next_cursor:IndexingWorkPageCursor|None
@dataclass(frozen=True)
class IndexingAttemptPageCursor:attempt_number:int;attempt_id:UUID
@dataclass(frozen=True)
class IndexingAttemptPage:items:tuple[DocumentIndexingAttempt,...];limit:int;has_more:bool;next_cursor:IndexingAttemptPageCursor|None

class DocumentIndexingRepository:
    def __init__(self,session:Session)->None:self._session=session
    def get_state(self,org,version,fingerprint):return self._one(self._state_query(org,version,fingerprint))
    def lock_state(self,org,version,fingerprint):return self._one(self._state_query(org,version,fingerprint).with_for_update())
    def add_state(self,org,version,state):
        _validate_state(org,version,state);self._session.add(state);self._flush("indexing state could not be created");return state
    def get_or_create_state(self,org,version,state):
        _validate_state(org,version,state);locked=self._one(select(DocumentVersion).where(DocumentVersion.organization_id==_require_uuid("organization_id",org),DocumentVersion.id==_require_uuid("version_id",version)).with_for_update())
        if locked is None:raise InvalidConnectorRepositoryRequest("document version was not found")
        existing=self.get_state(org,version,state.profile_fingerprint)
        if existing:return existing
        return self.add_state(org,version,state)
    def list_work(self,org,*,limit=100,cursor=None,status=None,reason=None,profile_fingerprint=None,embedding_model=None,embedding_dimensions=None,retry_ready_at=None,version_id=None,source_item_id=None):
        _require_uuid("organization_id",org);_require_limit(limit);_work_cursor(cursor);q=select(DocumentIndexingState)
        if source_item_id is not None:q=q.join(DocumentVersion,and_(DocumentVersion.organization_id==DocumentIndexingState.organization_id,DocumentVersion.id==DocumentIndexingState.document_version_id)).where(DocumentVersion.source_item_id==_require_uuid("source_item_id",source_item_id))
        q=q.where(DocumentIndexingState.organization_id==org)
        if version_id is not None:q=q.where(DocumentIndexingState.document_version_id==_require_uuid("version_id",version_id))
        if status is not None:q=q.where(DocumentIndexingState.status==_require_choice("status",status,STATE_STATUSES))
        if reason is not None:q=q.where(DocumentIndexingState.reason==_require_choice("reason",reason,STATE_REASONS))
        if profile_fingerprint is not None:q=q.where(DocumentIndexingState.profile_fingerprint==_identifier("profile_fingerprint",profile_fingerprint))
        if embedding_model is not None:q=q.where(DocumentIndexingState.embedding_model==_identifier("embedding_model",embedding_model))
        if embedding_dimensions is not None:q=q.where(DocumentIndexingState.embedding_dimensions==_require_positive_integer("embedding_dimensions",embedding_dimensions))
        if retry_ready_at is not None:_require_aware("retry_ready_at",retry_ready_at);q=q.where(DocumentIndexingState.next_retry_at.is_not(None),DocumentIndexingState.next_retry_at<=retry_ready_at)
        if cursor:q=q.where(or_(DocumentIndexingState.requested_at>cursor.requested_at,and_(DocumentIndexingState.requested_at==cursor.requested_at,DocumentIndexingState.id>cursor.state_id)))
        rows=self._all(q.order_by(DocumentIndexingState.requested_at,DocumentIndexingState.id).limit(limit+1));items=tuple(rows[:limit]);more=len(rows)>limit;next_cursor=IndexingWorkPageCursor(items[-1].requested_at,items[-1].id) if more and items else None;return IndexingWorkPage(items,limit,more,next_cursor)
    def persist_controlled_state(self,org,version,fingerprint,*,status,desired_generation,indexed_generation,attempt_count,requested_at,started_at=None,completed_at=None,last_attempt_at=None,next_retry_at=None,error_category=None,error_code=None):
        _state_values(status,desired_generation,indexed_generation,attempt_count,requested_at,started_at,completed_at,last_attempt_at,next_retry_at,error_category,error_code)
        return self._updated(update(DocumentIndexingState).where(*self._state_predicates(org,version,fingerprint)).values(status=status,desired_generation=desired_generation,indexed_generation=indexed_generation,attempt_count=attempt_count,requested_at=requested_at,started_at=started_at,completed_at=completed_at,last_attempt_at=last_attempt_at,next_retry_at=next_retry_at,last_error_category=error_category,last_error_code=error_code).returning(DocumentIndexingState))
    def request_generation(self,org,version,fingerprint,*,desired_generation,status,reason,requested_at):
        target=_require_positive_integer("desired_generation",desired_generation)
        _require_choice("status",status,STATE_STATUSES);_require_choice("reason",reason,STATE_REASONS);_require_aware("requested_at",requested_at)
        state=self.lock_state(org,version,fingerprint)
        if state is None:return None
        if target<=state.desired_generation:raise InvalidConnectorRepositoryRequest("desired_generation must increase")
        state.desired_generation=target;state.status=status;state.reason=reason;state.requested_at=requested_at;state.started_at=None;state.completed_at=None;state.next_retry_at=None;state.last_error_category=None;state.last_error_code=None;self._flush("indexing generation could not be requested");return state
    def allocate_attempt(self,org,version,fingerprint,*,trigger_type,started_at,sync_run_id=None,sync_item_id=None,worker_reference=None):
        trigger=_require_choice("trigger_type",trigger_type,ATTEMPT_TRIGGERS);_require_aware("started_at",started_at)
        if sync_run_id is not None:_require_uuid("sync_run_id",sync_run_id)
        if sync_item_id is not None:
            _require_uuid("sync_item_id",sync_item_id)
            if sync_run_id is None:raise InvalidConnectorRepositoryRequest("sync item requires sync run")
        if worker_reference is not None and (not isinstance(worker_reference,str) or not worker_reference.strip()):raise InvalidConnectorRepositoryRequest("worker_reference must be nonblank")
        state=self.lock_state(org,version,fingerprint)
        if state is None:raise InvalidConnectorRepositoryRequest("indexing state was not found")
        number=int(self._scalar(select(func.coalesce(func.max(DocumentIndexingAttempt.attempt_number),0)+1).where(DocumentIndexingAttempt.organization_id==org,DocumentIndexingAttempt.indexing_state_id==state.id)))
        state.attempt_count+=1;state.last_attempt_at=started_at;state.status="processing";state.started_at=started_at;state.completed_at=None;state.next_retry_at=None;state.last_error_category=None;state.last_error_code=None;self._flush("indexing state could not start attempt")
        row=DocumentIndexingAttempt(id=uuid4(),organization_id=org,indexing_state_id=state.id,connector_sync_run_id=sync_run_id,connector_sync_item_id=sync_item_id,attempt_number=number,trigger_type=trigger,status="running",worker_reference=worker_reference,started_at=started_at,retryable=False,summary={},summary_schema_version=1);self._session.add(row);self._flush("indexing attempt could not be created");return row
    def get_attempt(self,org,state_id,attempt_id):
        return self._one(select(DocumentIndexingAttempt).where(DocumentIndexingAttempt.organization_id==_require_uuid("organization_id",org),DocumentIndexingAttempt.indexing_state_id==_require_uuid("state_id",state_id),DocumentIndexingAttempt.id==_require_uuid("attempt_id",attempt_id)))
    def list_attempts(self,org,state_id,*,limit=100,cursor=None,status=None,trigger_type=None,sync_run_id=None):
        _require_uuid("organization_id",org);_require_uuid("state_id",state_id);_require_limit(limit);_attempt_cursor(cursor);q=select(DocumentIndexingAttempt).where(DocumentIndexingAttempt.organization_id==org,DocumentIndexingAttempt.indexing_state_id==state_id)
        if status is not None:q=q.where(DocumentIndexingAttempt.status==_require_choice("status",status,ATTEMPT_STATUSES))
        if trigger_type is not None:q=q.where(DocumentIndexingAttempt.trigger_type==_require_choice("trigger_type",trigger_type,ATTEMPT_TRIGGERS))
        if sync_run_id is not None:q=q.where(DocumentIndexingAttempt.connector_sync_run_id==_require_uuid("sync_run_id",sync_run_id))
        if cursor:q=q.where(or_(DocumentIndexingAttempt.attempt_number>cursor.attempt_number,and_(DocumentIndexingAttempt.attempt_number==cursor.attempt_number,DocumentIndexingAttempt.id>cursor.attempt_id)))
        rows=self._all(q.order_by(DocumentIndexingAttempt.attempt_number,DocumentIndexingAttempt.id).limit(limit+1));items=tuple(rows[:limit]);more=len(rows)>limit;next_cursor=IndexingAttemptPageCursor(items[-1].attempt_number,items[-1].id) if more and items else None;return IndexingAttemptPage(items,limit,more,next_cursor)
    def complete_attempt(self,org,state_id,attempt_id,*,status,completed_at,retryable,error_category=None,error_code=None,summary=None,summary_schema_version=1):
        terminal=_require_choice("status",status,frozenset({"succeeded","failed","cancelled"}));_require_aware("completed_at",completed_at)
        if not isinstance(retryable,bool):raise InvalidConnectorRepositoryRequest("retryable must be boolean")
        safe_summary={} if summary is None else summary
        _require_safe_json("summary",safe_summary);_require_positive_integer("summary_schema_version",summary_schema_version);_error_pair(error_category,error_code)
        if terminal=="succeeded" and error_category is not None:raise InvalidConnectorRepositoryRequest("succeeded attempt cannot contain errors")
        if terminal=="failed" and error_category is None:raise InvalidConnectorRepositoryRequest("failed attempt requires safe error fields")
        predicates=(DocumentIndexingAttempt.organization_id==_require_uuid("organization_id",org),DocumentIndexingAttempt.indexing_state_id==_require_uuid("state_id",state_id),DocumentIndexingAttempt.id==_require_uuid("attempt_id",attempt_id))
        attempt=self._one(select(DocumentIndexingAttempt).where(*predicates).with_for_update())
        if attempt is None:return None
        if attempt.status!="running":raise InvalidConnectorRepositoryRequest("attempt is not running")
        if completed_at<attempt.started_at:raise InvalidConnectorRepositoryRequest("completed_at cannot precede started_at")
        attempt.status=terminal;attempt.completed_at=completed_at;attempt.retryable=retryable;attempt.error_category=error_category;attempt.error_code=error_code;attempt.summary=safe_summary;attempt.summary_schema_version=summary_schema_version;self._flush("indexing attempt could not be completed");return attempt
    def _state_predicates(self,org,version,fingerprint):return (DocumentIndexingState.organization_id==_require_uuid("organization_id",org),DocumentIndexingState.document_version_id==_require_uuid("version_id",version),DocumentIndexingState.profile_fingerprint==_identifier("profile_fingerprint",fingerprint))
    def _state_query(self,org,version,fingerprint):return select(DocumentIndexingState).where(*self._state_predicates(org,version,fingerprint))
    def _one(self,q):
        try:return self._session.execute(q).scalar_one_or_none()
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError("indexing query failed") from exc
    def _all(self,q):
        try:return list(self._session.execute(q).scalars().all())
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError("indexing query failed") from exc
    def _updated(self,q):
        try:return self._session.execute(q).scalar_one_or_none()
        except IntegrityError as exc:raise ConnectorRepositoryConflict("indexing state conflicts with persisted constraints") from exc
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError("indexing state could not be persisted") from exc
    def _scalar(self,q):
        try:return self._session.execute(q).scalar_one()
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError("indexing query failed") from exc
    def _flush(self,message):
        try:self._session.flush()
        except IntegrityError as exc:raise ConnectorRepositoryConflict(message) from exc
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError(message) from exc

def _identifier(name,value):
    if not isinstance(value,str) or not _PROFILE_IDENTIFIER.fullmatch(value):raise InvalidConnectorRepositoryRequest(f"{name} must be normalized and nonblank")
    return value
def _error_pair(category,code):
    if (category is None)!=(code is None):raise InvalidConnectorRepositoryRequest("error category and code must be paired")
    if category is not None:_require_code("error_category",category);_require_code("error_code",code)
def _validate_state(org,version,state):
    if not isinstance(state,DocumentIndexingState) or state.organization_id!=org or state.document_version_id!=version:raise InvalidConnectorRepositoryRequest("indexing state context is invalid")
    for name,value in (("extraction_profile",state.extraction_profile),("extraction_version",state.extraction_version),("chunking_profile",state.chunking_profile),("chunking_version",state.chunking_version),("embedding_provider",state.embedding_provider),("embedding_model",state.embedding_model),("profile_fingerprint",state.profile_fingerprint)):_identifier(name,value)
    _require_positive_integer("embedding_dimensions",state.embedding_dimensions);_require_positive_integer("desired_generation",state.desired_generation)
    if state.indexed_generation is not None:
        _require_positive_integer("indexed_generation",state.indexed_generation)
        if state.indexed_generation>state.desired_generation:raise InvalidConnectorRepositoryRequest("indexed_generation cannot exceed desired_generation")
    _require_choice("status",state.status,STATE_STATUSES);_require_choice("reason",state.reason,STATE_REASONS);_require_aware("requested_at",state.requested_at);_error_pair(state.last_error_category,state.last_error_code)
    if isinstance(state.attempt_count,bool) or not isinstance(state.attempt_count,int) or state.attempt_count<0:raise InvalidConnectorRepositoryRequest("attempt_count must be nonnegative")
    for name,value in (("started_at",state.started_at),("completed_at",state.completed_at),("last_attempt_at",state.last_attempt_at),("next_retry_at",state.next_retry_at)):
        if value is not None:_require_aware(name,value)
def _work_cursor(value):
    if value is None:return
    if not isinstance(value,IndexingWorkPageCursor):raise InvalidConnectorRepositoryRequest("cursor is invalid")
    _require_aware("cursor.requested_at",value.requested_at);_require_uuid("cursor.state_id",value.state_id)
def _attempt_cursor(value):
    if value is None:return
    if not isinstance(value,IndexingAttemptPageCursor):raise InvalidConnectorRepositoryRequest("cursor is invalid")
    _require_positive_integer("cursor.attempt_number",value.attempt_number);_require_uuid("cursor.attempt_id",value.attempt_id)
def _state_values(status,desired,indexed,count,requested,started,completed,last_attempt,retry,category,code):
    _require_choice("status",status,STATE_STATUSES);_require_positive_integer("desired_generation",desired)
    if indexed is not None:_require_positive_integer("indexed_generation",indexed)
    if indexed is not None and indexed>desired:raise InvalidConnectorRepositoryRequest("indexed_generation cannot exceed desired_generation")
    if isinstance(count,bool) or not isinstance(count,int) or count<0:raise InvalidConnectorRepositoryRequest("attempt_count must be nonnegative")
    _require_aware("requested_at",requested)
    for name,value in (("started_at",started),("completed_at",completed),("last_attempt_at",last_attempt),("next_retry_at",retry)):
        if value is not None:_require_aware(name,value)
    _error_pair(category,code)
