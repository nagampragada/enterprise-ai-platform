"""Tenant-safe connector synchronization operational persistence."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from sqlalchemy import and_, null, or_, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session
from infrastructure.db.models import ConnectorSyncRun, ConnectorSyncItem, ConnectorSyncError, ConnectorSyncCursor
from infrastructure.repositories.connector_repository import ConnectorRepositoryConflict, ConnectorRepositoryPersistenceError, InvalidConnectorRepositoryRequest, _require_aware, _require_choice, _require_code, _require_json_object, _require_limit, _require_positive_integer, _require_uuid

MAX_SYNC_PAGE_LIMIT=100
RUN_MODES=frozenset({"initial","incremental","retry","reconciliation"}); RUN_TRIGGERS=frozenset({"manual","scheduled","webhook","retry","system"}); RUN_STATUSES=frozenset({"queued","running","cancelling","cancelled","completed","completed_with_errors","failed"}); CHANGE_TYPES=frozenset({"new","changed","unchanged","deleted","unknown"}); ITEM_STATUSES=frozenset({"pending","processing","succeeded","skipped","failed"}); ERROR_CATEGORIES=frozenset({"configuration","authentication","authorization","rate_limit","source_read","extraction","persistence","embedding","permission","internal"}); COUNTERS=("items_discovered","items_new","items_changed","items_unchanged","items_deleted","items_skipped","items_succeeded","items_failed")

@dataclass(frozen=True)
class SyncPageCursor: created_at: datetime; row_id: UUID
@dataclass(frozen=True)
class CursorHistoryPageCursor: cursor_version:int; cursor_id:UUID
@dataclass(frozen=True)
class SyncPage: items: tuple[object,...]; limit:int; has_more:bool; next_cursor:SyncPageCursor|CursorHistoryPageCursor|None
@dataclass(frozen=True)
class SafeSyncError:
    category:str; code:str; message:str; retryable:bool; attempt_number:int; details:dict[str,object]; occurred_at:datetime; retry_after_at:datetime|None=None

class ConnectorSyncRepository:
    def __init__(self,session:Session)->None:self._session=session
    def add_run(self,org:UUID,connector:UUID,scope:UUID,run:ConnectorSyncRun)->ConnectorSyncRun:
        _validate_run(org,connector,scope,run);self._session.add(run);self._flush("sync run could not be created");return run
    def get_run(self,org,connector,scope,run_id):return self._one(self._run_query(org,connector,scope,run_id))
    def lock_run(self,org,connector,scope,run_id):return self._one(self._run_query(org,connector,scope,run_id).with_for_update())
    def list_runs(self,org:UUID,*,limit=100,cursor=None,connector_id=None,scope_id=None,status=None,mode=None,trigger_type=None):
        _require_uuid("organization_id",org);_require_limit(limit);_cursor(cursor);q=select(ConnectorSyncRun).where(ConnectorSyncRun.organization_id==org)
        if connector_id is not None:q=q.where(ConnectorSyncRun.connector_id==_require_uuid("connector_id",connector_id))
        if scope_id is not None:q=q.where(ConnectorSyncRun.connector_scope_id==_require_uuid("scope_id",scope_id))
        if status is not None:q=q.where(ConnectorSyncRun.status==_require_choice("status",status,RUN_STATUSES))
        if mode is not None:q=q.where(ConnectorSyncRun.mode==_require_choice("mode",mode,RUN_MODES))
        if trigger_type is not None:q=q.where(ConnectorSyncRun.trigger_type==_require_choice("trigger_type",trigger_type,RUN_TRIGGERS))
        return self._page(q,ConnectorSyncRun,limit,cursor)
    def set_run_state(self,org,connector,scope,run_id,*,status,started_at=None,heartbeat_at=None,cancel_requested_at=None,finished_at=None,error_summary=None):
        _require_choice("status",status,RUN_STATUSES)
        for name,value in (("started_at",started_at),("heartbeat_at",heartbeat_at),("cancel_requested_at",cancel_requested_at),("finished_at",finished_at)):
            if value is not None:_require_aware(name,value)
        if error_summary is not None and (not isinstance(error_summary,str) or not error_summary.strip()):raise InvalidConnectorRepositoryRequest("error_summary must be nonblank")
        return self._updated(update(ConnectorSyncRun).where(*self._run_predicates(org,connector,scope,run_id)).values(status=status,started_at=started_at,heartbeat_at=heartbeat_at,cancel_requested_at=cancel_requested_at,finished_at=finished_at,error_summary=error_summary).returning(ConnectorSyncRun))
    def increment_counters(self,org,connector,scope,run_id,**deltas):
        if not deltas or any(k not in COUNTERS for k in deltas):raise InvalidConnectorRepositoryRequest("counter deltas are invalid")
        for value in deltas.values():
            if isinstance(value,bool) or not isinstance(value,int) or value<0:raise InvalidConnectorRepositoryRequest("counter deltas must be nonnegative integers")
        values={name:getattr(ConnectorSyncRun,name)+delta for name,delta in deltas.items()};return self._updated(update(ConnectorSyncRun).where(*self._run_predicates(org,connector,scope,run_id)).values(**values).returning(ConnectorSyncRun))
    def add_item(self,org,connector,scope,run_id,item):
        _validate_item(org,connector,scope,run_id,item);self._session.add(item);self._flush("sync item could not be created");return item
    def get_item(self,org,connector,run_id,item_id):return self._one(self._item_query(org,connector,run_id,item_id))
    def lock_item(self,org,connector,run_id,item_id):return self._one(self._item_query(org,connector,run_id,item_id).with_for_update())
    def get_item_by_key(self,org,connector,run_id,key):
        return self._one(select(ConnectorSyncItem).where(ConnectorSyncItem.organization_id==_require_uuid("organization_id",org),ConnectorSyncItem.connector_id==_require_uuid("connector_id",connector),ConnectorSyncItem.sync_run_id==_require_uuid("run_id",run_id),ConnectorSyncItem.source_item_key==_key(key)))
    def list_items(self,org,connector,run_id,*,limit=100,cursor=None,status=None,change_type=None):
        _require_limit(limit);_cursor(cursor);q=select(ConnectorSyncItem).where(ConnectorSyncItem.organization_id==_require_uuid("organization_id",org),ConnectorSyncItem.connector_id==_require_uuid("connector_id",connector),ConnectorSyncItem.sync_run_id==_require_uuid("run_id",run_id))
        if status is not None:q=q.where(ConnectorSyncItem.processing_status==_require_choice("status",status,ITEM_STATUSES))
        if change_type is not None:q=q.where(ConnectorSyncItem.change_type==_require_choice("change_type",change_type,CHANGE_TYPES))
        return self._page(q,ConnectorSyncItem,limit,cursor)
    def set_item_state(self,org,connector,run_id,item_id,*,status,attempt_count,started_at=None,finished_at=None,source_item_id=None):
        _require_choice("status",status,ITEM_STATUSES)
        if isinstance(attempt_count,bool) or not isinstance(attempt_count,int) or attempt_count<0:raise InvalidConnectorRepositoryRequest("attempt_count must be nonnegative")
        for n,v in (("started_at",started_at),("finished_at",finished_at)):
            if v is not None:_require_aware(n,v)
        if source_item_id is not None:_require_uuid("source_item_id",source_item_id)
        return self._updated(update(ConnectorSyncItem).where(*self._item_predicates(org,connector,run_id,item_id)).values(processing_status=status,attempt_count=attempt_count,started_at=started_at,finished_at=finished_at,source_item_id=source_item_id).returning(ConnectorSyncItem))
    def add_error(self,org,connector,scope,run_id,error:SafeSyncError,*,item_id=None):
        _validate_error(error)
        row=ConnectorSyncError(id=UUID(int=uuid_int()),organization_id=_require_uuid("organization_id",org),connector_id=_require_uuid("connector_id",connector),connector_scope_id=_require_uuid("scope_id",scope),sync_run_id=_require_uuid("run_id",run_id),sync_item_id=_require_uuid("item_id",item_id) if item_id else None,error_category=error.category,error_code=error.code,message=error.message,retryable=error.retryable,attempt_number=error.attempt_number,details=error.details,retry_after_at=error.retry_after_at,occurred_at=error.occurred_at)
        self._session.add(row);self._flush("sync error could not be created");return row
    def list_errors(self,org,connector,run_id,*,limit=100,cursor=None,category=None,retryable=None,item_id=None):
        _require_limit(limit);_cursor(cursor);q=select(ConnectorSyncError).where(ConnectorSyncError.organization_id==_require_uuid("organization_id",org),ConnectorSyncError.connector_id==_require_uuid("connector_id",connector),ConnectorSyncError.sync_run_id==_require_uuid("run_id",run_id))
        if category is not None:q=q.where(ConnectorSyncError.error_category==_require_choice("category",category,ERROR_CATEGORIES))
        if retryable is not None:
            if not isinstance(retryable,bool):raise InvalidConnectorRepositoryRequest("retryable must be boolean")
            q=q.where(ConnectorSyncError.retryable==retryable)
        if item_id is not None:q=q.where(ConnectorSyncError.sync_item_id==_require_uuid("item_id",item_id))
        return self._page(q,ConnectorSyncError,limit,cursor)
    def get_active_cursor(self,org,connector,scope,*,lock=False):
        q=select(ConnectorSyncCursor).where(ConnectorSyncCursor.organization_id==_require_uuid("organization_id",org),ConnectorSyncCursor.connector_id==_require_uuid("connector_id",connector),ConnectorSyncCursor.connector_scope_id==_require_uuid("scope_id",scope),ConnectorSyncCursor.state=="active")
        return self._one(q.with_for_update() if lock else q)
    def list_cursors(self,org,connector,scope,*,limit=100,cursor:CursorHistoryPageCursor|None=None):
        _require_limit(limit);_cursor_history(cursor);q=select(ConnectorSyncCursor).where(ConnectorSyncCursor.organization_id==_require_uuid("organization_id",org),ConnectorSyncCursor.connector_id==_require_uuid("connector_id",connector),ConnectorSyncCursor.connector_scope_id==_require_uuid("scope_id",scope))
        if cursor:q=q.where(or_(ConnectorSyncCursor.cursor_version>cursor.cursor_version,and_(ConnectorSyncCursor.cursor_version==cursor.cursor_version,ConnectorSyncCursor.id>cursor.cursor_id)))
        rows=self._all(q.order_by(ConnectorSyncCursor.cursor_version,ConnectorSyncCursor.id).limit(limit+1));items=tuple(rows[:limit]);more=len(rows)>limit;next_cursor=CursorHistoryPageCursor(items[-1].cursor_version,items[-1].id) if more and items else None;return SyncPage(items,limit,more,next_cursor)
    def replace_active_cursor(self,org,connector,scope,run_id,*,version,cursor_type,activated_at,safe_cursor=None,secret_reference=None):
        _require_positive_integer("version",version);_require_code("cursor_type",cursor_type);_require_aware("activated_at",activated_at);_cursor_storage(safe_cursor,secret_reference)
        current=self.get_active_cursor(org,connector,scope,lock=True)
        if current and version<=current.cursor_version:raise InvalidConnectorRepositoryRequest("cursor version must increase")
        if current:
            current.state="superseded";current.retired_at=activated_at;self._flush("cursor could not be superseded")
        row=ConnectorSyncCursor(id=UUID(int=uuid_int()),organization_id=org,connector_id=connector,connector_scope_id=scope,created_by_run_id=_require_uuid("run_id",run_id),cursor_version=version,cursor_type=cursor_type,state="active",safe_cursor=null() if safe_cursor is None else safe_cursor,secret_reference=secret_reference,activated_at=activated_at)  # type: ignore[arg-type]
        self._session.add(row);self._flush("cursor could not be promoted");return row
    def _run_predicates(self,o,c,s,r):return (ConnectorSyncRun.organization_id==_require_uuid("organization_id",o),ConnectorSyncRun.connector_id==_require_uuid("connector_id",c),ConnectorSyncRun.connector_scope_id==_require_uuid("scope_id",s),ConnectorSyncRun.id==_require_uuid("run_id",r))
    def _run_query(self,o,c,s,r):return select(ConnectorSyncRun).where(*self._run_predicates(o,c,s,r))
    def _item_predicates(self,o,c,r,i):return (ConnectorSyncItem.organization_id==_require_uuid("organization_id",o),ConnectorSyncItem.connector_id==_require_uuid("connector_id",c),ConnectorSyncItem.sync_run_id==_require_uuid("run_id",r),ConnectorSyncItem.id==_require_uuid("item_id",i))
    def _item_query(self,o,c,r,i):return select(ConnectorSyncItem).where(*self._item_predicates(o,c,r,i))
    def _page(self,q,model,limit,cursor):
        if cursor:q=q.where(or_(model.created_at>cursor.created_at,and_(model.created_at==cursor.created_at,model.id>cursor.row_id)))
        rows=self._all(q.order_by(model.created_at,model.id).limit(limit+1));items=tuple(rows[:limit]);more=len(rows)>limit;next_cursor=SyncPageCursor(items[-1].created_at,items[-1].id) if more and items else None;return SyncPage(items,limit,more,next_cursor)
    def _one(self,q):
        try:return self._session.execute(q).scalar_one_or_none()
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError("sync query failed") from exc
    def _all(self,q):
        try:return list(self._session.execute(q).scalars().all())
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError("sync query failed") from exc
    def _updated(self,q):
        try:return self._session.execute(q).scalar_one_or_none()
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError("sync state could not be persisted") from exc
    def _flush(self,message):
        try:self._session.flush()
        except IntegrityError as exc:raise ConnectorRepositoryConflict(message) from exc
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError(message) from exc


def _cursor(value):
    if value is None:return
    if not isinstance(value,SyncPageCursor):raise InvalidConnectorRepositoryRequest("cursor is invalid")
    _require_aware("cursor.created_at",value.created_at);_require_uuid("cursor.row_id",value.row_id)
def _cursor_history(value):
    if value is None:return
    if not isinstance(value,CursorHistoryPageCursor):raise InvalidConnectorRepositoryRequest("cursor is invalid")
    _require_positive_integer("cursor.cursor_version",value.cursor_version);_require_uuid("cursor.cursor_id",value.cursor_id)
def _key(value):
    if not isinstance(value,str) or not value.strip():raise InvalidConnectorRepositoryRequest("source_item_key must be nonblank")
    return value
def _validate_run(o,c,s,row):
    if not isinstance(row,ConnectorSyncRun) or (row.organization_id,row.connector_id,row.connector_scope_id)!=(o,c,s):raise InvalidConnectorRepositoryRequest("sync run context is invalid")
    _require_uuid("organization_id",o);_require_uuid("connector_id",c);_require_uuid("scope_id",s);_require_choice("mode",row.mode,RUN_MODES);_require_choice("trigger_type",row.trigger_type,RUN_TRIGGERS);_require_choice("status",row.status,RUN_STATUSES);_require_json_object("run_metadata",row.run_metadata)
def _validate_item(o,c,s,r,row):
    if not isinstance(row,ConnectorSyncItem) or (row.organization_id,row.connector_id,row.connector_scope_id,row.sync_run_id)!=(o,c,s,r):raise InvalidConnectorRepositoryRequest("sync item context is invalid")
    _key(row.source_item_key);_require_choice("change_type",row.change_type,CHANGE_TYPES);_require_choice("processing_status",row.processing_status,ITEM_STATUSES)
def _validate_error(error):
    if not isinstance(error,SafeSyncError):raise InvalidConnectorRepositoryRequest("safe error is required")
    _require_choice("category",error.category,ERROR_CATEGORIES);_require_code("code",error.code);_require_positive_integer("attempt_number",error.attempt_number);_require_json_object("details",error.details);_require_aware("occurred_at",error.occurred_at)
    if not isinstance(error.message,str) or not error.message.strip() or len(error.message)>1000 or "Traceback (most recent call last)" in error.message or error.message.count("\n")>3:raise InvalidConnectorRepositoryRequest("safe error message is invalid")
    if not isinstance(error.retryable,bool):raise InvalidConnectorRepositoryRequest("retryable must be boolean")
def _cursor_storage(safe,secret):
    if (safe is None)==(secret is None):raise InvalidConnectorRepositoryRequest("exactly one cursor storage value is required")
    if safe is not None:_require_json_object("safe_cursor",safe)
    if secret is not None and (not isinstance(secret,str) or not secret.strip()):raise InvalidConnectorRepositoryRequest("secret_reference must be nonblank")
def uuid_int():
    import uuid
    return uuid.uuid4().int
