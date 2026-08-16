"""Tenant-safe immutable document-version and materialization persistence."""
from __future__ import annotations
import re
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4
from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session
from infrastructure.db.models import Document, DocumentVersion, DocumentVersionDocument, SourceItem
from infrastructure.repositories.connector_repository import ConnectorRepositoryConflict, ConnectorRepositoryPersistenceError, InvalidConnectorRepositoryRequest, _require_aware, _require_choice, _require_code, _require_json_object, _require_limit, _require_positive_integer, _require_uuid

MAX_VERSION_PAGE_LIMIT=100
VERSION_CAUSES=frozenset({"discovered","content_changed","metadata_changed","restored","tombstone","manual_backfill"})
VERSION_LIFECYCLES=frozenset({"available","unavailable","deleted"})

@dataclass(frozen=True)
class DocumentVersionPageCursor: version_number:int; version_id:UUID
@dataclass(frozen=True)
class DocumentVersionPage: items:tuple[DocumentVersion,...];limit:int;has_more:bool;next_cursor:DocumentVersionPageCursor|None

class DocumentVersionRepository:
    def __init__(self,session:Session)->None:self._session=session
    def get_by_id(self,organization_id:UUID,source_item_id:UUID,version_id:UUID)->DocumentVersion|None:
        return self._one(self._version_query(organization_id,source_item_id).where(DocumentVersion.id==_require_uuid("version_id",version_id)))
    def get_current(self,organization_id:UUID,source_item_id:UUID)->DocumentVersion|None:
        return self._one(self._version_query(organization_id,source_item_id).where(DocumentVersion.is_current.is_(True)))
    def lock_current(self,organization_id:UUID,source_item_id:UUID)->DocumentVersion|None:
        return self._one(self._version_query(organization_id,source_item_id).where(DocumentVersion.is_current.is_(True)).with_for_update())
    def get_by_number(self,organization_id:UUID,source_item_id:UUID,version_number:int)->DocumentVersion|None:
        return self._one(self._version_query(organization_id,source_item_id).where(DocumentVersion.version_number==_require_positive_integer("version_number",version_number)))
    def list_history(self,organization_id:UUID,source_item_id:UUID,*,limit=100,cursor:DocumentVersionPageCursor|None=None,lifecycle=None,cause=None)->DocumentVersionPage:
        q=self._version_query(organization_id,source_item_id);_require_limit(limit);_cursor(cursor)
        if lifecycle is not None:q=q.where(DocumentVersion.lifecycle==_require_choice("lifecycle",lifecycle,VERSION_LIFECYCLES))
        if cause is not None:q=q.where(DocumentVersion.version_cause==_require_choice("cause",cause,VERSION_CAUSES))
        if cursor:q=q.where(or_(DocumentVersion.version_number>cursor.version_number,and_(DocumentVersion.version_number==cursor.version_number,DocumentVersion.id>cursor.version_id)))
        rows=self._all(q.order_by(DocumentVersion.version_number,DocumentVersion.id).limit(limit+1));items=tuple(rows[:limit]);more=len(rows)>limit;next_cursor=DocumentVersionPageCursor(items[-1].version_number,items[-1].id) if more and items else None;return DocumentVersionPage(items,limit,more,next_cursor)
    def create_current_version(self,organization_id:UUID,source_item_id:UUID,*,version_cause:str,lifecycle:str,discovered_at:datetime,provider_version_id=None,content_checksum=None,checksum_algorithm=None,source_modified_at=None,source_size_bytes=None,content_type=None,file_extension=None,metadata=None,metadata_schema_version=1)->DocumentVersion:
        version_metadata={} if metadata is None else metadata
        _validate_observation(version_cause,lifecycle,discovered_at,provider_version_id,content_checksum,checksum_algorithm,source_modified_at,source_size_bytes,content_type,file_extension,version_metadata,metadata_schema_version)
        source=self._one(select(SourceItem).where(SourceItem.organization_id==_require_uuid("organization_id",organization_id),SourceItem.id==_require_uuid("source_item_id",source_item_id)).with_for_update())
        if source is None:raise InvalidConnectorRepositoryRequest("source item was not found")
        current=self.lock_current(organization_id,source_item_id)
        next_number=self._scalar(select(func.coalesce(func.max(DocumentVersion.version_number),0)+1).where(DocumentVersion.organization_id==organization_id,DocumentVersion.source_item_id==source_item_id))
        if current:current.is_current=False;self._flush("current document version could not be replaced")
        row=DocumentVersion(id=uuid4(),organization_id=organization_id,connector_id=source.connector_id,source_item_id=source_item_id,version_number=int(next_number),provider_version_id=provider_version_id,content_checksum=content_checksum,checksum_algorithm=checksum_algorithm,source_modified_at=source_modified_at,source_size_bytes=source_size_bytes,content_type=content_type,file_extension=file_extension,version_cause=version_cause,lifecycle=lifecycle,is_current=True,discovered_at=discovered_at,version_metadata=version_metadata,metadata_schema_version=metadata_schema_version)
        self._session.add(row);self._flush("document version could not be created");return row
    def get_materialization(self,organization_id:UUID,version_id:UUID)->DocumentVersionDocument|None:
        return self._one(select(DocumentVersionDocument).where(DocumentVersionDocument.organization_id==_require_uuid("organization_id",organization_id),DocumentVersionDocument.document_version_id==_require_uuid("version_id",version_id)))
    def get_current_materialization(self,organization_id:UUID,source_item_id:UUID)->DocumentVersionDocument|None:
        return self._one(select(DocumentVersionDocument).join(DocumentVersion,and_(DocumentVersion.organization_id==DocumentVersionDocument.organization_id,DocumentVersion.id==DocumentVersionDocument.document_version_id)).where(DocumentVersion.organization_id==_require_uuid("organization_id",organization_id),DocumentVersion.source_item_id==_require_uuid("source_item_id",source_item_id),DocumentVersion.is_current.is_(True)))
    def replace_materialization(self,organization_id:UUID,source_item_id:UUID,version_id:UUID,document_id:UUID)->DocumentVersionDocument:
        org=_require_uuid("organization_id",organization_id);source_id=_require_uuid("source_item_id",source_item_id);version_uuid=_require_uuid("version_id",version_id);document_uuid=_require_uuid("document_id",document_id)
        source=self._one(select(SourceItem).where(SourceItem.organization_id==org,SourceItem.id==source_id).with_for_update());version=self._one(select(DocumentVersion).where(DocumentVersion.organization_id==org,DocumentVersion.source_item_id==source_id,DocumentVersion.id==version_uuid).with_for_update());document=self._one(select(Document).where(Document.organization_id==org,Document.id==document_uuid).with_for_update())
        if source is None or version is None or document is None:raise InvalidConnectorRepositoryRequest("materialization context was not found")
        mapping_ids=select(DocumentVersionDocument.id).join(DocumentVersion,and_(DocumentVersion.organization_id==DocumentVersionDocument.organization_id,DocumentVersion.id==DocumentVersionDocument.document_version_id)).where(DocumentVersionDocument.organization_id==org,or_(DocumentVersionDocument.document_version_id==version_uuid,DocumentVersionDocument.document_id==document_uuid,DocumentVersion.source_item_id==source_id)).with_for_update()
        ids=self._all(mapping_ids)
        if ids:self._delete(delete(DocumentVersionDocument).where(DocumentVersionDocument.id.in_(ids)));self._flush("materialization could not be replaced")
        row=DocumentVersionDocument(id=uuid4(),organization_id=org,document_version_id=version_uuid,document_id=document_uuid);self._session.add(row);self._flush("materialization could not be created");return row
    def remove_materialization(self,organization_id:UUID,source_item_id:UUID,version_id:UUID)->bool:
        org=_require_uuid("organization_id",organization_id);source=_require_uuid("source_item_id",source_item_id);version=_require_uuid("version_id",version_id)
        return bool(self._delete(delete(DocumentVersionDocument).where(DocumentVersionDocument.organization_id==org,DocumentVersionDocument.document_version_id==version,DocumentVersionDocument.document_version_id.in_(select(DocumentVersion.id).where(DocumentVersion.organization_id==org,DocumentVersion.source_item_id==source)))))
    def _version_query(self,org,source):return select(DocumentVersion).where(DocumentVersion.organization_id==_require_uuid("organization_id",org),DocumentVersion.source_item_id==_require_uuid("source_item_id",source))
    def _one(self,q):
        try:return self._session.execute(q).scalar_one_or_none()
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError("document version query failed") from exc
    def _all(self,q):
        try:return list(self._session.execute(q).scalars().all())
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError("document version query failed") from exc
    def _scalar(self,q):
        try:return self._session.execute(q).scalar_one()
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError("document version query failed") from exc
    def _delete(self,q):
        try:return int(self._session.execute(q).rowcount or 0)
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError("materialization could not be persisted") from exc
    def _flush(self,message):
        try:self._session.flush()
        except IntegrityError as exc:raise ConnectorRepositoryConflict(message) from exc
        except SQLAlchemyError as exc:raise ConnectorRepositoryPersistenceError(message) from exc

def _cursor(value):
    if value is None:return
    if not isinstance(value,DocumentVersionPageCursor):raise InvalidConnectorRepositoryRequest("cursor is invalid")
    _require_positive_integer("cursor.version_number",value.version_number);_require_uuid("cursor.version_id",value.version_id)
def _optional_nonblank(name,value):
    if value is not None and (not isinstance(value,str) or not value.strip()):raise InvalidConnectorRepositoryRequest(f"{name} must be nonblank")
def _validate_observation(cause,lifecycle,discovered,provider,checksum,algorithm,modified,size,content_type,extension,metadata,schema_version):
    _require_choice("version_cause",cause,VERSION_CAUSES);_require_choice("lifecycle",lifecycle,VERSION_LIFECYCLES);_require_aware("discovered_at",discovered);_require_safe_json("metadata",metadata);_require_positive_integer("metadata_schema_version",schema_version)
    for name,value in (("provider_version_id",provider),("content_checksum",checksum),("checksum_algorithm",algorithm),("content_type",content_type),("file_extension",extension)):_optional_nonblank(name,value)
    if (checksum is None)!=(algorithm is None):raise InvalidConnectorRepositoryRequest("checksum and algorithm must be paired")
    if algorithm is not None:_require_code("checksum_algorithm",algorithm)
    if modified is not None:_require_aware("source_modified_at",modified)
    if size is not None and (isinstance(size,bool) or not isinstance(size,int) or size<0):raise InvalidConnectorRepositoryRequest("source_size_bytes must be nonnegative")
    if cause=="tombstone" and (lifecycle not in {"deleted","unavailable"} or checksum is not None or size is not None):raise InvalidConnectorRepositoryRequest("tombstone observation is inconsistent")
def _require_safe_json(name,value):
    result=_require_json_object(name,value);forbidden={"content","raw_content","chunk","chunks","chunk_text","vector","vectors","embedding","embeddings","credential","credentials","token","access_token","refresh_token","secret","password","path","file_path","traceback","provider_response","provider_payload"}
    def inspect(item):
        if isinstance(item,dict):
            for key,nested in item.items():
                if not isinstance(key,str) or key.lower() in forbidden:raise InvalidConnectorRepositoryRequest(f"{name} contains unsafe data")
                inspect(nested)
        elif isinstance(item,list):
            for nested in item:inspect(nested)
        elif isinstance(item,str):
            lowered=item.lower()
            if "Traceback (most recent call last)" in item or "-----BEGIN " in item or "bearer " in lowered or "password=" in lowered or "secret=" in lowered or "api_key=" in lowered or re.match(r"^[a-zA-Z]:[\\/]",item) or item.startswith("/"):raise InvalidConnectorRepositoryRequest(f"{name} contains unsafe data")
    inspect(result);return result
