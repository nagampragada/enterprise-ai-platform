"""Tenant-safe source-item and scope-membership persistence."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from sqlalchemy import and_, or_, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session
from infrastructure.db.models import (
    Document,
    DocumentVersion,
    DocumentVersionDocument,
    SourceItem,
    SourceItemScopeMembership,
)
from infrastructure.repositories.connector_repository import (
    ConnectorRepositoryConflict, ConnectorRepositoryPersistenceError,
    InvalidConnectorRepositoryRequest, _require_aware, _require_choice,
    _require_code, _require_json_object, _require_limit, _require_positive_integer, _require_uuid,
)

MAX_SOURCE_ITEM_PAGE_LIMIT = 100
MAX_MEMBERSHIP_RECONCILIATION_PAGE_LIMIT = 500
SOURCE_STATUSES = frozenset({"active", "deleted", "unavailable"})
MEMBERSHIP_STATUSES = frozenset({"active", "removed"})

@dataclass(frozen=True)
class SourceItemPageCursor:
    created_at: datetime
    source_item_id: UUID

@dataclass(frozen=True)
class SourceItemPage:
    items: tuple[SourceItem, ...]
    limit: int
    has_more: bool
    next_cursor: SourceItemPageCursor | None

@dataclass(frozen=True)
class MembershipReconciliationCursor:
    last_seen_at: datetime
    membership_id: UUID

@dataclass(frozen=True)
class MembershipReconciliationPage:
    items: tuple[SourceItemScopeMembership, ...]
    limit: int
    has_more: bool
    next_cursor: MembershipReconciliationCursor | None

class SourceItemRepository:
    def __init__(self, session: Session) -> None: self._session = session

    def add(self, organization_id: UUID, connector_id: UUID, item: SourceItem) -> SourceItem:
        _require_uuid("organization_id", organization_id); _require_uuid("connector_id", connector_id)
        _validate_item(organization_id, connector_id, item); self._session.add(item); self._flush("source item could not be created"); return item

    def get_by_id(self, organization_id: UUID, connector_id: UUID, source_item_id: UUID) -> SourceItem | None:
        return self._one(self._item_query(organization_id, connector_id).where(SourceItem.id == _require_uuid("source_item_id", source_item_id)))

    def get_by_key(self, organization_id: UUID, connector_id: UUID, source_item_key: str) -> SourceItem | None:
        return self._one(self._item_query(organization_id, connector_id).where(SourceItem.source_item_key == _require_key(source_item_key)))

    def lock_by_id(self, organization_id: UUID, connector_id: UUID, source_item_id: UUID) -> SourceItem | None:
        return self._one(self._item_query(organization_id, connector_id).where(SourceItem.id == _require_uuid("source_item_id", source_item_id)).with_for_update())

    def lock_by_key(self, organization_id: UUID, connector_id: UUID, source_item_key: str) -> SourceItem | None:
        return self._one(self._item_query(organization_id, connector_id).where(SourceItem.source_item_key == _require_key(source_item_key)).with_for_update())

    def list_page(self, organization_id: UUID, connector_id: UUID, *, limit: int = 100, cursor: SourceItemPageCursor | None = None, status: str | None = None, source_item_type: str | None = None, parent_source_item_key: str | None = None) -> SourceItemPage:
        statement = self._item_query(organization_id, connector_id); _require_limit(limit); _validate_cursor(cursor)
        if status is not None: statement = statement.where(SourceItem.status == _require_choice("status", status, SOURCE_STATUSES))
        if source_item_type is not None: statement = statement.where(SourceItem.source_item_type == _require_code("source_item_type", source_item_type))
        if parent_source_item_key is not None: statement = statement.where(SourceItem.parent_source_item_key == _require_key(parent_source_item_key))
        return self._page(statement, limit, cursor)

    def list_for_scope(self, organization_id: UUID, connector_id: UUID, scope_id: UUID, *, limit: int = 100, cursor: SourceItemPageCursor | None = None, active_memberships_only: bool = True, source_status: str | None = None) -> SourceItemPage:
        _require_uuid("organization_id", organization_id); _require_uuid("connector_id", connector_id); _require_uuid("scope_id", scope_id); _require_limit(limit); _validate_cursor(cursor)
        statement = select(SourceItem).join(SourceItemScopeMembership, and_(SourceItemScopeMembership.organization_id == SourceItem.organization_id, SourceItemScopeMembership.connector_id == SourceItem.connector_id, SourceItemScopeMembership.source_item_id == SourceItem.id)).where(SourceItem.organization_id == organization_id, SourceItem.connector_id == connector_id, SourceItemScopeMembership.connector_scope_id == scope_id)
        if active_memberships_only: statement = statement.where(SourceItemScopeMembership.status == "active", SourceItemScopeMembership.removed_at.is_(None))
        if source_status is not None: statement = statement.where(SourceItem.status == _require_choice("source_status", source_status, SOURCE_STATUSES))
        return self._page(statement.distinct(), limit, cursor)

    def update_provider_state(self, organization_id: UUID, connector_id: UUID, source_item_id: UUID, *, source_metadata: dict[str, object], metadata_schema_version: int, last_seen_at: datetime, source_checksum: str | None = None, source_version: str | None = None, size_bytes: int | None = None, source_modified_at: datetime | None = None) -> SourceItem | None:
        _require_json_object("source_metadata", source_metadata); _require_positive_integer("metadata_schema_version", metadata_schema_version); _require_aware("last_seen_at", last_seen_at)
        if size_bytes is not None and (isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0): raise InvalidConnectorRepositoryRequest("size_bytes must be nonnegative")
        if source_modified_at is not None: _require_aware("source_modified_at", source_modified_at)
        return self._update(organization_id, connector_id, source_item_id, source_metadata=source_metadata, metadata_schema_version=metadata_schema_version, last_seen_at=last_seen_at, source_checksum=source_checksum, source_version=source_version, size_bytes=size_bytes, source_modified_at=source_modified_at)

    def set_lifecycle(self, organization_id: UUID, connector_id: UUID, source_item_id: UUID, status: str, *, deleted_at: datetime | None = None) -> SourceItem | None:
        normalized = _require_choice("status", status, SOURCE_STATUSES)
        if deleted_at is not None: _require_aware("deleted_at", deleted_at)
        if (normalized == "deleted") != (deleted_at is not None): raise InvalidConnectorRepositoryRequest("deleted status and deleted_at must be consistent")
        return self._update(organization_id, connector_id, source_item_id, status=normalized, deleted_at=deleted_at)

    def add_membership(self, organization_id: UUID, connector_id: UUID, membership: SourceItemScopeMembership) -> SourceItemScopeMembership:
        _validate_membership(organization_id, connector_id, membership); self._session.add(membership); self._flush("source membership could not be created"); return membership

    def get_membership(self, organization_id: UUID, connector_id: UUID, scope_id: UUID, source_item_id: UUID) -> SourceItemScopeMembership | None:
        return self._one(self._membership_query(organization_id, connector_id, scope_id, source_item_id))

    def lock_membership(self, organization_id: UUID, connector_id: UUID, scope_id: UUID, source_item_id: UUID) -> SourceItemScopeMembership | None:
        return self._one(self._membership_query(organization_id, connector_id, scope_id, source_item_id).with_for_update())

    def reactivate_membership(self, organization_id: UUID, connector_id: UUID, scope_id: UUID, source_item_id: UUID, seen_at: datetime) -> SourceItemScopeMembership | None:
        _require_aware("seen_at", seen_at); return self._membership_update(organization_id, connector_id, scope_id, source_item_id, status="active", removed_at=None, last_seen_at=seen_at)

    def remove_membership(self, organization_id: UUID, connector_id: UUID, scope_id: UUID, source_item_id: UUID, removed_at: datetime) -> SourceItemScopeMembership | None:
        _require_aware("removed_at", removed_at); return self._membership_update(organization_id, connector_id, scope_id, source_item_id, status="removed", removed_at=removed_at, last_seen_at=removed_at)

    def list_active_memberships_before(self, organization_id: UUID, connector_id: UUID, scope_id: UUID, cutoff: datetime, *, limit: int = 100, cursor: MembershipReconciliationCursor | None = None) -> MembershipReconciliationPage:
        _require_uuid("organization_id", organization_id); _require_uuid("connector_id", connector_id); _require_uuid("scope_id", scope_id); _require_aware("cutoff", cutoff); _require_limit(limit); _validate_membership_cursor(cursor)
        statement = select(SourceItemScopeMembership).where(SourceItemScopeMembership.organization_id == organization_id, SourceItemScopeMembership.connector_id == connector_id, SourceItemScopeMembership.connector_scope_id == scope_id, SourceItemScopeMembership.status == "active", SourceItemScopeMembership.last_seen_at < cutoff)
        if cursor is not None: statement = statement.where(or_(SourceItemScopeMembership.last_seen_at > cursor.last_seen_at, and_(SourceItemScopeMembership.last_seen_at == cursor.last_seen_at, SourceItemScopeMembership.id > cursor.membership_id)))
        rows = self._all(statement.order_by(SourceItemScopeMembership.last_seen_at, SourceItemScopeMembership.id).limit(limit + 1)); items = tuple(rows[:limit]); more = len(rows) > limit; next_cursor = MembershipReconciliationCursor(items[-1].last_seen_at, items[-1].id) if more and items else None; return MembershipReconciliationPage(items, limit, more, next_cursor)

    def list_active_github_memberships_before(
        self,
        organization_id: UUID,
        connector_id: UUID,
        scope_id: UUID,
        repository_id: int,
        cutoff: datetime,
        *,
        limit: int = 100,
        cursor: MembershipReconciliationCursor | None = None,
    ) -> MembershipReconciliationPage:
        """Page stale GitHub memberships inside one exact repository scope."""
        _require_uuid("organization_id", organization_id)
        _require_uuid("connector_id", connector_id)
        _require_uuid("scope_id", scope_id)
        _require_aware("cutoff", cutoff)
        if (
            isinstance(repository_id, bool)
            or not isinstance(repository_id, int)
            or repository_id < 1
        ):
            raise InvalidConnectorRepositoryRequest("repository_id must be positive")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_MEMBERSHIP_RECONCILIATION_PAGE_LIMIT:
            raise InvalidConnectorRepositoryRequest(
                f"limit must be between 1 and {MAX_MEMBERSHIP_RECONCILIATION_PAGE_LIMIT}"
            )
        _validate_membership_cursor(cursor)
        prefix = f"github:repository:{repository_id}:path:"
        statement = (
            select(SourceItemScopeMembership)
            .join(
                SourceItem,
                and_(
                    SourceItem.organization_id == SourceItemScopeMembership.organization_id,
                    SourceItem.connector_id == SourceItemScopeMembership.connector_id,
                    SourceItem.id == SourceItemScopeMembership.source_item_id,
                ),
            )
            .join(
                DocumentVersion,
                and_(
                    DocumentVersion.organization_id == SourceItem.organization_id,
                    DocumentVersion.source_item_id == SourceItem.id,
                    DocumentVersion.is_current.is_(True),
                    DocumentVersion.lifecycle == "available",
                ),
            )
            .join(
                DocumentVersionDocument,
                and_(
                    DocumentVersionDocument.organization_id == DocumentVersion.organization_id,
                    DocumentVersionDocument.document_version_id == DocumentVersion.id,
                ),
            )
            .join(
                Document,
                and_(
                    Document.organization_id == DocumentVersionDocument.organization_id,
                    Document.id == DocumentVersionDocument.document_id,
                    Document.deleted_at.is_(None),
                ),
            )
            .where(
                SourceItemScopeMembership.organization_id == organization_id,
                SourceItemScopeMembership.connector_id == connector_id,
                SourceItemScopeMembership.connector_scope_id == scope_id,
                SourceItemScopeMembership.status == "active",
                SourceItemScopeMembership.removed_at.is_(None),
                SourceItemScopeMembership.last_seen_at < cutoff,
                SourceItem.organization_id == organization_id,
                SourceItem.connector_id == connector_id,
                SourceItem.source_item_type == "file",
                SourceItem.status.in_(("active", "unavailable")),
                SourceItem.last_seen_at < cutoff,
                SourceItem.source_item_key.startswith(prefix),
                SourceItem.source_metadata["provider"].astext == "github",
                SourceItem.source_metadata["repository_id"].astext == str(repository_id),
            )
        )
        if cursor is not None:
            statement = statement.where(
                or_(
                    SourceItemScopeMembership.last_seen_at > cursor.last_seen_at,
                    and_(
                        SourceItemScopeMembership.last_seen_at == cursor.last_seen_at,
                        SourceItemScopeMembership.id > cursor.membership_id,
                    ),
                )
            )
        rows = self._all(
            statement.order_by(
                SourceItemScopeMembership.last_seen_at,
                SourceItemScopeMembership.id,
            ).limit(limit + 1)
        )
        items = tuple(rows[:limit])
        has_more = len(rows) > limit
        next_cursor = (
            MembershipReconciliationCursor(items[-1].last_seen_at, items[-1].id)
            if has_more and items
            else None
        )
        return MembershipReconciliationPage(items, limit, has_more, next_cursor)

    def has_active_membership(self, organization_id: UUID, connector_id: UUID, source_item_id: UUID) -> bool:
        _require_uuid("organization_id", organization_id); _require_uuid("connector_id", connector_id); _require_uuid("source_item_id", source_item_id)
        statement = select(SourceItemScopeMembership.id).where(SourceItemScopeMembership.organization_id == organization_id, SourceItemScopeMembership.connector_id == connector_id, SourceItemScopeMembership.source_item_id == source_item_id, SourceItemScopeMembership.status == "active").limit(1)
        return self._one(statement) is not None

    def _item_query(self, org, connector):
        _require_uuid("organization_id", org); _require_uuid("connector_id", connector); return select(SourceItem).where(SourceItem.organization_id == org, SourceItem.connector_id == connector)
    def _membership_query(self, org, connector, scope, item):
        _require_uuid("organization_id", org); _require_uuid("connector_id", connector); _require_uuid("scope_id", scope); _require_uuid("source_item_id", item); return select(SourceItemScopeMembership).where(SourceItemScopeMembership.organization_id == org, SourceItemScopeMembership.connector_id == connector, SourceItemScopeMembership.connector_scope_id == scope, SourceItemScopeMembership.source_item_id == item)
    def _page(self, statement, limit, cursor):
        if cursor is not None: statement = statement.where(or_(SourceItem.created_at > cursor.created_at, and_(SourceItem.created_at == cursor.created_at, SourceItem.id > cursor.source_item_id)))
        rows = self._all(statement.order_by(SourceItem.created_at, SourceItem.id).limit(limit + 1)); items=tuple(rows[:limit]); more=len(rows)>limit; next_cursor=SourceItemPageCursor(items[-1].created_at,items[-1].id) if more and items else None; return SourceItemPage(items,limit,more,next_cursor)
    def _update(self, org, connector, item, **values):
        _require_uuid("organization_id",org); _require_uuid("connector_id",connector); _require_uuid("source_item_id",item); return self._execute_update(update(SourceItem).where(SourceItem.organization_id==org,SourceItem.connector_id==connector,SourceItem.id==item).values(**values).returning(SourceItem))
    def _membership_update(self, org, connector, scope, item, **values): return self._execute_update(update(SourceItemScopeMembership).where(SourceItemScopeMembership.organization_id==org,SourceItemScopeMembership.connector_id==connector,SourceItemScopeMembership.connector_scope_id==scope,SourceItemScopeMembership.source_item_id==item).values(**values).returning(SourceItemScopeMembership))
    def _one(self, statement):
        try: return self._session.execute(statement).scalar_one_or_none()
        except SQLAlchemyError as exc: raise ConnectorRepositoryPersistenceError("source item query failed") from exc
    def _all(self, statement):
        try: return list(self._session.execute(statement).scalars().all())
        except SQLAlchemyError as exc: raise ConnectorRepositoryPersistenceError("source item query failed") from exc
    def _execute_update(self, statement):
        try: return self._session.execute(statement).scalar_one_or_none()
        except SQLAlchemyError as exc: raise ConnectorRepositoryPersistenceError("source item could not be persisted") from exc
    def _flush(self, message):
        try: self._session.flush()
        except IntegrityError as exc: raise ConnectorRepositoryConflict(message) from exc
        except SQLAlchemyError as exc: raise ConnectorRepositoryPersistenceError(message) from exc


def _require_key(value):
    if not isinstance(value,str) or not value.strip(): raise InvalidConnectorRepositoryRequest("source_item_key must be nonblank")
    return value

def _validate_cursor(cursor):
    if cursor is None:return
    if not isinstance(cursor,SourceItemPageCursor):raise InvalidConnectorRepositoryRequest("cursor is invalid")
    _require_aware("cursor.created_at",cursor.created_at);_require_uuid("cursor.source_item_id",cursor.source_item_id)
def _validate_membership_cursor(cursor):
    if cursor is None:return
    if not isinstance(cursor,MembershipReconciliationCursor):raise InvalidConnectorRepositoryRequest("cursor is invalid")
    _require_aware("cursor.last_seen_at",cursor.last_seen_at);_require_uuid("cursor.membership_id",cursor.membership_id)
def _validate_item(org,connector,item):
    if not isinstance(item,SourceItem) or item.organization_id!=org or item.connector_id!=connector:raise InvalidConnectorRepositoryRequest("source item context is invalid")
    _require_key(item.source_item_key);_require_code("source_item_type",item.source_item_type);_require_choice("status",item.status,SOURCE_STATUSES);_require_json_object("source_metadata",item.source_metadata);_require_positive_integer("metadata_schema_version",item.metadata_schema_version);_require_aware("first_seen_at",item.first_seen_at);_require_aware("last_seen_at",item.last_seen_at)
    if item.size_bytes is not None and (isinstance(item.size_bytes,bool) or item.size_bytes<0):raise InvalidConnectorRepositoryRequest("size_bytes must be nonnegative")
def _validate_membership(org,connector,membership):
    if not isinstance(membership,SourceItemScopeMembership) or membership.organization_id!=org or membership.connector_id!=connector:raise InvalidConnectorRepositoryRequest("membership context is invalid")
    _require_uuid("scope_id",membership.connector_scope_id);_require_uuid("source_item_id",membership.source_item_id);_require_choice("status",membership.status,MEMBERSHIP_STATUSES);_require_aware("first_discovered_at",membership.first_discovered_at);_require_aware("last_seen_at",membership.last_seen_at)
