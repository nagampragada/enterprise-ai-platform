"""Tenant-safe connector-scope persistence with bounded keyset pagination."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import and_, or_, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from infrastructure.db.models import ConnectorScope
from infrastructure.repositories.connector_repository import (
    ConnectorRepositoryConflict,
    ConnectorRepositoryPersistenceError,
    InvalidConnectorRepositoryRequest,
    _require_aware,
    _require_choice,
    _require_code,
    _require_json_object,
    _require_limit,
    _require_positive_integer,
    _require_slug,
    _require_uuid,
)

MAX_CONNECTOR_SCOPE_PAGE_LIMIT = 100
SCOPE_STATUSES = frozenset({"draft", "validating", "active", "invalid", "paused", "removed"})
ACCESS_MODES = frozenset({"platform_managed", "source_acl", "hybrid"})


@dataclass(frozen=True)
class ConnectorScopePageCursor:
    created_at: datetime
    scope_id: UUID


@dataclass(frozen=True)
class ConnectorScopePage:
    items: tuple[ConnectorScope, ...]
    limit: int
    has_more: bool
    next_cursor: ConnectorScopePageCursor | None


class ConnectorScopeRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, organization_id: UUID, scope: ConnectorScope) -> ConnectorScope:
        _require_uuid("organization_id", organization_id); _validate_scope(organization_id, scope)
        self._session.add(scope); self._flush("connector scope could not be created"); return scope

    def get_by_id(self, organization_id: UUID, scope_id: UUID) -> ConnectorScope | None:
        _require_uuid("organization_id", organization_id); _require_uuid("scope_id", scope_id)
        return self._one(select(ConnectorScope).where(ConnectorScope.organization_id == organization_id, ConnectorScope.id == scope_id))

    def get_by_connector_and_slug(self, organization_id: UUID, connector_id: UUID, slug: str) -> ConnectorScope | None:
        _require_uuid("organization_id", organization_id); _require_uuid("connector_id", connector_id); normalized = _require_slug("slug", slug)
        return self._one(select(ConnectorScope).where(ConnectorScope.organization_id == organization_id, ConnectorScope.connector_id == connector_id, ConnectorScope.slug == normalized))

    def lock_by_id(self, organization_id: UUID, scope_id: UUID) -> ConnectorScope | None:
        _require_uuid("organization_id", organization_id); _require_uuid("scope_id", scope_id)
        return self._one(select(ConnectorScope).where(ConnectorScope.organization_id == organization_id, ConnectorScope.id == scope_id).with_for_update())

    def list_page(
        self,
        organization_id: UUID,
        *,
        limit: int = MAX_CONNECTOR_SCOPE_PAGE_LIMIT,
        cursor: ConnectorScopePageCursor | None = None,
        connector_id: UUID | None = None,
        knowledge_space_id: UUID | None = None,
        access_mode: str | None = None,
        status: str | None = None,
    ) -> ConnectorScopePage:
        _require_uuid("organization_id", organization_id); _require_limit(limit); _validate_cursor(cursor)
        statement = select(ConnectorScope).where(ConnectorScope.organization_id == organization_id)
        if connector_id is not None: statement = statement.where(ConnectorScope.connector_id == _require_uuid("connector_id", connector_id))
        if knowledge_space_id is not None: statement = statement.where(ConnectorScope.knowledge_space_id == _require_uuid("knowledge_space_id", knowledge_space_id))
        if access_mode is not None: statement = statement.where(ConnectorScope.access_mode == _require_choice("access_mode", access_mode, ACCESS_MODES))
        if status is not None: statement = statement.where(ConnectorScope.status == _require_choice("status", status, SCOPE_STATUSES))
        if cursor is not None:
            statement = statement.where(or_(ConnectorScope.created_at > cursor.created_at, and_(ConnectorScope.created_at == cursor.created_at, ConnectorScope.id > cursor.scope_id)))
        statement = statement.order_by(ConnectorScope.created_at.asc(), ConnectorScope.id.asc()).limit(limit + 1)
        rows = self._all(statement); items = tuple(rows[:limit]); has_more = len(rows) > limit
        next_cursor = ConnectorScopePageCursor(items[-1].created_at, items[-1].id) if has_more and items else None
        return ConnectorScopePage(items, limit, has_more, next_cursor)

    def list_active_for_connector(self, organization_id: UUID, connector_id: UUID, *, limit: int, cursor: ConnectorScopePageCursor | None = None) -> ConnectorScopePage:
        return self.list_page(organization_id, connector_id=connector_id, status="active", limit=limit, cursor=cursor)

    def update_safe_configuration(self, organization_id: UUID, scope_id: UUID, safe_config: dict[str, object], config_schema_version: int) -> ConnectorScope | None:
        _require_json_object("safe_config", safe_config); _require_positive_integer("config_schema_version", config_schema_version)
        return self._update(organization_id, scope_id, safe_config=safe_config, config_schema_version=config_schema_version)

    def update_validation(self, organization_id: UUID, scope_id: UUID, *, status: str, validated_at: datetime) -> ConnectorScope | None:
        _require_choice("status", status, SCOPE_STATUSES); _require_aware("validated_at", validated_at)
        return self._update(organization_id, scope_id, status=status, last_validated_at=validated_at)

    def set_status(self, organization_id: UUID, scope_id: UUID, status: str, *, removed_at: datetime | None = None) -> ConnectorScope | None:
        normalized = _require_choice("status", status, SCOPE_STATUSES)
        if removed_at is not None: _require_aware("removed_at", removed_at)
        if (normalized == "removed") != (removed_at is not None): raise InvalidConnectorRepositoryRequest("removed status and removed_at must be consistent")
        return self._update(organization_id, scope_id, status=normalized, removed_at=removed_at)

    def _update(self, organization_id: UUID, scope_id: UUID, **values: object) -> ConnectorScope | None:
        _require_uuid("organization_id", organization_id); _require_uuid("scope_id", scope_id)
        statement = update(ConnectorScope).where(ConnectorScope.organization_id == organization_id, ConnectorScope.id == scope_id).values(**values).returning(ConnectorScope)
        try: return self._session.execute(statement).scalar_one_or_none()
        except SQLAlchemyError as exc: raise ConnectorRepositoryPersistenceError("connector scope could not be persisted") from exc

    def _one(self, statement):
        try: return self._session.execute(statement).scalar_one_or_none()
        except SQLAlchemyError as exc: raise ConnectorRepositoryPersistenceError("connector scope query failed") from exc

    def _all(self, statement):
        try: return list(self._session.execute(statement).scalars().all())
        except SQLAlchemyError as exc: raise ConnectorRepositoryPersistenceError("connector scope query failed") from exc

    def _flush(self, message: str) -> None:
        try: self._session.flush()
        except IntegrityError as exc: raise ConnectorRepositoryConflict(message) from exc
        except SQLAlchemyError as exc: raise ConnectorRepositoryPersistenceError(message) from exc


def _validate_scope(organization_id: UUID, scope: ConnectorScope) -> None:
    if not isinstance(scope, ConnectorScope) or scope.organization_id != organization_id: raise InvalidConnectorRepositoryRequest("connector scope tenant context is invalid")
    _require_uuid("connector_id", scope.connector_id); _require_uuid("knowledge_space_id", scope.knowledge_space_id)
    if scope.created_by_user_id is not None: _require_uuid("created_by_user_id", scope.created_by_user_id)
    if not isinstance(scope.display_name, str) or not scope.display_name.strip(): raise InvalidConnectorRepositoryRequest("display_name must be nonblank")
    _require_slug("slug", scope.slug); _require_code("scope_type", scope.scope_type)
    if not isinstance(scope.external_scope_key, str) or not scope.external_scope_key.strip(): raise InvalidConnectorRepositoryRequest("external_scope_key must be nonblank")
    _require_choice("access_mode", scope.access_mode, ACCESS_MODES); _require_choice("status", scope.status, SCOPE_STATUSES)
    _require_json_object("safe_config", scope.safe_config); _require_positive_integer("config_schema_version", scope.config_schema_version)


def _validate_cursor(cursor: ConnectorScopePageCursor | None) -> None:
    if cursor is None: return
    if not isinstance(cursor, ConnectorScopePageCursor): raise InvalidConnectorRepositoryRequest("cursor is invalid")
    _require_aware("cursor.created_at", cursor.created_at); _require_uuid("cursor.scope_id", cursor.scope_id)
