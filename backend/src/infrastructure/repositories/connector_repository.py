"""Tenant-safe connector persistence using caller-owned SQLAlchemy sessions."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import and_, or_, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from infrastructure.db.models import Connector

MAX_CONNECTOR_PAGE_LIMIT = 100
CONNECTOR_STATUSES = frozenset({"draft", "validating", "active", "degraded", "auth_failed", "paused", "archived"})
ACL_SUPPORT_VALUES = frozenset({"none", "partial", "complete"})
_CODE = re.compile(r"^[a-z][a-z0-9_]*$")
_SLUG = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


class InvalidConnectorRepositoryRequest(ValueError):
    """Raised before SQL execution when connector repository input is malformed."""


class ConnectorRepositoryConflict(RuntimeError):
    """Raised when connector persistence conflicts with committed constraints."""


class ConnectorRepositoryPersistenceError(RuntimeError):
    """Raised when connector persistence cannot be completed safely."""


@dataclass(frozen=True)
class ConnectorPageCursor:
    created_at: datetime
    connector_id: UUID


@dataclass(frozen=True)
class ConnectorPage:
    items: tuple[Connector, ...]
    limit: int
    has_more: bool
    next_cursor: ConnectorPageCursor | None


class ConnectorRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add(self, organization_id: UUID, connector: Connector) -> Connector:
        _require_uuid("organization_id", organization_id)
        _validate_connector(organization_id, connector)
        self._session.add(connector)
        self._flush("connector could not be created")
        return connector

    def get_by_id(self, organization_id: UUID, connector_id: UUID) -> Connector | None:
        _require_uuid("organization_id", organization_id); _require_uuid("connector_id", connector_id)
        return self._one(select(Connector).where(Connector.organization_id == organization_id, Connector.id == connector_id))

    def get_by_slug(self, organization_id: UUID, slug: str) -> Connector | None:
        _require_uuid("organization_id", organization_id); normalized = _require_slug("slug", slug)
        return self._one(select(Connector).where(Connector.organization_id == organization_id, Connector.slug == normalized))

    def lock_by_id(self, organization_id: UUID, connector_id: UUID) -> Connector | None:
        _require_uuid("organization_id", organization_id); _require_uuid("connector_id", connector_id)
        statement = select(Connector).where(Connector.organization_id == organization_id, Connector.id == connector_id).with_for_update()
        return self._one(statement)

    def list_page(
        self,
        organization_id: UUID,
        *,
        limit: int = MAX_CONNECTOR_PAGE_LIMIT,
        cursor: ConnectorPageCursor | None = None,
        connector_type: str | None = None,
        status: str | None = None,
    ) -> ConnectorPage:
        _require_uuid("organization_id", organization_id); _require_limit(limit); _validate_cursor(cursor)
        statement = select(Connector).where(Connector.organization_id == organization_id)
        if connector_type is not None:
            statement = statement.where(Connector.connector_type == _require_code("connector_type", connector_type))
        if status is not None:
            statement = statement.where(Connector.status == _require_choice("status", status, CONNECTOR_STATUSES))
        if cursor is not None:
            statement = statement.where(or_(Connector.created_at > cursor.created_at, and_(Connector.created_at == cursor.created_at, Connector.id > cursor.connector_id)))
        statement = statement.order_by(Connector.created_at.asc(), Connector.id.asc()).limit(limit + 1)
        rows = self._all(statement); items = tuple(rows[:limit]); has_more = len(rows) > limit
        next_cursor = ConnectorPageCursor(items[-1].created_at, items[-1].id) if has_more and items else None
        return ConnectorPage(items, limit, has_more, next_cursor)

    def update_safe_configuration(self, organization_id: UUID, connector_id: UUID, safe_config: dict[str, object], config_schema_version: int) -> Connector | None:
        _require_json_object("safe_config", safe_config); _require_positive_integer("config_schema_version", config_schema_version)
        return self._update(organization_id, connector_id, safe_config=safe_config, config_schema_version=config_schema_version)

    def update_validation(self, organization_id: UUID, connector_id: UUID, *, status: str, validated_at: datetime) -> Connector | None:
        _require_choice("status", status, CONNECTOR_STATUSES); _require_aware("validated_at", validated_at)
        return self._update(organization_id, connector_id, status=status, last_validated_at=validated_at)

    def set_status(self, organization_id: UUID, connector_id: UUID, status: str, *, archived_at: datetime | None = None) -> Connector | None:
        normalized = _require_choice("status", status, CONNECTOR_STATUSES)
        if archived_at is not None: _require_aware("archived_at", archived_at)
        if (normalized == "archived") != (archived_at is not None): raise InvalidConnectorRepositoryRequest("archived status and archived_at must be consistent")
        return self._update(organization_id, connector_id, status=normalized, archived_at=archived_at)

    def _update(self, organization_id: UUID, connector_id: UUID, **values: object) -> Connector | None:
        _require_uuid("organization_id", organization_id); _require_uuid("connector_id", connector_id)
        statement = update(Connector).where(Connector.organization_id == organization_id, Connector.id == connector_id).values(**values).returning(Connector)
        try: return self._session.execute(statement).scalar_one_or_none()
        except SQLAlchemyError as exc: raise ConnectorRepositoryPersistenceError("connector could not be persisted") from exc

    def _one(self, statement):
        try: return self._session.execute(statement).scalar_one_or_none()
        except SQLAlchemyError as exc: raise ConnectorRepositoryPersistenceError("connector query failed") from exc

    def _all(self, statement):
        try: return list(self._session.execute(statement).scalars().all())
        except SQLAlchemyError as exc: raise ConnectorRepositoryPersistenceError("connector query failed") from exc

    def _flush(self, message: str) -> None:
        try: self._session.flush()
        except IntegrityError as exc: raise ConnectorRepositoryConflict(message) from exc
        except SQLAlchemyError as exc: raise ConnectorRepositoryPersistenceError(message) from exc


def _validate_connector(organization_id: UUID, connector: Connector) -> None:
    if not isinstance(connector, Connector) or connector.organization_id != organization_id: raise InvalidConnectorRepositoryRequest("connector tenant context is invalid")
    _require_code("connector_type", connector.connector_type); _require_slug("slug", connector.slug)
    if not isinstance(connector.display_name, str) or not connector.display_name.strip(): raise InvalidConnectorRepositoryRequest("display_name must be nonblank")
    _require_choice("status", connector.status, CONNECTOR_STATUSES); _require_choice("acl_support", connector.acl_support, ACL_SUPPORT_VALUES)
    _require_json_object("capabilities", connector.capabilities); _require_json_object("safe_config", connector.safe_config); _require_positive_integer("config_schema_version", connector.config_schema_version)
    if connector.created_by_user_id is not None: _require_uuid("created_by_user_id", connector.created_by_user_id)


def _require_uuid(name: str, value: object) -> UUID:
    if not isinstance(value, UUID): raise InvalidConnectorRepositoryRequest(f"{name} must be a UUID")
    return value

def _require_slug(name: str, value: object) -> str:
    if not isinstance(value, str) or not _SLUG.fullmatch(value): raise InvalidConnectorRepositoryRequest(f"{name} must be normalized kebab-case")
    return value

def _require_code(name: str, value: object) -> str:
    if not isinstance(value, str) or not _CODE.fullmatch(value): raise InvalidConnectorRepositoryRequest(f"{name} must be a normalized code")
    return value

def _require_choice(name: str, value: object, choices: frozenset[str]) -> str:
    if not isinstance(value, str) or value not in choices: raise InvalidConnectorRepositoryRequest(f"{name} is invalid")
    return value

def _require_limit(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > MAX_CONNECTOR_PAGE_LIMIT: raise InvalidConnectorRepositoryRequest(f"limit must be between 1 and {MAX_CONNECTOR_PAGE_LIMIT}")
    return value

def _require_positive_integer(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1: raise InvalidConnectorRepositoryRequest(f"{name} must be a positive integer")
    return value

def _require_aware(name: str, value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.tzinfo.utcoffset(value) is None: raise InvalidConnectorRepositoryRequest(f"{name} must be timezone-aware")
    return value

def _require_json_object(name: str, value: object) -> dict[str, object]:
    if not isinstance(value, dict): raise InvalidConnectorRepositoryRequest(f"{name} must be a JSON object")
    try: json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as exc: raise InvalidConnectorRepositoryRequest(f"{name} must be finite JSON data") from exc
    return value

def _validate_cursor(cursor: ConnectorPageCursor | None) -> None:
    if cursor is None: return
    if not isinstance(cursor, ConnectorPageCursor): raise InvalidConnectorRepositoryRequest("cursor is invalid")
    _require_aware("cursor.created_at", cursor.created_at); _require_uuid("cursor.connector_id", cursor.connector_id)
