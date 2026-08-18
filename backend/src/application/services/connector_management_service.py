"""Tenant-safe connector administration orchestration without transaction ownership."""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from domain.connectors.capabilities import ConnectorCapabilities
from infrastructure.db.models import Connector, ConnectorScope, KnowledgeSpace
from infrastructure.repositories.connector_repository import (
    ConnectorPage,
    ConnectorPageCursor,
    ConnectorRepository,
    InvalidConnectorRepositoryRequest,
)
from infrastructure.repositories.connector_scope_repository import (
    ConnectorScopePage,
    ConnectorScopePageCursor,
    ConnectorScopeRepository,
)
from infrastructure.repositories.connector_sync_job_repository import (
    ConnectorSyncJobRepository,
    EnqueueResult,
    SyncJobHistoryItem,
    SyncJobPage,
    SyncJobPageCursor,
)

LOCAL_FOLDER_CAPABILITIES = ConnectorCapabilities(
    supports_incremental_sync=False,
    supports_permissions=False,
    supports_folders=True,
    supports_deletions=True,
    supports_version_history=False,
    supports_webhooks=False,
    supports_content_download=True,
)


class InvalidConnectorManagementRequest(ValueError):
    """Raised when connector-management input or lifecycle state is invalid."""


class ConnectorManagementNotFound(RuntimeError):
    """Raised when a tenant-scoped management resource is unavailable."""


class ConnectorManagementConflict(RuntimeError):
    """Raised when persisted lifecycle or capability state blocks an operation."""


class ConnectorManagementPersistenceError(RuntimeError):
    """Raised when management persistence cannot complete safely."""


class ConnectorManagementService:
    def __init__(
        self,
        session: Session,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._session = session
        self._connectors = ConnectorRepository(session)
        self._scopes = ConnectorScopeRepository(session)
        self._jobs = ConnectorSyncJobRepository(session)
        self._clock = clock

    def create_local_folder_connector(
        self,
        organization_id: UUID,
        creator_user_id: UUID,
        *,
        display_name: str,
        slug: str,
    ) -> Connector:
        connector = Connector(
            id=uuid4(),
            organization_id=organization_id,
            connector_type="local_folder",
            display_name=display_name,
            slug=slug,
            status="active",
            acl_support="none",
            capabilities=asdict(LOCAL_FOLDER_CAPABILITIES),
            safe_config={},
            config_schema_version=1,
            secret_reference=None,
            credential_status="not_configured",
            created_by_user_id=creator_user_id,
        )
        return self._connectors.add(organization_id, connector)

    def get_connector(self, organization_id: UUID, connector_id: UUID) -> Connector:
        connector = self._connectors.get_by_id(organization_id, connector_id)
        if connector is None:
            raise ConnectorManagementNotFound("connector was not found")
        return connector

    def list_connectors(
        self,
        organization_id: UUID,
        *,
        limit: int,
        cursor: ConnectorPageCursor | None = None,
        status: str | None = None,
    ) -> ConnectorPage:
        return self._connectors.list_page(
            organization_id,
            limit=limit,
            cursor=cursor,
            connector_type="local_folder",
            status=status,
        )

    def create_local_folder_scope(
        self,
        organization_id: UUID,
        creator_user_id: UUID,
        connector_id: UUID,
        *,
        knowledge_space_id: UUID,
        display_name: str,
        slug: str,
        root_path: str,
        follow_symlinks: bool,
    ) -> ConnectorScope:
        connector = self._connectors.lock_by_id(organization_id, connector_id)
        if connector is None:
            raise ConnectorManagementNotFound("connector was not found")
        if connector.connector_type != "local_folder":
            raise InvalidConnectorManagementRequest("connector type is not supported")
        if connector.status != "active":
            raise ConnectorManagementConflict("connector is not active")
        if connector.acl_support != "none":
            raise ConnectorManagementConflict("connector ACL support is incompatible")
        self._require_active_knowledge_space(organization_id, knowledge_space_id)
        normalized_root = _validate_local_folder_root(root_path)
        if follow_symlinks is not False:
            raise InvalidConnectorManagementRequest("symbolic link traversal is not supported")
        scope = ConnectorScope(
            id=uuid4(),
            organization_id=organization_id,
            connector_id=connector_id,
            knowledge_space_id=knowledge_space_id,
            display_name=display_name,
            slug=slug,
            scope_type="folder",
            external_scope_key=normalized_root,
            access_mode="platform_managed",
            status="active",
            safe_config={"follow_symlinks": False},
            config_schema_version=1,
            created_by_user_id=creator_user_id,
        )
        return self._scopes.add(organization_id, scope)

    def list_scopes(
        self,
        organization_id: UUID,
        connector_id: UUID,
        *,
        limit: int,
        cursor: ConnectorScopePageCursor | None = None,
        status: str | None = None,
    ) -> ConnectorScopePage:
        self.get_connector(organization_id, connector_id)
        return self._scopes.list_page(
            organization_id,
            connector_id=connector_id,
            limit=limit,
            cursor=cursor,
            status=status,
        )

    def enqueue_sync_job(
        self,
        organization_id: UUID,
        requester_user_id: UUID,
        connector_id: UUID,
        scope_id: UUID,
    ) -> tuple[EnqueueResult, SyncJobHistoryItem]:
        connector = self._connectors.lock_by_id(organization_id, connector_id)
        scope = self._scopes.lock_by_id(organization_id, scope_id)
        if connector is None or scope is None or scope.connector_id != connector_id:
            raise ConnectorManagementNotFound("connector scope was not found")
        if connector.connector_type != "local_folder":
            raise InvalidConnectorManagementRequest("connector type is not supported")
        if connector.status != "active" or scope.status != "active":
            raise ConnectorManagementConflict("connector scope is not active")
        result = self._jobs.enqueue_or_coalesce(
            organization_id,
            connector_id,
            scope_id,
            mode="incremental",
            trigger_type="manual",
            now=self._now(),
            requested_by_user_id=requester_user_id,
        )
        job = self._jobs.get(organization_id, result.job_id)
        if job is None:
            raise ConnectorManagementPersistenceError("synchronization job could not be read")
        return result, job

    def list_sync_jobs(
        self,
        organization_id: UUID,
        connector_id: UUID,
        scope_id: UUID,
        *,
        limit: int,
        cursor: SyncJobPageCursor | None = None,
        status: str | None = None,
    ) -> SyncJobPage:
        self._require_scope(organization_id, connector_id, scope_id)
        return self._jobs.list_history(
            organization_id,
            connector_id=connector_id,
            connector_scope_id=scope_id,
            limit=limit,
            cursor=cursor,
            status=status,
        )

    def get_sync_job(
        self,
        organization_id: UUID,
        connector_id: UUID,
        scope_id: UUID,
        job_id: UUID,
    ) -> SyncJobHistoryItem:
        self._require_scope(organization_id, connector_id, scope_id)
        job = self._jobs.get(organization_id, job_id)
        if job is None or job.connector_id != connector_id or job.connector_scope_id != scope_id:
            raise ConnectorManagementNotFound("synchronization job was not found")
        return job

    def _require_scope(
        self, organization_id: UUID, connector_id: UUID, scope_id: UUID
    ) -> ConnectorScope:
        connector = self._connectors.get_by_id(organization_id, connector_id)
        scope = self._scopes.get_by_id(organization_id, scope_id)
        if connector is None or scope is None or scope.connector_id != connector_id:
            raise ConnectorManagementNotFound("connector scope was not found")
        return scope

    def _require_active_knowledge_space(
        self, organization_id: UUID, knowledge_space_id: UUID
    ) -> None:
        try:
            found = self._session.execute(
                select(KnowledgeSpace.id).where(
                    KnowledgeSpace.organization_id == organization_id,
                    KnowledgeSpace.id == knowledge_space_id,
                    KnowledgeSpace.status == "active",
                )
            ).scalar_one_or_none()
        except SQLAlchemyError as exc:
            raise ConnectorManagementPersistenceError(
                "knowledge space could not be verified"
            ) from exc
        if found is None:
            raise ConnectorManagementNotFound("knowledge space was not found")

    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise InvalidConnectorManagementRequest("clock must return timezone-aware time")
        return value


def _validate_local_folder_root(value: object) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 1024:
        raise InvalidConnectorManagementRequest("Local Folder root is invalid")
    if "\x00" in value:
        raise InvalidConnectorManagementRequest("Local Folder root is invalid")
    root = Path(value)
    if not root.is_absolute() or ".." in root.parts:
        raise InvalidConnectorManagementRequest("Local Folder root must be an absolute path")
    return str(root)