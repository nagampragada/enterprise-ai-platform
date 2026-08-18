"""Authenticated organization-administrator connector management routes."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from starlette.responses import Response

from app.api.v1.connectors.schemas import (
    ConnectorCapabilitiesResponse,
    ConnectorPageResponse,
    ConnectorResponse,
    ConnectorScopePageResponse,
    ConnectorScopeResponse,
    CreateConnectorRequest,
    CreateConnectorScopeRequest,
    EnqueueSyncJobRequest,
    EnqueueSyncJobResponse,
    SyncJobPageResponse,
    SyncJobResponse,
    decode_cursor,
    encode_cursor,
)
from app.dependencies import (
    ConnectorAdministrator,
    get_connector_administrator,
    get_connector_management_service,
    get_db_session,
)
from application.services.connector_management_service import (
    ConnectorManagementConflict,
    ConnectorManagementNotFound,
    ConnectorManagementPersistenceError,
    ConnectorManagementService,
    InvalidConnectorManagementRequest,
)
from infrastructure.repositories.connector_repository import (
    ConnectorPageCursor,
    ConnectorRepositoryConflict,
    ConnectorRepositoryPersistenceError,
    InvalidConnectorRepositoryRequest,
)
from infrastructure.repositories.connector_scope_repository import ConnectorScopePageCursor
from infrastructure.repositories.connector_sync_job_repository import (
    InvalidSyncJobRequest,
    SyncJobConflict,
    SyncJobPageCursor,
    SyncJobPersistenceError,
)

class SafeConnectorValidationRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Awaitable[Response]]:
        original = super().get_route_handler()

        async def safe_handler(request: Request) -> Response:
            try:
                return await original(request)
            except RequestValidationError as exc:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail="Connector request is invalid",
                ) from exc

        return safe_handler


connectors_router = APIRouter(
    prefix="/connectors", tags=["connectors"], route_class=SafeConnectorValidationRoute
)


@connectors_router.post("", response_model=ConnectorResponse, status_code=status.HTTP_201_CREATED)
def create_connector(
    payload: CreateConnectorRequest,
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorManagementService = Depends(get_connector_management_service),
    db_session: Session = Depends(get_db_session),
) -> ConnectorResponse:
    try:
        connector = service.create_local_folder_connector(
            administrator.organization_id,
            administrator.user_id,
            display_name=payload.display_name,
            slug=payload.slug,
        )
        db_session.commit()
        return _connector_response(connector)
    except Exception as exc:
        db_session.rollback()
        _raise_http(exc)


@connectors_router.get("", response_model=ConnectorPageResponse)
def list_connectors(
    limit: int = Query(default=20, ge=1, le=100),
    cursor: str | None = Query(default=None, max_length=512),
    status_filter: Literal[
        "draft", "validating", "active", "degraded", "auth_failed", "paused", "archived"
    ] | None = Query(default=None, alias="status"),
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorManagementService = Depends(get_connector_management_service),
) -> ConnectorPageResponse:
    try:
        decoded = decode_cursor(cursor, "connector")
        page_cursor = ConnectorPageCursor(*decoded) if decoded else None
        page = service.list_connectors(
            administrator.organization_id,
            limit=limit,
            cursor=page_cursor,
            status=status_filter,
        )
        return ConnectorPageResponse(
            items=tuple(_connector_response(item) for item in page.items),
            limit=page.limit,
            has_more=page.has_more,
            next_cursor=(
                encode_cursor("connector", page.next_cursor.created_at, page.next_cursor.connector_id)
                if page.next_cursor else None
            ),
        )
    except Exception as exc:
        _raise_http(exc)


@connectors_router.get("/{connector_id}", response_model=ConnectorResponse)
def get_connector(
    connector_id: UUID,
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorManagementService = Depends(get_connector_management_service),
) -> ConnectorResponse:
    try:
        return _connector_response(service.get_connector(administrator.organization_id, connector_id))
    except Exception as exc:
        _raise_http(exc)


@connectors_router.post(
    "/{connector_id}/scopes",
    response_model=ConnectorScopeResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_scope(
    connector_id: UUID,
    payload: CreateConnectorScopeRequest,
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorManagementService = Depends(get_connector_management_service),
    db_session: Session = Depends(get_db_session),
) -> ConnectorScopeResponse:
    try:
        scope = service.create_local_folder_scope(
            administrator.organization_id,
            administrator.user_id,
            connector_id,
            knowledge_space_id=payload.knowledge_space_id,
            display_name=payload.display_name,
            slug=payload.slug,
            root_path=payload.configuration.root_path,
            follow_symlinks=payload.configuration.follow_symlinks,
        )
        db_session.commit()
        return _scope_response(scope)
    except Exception as exc:
        db_session.rollback()
        _raise_http(exc)


@connectors_router.get("/{connector_id}/scopes", response_model=ConnectorScopePageResponse)
def list_scopes(
    connector_id: UUID,
    limit: int = Query(default=20, ge=1, le=100),
    cursor: str | None = Query(default=None, max_length=512),
    status_filter: Literal["draft", "validating", "active", "invalid", "paused", "removed"] | None = Query(default=None, alias="status"),
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorManagementService = Depends(get_connector_management_service),
) -> ConnectorScopePageResponse:
    try:
        decoded = decode_cursor(cursor, "scope")
        page_cursor = ConnectorScopePageCursor(*decoded) if decoded else None
        page = service.list_scopes(
            administrator.organization_id,
            connector_id,
            limit=limit,
            cursor=page_cursor,
            status=status_filter,
        )
        return ConnectorScopePageResponse(
            items=tuple(_scope_response(item) for item in page.items),
            limit=page.limit,
            has_more=page.has_more,
            next_cursor=(
                encode_cursor("scope", page.next_cursor.created_at, page.next_cursor.scope_id)
                if page.next_cursor else None
            ),
        )
    except Exception as exc:
        _raise_http(exc)


@connectors_router.post(
    "/{connector_id}/scopes/{scope_id}/sync-jobs",
    response_model=EnqueueSyncJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def enqueue_sync_job(
    connector_id: UUID,
    scope_id: UUID,
    payload: EnqueueSyncJobRequest | None = Body(default=None),
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorManagementService = Depends(get_connector_management_service),
    db_session: Session = Depends(get_db_session),
) -> EnqueueSyncJobResponse:
    del payload
    try:
        result, job = service.enqueue_sync_job(
            administrator.organization_id,
            administrator.user_id,
            connector_id,
            scope_id,
        )
        db_session.commit()
        return EnqueueSyncJobResponse(**_job_values(job), coalesced=result.coalesced)
    except Exception as exc:
        db_session.rollback()
        _raise_http(exc)


@connectors_router.get(
    "/{connector_id}/scopes/{scope_id}/sync-jobs",
    response_model=SyncJobPageResponse,
)
def list_sync_jobs(
    connector_id: UUID,
    scope_id: UUID,
    limit: int = Query(default=20, ge=1, le=100),
    cursor: str | None = Query(default=None, max_length=512),
    status_filter: Literal["queued", "running", "retry_wait", "succeeded", "failed", "cancelled"] | None = Query(default=None, alias="status"),
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorManagementService = Depends(get_connector_management_service),
) -> SyncJobPageResponse:
    try:
        decoded = decode_cursor(cursor, "job")
        page_cursor = SyncJobPageCursor(*decoded) if decoded else None
        page = service.list_sync_jobs(
            administrator.organization_id,
            connector_id,
            scope_id,
            limit=limit,
            cursor=page_cursor,
            status=status_filter,
        )
        return SyncJobPageResponse(
            items=tuple(SyncJobResponse(**_job_values(item)) for item in page.items),
            limit=page.limit,
            has_more=page.has_more,
            next_cursor=(
                encode_cursor("job", page.next_cursor.created_at, page.next_cursor.job_id)
                if page.next_cursor else None
            ),
        )
    except Exception as exc:
        _raise_http(exc)


@connectors_router.get(
    "/{connector_id}/scopes/{scope_id}/sync-jobs/{job_id}",
    response_model=SyncJobResponse,
)
def get_sync_job(
    connector_id: UUID,
    scope_id: UUID,
    job_id: UUID,
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorManagementService = Depends(get_connector_management_service),
) -> SyncJobResponse:
    try:
        return SyncJobResponse(
            **_job_values(
                service.get_sync_job(
                    administrator.organization_id, connector_id, scope_id, job_id
                )
            )
        )
    except Exception as exc:
        _raise_http(exc)


def _connector_response(row) -> ConnectorResponse:
    return ConnectorResponse(
        connector_id=row.id,
        connector_type=row.connector_type,
        display_name=row.display_name,
        slug=row.slug,
        status=row.status,
        acl_support=row.acl_support,
        capabilities=ConnectorCapabilitiesResponse(**row.capabilities),
        created_at=row.created_at,
        updated_at=row.updated_at,
        last_validated_at=row.last_validated_at,
        archived_at=row.archived_at,
    )


def _scope_response(row) -> ConnectorScopeResponse:
    return ConnectorScopeResponse(
        scope_id=row.id,
        connector_id=row.connector_id,
        knowledge_space_id=row.knowledge_space_id,
        display_name=row.display_name,
        slug=row.slug,
        scope_type=row.scope_type,
        access_mode=row.access_mode,
        status=row.status,
        created_at=row.created_at,
        updated_at=row.updated_at,
        last_validated_at=row.last_validated_at,
        removed_at=row.removed_at,
    )


def _job_values(row) -> dict[str, object]:
    return {
        "job_id": row.job_id,
        "connector_id": row.connector_id,
        "scope_id": row.connector_scope_id,
        "mode": row.mode,
        "trigger_type": row.trigger_type,
        "status": row.status,
        "attempt_count": row.attempt_count,
        "max_attempts": row.max_attempts,
        "next_attempt_at": row.next_attempt_at,
        "cancellation_requested": row.cancellation_requested,
        "completed_at": row.completed_at,
        "last_error_category": row.last_error_category,
        "last_error_code": row.last_error_code,
        "created_at": row.created_at,
    }


def _raise_http(exc: Exception) -> None:
    if isinstance(exc, HTTPException):
        raise exc
    if isinstance(exc, ConnectorManagementNotFound):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Resource not found") from exc
    if isinstance(exc, (ConnectorRepositoryConflict, SyncJobConflict)):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Resource conflict") from exc
    if isinstance(exc, ConnectorManagementConflict):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Resource state conflict") from exc
    if isinstance(exc, InvalidConnectorManagementRequest):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Connector request is invalid",
        ) from exc
    if isinstance(exc, (InvalidConnectorRepositoryRequest, InvalidSyncJobRequest, ValueError)):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Connector request is invalid") from exc
    if isinstance(
        exc,
        (
            ConnectorManagementPersistenceError,
            ConnectorRepositoryPersistenceError,
            SyncJobPersistenceError,
            SQLAlchemyError,
        ),
    ):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error") from exc
    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error") from exc