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
from starlette.responses import RedirectResponse, Response

from app.api.v1.connectors.schemas import (
    ConnectorCapabilitiesResponse,
    ConnectorPageResponse,
    ConnectorResponse,
    ConnectorScopePageResponse,
    ConnectorScopeResponse,
    CreateGitHubRepositoryScopeRequest,
    CreateConnectorRequest,
    CreateConnectorScopeRequest,
    EnqueueSyncJobRequest,
    EnqueueSyncJobResponse,
    GitHubInstallationCompletionResponse,
    GitHubInstallationInitiationResponse,
    GitHubInstallationStatusResponse,
    GitHubRepositoryPageResponse,
    GitHubRepositoryResponse,
    GitHubRepositoryScopePageResponse,
    GitHubRepositoryScopeResponse,
    PatchSyncScheduleRequest,
    PutSyncScheduleRequest,
    SyncScheduleResponse,
    SyncJobPageResponse,
    SyncJobResponse,
    decode_cursor,
    encode_cursor,
)
from app.dependencies import (
    ConnectorAdministrator,
    get_connector_administrator,
    get_connector_management_service,
    get_connector_sync_schedule_service,
    get_github_app_installation_service,
    get_github_repository_discovery_service,
    get_github_repository_scope_service,
    get_github_repository_selection_service,
    get_db_session,
)
from application.services.connector_management_service import (
    ConnectorManagementConflict,
    ConnectorManagementNotFound,
    ConnectorManagementPersistenceError,
    ConnectorManagementService,
    InvalidConnectorManagementRequest,
)
from application.services.connector_sync_schedule_service import (
    ConnectorSyncScheduleService,
    SyncScheduleNotFound,
    SyncScheduleResourceConflict,
    SyncScheduleView,
)
from application.services.github_app_installation_service import (
    GitHubAppInstallationService, GitHubInstallationConflict,
    GitHubInstallationNotFound, GitHubInstallationRejected,
)
from application.services.github_repository_discovery_service import (
    GitHubRepositoryDiscoveryConflict,
    GitHubRepositoryDiscoveryNotFound,
    GitHubRepositoryDiscoveryRejected,
    GitHubRepositoryDiscoveryService,
)
from application.services.github_repository_selection_service import (
    GitHubRepositoryScopeView,
    GitHubRepositorySelectionConflict,
    GitHubRepositorySelectionNotFound,
    GitHubRepositorySelectionPersistenceError,
    GitHubRepositorySelectionRejected,
    GitHubRepositorySelectionService,
)
from application.ports.github_app import GitHubProviderError, GitHubProviderRateLimitError
from application.ports.secret_store import SecretStoreError
from infrastructure.repositories.github_app_installation_repository import (
    GitHubInstallationConflict as GitHubBindingConflict,
    GitHubInstallationPersistenceError,
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
from infrastructure.repositories.connector_sync_schedule_repository import (
    InvalidSyncScheduleRequest,
    SyncScheduleConflict,
    SyncSchedulePersistenceError,
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


@connectors_router.post("/{connector_id}/github/installation", response_model=GitHubInstallationInitiationResponse)
def initiate_github_installation(connector_id:UUID,administrator:ConnectorAdministrator=Depends(get_connector_administrator),
    service:GitHubAppInstallationService=Depends(get_github_app_installation_service),db_session:Session=Depends(get_db_session))->GitHubInstallationInitiationResponse:
    try:
        result=service.initiate(administrator.organization_id,connector_id,administrator.user_id);db_session.commit()
        return GitHubInstallationInitiationResponse(installation_url=result.installation_url,
            expires_at=result.expires_at)
    except Exception as exc:db_session.rollback();_raise_http(exc)


@connectors_router.get("/github/setup", response_class=RedirectResponse, status_code=status.HTTP_303_SEE_OTHER)
def complete_github_setup(
    state_value: str = Query(alias="state", min_length=43, max_length=512, pattern=r"^\S+$"),
    installation_id: int = Query(gt=0, le=9_223_372_036_854_775_807),
    setup_action: Literal["install"] = Query(),
    service: GitHubAppInstallationService = Depends(get_github_app_installation_service),
    db_session: Session = Depends(get_db_session),
) -> RedirectResponse:
    try:
        result = service.complete_setup(
            state=state_value,
            installation_id=installation_id,
            setup_action=setup_action,
        )
        db_session.commit()
        return RedirectResponse(result.authorization_url, status_code=status.HTTP_303_SEE_OTHER)
    except Exception as exc:
        db_session.rollback()
        _raise_http(exc)


@connectors_router.get("/github/callback", response_model=GitHubInstallationCompletionResponse)
def complete_github_callback(
    state_value: str = Query(alias="state", min_length=43, max_length=512, pattern=r"^\S+$"),
    code: str = Query(min_length=1, max_length=1024, pattern=r"^\S+$"),
    service: GitHubAppInstallationService = Depends(get_github_app_installation_service),
    db_session: Session = Depends(get_db_session),
) -> GitHubInstallationCompletionResponse:
    try:
        service.complete_callback(state=state_value, code=code)
        db_session.commit()
        return GitHubInstallationCompletionResponse(status="connected")
    except Exception as exc:
        db_session.rollback()
        _raise_http(exc)


@connectors_router.get("/{connector_id}/github/installation",response_model=GitHubInstallationStatusResponse)
def get_github_installation(connector_id:UUID,administrator:ConnectorAdministrator=Depends(get_connector_administrator),
    service:GitHubAppInstallationService=Depends(get_github_app_installation_service))->GitHubInstallationStatusResponse:
    try:return GitHubInstallationStatusResponse(**service.status(administrator.organization_id,connector_id).__dict__)
    except Exception as exc:_raise_http(exc)


@connectors_router.delete("/{connector_id}/github/installation",response_model=GitHubInstallationStatusResponse)
def disconnect_github_installation(connector_id:UUID,administrator:ConnectorAdministrator=Depends(get_connector_administrator),
    service:GitHubAppInstallationService=Depends(get_github_app_installation_service),db_session:Session=Depends(get_db_session))->GitHubInstallationStatusResponse:
    try:
        result=service.disconnect(administrator.organization_id,connector_id);db_session.commit()
        return GitHubInstallationStatusResponse(**result.__dict__)
    except Exception as exc:db_session.rollback();_raise_http(exc)


@connectors_router.get(
    "/{connector_id}/github/repositories",
    response_model=GitHubRepositoryPageResponse,
)
def list_github_repositories(
    connector_id: UUID,
    request: Request,
    page: int = Query(default=1, ge=1, le=1_000),
    page_size: int = Query(default=50, ge=1, le=100),
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: GitHubRepositoryDiscoveryService = Depends(
        get_github_repository_discovery_service
    ),
    db_session: Session = Depends(get_db_session),
) -> GitHubRepositoryPageResponse:
    try:
        if set(request.query_params) - {"page", "page_size"}:
            raise GitHubRepositoryDiscoveryRejected(
                "GitHub repository discovery request is invalid"
            )
        context = service.prepare(administrator.organization_id, connector_id)
        db_session.rollback()
        result = service.discover(context, page=page, page_size=page_size)
        return GitHubRepositoryPageResponse(
            items=tuple(GitHubRepositoryResponse(**item.__dict__) for item in result.items),
            page=result.page,
            page_size=result.page_size,
            has_next=result.has_next,
            total_count=result.total_count,
        )
    except Exception as exc:
        db_session.rollback()
        _raise_http(exc)


@connectors_router.post(
    "/{connector_id}/github/repository-scopes",
    response_model=GitHubRepositoryScopeResponse,
)
def select_github_repository(
    connector_id: UUID,
    payload: CreateGitHubRepositoryScopeRequest,
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: GitHubRepositorySelectionService = Depends(
        get_github_repository_selection_service
    ),
    db_session: Session = Depends(get_db_session),
) -> GitHubRepositoryScopeResponse:
    try:
        context = service.prepare(
            administrator.organization_id,
            connector_id,
            payload.knowledge_space_id,
            payload.repository_id,
        )
        db_session.rollback()
        repository = service.verify(context)
        result = service.persist(context, repository, administrator.user_id)
        db_session.commit()
        return _github_repository_scope_response(result)
    except Exception as exc:
        db_session.rollback()
        _raise_http(exc)


@connectors_router.get(
    "/{connector_id}/github/repository-scopes",
    response_model=GitHubRepositoryScopePageResponse,
)
def list_github_repository_scopes(
    connector_id: UUID,
    limit: int = Query(default=20, ge=1, le=100),
    cursor: str | None = Query(default=None, max_length=512),
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: GitHubRepositorySelectionService = Depends(
        get_github_repository_scope_service
    ),
) -> GitHubRepositoryScopePageResponse:
    try:
        decoded = decode_cursor(cursor, "github_repository_scope")
        page_cursor = ConnectorScopePageCursor(*decoded) if decoded else None
        page = service.list(
            administrator.organization_id,
            connector_id,
            limit=limit,
            cursor=page_cursor,
        )
        return GitHubRepositoryScopePageResponse(
            items=tuple(_github_repository_scope_response(item) for item in page.items),
            limit=page.limit,
            has_more=page.has_more,
            next_cursor=(
                encode_cursor(
                    "github_repository_scope",
                    page.next_cursor.created_at,
                    page.next_cursor.scope_id,
                )
                if page.next_cursor
                else None
            ),
        )
    except Exception as exc:
        _raise_http(exc)


@connectors_router.delete(
    "/{connector_id}/github/repository-scopes/{scope_id}",
    response_model=GitHubRepositoryScopeResponse,
)
def deselect_github_repository(
    connector_id: UUID,
    scope_id: UUID,
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: GitHubRepositorySelectionService = Depends(
        get_github_repository_scope_service
    ),
    db_session: Session = Depends(get_db_session),
) -> GitHubRepositoryScopeResponse:
    try:
        result = service.deselect(
            administrator.organization_id, connector_id, scope_id
        )
        db_session.commit()
        return _github_repository_scope_response(result)
    except Exception as exc:
        db_session.rollback()
        _raise_http(exc)


@connectors_router.post("", response_model=ConnectorResponse, status_code=status.HTTP_201_CREATED)
def create_connector(
    payload: CreateConnectorRequest,
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorManagementService = Depends(get_connector_management_service),
    db_session: Session = Depends(get_db_session),
) -> ConnectorResponse:
    try:
        if payload.connector_type == "local_folder":
            connector = service.create_local_folder_connector(
                administrator.organization_id,
                administrator.user_id,
                display_name=payload.display_name,
                slug=payload.slug,
            )
        elif payload.connector_type == "github":
            connector = service.create_github_connector(
                administrator.organization_id,
                administrator.user_id,
                display_name=payload.display_name,
                slug=payload.slug,
            )
        else:
            raise InvalidConnectorManagementRequest("connector type is not supported")
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


@connectors_router.put(
    "/{connector_id}/scopes/{scope_id}/schedule",
    response_model=SyncScheduleResponse,
)
def put_sync_schedule(
    connector_id: UUID,
    scope_id: UUID,
    payload: PutSyncScheduleRequest,
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorSyncScheduleService = Depends(get_connector_sync_schedule_service),
    db_session: Session = Depends(get_db_session),
) -> SyncScheduleResponse:
    try:
        schedule = service.create_or_replace(
            administrator.organization_id,
            administrator.user_id,
            connector_id,
            scope_id,
            interval_seconds=payload.interval_seconds,
            first_run_at=payload.first_run_at,
        )
        db_session.commit()
        return _schedule_response(schedule)
    except Exception as exc:
        db_session.rollback()
        _raise_http(exc)


@connectors_router.get(
    "/{connector_id}/scopes/{scope_id}/schedule",
    response_model=SyncScheduleResponse,
)
def get_sync_schedule(
    connector_id: UUID,
    scope_id: UUID,
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorSyncScheduleService = Depends(get_connector_sync_schedule_service),
) -> SyncScheduleResponse:
    try:
        return _schedule_response(
            service.get(administrator.organization_id, connector_id, scope_id)
        )
    except Exception as exc:
        _raise_http(exc)


@connectors_router.patch(
    "/{connector_id}/scopes/{scope_id}/schedule",
    response_model=SyncScheduleResponse,
)
def patch_sync_schedule(
    connector_id: UUID,
    scope_id: UUID,
    payload: PatchSyncScheduleRequest,
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorSyncScheduleService = Depends(get_connector_sync_schedule_service),
    db_session: Session = Depends(get_db_session),
) -> SyncScheduleResponse:
    try:
        operation = service.pause if payload.action == "pause" else service.resume
        schedule = operation(administrator.organization_id, connector_id, scope_id)
        db_session.commit()
        return _schedule_response(schedule)
    except Exception as exc:
        db_session.rollback()
        _raise_http(exc)


@connectors_router.delete(
    "/{connector_id}/scopes/{scope_id}/schedule",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_sync_schedule(
    connector_id: UUID,
    scope_id: UUID,
    administrator: ConnectorAdministrator = Depends(get_connector_administrator),
    service: ConnectorSyncScheduleService = Depends(get_connector_sync_schedule_service),
    db_session: Session = Depends(get_db_session),
) -> Response:
    try:
        service.delete(administrator.organization_id, connector_id, scope_id)
        db_session.commit()
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as exc:
        db_session.rollback()
        _raise_http(exc)


def _connector_response(row) -> ConnectorResponse:
    capabilities = {
        **row.capabilities,
        "supports_repository_discovery": row.connector_type == "github",
        "supports_repository_selection": row.connector_type == "github",
    }
    return ConnectorResponse(
        connector_id=row.id,
        connector_type=row.connector_type,
        display_name=row.display_name,
        slug=row.slug,
        status=row.status,
        acl_support=row.acl_support,
        capabilities=ConnectorCapabilitiesResponse(**capabilities),
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


def _github_repository_scope_response(
    view: GitHubRepositoryScopeView,
) -> GitHubRepositoryScopeResponse:
    return GitHubRepositoryScopeResponse(**view.__dict__)


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


def _schedule_response(row: SyncScheduleView) -> SyncScheduleResponse:
    return SyncScheduleResponse(
        schedule_id=row.schedule_id,
        connector_id=row.connector_id,
        scope_id=row.connector_scope_id,
        status=row.status,
        interval_seconds=row.interval_seconds,
        next_run_at=row.next_run_at,
        last_due_at=row.last_due_at,
        last_enqueued_at=row.last_enqueued_at,
        last_job_id=row.last_job_id,
        pause_reason_code=row.pause_reason_code,
        paused_at=row.paused_at,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _raise_http(exc: Exception) -> None:
    if isinstance(exc, HTTPException):
        raise exc
    if isinstance(exc, ConnectorManagementNotFound):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Resource not found") from exc
    if isinstance(exc, GitHubInstallationNotFound):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Resource not found") from exc
    if isinstance(exc, GitHubRepositoryDiscoveryNotFound):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Resource not found") from exc
    if isinstance(exc, GitHubRepositorySelectionNotFound):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Resource not found") from exc
    if isinstance(exc, GitHubInstallationRejected):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="GitHub installation request is invalid") from exc
    if isinstance(exc, GitHubRepositoryDiscoveryRejected):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="GitHub repository discovery request is invalid") from exc
    if isinstance(exc, GitHubRepositorySelectionRejected):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="GitHub repository selection request is invalid") from exc
    if isinstance(exc, (GitHubInstallationConflict, GitHubBindingConflict)):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Resource state conflict") from exc
    if isinstance(exc, GitHubRepositoryDiscoveryConflict):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Resource state conflict") from exc
    if isinstance(exc, GitHubRepositorySelectionConflict):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Resource state conflict") from exc
    if isinstance(exc, GitHubProviderRateLimitError):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="GitHub provider is temporarily unavailable") from exc
    if isinstance(exc, GitHubProviderError):
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="GitHub provider request failed") from exc
    if isinstance(exc, SecretStoreError):
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Connector provider is unavailable") from exc
    if isinstance(exc, SyncScheduleNotFound):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Resource not found") from exc
    if isinstance(exc, (ConnectorRepositoryConflict, SyncJobConflict, SyncScheduleConflict)):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Resource conflict") from exc
    if isinstance(exc, (ConnectorManagementConflict, SyncScheduleResourceConflict)):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Resource state conflict") from exc
    if isinstance(exc, InvalidConnectorManagementRequest):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Connector request is invalid",
        ) from exc
    if isinstance(
        exc,
        (InvalidConnectorRepositoryRequest, InvalidSyncJobRequest, InvalidSyncScheduleRequest, ValueError),
    ):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Connector request is invalid") from exc
    if isinstance(
        exc,
        (
            ConnectorManagementPersistenceError,
            ConnectorRepositoryPersistenceError,
            SyncJobPersistenceError,
            SyncSchedulePersistenceError,
            GitHubInstallationPersistenceError,
            GitHubRepositorySelectionPersistenceError,
            SQLAlchemyError,
        ),
    ):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error") from exc
    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal server error") from exc
