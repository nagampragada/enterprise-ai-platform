"""Central API router for versioned endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from app.api.v1.auth.router import auth_router
from app.api.v1.connectors.router import connectors_router
from app.api.v1.documents.router import documents_router
from infrastructure.db.health import check_database_connection
from app.config import (
    STRICT_RUNTIME_ENVIRONMENTS,
    load_runtime_environment,
    validate_api_process_environment,
)

api_router = APIRouter()


@api_router.get("/health")
def health_with_database(request: Request) -> JSONResponse:
    configuration_ready = _configuration_ready(request)
    db_health = check_database_connection()
    ready = configuration_ready and db_health.healthy and db_health.schema_current
    payload = {
        "status": "ready" if ready else "not_ready",
        "checks": {
            "configuration": "ready" if configuration_ready else "not_ready",
            "database": "ready" if db_health.healthy else "not_ready",
            "schema": "ready" if db_health.schema_current else "not_ready",
        },
    }
    return JSONResponse(
        status_code=status.HTTP_200_OK
        if ready
        else status.HTTP_503_SERVICE_UNAVAILABLE,
        content=payload,
    )


def _configuration_ready(request: Request) -> bool:
    try:
        validate_api_process_environment()
        runtime = load_runtime_environment()
    except Exception:
        return False
    if runtime in STRICT_RUNTIME_ENVIRONMENTS:
        return getattr(request.app.state, "github_composition_ready", False) is True
    return True


api_router.include_router(auth_router)
api_router.include_router(connectors_router)
api_router.include_router(documents_router)
