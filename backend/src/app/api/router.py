"""Central API router for versioned endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from app.api.v1.auth.router import auth_router
from app.api.v1.connectors.router import connectors_router
from app.api.v1.documents.router import documents_router
from infrastructure.db.health import check_database_connection

api_router = APIRouter()


@api_router.get("/health")
def health_with_database() -> dict[str, object]:
    db_health = check_database_connection()

    # Avoid leaking low-level database diagnostics from API responses.
    db_message = db_health.message if db_health.healthy else "Database connection check failed."

    return {
        "status": "healthy",
        "database": {
            "healthy": db_health.healthy,
            "message": db_message,
        },
    }


api_router.include_router(auth_router)
api_router.include_router(connectors_router)
api_router.include_router(documents_router)
