from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
import app.api.router as api_router_module
from infrastructure.db.health import DatabaseHealthResult


def test_app_metadata() -> None:
    assert app.title == "Enterprise AI Platform API"
    assert app.version == "0.1.0"


def test_get_health_returns_200() -> None:
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200


def test_get_health_returns_healthy_payload() -> None:
    client = TestClient(app)

    response = client.get("/health")

    assert response.json() == {"status": "healthy"}


def test_get_api_v1_health_returns_database_status(monkeypatch) -> None:
    client = TestClient(app)
    monkeypatch.setattr(
        api_router_module,
        "check_database_connection",
        lambda: DatabaseHealthResult(healthy=True, message="Database connection is healthy."),
    )

    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "database": {
            "healthy": True,
            "message": "Database connection is healthy.",
        },
    }


def test_database_failure_is_represented_safely_without_credentials(monkeypatch) -> None:
    client = TestClient(app)

    unsafe_message = "db failed for postgresql://user:secret-password@127.0.0.1:5432/db"
    monkeypatch.setattr(
        api_router_module,
        "check_database_connection",
        lambda: DatabaseHealthResult(healthy=False, message=unsafe_message),
    )

    response = client.get("/api/v1/health")

    assert response.status_code == 200
    body = response.json()
    assert body["database"]["healthy"] is False
    assert body["database"]["message"] == "Database connection check failed."
    assert "secret-password" not in body["database"]["message"]
    assert "postgresql://" not in body["database"]["message"]
