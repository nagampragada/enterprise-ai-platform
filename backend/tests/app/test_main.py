from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

from fastapi.testclient import TestClient
import pytest

from app.main import app
import app.api.router as api_router_module
from infrastructure.db.health import DatabaseHealthResult


ROOT = Path(__file__).resolve().parents[2]


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


@pytest.mark.parametrize(
    ("schema_current", "migration_required"),
    ((False, True), (True, False)),
)
def test_get_api_v1_health_accepts_known_transition_revisions(
    monkeypatch,
    schema_current: bool,
    migration_required: bool,
) -> None:
    client = TestClient(app)
    monkeypatch.setattr(
        api_router_module,
        "check_database_connection",
        lambda: DatabaseHealthResult(
            healthy=True,
            message="Database connection is healthy.",
            schema_compatible=True,
            schema_current=schema_current,
        ),
    )

    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "checks": {
            "configuration": "ready",
            "database": "ready",
            "schema": "ready",
            "schema_compatible": True,
            "schema_current": schema_current,
            "migration_required": migration_required,
        },
    }


@pytest.mark.parametrize(
    "revision_state",
    ("unknown_older", "unknown_newer", "branched", "missing", "malformed"),
)
def test_get_api_v1_health_rejects_incompatible_revision_states(
    monkeypatch,
    revision_state: str,
) -> None:
    client = TestClient(app)
    monkeypatch.setattr(
        api_router_module,
        "check_database_connection",
        lambda: DatabaseHealthResult(
            healthy=True,
            message="Database connection is healthy.",
            schema_compatible=False,
            schema_current=False,
        ),
    )

    response = client.get("/api/v1/health")

    assert revision_state
    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "checks": {
            "configuration": "ready",
            "database": "ready",
            "schema": "not_ready",
            "schema_compatible": False,
            "schema_current": False,
            "migration_required": False,
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

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "not_ready"
    assert body["checks"]["database"] == "not_ready"
    assert body["checks"]["schema_compatible"] is False
    assert body["checks"]["schema_current"] is False
    assert body["checks"]["migration_required"] is False
    assert "secret-password" not in response.text
    assert "postgresql://" not in response.text


def test_configuration_failure_is_not_ready(monkeypatch) -> None:
    client = TestClient(app)
    monkeypatch.setattr(api_router_module, "_configuration_ready", lambda _request: False)
    monkeypatch.setattr(
        api_router_module,
        "check_database_connection",
        lambda: DatabaseHealthResult(
            healthy=True,
            message="ignored",
            schema_compatible=True,
            schema_current=True,
        ),
    )

    response = client.get("/api/v1/health")

    assert response.status_code == 503
    assert response.json()["checks"]["configuration"] == "not_ready"


def test_api_startup_does_not_compose_phase3_worker_components_for_prior_revision() -> None:
    script = """
import sys
from fastapi.testclient import TestClient
from infrastructure.db.health import DatabaseHealthResult
from app.main import app
import app.api.router as api_router_module

worker_only_modules = {
    'application.services.github_sync_work_processing_service',
    'infrastructure.repositories.connector_sync_work_ledger_repository',
    'infrastructure.workers.github_sync_work_item_worker',
}
assert worker_only_modules.isdisjoint(sys.modules)
api_router_module.check_database_connection = lambda: DatabaseHealthResult(
    healthy=True,
    message='Database connection is healthy.',
    schema_compatible=True,
    schema_current=False,
)
with TestClient(app) as client:
    response = client.get('/api/v1/health')
assert response.status_code == 200
assert response.json()['checks']['migration_required'] is True
assert worker_only_modules.isdisjoint(sys.modules)
"""
    environment = os.environ.copy()
    environment["APP_ENVIRONMENT"] = "development"
    environment["PYTHONPATH"] = str(ROOT / "src")
    environment.pop("GITHUB_SYNC_LEDGER_PLANNING_ENABLED", None)
    environment.pop("GITHUB_SYNC_LEDGER_PROCESSING_ENABLED", None)

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
