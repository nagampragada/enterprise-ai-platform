from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.dependencies import (
    ConnectorAdministrator,
    get_connector_administrator,
    get_connector_management_service,
    get_db_session,
)
from app.main import app
from application.services.connector_management_service import (
    ConnectorManagementNotFound,
)
from infrastructure.repositories.connector_repository import (
    ConnectorPage,
    ConnectorPageCursor,
    ConnectorRepositoryConflict,
)
from infrastructure.repositories.connector_scope_repository import (
    ConnectorScopePage,
    ConnectorScopePageCursor,
)
from infrastructure.repositories.connector_sync_job_repository import (
    EnqueueResult,
    SyncJobHistoryItem,
    SyncJobPage,
    SyncJobPageCursor,
)

NOW = datetime(2026, 8, 18, tzinfo=timezone.utc)


@dataclass
class FakeSession:
    commit_calls: int = 0
    rollback_calls: int = 0

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1


def _connector(**overrides):
    values = dict(
        id=uuid4(),
        connector_type="local_folder",
        display_name="Local knowledge",
        slug="local-knowledge",
        status="active",
        acl_support="none",
        capabilities={
            "supports_incremental_sync": False,
            "supports_permissions": False,
            "supports_folders": True,
            "supports_deletions": True,
            "supports_version_history": False,
            "supports_webhooks": False,
            "supports_content_download": True,
        },
        safe_config={"secret": "must-not-leak"},
        secret_reference="vault://must-not-leak",
        created_at=NOW,
        updated_at=NOW,
        last_validated_at=None,
        archived_at=None,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def _scope(connector_id=None, **overrides):
    values = dict(
        id=uuid4(), connector_id=connector_id or uuid4(), knowledge_space_id=uuid4(),
        display_name="Finance folder", slug="finance-folder", scope_type="folder",
        access_mode="platform_managed", status="active", external_scope_key="C:\\secret-root",
        safe_config={"follow_symlinks": False}, created_at=NOW, updated_at=NOW,
        last_validated_at=None, removed_at=None,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def _job(connector_id=None, scope_id=None, **overrides):
    values = dict(
        job_id=uuid4(), connector_id=connector_id or uuid4(),
        connector_scope_id=scope_id or uuid4(), mode="incremental", trigger_type="manual",
        status="queued", attempt_count=0, max_attempts=3, next_attempt_at=NOW,
        cancellation_requested=False, completed_at=None, last_error_category=None,
        last_error_code=None, created_at=NOW,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def _setup(service=None, session=None):
    service = service or Mock()
    session = session or FakeSession()
    administrator = ConnectorAdministrator(uuid4(), uuid4())

    def session_override():
        yield session

    app.dependency_overrides[get_db_session] = session_override
    app.dependency_overrides[get_connector_administrator] = lambda: administrator
    app.dependency_overrides[get_connector_management_service] = lambda: service
    return service, session, administrator


@pytest.fixture(autouse=True)
def clear_overrides():
    yield
    app.dependency_overrides.clear()


def test_unauthenticated_connector_request_is_rejected():
    with TestClient(app) as client:
        response = client.get("/api/v1/connectors")
    assert response.status_code == 401


def test_authenticated_non_admin_is_forbidden():
    app.dependency_overrides[get_connector_administrator] = lambda: (_ for _ in ()).throw(
        HTTPException(status_code=403, detail="Connector administration is forbidden")
    )
    with TestClient(app) as client:
        response = client.get("/api/v1/connectors")
    assert response.status_code == 403


def test_create_connector_derives_actor_commits_and_redacts_sensitive_fields():
    service, session, admin = _setup()
    row = _connector()
    service.create_local_folder_connector.return_value = row
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/connectors",
            json={"connector_type": "local_folder", "display_name": "Local knowledge", "slug": "local-knowledge"},
        )
    assert response.status_code == 201
    service.create_local_folder_connector.assert_called_once_with(
        admin.organization_id, admin.user_id, display_name="Local knowledge", slug="local-knowledge"
    )
    assert session.commit_calls == 1 and session.rollback_calls == 0
    body = response.json()
    assert "safe_config" not in body and "secret_reference" not in body
    assert "credential_status" not in body and "organization_id" not in body


def test_connector_request_rejects_unsupported_type_unknown_fields_and_client_identity():
    _setup()
    payloads = (
        {"connector_type": "google_drive", "display_name": "Drive", "slug": "drive"},
        {"connector_type": "local_folder", "display_name": "Folder", "slug": "folder", "secret_reference": "raw"},
        {"connector_type": "local_folder", "display_name": "Folder", "slug": "folder", "organization_id": str(uuid4())},
    )
    with TestClient(app) as client:
        for payload in payloads:
            assert client.post("/api/v1/connectors", json=payload).status_code == 422


def test_duplicate_connector_is_safe_conflict_and_rolls_back():
    service, session, _ = _setup()
    service.create_local_folder_connector.side_effect = ConnectorRepositoryConflict("constraint name")
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/connectors",
            json={"connector_type": "local_folder", "display_name": "Folder", "slug": "folder"},
        )
    assert response.status_code == 409 and response.json() == {"detail": "Resource conflict"}
    assert session.rollback_calls == 1 and "constraint" not in response.text


def test_connector_list_uses_keyset_cursor_and_is_tenant_scoped():
    service, _, admin = _setup()
    first, second = _connector(slug="one"), _connector(slug="two")
    next_cursor = ConnectorPageCursor(second.created_at, second.id)
    service.list_connectors.return_value = ConnectorPage((first, second), 2, True, next_cursor)
    with TestClient(app) as client:
        response = client.get("/api/v1/connectors?limit=2&status=active")
        cursor = response.json()["next_cursor"]
        second_response = client.get(f"/api/v1/connectors?limit=2&cursor={cursor}")
    assert response.status_code == second_response.status_code == 200
    assert service.list_connectors.call_args_list[0].args[0] == admin.organization_id
    assert service.list_connectors.call_args_list[1].kwargs["cursor"] == next_cursor
    assert "secret_reference" not in response.text


def test_malformed_or_wrong_kind_cursor_is_rejected_safely():
    _setup()
    with TestClient(app) as client:
        response = client.get("/api/v1/connectors?cursor=not-base64")
    assert response.status_code == 422


def test_scope_creation_is_redacted_and_cannot_override_access_or_config():
    service, session, admin = _setup()
    connector_id = uuid4()
    row = _scope(connector_id)
    service.create_local_folder_scope.return_value = row
    payload = {
        "knowledge_space_id": str(row.knowledge_space_id), "display_name": "Finance folder",
        "slug": "finance-folder", "configuration": {"root_path": "C:\\sensitive-root"},
    }
    with TestClient(app) as client:
        response = client.post(f"/api/v1/connectors/{connector_id}/scopes", json=payload)
    assert response.status_code == 201 and session.commit_calls == 1
    service.create_local_folder_scope.assert_called_once_with(
        admin.organization_id, admin.user_id, connector_id,
        knowledge_space_id=row.knowledge_space_id, display_name="Finance folder",
        slug="finance-folder", root_path="C:\\sensitive-root", follow_symlinks=False,
    )
    assert "root_path" not in response.text and "external_scope_key" not in response.text
    assert "safe_config" not in response.text


def test_scope_unknown_secret_fields_and_unsupported_access_mode_are_rejected():
    _setup()
    connector_id = uuid4()
    base = {
        "knowledge_space_id": str(uuid4()), "display_name": "Folder", "slug": "folder",
        "configuration": {"root_path": "C:\\root"},
    }
    with TestClient(app) as client:
        access_response = client.post(
            f"/api/v1/connectors/{connector_id}/scopes",
            json={**base, "access_mode": "source_acl"},
        )
        secret_response = client.post(
            f"/api/v1/connectors/{connector_id}/scopes",
            json={**base, "configuration": {"root_path": "C:\\root", "password": "secret"}},
        )
    assert access_response.status_code == secret_response.status_code == 422
    assert access_response.json() == secret_response.json() == {"detail": "Connector request is invalid"}
    assert "C:\\\\root" not in secret_response.text
    assert "password" not in secret_response.text and "secret" not in secret_response.text


def test_scope_list_is_bounded_deterministic_and_redacted():
    service, _, admin = _setup()
    connector_id = uuid4()
    row = _scope(connector_id)
    service.list_scopes.return_value = ConnectorScopePage(
        (row,), 20, False, None
    )
    with TestClient(app) as client:
        response = client.get(f"/api/v1/connectors/{connector_id}/scopes")
    assert response.status_code == 200
    service.list_scopes.assert_called_once_with(
        admin.organization_id, connector_id, limit=20, cursor=None, status=None
    )
    assert "secret-root" not in response.text and "safe_config" not in response.text


def test_enqueue_returns_202_coalesces_and_never_executes_worker_or_accepts_body():
    service, session, admin = _setup()
    connector_id, scope_id = uuid4(), uuid4()
    job = _job(connector_id, scope_id)
    service.enqueue_sync_job.return_value = (EnqueueResult(job.job_id, "queued", False), job)
    with patch("infrastructure.workers.local_folder_sync_worker.LocalFolderSyncWorker.run_one") as run:
        with TestClient(app) as client:
            response = client.post(f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs")
            rejected = client.post(
                f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs",
                json={"max_attempts": 5},
            )
    assert response.status_code == 202 and rejected.status_code == 422
    assert response.json()["coalesced"] is False
    assert session.commit_calls == 1
    service.enqueue_sync_job.assert_called_with(admin.organization_id, admin.user_id, connector_id, scope_id)
    run.assert_not_called()
    assert "lease" not in response.text and "worker" not in response.text


def test_job_list_and_get_are_redacted_and_cross_tenant_not_found_is_safe():
    service, _, admin = _setup()
    connector_id, scope_id = uuid4(), uuid4()
    job = _job(connector_id, scope_id, last_error_category="internal", last_error_code="unknown_internal")
    service.list_sync_jobs.return_value = SyncJobPage((job,), 20, False, None)
    service.get_sync_job.return_value = job
    with TestClient(app) as client:
        listing = client.get(f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs")
        detail = client.get(f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs/{job.job_id}")
    assert listing.status_code == detail.status_code == 200
    assert service.list_sync_jobs.call_args.args[0] == admin.organization_id
    for response in (listing, detail):
        assert "summary" not in response.text and "lease" not in response.text
        assert "worker" not in response.text and "path" not in response.text

    service.get_sync_job.side_effect = ConnectorManagementNotFound("foreign id")
    with TestClient(app) as client:
        concealed = client.get(f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs/{uuid4()}")
    assert concealed.status_code == 404 and concealed.json() == {"detail": "Resource not found"}