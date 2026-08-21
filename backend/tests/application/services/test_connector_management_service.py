from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.services.connector_management_service import (
    ConnectorManagementConflict,
    ConnectorManagementNotFound,
    ConnectorManagementService,
    InvalidConnectorManagementRequest,
)
from infrastructure.repositories.connector_sync_job_repository import EnqueueResult

NOW = datetime(2026, 8, 18, tzinfo=timezone.utc)


def _service():
    session = Mock()
    service = ConnectorManagementService(session, clock=lambda: NOW)
    service._connectors = Mock()
    service._scopes = Mock()
    service._jobs = Mock()
    service._credentials = Mock()
    service._installations = Mock()
    return service, session


def test_connector_creation_uses_safe_server_owned_local_folder_defaults():
    service, session = _service()
    organization_id, user_id = uuid4(), uuid4()
    service._connectors.add.side_effect = lambda org, row: row
    connector = service.create_local_folder_connector(
        organization_id,
        user_id,
        display_name="Local knowledge",
        slug="local-knowledge",
    )
    assert connector.organization_id == organization_id
    assert connector.created_by_user_id == user_id
    assert connector.connector_type == "local_folder"
    assert connector.status == "active"
    assert connector.acl_support == "none"
    assert connector.safe_config == {}
    assert connector.capabilities["supports_permissions"] is False
    session.commit.assert_not_called()
    session.rollback.assert_not_called()


def test_github_connector_creation_uses_draft_and_only_committed_capabilities():
    service, session = _service()
    organization_id, user_id = uuid4(), uuid4()
    service._connectors.add.side_effect = lambda org, row: row
    connector = service.create_github_connector(
        organization_id,
        user_id,
        display_name="GitHub",
        slug="github",
    )
    assert connector.organization_id == organization_id
    assert connector.created_by_user_id == user_id
    assert connector.connector_type == "github"
    assert connector.status == "draft"
    assert connector.acl_support == "none"
    assert connector.safe_config == {}
    assert connector.capabilities["supports_repository_discovery"] is True
    assert connector.capabilities["supports_repository_selection"] is True
    assert connector.capabilities["supports_bounded_content_reading"] is True
    assert connector.capabilities["supports_staged_synchronization"] is True
    assert {
        value
        for key, value in connector.capabilities.items()
        if key
        not in {
            "supports_repository_discovery",
            "supports_repository_selection",
            "supports_bounded_content_reading",
            "supports_staged_synchronization",
        }
    } == {False}
    session.commit.assert_not_called()
    session.rollback.assert_not_called()


def test_missing_and_cross_tenant_connector_are_concealed():
    service, _ = _service()
    service._connectors.get_by_id.return_value = None
    with pytest.raises(ConnectorManagementNotFound, match="connector was not found"):
        service.get_connector(uuid4(), uuid4())


def test_scope_creation_validates_parent_space_root_and_safe_configuration(monkeypatch):
    service, session = _service()
    organization_id, user_id, connector_id, space_id = (uuid4() for _ in range(4))
    service._connectors.lock_by_id.return_value = SimpleNamespace(
        connector_type="local_folder", status="active", acl_support="none"
    )
    monkeypatch.setattr(service, "_require_active_knowledge_space", Mock())
    service._scopes.add.side_effect = lambda org, row: row
    scope = service.create_local_folder_scope(
        organization_id,
        user_id,
        connector_id,
        knowledge_space_id=space_id,
        display_name="Finance folder",
        slug="finance-folder",
        root_path="C:\\enterprise-data\\finance",
        follow_symlinks=False,
    )
    assert scope.organization_id == organization_id
    assert scope.connector_id == connector_id
    assert scope.knowledge_space_id == space_id
    assert scope.created_by_user_id == user_id
    assert scope.scope_type == "folder"
    assert scope.access_mode == "platform_managed"
    assert scope.status == "active"
    assert scope.safe_config == {"follow_symlinks": False}
    session.commit.assert_not_called()


@pytest.mark.parametrize(
    "root",
    ("relative/path", "../escape", "", "C:\\safe\\..\\escape"),
)
def test_scope_rejects_invalid_roots_without_filesystem_access(monkeypatch, root):
    service, _ = _service()
    service._connectors.lock_by_id.return_value = SimpleNamespace(
        connector_type="local_folder", status="active", acl_support="none"
    )
    monkeypatch.setattr(service, "_require_active_knowledge_space", Mock())
    with pytest.raises(InvalidConnectorManagementRequest):
        service.create_local_folder_scope(
            uuid4(), uuid4(), uuid4(), knowledge_space_id=uuid4(),
            display_name="Folder", slug="folder", root_path=root, follow_symlinks=False,
        )


def test_scope_rejects_unsupported_acl_and_inactive_connector(monkeypatch):
    service, _ = _service()
    monkeypatch.setattr(service, "_require_active_knowledge_space", Mock())
    for connector in (
        SimpleNamespace(connector_type="local_folder", status="paused", acl_support="none"),
        SimpleNamespace(connector_type="local_folder", status="active", acl_support="complete"),
        SimpleNamespace(connector_type="google_drive", status="active", acl_support="none"),
    ):
        service._connectors.lock_by_id.return_value = connector
        with pytest.raises((InvalidConnectorManagementRequest, ConnectorManagementConflict)):
            service.create_local_folder_scope(
                uuid4(), uuid4(), uuid4(), knowledge_space_id=uuid4(),
                display_name="Folder", slug="folder", root_path="C:\\safe", follow_symlinks=False,
            )


def test_enqueue_locks_tenant_parents_and_delegates_atomic_coalescing():
    service, session = _service()
    organization_id, user_id, connector_id, scope_id, job_id = (uuid4() for _ in range(5))
    service._connectors.lock_by_id.return_value = SimpleNamespace(
        connector_type="local_folder", status="active"
    )
    service._scopes.lock_by_id.return_value = SimpleNamespace(
        connector_id=connector_id, knowledge_space_id=uuid4(), scope_type="folder",
        status="active"
    )
    service._require_active_knowledge_space = Mock()
    service._jobs.enqueue_or_coalesce.return_value = EnqueueResult(job_id, "queued", False)
    history = SimpleNamespace(job_id=job_id)
    service._jobs.get.return_value = history
    result, returned = service.enqueue_sync_job(
        organization_id, user_id, connector_id, scope_id
    )
    assert result == EnqueueResult(job_id, "queued", False)
    assert returned is history
    service._jobs.enqueue_or_coalesce.assert_called_once_with(
        organization_id,
        connector_id,
        scope_id,
        mode="incremental",
        trigger_type="manual",
        now=NOW,
        requested_by_user_id=user_id,
    )
    session.commit.assert_not_called()
    session.rollback.assert_not_called()


def test_enqueue_rejects_mismatched_scope_and_inactive_lifecycle():
    service, _ = _service()
    connector_id = uuid4()
    service._connectors.lock_by_id.return_value = SimpleNamespace(
        connector_type="local_folder", status="active"
    )
    service._scopes.lock_by_id.return_value = SimpleNamespace(
        connector_id=uuid4(), knowledge_space_id=uuid4(), scope_type="folder", status="active"
    )
    with pytest.raises(ConnectorManagementNotFound):
        service.enqueue_sync_job(uuid4(), uuid4(), connector_id, uuid4())
    service._scopes.lock_by_id.return_value = SimpleNamespace(
        connector_id=connector_id, knowledge_space_id=uuid4(), scope_type="folder", status="paused"
    )
    with pytest.raises(ConnectorManagementConflict):
        service.enqueue_sync_job(uuid4(), uuid4(), connector_id, uuid4())


def test_github_enqueue_requires_canonical_scope_and_persisted_authorization():
    service, session = _service()
    organization_id, user_id, connector_id, scope_id, space_id = (
        uuid4() for _ in range(5)
    )
    credential_id, installation_id, job_id = uuid4(), 12345, uuid4()
    service._connectors.lock_by_id.return_value = SimpleNamespace(
        connector_type="github", status="active"
    )
    service._scopes.lock_by_id.return_value = SimpleNamespace(
        connector_id=connector_id,
        knowledge_space_id=space_id,
        scope_type="repository",
        access_mode="platform_managed",
        config_schema_version=1,
        external_scope_key="github:repository:987",
        status="active",
        safe_config={
            "repository_id": 987,
            "repository_name": "platform",
            "repository_full_name": "enterprise/platform",
            "owner_login": "enterprise",
            "private": True,
            "visibility": "private",
            "archived": False,
            "disabled": False,
            "default_branch": "main",
        },
    )
    service._require_active_knowledge_space = Mock()
    service._credentials.lock.return_value = SimpleNamespace(
        id=credential_id,
        status="active",
        provider_key="github",
        auth_scheme="app_installation",
        granted_scopes=["contents:read", "metadata:read"],
        expires_at=None,
        external_subject=str(installation_id),
    )
    service._installations.lock.return_value = SimpleNamespace(
        credential_id=credential_id,
        github_installation_id=installation_id,
        status="connected",
        disconnected_at=None,
        account_type="Organization",
    )
    service._jobs.enqueue_or_coalesce.return_value = EnqueueResult(job_id, "queued", False)
    service._jobs.get.return_value = SimpleNamespace(job_id=job_id)

    result, _ = service.enqueue_sync_job(
        organization_id, user_id, connector_id, scope_id
    )

    assert result.job_id == job_id
    service._credentials.lock.assert_called_once_with(organization_id, connector_id)
    service._installations.lock.assert_called_once_with(organization_id, connector_id)
    session.commit.assert_not_called()

    service._credentials.lock.return_value.status = "revoked"
    with pytest.raises(ConnectorManagementConflict, match="authorization"):
        service.enqueue_sync_job(organization_id, user_id, connector_id, scope_id)


def test_connector_job_reads_are_connector_qualified_and_run_history_is_bounded():
    service, _ = _service()
    organization_id, connector_id, job_id = uuid4(), uuid4(), uuid4()
    connector = SimpleNamespace(id=connector_id)
    job = SimpleNamespace(job_id=job_id)
    runs = (SimpleNamespace(run_id=uuid4()),)
    service._connectors.get_by_id.return_value = connector
    service._jobs.get_for_connector.return_value = job
    service._jobs.list_attempt_runs.return_value = runs

    detail = service.get_connector_sync_job(organization_id, connector_id, job_id)

    assert detail == (job, runs)
    service._jobs.get_for_connector.assert_called_once_with(
        organization_id, connector_id, job_id
    )
    service._jobs.list_attempt_runs.assert_called_once_with(
        organization_id, connector_id, job_id, limit=20
    )

    service._jobs.get_for_connector.return_value = None
    with pytest.raises(ConnectorManagementNotFound):
        service.get_connector_sync_job(organization_id, connector_id, uuid4())


@pytest.mark.parametrize("terminal_status", ("succeeded", "failed", "cancelled"))
def test_connector_job_terminal_cancellation_is_idempotent(terminal_status):
    service, session = _service()
    organization_id, user_id, connector_id, job_id = (
        uuid4() for _ in range(4)
    )
    terminal = SimpleNamespace(job_id=job_id, status=terminal_status)
    service._connectors.get_by_id.return_value = SimpleNamespace(id=connector_id)
    service._jobs.get_for_connector.return_value = terminal

    result = service.cancel_connector_sync_job(
        organization_id, user_id, connector_id, job_id
    )

    assert result is terminal
    service._jobs.get_for_connector.assert_called_once_with(
        organization_id, connector_id, job_id, lock=True
    )
    service._jobs.request_cancellation.assert_not_called()
    session.commit.assert_not_called()


def test_connector_job_running_cancellation_delegates_existing_atomic_lifecycle():
    service, _ = _service()
    organization_id, user_id, connector_id, job_id = (
        uuid4() for _ in range(4)
    )
    running = SimpleNamespace(job_id=job_id, status="running")
    requested = SimpleNamespace(job_id=job_id, status="running", cancellation_requested=True)
    service._connectors.get_by_id.return_value = SimpleNamespace(id=connector_id)
    service._jobs.get_for_connector.return_value = running
    service._jobs.request_cancellation.return_value = requested

    assert service.cancel_connector_sync_job(
        organization_id, user_id, connector_id, job_id
    ) is requested
    service._jobs.request_cancellation.assert_called_once_with(
        organization_id,
        job_id,
        requested_by_user_id=user_id,
        reason_code="user_requested",
        now=NOW,
    )


def test_source_contains_no_worker_provider_commit_or_generic_mutation():
    import inspect
    import application.services.connector_management_service as module

    source = inspect.getsource(module)
    assert "LocalFolderSyncWorker" not in source
    assert "EmbeddingProvider" not in source
    assert ".commit(" not in source
    assert ".rollback(" not in source
    assert "update_fields" not in source
