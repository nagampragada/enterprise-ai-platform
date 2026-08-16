from __future__ import annotations

from dataclasses import asdict, fields
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.services.local_folder_synchronization_service import (
    InvalidLocalFolderSynchronizationRequest,
    LocalFolderSynchronizationRequest,
    LocalFolderSynchronizationService,
    LocalFolderSynchronizationUnavailable,
    NonProgressingLocalFolderSynchronization,
    UnsafeLocalFolderConfiguration,
)
from infrastructure.connectors.local.connector import LocalFolderConnector
from infrastructure.db.models import Connector, ConnectorScope, ConnectorSyncCursor, ConnectorSyncRun


def _service() -> tuple[LocalFolderSynchronizationService, tuple[Mock, ...]]:
    dependencies = tuple(Mock() for _ in range(7))
    return LocalFolderSynchronizationService(*dependencies), dependencies


def _connector(organization_id, connector_id, root: Path, **values) -> Connector:
    provider = LocalFolderConnector(organization_id, connector_id, root)
    defaults = dict(
        id=connector_id,
        organization_id=organization_id,
        connector_type="local_folder",
        display_name="Local",
        slug="local",
        status="active",
        acl_support="none",
        capabilities=asdict(provider.capabilities),
        safe_config={},
        config_schema_version=1,
        credential_status="not_configured",
    )
    defaults.update(values)
    return Connector(**defaults)


def _scope(organization_id, connector_id, root: Path, **values) -> ConnectorScope:
    defaults = dict(
        id=uuid4(),
        organization_id=organization_id,
        connector_id=connector_id,
        knowledge_space_id=uuid4(),
        display_name="Root",
        slug="root",
        scope_type="folder",
        external_scope_key=str(root),
        access_mode="platform_managed",
        status="active",
        safe_config={"follow_symlinks": False},
        config_schema_version=1,
    )
    defaults.update(values)
    return ConnectorScope(**defaults)


def test_invalid_request_rejected_before_repository_or_filesystem_access() -> None:
    service, dependencies = _service()
    request = LocalFolderSynchronizationRequest("bad", uuid4(), uuid4())  # type: ignore[arg-type]

    with pytest.raises(InvalidLocalFolderSynchronizationRequest):
        service.synchronize(request)

    for dependency in dependencies:
        dependency.assert_not_called()


def test_cross_tenant_or_cross_connector_scope_rejected_before_traversal(tmp_path: Path) -> None:
    service, dependencies = _service()
    connectors, scopes, _, sync, *_ = dependencies
    organization_id, connector_id = uuid4(), uuid4()
    connectors.lock_by_id.return_value = _connector(organization_id, connector_id, tmp_path)
    scopes.lock_by_id.return_value = _scope(organization_id, uuid4(), tmp_path)

    with pytest.raises(LocalFolderSynchronizationUnavailable):
        service.synchronize(LocalFolderSynchronizationRequest(organization_id, connector_id, uuid4()))

    sync.add_run.assert_not_called()


@pytest.mark.parametrize(
    ("connector_values", "scope_values"),
    [
        ({"connector_type": "google_drive"}, {}),
        ({"status": "paused"}, {}),
        ({"status": "archived", "archived_at": datetime.now(UTC)}, {}),
        ({}, {"status": "paused"}),
        ({}, {"status": "removed", "removed_at": datetime.now(UTC)}),
        ({}, {"access_mode": "source_acl"}),
    ],
)
def test_incompatible_connector_or_scope_rejected(
    tmp_path: Path, connector_values: dict[str, object], scope_values: dict[str, object]
) -> None:
    service, dependencies = _service()
    connectors, scopes, _, sync, *_ = dependencies
    organization_id, connector_id, scope_id = uuid4(), uuid4(), uuid4()
    connectors.lock_by_id.return_value = _connector(
        organization_id, connector_id, tmp_path, **connector_values
    )
    scopes.lock_by_id.return_value = _scope(
        organization_id, connector_id, tmp_path, id=scope_id, **scope_values
    )

    with pytest.raises(LocalFolderSynchronizationUnavailable):
        service.synchronize(LocalFolderSynchronizationRequest(organization_id, connector_id, scope_id))

    sync.add_run.assert_not_called()


@pytest.mark.parametrize("root_kind", ["relative", "missing", "file", "traversal"])
def test_unsafe_persisted_root_fails_without_path_disclosure(tmp_path: Path, root_kind: str) -> None:
    if root_kind == "relative":
        root = Path("relative-root")
    elif root_kind == "traversal":
        (tmp_path / "safe").mkdir()
        root = tmp_path / "safe" / ".." / "safe"
    else:
        root = tmp_path / root_kind
    if root_kind == "file":
        root.write_text("not a directory", encoding="utf-8")
    service, dependencies = _service()
    connectors, scopes, _, sync, *_ = dependencies
    organization_id, connector_id, scope_id = uuid4(), uuid4(), uuid4()
    connectors.lock_by_id.return_value = _connector(organization_id, connector_id, tmp_path)
    scopes.lock_by_id.return_value = _scope(
        organization_id, connector_id, root, id=scope_id
    )

    with pytest.raises(UnsafeLocalFolderConfiguration) as raised:
        service.synchronize(LocalFolderSynchronizationRequest(organization_id, connector_id, scope_id))

    assert str(root) not in str(raised.value)
    sync.add_run.assert_not_called()


def test_request_has_no_runtime_root_override() -> None:
    assert "root_path" not in {field.name for field in fields(LocalFolderSynchronizationRequest)}
    assert "path" not in {field.name for field in fields(LocalFolderSynchronizationRequest)}


def test_capability_mismatch_rejected_before_run_start(tmp_path: Path) -> None:
    service, dependencies = _service()
    connectors, scopes, _, sync, *_ = dependencies
    organization_id, connector_id, scope_id = uuid4(), uuid4(), uuid4()
    connector = _connector(organization_id, connector_id, tmp_path)
    connector.capabilities = {"supports_folders": True}
    connectors.lock_by_id.return_value = connector
    scopes.lock_by_id.return_value = _scope(
        organization_id, connector_id, tmp_path, id=scope_id
    )

    with pytest.raises(LocalFolderSynchronizationUnavailable):
        service.synchronize(LocalFolderSynchronizationRequest(organization_id, connector_id, scope_id))

    sync.add_run.assert_not_called()


def test_invalid_persisted_continuation_is_detected_without_traversal(tmp_path: Path) -> None:
    service, dependencies = _service()
    connectors, scopes, _, sync, *_ = dependencies
    organization_id, connector_id, scope_id, run_id = uuid4(), uuid4(), uuid4(), uuid4()
    connectors.lock_by_id.return_value = _connector(organization_id, connector_id, tmp_path)
    scopes.lock_by_id.return_value = _scope(
        organization_id, connector_id, tmp_path, id=scope_id
    )
    sync.lock_run.return_value = ConnectorSyncRun(
        id=run_id,
        organization_id=organization_id,
        connector_id=connector_id,
        connector_scope_id=scope_id,
        mode="incremental",
        trigger_type="manual",
        status="running",
        started_at=datetime.now(UTC),
        run_metadata={},
    )
    sync.get_active_cursor.return_value = ConnectorSyncCursor(
        id=uuid4(),
        organization_id=organization_id,
        connector_id=connector_id,
        connector_scope_id=scope_id,
        created_by_run_id=run_id,
        cursor_version=1,
        cursor_type="local_folder_progress",
        state="active",
        safe_cursor={"phase": "discovery", "after_key": ""},
        activated_at=datetime.now(UTC),
    )

    with pytest.raises(NonProgressingLocalFolderSynchronization):
        service.synchronize(
            LocalFolderSynchronizationRequest(
                organization_id, connector_id, scope_id, sync_run_id=run_id
            )
        )


def test_service_owns_no_session_commit_or_rollback() -> None:
    service, dependencies = _service()
    assert not hasattr(service, "_session")
    for dependency in dependencies:
        dependency.commit.assert_not_called()
        dependency.rollback.assert_not_called()
