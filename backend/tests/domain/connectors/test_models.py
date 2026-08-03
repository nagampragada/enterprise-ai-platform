from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from domain.connectors.capabilities import ConnectorCapabilities
from domain.connectors.models import (
    ConnectorType,
    PermissionEffect,
    PermissionPrincipalType,
    SourceItem,
    SourceItemType,
    SourcePermission,
    SyncCheckpoint,
)


def _build_source_item(**overrides):
    base = {
        "organization_id": uuid4(),
        "connector_id": uuid4(),
        "external_id": "ext-123",
        "connector_type": ConnectorType.GITHUB,
        "item_type": SourceItemType.FILE,
        "title": "Quarterly Plan",
    }
    base.update(overrides)
    return SourceItem(**base)


def test_connector_capabilities_is_frozen() -> None:
    capabilities = ConnectorCapabilities(
        supports_incremental_sync=True,
        supports_permissions=True,
        supports_folders=True,
        supports_deletions=True,
        supports_version_history=False,
        supports_webhooks=False,
        supports_content_download=True,
    )

    with pytest.raises(FrozenInstanceError):
        capabilities.supports_webhooks = True


def test_source_permission_is_frozen() -> None:
    permission = SourcePermission(
        principal_id="u-1",
        principal_type=PermissionPrincipalType.USER,
        effect=PermissionEffect.ALLOW,
    )

    with pytest.raises(FrozenInstanceError):
        permission.role = "owner"


def test_sync_checkpoint_is_frozen() -> None:
    checkpoint = SyncCheckpoint(cursor="abc")

    with pytest.raises(FrozenInstanceError):
        checkpoint.cursor = "def"


def test_source_item_is_frozen() -> None:
    item = _build_source_item()

    with pytest.raises(FrozenInstanceError):
        item.title = "New Title"


def test_source_item_rejects_blank_external_id() -> None:
    with pytest.raises(ValueError, match="external_id must not be blank"):
        _build_source_item(external_id="")


def test_source_item_rejects_whitespace_only_external_id() -> None:
    with pytest.raises(ValueError, match="external_id must not be blank"):
        _build_source_item(external_id="   ")


def test_source_item_rejects_blank_title() -> None:
    with pytest.raises(ValueError, match="title must not be blank"):
        _build_source_item(title="")


def test_source_item_rejects_whitespace_only_title() -> None:
    with pytest.raises(ValueError, match="title must not be blank"):
        _build_source_item(title="   ")


def test_source_item_accepts_timezone_aware_timestamps() -> None:
    created_at = datetime.now(UTC)
    updated_at = datetime.now(UTC)

    item = _build_source_item(created_at=created_at, updated_at=updated_at)

    assert item.created_at == created_at
    assert item.updated_at == updated_at


def test_source_item_rejects_naive_created_at() -> None:
    with pytest.raises(ValueError, match="created_at must be timezone-aware"):
        _build_source_item(created_at=datetime.utcnow())


def test_source_item_rejects_naive_updated_at() -> None:
    with pytest.raises(ValueError, match="updated_at must be timezone-aware"):
        _build_source_item(updated_at=datetime.utcnow())


def test_sync_checkpoint_accepts_timezone_aware_last_synced_at() -> None:
    checkpoint_time = datetime.now(UTC)

    checkpoint = SyncCheckpoint(last_synced_at=checkpoint_time)

    assert checkpoint.last_synced_at == checkpoint_time


def test_sync_checkpoint_rejects_naive_last_synced_at() -> None:
    with pytest.raises(ValueError, match="last_synced_at must be timezone-aware"):
        SyncCheckpoint(last_synced_at=datetime.utcnow())


def test_source_item_metadata_defaults_safely_and_is_immutable() -> None:
    first = _build_source_item()
    second = _build_source_item()

    assert first.metadata == {}
    assert second.metadata == {}
    assert first.metadata is not second.metadata

    with pytest.raises(TypeError):
        first.metadata["x"] = "y"


def test_sync_checkpoint_metadata_defaults_safely_and_is_immutable() -> None:
    first = SyncCheckpoint()
    second = SyncCheckpoint()

    assert first.metadata == {}
    assert second.metadata == {}
    assert first.metadata is not second.metadata

    with pytest.raises(TypeError):
        first.metadata["x"] = "y"


def test_source_item_permissions_default_to_empty_tuple() -> None:
    item = _build_source_item()

    assert item.permissions == ()


def test_source_item_permissions_are_stored_as_immutable_tuple() -> None:
    permission = SourcePermission(
        principal_id="g-1",
        principal_type=PermissionPrincipalType.GROUP,
        effect=PermissionEffect.ALLOW,
        role="reader",
    )
    item = _build_source_item(permissions=[permission])

    assert isinstance(item.permissions, tuple)
    assert item.permissions == (permission,)

    with pytest.raises(FrozenInstanceError):
        item.permissions = ()


def test_enum_values_match_public_contract() -> None:
    assert [member.value for member in ConnectorType] == [
        "local_folder",
        "google_drive",
        "sharepoint",
        "onedrive",
        "slack",
        "jira",
        "confluence",
        "github",
        "gmail",
        "outlook",
        "dropbox",
        "box",
        "s3",
        "azure_blob",
    ]
    assert [member.value for member in SourceItemType] == [
        "file",
        "folder",
        "message",
        "thread",
        "issue",
        "pull_request",
        "email",
        "page",
        "record",
    ]
    assert [member.value for member in PermissionPrincipalType] == [
        "user",
        "group",
        "domain",
        "organization",
        "public",
    ]
    assert [member.value for member in PermissionEffect] == ["allow", "deny"]
