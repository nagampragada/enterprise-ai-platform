"""Core domain models for reusable connector contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any
from uuid import UUID


class ConnectorType(str, Enum):
    """Supported connector providers."""

    LOCAL_FOLDER = "local_folder"
    GOOGLE_DRIVE = "google_drive"
    SHAREPOINT = "sharepoint"
    ONEDRIVE = "onedrive"
    SLACK = "slack"
    JIRA = "jira"
    CONFLUENCE = "confluence"
    GITHUB = "github"
    GMAIL = "gmail"
    OUTLOOK = "outlook"
    DROPBOX = "dropbox"
    BOX = "box"
    S3 = "s3"
    AZURE_BLOB = "azure_blob"


class SourceItemType(str, Enum):
    """Logical source item categories."""

    FILE = "file"
    FOLDER = "folder"
    MESSAGE = "message"
    THREAD = "thread"
    ISSUE = "issue"
    PULL_REQUEST = "pull_request"
    EMAIL = "email"
    PAGE = "page"
    RECORD = "record"


class PermissionPrincipalType(str, Enum):
    """Permission principal categories."""

    USER = "user"
    GROUP = "group"
    DOMAIN = "domain"
    ORGANIZATION = "organization"
    PUBLIC = "public"


class PermissionEffect(str, Enum):
    """Permission decision effects."""

    ALLOW = "allow"
    DENY = "deny"


@dataclass(frozen=True)
class SourcePermission:
    """Permission entry for a source item."""

    principal_id: str
    principal_type: PermissionPrincipalType
    effect: PermissionEffect
    role: str | None = None


@dataclass(frozen=True)
class SyncCheckpoint:
    """Opaque checkpoint state used for incremental sync."""

    cursor: str | None = None
    last_synced_at: datetime | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.last_synced_at is not None:
            _validate_timezone_aware(self.last_synced_at, "last_synced_at")
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))


@dataclass(frozen=True)
class SourceItem:
    """Domain representation of content discovered by a connector."""

    organization_id: UUID
    connector_id: UUID
    external_id: str
    connector_type: ConnectorType
    item_type: SourceItemType
    title: str
    source_url: str | None = None
    mime_type: str | None = None
    content: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    permissions: tuple[SourcePermission, ...] = field(default_factory=tuple)
    checksum: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    deleted: bool = False

    def __post_init__(self) -> None:
        if not self.external_id.strip():
            raise ValueError("external_id must not be blank")
        if not self.title.strip():
            raise ValueError("title must not be blank")

        if self.created_at is not None:
            _validate_timezone_aware(self.created_at, "created_at")
        if self.updated_at is not None:
            _validate_timezone_aware(self.updated_at, "updated_at")

        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))
        object.__setattr__(self, "permissions", tuple(self.permissions))


def _freeze_mapping(mapping: Mapping[str, Any]) -> MappingProxyType[str, Any]:
    return MappingProxyType(dict(mapping))


def _validate_timezone_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{field_name} must be timezone-aware")
