"""Strict and redacted connector-management API contracts."""

from __future__ import annotations

import base64
import json
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Slug = str


class CreateConnectorRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connector_type: Literal["local_folder", "github"]
    display_name: str = Field(min_length=1, max_length=255)
    slug: str = Field(min_length=1, max_length=255, pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
    status: Literal["active", "draft"] | None = None

    @field_validator("display_name")
    @classmethod
    def _name_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("display_name must not be blank")
        return value.strip()

    @model_validator(mode="after")
    def _provider_lifecycle_is_server_owned(self) -> "CreateConnectorRequest":
        expected = "active" if self.connector_type == "local_folder" else "draft"
        if self.status is not None and self.status != expected:
            raise ValueError("connector status is incompatible with connector_type")
        return self


class GitHubInstallationInitiationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    installation_url: str
    expires_at: datetime


class GitHubInstallationCompletionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    status: Literal["connected"]


class GitHubInstallationStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    connected: bool
    account_login: str | None = None
    account_type: Literal["Organization", "User"] | None = None
    external_account_id: str | None = None
    repository_selection: Literal["all", "selected"] | None = None
    credential_status: Literal["active", "expired", "revoked", "invalid"] | None = None
    provider_created_at: datetime | None = None
    provider_updated_at: datetime | None = None
    last_verified_at: datetime | None = None


class GitHubRepositoryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    repository_id: int
    name: str
    full_name: str
    owner_login: str
    private: bool
    visibility: Literal["public", "private", "internal"] | None
    archived: bool
    disabled: bool
    default_branch: str | None
    html_url: str
    updated_at: datetime | None


class GitHubRepositoryPageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    items: tuple[GitHubRepositoryResponse, ...]
    page: int
    page_size: int
    has_next: bool
    total_count: int | None


class LocalFolderScopeConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    root_path: str = Field(min_length=1, max_length=1024)
    follow_symlinks: Literal[False] = False


class CreateConnectorScopeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    knowledge_space_id: UUID
    display_name: str = Field(min_length=1, max_length=255)
    slug: str = Field(min_length=1, max_length=255, pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
    access_mode: Literal["platform_managed"] = "platform_managed"
    status: Literal["active"] = "active"
    configuration: LocalFolderScopeConfiguration

    @field_validator("display_name")
    @classmethod
    def _name_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("display_name must not be blank")
        return value.strip()


class ConnectorCapabilitiesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    supports_incremental_sync: bool
    supports_permissions: bool
    supports_folders: bool
    supports_deletions: bool
    supports_version_history: bool
    supports_webhooks: bool
    supports_content_download: bool
    supports_repository_discovery: bool = False


class ConnectorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    connector_id: UUID
    connector_type: str
    display_name: str
    slug: str
    status: str
    acl_support: str
    capabilities: ConnectorCapabilitiesResponse
    created_at: datetime
    updated_at: datetime
    last_validated_at: datetime | None
    archived_at: datetime | None


class ConnectorScopeResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    scope_id: UUID
    connector_id: UUID
    knowledge_space_id: UUID
    display_name: str
    slug: str
    scope_type: str
    access_mode: str
    status: str
    created_at: datetime
    updated_at: datetime
    last_validated_at: datetime | None
    removed_at: datetime | None


class SyncJobResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    job_id: UUID
    connector_id: UUID
    scope_id: UUID
    mode: str
    trigger_type: str
    status: str
    attempt_count: int
    max_attempts: int
    next_attempt_at: datetime | None
    cancellation_requested: bool
    completed_at: datetime | None
    last_error_category: str | None
    last_error_code: str | None
    created_at: datetime


class EnqueueSyncJobResponse(SyncJobResponse):
    coalesced: bool


class EnqueueSyncJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class PutSyncScheduleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    interval_seconds: int = Field(ge=900, le=2_592_000)
    first_run_at: datetime | None = None

    @field_validator("first_run_at")
    @classmethod
    def _first_run_must_be_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("first_run_at must be timezone-aware")
        return value


class PatchSyncScheduleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    action: Literal["pause", "resume"]


class SyncScheduleResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schedule_id: UUID
    connector_id: UUID
    scope_id: UUID
    status: Literal["active", "paused"]
    interval_seconds: int
    next_run_at: datetime
    last_due_at: datetime | None
    last_enqueued_at: datetime | None
    last_job_id: UUID | None
    pause_reason_code: str | None
    paused_at: datetime | None
    created_at: datetime
    updated_at: datetime


class ConnectorPageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    items: tuple[ConnectorResponse, ...]
    limit: int
    has_more: bool
    next_cursor: str | None


class ConnectorScopePageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    items: tuple[ConnectorScopeResponse, ...]
    limit: int
    has_more: bool
    next_cursor: str | None


class SyncJobPageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    items: tuple[SyncJobResponse, ...]
    limit: int
    has_more: bool
    next_cursor: str | None


def encode_cursor(kind: str, created_at: datetime, row_id: UUID) -> str:
    payload = json.dumps(
        {"v": 1, "k": kind, "t": created_at.isoformat(), "i": str(row_id)},
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_cursor(value: str | None, expected_kind: str) -> tuple[datetime, UUID] | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ValueError("cursor is invalid")
    try:
        padded = value + "=" * (-len(value) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
        if set(payload) != {"v", "k", "t", "i"}:
            raise ValueError
        if payload["v"] != 1 or payload["k"] != expected_kind:
            raise ValueError
        created_at = datetime.fromisoformat(payload["t"])
        if created_at.tzinfo is None or created_at.utcoffset() is None:
            raise ValueError
        return created_at, UUID(payload["i"])
    except (ValueError, TypeError, KeyError, json.JSONDecodeError, UnicodeError) as exc:
        raise ValueError("cursor is invalid") from exc
