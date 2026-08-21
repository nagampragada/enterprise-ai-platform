"""Capability contracts for external source connectors."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConnectorCapabilities:
    """Declares optional features a connector implementation can support."""

    supports_incremental_sync: bool
    supports_permissions: bool
    supports_folders: bool
    supports_deletions: bool
    supports_version_history: bool
    supports_webhooks: bool
    supports_content_download: bool
    supports_repository_discovery: bool = False
    supports_repository_selection: bool = False
    supports_bounded_content_reading: bool = False
    supports_staged_synchronization: bool = False
