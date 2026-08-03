"""Reusable domain contracts for external content connectors."""

from domain.connectors.capabilities import ConnectorCapabilities
from domain.connectors.connector import Connector
from domain.connectors.exceptions import (
    ConnectorAuthenticationError,
    ConnectorAuthorizationError,
    ConnectorCheckpointError,
    ConnectorContentError,
    ConnectorError,
    ConnectorItemNotFoundError,
    ConnectorRateLimitError,
    ConnectorUnavailableError,
)
from domain.connectors.models import (
    ConnectorType,
    PermissionEffect,
    PermissionPrincipalType,
    SourceItem,
    SourceItemType,
    SourcePermission,
    SyncCheckpoint,
)


__all__ = [
    "Connector",
    "ConnectorAuthenticationError",
    "ConnectorAuthorizationError",
    "ConnectorCapabilities",
    "ConnectorCheckpointError",
    "ConnectorContentError",
    "ConnectorError",
    "ConnectorItemNotFoundError",
    "ConnectorRateLimitError",
    "ConnectorType",
    "ConnectorUnavailableError",
    "PermissionEffect",
    "PermissionPrincipalType",
    "SourceItem",
    "SourceItemType",
    "SourcePermission",
    "SyncCheckpoint",
]
