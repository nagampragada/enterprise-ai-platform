"""Domain exceptions for connector contract operations."""

from __future__ import annotations


class ConnectorError(Exception):
    """Base connector error for all connector-related failures."""


class ConnectorAuthenticationError(ConnectorError):
    """Raised when connector authentication fails."""


class ConnectorAuthorizationError(ConnectorError):
    """Raised when connector authorization is denied."""


class ConnectorRateLimitError(ConnectorError):
    """Raised when connector API rate limits are exceeded."""


class ConnectorUnavailableError(ConnectorError):
    """Raised when a connector backend is temporarily unavailable."""


class ConnectorItemNotFoundError(ConnectorError):
    """Raised when a source item cannot be found."""


class ConnectorCheckpointError(ConnectorError):
    """Raised when a sync checkpoint is invalid or unusable."""


class ConnectorContentError(ConnectorError):
    """Raised when connector content cannot be retrieved or parsed."""
