from __future__ import annotations

import domain.connectors as contracts
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


def test_all_connector_specific_exceptions_inherit_from_connector_error() -> None:
    exception_types = [
        ConnectorAuthenticationError,
        ConnectorAuthorizationError,
        ConnectorRateLimitError,
        ConnectorUnavailableError,
        ConnectorItemNotFoundError,
        ConnectorCheckpointError,
        ConnectorContentError,
    ]

    for exc_type in exception_types:
        assert issubclass(exc_type, ConnectorError)


def test_exception_messages_are_preserved() -> None:
    message = "connector failed"

    exc = ConnectorUnavailableError(message)

    assert str(exc) == message


def test_package_exports_expose_all_public_contracts() -> None:
    expected_exports = {
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
    }

    assert set(contracts.__all__) == expected_exports
    for name in expected_exports:
        assert hasattr(contracts, name)
