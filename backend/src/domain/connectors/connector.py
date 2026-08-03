"""Abstract connector contract for external source integrations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from domain.connectors.capabilities import ConnectorCapabilities
from domain.connectors.models import ConnectorType, SourceItem, SourcePermission, SyncCheckpoint


class Connector(ABC):
    """Base contract that all connector implementations must satisfy."""

    @property
    @abstractmethod
    def connector_type(self) -> ConnectorType:
        """Returns the concrete connector type."""

    @property
    @abstractmethod
    def capabilities(self) -> ConnectorCapabilities:
        """Returns supported connector capabilities."""

    @abstractmethod
    def authenticate(self) -> None:
        """Validates connector access for upcoming operations."""

    @abstractmethod
    def disconnect(self) -> None:
        """Releases connector resources if needed."""

    @abstractmethod
    def crawl(self, checkpoint: SyncCheckpoint | None = None) -> Iterable[SourceItem]:
        """Enumerates source items, optionally from a checkpoint."""

    @abstractmethod
    def fetch_item(self, external_id: str) -> SourceItem:
        """Fetches a single source item by external identifier."""

    @abstractmethod
    def fetch_permissions(self, external_id: str) -> tuple[SourcePermission, ...]:
        """Fetches permission entries for a source item."""

    @abstractmethod
    def health_check(self) -> bool:
        """Returns whether the connector backend appears healthy."""
