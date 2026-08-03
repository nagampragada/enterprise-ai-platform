from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from domain.connectors.capabilities import ConnectorCapabilities
from domain.connectors.connector import Connector
from domain.connectors.models import ConnectorType, SourceItem, SourceItemType, SyncCheckpoint


@dataclass
class DummyConnector(Connector):
    _health: bool = True

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.GITHUB

    @property
    def capabilities(self) -> ConnectorCapabilities:
        return ConnectorCapabilities(
            supports_incremental_sync=True,
            supports_permissions=True,
            supports_folders=True,
            supports_deletions=True,
            supports_version_history=False,
            supports_webhooks=False,
            supports_content_download=True,
        )

    def authenticate(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def crawl(self, checkpoint: SyncCheckpoint | None = None):
        del checkpoint
        return [
            SourceItem(
                organization_id=uuid4(),
                connector_id=uuid4(),
                external_id="item-1",
                connector_type=self.connector_type,
                item_type=SourceItemType.FILE,
                title="Readme",
                created_at=datetime.now(UTC),
            )
        ]

    def fetch_item(self, external_id: str) -> SourceItem:
        return SourceItem(
            organization_id=uuid4(),
            connector_id=uuid4(),
            external_id=external_id,
            connector_type=self.connector_type,
            item_type=SourceItemType.FILE,
            title="Fetched",
            created_at=datetime.now(UTC),
        )

    def fetch_permissions(self, external_id: str):
        del external_id
        return ()

    def health_check(self) -> bool:
        return self._health


def test_connector_cannot_be_instantiated_directly() -> None:
    with pytest.raises(TypeError):
        Connector()


def test_minimal_concrete_test_connector_implements_all_abstract_members() -> None:
    connector = DummyConnector()

    assert isinstance(connector, Connector)


def test_concrete_test_connector_exposes_connector_type() -> None:
    connector = DummyConnector()

    assert connector.connector_type == ConnectorType.GITHUB


def test_concrete_test_connector_exposes_capabilities() -> None:
    connector = DummyConnector()

    assert isinstance(connector.capabilities, ConnectorCapabilities)
    assert connector.capabilities.supports_permissions is True


def test_crawl_accepts_optional_checkpoint() -> None:
    connector = DummyConnector()
    checkpoint = SyncCheckpoint(cursor="next", last_synced_at=datetime.now(UTC))

    with_checkpoint = connector.crawl(checkpoint)
    without_checkpoint = connector.crawl()

    assert len(with_checkpoint) == 1
    assert len(without_checkpoint) == 1


def test_fetch_item_returns_source_item() -> None:
    connector = DummyConnector()

    item = connector.fetch_item("ext-42")

    assert isinstance(item, SourceItem)
    assert item.external_id == "ext-42"


def test_fetch_permissions_returns_tuple() -> None:
    connector = DummyConnector()

    permissions = connector.fetch_permissions("ext-42")

    assert isinstance(permissions, tuple)


def test_health_check_returns_bool() -> None:
    healthy = DummyConnector(_health=True)
    unhealthy = DummyConnector(_health=False)

    assert healthy.health_check() is True
    assert unhealthy.health_check() is False
