from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import mimetypes
from pathlib import Path
from uuid import uuid4

import pytest

from domain.connectors.capabilities import ConnectorCapabilities
from domain.connectors.exceptions import (
    ConnectorAuthenticationError,
    ConnectorContentError,
    ConnectorItemNotFoundError,
)
from domain.connectors.models import ConnectorType, SourceItemType, SyncCheckpoint
from infrastructure.connectors.local.connector import LocalFolderConnector


def _make_connector(tmp_path: Path, **overrides) -> LocalFolderConnector:
    base = {
        "organization_id": uuid4(),
        "connector_id": uuid4(),
        "root_path": tmp_path,
    }
    base.update(overrides)
    return LocalFolderConnector(**base)


def _write_file(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def test_connector_type_is_local_folder(tmp_path: Path) -> None:
    connector = _make_connector(tmp_path)

    assert connector.connector_type == ConnectorType.LOCAL_FOLDER


def test_capabilities_match_contract_exactly(tmp_path: Path) -> None:
    connector = _make_connector(tmp_path)

    assert connector.capabilities == ConnectorCapabilities(
        supports_incremental_sync=False,
        supports_permissions=False,
        supports_folders=True,
        supports_deletions=True,
        supports_version_history=False,
        supports_webhooks=False,
        supports_content_download=True,
    )


def test_authenticate_succeeds_for_valid_readable_directory(tmp_path: Path) -> None:
    connector = _make_connector(tmp_path)

    connector.authenticate()


def test_authenticate_rejects_missing_root_path(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    connector = _make_connector(tmp_path, root_path=missing)

    with pytest.raises(ConnectorAuthenticationError):
        connector.authenticate()


def test_authenticate_rejects_root_path_that_is_file(tmp_path: Path) -> None:
    file_path = tmp_path / "root.txt"
    file_path.write_text("root")
    connector = _make_connector(tmp_path, root_path=file_path)

    with pytest.raises(ConnectorAuthenticationError):
        connector.authenticate()


def test_health_check_returns_true_for_valid_readable_directory(tmp_path: Path) -> None:
    connector = _make_connector(tmp_path)

    assert connector.health_check() is True


def test_health_check_returns_false_for_missing_path(tmp_path: Path) -> None:
    connector = _make_connector(tmp_path, root_path=tmp_path / "missing")

    assert connector.health_check() is False


def test_health_check_returns_false_when_root_path_is_file(tmp_path: Path) -> None:
    root_file = tmp_path / "root.txt"
    root_file.write_text("x")
    connector = _make_connector(tmp_path, root_path=root_file)

    assert connector.health_check() is False


def test_crawl_recursively_finds_supported_files(tmp_path: Path) -> None:
    _write_file(tmp_path / "docs" / "a.txt", b"hello")
    _write_file(tmp_path / "docs" / "b.md", b"world")
    connector = _make_connector(tmp_path)

    items = list(connector.crawl())

    external_ids = {item.external_id for item in items}
    assert external_ids == {"docs/a.txt", "docs/b.md"}


def test_crawl_ignores_unsupported_files(tmp_path: Path) -> None:
    _write_file(tmp_path / "keep.txt", b"ok")
    _write_file(tmp_path / "skip.exe", b"no")
    connector = _make_connector(tmp_path)

    items = list(connector.crawl())

    assert [item.external_id for item in items] == ["keep.txt"]


def test_crawl_does_not_return_directories(tmp_path: Path) -> None:
    (tmp_path / "folder").mkdir()
    _write_file(tmp_path / "folder" / "file.txt", b"ok")
    connector = _make_connector(tmp_path)

    items = list(connector.crawl())

    assert all(item.item_type == SourceItemType.FILE for item in items)
    assert all(not item.external_id.endswith("/") for item in items)


def test_crawl_returns_deterministic_ordering_by_relative_path(tmp_path: Path) -> None:
    _write_file(tmp_path / "z" / "z.txt", b"z")
    _write_file(tmp_path / "a" / "b.txt", b"b")
    _write_file(tmp_path / "a" / "a.txt", b"a")
    connector = _make_connector(tmp_path)

    items = list(connector.crawl())

    assert [item.external_id for item in items] == ["a/a.txt", "a/b.txt", "z/z.txt"]


def test_crawl_resumes_after_checkpoint_cursor(tmp_path: Path) -> None:
    _write_file(tmp_path / "a.txt", b"a")
    _write_file(tmp_path / "b.txt", b"b")
    connector = _make_connector(tmp_path)
    checkpoint = SyncCheckpoint(cursor="a.txt", last_synced_at=datetime.now(UTC))

    items = list(connector.crawl(checkpoint=checkpoint))

    assert [item.external_id for item in items] == ["b.txt"]


def test_source_item_fields_are_correct(tmp_path: Path) -> None:
    data = b"# alpha\n"
    file_path = tmp_path / "data.markdown"
    _write_file(file_path, data)
    connector = _make_connector(tmp_path)

    item = connector.fetch_item("data.markdown")

    assert item.organization_id == connector.organization_id
    assert item.connector_id == connector.connector_id
    assert item.external_id == "data.markdown"
    assert item.connector_type == ConnectorType.LOCAL_FOLDER
    assert item.item_type == SourceItemType.FILE
    assert item.title == "data.markdown"
    assert item.source_url == file_path.resolve().as_uri()
    assert item.mime_type == mimetypes.guess_type(file_path.name)[0]
    assert item.content is None
    assert item.metadata["relative_path"] == "data.markdown"
    assert item.metadata["extension"] == ".markdown"
    assert item.metadata["size_bytes"] == len(data)
    assert item.permissions == ()
    assert item.checksum is not None
    assert item.created_at is not None
    assert item.updated_at is not None
    assert item.deleted is False


def test_checksum_is_sha256_digest_of_file_bytes(tmp_path: Path) -> None:
    payload = b"checksum-me"
    _write_file(tmp_path / "file.txt", payload)
    connector = _make_connector(tmp_path)

    item = connector.fetch_item("file.txt")

    assert item.checksum == hashlib.sha256(payload).hexdigest()


def test_created_and_updated_are_timezone_aware(tmp_path: Path) -> None:
    _write_file(tmp_path / "file.txt", b"x")
    connector = _make_connector(tmp_path)

    item = connector.fetch_item("file.txt")

    assert item.created_at is not None
    assert item.updated_at is not None
    assert item.created_at.tzinfo is not None
    assert item.updated_at.tzinfo is not None
    assert item.created_at.tzinfo.utcoffset(item.created_at) is not None
    assert item.updated_at.tzinfo.utcoffset(item.updated_at) is not None


def test_fetch_item_returns_expected_supported_file(tmp_path: Path) -> None:
    _write_file(tmp_path / "docs" / "notes.md", b"# notes")
    connector = _make_connector(tmp_path)

    item = connector.fetch_item("docs/notes.md")

    assert item.external_id == "docs/notes.md"
    assert item.title == "notes.md"


def test_resolve_content_path_returns_only_validated_contained_file(tmp_path: Path) -> None:
    file_path = tmp_path / "docs" / "notes.md"
    _write_file(file_path, b"# notes")
    connector = _make_connector(tmp_path)

    assert connector.resolve_content_path("docs/notes.md") == file_path.resolve()

    with pytest.raises(ConnectorContentError):
        connector.resolve_content_path("../outside.md")


def test_fetch_item_rejects_missing_file(tmp_path: Path) -> None:
    connector = _make_connector(tmp_path)

    with pytest.raises(ConnectorItemNotFoundError):
        connector.fetch_item("missing.txt")


def test_fetch_item_rejects_unsupported_extension(tmp_path: Path) -> None:
    _write_file(tmp_path / "bad.exe", b"x")
    connector = _make_connector(tmp_path)

    with pytest.raises(ConnectorContentError):
        connector.fetch_item("bad.exe")


def test_fetch_item_rejects_parent_path_traversal(tmp_path: Path) -> None:
    connector = _make_connector(tmp_path)

    with pytest.raises(ConnectorContentError):
        connector.fetch_item("../outside.txt")


def test_fetch_item_rejects_absolute_path_outside_root(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside.txt"
    outside.write_bytes(b"x")
    connector = _make_connector(tmp_path)

    with pytest.raises(ConnectorContentError):
        connector.fetch_item(str(outside.resolve()))


def test_symlink_outside_root_is_rejected_or_safely_skipped(tmp_path: Path) -> None:
    outside_dir = tmp_path.parent / "outside-target"
    outside_dir.mkdir(exist_ok=True)
    outside_file = outside_dir / "secret.txt"
    outside_file.write_bytes(b"secret")

    inside_link = tmp_path / "linked.txt"
    try:
        inside_link.symlink_to(outside_file)
    except (OSError, NotImplementedError):
        pytest.skip("Symlink creation is unavailable on this operating system")

    connector = _make_connector(tmp_path)

    # fetch_item path should be rejected if symlink escapes root
    with pytest.raises(ConnectorContentError):
        connector.fetch_item("linked.txt")

    # crawl should skip escaped symlink item
    items = list(connector.crawl())
    assert all(item.external_id != "linked.txt" for item in items)


def test_fetch_permissions_returns_empty_tuple(tmp_path: Path) -> None:
    connector = _make_connector(tmp_path)

    assert connector.fetch_permissions("anything") == ()


def test_disconnect_is_safe_noop(tmp_path: Path) -> None:
    connector = _make_connector(tmp_path)

    assert connector.disconnect() is None


def test_allowed_extensions_can_be_customized(tmp_path: Path) -> None:
    _write_file(tmp_path / "keep.log", b"log")
    _write_file(tmp_path / "skip.txt", b"txt")
    connector = _make_connector(tmp_path, allowed_extensions=(".log",))

    items = list(connector.crawl())

    assert [item.external_id for item in items] == ["keep.log"]


def test_extension_matching_is_case_insensitive(tmp_path: Path) -> None:
    _write_file(tmp_path / "UPPER.TXT", b"x")
    connector = _make_connector(tmp_path)

    items = list(connector.crawl())

    assert [item.external_id for item in items] == ["UPPER.TXT"]


def test_crawl_does_not_expose_file_content(tmp_path: Path) -> None:
    _write_file(tmp_path / "secret.txt", b"top-secret")
    connector = _make_connector(tmp_path)

    item = list(connector.crawl())[0]

    assert item.content is None
