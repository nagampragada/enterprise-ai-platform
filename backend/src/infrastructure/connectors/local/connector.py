"""Local folder connector implementation."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import heapq
import mimetypes
import os
from pathlib import Path
from uuid import UUID

from domain.connectors.capabilities import ConnectorCapabilities
from domain.connectors.connector import Connector
from domain.connectors.exceptions import (
    ConnectorAuthenticationError,
    ConnectorContentError,
    ConnectorItemNotFoundError,
)
from domain.connectors.models import ConnectorType, SourceItem, SourceItemType, SourcePermission, SyncCheckpoint


@dataclass
class LocalFolderConnector(Connector):
    """Connector that scans files from a local directory tree."""

    organization_id: UUID
    connector_id: UUID
    root_path: Path
    allowed_extensions: tuple[str, ...] = (".pdf", ".docx", ".txt", ".md", ".markdown")

    def __post_init__(self) -> None:
        self.root_path = self.root_path.resolve()
        self.allowed_extensions = tuple(ext.lower() for ext in self.allowed_extensions)

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.LOCAL_FOLDER

    @property
    def capabilities(self) -> ConnectorCapabilities:
        return ConnectorCapabilities(
            supports_incremental_sync=False,
            supports_permissions=False,
            supports_folders=True,
            supports_deletions=True,
            supports_version_history=False,
            supports_webhooks=False,
            supports_content_download=True,
        )

    def authenticate(self) -> None:
        """Validates that the configured root path is accessible."""
        if not self.root_path.exists():
            raise ConnectorAuthenticationError("Local folder connector root path is unavailable.")
        if not self.root_path.is_dir():
            raise ConnectorAuthenticationError("Local folder connector root path is unavailable.")
        if not os.access(self.root_path, os.R_OK):
            raise ConnectorAuthenticationError("Local folder connector root path is unavailable.")

        try:
            # Force directory access check without consuming file data.
            next(self.root_path.iterdir(), None)
        except OSError as exc:
            raise ConnectorAuthenticationError("Local folder connector root path is unavailable.") from exc

    def disconnect(self) -> None:
        """No-op for filesystem-backed connector."""
        return None

    def crawl(self, checkpoint: SyncCheckpoint | None = None) -> Iterable[SourceItem]:
        """Recursively yields supported file items from the configured root path."""
        self.authenticate()
        after_key = checkpoint.cursor if checkpoint is not None else None
        return self._iter_source_items(after_key)

    def fetch_item(self, external_id: str) -> SourceItem:
        """Fetches one supported file item using its root-relative external id."""
        self.authenticate()

        file_path = self.resolve_content_path(external_id)
        return self._build_source_item(file_path)

    def resolve_content_path(self, external_id: str) -> Path:
        """Return a validated regular-file path contained by the configured root."""
        file_path = self._resolve_external_id_to_path(external_id)
        if file_path.is_symlink():
            raise ConnectorContentError("Symbolic links are not supported.")
        if not file_path.exists() or not file_path.is_file():
            raise ConnectorItemNotFoundError("Source item was not found.")
        if file_path.suffix.lower() not in self.allowed_extensions:
            raise ConnectorContentError("Unsupported source item extension.")
        return file_path

    def fetch_permissions(self, external_id: str) -> tuple[SourcePermission, ...]:
        """Permissions are not supported for local folder sources."""
        del external_id
        return ()

    def health_check(self) -> bool:
        """Returns health status without raising exceptions."""
        if not self.root_path.exists() or not self.root_path.is_dir():
            return False
        if not os.access(self.root_path, os.R_OK):
            return False

        try:
            next(self.root_path.iterdir(), None)
        except OSError:
            return False
        return True

    def _build_source_item(self, file_path: Path) -> SourceItem:
        if file_path.is_symlink():
            raise ConnectorContentError("Symbolic links are not supported.")
        resolved_path = file_path.resolve()
        if not self._is_within_root(resolved_path):
            raise ConnectorContentError("File path is outside connector root path.")

        if not resolved_path.exists() or not resolved_path.is_file():
            raise ConnectorItemNotFoundError("Source item was not found.")

        extension = resolved_path.suffix.lower()
        if extension not in self.allowed_extensions:
            raise ConnectorContentError("Unsupported source item extension.")

        stat = resolved_path.stat()
        relative_path = file_path.relative_to(self.root_path).as_posix()
        checksum = self._compute_sha256(resolved_path)
        mime_type, _ = mimetypes.guess_type(resolved_path.name)

        return SourceItem(
            organization_id=self.organization_id,
            connector_id=self.connector_id,
            external_id=relative_path,
            connector_type=self.connector_type,
            item_type=SourceItemType.FILE,
            title=resolved_path.name,
            source_url=resolved_path.as_uri(),
            mime_type=mime_type,
            content=None,
            metadata={
                "relative_path": relative_path,
                "extension": extension,
                "size_bytes": stat.st_size,
            },
            permissions=(),
            checksum=checksum,
            created_at=datetime.fromtimestamp(stat.st_ctime, tz=UTC),
            updated_at=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
            deleted=False,
        )

    def _resolve_external_id_to_path(self, external_id: str) -> Path:
        if not external_id.strip():
            raise ConnectorItemNotFoundError("Source item was not found.")

        relative_path = Path(external_id)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ConnectorContentError("Invalid external id.")

        candidate = (self.root_path / relative_path).resolve()
        if not self._is_within_root(candidate):
            raise ConnectorContentError("Invalid external id.")

        return candidate

    def _is_within_root(self, path: Path) -> bool:
        try:
            path.relative_to(self.root_path)
        except ValueError:
            return False
        return True

    def _iter_source_items(self, after_key: str | None) -> Iterable[SourceItem]:
        pending: list[tuple[str, Path, bool]] = [("", self.root_path, True)]
        while pending:
            relative_key, path, is_directory = heapq.heappop(pending)
            if is_directory:
                try:
                    entries = tuple(path.iterdir())
                except OSError as exc:
                    raise ConnectorContentError("Local folder discovery failed.") from exc
                for entry in entries:
                    if entry.is_symlink():
                        continue
                    relative_path = entry.relative_to(self.root_path).as_posix()
                    try:
                        entry_is_directory = entry.is_dir()
                    except OSError as exc:
                        raise ConnectorContentError("Local folder discovery failed.") from exc
                    heapq.heappush(
                        pending,
                        (relative_path + "/" if entry_is_directory else relative_path, entry, entry_is_directory),
                    )
                continue
            if after_key is not None and relative_key <= after_key:
                continue
            try:
                yield self._build_source_item(path)
            except (ConnectorContentError, ConnectorItemNotFoundError):
                continue

    @staticmethod
    def _compute_sha256(file_path: Path) -> str:
        digest = hashlib.sha256()
        with file_path.open("rb") as file_handle:
            for chunk in iter(lambda: file_handle.read(8192), b""):
                digest.update(chunk)
        return digest.hexdigest()
