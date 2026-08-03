"""Abstract contracts for content extractor implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from domain.content_extraction.models import ExtractedContent


class ContentExtractor(ABC):
    """Base interface for extracting structured content from files."""

    @property
    @abstractmethod
    def supported_extensions(self) -> tuple[str, ...]:
        """File extensions supported by this extractor."""

    @property
    @abstractmethod
    def supported_mime_types(self) -> tuple[str, ...]:
        """MIME types supported by this extractor."""

    @abstractmethod
    def extract(
        self,
        file_path: Path,
        *,
        mime_type: str | None = None,
    ) -> ExtractedContent:
        """Extracts normalized content from a file path."""

    def supports(self, file_path: Path, mime_type: str | None = None) -> bool:
        """Returns whether the extractor supports a file by extension or MIME type."""
        extension = file_path.suffix.lower()
        supported_extensions = {ext.lower() for ext in self.supported_extensions}
        if extension and extension in supported_extensions:
            return True

        if mime_type is None:
            return False

        normalized_mime = mime_type.strip().lower()
        supported_mime_types = {item.lower() for item in self.supported_mime_types}
        return bool(normalized_mime) and normalized_mime in supported_mime_types
