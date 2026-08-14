"""Explicit content-extractor registry and Version 1 composition factory."""

from __future__ import annotations

from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from domain.content_extraction.exceptions import UnsupportedContentTypeError
from domain.content_extraction.extractor import ContentExtractor
from domain.content_extraction.models import ExtractedContent
from infrastructure.content_extraction.docx import DocxContentExtractor
from infrastructure.content_extraction.markdown import MarkdownContentExtractor
from infrastructure.content_extraction.pdf import PdfContentExtractor
from infrastructure.content_extraction.text import TextContentExtractor


class ContentExtractorRegistry:
    """Instance-scoped mapping from normalized extensions to extractors."""

    def __init__(self) -> None:
        self._extractors: dict[str, ContentExtractor] = {}

    @property
    def extractors(self) -> Mapping[str, ContentExtractor]:
        """Expose a read-only view of the registry mapping."""
        return MappingProxyType(self._extractors)

    def register(self, extension: str, extractor: ContentExtractor) -> None:
        """Register one extractor without replacing an existing mapping."""
        normalized_extension = normalize_extension(extension)
        if not isinstance(extractor, ContentExtractor):
            raise TypeError("extractor must implement ContentExtractor")
        if normalized_extension in self._extractors:
            raise ValueError(f"extractor already registered for {normalized_extension}")
        self._extractors[normalized_extension] = extractor

    def get_by_extension(self, extension: str) -> ContentExtractor:
        """Return the extractor registered for an extension."""
        normalized_extension = normalize_extension(extension)
        try:
            return self._extractors[normalized_extension]
        except KeyError as exc:
            raise UnsupportedContentTypeError(
                f"No content extractor registered for {normalized_extension}"
            ) from exc

    def get_for_path(self, file_path: Path) -> ContentExtractor:
        """Resolve an extractor from a path extension without reading the path."""
        extension = file_path.suffix
        if not extension:
            raise UnsupportedContentTypeError("File path has no supported extension")
        return self.get_by_extension(extension)

    def extract(self, file_path: Path) -> ExtractedContent:
        """Delegate extraction to the path-selected extractor exactly once."""
        return self.get_for_path(file_path).extract(file_path)


def normalize_extension(extension: str) -> str:
    """Normalize a registration extension to a lowercase leading-dot key."""
    if not isinstance(extension, str):
        raise TypeError("extension must be a string")
    normalized = extension.strip().lower()
    if not normalized:
        raise ValueError("extension must not be blank")
    if "/" in normalized or "\\" in normalized:
        raise ValueError("extension must not contain path separators")
    if not normalized.startswith("."):
        normalized = "." + normalized
    if normalized == "." or normalized[1:].strip() == "":
        raise ValueError("extension must contain a suffix")
    return normalized


def create_default_content_extractor_registry() -> ContentExtractorRegistry:
    """Create a new independent Version 1 extractor registry."""
    registry = ContentExtractorRegistry()
    markdown = MarkdownContentExtractor()
    registry.register(".txt", TextContentExtractor())
    registry.register(".md", markdown)
    registry.register("markdown", markdown)
    registry.register(".docx", DocxContentExtractor())
    registry.register("pdf", PdfContentExtractor())
    return registry