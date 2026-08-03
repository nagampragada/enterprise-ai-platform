"""Plain-text content extractor implementation."""

from __future__ import annotations

from pathlib import Path

from domain.content_extraction.exceptions import (
    ContentParseError,
    ContentReadError,
    UnsupportedContentTypeError,
)
from domain.content_extraction.extractor import ContentExtractor
from domain.content_extraction.models import ExtractedContent, ExtractedSection


class TextContentExtractor(ContentExtractor):
    """Extracts normalized content from plain UTF-8 text files."""

    @property
    def supported_extensions(self) -> tuple[str, ...]:
        return (".txt",)

    @property
    def supported_mime_types(self) -> tuple[str, ...]:
        return ("text/plain",)

    def extract(self, file_path: Path, *, mime_type: str | None = None) -> ExtractedContent:
        if not self.supports(file_path, mime_type=mime_type):
            raise UnsupportedContentTypeError("Unsupported content type for text extractor.")

        text, encoding = self._read_text(file_path)
        if not text.strip():
            raise ContentParseError("Text content is blank.")

        resolved_path = file_path.resolve()
        stat = resolved_path.stat()
        extension = resolved_path.suffix.lower()

        return ExtractedContent(
            title=resolved_path.stem,
            text=text,
            mime_type=(mime_type or "text/plain"),
            sections=(ExtractedSection(heading=None, text=text, section_index=0),),
            tables=(),
            warnings=(),
            metadata={
                "file_name": resolved_path.name,
                "extension": extension,
                "size_bytes": stat.st_size,
                "encoding": encoding,
            },
        )

    def _read_text(self, file_path: Path) -> tuple[str, str]:
        try:
            return file_path.read_text(encoding="utf-8"), "utf-8"
        except UnicodeDecodeError:
            try:
                return file_path.read_text(encoding="utf-8-sig"), "utf-8-sig"
            except (OSError, UnicodeError) as exc:
                raise ContentReadError("Failed to read text content.") from exc
        except OSError as exc:
            raise ContentReadError("Failed to read text content.") from exc
