"""Markdown content extractor implementation."""

from __future__ import annotations

from pathlib import Path

from domain.content_extraction.exceptions import (
    ContentParseError,
    ContentReadError,
    UnsupportedContentTypeError,
)
from domain.content_extraction.extractor import ContentExtractor
from domain.content_extraction.models import ExtractedContent, ExtractedSection


class MarkdownContentExtractor(ContentExtractor):
    """Extracts sectioned content from Markdown source files."""

    @property
    def supported_extensions(self) -> tuple[str, ...]:
        return (".md", ".markdown")

    @property
    def supported_mime_types(self) -> tuple[str, ...]:
        return ("text/markdown", "text/x-markdown")

    def extract(self, file_path: Path, *, mime_type: str | None = None) -> ExtractedContent:
        if not self.supports(file_path, mime_type=mime_type):
            raise UnsupportedContentTypeError("Unsupported content type for markdown extractor.")

        text = self._read_markdown(file_path)
        if not text.strip():
            raise ContentParseError("Markdown content is blank.")

        resolved_path = file_path.resolve()
        stat = resolved_path.stat()
        extension = resolved_path.suffix.lower()

        sections, heading_count = self._build_sections(text)
        title = self._extract_level_one_title(text) or resolved_path.stem

        return ExtractedContent(
            title=title,
            text=text,
            mime_type=(mime_type or "text/markdown"),
            sections=sections,
            tables=(),
            warnings=(),
            metadata={
                "file_name": resolved_path.name,
                "extension": extension,
                "size_bytes": stat.st_size,
                "encoding": "utf-8",
                "heading_count": heading_count,
            },
        )

    def _read_markdown(self, file_path: Path) -> str:
        try:
            return file_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ContentReadError("Failed to read markdown content.") from exc
        except UnicodeError as exc:
            raise ContentReadError("Failed to read markdown content.") from exc

    def _extract_level_one_title(self, text: str) -> str | None:
        for line in text.splitlines():
            heading = self._parse_heading(line)
            if heading is None:
                continue
            level, heading_text = heading
            if level == 1:
                return heading_text
        return None

    def _build_sections(self, text: str) -> tuple[tuple[ExtractedSection, ...], int]:
        lines = text.splitlines()
        sections: list[ExtractedSection] = []
        heading_count = 0

        current_heading: str | None = None
        current_body: list[str] = []

        def flush_section() -> None:
            nonlocal current_heading, current_body
            if not current_body:
                return
            body_text = "\n".join(current_body).strip()
            if not body_text:
                return
            sections.append(
                ExtractedSection(
                    heading=current_heading,
                    text=body_text,
                    section_index=len(sections),
                )
            )

        for line in lines:
            heading = self._parse_heading(line)
            if heading is None:
                current_body.append(line)
                continue

            heading_count += 1
            flush_section()
            current_heading = heading[1]
            current_body = []

        flush_section()

        if not sections:
            sections.append(ExtractedSection(heading=None, text=text.strip(), section_index=0))

        return tuple(sections), heading_count

    @staticmethod
    def _parse_heading(line: str) -> tuple[int, str] | None:
        stripped = line.lstrip()
        if not stripped.startswith("#"):
            return None

        marker_len = 0
        for char in stripped:
            if char == "#":
                marker_len += 1
            else:
                break

        if marker_len == 0 or marker_len > 6:
            return None

        remainder = stripped[marker_len:]
        if not remainder.startswith(" "):
            return None

        heading_text = remainder.strip()
        if not heading_text:
            return None

        return marker_len, heading_text
