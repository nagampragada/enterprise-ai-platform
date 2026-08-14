"""In-memory DOCX content extraction implementation."""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from docx import Document
from docx.document import Document as DocumentType
from docx.table import Table
from docx.text.paragraph import Paragraph
from docx.oxml.ns import qn
from docx.opc.exceptions import PackageNotFoundError

from domain.content_extraction.exceptions import (
    ContentParseError,
    ContentReadError,
    ContentTooLargeError,
    EncryptedContentError,
    UnsupportedContentTypeError,
)
from domain.content_extraction.extractor import ContentExtractor
from domain.content_extraction.models import ExtractedContent, ExtractedSection, ExtractedTable


DEFAULT_MAX_INPUT_SIZE_BYTES = 25 * 1024 * 1024
DEFAULT_MIME_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
_TABLE_CELL_SEPARATOR = " | "
_TABLE_ROW_SEPARATOR = "\n"
_BODY_NAMESPACE = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"


class DocxContentExtractor(ContentExtractor):
    """Extract body paragraphs and tables without executing embedded content."""

    def __init__(self, max_input_size_bytes: int = DEFAULT_MAX_INPUT_SIZE_BYTES) -> None:
        if max_input_size_bytes < 1:
            raise ValueError("max_input_size_bytes must be greater than zero")
        self.max_input_size_bytes = max_input_size_bytes

    @property
    def supported_extensions(self) -> tuple[str, ...]:
        return (".docx",)

    @property
    def supported_mime_types(self) -> tuple[str, ...]:
        return (DEFAULT_MIME_TYPE,)

    def extract(self, file_path: Path, *, mime_type: str | None = None) -> ExtractedContent:
        if not self.supports(file_path, mime_type=mime_type):
            raise UnsupportedContentTypeError("Unsupported content type for DOCX extractor.")

        try:
            if file_path.stat().st_size > self.max_input_size_bytes:
                raise ContentTooLargeError("DOCX content exceeds the configured maximum size.")
            file_bytes = file_path.read_bytes()
        except ContentTooLargeError:
            raise
        except OSError as exc:
            raise ContentReadError("Failed to read DOCX content.") from exc

        return self.extract_bytes(
            file_bytes,
            file_name=file_path.name,
            mime_type=mime_type,
        )

    def extract_bytes(
        self,
        file_bytes: bytes,
        *,
        file_name: str = "document.docx",
        mime_type: str | None = None,
    ) -> ExtractedContent:
        """Extract DOCX bytes without persisting them to disk."""
        if not self.supports(Path(file_name), mime_type=mime_type):
            raise UnsupportedContentTypeError("Unsupported content type for DOCX extractor.")
        if not isinstance(file_bytes, bytes) or not file_bytes:
            raise ContentParseError("DOCX content is empty.")
        if len(file_bytes) > self.max_input_size_bytes:
            raise ContentTooLargeError("DOCX content exceeds the configured maximum size.")

        try:
            with ZipFile(BytesIO(file_bytes)) as archive:
                if any(info.flag_bits & 0x1 for info in archive.infolist()):
                    raise EncryptedContentError("Encrypted DOCX content is not supported.")
                if "[Content_Types].xml" not in archive.namelist() or "word/document.xml" not in archive.namelist():
                    raise ContentParseError("DOCX package is missing required document parts.")
            document = Document(BytesIO(file_bytes))
        except EncryptedContentError:
            raise
        except (BadZipFile, PackageNotFoundError, ValueError, OSError) as exc:
            raise ContentParseError("DOCX content is invalid or corrupted.") from exc

        return self._build_result(document, file_name=file_name, mime_type=mime_type or DEFAULT_MIME_TYPE)

    def _build_result(self, document: DocumentType, *, file_name: str, mime_type: str) -> ExtractedContent:
        body_parts: list[str] = []
        sections: list[ExtractedSection] = []
        tables: list[ExtractedTable] = []

        for body_child in document.element.body.iterchildren():
            if body_child.tag == qn("w:p"):
                paragraph = Paragraph(body_child, document)
                text = _normalize_text(paragraph.text)
                if not text.strip():
                    continue
                body_parts.append(text)
                sections.append(
                    ExtractedSection(
                        heading=_paragraph_heading(paragraph),
                        text=text,
                        section_index=len(sections),
                    )
                )
            elif body_child.tag == qn("w:tbl"):
                table = Table(body_child, document)
                extracted_table, table_text = _extract_table(table, len(tables))
                if extracted_table is None:
                    continue
                tables.append(extracted_table)
                body_parts.append(table_text)

        normalized_text = "\n\n".join(part for part in body_parts if part.strip()).strip()
        if not normalized_text:
            raise ContentParseError("DOCX document contains no extractable body content.")

        properties = document.core_properties
        title = _clean_optional(properties.title) or Path(file_name).stem
        metadata: dict[str, object] = {
            "file_name": file_name,
            "extension": ".docx",
            "encoding": "xml-utf-8",
        }
        _add_property(metadata, "author", properties.author)
        _add_property(metadata, "subject", properties.subject)
        _add_property(metadata, "created_at", properties.created)
        _add_property(metadata, "modified_at", properties.modified)

        return ExtractedContent(
            title=title,
            text=normalized_text,
            mime_type=mime_type,
            sections=tuple(sections),
            tables=tuple(tables),
            warnings=(),
            metadata=metadata,
        )


def _extract_table(table: Table, table_index: int) -> tuple[ExtractedTable | None, str]:
    rows: list[tuple[str, ...]] = []
    rendered_rows: list[str] = []
    for row in table.rows:
        values: list[str] = []
        seen_cells: set[object] = set()
        for cell in row.cells:
            cell_id = cell._tc
            value = _normalize_text(cell.text)
            if cell_id in seen_cells:
                continue
            seen_cells.add(cell_id)
            if value:
                values.append(value)
        if values:
            row_values = tuple(values)
            rows.append(row_values)
            rendered_rows.append(_TABLE_CELL_SEPARATOR.join(row_values))
    if not rows:
        return None, ""
    return ExtractedTable(rows=tuple(rows), table_index=table_index), _TABLE_ROW_SEPARATOR.join(rendered_rows)


def _paragraph_heading(paragraph: Paragraph) -> str | None:
    style_name = paragraph.style.name if paragraph.style is not None else ""
    if style_name.lower().startswith("heading"):
        return paragraph.text.strip()
    return None


def _normalize_text(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n").strip()


def _clean_optional(value: str | None) -> str | None:
    return value.strip() if value and value.strip() else None


def _add_property(metadata: dict[str, object], key: str, value: str | datetime | None) -> None:
    if isinstance(value, str):
        cleaned = _clean_optional(value)
        if cleaned:
            metadata[key] = cleaned
    elif isinstance(value, datetime):
        metadata[key] = value