"""Secure, text-based PDF content extraction implementation."""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path

from pypdf import PdfReader
from pypdf.errors import PdfReadError

from domain.content_extraction.exceptions import (
    ContentParseError,
    ContentReadError,
    ContentTooLargeError,
    EncryptedContentError,
    UnsupportedContentTypeError,
)
from domain.content_extraction.extractor import ContentExtractor
from domain.content_extraction.models import ExtractedContent, ExtractedSection


DEFAULT_MAX_INPUT_SIZE_BYTES = 25 * 1024 * 1024
DEFAULT_MAX_PAGE_COUNT = 500
DEFAULT_MAX_EXTRACTED_CHARACTERS = 5_000_000
DEFAULT_MIME_TYPE = "application/pdf"
PAGE_SEPARATOR = "\n\n"


class PdfContentExtractor(ContentExtractor):
    """Extract page text and safe standard metadata without executing PDF content."""

    def __init__(
        self,
        max_input_size_bytes: int = DEFAULT_MAX_INPUT_SIZE_BYTES,
        max_page_count: int = DEFAULT_MAX_PAGE_COUNT,
        max_extracted_characters: int = DEFAULT_MAX_EXTRACTED_CHARACTERS,
    ) -> None:
        for name, value in (
            ("max_input_size_bytes", max_input_size_bytes),
            ("max_page_count", max_page_count),
            ("max_extracted_characters", max_extracted_characters),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be greater than zero")
        self.max_input_size_bytes = max_input_size_bytes
        self.max_page_count = max_page_count
        self.max_extracted_characters = max_extracted_characters

    @property
    def supported_extensions(self) -> tuple[str, ...]:
        return (".pdf",)

    @property
    def supported_mime_types(self) -> tuple[str, ...]:
        return (DEFAULT_MIME_TYPE,)

    def extract(self, file_path: Path, *, mime_type: str | None = None) -> ExtractedContent:
        if not self.supports(file_path, mime_type=mime_type):
            raise UnsupportedContentTypeError("Unsupported content type for PDF extractor.")
        try:
            if file_path.stat().st_size > self.max_input_size_bytes:
                raise ContentTooLargeError("PDF content exceeds the configured maximum size.")
            file_bytes = file_path.read_bytes()
        except ContentTooLargeError:
            raise
        except OSError as exc:
            raise ContentReadError("Failed to read PDF content.") from exc
        return self.extract_bytes(file_bytes, file_name=file_path.name, mime_type=mime_type)

    def extract_bytes(
        self,
        file_bytes: bytes,
        *,
        file_name: str = "document.pdf",
        mime_type: str | None = None,
    ) -> ExtractedContent:
        """Extract PDF bytes in memory; no OCR, actions, attachments, or network access are used."""
        if not self.supports(Path(file_name), mime_type=mime_type):
            raise UnsupportedContentTypeError("Unsupported content type for PDF extractor.")
        if not isinstance(file_bytes, bytes) or not file_bytes:
            raise ContentParseError("PDF content is empty.")
        if len(file_bytes) > self.max_input_size_bytes:
            raise ContentTooLargeError("PDF content exceeds the configured maximum size.")

        try:
            reader = PdfReader(BytesIO(file_bytes), strict=True)
            if reader.is_encrypted:
                raise EncryptedContentError("Encrypted PDF content is not supported.")
            page_count = len(reader.pages)
        except EncryptedContentError:
            raise
        except (PdfReadError, ValueError, OSError, TypeError) as exc:
            raise ContentParseError("PDF content is invalid or corrupted.") from exc

        if page_count > self.max_page_count:
            raise ContentTooLargeError("PDF page count exceeds the configured maximum.")

        sections: list[ExtractedSection] = []
        page_texts: list[str] = []
        extracted_characters = 0
        try:
            for page_index, page in enumerate(reader.pages, start=1):
                page_text = _normalize_text(page.extract_text() or "")
                if not page_text:
                    continue
                extracted_characters += len(page_text)
                if extracted_characters > self.max_extracted_characters:
                    raise ContentTooLargeError("PDF extracted text exceeds the configured maximum.")
                page_texts.append(page_text)
                sections.append(
                    ExtractedSection(
                        heading=f"Page {page_index}",
                        text=page_text,
                        page_number=page_index,
                        section_index=len(sections),
                    )
                )
        except ContentTooLargeError:
            raise
        except (PdfReadError, ValueError, TypeError, OSError) as exc:
            raise ContentParseError("PDF text extraction failed.") from exc

        combined_text = PAGE_SEPARATOR.join(page_texts)
        if not combined_text:
            # ExtractedContent intentionally requires nonblank text, so a valid
            # image-only PDF cannot be represented as an empty result yet.
            raise ContentParseError("PDF contains no extractable text.")

        metadata = {
            "file_name": file_name,
            "extension": ".pdf",
            "encoding": "pdf-text",
            "size_bytes": len(file_bytes),
            "page_count": page_count,
            "text_page_count": len(sections),
        }
        _add_metadata(metadata, reader.metadata)
        return ExtractedContent(
            title=_metadata_text(reader.metadata, "/Title") or Path(file_name).stem,
            text=combined_text,
            mime_type=mime_type or DEFAULT_MIME_TYPE,
            sections=tuple(sections),
            tables=(),
            warnings=(),
            metadata=metadata,
        )


def _normalize_text(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n").strip()


def _metadata_text(metadata: object, key: str) -> str | None:
    value = metadata.get(key) if metadata is not None and hasattr(metadata, "get") else None
    return value.strip() if isinstance(value, str) and value.strip() else None


def _add_metadata(target: dict[str, object], metadata: object) -> None:
    if metadata is None or not hasattr(metadata, "get"):
        return
    allowed = {
        "/Title": "title",
        "/Author": "author",
        "/Subject": "subject",
        "/Creator": "creator",
        "/Producer": "producer",
        "/CreationDate": "creation_date",
        "/ModDate": "modification_date",
    }
    for source_key, target_key in allowed.items():
        value = metadata.get(source_key)
        if isinstance(value, datetime):
            target[target_key] = value
        elif isinstance(value, str) and value.strip():
            target[target_key] = value.strip()