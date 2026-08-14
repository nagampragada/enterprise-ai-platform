from __future__ import annotations

from io import BytesIO
from pathlib import Path
from unittest.mock import Mock
from zipfile import ZipFile

import pytest
from reportlab.pdfgen import canvas

from domain.content_extraction.exceptions import (
    ContentParseError,
    ContentTooLargeError,
    EncryptedContentError,
)
from infrastructure.content_extraction.pdf.extractor import PdfContentExtractor


def _pdf_bytes(*pages: str, title: str | None = None) -> bytes:
    output = BytesIO()
    pdf = canvas.Canvas(output)
    if title:
        pdf.setTitle(title)
        pdf.setAuthor("Test Author")
        pdf.setSubject("Test Subject")
        pdf.setCreator("Test Creator")
        pdf.setProducer("Test Producer")
    for text in pages:
        if text:
            for line_index, line in enumerate(text.split("\n")):
                pdf.drawString(72, 750 - line_index * 16, line)
        pdf.showPage()
    pdf.save()
    return output.getvalue()


def test_simple_pdf_pages_sections_and_metadata() -> None:
    result = PdfContentExtractor().extract_bytes(_pdf_bytes("first page", title="Report"), file_name="report.pdf")
    assert result.title == "Report"
    assert result.text == "first page"
    assert result.sections[0].heading == "Page 1"
    assert result.sections[0].page_number == 1
    assert result.metadata["page_count"] == 1
    assert result.metadata["author"] == "Test Author"
    assert result.metadata["subject"] == "Test Subject"
    assert result.metadata["creator"] == "Test Creator"
    assert result.metadata["producer"] == "Test Producer"


def test_multiple_pages_preserve_order_and_skip_blank_pages() -> None:
    result = PdfContentExtractor().extract_bytes(_pdf_bytes("first", "", "third"))
    assert result.text == "first\n\nthird"
    assert [section.page_number for section in result.sections] == [1, 3]
    assert result.metadata["page_count"] == 3
    assert result.metadata["text_page_count"] == 2


def test_unicode_and_line_endings_are_normalized() -> None:
    result = PdfContentExtractor().extract_bytes(_pdf_bytes("café\nnaïve\rvalue"))
    assert "café" in result.text
    assert "naïve" in result.text
    assert "\r" not in result.text


def test_empty_non_pdf_and_truncated_pdf_are_rejected() -> None:
    extractor = PdfContentExtractor()
    with pytest.raises(ContentParseError):
        extractor.extract_bytes(b"")
    with pytest.raises(ContentParseError):
        extractor.extract_bytes(b"not a PDF")
    with pytest.raises(ContentParseError):
        extractor.extract_bytes(_pdf_bytes("valid")[:80])


def test_encrypted_pdf_is_rejected_without_password_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    reader = Mock(is_encrypted=True)
    monkeypatch.setattr("infrastructure.content_extraction.pdf.extractor.PdfReader", lambda *args, **kwargs: reader)
    with pytest.raises(EncryptedContentError):
        PdfContentExtractor().extract_bytes(b"%PDF-encrypted")


def test_limits_and_configuration_are_enforced(tmp_path: Path) -> None:
    payload = _pdf_bytes("page one", "page two")
    with pytest.raises(ContentTooLargeError):
        PdfContentExtractor(max_input_size_bytes=len(payload) - 1).extract_bytes(payload)
    with pytest.raises(ContentTooLargeError):
        PdfContentExtractor(max_page_count=1).extract_bytes(payload)
    with pytest.raises(ContentTooLargeError):
        PdfContentExtractor(max_extracted_characters=3).extract_bytes(_pdf_bytes("long text"))
    with pytest.raises(ValueError):
        PdfContentExtractor(max_page_count=0)

    file_path = tmp_path / "large.pdf"
    file_path.write_bytes(b"x" * 11)
    with pytest.raises(ContentTooLargeError):
        PdfContentExtractor(max_input_size_bytes=10).extract(file_path)


def test_path_contract_determinism_and_no_persistent_write(tmp_path: Path) -> None:
    payload = _pdf_bytes("stable")
    file_path = tmp_path / "stable.pdf"
    file_path.write_bytes(payload)
    before = set(tmp_path.iterdir())
    extractor = PdfContentExtractor()
    first = extractor.extract(file_path)
    second = extractor.extract_bytes(payload, file_name="stable.pdf")
    assert first == second
    assert set(tmp_path.iterdir()) == before


def test_external_links_are_not_followed_and_no_network_is_used(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _pdf_bytes("https://example.com")
    connect = Mock(side_effect=AssertionError("network access is forbidden"))
    monkeypatch.setattr("socket.socket.connect", connect)
    result = PdfContentExtractor().extract_bytes(payload)
    assert "https://example.com" in result.text
    connect.assert_not_called()