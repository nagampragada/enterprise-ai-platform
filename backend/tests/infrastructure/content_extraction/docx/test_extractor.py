from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from zipfile import ZipFile

import pytest
from docx import Document

from domain.content_extraction.exceptions import (
    ContentParseError,
    ContentTooLargeError,
    EncryptedContentError,
    UnsupportedContentTypeError,
)
from domain.content_extraction.models import ExtractedContent
from infrastructure.content_extraction.docx import extractor as extractor_module
from infrastructure.content_extraction.docx.extractor import DocxContentExtractor


def _document_bytes(builder) -> bytes:
    document = Document()
    builder(document)
    output = BytesIO()
    document.save(output)
    return output.getvalue()


def test_extracts_simple_document_and_core_properties() -> None:
    def build(document: Document) -> None:
        document.core_properties.title = "Quarterly Report"
        document.core_properties.author = "Author"
        document.core_properties.subject = "Subject"
        document.core_properties.created = datetime(2026, 8, 1, tzinfo=timezone.utc)
        document.core_properties.modified = datetime(2026, 8, 2, tzinfo=timezone.utc)
        document.add_paragraph("Hello world")

    result = DocxContentExtractor().extract_bytes(_document_bytes(build), file_name="report.docx")
    assert isinstance(result, ExtractedContent)
    assert result.title == "Quarterly Report"
    assert result.text == "Hello world"
    assert result.metadata["author"] == "Author"
    assert result.metadata["subject"] == "Subject"
    assert result.metadata["created_at"] == datetime(2026, 8, 1, tzinfo=timezone.utc)


def test_paragraphs_headings_lists_and_tables_remain_in_body_order() -> None:
    def build(document: Document) -> None:
        document.add_heading("Heading", level=1)
        document.add_paragraph("before")
        document.add_paragraph("item", style="List Bullet")
        table = document.add_table(rows=2, cols=2)
        table.cell(0, 0).text = "A"
        table.cell(0, 1).text = "B"
        table.cell(1, 0).text = "C"
        table.cell(1, 1).text = "D"
        document.add_paragraph("after")

    result = DocxContentExtractor().extract_bytes(_document_bytes(build))
    assert result.text == "Heading\n\nbefore\n\nitem\n\nA | B\nC | D\n\nafter"
    assert result.sections[0].heading == "Heading"
    assert result.tables[0].rows == (("A", "B"), ("C", "D"))


def test_multiple_tables_empty_paragraphs_empty_cells_and_merged_cells() -> None:
    def build(document: Document) -> None:
        document.add_paragraph("")
        first = document.add_table(rows=1, cols=3)
        first.cell(0, 0).text = "left"
        first.cell(0, 1).text = ""
        first.cell(0, 2).text = "right"
        first.cell(0, 0).merge(first.cell(0, 1))
        second = document.add_table(rows=1, cols=1)
        second.cell(0, 0).text = "second"

    result = DocxContentExtractor().extract_bytes(_document_bytes(build))
    assert result.text == "left | right\n\nsecond"
    assert len(result.tables) == 2
    assert result.tables[0].rows == (("left", "right"),)


def test_unicode_and_line_endings_are_normalized() -> None:
    def build(document: Document) -> None:
        document.add_paragraph("café\n東京\rvalue")

    result = DocxContentExtractor().extract_bytes(_document_bytes(build))
    assert "café" in result.text
    assert "東京" in result.text
    assert "\r" not in result.text


def test_empty_input_invalid_zip_and_valid_non_docx_zip_are_rejected() -> None:
    extractor = DocxContentExtractor()
    with pytest.raises(ContentParseError):
        extractor.extract_bytes(b"")
    with pytest.raises(ContentParseError):
        extractor.extract_bytes(b"not a zip")
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        archive.writestr("file.txt", "not docx")
    with pytest.raises(ContentParseError):
        extractor.extract_bytes(output.getvalue())


def test_corrupted_docx_package_is_rejected() -> None:
    payload = _document_bytes(lambda document: document.add_paragraph("content"))

    with pytest.raises(ContentParseError):
        DocxContentExtractor().extract_bytes(payload[:64])


def test_encrypted_package_is_rejected_at_zip_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    class EncryptedArchive:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            return None

        def infolist(self):
            return [type("Info", (), {"flag_bits": 0x1})()]

        def namelist(self):
            return ["[Content_Types].xml", "word/document.xml"]

    monkeypatch.setattr(extractor_module, "ZipFile", lambda _: EncryptedArchive())
    with pytest.raises(EncryptedContentError):
        DocxContentExtractor().extract_bytes(b"encrypted-placeholder")


def test_unsupported_extension_and_size_limit_are_rejected() -> None:
    extractor = DocxContentExtractor(max_input_size_bytes=10)
    with pytest.raises(UnsupportedContentTypeError):
        extractor.extract_bytes(b"12345678901", file_name="document.docm")
    with pytest.raises(ContentTooLargeError):
        extractor.extract_bytes(b"12345678901")


def test_repeated_extraction_is_deterministic_and_does_not_write_files(tmp_path) -> None:
    payload = _document_bytes(lambda document: document.add_paragraph("stable"))
    before = set(tmp_path.iterdir())
    extractor = DocxContentExtractor()
    first = extractor.extract_bytes(payload)
    second = extractor.extract_bytes(payload)
    assert first == second
    assert set(tmp_path.iterdir()) == before


def test_core_properties_do_not_expose_custom_properties() -> None:
    def build(document: Document) -> None:
        document.core_properties.title = "Public title"
        document.add_paragraph("body")

    result = DocxContentExtractor().extract_bytes(_document_bytes(build))

    assert "custom_properties" not in result.metadata
    assert result.metadata["file_name"] == "document.docx"


def test_oversized_path_is_rejected_before_reading_bytes(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    file_path = tmp_path / "large.docx"
    file_path.write_bytes(b"x" * 11)

    def fail_if_read() -> bytes:
        raise AssertionError("oversized input should be rejected before read_bytes")

    monkeypatch.setattr("pathlib.Path.read_bytes", lambda _: fail_if_read())
    with pytest.raises(ContentTooLargeError):
        DocxContentExtractor(max_input_size_bytes=10).extract(file_path)


def test_extract_bytes_does_not_access_the_network(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _document_bytes(lambda document: document.add_paragraph("https://example.com"))

    def fail_network(*args, **kwargs):
        raise AssertionError("DOCX extraction must not access the network")

    monkeypatch.setattr("socket.socket.connect", fail_network)
    result = DocxContentExtractor().extract_bytes(payload)
    assert "https://example.com" in result.text


def test_path_contract_reads_docx_without_changing_result(tmp_path) -> None:
    payload = _document_bytes(lambda document: document.add_paragraph("path content"))
    file_path = tmp_path / "sample.docx"
    file_path.write_bytes(payload)
    result = DocxContentExtractor().extract(file_path)
    assert result.text == "path content"