from __future__ import annotations

from pathlib import Path

import pytest

from domain.content_extraction.exceptions import (
    ContentParseError,
    ContentReadError,
    UnsupportedContentTypeError,
)
from infrastructure.content_extraction.text.extractor import TextContentExtractor


def test_supported_extensions_exact() -> None:
    extractor = TextContentExtractor()

    assert extractor.supported_extensions == (".txt",)


def test_supported_mime_types_exact() -> None:
    extractor = TextContentExtractor()

    assert extractor.supported_mime_types == ("text/plain",)


def test_extracts_valid_utf8_text_file(tmp_path: Path) -> None:
    file_path = tmp_path / "example.txt"
    file_path.write_text("hello world", encoding="utf-8")

    extracted = TextContentExtractor().extract(file_path)

    assert extracted.text == "hello world"


def test_title_equals_file_stem(tmp_path: Path) -> None:
    file_path = tmp_path / "meeting_notes.txt"
    file_path.write_text("content", encoding="utf-8")

    extracted = TextContentExtractor().extract(file_path)

    assert extracted.title == "meeting_notes"


def test_supplied_mime_type_is_preserved(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.txt"
    file_path.write_text("content", encoding="utf-8")

    extracted = TextContentExtractor().extract(file_path, mime_type="TEXT/PLAIN")

    assert extracted.mime_type == "TEXT/PLAIN"


def test_default_mime_type_is_text_plain(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.txt"
    file_path.write_text("content", encoding="utf-8")

    extracted = TextContentExtractor().extract(file_path)

    assert extracted.mime_type == "text/plain"


def test_full_file_text_is_preserved(tmp_path: Path) -> None:
    text = "line one\nline two\nline three"
    file_path = tmp_path / "story.txt"
    file_path.write_text(text, encoding="utf-8")

    extracted = TextContentExtractor().extract(file_path)

    assert extracted.text == text


def test_returns_one_section_with_full_text(tmp_path: Path) -> None:
    text = "body text"
    file_path = tmp_path / "single.txt"
    file_path.write_text(text, encoding="utf-8")

    extracted = TextContentExtractor().extract(file_path)

    assert len(extracted.sections) == 1
    assert extracted.sections[0].heading is None
    assert extracted.sections[0].text == text


def test_tables_and_warnings_are_empty_tuples(tmp_path: Path) -> None:
    file_path = tmp_path / "plain.txt"
    file_path.write_text("body", encoding="utf-8")

    extracted = TextContentExtractor().extract(file_path)

    assert extracted.tables == ()
    assert extracted.warnings == ()


def test_metadata_contains_required_fields(tmp_path: Path) -> None:
    text = "hello"
    file_path = tmp_path / "meta.txt"
    file_path.write_text(text, encoding="utf-8")

    extracted = TextContentExtractor().extract(file_path)

    assert extracted.metadata["file_name"] == "meta.txt"
    assert extracted.metadata["extension"] == ".txt"
    assert extracted.metadata["size_bytes"] == len(text.encode("utf-8"))
    assert extracted.metadata["encoding"] in {"utf-8", "utf-8-sig"}


def test_rejects_unsupported_extension(tmp_path: Path) -> None:
    file_path = tmp_path / "bad.md"
    file_path.write_text("text", encoding="utf-8")

    with pytest.raises(UnsupportedContentTypeError):
        TextContentExtractor().extract(file_path)


def test_rejects_blank_content(tmp_path: Path) -> None:
    file_path = tmp_path / "blank.txt"
    file_path.write_text("", encoding="utf-8")

    with pytest.raises(ContentParseError):
        TextContentExtractor().extract(file_path)


def test_rejects_whitespace_only_content(tmp_path: Path) -> None:
    file_path = tmp_path / "whitespace.txt"
    file_path.write_text(" \n\t  ", encoding="utf-8")

    with pytest.raises(ContentParseError):
        TextContentExtractor().extract(file_path)


def test_missing_file_produces_content_read_error(tmp_path: Path) -> None:
    file_path = tmp_path / "missing.txt"

    with pytest.raises(ContentReadError):
        TextContentExtractor().extract(file_path)


def test_utf8_bom_input_is_handled(tmp_path: Path) -> None:
    file_path = tmp_path / "bom.txt"
    file_path.write_bytes(b"\xef\xbb\xbfBOM text")

    extracted = TextContentExtractor().extract(file_path)

    assert extracted.text.replace("\ufeff", "") == "BOM text"
    assert extracted.metadata["encoding"] in {"utf-8", "utf-8-sig"}


def test_extension_matching_is_case_insensitive(tmp_path: Path) -> None:
    file_path = tmp_path / "UPPER.TXT"
    file_path.write_text("ok", encoding="utf-8")

    extracted = TextContentExtractor().extract(file_path)

    assert extracted.title == "UPPER"


def test_does_not_alter_or_execute_text_content(tmp_path: Path) -> None:
    payload = "__import__('os').system('echo should-not-run')"
    file_path = tmp_path / "literal.txt"
    file_path.write_text(payload, encoding="utf-8")

    extracted = TextContentExtractor().extract(file_path)

    assert extracted.text == payload
