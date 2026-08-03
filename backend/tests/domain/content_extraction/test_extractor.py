from __future__ import annotations

from pathlib import Path

import pytest

from domain.content_extraction.extractor import ContentExtractor
from domain.content_extraction.models import ExtractedContent


class DummyExtractor(ContentExtractor):
    @property
    def supported_extensions(self) -> tuple[str, ...]:
        return (".txt", ".md")

    @property
    def supported_mime_types(self) -> tuple[str, ...]:
        return ("text/plain", "text/markdown")

    def extract(self, file_path: Path, *, mime_type: str | None = None) -> ExtractedContent:
        del file_path, mime_type
        return ExtractedContent(title="Sample", text="Extracted body", mime_type="text/plain")


def test_content_extractor_cannot_be_instantiated_directly() -> None:
    with pytest.raises(TypeError):
        ContentExtractor()


def test_minimal_concrete_extractor_implements_all_abstract_members() -> None:
    extractor = DummyExtractor()

    assert isinstance(extractor, ContentExtractor)


def test_supports_matches_extensions_case_insensitively() -> None:
    extractor = DummyExtractor()

    assert extractor.supports(Path("README.TXT")) is True
    assert extractor.supports(Path("notes.Md")) is True


def test_supports_matches_mime_types_case_insensitively() -> None:
    extractor = DummyExtractor()

    assert extractor.supports(Path("noext"), mime_type="TEXT/PLAIN") is True
    assert extractor.supports(Path("noext"), mime_type="Text/Markdown") is True


def test_supports_returns_true_when_extension_matches() -> None:
    extractor = DummyExtractor()

    assert extractor.supports(Path("doc.txt"), mime_type="application/octet-stream") is True


def test_supports_returns_true_when_mime_type_matches() -> None:
    extractor = DummyExtractor()

    assert extractor.supports(Path("file.unknown"), mime_type="text/plain") is True


def test_supports_returns_false_when_neither_matches() -> None:
    extractor = DummyExtractor()

    assert extractor.supports(Path("archive.zip"), mime_type="application/zip") is False


def test_extract_returns_extracted_content_in_test_implementation() -> None:
    extractor = DummyExtractor()

    result = extractor.extract(Path("anything.txt"))

    assert isinstance(result, ExtractedContent)
    assert result.text == "Extracted body"
