from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from domain.content_extraction.exceptions import ContentParseError, UnsupportedContentTypeError
from domain.content_extraction.extractor import ContentExtractor
from domain.content_extraction.models import ExtractedContent
from infrastructure.content_extraction.docx import DocxContentExtractor
from infrastructure.content_extraction.markdown import MarkdownContentExtractor
from infrastructure.content_extraction.pdf import PdfContentExtractor
from infrastructure.content_extraction.registry import (
    ContentExtractorRegistry,
    create_default_content_extractor_registry,
    normalize_extension,
)
from infrastructure.content_extraction.text import TextContentExtractor


class FakeExtractor(ContentExtractor):
    def __init__(self, *extensions: str) -> None:
        self._extensions = extensions
        self.extract_calls: list[tuple[Path, str | None]] = []

    @property
    def supported_extensions(self) -> tuple[str, ...]:
        return self._extensions

    @property
    def supported_mime_types(self) -> tuple[str, ...]:
        return ()

    def extract(self, file_path: Path, *, mime_type: str | None = None) -> ExtractedContent:
        self.extract_calls.append((file_path, mime_type))
        return ExtractedContent(title="fake", text="content", mime_type="text/plain")


def test_extension_normalization_and_registration_without_dot() -> None:
    assert normalize_extension("  TXT ") == ".txt"
    registry = ContentExtractorRegistry()
    extractor = FakeExtractor(".txt")
    registry.register("TXT", extractor)
    assert registry.get_by_extension(".txt") is extractor


@pytest.mark.parametrize("extension", ["", "   ", ".", "folder/file", "folder\\file"])
def test_blank_or_malformed_extensions_are_rejected(extension: str) -> None:
    with pytest.raises((ValueError, TypeError)):
        normalize_extension(extension)


def test_duplicate_registration_is_rejected_without_replacement() -> None:
    registry = ContentExtractorRegistry()
    first = FakeExtractor(".txt")
    registry.register(".txt", first)
    with pytest.raises(ValueError):
        registry.register("TXT", FakeExtractor(".txt"))
    assert registry.get_by_extension("txt") is first


def test_unsupported_extension_and_missing_path_extension_are_rejected() -> None:
    registry = ContentExtractorRegistry()
    with pytest.raises(UnsupportedContentTypeError):
        registry.get_by_extension(".bin")
    with pytest.raises(UnsupportedContentTypeError):
        registry.get_for_path(Path("document"))


def test_lookup_by_path_returns_interface_and_does_not_read_file(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = ContentExtractorRegistry()
    extractor = FakeExtractor(".txt")
    registry.register(".txt", extractor)
    monkeypatch.setattr(Path, "read_bytes", lambda _: (_ for _ in ()).throw(AssertionError("read")))
    assert isinstance(registry.get_for_path(Path("document.TXT")), ContentExtractor)


def test_extract_delegates_once_with_original_path_and_propagates_errors() -> None:
    registry = ContentExtractorRegistry()
    extractor = FakeExtractor(".txt")
    registry.register("txt", extractor)
    path = Path("relative/Document.TXT")
    result = registry.extract(path)
    assert result.text == "content"
    assert extractor.extract_calls == [(path, None)]

    failing = Mock(spec=ContentExtractor)
    failing.supported_extensions = (".bad",)
    failing.extract.side_effect = ContentParseError("parse failed")
    registry.register("bad", failing)
    with pytest.raises(ContentParseError, match="parse failed"):
        registry.extract(Path("file.bad"))
    failing.extract.assert_called_once_with(Path("file.bad"))


def test_default_registry_registers_only_supported_version_one_types() -> None:
    registry = create_default_content_extractor_registry()
    assert isinstance(registry.get_by_extension(".txt"), TextContentExtractor)
    assert isinstance(registry.get_by_extension(".md"), MarkdownContentExtractor)
    assert registry.get_by_extension(".markdown") is registry.get_by_extension(".md")
    assert isinstance(registry.get_by_extension(".docx"), DocxContentExtractor)
    assert isinstance(registry.get_by_extension(".pdf"), PdfContentExtractor)
    for extension in (".docm", ".doc", ".rtf", ".csv", ".png", ".bin"):
        with pytest.raises(UnsupportedContentTypeError):
            registry.get_by_extension(extension)


def test_default_registries_are_independent_and_mapping_is_read_only() -> None:
    first = create_default_content_extractor_registry()
    second = create_default_content_extractor_registry()
    first.register(".custom", FakeExtractor(".custom"))
    with pytest.raises(UnsupportedContentTypeError):
        second.get_by_extension(".custom")
    with pytest.raises(TypeError):
        first.extractors[".other"] = FakeExtractor(".other")  # type: ignore[index]


def test_mime_is_not_used_to_route_unknown_extensions() -> None:
    registry = create_default_content_extractor_registry()
    with pytest.raises(UnsupportedContentTypeError):
        registry.get_for_path(Path("unknown.bin"))