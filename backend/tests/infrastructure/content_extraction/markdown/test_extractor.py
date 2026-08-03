from __future__ import annotations

from pathlib import Path

import pytest

from domain.content_extraction.exceptions import (
    ContentParseError,
    ContentReadError,
    UnsupportedContentTypeError,
)
from infrastructure.content_extraction.markdown.extractor import MarkdownContentExtractor


def test_supported_extensions_exact() -> None:
    extractor = MarkdownContentExtractor()

    assert extractor.supported_extensions == (".md", ".markdown")


def test_supported_mime_types_exact() -> None:
    extractor = MarkdownContentExtractor()

    assert extractor.supported_mime_types == ("text/markdown", "text/x-markdown")


def test_extracts_valid_markdown(tmp_path: Path) -> None:
    file_path = tmp_path / "doc.md"
    file_path.write_text("# Title\n\nBody", encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert extracted.text.startswith("# Title")


def test_preserves_complete_original_markdown(tmp_path: Path) -> None:
    markdown = "# H1\n\nParagraph\n\n- item"
    file_path = tmp_path / "source.md"
    file_path.write_text(markdown, encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert extracted.text == markdown


def test_first_level_one_heading_becomes_title(tmp_path: Path) -> None:
    file_path = tmp_path / "title.md"
    file_path.write_text("# My Title\n\n## Sub\nText", encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert extracted.title == "My Title"


def test_file_stem_becomes_title_when_no_level_one_heading(tmp_path: Path) -> None:
    file_path = tmp_path / "fallback_title.md"
    file_path.write_text("## Subheading\n\nText", encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert extracted.title == "fallback_title"


def test_creates_sections_from_headings(tmp_path: Path) -> None:
    file_path = tmp_path / "sections.md"
    file_path.write_text("# One\nalpha\n## Two\nbeta", encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert len(extracted.sections) == 2
    assert extracted.sections[0].heading == "One"
    assert extracted.sections[0].text == "alpha"
    assert extracted.sections[1].heading == "Two"
    assert extracted.sections[1].text == "beta"


def test_heading_text_stored_without_markers(tmp_path: Path) -> None:
    file_path = tmp_path / "markers.md"
    file_path.write_text("### Deep Heading\ncontent", encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert extracted.sections[0].heading == "Deep Heading"


def test_content_before_first_heading_becomes_section_with_none_heading(tmp_path: Path) -> None:
    file_path = tmp_path / "preface.md"
    file_path.write_text("Preface line\n\n# Start\nsection body", encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert extracted.sections[0].heading is None
    assert extracted.sections[0].text == "Preface line"
    assert extracted.sections[1].heading == "Start"


def test_section_indexes_start_at_zero_and_increase_deterministically(tmp_path: Path) -> None:
    file_path = tmp_path / "indexes.md"
    file_path.write_text("# A\na\n# B\nb\n# C\nc", encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert [section.section_index for section in extracted.sections] == [0, 1, 2]


def test_document_without_headings_creates_one_full_text_section(tmp_path: Path) -> None:
    text = "Just plain markdown content"
    file_path = tmp_path / "no_headings.md"
    file_path.write_text(text, encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert len(extracted.sections) == 1
    assert extracted.sections[0].heading is None
    assert extracted.sections[0].text == text


def test_heading_count_metadata_is_correct(tmp_path: Path) -> None:
    file_path = tmp_path / "heading_count.md"
    file_path.write_text("# H1\ntext\n## H2\ntext\n### H3\ntext", encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert extracted.metadata["heading_count"] == 3


def test_metadata_contains_required_fields(tmp_path: Path) -> None:
    markdown = "# T\nBody"
    file_path = tmp_path / "meta.markdown"
    file_path.write_text(markdown, encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert extracted.metadata["file_name"] == "meta.markdown"
    assert extracted.metadata["extension"] == ".markdown"
    assert extracted.metadata["size_bytes"] == file_path.stat().st_size
    assert extracted.metadata["encoding"] == "utf-8"


def test_rejects_unsupported_extension(tmp_path: Path) -> None:
    file_path = tmp_path / "bad.txt"
    file_path.write_text("# nope", encoding="utf-8")

    with pytest.raises(UnsupportedContentTypeError):
        MarkdownContentExtractor().extract(file_path)


def test_rejects_blank_content(tmp_path: Path) -> None:
    file_path = tmp_path / "blank.md"
    file_path.write_text("", encoding="utf-8")

    with pytest.raises(ContentParseError):
        MarkdownContentExtractor().extract(file_path)


def test_rejects_whitespace_only_content(tmp_path: Path) -> None:
    file_path = tmp_path / "ws.md"
    file_path.write_text("  \n\t", encoding="utf-8")

    with pytest.raises(ContentParseError):
        MarkdownContentExtractor().extract(file_path)


def test_missing_file_produces_content_read_error(tmp_path: Path) -> None:
    file_path = tmp_path / "missing.md"

    with pytest.raises(ContentReadError):
        MarkdownContentExtractor().extract(file_path)


def test_extension_matching_is_case_insensitive(tmp_path: Path) -> None:
    file_path = tmp_path / "UPPER.MD"
    file_path.write_text("# Upper\nBody", encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert extracted.title == "Upper"


def test_mime_matching_is_case_insensitive(tmp_path: Path) -> None:
    file_path = tmp_path / "README.unknown"
    file_path.write_text("# Readme\nBody", encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path, mime_type="TEXT/MARKDOWN")

    assert extracted.mime_type == "TEXT/MARKDOWN"


def test_markdown_links_remain_plain_source_text_and_are_not_fetched(tmp_path: Path) -> None:
    markdown = "# Links\nSee [example](https://example.com)."
    file_path = tmp_path / "links.md"
    file_path.write_text(markdown, encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert extracted.text == markdown
    assert "https://example.com" in extracted.text


def test_embedded_html_or_code_blocks_are_not_executed(tmp_path: Path) -> None:
    markdown = """# Mixed

<div onclick=\"alert('x')\">hello</div>

```python
print('code block')
```
"""
    file_path = tmp_path / "mixed.md"
    file_path.write_text(markdown, encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert "<div onclick=\"alert('x')\">hello</div>" in extracted.text
    assert "print('code block')" in extracted.text


def test_tables_and_warnings_remain_empty_tuples(tmp_path: Path) -> None:
    file_path = tmp_path / "simple.md"
    file_path.write_text("# T\nBody", encoding="utf-8")

    extracted = MarkdownContentExtractor().extract(file_path)

    assert extracted.tables == ()
    assert extracted.warnings == ()
