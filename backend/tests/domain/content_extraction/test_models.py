from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from domain.content_extraction.models import (
    ExtractedContent,
    ExtractedSection,
    ExtractedTable,
    ExtractionWarning,
)


def test_extracted_section_is_frozen() -> None:
    section = ExtractedSection(heading="Intro", text="Hello")

    with pytest.raises(FrozenInstanceError):
        section.text = "Updated"


def test_extracted_section_rejects_blank_and_whitespace_only_text() -> None:
    with pytest.raises(ValueError, match="text must not be blank"):
        ExtractedSection(heading=None, text="")

    with pytest.raises(ValueError, match="text must not be blank"):
        ExtractedSection(heading=None, text="   ")


def test_extracted_section_accepts_valid_page_number() -> None:
    section = ExtractedSection(heading=None, text="Body", page_number=2)

    assert section.page_number == 2


def test_extracted_section_rejects_page_number_lte_zero() -> None:
    with pytest.raises(ValueError, match="page_number must be greater than 0"):
        ExtractedSection(heading=None, text="Body", page_number=0)

    with pytest.raises(ValueError, match="page_number must be greater than 0"):
        ExtractedSection(heading=None, text="Body", page_number=-1)


def test_extracted_section_rejects_negative_section_index() -> None:
    with pytest.raises(ValueError, match="section_index must be >= 0"):
        ExtractedSection(heading=None, text="Body", section_index=-1)


def test_extracted_section_metadata_defaults_safely_and_is_immutable() -> None:
    first = ExtractedSection(heading=None, text="One")
    second = ExtractedSection(heading=None, text="Two")

    assert first.metadata == {}
    assert second.metadata == {}
    assert first.metadata is not second.metadata

    with pytest.raises(TypeError):
        first.metadata["new"] = "value"


def test_extracted_table_is_frozen() -> None:
    table = ExtractedTable(rows=(("a",),))

    with pytest.raises(FrozenInstanceError):
        table.table_index = 2


def test_extracted_table_rejects_empty_rows() -> None:
    with pytest.raises(ValueError, match="rows must not be empty"):
        ExtractedTable(rows=())


def test_extracted_table_rejects_any_empty_row() -> None:
    with pytest.raises(ValueError, match="every row must contain at least one cell"):
        ExtractedTable(rows=((), ("a",)))


def test_extracted_table_normalizes_rows_to_tuple_of_tuples() -> None:
    table = ExtractedTable(rows=[("a", "b"), ["c"]])

    assert table.rows == (("a", "b"), ("c",))
    assert isinstance(table.rows, tuple)
    assert all(isinstance(row, tuple) for row in table.rows)


def test_extracted_table_rejects_page_number_lte_zero() -> None:
    with pytest.raises(ValueError, match="page_number must be greater than 0"):
        ExtractedTable(rows=(("x",),), page_number=0)

    with pytest.raises(ValueError, match="page_number must be greater than 0"):
        ExtractedTable(rows=(("x",),), page_number=-5)


def test_extracted_table_rejects_negative_table_index() -> None:
    with pytest.raises(ValueError, match="table_index must be >= 0"):
        ExtractedTable(rows=(("x",),), table_index=-1)


def test_extracted_table_metadata_defaults_safely_and_is_immutable() -> None:
    first = ExtractedTable(rows=(("a",),))
    second = ExtractedTable(rows=(("b",),))

    assert first.metadata == {}
    assert second.metadata == {}
    assert first.metadata is not second.metadata

    with pytest.raises(TypeError):
        first.metadata["k"] = "v"


def test_extraction_warning_rejects_blank_code() -> None:
    with pytest.raises(ValueError, match="code must not be blank"):
        ExtractionWarning(code="", message="warn")

    with pytest.raises(ValueError, match="code must not be blank"):
        ExtractionWarning(code="   ", message="warn")


def test_extraction_warning_rejects_blank_message() -> None:
    with pytest.raises(ValueError, match="message must not be blank"):
        ExtractionWarning(code="W001", message="")

    with pytest.raises(ValueError, match="message must not be blank"):
        ExtractionWarning(code="W001", message="   ")


def test_extraction_warning_is_frozen() -> None:
    warning = ExtractionWarning(code="W001", message="warning")

    with pytest.raises(FrozenInstanceError):
        warning.code = "W002"


def test_extracted_content_is_frozen() -> None:
    content = ExtractedContent(title=None, text="Body", mime_type="text/plain")

    with pytest.raises(FrozenInstanceError):
        content.title = "Updated"


def test_extracted_content_rejects_blank_and_whitespace_only_text() -> None:
    with pytest.raises(ValueError, match="text must not be blank"):
        ExtractedContent(title=None, text="", mime_type=None)

    with pytest.raises(ValueError, match="text must not be blank"):
        ExtractedContent(title=None, text="   ", mime_type=None)


def test_extracted_content_defaults_sections_tables_warnings_to_empty_tuples() -> None:
    content = ExtractedContent(title="T", text="Body", mime_type="text/plain")

    assert content.sections == ()
    assert content.tables == ()
    assert content.warnings == ()


def test_extracted_content_normalizes_iterable_values_to_tuples() -> None:
    section = ExtractedSection(heading="H", text="S")
    table = ExtractedTable(rows=(("r1",),))
    warning = ExtractionWarning(code="W001", message="warn")

    content = ExtractedContent(
        title="T",
        text="Body",
        mime_type="text/plain",
        sections=[section],
        tables=[table],
        warnings=[warning],
    )

    assert content.sections == (section,)
    assert content.tables == (table,)
    assert content.warnings == (warning,)
    assert isinstance(content.sections, tuple)
    assert isinstance(content.tables, tuple)
    assert isinstance(content.warnings, tuple)


def test_extracted_content_metadata_defaults_safely_and_is_immutable() -> None:
    first = ExtractedContent(title="T1", text="Body1", mime_type="text/plain")
    second = ExtractedContent(title="T2", text="Body2", mime_type="text/plain")

    assert first.metadata == {}
    assert second.metadata == {}
    assert first.metadata is not second.metadata

    with pytest.raises(TypeError):
        first.metadata["x"] = "y"
