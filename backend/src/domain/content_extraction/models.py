"""Domain models for reusable content extraction contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any


def _freeze_mapping(mapping: Mapping[str, Any]) -> MappingProxyType[str, Any]:
    return MappingProxyType(dict(mapping))


@dataclass(frozen=True)
class ExtractedSection:
    """A logical content section extracted from a source document."""

    heading: str | None
    text: str
    page_number: int | None = None
    section_index: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.text.strip():
            raise ValueError("text must not be blank")
        if self.page_number is not None and self.page_number <= 0:
            raise ValueError("page_number must be greater than 0")
        if self.section_index < 0:
            raise ValueError("section_index must be >= 0")
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))


@dataclass(frozen=True)
class ExtractedTable:
    """A tabular structure extracted from a source document."""

    rows: tuple[tuple[str, ...], ...]
    page_number: int | None = None
    table_index: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        normalized_rows = tuple(tuple(row) for row in self.rows)
        if not normalized_rows:
            raise ValueError("rows must not be empty")
        if any(len(row) == 0 for row in normalized_rows):
            raise ValueError("every row must contain at least one cell")
        if self.page_number is not None and self.page_number <= 0:
            raise ValueError("page_number must be greater than 0")
        if self.table_index < 0:
            raise ValueError("table_index must be >= 0")

        object.__setattr__(self, "rows", normalized_rows)
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))


@dataclass(frozen=True)
class ExtractionWarning:
    """A non-fatal warning emitted during extraction."""

    code: str
    message: str

    def __post_init__(self) -> None:
        if not self.code.strip():
            raise ValueError("code must not be blank")
        if not self.message.strip():
            raise ValueError("message must not be blank")


@dataclass(frozen=True)
class ExtractedContent:
    """Canonical extraction payload produced by content extractors."""

    title: str | None
    text: str
    mime_type: str | None
    sections: tuple[ExtractedSection, ...] = field(default_factory=tuple)
    tables: tuple[ExtractedTable, ...] = field(default_factory=tuple)
    warnings: tuple[ExtractionWarning, ...] = field(default_factory=tuple)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.text.strip():
            raise ValueError("text must not be blank")

        object.__setattr__(self, "sections", tuple(self.sections))
        object.__setattr__(self, "tables", tuple(self.tables))
        object.__setattr__(self, "warnings", tuple(self.warnings))
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata))
