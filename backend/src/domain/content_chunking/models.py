"""Immutable domain models for deterministic text chunking."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ChunkingConfig:
    """Character-based chunking configuration."""

    max_chunk_size: int = 2000
    overlap: int = 200
    minimum_preferred_size: int = 200

    def __post_init__(self) -> None:
        if self.max_chunk_size < 1:
            raise ValueError("max_chunk_size must be greater than zero")
        if self.overlap < 0:
            raise ValueError("overlap must not be negative")
        if self.overlap >= self.max_chunk_size:
            raise ValueError("overlap must be less than max_chunk_size")
        if self.minimum_preferred_size < 1:
            raise ValueError("minimum_preferred_size must be greater than zero")
        if self.minimum_preferred_size > self.max_chunk_size:
            raise ValueError("minimum_preferred_size must not exceed max_chunk_size")


@dataclass(frozen=True)
class ChunkResult:
    """A deterministic chunk and its normalized source range."""

    chunk_index: int
    content: str
    content_checksum: str
    character_count: int
    start_offset: int
    end_offset: int
    token_count: None = None

    def __post_init__(self) -> None:
        if self.chunk_index < 0:
            raise ValueError("chunk_index must be nonnegative")
        if not self.content.strip():
            raise ValueError("content must not be blank")
        if self.character_count != len(self.content):
            raise ValueError("character_count must match content length")
        if self.start_offset < 0 or self.end_offset <= self.start_offset:
            raise ValueError("chunk offsets must define a nonempty range")
        if self.end_offset - self.start_offset != self.character_count:
            raise ValueError("chunk offsets must match content length")
        if len(self.content_checksum) != 64 or any(
            character not in "0123456789abcdef" for character in self.content_checksum
        ):
            raise ValueError("content_checksum must be lowercase SHA-256 hex")
        if self.token_count is not None:
            raise ValueError("token_count must be None until an approved tokenizer exists")