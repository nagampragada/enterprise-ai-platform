"""Deterministic character-based text chunker."""

from __future__ import annotations

import hashlib
import re

from domain.content_chunking.chunker import ContentChunker
from domain.content_chunking.models import ChunkResult, ChunkingConfig


_PARAGRAPH_BOUNDARY = re.compile(r"\n[ \t]*\n[ \t]*")
_SENTENCE_BOUNDARY = re.compile(r"[.!?](?=\s)")


class DeterministicTextChunker(ContentChunker):
    """Split normalized text at the best available natural boundary.

    Overlap is measured from the selected raw boundary and may be shortened
    when whitespace is skipped at the next chunk start. This keeps ranges
    aligned to content while guaranteeing forward progress.
    """

    def chunk(self, text: str, *, config: ChunkingConfig | None = None) -> tuple[ChunkResult, ...]:
        active_config = config or ChunkingConfig()
        normalized_text = _normalize_line_endings(text)
        if not normalized_text.strip():
            return ()

        chunks: list[ChunkResult] = []
        cursor = 0
        source_length = len(normalized_text)
        while cursor < source_length:
            start = _skip_leading_whitespace(normalized_text, cursor)
            if start >= source_length:
                break

            raw_end = min(start + active_config.max_chunk_size, source_length)
            if raw_end < source_length:
                raw_end = _select_boundary(normalized_text, start, raw_end, active_config)

            end = _trim_trailing_whitespace(normalized_text, start, raw_end)
            if end <= start:
                end = min(start + active_config.max_chunk_size, source_length)
                end = _trim_trailing_whitespace(normalized_text, start, end)
            if end <= start:
                break

            content = normalized_text[start:end]
            chunks.append(
                ChunkResult(
                    chunk_index=len(chunks),
                    content=content,
                    content_checksum=hashlib.sha256(content.encode("utf-8")).hexdigest(),
                    character_count=len(content),
                    start_offset=start,
                    end_offset=end,
                )
            )

            if end >= source_length:
                break
            next_cursor = max(end - active_config.overlap, start + 1)
            if next_cursor <= cursor:
                next_cursor = cursor + 1
            cursor = next_cursor

        return tuple(chunks)


def _normalize_line_endings(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _skip_leading_whitespace(text: str, start: int) -> int:
    while start < len(text) and text[start].isspace():
        start += 1
    return start


def _trim_trailing_whitespace(text: str, start: int, end: int) -> int:
    while end > start and text[end - 1].isspace():
        end -= 1
    return end


def _select_boundary(text: str, start: int, limit: int, config: ChunkingConfig) -> int:
    preferred_start = min(start + config.minimum_preferred_size, limit)
    paragraph = [match.end() for match in _PARAGRAPH_BOUNDARY.finditer(text, start, limit)]
    if paragraph:
        return max(paragraph)

    lines = [index + 1 for index in range(start, limit) if text[index] == "\n"]
    preferred_lines = [index for index in lines if index >= preferred_start]
    if preferred_lines:
        return max(preferred_lines)
    if lines:
        return max(lines)

    sentences = [match.end() for match in _SENTENCE_BOUNDARY.finditer(text, start, limit)]
    preferred_sentences = [index for index in sentences if index >= preferred_start]
    if preferred_sentences:
        return max(preferred_sentences)
    if sentences:
        return max(sentences)

    whitespace = [index for index in range(start + 1, limit) if text[index].isspace()]
    preferred_whitespace = [index for index in whitespace if index >= preferred_start]
    if preferred_whitespace:
        return max(preferred_whitespace)
    if whitespace:
        return max(whitespace)
    return limit