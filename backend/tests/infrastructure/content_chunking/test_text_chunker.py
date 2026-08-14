from __future__ import annotations

import hashlib

import pytest

from domain.content_chunking.models import ChunkingConfig
from infrastructure.content_chunking.text_chunker import DeterministicTextChunker


chunker = DeterministicTextChunker()


def test_empty_and_whitespace_only_text_return_no_chunks() -> None:
    assert chunker.chunk("") == ()
    assert chunker.chunk(" \n\t ") == ()


def test_small_and_exact_maximum_text_return_one_chunk() -> None:
    config = ChunkingConfig(max_chunk_size=20, overlap=3, minimum_preferred_size=5)
    for text in ("small text", "x" * 20):
        result = chunker.chunk(text, config=config)
        assert len(result) == 1
        assert result[0].content == text


def test_boundary_priority_prefers_paragraph_then_line_then_sentence_then_whitespace() -> None:
    paragraph_config = ChunkingConfig(max_chunk_size=35, overlap=3, minimum_preferred_size=5)
    paragraph = chunker.chunk("first paragraph words here.\n\nsecond paragraph words.", config=paragraph_config)
    assert paragraph[0].content == "first paragraph words here."

    line_config = ChunkingConfig(max_chunk_size=22, overlap=2, minimum_preferred_size=5)
    line = chunker.chunk("first line content\nsecond line", config=line_config)
    assert line[0].content == "first line content"

    sentence_config = ChunkingConfig(max_chunk_size=24, overlap=2, minimum_preferred_size=5)
    sentence = chunker.chunk("First sentence here. Next sentence follows", config=sentence_config)
    assert sentence[0].content == "First sentence here."

    whitespace_config = ChunkingConfig(max_chunk_size=15, overlap=2, minimum_preferred_size=5)
    whitespace = chunker.chunk("alpha beta gamma delta", config=whitespace_config)
    assert whitespace[0].content == "alpha beta"


def test_unbroken_text_uses_hard_boundaries_and_all_chunks_are_bounded() -> None:
    config = ChunkingConfig(max_chunk_size=10, overlap=2, minimum_preferred_size=5)
    results = chunker.chunk("x" * 31, config=config)
    assert [result.character_count for result in results] == [10, 10, 10, 7]
    assert all(result.character_count <= config.max_chunk_size for result in results)


def test_overlap_is_retained_and_terminal_chunk_is_not_duplicated() -> None:
    config = ChunkingConfig(max_chunk_size=10, overlap=3, minimum_preferred_size=5)
    results = chunker.chunk("abcdefghijABCDEFGHIJ0123456789", config=config)
    assert len(results) > 1
    assert results[1].start_offset < results[0].end_offset
    assert results[-1].end_offset == len("abcdefghijABCDEFGHIJ0123456789")
    assert len({(result.start_offset, result.end_offset) for result in results}) == len(results)


def test_indexes_checksums_and_repeated_runs_are_stable() -> None:
    text = "One paragraph.\n\nTwo paragraphs with unicode: café."
    config = ChunkingConfig(max_chunk_size=20, overlap=4, minimum_preferred_size=8)
    first = chunker.chunk(text, config=config)
    second = chunker.chunk(text, config=config)
    assert first == second
    assert [result.chunk_index for result in first] == list(range(len(first)))
    assert first[0].content_checksum == hashlib.sha256(first[0].content.encode("utf-8")).hexdigest()
    assert first != chunker.chunk(text + " changed", config=config)


def test_offsets_match_normalized_source_and_line_endings_are_equivalent() -> None:
    crlf = "alpha\r\nbeta\r\ngamma"
    lf = "alpha\nbeta\ngamma"
    cr = "alpha\rbeta\rgamma"
    config = ChunkingConfig(max_chunk_size=8, overlap=2, minimum_preferred_size=3)
    expected = chunker.chunk(lf, config=config)
    assert chunker.chunk(crlf, config=config) == expected
    assert chunker.chunk(cr, config=config) == expected
    for result in expected:
        assert lf[result.start_offset : result.end_offset] == result.content


def test_markdown_structure_and_unicode_are_preserved() -> None:
    text = "# Heading\n\n- café\n- 東京\n\nParagraph"
    results = chunker.chunk(text, config=ChunkingConfig(max_chunk_size=24, overlap=3, minimum_preferred_size=5))
    joined = "\n".join(result.content for result in results)
    assert "# Heading" in joined
    assert "- café" in joined
    assert "- 東京" in joined
    assert all(result.content.strip() for result in results)


def test_large_valid_overlap_makes_forward_progress() -> None:
    config = ChunkingConfig(max_chunk_size=10, overlap=9, minimum_preferred_size=2)
    results = chunker.chunk("0123456789" * 5, config=config)
    assert len(results) > 1
    assert all(current.start_offset > previous.start_offset for previous, current in zip(results, results[1:]))


def test_leading_and_trailing_whitespace_is_trimmed_with_adjusted_offsets() -> None:
    text = "   alpha beta   "
    result = chunker.chunk(text, config=ChunkingConfig(max_chunk_size=20, overlap=2, minimum_preferred_size=3))[0]
    assert result.content == "alpha beta"
    assert text[result.start_offset : result.end_offset] == result.content