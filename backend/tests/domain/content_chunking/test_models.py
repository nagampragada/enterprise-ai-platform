from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from domain.content_chunking.models import ChunkResult, ChunkingConfig


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_chunk_size": 0},
        {"overlap": -1},
        {"overlap": 10, "max_chunk_size": 10},
        {"minimum_preferred_size": 0},
        {"minimum_preferred_size": 11, "max_chunk_size": 10},
    ],
)
def test_invalid_chunking_configuration_is_rejected(kwargs: dict[str, int]) -> None:
    with pytest.raises(ValueError):
        ChunkingConfig(**kwargs)


def test_chunk_result_is_frozen_and_requires_token_count_none() -> None:
    result = ChunkResult(0, "text", "a" * 64, 4, 0, 4)
    with pytest.raises(FrozenInstanceError):
        result.content = "changed"

    with pytest.raises(ValueError):
        ChunkResult(0, "text", "a" * 64, 4, 0, 4, token_count=1)