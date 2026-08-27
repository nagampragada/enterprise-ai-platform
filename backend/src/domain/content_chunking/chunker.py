"""Abstract contract for provider-independent content chunkers."""

from __future__ import annotations

from abc import ABC, abstractmethod

from domain.content_chunking.models import ChunkResult, ChunkingConfig


class ContentChunker(ABC):
    """Converts extracted text into deterministic chunk results."""

    algorithm_version = 1

    @abstractmethod
    def chunk(self, text: str, *, config: ChunkingConfig | None = None) -> tuple[ChunkResult, ...]:
        """Return deterministic chunks for text and configuration."""


def chunking_profile_signature(
    chunker: ContentChunker,
    *,
    config: ChunkingConfig | None = None,
) -> dict[str, object]:
    """Return the deterministic identity inputs for a chunking implementation."""
    active_config = config or ChunkingConfig()
    chunker_type = type(chunker)
    algorithm_version = getattr(chunker_type, "algorithm_version", 1)
    if (
        isinstance(algorithm_version, bool)
        or not isinstance(algorithm_version, int)
        or algorithm_version < 1
    ):
        raise ValueError("content chunker algorithm_version must be a positive integer")
    return {
        "implementation": f"{chunker_type.__module__}.{chunker_type.__qualname__}",
        "algorithm_version": algorithm_version,
        "config": {
            "max_chunk_size": active_config.max_chunk_size,
            "overlap": active_config.overlap,
            "minimum_preferred_size": active_config.minimum_preferred_size,
        },
    }
