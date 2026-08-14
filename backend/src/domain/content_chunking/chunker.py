"""Abstract contract for provider-independent content chunkers."""

from __future__ import annotations

from abc import ABC, abstractmethod

from domain.content_chunking.models import ChunkResult, ChunkingConfig


class ContentChunker(ABC):
    """Converts extracted text into deterministic chunk results."""

    @abstractmethod
    def chunk(self, text: str, *, config: ChunkingConfig | None = None) -> tuple[ChunkResult, ...]:
        """Return deterministic chunks for text and configuration."""