"""Abstract provider contract with no external SDK dependencies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from domain.embeddings.models import EmbeddingProfile, EmbeddingRequest, EmbeddingResult


class EmbeddingProvider(ABC):
    """Synchronous ordered batch embedding provider contract."""

    @property
    @abstractmethod
    def profile(self) -> EmbeddingProfile:
        """Return the immutable profile implemented by this provider."""

    @abstractmethod
    def embed_batch(self, requests: Sequence[EmbeddingRequest]) -> tuple[EmbeddingResult, ...]:
        """Embed requests and return results carrying their input indexes."""