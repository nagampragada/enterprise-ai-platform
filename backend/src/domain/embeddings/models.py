"""Immutable models for provider-independent embedding operations."""

from __future__ import annotations

from dataclasses import dataclass

from domain.embeddings.exceptions import (
    InvalidEmbeddingConfigurationError,
    InvalidEmbeddingInputError,
    InvalidEmbeddingResultError,
)


@dataclass(frozen=True)
class EmbeddingProfile:
    """Embedding model identity and persistence compatibility profile."""

    provider_name: str
    model_name: str
    dimension: int
    model_identifier: str
    max_batch_size: int | None = None

    def __post_init__(self) -> None:
        if not self.provider_name.strip():
            raise InvalidEmbeddingConfigurationError("provider_name must not be blank")
        if not self.model_name.strip():
            raise InvalidEmbeddingConfigurationError("model_name must not be blank")
        if self.dimension <= 0:
            raise InvalidEmbeddingConfigurationError("dimension must be greater than zero")
        if self.dimension != 1536:
            raise InvalidEmbeddingConfigurationError("dimension must be 1536 for Version 1 persistence")
        if not self.model_identifier.strip():
            raise InvalidEmbeddingConfigurationError("model_identifier must not be blank")
        if self.max_batch_size is not None and self.max_batch_size <= 0:
            raise InvalidEmbeddingConfigurationError("max_batch_size must be greater than zero")


@dataclass(frozen=True)
class EmbeddingRequest:
    """One ordered text input sent to an embedding provider."""

    input_index: int
    text: str

    def __post_init__(self) -> None:
        if self.input_index < 0:
            raise InvalidEmbeddingInputError("input_index must be nonnegative")
        if not self.text.strip():
            raise InvalidEmbeddingInputError("text must not be blank")


@dataclass(frozen=True)
class EmbeddingResult:
    """One provider result carrying its original input index."""

    input_index: int
    vector: tuple[float, ...]
    model_identifier: str
    dimension: int

    def __post_init__(self) -> None:
        if self.input_index < 0:
            raise InvalidEmbeddingResultError("input_index must be nonnegative")
        object.__setattr__(self, "vector", tuple(self.vector))