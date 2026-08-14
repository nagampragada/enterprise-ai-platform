"""Provider-independent embedding contracts and validation."""

from domain.embeddings.exceptions import (
    EmbeddingError,
    InvalidEmbeddingConfigurationError,
    InvalidEmbeddingInputError,
    InvalidEmbeddingResultError,
    PermanentEmbeddingProviderError,
    RetryableEmbeddingProviderError,
)
from domain.embeddings.models import EmbeddingProfile, EmbeddingRequest, EmbeddingResult
from domain.embeddings.provider import EmbeddingProvider
from domain.embeddings.validation import validate_embedding_inputs, validate_embedding_results

__all__ = [
    "EmbeddingError",
    "EmbeddingProfile",
    "EmbeddingProvider",
    "EmbeddingRequest",
    "EmbeddingResult",
    "InvalidEmbeddingConfigurationError",
    "InvalidEmbeddingInputError",
    "InvalidEmbeddingResultError",
    "PermanentEmbeddingProviderError",
    "RetryableEmbeddingProviderError",
    "validate_embedding_inputs",
    "validate_embedding_results",
]