"""Provider-neutral embedding exception taxonomy."""


class EmbeddingError(Exception):
    """Base error for embedding contract and provider failures."""


class InvalidEmbeddingConfigurationError(EmbeddingError, ValueError):
    """Raised when an embedding profile is invalid or incompatible."""


class InvalidEmbeddingInputError(EmbeddingError, ValueError):
    """Raised when embedding input text or batch structure is invalid."""


class InvalidEmbeddingResultError(EmbeddingError, ValueError):
    """Raised when a provider returns invalid or incomplete results."""


class RetryableEmbeddingProviderError(EmbeddingError):
    """Raised for provider failures that may succeed when retried."""


class PermanentEmbeddingProviderError(EmbeddingError):
    """Raised for provider failures that should not be retried automatically."""