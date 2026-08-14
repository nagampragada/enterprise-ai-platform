"""Official OpenAI embeddings adapter."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from openai import (
    APIConnectionError,
    APIError,
    APIStatusError,
    APITimeoutError,
    AuthenticationError,
    BadRequestError,
    InternalServerError,
    OpenAI,
    OpenAIError,
    PermissionDeniedError,
    RateLimitError,
)

from domain.embeddings.exceptions import (
    EmbeddingProviderAuthenticationError,
    InvalidEmbeddingInputError,
    InvalidEmbeddingResultError,
    PermanentEmbeddingProviderError,
    RetryableEmbeddingProviderError,
)
from domain.embeddings.models import EmbeddingProfile, EmbeddingRequest, EmbeddingResult
from domain.embeddings.provider import EmbeddingProvider
from domain.embeddings.validation import validate_embedding_inputs, validate_embedding_results


OPENAI_MODEL = "text-embedding-3-small"
OPENAI_DIMENSION = 1536
OPENAI_MAX_BATCH_SIZE = 128


class OpenAIEmbeddingProvider(EmbeddingProvider):
    """Synchronous OpenAI adapter with injectable client for tests."""

    def __init__(self, client: OpenAI | None = None) -> None:
        if client is not None:
            self._client = client
            return
        try:
            self._client = OpenAI()
        except Exception as exc:
            raise EmbeddingProviderAuthenticationError(
                "OpenAI client configuration is unavailable"
            ) from exc

    @property
    def profile(self) -> EmbeddingProfile:
        return EmbeddingProfile(
            provider_name="openai",
            model_name=OPENAI_MODEL,
            dimension=OPENAI_DIMENSION,
            model_identifier=f"openai:{OPENAI_MODEL}:{OPENAI_DIMENSION}",
            max_batch_size=OPENAI_MAX_BATCH_SIZE,
        )

    def embed_batch(self, requests: Sequence[EmbeddingRequest]) -> tuple[EmbeddingResult, ...]:
        texts = [request.text for request in requests]
        validated_requests = validate_embedding_inputs(texts, self.profile)
        if any(request.input_index != index for index, request in enumerate(requests)):
            raise InvalidEmbeddingInputError("request indexes must cover the complete input range")

        try:
            response = self._client.embeddings.create(
                model=OPENAI_MODEL,
                input=[request.text for request in validated_requests],
                dimensions=OPENAI_DIMENSION,
                encoding_format="float",
            )
        except AuthenticationError as exc:
            raise EmbeddingProviderAuthenticationError("OpenAI authentication failed") from exc
        except PermissionDeniedError as exc:
            raise EmbeddingProviderAuthenticationError("OpenAI permission was denied") from exc
        except (RateLimitError, APITimeoutError, APIConnectionError, InternalServerError) as exc:
            raise RetryableEmbeddingProviderError("OpenAI request is temporarily unavailable") from exc
        except APIStatusError as exc:
            if exc.status_code >= 500:
                raise RetryableEmbeddingProviderError("OpenAI service failed temporarily") from exc
            raise PermanentEmbeddingProviderError("OpenAI rejected the embedding request") from exc
        except BadRequestError as exc:
            raise PermanentEmbeddingProviderError("OpenAI rejected the embedding request") from exc
        except (APIError, OpenAIError) as exc:
            raise PermanentEmbeddingProviderError("OpenAI embedding request failed") from exc
        except Exception as exc:
            raise PermanentEmbeddingProviderError("OpenAI embedding request failed") from exc

        try:
            results = tuple(
                EmbeddingResult(
                    input_index=item.index,
                    vector=tuple(item.embedding),
                    model_identifier=self.profile.model_identifier,
                    dimension=len(item.embedding),
                )
                for item in response.data
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise InvalidEmbeddingResultError("OpenAI returned a malformed embedding response") from exc

        return validate_embedding_results(validated_requests, results, self.profile)