"""Shared embedding input and provider-result validation."""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence

from domain.embeddings.exceptions import (
    InvalidEmbeddingInputError,
    InvalidEmbeddingResultError,
)
from domain.embeddings.models import EmbeddingProfile, EmbeddingRequest, EmbeddingResult


def validate_embedding_inputs(
    texts: Iterable[str],
    profile: EmbeddingProfile,
) -> tuple[EmbeddingRequest, ...]:
    """Copy, validate, and index inputs without changing their text."""
    values = tuple(texts)
    if not values:
        raise InvalidEmbeddingInputError("embedding batch must not be empty")
    if profile.max_batch_size is not None and len(values) > profile.max_batch_size:
        raise InvalidEmbeddingInputError("embedding batch exceeds profile max_batch_size")

    requests: list[EmbeddingRequest] = []
    for index, value in enumerate(values):
        if not isinstance(value, str):
            raise InvalidEmbeddingInputError("embedding inputs must be strings")
        if not value.strip():
            raise InvalidEmbeddingInputError("embedding input must not be blank")
        requests.append(EmbeddingRequest(input_index=index, text=value))
    return tuple(requests)


def validate_embedding_results(
    requests: Sequence[EmbeddingRequest],
    results: Iterable[EmbeddingResult],
    profile: EmbeddingProfile,
) -> tuple[EmbeddingResult, ...]:
    """Validate provider results and return them in original input order."""
    result_values = tuple(results)
    expected_indexes = set(range(len(requests)))
    actual_indexes = [result.input_index for result in result_values]
    if len(result_values) != len(requests):
        raise InvalidEmbeddingResultError("result count must equal input count")
    if len(set(actual_indexes)) != len(actual_indexes):
        raise InvalidEmbeddingResultError("result indexes must be unique")
    if set(actual_indexes) != expected_indexes:
        raise InvalidEmbeddingResultError("result indexes must cover the complete input range")

    by_index: dict[int, EmbeddingResult] = {}
    for result in result_values:
        if result.dimension != profile.dimension:
            raise InvalidEmbeddingResultError("result dimension does not match profile")
        if result.model_identifier != profile.model_identifier:
            raise InvalidEmbeddingResultError("result model identifier does not match profile")
        if len(result.vector) != profile.dimension:
            raise InvalidEmbeddingResultError("result vector length does not match profile")
        for value in result.vector:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise InvalidEmbeddingResultError("result vector values must be numeric")
            if not math.isfinite(float(value)):
                raise InvalidEmbeddingResultError("result vector values must be finite")
        by_index[result.input_index] = result
    return tuple(by_index[index] for index in range(len(requests)))