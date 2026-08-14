from __future__ import annotations

import math

import pytest

from domain.embeddings import (
    EmbeddingError,
    EmbeddingProfile,
    EmbeddingProvider,
    EmbeddingRequest,
    EmbeddingResult,
    InvalidEmbeddingConfigurationError,
    InvalidEmbeddingInputError,
    InvalidEmbeddingResultError,
    PermanentEmbeddingProviderError,
    RetryableEmbeddingProviderError,
    validate_embedding_inputs,
    validate_embedding_results,
)


def _profile(**overrides) -> EmbeddingProfile:
    values = {
        "provider_name": "fake",
        "model_name": "fake-1536",
        "dimension": 1536,
        "model_identifier": "fake:fake-1536:1536",
    }
    values.update(overrides)
    return EmbeddingProfile(**values)


def _vector(value: float = 0.1, dimension: int = 1536) -> tuple[float, ...]:
    return (value,) * dimension


@pytest.mark.parametrize(
    "overrides",
    [
        {"provider_name": " "},
        {"model_name": " "},
        {"dimension": 0},
        {"dimension": 768},
        {"max_batch_size": 0},
        {"model_identifier": " "},
    ],
)
def test_invalid_profile_is_rejected(overrides) -> None:
    with pytest.raises((ValueError, InvalidEmbeddingConfigurationError)):
        _profile(**overrides)


def test_valid_profile_and_immutable_requests_preserve_order_and_duplicates() -> None:
    profile = _profile(max_batch_size=3)
    texts = ["same", "other", "same"]
    requests = validate_embedding_inputs(texts, profile)
    texts.append("caller mutation")
    assert [request.input_index for request in requests] == [0, 1, 2]
    assert [request.text for request in requests] == ["same", "other", "same"]


@pytest.mark.parametrize("texts", [[], [" "]])
def test_empty_or_blank_inputs_are_rejected(texts) -> None:
    with pytest.raises(InvalidEmbeddingInputError):
        validate_embedding_inputs(texts, _profile())


def test_batch_limit_is_enforced_without_provider_specific_token_limits() -> None:
    with pytest.raises(InvalidEmbeddingInputError):
        validate_embedding_inputs(["a", "b"], _profile(max_batch_size=1))


def test_valid_results_are_accepted_and_restored_to_input_order() -> None:
    profile = _profile()
    requests = validate_embedding_inputs(["a", "b", "a"], profile)
    results = [
        EmbeddingResult(2, _vector(0.3), profile.model_identifier, 1536),
        EmbeddingResult(0, _vector(0.1), profile.model_identifier, 1536),
        EmbeddingResult(1, _vector(0.2), profile.model_identifier, 1536),
    ]
    ordered = validate_embedding_results(requests, results, profile)
    assert [result.input_index for result in ordered] == [0, 1, 2]
    assert ordered[0].vector[0] == 0.1
    assert ordered[2].vector[0] == 0.3


@pytest.mark.parametrize(
    "results",
    [
        [EmbeddingResult(0, _vector(), "fake:fake-1536:1536", 1536)],
        [EmbeddingResult(0, _vector(), "fake:fake-1536:1536", 1536), EmbeddingResult(0, _vector(), "fake:fake-1536:1536", 1536)],
        [EmbeddingResult(0, _vector(), "fake:fake-1536:1536", 1536), EmbeddingResult(2, _vector(), "fake:fake-1536:1536", 1536)],
    ],
)
def test_result_index_shape_is_validated(results) -> None:
    requests = validate_embedding_inputs(["a", "b"], _profile())
    with pytest.raises(InvalidEmbeddingResultError):
        validate_embedding_results(requests, results, _profile())


@pytest.mark.parametrize(
    "result",
    [
        EmbeddingResult(0, _vector(dimension=1535), "fake:fake-1536:1536", 1536),
        EmbeddingResult(0, _vector(), "wrong:model", 1536),
        EmbeddingResult(0, _vector(), "fake:fake-1536:1536", 768),
        EmbeddingResult(0, (True,) * 1536, "fake:fake-1536:1536", 1536),
        EmbeddingResult(0, ("x",) * 1536, "fake:fake-1536:1536", 1536),
        EmbeddingResult(0, (math.nan,) * 1536, "fake:fake-1536:1536", 1536),
        EmbeddingResult(0, (math.inf,) * 1536, "fake:fake-1536:1536", 1536),
        EmbeddingResult(0, (-math.inf,) * 1536, "fake:fake-1536:1536", 1536),
    ],
)
def test_result_values_and_identity_are_validated(result) -> None:
    requests = validate_embedding_inputs(["a"], _profile())
    with pytest.raises(InvalidEmbeddingResultError):
        validate_embedding_results(requests, [result], _profile())


class FakeEmbeddingProvider(EmbeddingProvider):
    @property
    def profile(self) -> EmbeddingProfile:
        return _profile()

    def embed_batch(self, requests: tuple[EmbeddingRequest, ...]) -> tuple[EmbeddingResult, ...]:
        return tuple(
            EmbeddingResult(request.input_index, _vector(), self.profile.model_identifier, self.profile.dimension)
            for request in requests
        )


def test_provider_contract_supports_a_deterministic_fake() -> None:
    provider = FakeEmbeddingProvider()
    requests = validate_embedding_inputs(["a", "b"], provider.profile)
    assert len(provider.embed_batch(requests)) == 2


def test_provider_exception_taxonomy_is_provider_neutral() -> None:
    assert issubclass(InvalidEmbeddingInputError, EmbeddingError)
    assert issubclass(InvalidEmbeddingResultError, EmbeddingError)
    assert issubclass(RetryableEmbeddingProviderError, EmbeddingError)
    assert issubclass(PermanentEmbeddingProviderError, EmbeddingError)