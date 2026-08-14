from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from domain.embeddings.exceptions import (
    EmbeddingProviderAuthenticationError,
    InvalidEmbeddingInputError,
    InvalidEmbeddingResultError,
    PermanentEmbeddingProviderError,
    RetryableEmbeddingProviderError,
)
from domain.embeddings.models import EmbeddingRequest
from infrastructure.embeddings.openai import provider as provider_module
from infrastructure.embeddings.openai.provider import (
    OPENAI_DIMENSION,
    OPENAI_MAX_BATCH_SIZE,
    OPENAI_MODEL,
    OpenAIEmbeddingProvider,
)


def _requests(*texts: str) -> tuple[EmbeddingRequest, ...]:
    return tuple(EmbeddingRequest(index, text) for index, text in enumerate(texts))


def _vector(value: float = 0.1, dimension: int = OPENAI_DIMENSION) -> list[float]:
    return [value] * dimension


def _response(*items: tuple[int, list[float]]) -> SimpleNamespace:
    return SimpleNamespace(data=[SimpleNamespace(index=index, embedding=embedding) for index, embedding in items])


def _provider(response: object | None = None) -> tuple[OpenAIEmbeddingProvider, Mock]:
    client = Mock()
    if response is not None:
        client.embeddings.create.return_value = response
    return OpenAIEmbeddingProvider(client=client), client


def test_profile_is_openai_specific() -> None:
    provider, _ = _provider()
    profile = provider.profile
    assert profile.provider_name == "openai"
    assert profile.model_name == OPENAI_MODEL == "text-embedding-3-small"
    assert profile.dimension == OPENAI_DIMENSION == 1536
    assert profile.max_batch_size == OPENAI_MAX_BATCH_SIZE == 128
    assert profile.model_identifier == "openai:text-embedding-3-small:1536"


def test_valid_batch_sends_exact_request_and_preserves_duplicates() -> None:
    provider, client = _provider(_response((0, _vector(0.1)), (1, _vector(0.2)), (2, _vector(0.1))))
    requests = _requests("same text", "other text", "same text")

    results = provider.embed_batch(requests)

    client.embeddings.create.assert_called_once_with(
        model="text-embedding-3-small",
        input=["same text", "other text", "same text"],
        dimensions=1536,
        encoding_format="float",
    )
    assert [result.input_index for result in results] == [0, 1, 2]
    assert results[0].vector == results[2].vector
    assert requests == _requests("same text", "other text", "same text")


def test_out_of_order_response_is_restored_to_input_order() -> None:
    provider, _ = _provider(_response((2, _vector(0.3)), (0, _vector(0.1)), (1, _vector(0.2))))

    results = provider.embed_batch(_requests("a", "b", "c"))

    assert [result.vector[0] for result in results] == [0.1, 0.2, 0.3]


def test_empty_batch_is_rejected_before_sdk_call() -> None:
    provider, client = _provider()

    with pytest.raises(InvalidEmbeddingInputError):
        provider.embed_batch(())

    client.embeddings.create.assert_not_called()


def test_blank_input_is_rejected_before_sdk_call(monkeypatch: pytest.MonkeyPatch) -> None:
    provider, client = _provider()
    monkeypatch.setattr(
        provider_module,
        "validate_embedding_inputs",
        lambda *_: (_ for _ in ()).throw(InvalidEmbeddingInputError("blank")),
    )

    with pytest.raises(InvalidEmbeddingInputError):
        provider.embed_batch(_requests("blank"))

    client.embeddings.create.assert_not_called()


def test_batch_over_provider_limit_is_rejected_before_sdk_call() -> None:
    provider, client = _provider()

    with pytest.raises(InvalidEmbeddingInputError):
        provider.embed_batch(_requests(*(["text"] * (OPENAI_MAX_BATCH_SIZE + 1))))

    client.embeddings.create.assert_not_called()


@pytest.mark.parametrize(
    "items",
    [
        ((0, _vector()), (0, _vector())),
        ((0, _vector()),),
        ((0, _vector()), (2, _vector())),
        ((0, _vector(dimension=OPENAI_DIMENSION - 1)), (1, _vector())),
        ((0, [True] * OPENAI_DIMENSION), (1, _vector())),
        ((0, ["x"] * OPENAI_DIMENSION), (1, _vector())),
        ((0, [float("nan")] * OPENAI_DIMENSION), (1, _vector())),
        ((0, [float("inf")] * OPENAI_DIMENSION), (1, _vector())),
    ],
)
def test_malformed_success_response_is_rejected(items) -> None:
    provider, _ = _provider(_response(*items))

    with pytest.raises(InvalidEmbeddingResultError):
        provider.embed_batch(_requests("a", "b"))


def test_missing_response_fields_are_rejected() -> None:
    provider, _ = _provider(SimpleNamespace(data=[SimpleNamespace(index=0)]))

    with pytest.raises(InvalidEmbeddingResultError):
        provider.embed_batch(_requests("a"))


@pytest.mark.parametrize(
    ("sdk_name", "domain_error"),
    [
        ("AuthenticationError", EmbeddingProviderAuthenticationError),
        ("PermissionDeniedError", EmbeddingProviderAuthenticationError),
        ("RateLimitError", RetryableEmbeddingProviderError),
        ("APITimeoutError", RetryableEmbeddingProviderError),
        ("APIConnectionError", RetryableEmbeddingProviderError),
        ("InternalServerError", RetryableEmbeddingProviderError),
        ("BadRequestError", PermanentEmbeddingProviderError),
    ],
)
def test_sdk_failures_map_to_provider_neutral_errors(sdk_name: str, domain_error: type[Exception], monkeypatch) -> None:
    provider, client = _provider()
    sdk_error = type(f"Fake{sdk_name}", (Exception,), {})
    monkeypatch.setattr(provider_module, sdk_name, sdk_error)
    client.embeddings.create.side_effect = sdk_error("failure")

    with pytest.raises(domain_error):
        provider.embed_batch(_requests("text"))


def test_unknown_sdk_failure_maps_to_permanent_provider_error() -> None:
    provider, client = _provider()
    client.embeddings.create.side_effect = RuntimeError("provider failure")

    with pytest.raises(PermanentEmbeddingProviderError):
        provider.embed_batch(_requests("text"))


def test_http_status_failures_map_by_status(monkeypatch) -> None:
    class FakeStatusError(Exception):
        def __init__(self, status_code: int) -> None:
            self.status_code = status_code

    monkeypatch.setattr(provider_module, "APIStatusError", FakeStatusError)
    for status_code, expected in ((500, RetryableEmbeddingProviderError), (400, PermanentEmbeddingProviderError)):
        provider, client = _provider()
        client.embeddings.create.side_effect = FakeStatusError(status_code)
        with pytest.raises(expected):
            provider.embed_batch(_requests("text"))


def test_injected_client_requires_no_api_key_and_makes_one_call() -> None:
    provider, client = _provider(_response((0, _vector())))

    provider.embed_batch(_requests("text"))

    assert client.embeddings.create.call_count == 1
