from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.services.document_chunk_embedding_service import (
    DocumentChunkEmbeddingPersistenceError,
    DocumentChunkEmbeddingService,
    InvalidDocumentChunkEmbeddingBatchError,
)
from domain.embeddings.exceptions import RetryableEmbeddingProviderError
from domain.embeddings.models import EmbeddingProfile, EmbeddingResult
from infrastructure.db.models import DocumentChunk


DIMENSION = 1536


def _profile(max_batch_size: int | None = 2) -> EmbeddingProfile:
    return EmbeddingProfile("fake", "fake-model", DIMENSION, "fake:fake-model:1536", max_batch_size)


def _chunk(organization_id, document_id, index: int, *, text: str | None = None, embedding=None, model=None):
    return DocumentChunk(
        id=uuid4(),
        organization_id=organization_id,
        document_id=document_id,
        chunk_index=index,
        chunk_text=text or f"chunk {index}",
        token_count=None,
        content_hash=f"hash-{index}-{uuid4()}",
        embedding=embedding,
        embedding_model=model,
    )


def _provider_and_repository(profile=None):
    provider = Mock()
    provider.profile = profile or _profile()
    repository = Mock()
    repository.set_embedding.return_value = True
    return provider, repository


def _results(requests, profile):
    return tuple(
        EmbeddingResult(request.input_index, (float(request.input_index),) * DIMENSION, profile.model_identifier, DIMENSION)
        for request in requests
    )


def test_empty_input_is_rejected_before_calls():
    provider, repository = _provider_and_repository()
    with pytest.raises(InvalidDocumentChunkEmbeddingBatchError):
        DocumentChunkEmbeddingService(provider, repository).embed_chunks(uuid4(), uuid4(), [])
    provider.embed_batch.assert_not_called()
    repository.set_embedding.assert_not_called()


@pytest.mark.parametrize(
    "bad_chunk_factory",
    [
        lambda organization, document: _chunk(uuid4(), document, 0),
        lambda organization, document: _chunk(organization, uuid4(), 0),
        lambda organization, document: _chunk(organization, document, 0, text=" "),
    ],
)
def test_invalid_tenant_document_or_content_is_rejected_before_calls(bad_chunk_factory):
    organization, document = uuid4(), uuid4()
    provider, repository = _provider_and_repository()
    with pytest.raises(InvalidDocumentChunkEmbeddingBatchError):
        DocumentChunkEmbeddingService(provider, repository).embed_chunks(
            organization, document, [_chunk(organization, document, 1), bad_chunk_factory(organization, document)]
        )
    provider.embed_batch.assert_not_called()
    repository.set_embedding.assert_not_called()


@pytest.mark.parametrize("duplicate_kind", ["id", "index"])
def test_duplicate_chunk_identity_is_rejected(duplicate_kind):
    organization, document = uuid4(), uuid4()
    first = _chunk(organization, document, 0)
    second = _chunk(organization, document, 0 if duplicate_kind == "index" else 1)
    if duplicate_kind == "id":
        second.id = first.id
    provider, repository = _provider_and_repository()
    with pytest.raises(InvalidDocumentChunkEmbeddingBatchError):
        DocumentChunkEmbeddingService(provider, repository).embed_chunks(organization, document, [first, second])


def test_unordered_chunks_are_sorted_and_duplicate_text_is_preserved():
    organization, document = uuid4(), uuid4()
    chunks = [_chunk(organization, document, 2, text="same"), _chunk(organization, document, 0, text="same"), _chunk(organization, document, 1)]
    provider, repository = _provider_and_repository()
    provider.embed_batch.side_effect = lambda requests: _results(requests, provider.profile)

    summary = DocumentChunkEmbeddingService(provider, repository).embed_chunks(organization, document, chunks)

    sent = [
        request.text
        for provider_call in provider.embed_batch.call_args_list
        for request in provider_call.args[0]
    ]
    assert sent == ["same", "chunk 1", "same"]
    assert summary.embedded_chunk_ids == tuple(chunk.id for chunk in sorted(chunks, key=lambda item: item.chunk_index))
    assert summary.provider_batches == 2


def test_same_model_is_skipped_missing_is_embedded_and_different_model_regenerated():
    organization, document = uuid4(), uuid4()
    model = _profile().model_identifier
    chunks = [
        _chunk(organization, document, 0, embedding=(0.1,) * DIMENSION, model=model),
        _chunk(organization, document, 1),
        _chunk(organization, document, 2, embedding=(0.1,) * DIMENSION, model="old:model:1536"),
    ]
    provider, repository = _provider_and_repository()
    provider.embed_batch.side_effect = lambda requests: _results(requests, provider.profile)

    summary = DocumentChunkEmbeddingService(provider, repository).embed_chunks(organization, document, chunks)

    assert summary.skipped_chunks == 1
    assert summary.embedded_chunks == 2
    assert [call.kwargs["chunk_id"] for call in repository.set_embedding.call_args_list] == [chunks[1].id, chunks[2].id]


def test_all_chunks_with_same_model_are_skipped_without_provider_call():
    organization, document = uuid4(), uuid4()
    model = _profile().model_identifier
    chunks = [_chunk(organization, document, index, embedding=(0.1,) * DIMENSION, model=model) for index in range(2)]
    provider, repository = _provider_and_repository()

    summary = DocumentChunkEmbeddingService(provider, repository).embed_chunks(organization, document, chunks)

    assert summary.skipped_chunks == 2
    assert summary.embedded_chunks == 0
    assert summary.provider_batches == 0
    provider.embed_batch.assert_not_called()
    repository.set_embedding.assert_not_called()


def test_half_populated_embedding_state_fails_before_provider_call():
    organization, document = uuid4(), uuid4()
    chunk = _chunk(organization, document, 0, embedding=(0.1,) * DIMENSION)
    provider, repository = _provider_and_repository()
    with pytest.raises(InvalidDocumentChunkEmbeddingBatchError):
        DocumentChunkEmbeddingService(provider, repository).embed_chunks(organization, document, [chunk])
    provider.embed_batch.assert_not_called()


def test_result_indexes_map_to_chunk_ids_and_summary_excludes_vectors_and_text():
    organization, document = uuid4(), uuid4()
    chunks = [_chunk(organization, document, 0), _chunk(organization, document, 1)]
    provider, repository = _provider_and_repository()
    provider.embed_batch.side_effect = lambda requests: tuple(reversed(_results(requests, provider.profile)))

    summary = DocumentChunkEmbeddingService(provider, repository).embed_chunks(organization, document, chunks)

    assert [call.kwargs["chunk_id"] for call in repository.set_embedding.call_args_list] == [chunks[0].id, chunks[1].id]
    assert not hasattr(summary, "content")
    assert not hasattr(summary, "vector")


def test_later_provider_batch_failure_happens_before_repository_updates():
    organization, document = uuid4(), uuid4()
    chunks = [_chunk(organization, document, index) for index in range(3)]
    provider, repository = _provider_and_repository()
    provider.embed_batch.side_effect = [
        _results((SimpleNamespace(input_index=0, text="chunk 0"), SimpleNamespace(input_index=1, text="chunk 1")), provider.profile),
        RetryableEmbeddingProviderError("temporary"),
    ]

    with pytest.raises(RetryableEmbeddingProviderError):
        DocumentChunkEmbeddingService(provider, repository).embed_chunks(organization, document, chunks)
    repository.set_embedding.assert_not_called()


def test_persistence_false_raises_without_commit_or_rollback():
    organization, document = uuid4(), uuid4()
    provider, repository = _provider_and_repository()
    provider.embed_batch.side_effect = lambda requests: _results(requests, provider.profile)
    repository.set_embedding.return_value = False
    repository.commit = Mock()
    repository.rollback = Mock()

    with pytest.raises(DocumentChunkEmbeddingPersistenceError):
        DocumentChunkEmbeddingService(provider, repository).embed_chunks(organization, document, [_chunk(organization, document, 0)])
    repository.commit.assert_not_called()
    repository.rollback.assert_not_called()