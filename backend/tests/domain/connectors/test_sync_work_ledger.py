from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from domain.connectors.sync_work_ledger import (
    FileWorkCounters,
    FileWorkManifestEntry,
    FileWorkLease,
    FileWorkMaterialization,
    FileWorkMaterializationChunk,
    GenerationObservationDisposition,
    GenerationPromotionRequest,
    GenerationReconciliationRequest,
    GenerationSourceObservation,
    RepositoryGenerationRegistration,
)


NOW = datetime(2026, 8, 28, 12, tzinfo=timezone.utc)
PROFILE = "github:extract-v1:chunk-v2:embed-v1"


def _generation(**overrides: object) -> RepositoryGenerationRegistration:
    values: dict[str, object] = {
        "organization_id": uuid4(),
        "connector_id": uuid4(),
        "connector_scope_id": uuid4(),
        "sync_job_id": uuid4(),
        "provider_key": "github",
        "repository_identity": "github:repository:123",
        "branch_name": "main",
        "commit_object_id": "a" * 40,
        "root_tree_object_id": "b" * 40,
        "profile_fingerprint": PROFILE,
        "created_at": NOW,
    }
    values.update(overrides)
    return RepositoryGenerationRegistration(**values)  # type: ignore[arg-type]


def _entry(**overrides: object) -> FileWorkManifestEntry:
    values: dict[str, object] = {
        "source_item_key": "github:file:README.md",
        "repository_path": "README.md",
        "provider_blob_id": "b" * 40,
        "provider_revision_id": "a" * 40,
        "profile_fingerprint": PROFILE,
        "file_size_bytes": 42,
        "file_extension": ".md",
        "mime_type": "text/markdown",
    }
    values.update(overrides)
    return FileWorkManifestEntry(**values)  # type: ignore[arg-type]


def _materialization(**overrides: object) -> FileWorkMaterialization:
    values: dict[str, object] = {
        "repository_identity": "github:repository:123",
        "branch_name": "main",
        "root_tree_object_id": "b" * 40,
        "source_item_key": "github:file:README.md",
        "repository_path": "README.md",
        "provider_blob_id": "c" * 40,
        "provider_revision_id": "a" * 40,
        "profile_fingerprint": PROFILE,
        "content_checksum": "d" * 64,
        "title": "README",
        "mime_type": "text/markdown",
        "embedding_model": "fake:model:1536",
        "chunks": (
            FileWorkMaterializationChunk(
                0, "content", "e" * 64, (1.0,) * 1536, "fake:model:1536"
            ),
        ),
    }
    values.update(overrides)
    return FileWorkMaterialization(**values)  # type: ignore[arg-type]


def test_generation_and_manifest_contracts_are_immutable() -> None:
    generation = _generation()
    entry = _entry()

    with pytest.raises(FrozenInstanceError):
        generation.branch_name = "other"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        entry.repository_path = "other.md"  # type: ignore[misc]


def test_file_materialization_contract_is_immutable_and_generation_ready() -> None:
    value = _materialization()
    assert value.chunks[0].chunk_index == 0
    assert len(value.chunks[0].embedding) == 1536
    with pytest.raises(FrozenInstanceError):
        value.repository_path = "other.md"  # type: ignore[misc]


@pytest.mark.parametrize(
    "chunks",
    (
        (),
        (
            FileWorkMaterializationChunk(
                1, "content", "e" * 64, (1.0,) * 1536, "fake:model:1536"
            ),
        ),
        (
            FileWorkMaterializationChunk(
                0, "content", "e" * 64, (1.0,) * 1536, "other:model:1536"
            ),
        ),
    ),
)
def test_file_materialization_rejects_missing_or_inconsistent_chunks(chunks) -> None:
    with pytest.raises(ValueError):
        _materialization(chunks=chunks)


def test_file_materialization_chunk_rejects_wrong_vector_dimension() -> None:
    with pytest.raises(ValueError, match="1536"):
        FileWorkMaterializationChunk(
            0, "content", "e" * 64, (1.0,), "fake:model:1536"
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("provider_key", "GitHub"),
        ("repository_identity", ""),
        ("profile_fingerprint", "UPPER"),
        ("created_at", datetime(2026, 8, 28, 12)),
    ],
)
def test_generation_rejects_unbounded_or_unnormalized_values(
    field: str, value: object
) -> None:
    with pytest.raises(ValueError):
        _generation(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_item_key", ""),
        ("repository_path", " path.md"),
        ("profile_fingerprint", "UPPER"),
        ("file_size_bytes", 1_073_741_825),
        ("max_attempts", 11),
    ],
)
def test_manifest_rejects_unbounded_or_unnormalized_values(
    field: str, value: object
) -> None:
    with pytest.raises(ValueError):
        _entry(**{field: value})


def test_safe_counters_enforce_hard_bounds() -> None:
    assert FileWorkCounters(1, 2, 3, 4).embedding_batch_count == 4
    with pytest.raises(ValueError):
        FileWorkCounters(downloaded_bytes=1_073_741_825)
    with pytest.raises(ValueError):
        FileWorkCounters(chunk_count=100_001)


def test_file_work_lease_requires_positive_fairness_sequence() -> None:
    values = (
        uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), "worker", uuid4(),
        1, 1, 3, NOW + timedelta(minutes=5),
    )
    fair = FileWorkLease(*values, fairness_claim_sequence=9)
    assert fair.fairness_claim_sequence == 9
    with pytest.raises(ValueError, match="positive"):
        FileWorkLease(*values, fairness_claim_sequence=0)


def test_generation_promotion_request_is_immutable_and_strict() -> None:
    registration = _generation()
    request = GenerationPromotionRequest(
        registration.organization_id,
        registration.connector_id,
        registration.connector_scope_id,
        uuid4(),
        registration.sync_job_id,
        registration.provider_key,
        registration.repository_identity,
        registration.branch_name,
        registration.commit_object_id,
        registration.root_tree_object_id,
        registration.profile_fingerprint,
    )
    with pytest.raises(FrozenInstanceError):
        request.commit_object_id = "c" * 40  # type: ignore[misc]
    with pytest.raises(ValueError):
        GenerationPromotionRequest(
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            request.generation_id,
            request.sync_job_id,
            "GitHub",
            request.repository_identity,
            request.branch_name,
            request.commit_object_id,
            request.root_tree_object_id,
            request.profile_fingerprint,
        )


def test_generation_observation_and_reconciliation_contracts_are_strict() -> None:
    registration = _generation()
    observation = GenerationSourceObservation(
        "github:repository:123:path:README.md",
        "README.md",
        "b" * 40,
        registration.commit_object_id,
        registration.profile_fingerprint,
        "regular_blob",
        GenerationObservationDisposition.ELIGIBLE,
        42,
    )
    request = GenerationReconciliationRequest(
        registration.organization_id,
        registration.connector_id,
        registration.connector_scope_id,
        uuid4(),
        registration.sync_job_id,
        registration.provider_key,
        registration.repository_identity,
        registration.branch_name,
        registration.commit_object_id,
        registration.root_tree_object_id,
        registration.profile_fingerprint,
    )
    with pytest.raises(FrozenInstanceError):
        observation.repository_path = "other.md"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        request.generation_id = uuid4()  # type: ignore[misc]
    with pytest.raises(ValueError):
        _generation(manifest_schema_version=1)
    with pytest.raises(ValueError):
        GenerationSourceObservation(
            observation.source_item_key,
            observation.repository_path,
            observation.provider_object_id,
            observation.provider_revision_id,
            observation.profile_fingerprint,
            observation.entry_type,
            "eligible",  # type: ignore[arg-type]
            observation.file_size_bytes,
        )
