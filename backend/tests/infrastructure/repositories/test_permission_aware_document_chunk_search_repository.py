from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from unittest.mock import Mock
from uuid import uuid4

import pytest
from sqlalchemy.exc import SQLAlchemyError

from infrastructure.repositories.document_chunk_repository import EMBEDDING_DIMENSION
from infrastructure.repositories.permission_aware_document_chunk_search_repository import (
    MAX_SEARCH_LIMIT,
    SEARCH_SQL,
    InvalidPermissionAwareChunkSearchRequest,
    PermissionAwareChunkSearchPersistenceError,
    PermissionAwareChunkSearchResult,
    PermissionAwareDocumentChunkSearchRepository,
)


def _vector(value: float = 0.1) -> list[float]:
    return [value] * EMBEDDING_DIMENSION


def _repository() -> tuple[PermissionAwareDocumentChunkSearchRepository, Mock]:
    session = Mock()
    session.execute.return_value.mappings.return_value.all.return_value = []
    return PermissionAwareDocumentChunkSearchRepository(session), session


@pytest.mark.parametrize(
    ("overrides"),
    [
        {"organization_id": "bad"},
        {"user_id": "bad"},
        {"embedding_model": " "},
        {"query_embedding": []},
        {"query_embedding": [0.0] * (EMBEDDING_DIMENSION - 1)},
        {"query_embedding": [float("nan")] * EMBEDDING_DIMENSION},
        {"query_embedding": [float("inf")] * EMBEDDING_DIMENSION},
        {"query_embedding": [True] * EMBEDDING_DIMENSION},
        {"limit": 0},
        {"limit": MAX_SEARCH_LIMIT + 1},
        {"limit": True},
        {"knowledge_space_ids": []},
        {"knowledge_space_ids": ["bad"]},
        {"connector_ids": []},
        {"connector_ids": ["bad"]},
        {"source_item_types": []},
        {"source_item_types": ["Bad-Type"]},
    ],
)
def test_invalid_inputs_fail_before_database_execution(overrides) -> None:
    repository, session = _repository()
    arguments = {
        "organization_id": uuid4(),
        "user_id": uuid4(),
        "query_embedding": _vector(),
        "embedding_model": "text-embedding-3-small",
        "limit": 10,
    }
    arguments.update(overrides)

    with pytest.raises(InvalidPermissionAwareChunkSearchRequest):
        repository.search(**arguments)

    session.execute.assert_not_called()


def test_optional_filter_ids_must_belong_to_tenant() -> None:
    repository, session = _repository()
    session.execute.return_value.scalar_one.return_value = 0

    with pytest.raises(InvalidPermissionAwareChunkSearchRequest):
        repository.search(
            uuid4(), uuid4(), _vector(), "text-embedding-3-small", 5,
            connector_ids=[uuid4()],
        )

    assert session.execute.call_count == 1


def test_repository_never_commits_or_rolls_back() -> None:
    repository, session = _repository()

    assert repository.search(uuid4(), uuid4(), _vector(), "text-embedding-3-small", 5) == ()

    session.commit.assert_not_called()
    session.rollback.assert_not_called()


def test_safe_result_is_immutable_and_contains_no_security_internals() -> None:
    result = PermissionAwareChunkSearchResult(
        chunk_id=uuid4(), document_id=uuid4(), document_version_id=uuid4(),
        source_item_id=uuid4(), knowledge_space_id=uuid4(), connector_scope_id=uuid4(),
        chunk_index=0, chunk_text="authorized text", document_title="Title",
        source_type="google_drive", source_document_key="provider-key",
        distance=0.1, similarity=0.9, embedding_model="text-embedding-3-small",
    )

    with pytest.raises(FrozenInstanceError):
        result.chunk_text = "changed"  # type: ignore[misc]
    names = {field.name for field in fields(result)}
    assert not {"embedding", "acl_entry", "external_principal_id", "authorization_reason"}.intersection(names)


def test_database_failures_are_wrapped_without_driver_details() -> None:
    repository, session = _repository()
    session.execute.side_effect = SQLAlchemyError("sensitive SQL detail")

    with pytest.raises(PermissionAwareChunkSearchPersistenceError) as error:
        repository.search(uuid4(), uuid4(), _vector(), "text-embedding-3-small", 5)

    assert str(error.value) == "permission-aware chunk search failed"
    assert isinstance(error.value.__cause__, SQLAlchemyError)


def test_ranked_sql_contains_authorization_before_vector_distance() -> None:
    required_tables = {
        "users", "knowledge_space_organization_grants", "department_memberships",
        "team_memberships", "knowledge_space_user_grants", "user_external_identity_links",
        "external_directory_states", "external_group_memberships", "source_acl_snapshots",
        "source_acl_entries", "connector_scopes", "source_item_scope_memberships",
        "document_versions", "document_version_documents", "document_indexing_states",
        "document_chunks",
    }
    lowered = SEARCH_SQL.lower()
    assert all(table in lowered for table in required_tables)
    assert lowered.index("authorized_paths as") < lowered.index("<=>") < lowered.index("order by distance")
    assert "not exists" in lowered
    assert "limit :result_limit" in lowered
