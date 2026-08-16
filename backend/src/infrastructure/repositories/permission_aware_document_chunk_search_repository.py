"""Permission-aware PostgreSQL chunk retrieval with authorization before ranking."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import bindparam, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from infrastructure.repositories.document_chunk_repository import EMBEDDING_DIMENSION

MAX_SEARCH_LIMIT = 100
MAX_GROUP_DEPTH = 16


class InvalidPermissionAwareChunkSearchRequest(ValueError):
    """Raised when a permission-aware search request is invalid."""


class PermissionAwareChunkSearchPersistenceError(RuntimeError):
    """Raised when secure chunk retrieval cannot be completed."""


@dataclass(frozen=True)
class PermissionAwareChunkSearchResult:
    chunk_id: UUID
    document_id: UUID
    document_version_id: UUID
    source_item_id: UUID
    knowledge_space_id: UUID
    connector_scope_id: UUID
    chunk_index: int
    chunk_text: str
    document_title: str
    source_type: str
    source_document_key: str | None
    distance: float
    similarity: float
    embedding_model: str


SEARCH_SQL = """
WITH RECURSIVE
authenticated_user AS (
    SELECT u.id, u.organization_id
    FROM users u
    WHERE u.organization_id = :organization_id
      AND u.id = :user_id
      AND u.status = 'active'
),
platform_granted_spaces AS (
    SELECT g.knowledge_space_id
    FROM knowledge_space_organization_grants g
    JOIN authenticated_user u ON u.organization_id = g.organization_id
    WHERE g.granted_at <= CURRENT_TIMESTAMP
      AND (g.expires_at IS NULL OR g.expires_at > CURRENT_TIMESTAMP) AND g.revoked_at IS NULL
    UNION
    SELECT g.knowledge_space_id
    FROM knowledge_space_department_grants g
    JOIN department_memberships m ON m.organization_id = g.organization_id AND m.department_id = g.department_id
    JOIN departments d ON d.organization_id = m.organization_id AND d.id = m.department_id AND d.status = 'active'
    JOIN authenticated_user u ON u.organization_id = m.organization_id AND u.id = m.user_id
    WHERE g.granted_at <= CURRENT_TIMESTAMP
      AND (g.expires_at IS NULL OR g.expires_at > CURRENT_TIMESTAMP) AND g.revoked_at IS NULL
      AND m.status = 'active' AND m.effective_from <= CURRENT_TIMESTAMP
      AND (m.expires_at IS NULL OR m.expires_at > CURRENT_TIMESTAMP) AND m.revoked_at IS NULL
    UNION
    SELECT g.knowledge_space_id
    FROM knowledge_space_team_grants g
    JOIN team_memberships m ON m.organization_id = g.organization_id AND m.team_id = g.team_id
    JOIN teams t ON t.organization_id = m.organization_id AND t.id = m.team_id AND t.status = 'active'
    JOIN authenticated_user u ON u.organization_id = m.organization_id AND u.id = m.user_id
    WHERE g.granted_at <= CURRENT_TIMESTAMP
      AND (g.expires_at IS NULL OR g.expires_at > CURRENT_TIMESTAMP) AND g.revoked_at IS NULL
      AND m.status = 'active' AND m.effective_from <= CURRENT_TIMESTAMP
      AND (m.expires_at IS NULL OR m.expires_at > CURRENT_TIMESTAMP) AND m.revoked_at IS NULL
    UNION
    SELECT g.knowledge_space_id
    FROM knowledge_space_user_grants g
    JOIN authenticated_user u ON u.organization_id = g.organization_id AND u.id = g.user_id
    WHERE g.granted_at <= CURRENT_TIMESTAMP
      AND (g.expires_at IS NULL OR g.expires_at > CURRENT_TIMESTAMP) AND g.revoked_at IS NULL
),
verified_users AS (
    SELECT l.connector_id, p.id AS principal_id, p.normalized_email
    FROM user_external_identity_links l
    JOIN authenticated_user u ON u.organization_id = l.organization_id AND u.id = l.user_id
    JOIN external_principals p ON p.organization_id = l.organization_id
      AND p.connector_id = l.connector_id AND p.id = l.external_principal_id
    WHERE l.status = 'verified' AND l.verified_at IS NOT NULL AND l.revoked_at IS NULL
      AND p.principal_type = 'user' AND p.lifecycle = 'active'
),
usable_directory AS (
    SELECT organization_id, connector_id, current_generation
    FROM external_directory_states
    WHERE organization_id = :organization_id
      AND current_generation IS NOT NULL AND current_generation > 0
      AND last_successful_at IS NOT NULL
      AND status IN ('complete', 'syncing', 'stale', 'failed')
),
group_closure(connector_id, principal_id, depth, path) AS (
    SELECT m.connector_id, m.group_principal_id, 1,
           ARRAY[v.principal_id, m.group_principal_id]::uuid[]
    FROM verified_users v
    JOIN usable_directory ds ON ds.connector_id = v.connector_id
    JOIN external_group_memberships m ON m.organization_id = :organization_id
      AND m.connector_id = v.connector_id AND m.member_principal_id = v.principal_id
      AND m.lifecycle = 'active' AND m.removed_at IS NULL
      AND m.first_seen_generation <= ds.current_generation
      AND m.last_seen_generation >= ds.current_generation
    JOIN external_principals gp ON gp.organization_id = m.organization_id
      AND gp.connector_id = m.connector_id AND gp.id = m.group_principal_id
      AND gp.principal_type = 'group' AND gp.lifecycle = 'active'
    UNION ALL
    SELECT m.connector_id, m.group_principal_id, gc.depth + 1,
           gc.path || m.group_principal_id
    FROM group_closure gc
    JOIN usable_directory ds ON ds.connector_id = gc.connector_id
    JOIN external_group_memberships m ON m.organization_id = :organization_id
      AND m.connector_id = gc.connector_id AND m.member_principal_id = gc.principal_id
      AND m.lifecycle = 'active' AND m.removed_at IS NULL
      AND m.first_seen_generation <= ds.current_generation
      AND m.last_seen_generation >= ds.current_generation
    JOIN external_principals gp ON gp.organization_id = m.organization_id
      AND gp.connector_id = m.connector_id AND gp.id = m.group_principal_id
      AND gp.principal_type = 'group' AND gp.lifecycle = 'active'
    WHERE gc.depth < :max_group_depth AND NOT m.group_principal_id = ANY(gc.path)
),
resolved_principals AS (
    SELECT connector_id, principal_id FROM verified_users
    UNION
    SELECT connector_id, principal_id FROM group_closure
    UNION
    SELECT p.connector_id, p.id
    FROM external_principals p
    JOIN verified_users v ON v.connector_id = p.connector_id
    WHERE p.organization_id = :organization_id AND p.principal_type = 'domain'
      AND p.lifecycle = 'active' AND v.normalized_email IS NOT NULL
      AND btrim(v.normalized_email) <> '' AND position('@' IN v.normalized_email) > 1
      AND p.normalized_domain IS NOT NULL AND btrim(p.normalized_domain) <> ''
      AND p.normalized_domain = split_part(v.normalized_email, '@', 2)
    UNION
    SELECT p.connector_id, p.id
    FROM external_principals p
    WHERE p.organization_id = :organization_id AND p.principal_type = 'anyone'
      AND p.lifecycle = 'active'
),
eligible_scope_paths AS (
    SELECT sim.source_item_id, cs.id AS connector_scope_id, cs.knowledge_space_id,
           cs.connector_id, cs.access_mode
    FROM source_item_scope_memberships sim
    JOIN authenticated_user u ON u.organization_id = sim.organization_id
    JOIN connector_scopes cs ON cs.organization_id = sim.organization_id
      AND cs.connector_id = sim.connector_id AND cs.id = sim.connector_scope_id
      AND cs.status = 'active'
    JOIN connectors c ON c.organization_id = cs.organization_id AND c.id = cs.connector_id
      AND c.status = 'active'
    JOIN knowledge_spaces ks ON ks.organization_id = cs.organization_id
      AND ks.id = cs.knowledge_space_id AND ks.status = 'active'
    JOIN source_items si ON si.organization_id = sim.organization_id
      AND si.connector_id = sim.connector_id AND si.id = sim.source_item_id
      AND si.status = 'active' AND si.deleted_at IS NULL
    WHERE sim.organization_id = :organization_id AND sim.status = 'active' AND sim.removed_at IS NULL
      AND (CAST(:knowledge_space_ids AS uuid[]) IS NULL OR cs.knowledge_space_id = ANY(CAST(:knowledge_space_ids AS uuid[])))
      AND (CAST(:connector_ids AS uuid[]) IS NULL OR cs.connector_id = ANY(CAST(:connector_ids AS uuid[])))
      AND (CAST(:source_item_types AS text[]) IS NULL OR si.source_item_type = ANY(CAST(:source_item_types AS text[])))
),
authorized_paths AS (
    SELECT esp.*
    FROM eligible_scope_paths esp
    WHERE
      (esp.access_mode = 'platform_managed' AND EXISTS (
          SELECT 1 FROM platform_granted_spaces pgs WHERE pgs.knowledge_space_id = esp.knowledge_space_id
      ))
      OR
      (esp.access_mode IN ('source_acl', 'hybrid')
       AND (esp.access_mode = 'source_acl' OR EXISTS (
          SELECT 1 FROM platform_granted_spaces pgs WHERE pgs.knowledge_space_id = esp.knowledge_space_id
       ))
       AND EXISTS (
          SELECT 1 FROM source_acl_snapshots s
          JOIN source_acl_entries e ON e.organization_id = s.organization_id
            AND e.connector_id = s.connector_id AND e.source_item_id = s.source_item_id
            AND e.acl_snapshot_id = s.id
          JOIN resolved_principals rp ON rp.connector_id = e.connector_id
            AND rp.principal_id = e.external_principal_id
          WHERE s.organization_id = :organization_id AND s.connector_id = esp.connector_id
            AND s.source_item_id = esp.source_item_id AND s.is_current
            AND s.status = 'complete' AND s.inheritance_completeness = 'complete'
            AND s.completed_at IS NOT NULL AND s.captured_at IS NOT NULL
            AND e.effect = 'allow' AND e.grants_read AND e.permission_level <> 'unknown'
            AND (e.expires_at IS NULL OR e.expires_at > CURRENT_TIMESTAMP)
       )
       AND NOT EXISTS (
          SELECT 1 FROM source_acl_snapshots s
          JOIN source_acl_entries e ON e.organization_id = s.organization_id
            AND e.connector_id = s.connector_id AND e.source_item_id = s.source_item_id
            AND e.acl_snapshot_id = s.id
          JOIN resolved_principals rp ON rp.connector_id = e.connector_id
            AND rp.principal_id = e.external_principal_id
          WHERE s.organization_id = :organization_id AND s.connector_id = esp.connector_id
            AND s.source_item_id = esp.source_item_id AND s.is_current
            AND s.status = 'complete' AND s.inheritance_completeness = 'complete'
            AND s.completed_at IS NOT NULL AND s.captured_at IS NOT NULL
            AND e.effect = 'deny'
            AND (e.expires_at IS NULL OR e.expires_at > CURRENT_TIMESTAMP)
       ))
),
authorized_sources AS (
    SELECT DISTINCT ON (source_item_id) source_item_id, connector_scope_id, knowledge_space_id
    FROM authorized_paths
    ORDER BY source_item_id, connector_scope_id
),
authorized_chunks AS (
    SELECT dc.id AS chunk_id, dc.document_id, dv.id AS document_version_id,
           dv.source_item_id, a.knowledge_space_id, a.connector_scope_id,
           dc.chunk_index, dc.chunk_text, d.title AS document_title,
           d.source_type, d.source_document_key, dc.embedding_model,
           dc.embedding <=> CAST(:query_embedding AS vector) AS distance
    FROM authorized_sources a
    JOIN document_versions dv ON dv.organization_id = :organization_id
      AND dv.source_item_id = a.source_item_id AND dv.is_current AND dv.lifecycle = 'available'
    JOIN document_version_documents dvd ON dvd.organization_id = dv.organization_id
      AND dvd.document_version_id = dv.id
    JOIN documents d ON d.organization_id = dvd.organization_id AND d.id = dvd.document_id
      AND d.status = 'ready' AND d.deleted_at IS NULL
    JOIN document_chunks dc ON dc.organization_id = d.organization_id AND dc.document_id = d.id
      AND dc.embedding IS NOT NULL AND dc.embedding_model = :embedding_model
    WHERE EXISTS (
        SELECT 1 FROM document_indexing_states dis
        WHERE dis.organization_id = dv.organization_id
          AND dis.document_version_id = dv.id AND dis.status = 'indexed'
          AND dis.indexed_generation = dis.desired_generation
          AND dis.embedding_model = :embedding_model
          AND dis.embedding_dimensions = :embedding_dimension
    )
)
SELECT chunk_id, document_id, document_version_id, source_item_id,
       knowledge_space_id, connector_scope_id, chunk_index, chunk_text,
       document_title, source_type, source_document_key, distance,
       1.0 - distance AS similarity, embedding_model
FROM authorized_chunks
ORDER BY distance ASC, chunk_id ASC
LIMIT :result_limit
"""


class PermissionAwareDocumentChunkSearchRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def search(
        self,
        organization_id: UUID,
        user_id: UUID,
        query_embedding: Sequence[float],
        embedding_model: str,
        limit: int,
        *,
        knowledge_space_ids: Sequence[UUID] | None = None,
        connector_ids: Sequence[UUID] | None = None,
        source_item_types: Sequence[str] | None = None,
    ) -> tuple[PermissionAwareChunkSearchResult, ...]:
        params = _validate_request(
            organization_id, user_id, query_embedding, embedding_model, limit,
            knowledge_space_ids, connector_ids, source_item_types,
        )
        try:
            self._validate_filter_ownership(organization_id, params["knowledge_space_ids"], params["connector_ids"])
            rows = self._session.execute(text(SEARCH_SQL), params).mappings().all()
        except SQLAlchemyError as exc:
            raise PermissionAwareChunkSearchPersistenceError("permission-aware chunk search failed") from exc
        return tuple(PermissionAwareChunkSearchResult(**dict(row)) for row in rows)

    def _validate_filter_ownership(
        self,
        organization_id: UUID,
        knowledge_space_ids: tuple[UUID, ...] | None,
        connector_ids: tuple[UUID, ...] | None,
    ) -> None:
        checks = (
            ("knowledge_spaces", knowledge_space_ids),
            ("connectors", connector_ids),
        )
        for table_name, identifiers in checks:
            if identifiers is None:
                continue
            statement = text(
                f"SELECT count(*) FROM {table_name} "
                "WHERE organization_id = :organization_id AND id = ANY(CAST(:identifiers AS uuid[]))"
            )
            found = self._session.execute(
                statement, {"organization_id": organization_id, "identifiers": list(identifiers)}
            ).scalar_one()
            if found != len(identifiers):
                raise InvalidPermissionAwareChunkSearchRequest("optional filters must belong to the authenticated tenant")


def _validate_request(
    organization_id: UUID,
    user_id: UUID,
    query_embedding: Sequence[float],
    embedding_model: str,
    limit: int,
    knowledge_space_ids: Sequence[UUID] | None,
    connector_ids: Sequence[UUID] | None,
    source_item_types: Sequence[str] | None,
) -> dict[str, object]:
    if not isinstance(organization_id, UUID) or not isinstance(user_id, UUID):
        raise InvalidPermissionAwareChunkSearchRequest("organization_id and user_id must be UUIDs")
    if not isinstance(embedding_model, str) or not embedding_model.strip():
        raise InvalidPermissionAwareChunkSearchRequest("embedding_model must be nonblank")
    if isinstance(query_embedding, (str, bytes)) or len(query_embedding) != EMBEDDING_DIMENSION:
        raise InvalidPermissionAwareChunkSearchRequest(f"query_embedding must contain {EMBEDDING_DIMENSION} values")
    vector: list[float] = []
    for value in query_embedding:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise InvalidPermissionAwareChunkSearchRequest("query_embedding values must be finite numbers")
        vector.append(float(value))
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > MAX_SEARCH_LIMIT:
        raise InvalidPermissionAwareChunkSearchRequest(f"limit must be between 1 and {MAX_SEARCH_LIMIT}")
    spaces = _validate_uuid_filter("knowledge_space_ids", knowledge_space_ids)
    connectors = _validate_uuid_filter("connector_ids", connector_ids)
    types = _validate_type_filter(source_item_types)
    return {
        "organization_id": organization_id,
        "user_id": user_id,
        "query_embedding": "[" + ",".join(format(value, ".17g") for value in vector) + "]",
        "embedding_model": embedding_model.strip(),
        "embedding_dimension": EMBEDDING_DIMENSION,
        "result_limit": limit,
        "max_group_depth": MAX_GROUP_DEPTH,
        "knowledge_space_ids": list(spaces) if spaces else None,
        "connector_ids": list(connectors) if connectors else None,
        "source_item_types": list(types) if types else None,
    }


def _validate_uuid_filter(name: str, values: Sequence[UUID] | None) -> tuple[UUID, ...] | None:
    if values is None:
        return None
    if isinstance(values, (str, bytes)) or not values:
        raise InvalidPermissionAwareChunkSearchRequest(f"{name} must be a nonempty UUID sequence")
    normalized = tuple(values)
    if any(not isinstance(value, UUID) for value in normalized):
        raise InvalidPermissionAwareChunkSearchRequest(f"{name} must contain UUIDs")
    return tuple(dict.fromkeys(normalized))


def _validate_type_filter(values: Sequence[str] | None) -> tuple[str, ...] | None:
    if values is None:
        return None
    if isinstance(values, (str, bytes)) or not values:
        raise InvalidPermissionAwareChunkSearchRequest("source_item_types must be a nonempty sequence")
    normalized = tuple(value.strip() for value in values if isinstance(value, str))
    if len(normalized) != len(values) or any(not value or value != value.lower() or not value.replace("_", "a").isalnum() or not value[0].isalpha() for value in normalized):
        raise InvalidPermissionAwareChunkSearchRequest("source_item_types must contain normalized codes")
    return tuple(dict.fromkeys(normalized))
