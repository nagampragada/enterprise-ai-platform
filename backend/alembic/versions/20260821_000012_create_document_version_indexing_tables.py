"""Create immutable document versions and durable indexing state.

Revision ID: 20260821_000012
Revises: 20260820_000011
Create Date: 2026-08-21 00:00:12
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260821_000012"
down_revision = "20260820_000011"
branch_labels = None
depends_on = None
SYNC_RUN_TENANT_KEY = "uq_sync_runs_organization_id_id"


def upgrade() -> None:
    op.create_unique_constraint(SYNC_RUN_TENANT_KEY, "connector_sync_runs", ["organization_id", "id"])
    op.create_table(
        "document_versions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_number", sa.BigInteger(), nullable=False),
        sa.Column("provider_version_id", sa.String(255), nullable=True),
        sa.Column("content_checksum", sa.String(255), nullable=True),
        sa.Column("checksum_algorithm", sa.String(64), nullable=True),
        sa.Column("source_modified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("content_type", sa.String(255), nullable=True),
        sa.Column("file_extension", sa.String(64), nullable=True),
        sa.Column("version_cause", sa.String(32), nullable=False),
        sa.Column("lifecycle", sa.String(32), nullable=False),
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("discovered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("metadata_schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.PrimaryKeyConstraint("id", name="pk_document_versions"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "source_item_id"], ["source_items.organization_id", "source_items.connector_id", "source_items.id"], name="fk_document_versions_source_item_tenant", ondelete="CASCADE"),
        sa.UniqueConstraint("organization_id", "id", name="uq_document_versions_organization_id_id"),
        sa.UniqueConstraint("organization_id", "source_item_id", "version_number", name="uq_document_versions_source_version"),
        sa.CheckConstraint("version_number > 0", name="version_number_positive"),
        sa.CheckConstraint("provider_version_id IS NULL OR btrim(provider_version_id) <> ''", name="provider_version_not_blank"),
        sa.CheckConstraint("(content_checksum IS NULL AND checksum_algorithm IS NULL) OR (content_checksum IS NOT NULL AND btrim(content_checksum) <> '' AND checksum_algorithm IS NOT NULL)", name="checksum_pair_valid"),
        sa.CheckConstraint("checksum_algorithm IS NULL OR checksum_algorithm ~ '^[a-z][a-z0-9_]*$'", name="checksum_algorithm_valid"),
        sa.CheckConstraint("source_size_bytes IS NULL OR source_size_bytes >= 0", name="size_nonnegative"),
        sa.CheckConstraint("content_type IS NULL OR btrim(content_type) <> ''", name="content_type_not_blank"),
        sa.CheckConstraint("file_extension IS NULL OR btrim(file_extension) <> ''", name="extension_not_blank"),
        sa.CheckConstraint("version_cause IN ('discovered', 'content_changed', 'metadata_changed', 'restored', 'tombstone', 'manual_backfill')", name="cause_valid"),
        sa.CheckConstraint("lifecycle IN ('available', 'unavailable', 'deleted')", name="lifecycle_valid"),
        sa.CheckConstraint("version_cause <> 'tombstone' OR (lifecycle IN ('unavailable', 'deleted') AND content_checksum IS NULL AND checksum_algorithm IS NULL AND source_size_bytes IS NULL)", name="tombstone_consistent"),
        sa.CheckConstraint("jsonb_typeof(metadata) = 'object'", name="metadata_object"),
        sa.CheckConstraint("metadata_schema_version > 0", name="metadata_version_positive"),
    )
    op.create_index("uq_document_versions_current_source", "document_versions", ["organization_id", "source_item_id"], unique=True, postgresql_where=sa.text("is_current"))
    op.create_index("ix_document_versions_org_source_number", "document_versions", ["organization_id", "source_item_id", "version_number"])
    op.create_index("ix_document_versions_org_checksum", "document_versions", ["organization_id", "content_checksum"])
    op.create_index("ix_document_versions_org_provider_version", "document_versions", ["organization_id", "provider_version_id"])

    op.create_table(
        "document_version_documents",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("document_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("linked_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_document_version_documents"),
        sa.ForeignKeyConstraint(["organization_id", "document_version_id"], ["document_versions.organization_id", "document_versions.id"], name="fk_version_documents_version_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "document_id"], ["documents.organization_id", "documents.id"], name="fk_version_documents_document_tenant", ondelete="CASCADE"),
        sa.UniqueConstraint("organization_id", "document_version_id", name="uq_version_documents_version"),
        sa.UniqueConstraint("organization_id", "document_id", name="uq_version_documents_document"),
    )
    op.create_index("ix_version_documents_org_document", "document_version_documents", ["organization_id", "document_id"])

    op.create_table(
        "document_indexing_states",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("document_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("extraction_profile", sa.String(128), nullable=False),
        sa.Column("extraction_version", sa.String(64), nullable=False),
        sa.Column("chunking_profile", sa.String(128), nullable=False),
        sa.Column("chunking_version", sa.String(64), nullable=False),
        sa.Column("embedding_provider", sa.String(128), nullable=False),
        sa.Column("embedding_model", sa.String(255), nullable=False),
        sa.Column("embedding_dimensions", sa.Integer(), nullable=False),
        sa.Column("profile_fingerprint", sa.String(255), nullable=False),
        sa.Column("desired_generation", sa.BigInteger(), nullable=False),
        sa.Column("indexed_generation", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.String(32), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("reason", sa.String(32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error_category", sa.String(64), nullable=True),
        sa.Column("last_error_code", sa.String(128), nullable=True),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_document_indexing_states"),
        sa.ForeignKeyConstraint(["organization_id", "document_version_id"], ["document_versions.organization_id", "document_versions.id"], name="fk_indexing_states_version_tenant", ondelete="CASCADE"),
        sa.UniqueConstraint("organization_id", "id", name="uq_indexing_states_organization_id_id"),
        sa.UniqueConstraint("organization_id", "document_version_id", "profile_fingerprint", name="uq_indexing_states_version_profile"),
        sa.CheckConstraint("extraction_profile ~ '^[a-z0-9][a-z0-9._:/-]*$' AND extraction_version ~ '^[a-z0-9][a-z0-9._:/-]*$' AND chunking_profile ~ '^[a-z0-9][a-z0-9._:/-]*$' AND chunking_version ~ '^[a-z0-9][a-z0-9._:/-]*$' AND embedding_provider ~ '^[a-z0-9][a-z0-9._:/-]*$' AND embedding_model ~ '^[a-z0-9][a-z0-9._:/-]*$' AND profile_fingerprint ~ '^[a-z0-9][a-z0-9._:/-]*$'", name="profile_identifiers_valid"),
        sa.CheckConstraint("embedding_dimensions > 0", name="embedding_dimensions_positive"),
        sa.CheckConstraint("desired_generation > 0", name="desired_generation_positive"),
        sa.CheckConstraint("indexed_generation IS NULL OR (indexed_generation > 0 AND indexed_generation <= desired_generation)", name="indexed_generation_valid"),
        sa.CheckConstraint("status IN ('pending', 'processing', 'indexed', 'stale', 'failed', 'cancelled')", name="status_valid"),
        sa.CheckConstraint("reason IN ('new_version', 'content_changed', 'profile_changed', 'embedding_model_changed', 'manual_backfill', 'repair')", name="reason_valid"),
        sa.CheckConstraint("attempt_count >= 0", name="attempt_count_nonnegative"),
        sa.CheckConstraint("(last_error_category IS NULL AND last_error_code IS NULL) OR (last_error_category ~ '^[a-z][a-z0-9_]*$' AND last_error_code ~ '^[a-z][a-z0-9_]*$')", name="error_pair_valid"),
        sa.CheckConstraint("(status = 'pending' AND started_at IS NULL AND completed_at IS NULL) OR (status = 'processing' AND started_at IS NOT NULL AND completed_at IS NULL) OR (status = 'indexed' AND completed_at IS NOT NULL AND indexed_generation = desired_generation AND next_retry_at IS NULL) OR (status = 'failed' AND completed_at IS NOT NULL AND last_error_category IS NOT NULL AND last_error_code IS NOT NULL) OR status IN ('stale', 'cancelled')", name="status_state_valid"),
        sa.CheckConstraint("next_retry_at IS NULL OR status IN ('pending', 'failed')", name="retry_status_valid"),
        sa.CheckConstraint("completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at", name="completed_order_valid"),
    )
    op.create_index("ix_indexing_states_org_status_requested", "document_indexing_states", ["organization_id", "status", "requested_at"])
    op.create_index("ix_indexing_states_org_retry", "document_indexing_states", ["organization_id", "next_retry_at"])
    op.create_index("ix_indexing_states_org_profile", "document_indexing_states", ["organization_id", "profile_fingerprint", "status"])

    op.create_table(
        "document_indexing_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("indexing_state_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_sync_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("connector_sync_item_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("trigger_type", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("worker_reference", sa.String(255), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_category", sa.String(64), nullable=True),
        sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("retryable", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("summary", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("summary_schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_document_indexing_attempts"),
        sa.ForeignKeyConstraint(["organization_id", "indexing_state_id"], ["document_indexing_states.organization_id", "document_indexing_states.id"], name="fk_indexing_attempts_state_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_sync_run_id"], ["connector_sync_runs.organization_id", "connector_sync_runs.id"], name="fk_indexing_attempts_run_tenant", ondelete="SET NULL (connector_sync_run_id)"),
        sa.ForeignKeyConstraint(["organization_id", "connector_sync_run_id", "connector_sync_item_id"], ["connector_sync_items.organization_id", "connector_sync_items.sync_run_id", "connector_sync_items.id"], name="fk_indexing_attempts_item_tenant", ondelete="SET NULL (connector_sync_item_id)"),
        sa.UniqueConstraint("organization_id", "indexing_state_id", "attempt_number", name="uq_indexing_attempts_state_number"),
        sa.CheckConstraint("attempt_number > 0", name="attempt_number_positive"),
        sa.CheckConstraint("connector_sync_item_id IS NULL OR connector_sync_run_id IS NOT NULL", name="item_requires_run"),
        sa.CheckConstraint("trigger_type IN ('sync', 'retry', 'manual_backfill', 'scheduled_backfill', 'repair')", name="trigger_valid"),
        sa.CheckConstraint("status IN ('running', 'succeeded', 'failed', 'cancelled')", name="status_valid"),
        sa.CheckConstraint("worker_reference IS NULL OR btrim(worker_reference) <> ''", name="worker_not_blank"),
        sa.CheckConstraint("(error_category IS NULL AND error_code IS NULL) OR (error_category ~ '^[a-z][a-z0-9_]*$' AND error_code ~ '^[a-z][a-z0-9_]*$')", name="error_pair_valid"),
        sa.CheckConstraint("(status = 'running' AND completed_at IS NULL) OR (status IN ('succeeded', 'failed', 'cancelled') AND completed_at IS NOT NULL)", name="completion_matches_status"),
        sa.CheckConstraint("status <> 'succeeded' OR (error_category IS NULL AND error_code IS NULL)", name="success_has_no_error"),
        sa.CheckConstraint("status <> 'failed' OR (error_category IS NOT NULL AND error_code IS NOT NULL)", name="failure_has_error"),
        sa.CheckConstraint("completed_at IS NULL OR completed_at >= started_at", name="completed_order_valid"),
        sa.CheckConstraint("jsonb_typeof(summary) = 'object'", name="summary_object"),
        sa.CheckConstraint("summary_schema_version > 0", name="summary_version_positive"),
    )
    op.create_index("ix_indexing_attempts_org_state_number", "document_indexing_attempts", ["organization_id", "indexing_state_id", "attempt_number"])
    op.create_index("ix_indexing_attempts_org_sync_run", "document_indexing_attempts", ["organization_id", "connector_sync_run_id"])


def downgrade() -> None:
    op.drop_index("ix_indexing_attempts_org_sync_run", table_name="document_indexing_attempts")
    op.drop_index("ix_indexing_attempts_org_state_number", table_name="document_indexing_attempts")
    op.drop_table("document_indexing_attempts")
    op.drop_index("ix_indexing_states_org_profile", table_name="document_indexing_states")
    op.drop_index("ix_indexing_states_org_retry", table_name="document_indexing_states")
    op.drop_index("ix_indexing_states_org_status_requested", table_name="document_indexing_states")
    op.drop_table("document_indexing_states")
    op.drop_index("ix_version_documents_org_document", table_name="document_version_documents")
    op.drop_table("document_version_documents")
    op.drop_index("ix_document_versions_org_provider_version", table_name="document_versions")
    op.drop_index("ix_document_versions_org_checksum", table_name="document_versions")
    op.drop_index("ix_document_versions_org_source_number", table_name="document_versions")
    op.drop_index("uq_document_versions_current_source", table_name="document_versions")
    op.drop_table("document_versions")
    op.drop_constraint(SYNC_RUN_TENANT_KEY, "connector_sync_runs", type_="unique")
