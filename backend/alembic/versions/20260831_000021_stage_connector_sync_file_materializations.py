"""Stage generation-scoped connector file materializations.

Revision ID: 20260831_000021
Revises: 20260828_000020
Create Date: 2026-08-31
"""

from alembic import op
from pgvector.sqlalchemy import Vector
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260831_000021"
down_revision = "20260828_000020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connector_sync_file_materializations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("work_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("repository_identity", sa.String(255), nullable=False),
        sa.Column("branch_name", sa.String(255), nullable=False),
        sa.Column("root_tree_object_id", sa.String(255), nullable=False),
        sa.Column("source_item_key", sa.String(1024), nullable=False),
        sa.Column("source_key_hash", sa.String(64), nullable=False),
        sa.Column("repository_path", sa.String(1024), nullable=False),
        sa.Column("provider_blob_id", sa.String(255), nullable=False),
        sa.Column("provider_revision_id", sa.String(255), nullable=False),
        sa.Column("profile_fingerprint", sa.String(255), nullable=False),
        sa.Column("content_checksum", sa.String(64), nullable=False),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("mime_type", sa.String(255), nullable=False),
        sa.Column("embedding_model", sa.String(255), nullable=False),
        sa.Column("chunk_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_connector_sync_file_materializations"),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id", "connector_scope_id", "generation_id", "profile_fingerprint"],
            ["connector_sync_generations.organization_id", "connector_sync_generations.connector_id", "connector_sync_generations.connector_scope_id", "connector_sync_generations.id", "connector_sync_generations.profile_fingerprint"],
            name="fk_sync_file_materializations_generation_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "generation_id", "work_item_id"],
            ["connector_sync_file_work_items.organization_id", "connector_sync_file_work_items.generation_id", "connector_sync_file_work_items.id"],
            name="fk_sync_file_materializations_work_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "organization_id", "generation_id", "id",
            name="uq_sync_file_materializations_generation_id",
        ),
        sa.UniqueConstraint(
            "organization_id", "generation_id", "work_item_id",
            name="uq_sync_file_materializations_generation_work",
        ),
        sa.UniqueConstraint(
            "organization_id", "generation_id", "source_key_hash", "provider_blob_id",
            "provider_revision_id", "profile_fingerprint",
            name="uq_sync_file_materializations_logical_identity",
        ),
        sa.CheckConstraint("btrim(repository_identity) <> ''", name="repo_not_blank"),
        sa.CheckConstraint("btrim(branch_name) <> ''", name="branch_name_not_blank"),
        sa.CheckConstraint("btrim(root_tree_object_id) <> ''", name="tree_not_blank"),
        sa.CheckConstraint("btrim(source_item_key) <> ''", name="key_not_blank"),
        sa.CheckConstraint("source_key_hash ~ '^[0-9a-f]{64}$'", name="key_hash_valid"),
        sa.CheckConstraint("btrim(repository_path) <> ''", name="path_not_blank"),
        sa.CheckConstraint("btrim(provider_blob_id) <> ''", name="blob_not_blank"),
        sa.CheckConstraint("btrim(provider_revision_id) <> ''", name="revision_valid"),
        sa.CheckConstraint(
            "profile_fingerprint ~ '^[a-z0-9][a-z0-9._:/-]*$'",
            name="profile_valid",
        ),
        sa.CheckConstraint("content_checksum ~ '^[0-9a-f]{64}$'", name="checksum_valid"),
        sa.CheckConstraint("btrim(title) <> ''", name="title_not_blank"),
        sa.CheckConstraint("btrim(mime_type) <> ''", name="mime_not_blank"),
        sa.CheckConstraint("btrim(embedding_model) <> ''", name="model_not_blank"),
        sa.CheckConstraint("chunk_count BETWEEN 1 AND 100000", name="count_bounded"),
    )
    op.create_index(
        "ix_sync_file_materializations_generation",
        "connector_sync_file_materializations",
        ["organization_id", "generation_id", "work_item_id"],
    )

    op.create_table(
        "connector_sync_file_materialization_chunks",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("materialization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("chunk_text", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(128), nullable=False),
        sa.Column("embedding", Vector(1536), nullable=False),
        sa.Column("embedding_model", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_connector_sync_file_materialization_chunks"),
        sa.ForeignKeyConstraint(
            ["organization_id", "generation_id", "materialization_id"],
            ["connector_sync_file_materializations.organization_id", "connector_sync_file_materializations.generation_id", "connector_sync_file_materializations.id"],
            name="fk_sync_file_materialization_chunks_parent",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "organization_id", "generation_id", "materialization_id", "chunk_index",
            name="uq_sync_file_materialization_chunks_index",
        ),
        sa.CheckConstraint("chunk_index >= 0", name="index_valid"),
        sa.CheckConstraint("btrim(chunk_text) <> ''", name="text_valid"),
        sa.CheckConstraint("btrim(content_hash) <> ''", name="hash_valid"),
        sa.CheckConstraint("btrim(embedding_model) <> ''", name="model_valid"),
    )
    op.create_index(
        "ix_sync_file_materialization_chunks_parent",
        "connector_sync_file_materialization_chunks",
        ["organization_id", "generation_id", "materialization_id", "chunk_index"],
    )


def downgrade() -> None:
    op.drop_table("connector_sync_file_materialization_chunks")
    op.drop_table("connector_sync_file_materializations")
