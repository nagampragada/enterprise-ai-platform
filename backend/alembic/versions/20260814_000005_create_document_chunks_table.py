"""Create tenant-scoped document chunks with pgvector support.

Revision ID: 20260814_000005
Revises: 20260814_000004
Create Date: 2026-08-14 00:00:05
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql


revision = "20260814_000005"
down_revision = "20260814_000004"
branch_labels = None
depends_on = None


DOCUMENT_TENANT_KEY = "uq_documents_organization_id_id"


def upgrade() -> None:
    op.create_unique_constraint(
        DOCUMENT_TENANT_KEY,
        "documents",
        ["organization_id", "id"],
    )
    op.create_table(
        "document_chunks",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("document_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("chunk_text", sa.Text(), nullable=False),
        sa.Column("token_count", sa.Integer(), nullable=True),
        sa.Column("content_hash", sa.String(length=128), nullable=False),
        sa.Column("embedding", Vector(1536), nullable=True),
        sa.Column("embedding_model", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_document_chunks"),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_document_chunks_organization_id_organizations",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "document_id"],
            ["documents.organization_id", "documents.id"],
            name="fk_document_chunks_organization_id_document_id_documents",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "document_id",
            "chunk_index",
            name="uq_document_chunks_organization_id_document_id_chunk_index",
        ),
        sa.CheckConstraint("chunk_index >= 0", name="document_chunks_chunk_index_nonnegative"),
        sa.CheckConstraint(
            "token_count IS NULL OR token_count >= 0",
            name="document_chunks_token_count_nonnegative",
        ),
        sa.CheckConstraint("btrim(chunk_text) <> ''", name="document_chunks_chunk_text_not_blank"),
        sa.CheckConstraint("btrim(content_hash) <> ''", name="document_chunks_content_hash_not_blank"),
        sa.CheckConstraint(
            "embedding_model IS NULL OR btrim(embedding_model) <> ''",
            name="document_chunks_embedding_model_not_blank",
        ),
        sa.CheckConstraint(
            "(embedding IS NULL AND embedding_model IS NULL) OR "
            "(embedding IS NOT NULL AND embedding_model IS NOT NULL)",
            name="document_chunks_embedding_model_pair",
        ),
    )
    op.create_index(
        "ix_document_chunks_organization_id_document_id",
        "document_chunks",
        ["organization_id", "document_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_document_chunks_organization_id_document_id", table_name="document_chunks")
    op.drop_table("document_chunks")
    op.drop_constraint(DOCUMENT_TENANT_KEY, "documents", type_="unique")