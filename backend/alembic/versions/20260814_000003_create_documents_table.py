"""Create tenant-scoped documents table.

Revision ID: 20260814_000003
Revises: 20260802_000002
Create Date: 2026-08-14 00:00:03
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260814_000003"
down_revision = "20260802_000002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "documents",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_type", sa.String(length=64), nullable=False),
        sa.Column("source_document_key", sa.String(length=1024), nullable=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("mime_type", sa.String(length=255), nullable=True),
        sa.Column("checksum_latest", sa.String(length=128), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("source_created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_documents"),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_documents_organization_id_organizations",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "source_type",
            "source_document_key",
            name="uq_documents_organization_id_source_type_source_document_key",
        ),
        sa.CheckConstraint("btrim(source_type) <> ''", name="documents_source_type_not_blank"),
        sa.CheckConstraint("btrim(title) <> ''", name="documents_title_not_blank"),
        sa.CheckConstraint(
            "status IN ('pending', 'processing', 'ready', 'failed')",
            name="documents_status_valid",
        ),
    )
    op.create_index("ix_documents_organization_id_status", "documents", ["organization_id", "status"], unique=False)
    op.create_index(
        "ix_documents_organization_id_source_type",
        "documents",
        ["organization_id", "source_type"],
        unique=False,
    )
    op.create_index(
        "ix_documents_organization_id_deleted_at",
        "documents",
        ["organization_id", "deleted_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_documents_organization_id_deleted_at", table_name="documents")
    op.drop_index("ix_documents_organization_id_source_type", table_name="documents")
    op.drop_index("ix_documents_organization_id_status", table_name="documents")
    op.drop_table("documents")