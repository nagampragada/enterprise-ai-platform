"""Create canonical tenant-safe source item tables.

Revision ID: 20260819_000010
Revises: 20260818_000009
Create Date: 2026-08-19 00:00:10
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260819_000010"
down_revision = "20260818_000009"
branch_labels = None
depends_on = None

SCOPE_CONNECTOR_KEY = "uq_connector_scopes_org_connector_id"


def upgrade() -> None:
    op.create_unique_constraint(
        SCOPE_CONNECTOR_KEY, "connector_scopes", ["organization_id", "connector_id", "id"]
    )
    op.create_table(
        "source_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_item_key", sa.String(length=1024), nullable=False),
        sa.Column("parent_source_item_key", sa.String(length=1024), nullable=True),
        sa.Column("source_item_type", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=1024), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("mime_type", sa.String(length=255), nullable=True),
        sa.Column("source_checksum", sa.String(length=255), nullable=True),
        sa.Column("source_version", sa.String(length=255), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("source_created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_modified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'active'")),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("metadata_schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_source_items"),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], name="fk_source_items_organization", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id"], ["connectors.organization_id", "connectors.id"], name="fk_source_items_connector_tenant", ondelete="CASCADE"),
        sa.UniqueConstraint("organization_id", "connector_id", "id", name="uq_source_items_connector_id"),
        sa.UniqueConstraint("organization_id", "connector_id", "source_item_key", name="uq_source_items_connector_key"),
        sa.CheckConstraint("btrim(source_item_key) <> ''", name="key_not_blank"),
        sa.CheckConstraint("parent_source_item_key IS NULL OR btrim(parent_source_item_key) <> ''", name="parent_key_not_blank"),
        sa.CheckConstraint("parent_source_item_key IS NULL OR parent_source_item_key <> source_item_key", name="parent_key_not_self"),
        sa.CheckConstraint("source_item_type ~ '^[a-z][a-z0-9_]*$'", name="type_code_valid"),
        sa.CheckConstraint("btrim(title) <> ''", name="title_not_blank"),
        sa.CheckConstraint("source_url IS NULL OR btrim(source_url) <> ''", name="url_not_blank"),
        sa.CheckConstraint("mime_type IS NULL OR btrim(mime_type) <> ''", name="mime_type_not_blank"),
        sa.CheckConstraint("source_checksum IS NULL OR btrim(source_checksum) <> ''", name="checksum_not_blank"),
        sa.CheckConstraint("source_version IS NULL OR btrim(source_version) <> ''", name="version_not_blank"),
        sa.CheckConstraint("size_bytes IS NULL OR size_bytes >= 0", name="size_nonnegative"),
        sa.CheckConstraint("last_seen_at >= first_seen_at", name="seen_order_valid"),
        sa.CheckConstraint("status IN ('active', 'deleted', 'unavailable')", name="status_valid"),
        sa.CheckConstraint("(status = 'deleted' AND deleted_at IS NOT NULL) OR (status <> 'deleted' AND deleted_at IS NULL)", name="deletion_consistent"),
        sa.CheckConstraint("jsonb_typeof(metadata) = 'object'", name="metadata_object"),
        sa.CheckConstraint("metadata_schema_version > 0", name="metadata_version_positive"),
    )
    op.create_index("ix_source_items_org_connector_status", "source_items", ["organization_id", "connector_id", "status"])
    op.create_index("ix_source_items_org_connector_type", "source_items", ["organization_id", "connector_id", "source_item_type"])
    op.create_index("ix_source_items_org_connector_seen", "source_items", ["organization_id", "connector_id", "last_seen_at"])

    op.create_table(
        "source_item_scope_memberships",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'active'")),
        sa.Column("first_discovered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_source_item_scope_memberships"),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], name="fk_source_scope_memberships_organization", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id", "source_item_id"],
            ["source_items.organization_id", "source_items.connector_id", "source_items.id"],
            name="fk_source_scope_memberships_item_tenant", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id", "connector_scope_id"],
            ["connector_scopes.organization_id", "connector_scopes.connector_id", "connector_scopes.id"],
            name="fk_source_scope_memberships_scope_tenant", ondelete="CASCADE",
        ),
        sa.UniqueConstraint("organization_id", "source_item_id", "connector_scope_id", name="uq_source_scope_memberships_item_scope"),
        sa.CheckConstraint("status IN ('active', 'removed')", name="status_valid"),
        sa.CheckConstraint("last_seen_at >= first_discovered_at", name="seen_order_valid"),
        sa.CheckConstraint("(status = 'removed' AND removed_at IS NOT NULL) OR (status <> 'removed' AND removed_at IS NULL)", name="removal_consistent"),
    )
    op.create_index("ix_source_scope_memberships_org_scope_status", "source_item_scope_memberships", ["organization_id", "connector_scope_id", "status"])
    op.create_index("ix_source_scope_memberships_org_item_status", "source_item_scope_memberships", ["organization_id", "source_item_id", "status"])


def downgrade() -> None:
    op.drop_index("ix_source_scope_memberships_org_item_status", table_name="source_item_scope_memberships")
    op.drop_index("ix_source_scope_memberships_org_scope_status", table_name="source_item_scope_memberships")
    op.drop_table("source_item_scope_memberships")
    op.drop_index("ix_source_items_org_connector_seen", table_name="source_items")
    op.drop_index("ix_source_items_org_connector_type", table_name="source_items")
    op.drop_index("ix_source_items_org_connector_status", table_name="source_items")
    op.drop_table("source_items")
    op.drop_constraint(SCOPE_CONNECTOR_KEY, "connector_scopes", type_="unique")