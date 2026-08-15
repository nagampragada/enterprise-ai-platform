"""Create tenant-safe connector core tables.

Revision ID: 20260818_000009
Revises: 20260817_000008
Create Date: 2026-08-18 00:00:09
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260818_000009"
down_revision = "20260817_000008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connectors",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_type", sa.String(length=64), nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'draft'")),
        sa.Column("acl_support", sa.String(length=32), nullable=False, server_default=sa.text("'none'")),
        sa.Column("capabilities", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("safe_config", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("config_schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("secret_reference", sa.String(length=1024), nullable=True),
        sa.Column("credential_status", sa.String(length=32), nullable=False, server_default=sa.text("'not_configured'")),
        sa.Column("credential_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("last_validated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_connectors"),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], name="fk_connectors_organization_id_organizations", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["organization_id", "created_by_user_id"], ["users.organization_id", "users.id"],
            name="fk_connectors_creator_tenant", ondelete="SET NULL (created_by_user_id)",
        ),
        sa.UniqueConstraint("organization_id", "id", name="uq_connectors_organization_id_id"),
        sa.UniqueConstraint("organization_id", "slug", name="uq_connectors_organization_id_slug"),
        sa.CheckConstraint("connector_type ~ '^[a-z][a-z0-9_]*$'", name="type_code_valid"),
        sa.CheckConstraint("btrim(display_name) <> ''", name="display_name_not_blank"),
        sa.CheckConstraint("slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'", name="slug_kebab_case"),
        sa.CheckConstraint("status IN ('draft', 'validating', 'active', 'degraded', 'auth_failed', 'paused', 'archived')", name="status_valid"),
        sa.CheckConstraint("acl_support IN ('none', 'partial', 'complete')", name="acl_support_valid"),
        sa.CheckConstraint("credential_status IN ('not_configured', 'validating', 'valid', 'expiring', 'expired', 'revoked', 'invalid')", name="credential_status_valid"),
        sa.CheckConstraint("jsonb_typeof(capabilities) = 'object'", name="capabilities_object"),
        sa.CheckConstraint("jsonb_typeof(safe_config) = 'object'", name="safe_config_object"),
        sa.CheckConstraint("config_schema_version > 0", name="config_version_positive"),
        sa.CheckConstraint("secret_reference IS NULL OR btrim(secret_reference) <> ''", name="secret_reference_not_blank"),
        sa.CheckConstraint("(status = 'archived' AND archived_at IS NOT NULL) OR (status <> 'archived' AND archived_at IS NULL)", name="archive_consistent"),
        sa.CheckConstraint("credential_expires_at IS NULL OR credential_expires_at > created_at", name="credential_expiry_after_created"),
    )
    op.create_index("ix_connectors_organization_id_status", "connectors", ["organization_id", "status"])
    op.create_index("ix_connectors_org_type_status", "connectors", ["organization_id", "connector_type", "status"])
    op.create_index("ix_connectors_org_credential_status", "connectors", ["organization_id", "credential_status"])

    op.create_table(
        "connector_scopes",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("knowledge_space_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=False),
        sa.Column("scope_type", sa.String(length=64), nullable=False),
        sa.Column("external_scope_key", sa.String(length=1024), nullable=False),
        sa.Column("access_mode", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'draft'")),
        sa.Column("safe_config", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("config_schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("last_validated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_connector_scopes"),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], name="fk_connector_scopes_organization", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id"], ["connectors.organization_id", "connectors.id"], name="fk_connector_scopes_connector_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "knowledge_space_id"], ["knowledge_spaces.organization_id", "knowledge_spaces.id"], name="fk_connector_scopes_space_tenant", ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["organization_id", "created_by_user_id"], ["users.organization_id", "users.id"], name="fk_connector_scopes_creator_tenant", ondelete="SET NULL (created_by_user_id)"),
        sa.UniqueConstraint("organization_id", "id", name="uq_connector_scopes_organization_id_id"),
        sa.UniqueConstraint("organization_id", "connector_id", "slug", name="uq_connector_scopes_connector_slug"),
        sa.UniqueConstraint("organization_id", "connector_id", "external_scope_key", name="uq_connector_scopes_connector_external_key"),
        sa.CheckConstraint("btrim(display_name) <> ''", name="display_name_not_blank"),
        sa.CheckConstraint("slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'", name="slug_kebab_case"),
        sa.CheckConstraint("scope_type ~ '^[a-z][a-z0-9_]*$'", name="type_code_valid"),
        sa.CheckConstraint("btrim(external_scope_key) <> ''", name="external_key_not_blank"),
        sa.CheckConstraint("access_mode IN ('platform_managed', 'source_acl', 'hybrid')", name="access_mode_valid"),
        sa.CheckConstraint("status IN ('draft', 'validating', 'active', 'invalid', 'paused', 'removed')", name="status_valid"),
        sa.CheckConstraint("jsonb_typeof(safe_config) = 'object'", name="safe_config_object"),
        sa.CheckConstraint("config_schema_version > 0", name="config_version_positive"),
        sa.CheckConstraint("(status = 'removed' AND removed_at IS NOT NULL) OR (status <> 'removed' AND removed_at IS NULL)", name="removal_consistent"),
    )
    op.create_index("ix_connector_scopes_org_connector_status", "connector_scopes", ["organization_id", "connector_id", "status"])
    op.create_index("ix_connector_scopes_org_space_status", "connector_scopes", ["organization_id", "knowledge_space_id", "status"])
    op.create_index("ix_connector_scopes_org_access_status", "connector_scopes", ["organization_id", "access_mode", "status"])


def downgrade() -> None:
    op.drop_index("ix_connector_scopes_org_access_status", table_name="connector_scopes")
    op.drop_index("ix_connector_scopes_org_space_status", table_name="connector_scopes")
    op.drop_index("ix_connector_scopes_org_connector_status", table_name="connector_scopes")
    op.drop_table("connector_scopes")
    op.drop_index("ix_connectors_org_credential_status", table_name="connectors")
    op.drop_index("ix_connectors_org_type_status", table_name="connectors")
    op.drop_index("ix_connectors_organization_id_status", table_name="connectors")
    op.drop_table("connectors")