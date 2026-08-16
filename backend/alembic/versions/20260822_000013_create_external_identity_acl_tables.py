"""Create tenant-safe external identity and source ACL tables.

Revision ID: 20260822_000013
Revises: 20260821_000012
Create Date: 2026-08-22 00:00:13
"""
from __future__ import annotations
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260822_000013"
down_revision = "20260821_000012"
branch_labels = None
depends_on = None
RUN_KEY = "uq_sync_runs_org_connector_id"
ITEM_KEY = "uq_sync_items_org_connector_run_id"


def upgrade() -> None:
    op.create_unique_constraint(RUN_KEY, "connector_sync_runs", ["organization_id", "connector_id", "id"])
    op.create_unique_constraint(ITEM_KEY, "connector_sync_items", ["organization_id", "connector_id", "sync_run_id", "id"])
    op.create_table(
        "external_principals",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("principal_key", sa.String(1024), nullable=False),
        sa.Column("principal_type", sa.String(32), nullable=False), sa.Column("display_name", sa.String(255), nullable=True),
        sa.Column("normalized_email", sa.String(320), nullable=True), sa.Column("normalized_domain", sa.String(255), nullable=True),
        sa.Column("provider_login", sa.String(255), nullable=True), sa.Column("lifecycle", sa.String(32), nullable=False, server_default=sa.text("'active'")),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False), sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True), sa.Column("provider_modified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("metadata_schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_external_principals"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id"], ["connectors.organization_id", "connectors.id"], name="fk_external_principals_connector", ondelete="CASCADE"),
        sa.UniqueConstraint("organization_id", "connector_id", "id", name="uq_external_principals_connector_id"),
        sa.UniqueConstraint("organization_id", "connector_id", "principal_key", name="uq_external_principals_connector_key"),
        sa.CheckConstraint("btrim(principal_key) <> ''", name="key_not_blank"),
        sa.CheckConstraint("principal_type IN ('user', 'group', 'domain', 'anyone', 'service_account')", name="type_valid"),
        sa.CheckConstraint("normalized_email IS NULL OR normalized_email = lower(btrim(normalized_email))", name="email_normalized"),
        sa.CheckConstraint("normalized_domain IS NULL OR normalized_domain = lower(btrim(normalized_domain))", name="domain_normalized"),
        sa.CheckConstraint("provider_login IS NULL OR btrim(provider_login) <> ''", name="login_not_blank"),
        sa.CheckConstraint("principal_type <> 'anyone' OR (normalized_email IS NULL AND normalized_domain IS NULL AND provider_login IS NULL)", name="anyone_fields_empty"),
        sa.CheckConstraint("principal_type <> 'domain' OR (normalized_domain IS NOT NULL AND normalized_email IS NULL)", name="domain_fields_valid"),
        sa.CheckConstraint("lifecycle IN ('active', 'disabled', 'deleted', 'unknown')", name="lifecycle_valid"),
        sa.CheckConstraint("(lifecycle = 'deleted' AND deleted_at IS NOT NULL) OR (lifecycle <> 'deleted' AND deleted_at IS NULL)", name="deletion_consistent"),
        sa.CheckConstraint("last_seen_at >= first_seen_at", name="seen_order_valid"),
        sa.CheckConstraint("jsonb_typeof(metadata) = 'object'", name="metadata_object"),
        sa.CheckConstraint("metadata_schema_version > 0", name="metadata_version_positive"),
    )
    op.create_index("ix_external_principals_org_connector_email", "external_principals", ["organization_id", "connector_id", "normalized_email"])
    op.create_index("ix_external_principals_org_connector_lifecycle", "external_principals", ["organization_id", "connector_id", "lifecycle"])

    op.create_table(
        "user_external_identity_links",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("external_principal_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("verification_method", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default=sa.text("'pending'")), sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True), sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("evidence", postgresql.JSONB(), nullable=True), sa.Column("evidence_schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()), sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_user_external_identity_links"),
        sa.ForeignKeyConstraint(["organization_id", "user_id"], ["users.organization_id", "users.id"], name="fk_identity_links_user_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "external_principal_id"], ["external_principals.organization_id", "external_principals.connector_id", "external_principals.id"], name="fk_identity_links_principal_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "created_by_user_id"], ["users.organization_id", "users.id"], name="fk_identity_links_creator_tenant", ondelete="SET NULL (created_by_user_id)"),
        sa.UniqueConstraint("organization_id", "user_id", "external_principal_id", name="uq_identity_links_user_principal"),
        sa.CheckConstraint("verification_method IN ('admin', 'sso_subject', 'verified_email', 'provider_identity')", name="method_valid"),
        sa.CheckConstraint("status IN ('pending', 'verified', 'revoked')", name="status_valid"),
        sa.CheckConstraint("(status = 'pending' AND verified_at IS NULL AND revoked_at IS NULL) OR (status = 'verified' AND verified_at IS NOT NULL AND revoked_at IS NULL) OR (status = 'revoked' AND revoked_at IS NOT NULL)", name="timestamps_match_status"),
        sa.CheckConstraint("evidence IS NULL OR jsonb_typeof(evidence) = 'object'", name="evidence_object"),
        sa.CheckConstraint("evidence_schema_version > 0", name="evidence_version_positive"),
    )
    op.create_index("uq_identity_links_verified_principal", "user_external_identity_links", ["organization_id", "external_principal_id"], unique=True, postgresql_where=sa.text("status = 'verified'"))
    op.create_index("ix_identity_links_org_user_verified", "user_external_identity_links", ["organization_id", "user_id"], postgresql_where=sa.text("status = 'verified'"))

    op.create_table(
        "external_directory_states",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default=sa.text("'not_started'")), sa.Column("current_generation", sa.BigInteger(), nullable=True),
        sa.Column("in_progress_generation", sa.BigInteger(), nullable=True), sa.Column("started_at", sa.DateTime(timezone=True), nullable=True), sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_successful_at", sa.DateTime(timezone=True), nullable=True), sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_category", sa.String(64), nullable=True), sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()), sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_external_directory_states"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id"], ["connectors.organization_id", "connectors.id"], name="fk_directory_states_connector", ondelete="CASCADE"),
        sa.UniqueConstraint("organization_id", "connector_id", name="uq_directory_states_connector"),
        sa.CheckConstraint("status IN ('not_started', 'syncing', 'complete', 'stale', 'failed')", name="status_valid"),
        sa.CheckConstraint("current_generation IS NULL OR current_generation > 0", name="current_generation_positive"),
        sa.CheckConstraint("in_progress_generation IS NULL OR in_progress_generation > 0", name="progress_generation_positive"),
        sa.CheckConstraint("current_generation IS NULL OR in_progress_generation IS NULL OR in_progress_generation > current_generation", name="generation_order_valid"),
        sa.CheckConstraint("(error_category IS NULL AND error_code IS NULL) OR (error_category ~ '^[a-z][a-z0-9_]*$' AND error_code ~ '^[a-z][a-z0-9_]*$')", name="error_pair_valid"),
        sa.CheckConstraint("(status = 'not_started' AND current_generation IS NULL AND in_progress_generation IS NULL AND started_at IS NULL AND completed_at IS NULL) OR (status = 'syncing' AND in_progress_generation IS NOT NULL AND started_at IS NOT NULL) OR (status = 'complete' AND current_generation IS NOT NULL AND completed_at IS NOT NULL AND last_successful_at IS NOT NULL) OR (status = 'failed' AND error_category IS NOT NULL AND error_code IS NOT NULL) OR status = 'stale'", name="state_consistent"),
    )
    op.create_index("ix_directory_states_org_connector_status", "external_directory_states", ["organization_id", "connector_id", "status"])

    op.create_table(
        "external_group_memberships",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("group_principal_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("member_principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("first_seen_generation", sa.BigInteger(), nullable=False), sa.Column("last_seen_generation", sa.BigInteger(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False), sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False), sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lifecycle", sa.String(32), nullable=False, server_default=sa.text("'active'")), sa.Column("is_direct", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")), sa.Column("metadata_schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()), sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_external_group_memberships"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id"], ["connectors.organization_id", "connectors.id"], name="fk_group_memberships_connector", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "group_principal_id"], ["external_principals.organization_id", "external_principals.connector_id", "external_principals.id"], name="fk_group_memberships_group_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "member_principal_id"], ["external_principals.organization_id", "external_principals.connector_id", "external_principals.id"], name="fk_group_memberships_member_tenant", ondelete="CASCADE"),
        sa.UniqueConstraint("organization_id", "connector_id", "group_principal_id", "member_principal_id", name="uq_group_memberships_edge"),
        sa.CheckConstraint("group_principal_id <> member_principal_id", name="not_self"),
        sa.CheckConstraint("first_seen_generation > 0 AND last_seen_generation > 0 AND last_seen_generation >= first_seen_generation", name="generation_valid"),
        sa.CheckConstraint("last_seen_at >= first_seen_at", name="seen_order_valid"),
        sa.CheckConstraint("lifecycle IN ('active', 'removed')", name="lifecycle_valid"),
        sa.CheckConstraint("(lifecycle = 'active' AND removed_at IS NULL) OR (lifecycle = 'removed' AND removed_at IS NOT NULL)", name="removal_consistent"),
        sa.CheckConstraint("jsonb_typeof(metadata) = 'object'", name="metadata_object"), sa.CheckConstraint("metadata_schema_version > 0", name="metadata_version_positive"),
    )
    op.create_index("ix_group_memberships_org_group_active", "external_group_memberships", ["organization_id", "group_principal_id"], postgresql_where=sa.text("lifecycle = 'active'"))
    op.create_index("ix_group_memberships_org_member", "external_group_memberships", ["organization_id", "member_principal_id", "lifecycle"])

    op.create_table(
        "source_acl_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("source_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("snapshot_version", sa.BigInteger(), nullable=False), sa.Column("connector_sync_run_id", postgresql.UUID(as_uuid=True), nullable=True), sa.Column("connector_sync_item_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("status", sa.String(32), nullable=False), sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.text("false")), sa.Column("source_revision", sa.String(255), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False), sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True), sa.Column("captured_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("entry_count", sa.Integer(), nullable=False, server_default=sa.text("0")), sa.Column("inheritance_completeness", sa.String(32), nullable=False, server_default=sa.text("'unknown'")),
        sa.Column("error_category", sa.String(64), nullable=True), sa.Column("error_code", sa.String(128), nullable=True),
        sa.Column("summary", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")), sa.Column("summary_schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()), sa.PrimaryKeyConstraint("id", name="pk_source_acl_snapshots"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "source_item_id"], ["source_items.organization_id", "source_items.connector_id", "source_items.id"], name="fk_acl_snapshots_source_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "connector_sync_run_id"], ["connector_sync_runs.organization_id", "connector_sync_runs.connector_id", "connector_sync_runs.id"], name="fk_acl_snapshots_run_tenant", ondelete="SET NULL (connector_sync_run_id)"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "connector_sync_run_id", "connector_sync_item_id"], ["connector_sync_items.organization_id", "connector_sync_items.connector_id", "connector_sync_items.sync_run_id", "connector_sync_items.id"], name="fk_acl_snapshots_item_tenant", ondelete="SET NULL (connector_sync_item_id)"),
        sa.UniqueConstraint("organization_id", "connector_id", "source_item_id", "id", name="uq_acl_snapshots_source_id"), sa.UniqueConstraint("organization_id", "source_item_id", "snapshot_version", name="uq_acl_snapshots_source_version"),
        sa.CheckConstraint("snapshot_version > 0", name="version_positive"), sa.CheckConstraint("status IN ('building', 'complete', 'failed', 'stale')", name="status_valid"),
        sa.CheckConstraint("entry_count >= 0", name="entry_count_nonnegative"), sa.CheckConstraint("inheritance_completeness IN ('complete', 'partial', 'unknown')", name="inheritance_valid"),
        sa.CheckConstraint("connector_sync_item_id IS NULL OR connector_sync_run_id IS NOT NULL", name="item_requires_run"),
        sa.CheckConstraint("(error_category IS NULL AND error_code IS NULL) OR (error_category ~ '^[a-z][a-z0-9_]*$' AND error_code ~ '^[a-z][a-z0-9_]*$')", name="error_pair_valid"),
        sa.CheckConstraint("(status = 'building' AND completed_at IS NULL AND captured_at IS NULL AND NOT is_current) OR (status = 'complete' AND completed_at IS NOT NULL AND captured_at IS NOT NULL AND error_category IS NULL AND error_code IS NULL AND inheritance_completeness = 'complete') OR (status = 'failed' AND completed_at IS NOT NULL AND error_category IS NOT NULL AND error_code IS NOT NULL AND NOT is_current) OR (status = 'stale' AND NOT is_current)", name="state_consistent"),
        sa.CheckConstraint("NOT is_current OR (status = 'complete' AND inheritance_completeness = 'complete')", name="current_complete_only"),
        sa.CheckConstraint("completed_at IS NULL OR completed_at >= started_at", name="completed_order_valid"), sa.CheckConstraint("captured_at IS NULL OR captured_at >= started_at", name="captured_order_valid"),
        sa.CheckConstraint("jsonb_typeof(summary) = 'object'", name="summary_object"), sa.CheckConstraint("summary_schema_version > 0", name="summary_version_positive"),
    )
    op.create_index("uq_acl_snapshots_current_source", "source_acl_snapshots", ["organization_id", "source_item_id"], unique=True, postgresql_where=sa.text("is_current"))
    op.create_index("ix_acl_snapshots_org_source_version", "source_acl_snapshots", ["organization_id", "source_item_id", "snapshot_version"])

    op.create_table(
        "source_acl_entries",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_item_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("acl_snapshot_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("external_principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_permission_key", sa.String(1024), nullable=True), sa.Column("effect", sa.String(16), nullable=False), sa.Column("permission_level", sa.String(32), nullable=False),
        sa.Column("grants_read", sa.Boolean(), nullable=False, server_default=sa.text("false")), sa.Column("inherited", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("inherited_from_source_item_key", sa.String(1024), nullable=True), sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")), sa.Column("metadata_schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()), sa.PrimaryKeyConstraint("id", name="pk_source_acl_entries"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id"], ["connectors.organization_id", "connectors.id"], name="fk_acl_entries_connector", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "source_item_id", "acl_snapshot_id"], ["source_acl_snapshots.organization_id", "source_acl_snapshots.connector_id", "source_acl_snapshots.source_item_id", "source_acl_snapshots.id"], name="fk_acl_entries_snapshot_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "external_principal_id"], ["external_principals.organization_id", "external_principals.connector_id", "external_principals.id"], name="fk_acl_entries_principal_tenant", ondelete="RESTRICT"),
        sa.CheckConstraint("provider_permission_key IS NULL OR btrim(provider_permission_key) <> ''", name="provider_key_not_blank"), sa.CheckConstraint("effect IN ('allow', 'deny')", name="effect_valid"),
        sa.CheckConstraint("permission_level IN ('viewer', 'commenter', 'editor', 'owner', 'unknown')", name="permission_valid"), sa.CheckConstraint("effect <> 'deny' OR NOT grants_read", name="deny_not_grant"),
        sa.CheckConstraint("permission_level <> 'unknown' OR NOT grants_read", name="unknown_not_grant"), sa.CheckConstraint("effect <> 'allow' OR permission_level = 'unknown' OR grants_read", name="known_allow_grants_read"),
        sa.CheckConstraint("inherited_from_source_item_key IS NULL OR btrim(inherited_from_source_item_key) <> ''", name="inherited_key_not_blank"),
        sa.CheckConstraint("jsonb_typeof(metadata) = 'object'", name="metadata_object"), sa.CheckConstraint("metadata_schema_version > 0", name="metadata_version_positive"),
    )
    op.create_index("uq_acl_entries_normalized", "source_acl_entries", ["organization_id", "acl_snapshot_id", "external_principal_id", "effect", "permission_level", "inherited", sa.text("coalesce(provider_permission_key, '')"), sa.text("coalesce(inherited_from_source_item_key, '')")], unique=True)
    op.create_index("ix_acl_entries_org_snapshot", "source_acl_entries", ["organization_id", "acl_snapshot_id"])
    op.create_index("ix_acl_entries_org_principal", "source_acl_entries", ["organization_id", "external_principal_id"])
    op.create_index("ix_acl_entries_current_readable", "source_acl_entries", ["organization_id", "source_item_id", "external_principal_id"], postgresql_where=sa.text("effect = 'allow' AND grants_read"))


def downgrade() -> None:
    for name in ("ix_acl_entries_current_readable", "ix_acl_entries_org_principal", "ix_acl_entries_org_snapshot", "uq_acl_entries_normalized"):
        op.drop_index(name, table_name="source_acl_entries")
    op.drop_table("source_acl_entries")
    op.drop_index("ix_acl_snapshots_org_source_version", table_name="source_acl_snapshots")
    op.drop_index("uq_acl_snapshots_current_source", table_name="source_acl_snapshots")
    op.drop_table("source_acl_snapshots")
    op.drop_index("ix_group_memberships_org_member", table_name="external_group_memberships")
    op.drop_index("ix_group_memberships_org_group_active", table_name="external_group_memberships")
    op.drop_table("external_group_memberships")
    op.drop_index("ix_directory_states_org_connector_status", table_name="external_directory_states")
    op.drop_table("external_directory_states")
    op.drop_index("ix_identity_links_org_user_verified", table_name="user_external_identity_links")
    op.drop_index("uq_identity_links_verified_principal", table_name="user_external_identity_links")
    op.drop_table("user_external_identity_links")
    op.drop_index("ix_external_principals_org_connector_lifecycle", table_name="external_principals")
    op.drop_index("ix_external_principals_org_connector_email", table_name="external_principals")
    op.drop_table("external_principals")
    op.drop_constraint(ITEM_KEY, "connector_sync_items", type_="unique")
    op.drop_constraint(RUN_KEY, "connector_sync_runs", type_="unique")
