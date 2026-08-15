"""Create tenant-safe connector synchronization tables.

Revision ID: 20260820_000011
Revises: 20260819_000010
Create Date: 2026-08-20 00:00:11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260820_000011"
down_revision = "20260819_000010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connector_sync_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("parent_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("mode", sa.String(32), nullable=False),
        sa.Column("trigger_type", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("initiated_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("run_metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        *[sa.Column(name, sa.Integer(), nullable=False, server_default=sa.text("0")) for name in ("items_discovered", "items_new", "items_changed", "items_unchanged", "items_deleted", "items_skipped", "items_succeeded", "items_failed")],
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_connector_sync_runs"),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], name="fk_sync_runs_organization", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id"], ["connectors.organization_id", "connectors.id"], name="fk_sync_runs_connector_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "connector_scope_id"], ["connector_scopes.organization_id", "connector_scopes.connector_id", "connector_scopes.id"], name="fk_sync_runs_scope_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "connector_scope_id", "parent_run_id"], ["connector_sync_runs.organization_id", "connector_sync_runs.connector_id", "connector_sync_runs.connector_scope_id", "connector_sync_runs.id"], name="fk_sync_runs_parent_tenant", ondelete="SET NULL (parent_run_id)"),
        sa.ForeignKeyConstraint(["organization_id", "initiated_by_user_id"], ["users.organization_id", "users.id"], name="fk_sync_runs_initiator_tenant", ondelete="SET NULL (initiated_by_user_id)"),
        sa.UniqueConstraint("organization_id", "connector_id", "connector_scope_id", "id", name="uq_sync_runs_scope_id"),
        sa.CheckConstraint("mode IN ('initial', 'incremental', 'retry', 'reconciliation')", name="mode_valid"),
        sa.CheckConstraint("trigger_type IN ('manual', 'scheduled', 'webhook', 'retry', 'system')", name="trigger_valid"),
        sa.CheckConstraint("status IN ('queued', 'running', 'cancelling', 'cancelled', 'completed', 'completed_with_errors', 'failed')", name="status_valid"),
        sa.CheckConstraint("(status = 'queued' AND started_at IS NULL AND finished_at IS NULL) OR (status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL) OR (status = 'cancelling' AND started_at IS NOT NULL AND cancel_requested_at IS NOT NULL AND finished_at IS NULL) OR (status IN ('cancelled', 'completed', 'completed_with_errors', 'failed') AND started_at IS NOT NULL AND finished_at IS NOT NULL)", name="timestamps_match_status"),
        sa.CheckConstraint("finished_at IS NULL OR finished_at >= started_at", name="finished_order_valid"),
        sa.CheckConstraint("heartbeat_at IS NULL OR started_at IS NULL OR heartbeat_at >= started_at", name="heartbeat_order_valid"),
        sa.CheckConstraint("cancel_requested_at IS NULL OR (started_at IS NOT NULL AND cancel_requested_at >= started_at)", name="cancel_order_valid"),
        sa.CheckConstraint("error_summary IS NULL OR btrim(error_summary) <> ''", name="error_summary_not_blank"),
        sa.CheckConstraint("jsonb_typeof(run_metadata) = 'object'", name="metadata_object"),
        sa.CheckConstraint("items_discovered >= 0 AND items_new >= 0 AND items_changed >= 0 AND items_unchanged >= 0 AND items_deleted >= 0 AND items_skipped >= 0 AND items_succeeded >= 0 AND items_failed >= 0", name="counters_nonnegative"),
        sa.CheckConstraint("parent_run_id IS NULL OR parent_run_id <> id", name="parent_not_self"),
    )
    op.create_index("ix_sync_runs_org_scope_created", "connector_sync_runs", ["organization_id", "connector_scope_id", "created_at"])
    op.create_index("ix_sync_runs_org_connector_status", "connector_sync_runs", ["organization_id", "connector_id", "status"])
    op.create_index("ix_sync_runs_org_status_created", "connector_sync_runs", ["organization_id", "status", "created_at"])
    op.create_index("uq_sync_runs_org_scope_active", "connector_sync_runs", ["organization_id", "connector_scope_id"], unique=True, postgresql_where=sa.text("status IN ('running', 'cancelling')"))

    op.create_table(
        "connector_sync_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sync_run_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("source_item_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source_item_key", sa.String(1024), nullable=False), sa.Column("change_type", sa.String(32), nullable=False),
        sa.Column("processing_status", sa.String(32), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("previous_checksum", sa.String(255), nullable=True), sa.Column("current_checksum", sa.String(255), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")), sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()), sa.PrimaryKeyConstraint("id", name="pk_connector_sync_items"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "connector_scope_id", "sync_run_id"], ["connector_sync_runs.organization_id", "connector_sync_runs.connector_id", "connector_sync_runs.connector_scope_id", "connector_sync_runs.id"], name="fk_sync_items_run_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "source_item_id"], ["source_items.organization_id", "source_items.connector_id", "source_items.id"], name="fk_sync_items_source_tenant", ondelete="SET NULL (source_item_id)"),
        sa.UniqueConstraint("organization_id", "sync_run_id", "id", name="uq_sync_items_run_id"), sa.UniqueConstraint("organization_id", "sync_run_id", "source_item_key", name="uq_sync_items_run_key"),
        sa.CheckConstraint("btrim(source_item_key) <> ''", name="key_not_blank"), sa.CheckConstraint("change_type IN ('new', 'changed', 'unchanged', 'deleted', 'unknown')", name="change_type_valid"),
        sa.CheckConstraint("processing_status IN ('pending', 'processing', 'succeeded', 'skipped', 'failed')", name="status_valid"), sa.CheckConstraint("previous_checksum IS NULL OR btrim(previous_checksum) <> ''", name="previous_checksum_not_blank"),
        sa.CheckConstraint("current_checksum IS NULL OR btrim(current_checksum) <> ''", name="current_checksum_not_blank"), sa.CheckConstraint("attempt_count >= 0", name="attempt_nonnegative"),
        sa.CheckConstraint("(processing_status = 'pending' AND started_at IS NULL AND finished_at IS NULL) OR (processing_status = 'processing' AND started_at IS NOT NULL AND finished_at IS NULL) OR (processing_status IN ('succeeded', 'skipped', 'failed') AND started_at IS NOT NULL AND finished_at IS NOT NULL)", name="timestamps_match_status"),
        sa.CheckConstraint("finished_at IS NULL OR finished_at >= started_at", name="finished_order_valid"),
    )
    op.create_index("ix_sync_items_org_run_status", "connector_sync_items", ["organization_id", "sync_run_id", "processing_status"])
    op.create_index("ix_sync_items_org_source", "connector_sync_items", ["organization_id", "source_item_id"])
    op.create_index("ix_sync_items_org_scope_key", "connector_sync_items", ["organization_id", "connector_scope_id", "source_item_key"])

    op.create_table(
        "connector_sync_errors",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sync_run_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("sync_item_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("error_category", sa.String(32), nullable=False), sa.Column("error_code", sa.String(128), nullable=False), sa.Column("message", sa.Text(), nullable=False),
        sa.Column("retryable", sa.Boolean(), nullable=False), sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")), sa.Column("retry_after_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True), sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()), sa.PrimaryKeyConstraint("id", name="pk_connector_sync_errors"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "connector_scope_id", "sync_run_id"], ["connector_sync_runs.organization_id", "connector_sync_runs.connector_id", "connector_sync_runs.connector_scope_id", "connector_sync_runs.id"], name="fk_sync_errors_run_tenant", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id", "sync_run_id", "sync_item_id"], ["connector_sync_items.organization_id", "connector_sync_items.sync_run_id", "connector_sync_items.id"], name="fk_sync_errors_item_tenant", ondelete="SET NULL (sync_item_id)"),
        sa.CheckConstraint("error_category IN ('configuration', 'authentication', 'authorization', 'rate_limit', 'source_read', 'extraction', 'persistence', 'embedding', 'permission', 'internal')", name="category_valid"),
        sa.CheckConstraint("error_code ~ '^[a-z][a-z0-9_]*$'", name="code_valid"), sa.CheckConstraint("btrim(message) <> ''", name="message_not_blank"),
        sa.CheckConstraint("attempt_number > 0", name="attempt_positive"), sa.CheckConstraint("jsonb_typeof(details) = 'object'", name="details_object"),
        sa.CheckConstraint("retry_after_at IS NULL OR retry_after_at >= occurred_at", name="retry_order_valid"), sa.CheckConstraint("resolved_at IS NULL OR resolved_at >= occurred_at", name="resolved_order_valid"),
    )
    op.create_index("ix_sync_errors_org_run_occurred", "connector_sync_errors", ["organization_id", "sync_run_id", "occurred_at"])
    op.create_index("ix_sync_errors_org_item_occurred", "connector_sync_errors", ["organization_id", "sync_item_id", "occurred_at"])
    op.create_index("ix_sync_errors_org_retry_resolved", "connector_sync_errors", ["organization_id", "retryable", "resolved_at"])

    op.create_table(
        "connector_sync_cursors",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_by_run_id", postgresql.UUID(as_uuid=True), nullable=False), sa.Column("cursor_version", sa.BigInteger(), nullable=False),
        sa.Column("cursor_type", sa.String(64), nullable=False), sa.Column("state", sa.String(32), nullable=False), sa.Column("safe_cursor", postgresql.JSONB(), nullable=True),
        sa.Column("secret_reference", sa.String(1024), nullable=True), sa.Column("source_watermark_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=False), sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()), sa.PrimaryKeyConstraint("id", name="pk_connector_sync_cursors"),
        sa.ForeignKeyConstraint(["organization_id", "connector_id", "connector_scope_id", "created_by_run_id"], ["connector_sync_runs.organization_id", "connector_sync_runs.connector_id", "connector_sync_runs.connector_scope_id", "connector_sync_runs.id"], name="fk_sync_cursors_run_tenant", ondelete="RESTRICT"),
        sa.UniqueConstraint("organization_id", "connector_scope_id", "cursor_version", name="uq_sync_cursors_scope_version"),
        sa.CheckConstraint("cursor_version > 0", name="version_positive"), sa.CheckConstraint("cursor_type ~ '^[a-z][a-z0-9_]*$'", name="type_code_valid"),
        sa.CheckConstraint("state IN ('active', 'superseded', 'invalid')", name="state_valid"),
        sa.CheckConstraint("(safe_cursor IS NOT NULL AND secret_reference IS NULL) OR (safe_cursor IS NULL AND secret_reference IS NOT NULL)", name="storage_exactly_one"),
        sa.CheckConstraint("safe_cursor IS NULL OR jsonb_typeof(safe_cursor) = 'object'", name="safe_cursor_object"),
        sa.CheckConstraint("secret_reference IS NULL OR btrim(secret_reference) <> ''", name="secret_reference_not_blank"),
        sa.CheckConstraint("(state = 'active' AND retired_at IS NULL) OR (state IN ('superseded', 'invalid') AND retired_at IS NOT NULL)", name="retirement_matches_state"),
        sa.CheckConstraint("retired_at IS NULL OR retired_at >= activated_at", name="retired_order_valid"),
    )
    op.create_index("uq_sync_cursors_org_scope_active", "connector_sync_cursors", ["organization_id", "connector_scope_id"], unique=True, postgresql_where=sa.text("state = 'active'"))
    op.create_index("ix_sync_cursors_org_scope_version", "connector_sync_cursors", ["organization_id", "connector_scope_id", "cursor_version"])


def downgrade() -> None:
    op.drop_index("ix_sync_cursors_org_scope_version", table_name="connector_sync_cursors")
    op.drop_index("uq_sync_cursors_org_scope_active", table_name="connector_sync_cursors")
    op.drop_table("connector_sync_cursors")
    for name in ("ix_sync_errors_org_retry_resolved", "ix_sync_errors_org_item_occurred", "ix_sync_errors_org_run_occurred"):
        op.drop_index(name, table_name="connector_sync_errors")
    op.drop_table("connector_sync_errors")
    for name in ("ix_sync_items_org_scope_key", "ix_sync_items_org_source", "ix_sync_items_org_run_status"):
        op.drop_index(name, table_name="connector_sync_items")
    op.drop_table("connector_sync_items")
    for name in ("uq_sync_runs_org_scope_active", "ix_sync_runs_org_status_created", "ix_sync_runs_org_connector_status", "ix_sync_runs_org_scope_created"):
        op.drop_index(name, table_name="connector_sync_runs")
    op.drop_table("connector_sync_runs")
