"""Create durable connector synchronization execution-control jobs.

Revision ID: 20260823_000014
Revises: 20260822_000013
Create Date: 2026-08-23 00:00:14
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260823_000014"
down_revision = "20260822_000013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connector_sync_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("mode", sa.String(32), nullable=False),
        sa.Column("trigger_type", sa.String(32), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default=sa.text("'queued'")),
        sa.Column("requested_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("priority", sa.SmallInteger(), nullable=False, server_default=sa.text("100")),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default=sa.text("3")),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True, server_default=sa.func.now()),
        sa.Column("lease_owner", sa.String(255), nullable=True),
        sa.Column("lease_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("fencing_token", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("lease_acquired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_requested_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("cancel_reason_code", sa.String(64), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_category", sa.String(32), nullable=True),
        sa.Column("last_error_code", sa.String(128), nullable=True),
        sa.Column("last_error_summary", sa.String(512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_connector_sync_jobs"),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_sync_jobs_organization", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id"],
            ["connectors.organization_id", "connectors.id"],
            name="fk_sync_jobs_connector_tenant", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id", "connector_scope_id"],
            ["connector_scopes.organization_id", "connector_scopes.connector_id", "connector_scopes.id"],
            name="fk_sync_jobs_scope_tenant", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "requested_by_user_id"],
            ["users.organization_id", "users.id"],
            name="fk_sync_jobs_requester_tenant", ondelete="SET NULL (requested_by_user_id)",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "cancel_requested_by_user_id"],
            ["users.organization_id", "users.id"],
            name="fk_sync_jobs_cancel_requester_tenant",
            ondelete="SET NULL (cancel_requested_by_user_id)",
        ),
        sa.UniqueConstraint("organization_id", "id", name="uq_sync_jobs_organization_id_id"),
        sa.UniqueConstraint(
            "organization_id", "connector_id", "connector_scope_id", "id",
            name="uq_sync_jobs_scope_id",
        ),
        sa.UniqueConstraint("lease_id", name="uq_sync_jobs_lease_id"),
        sa.CheckConstraint("mode IN ('initial', 'incremental', 'reconciliation')", name="mode_valid"),
        sa.CheckConstraint(
            "trigger_type IN ('manual', 'scheduled', 'webhook', 'system')", name="trigger_valid"
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'retry_wait', 'succeeded', 'failed', 'cancelled')",
            name="status_valid",
        ),
        sa.CheckConstraint("attempt_count >= 0", name="attempt_count_nonnegative"),
        sa.CheckConstraint("max_attempts > 0", name="max_attempts_positive"),
        sa.CheckConstraint("attempt_count <= max_attempts", name="attempt_count_within_max"),
        sa.CheckConstraint("fencing_token >= 0", name="fencing_token_nonnegative"),
        sa.CheckConstraint("fencing_token = attempt_count", name="fencing_matches_attempt_count"),
        sa.CheckConstraint(
            "(status IN ('queued', 'retry_wait') AND next_attempt_at IS NOT NULL) OR "
            "(status NOT IN ('queued', 'retry_wait') AND next_attempt_at IS NULL)",
            name="availability_matches_status",
        ),
        sa.CheckConstraint(
            "status <> 'retry_wait' OR (attempt_count > 0 AND attempt_count < max_attempts)",
            name="retry_attempt_available",
        ),
        sa.CheckConstraint("status <> 'running' OR attempt_count > 0", name="running_attempt_positive"),
        sa.CheckConstraint(
            "status NOT IN ('succeeded', 'failed') OR attempt_count > 0",
            name="executed_terminal_attempt_positive",
        ),
        sa.CheckConstraint(
            "(status IN ('queued', 'running', 'retry_wait') AND completed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed', 'cancelled') AND completed_at IS NOT NULL)",
            name="completion_matches_status",
        ),
        sa.CheckConstraint(
            "(status = 'running' AND lease_owner IS NOT NULL AND lease_id IS NOT NULL "
            "AND lease_acquired_at IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND heartbeat_at IS NOT NULL AND fencing_token > 0) OR "
            "(status <> 'running' AND lease_owner IS NULL AND lease_id IS NULL "
            "AND lease_acquired_at IS NULL AND lease_expires_at IS NULL AND heartbeat_at IS NULL)",
            name="lease_matches_status",
        ),
        sa.CheckConstraint("lease_owner IS NULL OR btrim(lease_owner) <> ''", name="lease_owner_not_blank"),
        sa.CheckConstraint(
            "lease_expires_at IS NULL OR lease_expires_at > lease_acquired_at",
            name="lease_expiry_after_acquired",
        ),
        sa.CheckConstraint(
            "heartbeat_at IS NULL OR heartbeat_at >= lease_acquired_at",
            name="heartbeat_after_acquired",
        ),
        sa.CheckConstraint(
            "heartbeat_at IS NULL OR heartbeat_at < lease_expires_at", name="heartbeat_before_expiry"
        ),
        sa.CheckConstraint(
            "cancel_requested_by_user_id IS NULL OR cancel_requested_at IS NOT NULL",
            name="cancel_requester_requires_request",
        ),
        sa.CheckConstraint(
            "cancel_reason_code IS NULL OR cancel_requested_at IS NOT NULL",
            name="cancel_reason_requires_request",
        ),
        sa.CheckConstraint(
            "cancel_reason_code IS NULL OR cancel_reason_code ~ '^[a-z][a-z0-9_]*$'",
            name="cancel_reason_code_valid",
        ),
        sa.CheckConstraint(
            "status <> 'cancelled' OR cancel_requested_at IS NOT NULL",
            name="cancelled_requires_request",
        ),
        sa.CheckConstraint(
            "cancel_requested_at IS NULL OR cancel_requested_at >= created_at",
            name="cancel_request_after_created",
        ),
        sa.CheckConstraint(
            "completed_at IS NULL OR completed_at >= created_at", name="completion_after_created"
        ),
        sa.CheckConstraint(
            "cancel_requested_at IS NULL OR completed_at IS NULL OR completed_at >= cancel_requested_at",
            name="completion_after_cancel_request",
        ),
        sa.CheckConstraint(
            "(last_error_category IS NULL AND last_error_code IS NULL AND last_error_summary IS NULL) OR "
            "(last_error_category IS NOT NULL AND last_error_code IS NOT NULL)",
            name="last_error_fields_consistent",
        ),
        sa.CheckConstraint(
            "last_error_category IS NULL OR last_error_category IN "
            "('configuration', 'authentication', 'authorization', 'rate_limit', 'source_read', "
            "'extraction', 'persistence', 'embedding', 'permission', 'internal')",
            name="last_error_category_valid",
        ),
        sa.CheckConstraint(
            "last_error_code IS NULL OR last_error_code ~ '^[a-z][a-z0-9_]*$'",
            name="last_error_code_valid",
        ),
        sa.CheckConstraint(
            "last_error_summary IS NULL OR btrim(last_error_summary) <> ''",
            name="last_error_summary_not_blank",
        ),
    )
    op.create_index(
        "uq_sync_jobs_org_scope_nonterminal", "connector_sync_jobs",
        ["organization_id", "connector_scope_id"], unique=True,
        postgresql_where=sa.text("status IN ('queued', 'running', 'retry_wait')"),
    )
    op.create_index(
        "ix_sync_jobs_ready", "connector_sync_jobs",
        ["status", "priority", "next_attempt_at", "created_at", "id"],
    )
    op.create_index(
        "ix_sync_jobs_expired_leases", "connector_sync_jobs", ["status", "lease_expires_at"],
        postgresql_where=sa.text("status = 'running'"),
    )
    op.create_index(
        "ix_sync_jobs_cancellation_requests", "connector_sync_jobs", ["status", "cancel_requested_at"],
        postgresql_where=sa.text("cancel_requested_at IS NOT NULL AND completed_at IS NULL"),
    )
    op.create_index(
        "ix_sync_jobs_org_scope_created", "connector_sync_jobs",
        ["organization_id", "connector_scope_id", "created_at", "id"],
    )
    op.create_index(
        "ix_sync_jobs_org_connector_created", "connector_sync_jobs",
        ["organization_id", "connector_id", "created_at", "id"],
    )

    op.add_column(
        "connector_sync_runs",
        sa.Column("sync_job_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "connector_sync_runs", sa.Column("job_attempt_number", sa.Integer(), nullable=True)
    )
    op.create_foreign_key(
        "fk_sync_runs_job_tenant",
        "connector_sync_runs",
        "connector_sync_jobs",
        ["organization_id", "connector_id", "connector_scope_id", "sync_job_id"],
        ["organization_id", "connector_id", "connector_scope_id", "id"],
        ondelete="CASCADE",
    )
    op.create_unique_constraint(
        "uq_sync_runs_job_attempt",
        "connector_sync_runs",
        ["organization_id", "sync_job_id", "job_attempt_number"],
    )
    op.create_check_constraint(
        "job_attempt_consistent",
        "connector_sync_runs",
        "(sync_job_id IS NULL AND job_attempt_number IS NULL) OR "
        "(sync_job_id IS NOT NULL AND job_attempt_number IS NOT NULL AND job_attempt_number > 0)",
    )


def downgrade() -> None:
    op.drop_constraint("job_attempt_consistent", "connector_sync_runs", type_="check")
    op.drop_constraint("uq_sync_runs_job_attempt", "connector_sync_runs", type_="unique")
    op.drop_constraint("fk_sync_runs_job_tenant", "connector_sync_runs", type_="foreignkey")
    op.drop_column("connector_sync_runs", "job_attempt_number")
    op.drop_column("connector_sync_runs", "sync_job_id")

    for index_name in (
        "ix_sync_jobs_org_connector_created",
        "ix_sync_jobs_org_scope_created",
        "ix_sync_jobs_cancellation_requests",
        "ix_sync_jobs_expired_leases",
        "ix_sync_jobs_ready",
        "uq_sync_jobs_org_scope_nonterminal",
    ):
        op.drop_index(index_name, table_name="connector_sync_jobs")
    op.drop_table("connector_sync_jobs")