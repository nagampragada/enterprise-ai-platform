"""Create recurring connector synchronization schedules.

Revision ID: 20260824_000015
Revises: 20260823_000014
Create Date: 2026-08-24 00:00:15
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260824_000015"
down_revision = "20260823_000014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connector_sync_schedules",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default=sa.text("'active'")),
        sa.Column("interval_seconds", sa.Integer(), nullable=False),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_enqueued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("pause_reason_code", sa.String(64), nullable=True),
        sa.Column("paused_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_connector_sync_schedules"),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_sync_schedules_organization", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id", "connector_scope_id"],
            ["connector_scopes.organization_id", "connector_scopes.connector_id", "connector_scopes.id"],
            name="fk_sync_schedules_scope_tenant", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id", "connector_scope_id", "last_job_id"],
            ["connector_sync_jobs.organization_id", "connector_sync_jobs.connector_id", "connector_sync_jobs.connector_scope_id", "connector_sync_jobs.id"],
            name="fk_sync_schedules_last_job_tenant", ondelete="SET NULL (last_job_id)",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "created_by_user_id"],
            ["users.organization_id", "users.id"],
            name="fk_sync_schedules_creator_tenant", ondelete="SET NULL (created_by_user_id)",
        ),
        sa.UniqueConstraint("organization_id", "id", name="uq_sync_schedules_organization_id_id"),
        sa.UniqueConstraint(
            "organization_id", "connector_id", "connector_scope_id",
            name="uq_sync_schedules_scope",
        ),
        sa.CheckConstraint("status IN ('active', 'paused')", name="schedule_status_valid"),
        sa.CheckConstraint(
            "interval_seconds BETWEEN 900 AND 2592000", name="schedule_interval_valid"
        ),
        sa.CheckConstraint(
            "(status = 'active' AND paused_at IS NULL AND pause_reason_code IS NULL) OR "
            "(status = 'paused' AND paused_at IS NOT NULL)",
            name="schedule_pause_consistent",
        ),
        sa.CheckConstraint(
            "pause_reason_code IS NULL OR pause_reason_code ~ '^[a-z][a-z0-9_]*$'",
            name="schedule_pause_reason_valid",
        ),
        sa.CheckConstraint(
            "last_enqueued_at IS NULL OR last_due_at IS NOT NULL",
            name="schedule_enqueue_requires_due",
        ),
        sa.CheckConstraint("updated_at >= created_at", name="schedule_updated_after_created"),
    )
    op.create_index(
        "ix_sync_schedules_due", "connector_sync_schedules", ["next_run_at", "id"],
        postgresql_where=sa.text("status = 'active'"),
    )
    op.create_index(
        "ix_sync_schedules_org_scope", "connector_sync_schedules",
        ["organization_id", "connector_scope_id"],
    )
    op.create_index(
        "ix_sync_schedules_status_next", "connector_sync_schedules", ["status", "next_run_at"]
    )
    op.create_index(
        "ix_sync_schedules_last_job", "connector_sync_schedules", ["organization_id", "last_job_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_sync_schedules_last_job", table_name="connector_sync_schedules")
    op.drop_index("ix_sync_schedules_status_next", table_name="connector_sync_schedules")
    op.drop_index("ix_sync_schedules_org_scope", table_name="connector_sync_schedules")
    op.drop_index("ix_sync_schedules_due", table_name="connector_sync_schedules")
    op.drop_table("connector_sync_schedules")