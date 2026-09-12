"""Add bounded controlled synchronization reservations.

Revision ID: 20260911_000025
Revises: 20260905_000024
Create Date: 2026-09-11
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260911_000025"
down_revision = "20260905_000024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connector_sync_control_reservations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sync_job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("work_item_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_token_hash", sa.String(64), nullable=False),
        sa.Column("target_source_key_hash", sa.String(64), nullable=False),
        sa.Column("target_provider_blob_id", sa.String(255), nullable=False),
        sa.Column("target_provider_revision_id", sa.String(255), nullable=False),
        sa.Column("target_profile_fingerprint", sa.String(255), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("planner_lease_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("processor_lease_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("handed_off_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_connector_sync_control_reservations"),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_sync_control_reservations_organization", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "created_by_user_id"],
            ["users.organization_id", "users.id"],
            name="fk_sync_control_reservations_creator_tenant", ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id", "connector_scope_id", "sync_job_id"],
            ["connector_sync_jobs.organization_id", "connector_sync_jobs.connector_id", "connector_sync_jobs.connector_scope_id", "connector_sync_jobs.id"],
            name="fk_sync_control_reservations_job_tenant", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "generation_id", "work_item_id"],
            ["connector_sync_file_work_items.organization_id", "connector_sync_file_work_items.generation_id", "connector_sync_file_work_items.id"],
            name="fk_sync_control_reservations_work_item_tenant", ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "organization_id", "sync_job_id", name="uq_sync_control_reservations_job"
        ),
        sa.UniqueConstraint(
            "owner_token_hash", name="uq_sync_control_reservations_owner_token_hash"
        ),
        sa.UniqueConstraint(
            "organization_id", "generation_id", "work_item_id",
            name="uq_sync_control_reservations_work_item",
        ),
        sa.CheckConstraint("state IN ('job', 'work_item', 'released')", name="state_valid"),
        sa.CheckConstraint("owner_token_hash ~ '^[0-9a-f]{64}$'", name="owner_token_hash_valid"),
        sa.CheckConstraint("target_source_key_hash ~ '^[0-9a-f]{64}$'", name="target_source_key_hash_valid"),
        sa.CheckConstraint(
            "target_provider_blob_id ~ '^([0-9a-f]{40}|[0-9a-f]{64})$' AND "
            "target_provider_revision_id ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'",
            name="provider_identity_valid",
        ),
        sa.CheckConstraint(
            "target_profile_fingerprint ~ '^[a-z0-9][a-z0-9._:/-]*$'",
            name="target_profile_fingerprint_valid",
        ),
        sa.CheckConstraint(
            "expires_at >= created_at + interval '5 minutes' AND "
            "expires_at <= created_at + interval '2 hours'",
            name="expiry_bounded",
        ),
        sa.CheckConstraint(
            "(state = 'job' AND generation_id IS NULL AND work_item_id IS NULL "
            "AND handed_off_at IS NULL AND released_at IS NULL) OR "
            "(state = 'work_item' AND generation_id IS NOT NULL "
            "AND work_item_id IS NOT NULL AND handed_off_at IS NOT NULL "
            "AND released_at IS NULL) OR "
            "(state = 'released' AND released_at IS NOT NULL "
            "AND ((generation_id IS NULL AND work_item_id IS NULL AND handed_off_at IS NULL) "
            "OR (generation_id IS NOT NULL AND work_item_id IS NOT NULL "
            "AND handed_off_at IS NOT NULL)))",
            name="lifecycle_consistent",
        ),
        sa.CheckConstraint(
            "planner_lease_id IS NULL OR (state = 'job' AND processor_lease_id IS NULL)",
            name="planner_lease_state_valid",
        ),
        sa.CheckConstraint(
            "processor_lease_id IS NULL OR (state = 'work_item' AND planner_lease_id IS NULL)",
            name="processor_lease_state_valid",
        ),
        sa.CheckConstraint(
            "handed_off_at IS NULL OR handed_off_at >= created_at",
            name="handoff_after_created",
        ),
        sa.CheckConstraint(
            "released_at IS NULL OR released_at >= created_at",
            name="release_after_created",
        ),
    )
    op.create_index(
        "ix_sync_control_reservations_job_live",
        "connector_sync_control_reservations",
        ["organization_id", "sync_job_id", "expires_at"],
        unique=False,
        postgresql_where=sa.text("released_at IS NULL AND state = 'job'"),
    )
    op.create_index(
        "ix_sync_control_reservations_work_live",
        "connector_sync_control_reservations",
        ["organization_id", "generation_id", "work_item_id", "expires_at"],
        unique=False,
        postgresql_where=sa.text("released_at IS NULL AND state = 'work_item'"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_sync_control_reservations_work_live",
        table_name="connector_sync_control_reservations",
    )
    op.drop_index(
        "ix_sync_control_reservations_job_live",
        table_name="connector_sync_control_reservations",
    )
    op.drop_table("connector_sync_control_reservations")
