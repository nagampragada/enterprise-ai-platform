"""Add durable organization fairness for dedicated ledger claims.

Revision ID: 20260902_000022
Revises: 20260831_000021
Create Date: 2026-09-02
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260902_000022"
down_revision = "20260831_000021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        sa.schema.CreateSequence(
            sa.Sequence(
                "connector_sync_org_fair_claim_seq", data_type=sa.BigInteger()
            )
        )
    )
    op.create_table(
        "connector_sync_organization_claim_schedules",
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("last_claim_sequence", sa.BigInteger(), nullable=False),
        sa.Column("claim_count", sa.BigInteger(), nullable=False),
        sa.Column("last_claimed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "organization_id", name="pk_connector_sync_org_claim_schedules"
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_sync_org_claim_schedules_organization",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "last_claim_sequence", name="uq_sync_org_claim_schedules_sequence"
        ),
        sa.CheckConstraint("claim_count > 0", name="claim_count_positive"),
        sa.CheckConstraint("last_claim_sequence > 0", name="sequence_positive"),
        sa.CheckConstraint(
            "last_claimed_at >= created_at", name="last_claim_after_created"
        ),
        sa.CheckConstraint("updated_at >= created_at", name="updated_after_created"),
    )
    op.create_index(
        "ix_sync_org_claim_schedules_fair_order",
        "connector_sync_organization_claim_schedules",
        ["last_claim_sequence", "organization_id"],
    )
    op.create_index(
        "ix_sync_file_work_fair_eligible",
        "connector_sync_file_work_items",
        [
            "organization_id",
            "profile_fingerprint",
            "next_attempt_at",
            "generation_id",
            "id",
        ],
        postgresql_where=sa.text(
            "status IN ('pending', 'retry_wait') "
            "AND cancel_requested_at IS NULL "
            "AND attempt_count < max_attempts"
        ),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_sync_file_work_fair_eligible",
        table_name="connector_sync_file_work_items",
    )
    op.drop_table("connector_sync_organization_claim_schedules")
    op.execute(
        sa.schema.DropSequence(
            sa.Sequence(
                "connector_sync_org_fair_claim_seq", data_type=sa.BigInteger()
            )
        )
    )
