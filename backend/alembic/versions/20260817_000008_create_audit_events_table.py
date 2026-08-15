"""Create tenant-safe audit events table.

Revision ID: 20260817_000008
Revises: 20260816_000007
Create Date: 2026-08-17 00:00:08
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260817_000008"
down_revision = "20260816_000007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "audit_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("actor_type", sa.String(length=32), nullable=False),
        sa.Column("actor_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("actor_reference", sa.String(length=255), nullable=True),
        sa.Column("action", sa.String(length=128), nullable=False),
        sa.Column("resource_type", sa.String(length=128), nullable=False),
        sa.Column("resource_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("request_id", sa.String(length=255), nullable=True),
        sa.Column("correlation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("change_summary", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("context", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.PrimaryKeyConstraint("id", name="pk_audit_events"),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_audit_events_organization_id_organizations",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "actor_user_id"],
            ["users.organization_id", "users.id"],
            name="fk_audit_events_actor_user_tenant",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("actor_type IN ('user', 'system', 'service')", name="actor_type_valid"),
        sa.CheckConstraint(
            "(actor_type = 'user' AND actor_user_id IS NOT NULL) OR "
            "(actor_type IN ('system', 'service') AND actor_user_id IS NULL "
            "AND actor_reference IS NOT NULL AND btrim(actor_reference) <> '')",
            name="actor_consistent",
        ),
        sa.CheckConstraint("btrim(action) <> ''", name="action_not_blank"),
        sa.CheckConstraint("btrim(resource_type) <> ''", name="resource_type_not_blank"),
        sa.CheckConstraint("outcome IN ('success', 'failure', 'denied')", name="outcome_valid"),
        sa.CheckConstraint("reason IS NULL OR btrim(reason) <> ''", name="reason_not_blank"),
        sa.CheckConstraint("request_id IS NULL OR btrim(request_id) <> ''", name="request_id_not_blank"),
        sa.CheckConstraint("actor_reference IS NULL OR btrim(actor_reference) <> ''", name="actor_reference_not_blank"),
        sa.CheckConstraint("schema_version > 0", name="schema_version_positive"),
        sa.CheckConstraint("jsonb_typeof(change_summary) = 'object'", name="change_summary_object"),
        sa.CheckConstraint("jsonb_typeof(context) = 'object'", name="context_object"),
    )
    op.create_index("ix_audit_events_organization_id_occurred_at", "audit_events", ["organization_id", "occurred_at"])
    op.create_index("ix_audit_events_org_actor_occurred", "audit_events", ["organization_id", "actor_user_id", "occurred_at"])
    op.create_index("ix_audit_events_org_resource_occurred", "audit_events", ["organization_id", "resource_type", "resource_id", "occurred_at"])
    op.create_index("ix_audit_events_org_action_occurred", "audit_events", ["organization_id", "action", "occurred_at"])
    op.create_index(
        "ix_audit_events_org_correlation",
        "audit_events",
        ["organization_id", "correlation_id"],
        postgresql_where=sa.text("correlation_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_audit_events_org_correlation", table_name="audit_events")
    op.drop_index("ix_audit_events_org_action_occurred", table_name="audit_events")
    op.drop_index("ix_audit_events_org_resource_occurred", table_name="audit_events")
    op.drop_index("ix_audit_events_org_actor_occurred", table_name="audit_events")
    op.drop_index("ix_audit_events_organization_id_occurred_at", table_name="audit_events")
    op.drop_table("audit_events")