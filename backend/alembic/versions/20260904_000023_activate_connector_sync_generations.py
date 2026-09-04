"""Activate complete connector synchronization generations.

Revision ID: 20260904_000023
Revises: 20260902_000022
Create Date: 2026-09-04
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260904_000023"
down_revision = "20260902_000022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connector_sync_generation_activations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("repository_identity", sa.String(255), nullable=False),
        sa.Column("commit_object_id", sa.String(255), nullable=False),
        sa.Column("profile_fingerprint", sa.String(255), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_connector_sync_generation_activations"),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id", "connector_scope_id"],
            [
                "connector_scopes.organization_id",
                "connector_scopes.connector_id",
                "connector_scopes.id",
            ],
            name="fk_sync_generation_activations_scope_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            [
                "organization_id",
                "connector_id",
                "connector_scope_id",
                "generation_id",
                "profile_fingerprint",
            ],
            [
                "connector_sync_generations.organization_id",
                "connector_sync_generations.connector_id",
                "connector_sync_generations.connector_scope_id",
                "connector_sync_generations.id",
                "connector_sync_generations.profile_fingerprint",
            ],
            name="fk_sync_generation_activations_generation_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "generation_id",
            name="uq_sync_generation_activations_generation",
        ),
        sa.CheckConstraint("status IN ('active', 'retired')", name="status_valid"),
        sa.CheckConstraint(
            "btrim(repository_identity) <> ''", name="repository_identity_not_blank"
        ),
        sa.CheckConstraint(
            "btrim(commit_object_id) <> ''", name="commit_object_id_not_blank"
        ),
        sa.CheckConstraint(
            "profile_fingerprint ~ '^[a-z0-9][a-z0-9._:/-]*$'",
            name="profile_fingerprint_valid",
        ),
        sa.CheckConstraint(
            "(status = 'active' AND retired_at IS NULL) OR "
            "(status = 'retired' AND retired_at IS NOT NULL)",
            name="retirement_consistent",
        ),
        sa.CheckConstraint(
            "retired_at IS NULL OR retired_at >= activated_at",
            name="retired_after_activation",
        ),
        sa.CheckConstraint("updated_at >= created_at", name="updated_after_created"),
    )
    op.create_index(
        "uq_sync_generation_activations_active_scope",
        "connector_sync_generation_activations",
        ["organization_id", "connector_id", "connector_scope_id"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )
    op.create_index(
        "ix_sync_generation_activations_scope_history",
        "connector_sync_generation_activations",
        ["organization_id", "connector_scope_id", "activated_at", "id"],
    )


def downgrade() -> None:
    op.drop_table("connector_sync_generation_activations")
