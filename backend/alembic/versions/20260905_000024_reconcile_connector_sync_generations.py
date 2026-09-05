"""Add authoritative generation observations and reconciliation progress.

Revision ID: 20260905_000024
Revises: 20260904_000023
Create Date: 2026-09-05
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260905_000024"
down_revision = "20260904_000023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "connector_sync_generations",
        sa.Column(
            "manifest_schema_version",
            sa.SmallInteger(),
            nullable=False,
            server_default=sa.text("1"),
        ),
    )
    op.add_column(
        "connector_sync_generations",
        sa.Column("reconciliation_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "connector_sync_generations",
        sa.Column("reconciliation_completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    for column_name in (
        "reconciled_membership_count",
        "reconciled_source_count",
        "reconciled_document_count",
    ):
        op.add_column(
            "connector_sync_generations",
            sa.Column(
                column_name,
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text("0"),
            ),
        )
    op.create_check_constraint(
        "ck_connector_sync_generations_manifest_schema_version_valid",
        "connector_sync_generations",
        "manifest_schema_version IN (1, 2)",
    )
    op.create_check_constraint(
        "ck_connector_sync_generations_reconciliation_authority_valid",
        "connector_sync_generations",
        "NOT reconciliation_eligible OR "
        "(manifest_schema_version = 2 AND status = 'completed')",
    )
    op.create_check_constraint(
        "ck_connector_sync_generations_reconciliation_progress_requires_authority",
        "connector_sync_generations",
        "(NOT reconciliation_eligible AND reconciliation_started_at IS NULL "
        "AND reconciliation_completed_at IS NULL "
        "AND reconciled_membership_count = 0 AND reconciled_source_count = 0 "
        "AND reconciled_document_count = 0) OR reconciliation_eligible",
    )
    op.create_check_constraint(
        "ck_connector_sync_generations_reconciliation_start_order_valid",
        "connector_sync_generations",
        "reconciliation_started_at IS NULL OR "
        "(reconciliation_eligible_at IS NOT NULL "
        "AND reconciliation_started_at >= reconciliation_eligible_at)",
    )
    op.create_check_constraint(
        "ck_connector_sync_generations_reconciliation_completion_order_valid",
        "connector_sync_generations",
        "reconciliation_completed_at IS NULL OR "
        "(reconciliation_started_at IS NOT NULL "
        "AND reconciliation_completed_at >= reconciliation_started_at)",
    )
    op.create_check_constraint(
        "ck_connector_sync_generations_reconciliation_counters_valid",
        "connector_sync_generations",
        "reconciled_membership_count >= 0 AND reconciled_source_count >= 0 "
        "AND reconciled_document_count >= 0 "
        "AND reconciled_source_count <= reconciled_membership_count "
        "AND reconciled_document_count <= reconciled_source_count",
    )

    op.create_table(
        "connector_sync_generation_observations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_item_key", sa.String(1024), nullable=False),
        sa.Column("source_key_hash", sa.String(64), nullable=False),
        sa.Column("repository_path", sa.String(1024), nullable=False),
        sa.Column("provider_object_id", sa.String(255), nullable=False),
        sa.Column("provider_revision_id", sa.String(255), nullable=False),
        sa.Column("profile_fingerprint", sa.String(255), nullable=False),
        sa.Column("entry_type", sa.String(32), nullable=False),
        sa.Column("disposition", sa.String(32), nullable=False),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_connector_sync_generation_observations"),
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
            name="fk_sync_generation_observations_generation_tenant",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "generation_id",
            "id",
            name="uq_sync_generation_observations_generation_id",
        ),
        sa.UniqueConstraint(
            "organization_id",
            "generation_id",
            "source_key_hash",
            name="uq_sync_generation_observations_source",
        ),
        sa.CheckConstraint("btrim(source_item_key) <> ''", name="source_item_key_not_blank"),
        sa.CheckConstraint("source_key_hash ~ '^[0-9a-f]{64}$'", name="source_key_hash_valid"),
        sa.CheckConstraint("btrim(repository_path) <> ''", name="repository_path_not_blank"),
        sa.CheckConstraint("btrim(provider_object_id) <> ''", name="provider_object_id_not_blank"),
        sa.CheckConstraint("btrim(provider_revision_id) <> ''", name="provider_revision_not_blank"),
        sa.CheckConstraint(
            "profile_fingerprint ~ '^[a-z0-9][a-z0-9._:/-]*$'",
            name="profile_fingerprint_valid",
        ),
        sa.CheckConstraint(
            "entry_type IN ('regular_blob', 'symlink', 'submodule')",
            name="entry_type_valid",
        ),
        sa.CheckConstraint(
            "disposition IN ('eligible', 'unsupported_format', 'oversized', "
            "'unsupported_object_type')",
            name="disposition_valid",
        ),
        sa.CheckConstraint(
            "file_size_bytes IS NULL OR file_size_bytes BETWEEN 0 AND 1073741824",
            name="file_size_bounded",
        ),
        sa.CheckConstraint(
            "(entry_type = 'submodule' AND file_size_bytes IS NULL) OR "
            "(entry_type <> 'submodule' AND file_size_bytes IS NOT NULL)",
            name="size_matches_entry_type",
        ),
        sa.CheckConstraint(
            "(disposition = 'eligible' AND entry_type = 'regular_blob') OR "
            "disposition <> 'eligible'",
            name="eligible_regular_blob",
        ),
    )
    op.create_index(
        "ix_sync_generation_observations_scope_path",
        "connector_sync_generation_observations",
        ["organization_id", "connector_scope_id", "generation_id", "repository_path"],
    )


def downgrade() -> None:
    op.drop_table("connector_sync_generation_observations")
    for constraint_name in (
        "ck_connector_sync_generations_reconciliation_counters_valid",
        "ck_connector_sync_generations_reconciliation_completion_order_valid",
        "ck_connector_sync_generations_reconciliation_start_order_valid",
        "ck_connector_sync_generations_reconciliation_progress_requires_authority",
        "ck_connector_sync_generations_reconciliation_authority_valid",
        "ck_connector_sync_generations_manifest_schema_version_valid",
    ):
        op.drop_constraint(
            constraint_name, "connector_sync_generations", type_="check"
        )
    for column_name in (
        "reconciled_document_count",
        "reconciled_source_count",
        "reconciled_membership_count",
        "reconciliation_completed_at",
        "reconciliation_started_at",
        "manifest_schema_version",
    ):
        op.drop_column("connector_sync_generations", column_name)
