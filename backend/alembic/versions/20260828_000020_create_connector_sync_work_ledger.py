"""Create feature-gated connector generation and file-work ledger.

Revision ID: 20260828_000020
Revises: 20260828_000019
Create Date: 2026-08-28
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260828_000020"
down_revision = "20260828_000019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connector_sync_generations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sync_job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_key", sa.String(64), nullable=False),
        sa.Column("repository_identity", sa.String(255), nullable=False),
        sa.Column("branch_name", sa.String(255), nullable=False),
        sa.Column("commit_object_id", sa.String(255), nullable=False),
        sa.Column("root_tree_object_id", sa.String(255), nullable=False),
        sa.Column("profile_fingerprint", sa.String(255), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default=sa.text("'discovering'")),
        sa.Column("discovery_complete", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("discovery_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reconciliation_eligible", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("reconciliation_eligible_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resync_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("resync_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("items_discovered", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("items_registered", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("declared_bytes", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("terminal_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_connector_sync_generations"),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_sync_generations_organization", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id"],
            ["connectors.organization_id", "connectors.id"],
            name="fk_sync_generations_connector_tenant", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id", "connector_scope_id"],
            ["connector_scopes.organization_id", "connector_scopes.connector_id", "connector_scopes.id"],
            name="fk_sync_generations_scope_tenant", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id", "connector_scope_id", "sync_job_id"],
            ["connector_sync_jobs.organization_id", "connector_sync_jobs.connector_id", "connector_sync_jobs.connector_scope_id", "connector_sync_jobs.id"],
            name="fk_sync_generations_job_tenant", ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("organization_id", "id", name="uq_sync_generations_org_id"),
        sa.UniqueConstraint("organization_id", "sync_job_id", name="uq_sync_generations_org_job"),
        sa.UniqueConstraint(
            "organization_id", "connector_id", "connector_scope_id", "id", "profile_fingerprint",
            name="uq_sync_generations_scope_profile_id",
        ),
        sa.CheckConstraint("provider_key ~ '^[a-z][a-z0-9_]*$'", name="provider_key_valid"),
        sa.CheckConstraint("btrim(repository_identity) <> ''", name="repository_identity_not_blank"),
        sa.CheckConstraint("btrim(branch_name) <> ''", name="branch_name_not_blank"),
        sa.CheckConstraint("btrim(commit_object_id) <> ''", name="commit_object_id_not_blank"),
        sa.CheckConstraint("btrim(root_tree_object_id) <> ''", name="root_tree_object_id_not_blank"),
        sa.CheckConstraint(
            "profile_fingerprint ~ '^[a-z0-9][a-z0-9._:/-]*$'",
            name="profile_fingerprint_valid",
        ),
        sa.CheckConstraint(
            "status IN ('discovering', 'processing', 'completed', 'completed_with_errors', 'failed', 'cancelled')",
            name="status_valid",
        ),
        sa.CheckConstraint(
            "(discovery_complete AND discovery_completed_at IS NOT NULL) OR "
            "(NOT discovery_complete AND discovery_completed_at IS NULL)",
            name="discovery_completion_consistent",
        ),
        sa.CheckConstraint(
            "(reconciliation_eligible AND reconciliation_eligible_at IS NOT NULL AND discovery_complete) OR "
            "(NOT reconciliation_eligible AND reconciliation_eligible_at IS NULL)",
            name="reconcile_eligibility_consistent",
        ),
        sa.CheckConstraint(
            "(resync_required AND resync_requested_at IS NOT NULL) OR "
            "(NOT resync_required AND resync_requested_at IS NULL)",
            name="resync_state_consistent",
        ),
        sa.CheckConstraint(
            "(status IN ('completed', 'completed_with_errors', 'failed', 'cancelled') "
            "AND terminal_at IS NOT NULL) OR "
            "(status IN ('discovering', 'processing') AND terminal_at IS NULL)",
            name="terminal_state_consistent",
        ),
        sa.CheckConstraint(
            "items_discovered >= 0 AND items_registered >= 0 "
            "AND items_registered <= items_discovered AND declared_bytes >= 0",
            name="counters_nonnegative",
        ),
        sa.CheckConstraint("updated_at >= created_at", name="updated_after_created"),
        sa.CheckConstraint("terminal_at IS NULL OR terminal_at >= created_at", name="terminal_after_created"),
    )
    op.create_index(
        "ix_sync_generations_org_scope_created", "connector_sync_generations",
        ["organization_id", "connector_scope_id", "created_at", "id"],
    )
    op.create_index(
        "ix_sync_generations_org_scope_terminal", "connector_sync_generations",
        ["organization_id", "connector_scope_id", "terminal_at", "id"],
        postgresql_where=sa.text("terminal_at IS NOT NULL"),
    )
    op.create_index(
        "ix_sync_generations_follow_up", "connector_sync_generations",
        ["organization_id", "resync_requested_at", "id"],
        postgresql_where=sa.text("resync_required"),
    )

    op.create_table(
        "connector_sync_file_work_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_scope_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_item_key", sa.String(1024), nullable=False),
        sa.Column("source_key_hash", sa.String(64), nullable=False),
        sa.Column("repository_path", sa.String(1024), nullable=False),
        sa.Column("provider_blob_id", sa.String(255), nullable=False),
        sa.Column("provider_revision_id", sa.String(255), nullable=False),
        sa.Column("profile_fingerprint", sa.String(255), nullable=False),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("file_extension", sa.String(64), nullable=True),
        sa.Column("mime_type", sa.String(255), nullable=True),
        sa.Column("status", sa.String(32), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default=sa.text("3")),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_owner", sa.String(255), nullable=True),
        sa.Column("lease_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("fencing_token", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("lease_acquired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_reason_code", sa.String(64), nullable=True),
        sa.Column("last_error_category", sa.String(32), nullable=True),
        sa.Column("last_error_code", sa.String(128), nullable=True),
        sa.Column("quarantine_reason_code", sa.String(128), nullable=True),
        sa.Column("downloaded_bytes", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("extracted_characters", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("chunk_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("embedding_batch_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("terminal_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_connector_sync_file_work_items"),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id", "connector_scope_id", "generation_id", "profile_fingerprint"],
            ["connector_sync_generations.organization_id", "connector_sync_generations.connector_id", "connector_sync_generations.connector_scope_id", "connector_sync_generations.id", "connector_sync_generations.profile_fingerprint"],
            name="fk_sync_file_work_generation_tenant", ondelete="CASCADE",
        ),
        sa.UniqueConstraint("organization_id", "generation_id", "id", name="uq_sync_file_work_generation_id"),
        sa.UniqueConstraint("lease_id", name="uq_sync_file_work_lease_id"),
        sa.UniqueConstraint(
            "organization_id", "generation_id", "source_key_hash", "provider_blob_id",
            "provider_revision_id", "profile_fingerprint",
            name="uq_sync_file_work_logical_identity",
        ),
        sa.CheckConstraint("btrim(source_item_key) <> ''", name="source_item_key_not_blank"),
        sa.CheckConstraint("source_key_hash ~ '^[0-9a-f]{64}$'", name="source_key_hash_valid"),
        sa.CheckConstraint("btrim(repository_path) <> ''", name="repository_path_not_blank"),
        sa.CheckConstraint("btrim(provider_blob_id) <> ''", name="provider_blob_id_not_blank"),
        sa.CheckConstraint("btrim(provider_revision_id) <> ''", name="provider_revision_not_blank"),
        sa.CheckConstraint(
            "profile_fingerprint ~ '^[a-z0-9][a-z0-9._:/-]*$'", name="profile_fingerprint_valid"
        ),
        sa.CheckConstraint(
            "file_size_bytes IS NULL OR file_size_bytes BETWEEN 0 AND 1073741824",
            name="file_size_bounded",
        ),
        sa.CheckConstraint("file_extension IS NULL OR btrim(file_extension) <> ''", name="extension_not_blank"),
        sa.CheckConstraint("mime_type IS NULL OR btrim(mime_type) <> ''", name="mime_type_not_blank"),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'retry_wait', 'succeeded', 'skipped', "
            "'quarantined', 'failed', 'cancelled')",
            name="status_valid",
        ),
        sa.CheckConstraint("attempt_count >= 0", name="attempt_count_nonnegative"),
        sa.CheckConstraint("max_attempts BETWEEN 1 AND 10", name="max_attempts_bounded"),
        sa.CheckConstraint("attempt_count <= max_attempts", name="attempt_count_within_max"),
        sa.CheckConstraint("fencing_token = attempt_count", name="fencing_matches_attempt_count"),
        sa.CheckConstraint(
            "(status IN ('pending', 'retry_wait') AND next_attempt_at IS NOT NULL) OR "
            "(status NOT IN ('pending', 'retry_wait') AND next_attempt_at IS NULL)",
            name="availability_matches_status",
        ),
        sa.CheckConstraint(
            "status <> 'retry_wait' OR (attempt_count > 0 AND attempt_count < max_attempts)",
            name="retry_attempt_available",
        ),
        sa.CheckConstraint(
            "(status = 'running' AND lease_owner IS NOT NULL AND lease_id IS NOT NULL "
            "AND lease_acquired_at IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND heartbeat_at IS NOT NULL AND attempt_count > 0) OR "
            "(status <> 'running' AND lease_owner IS NULL AND lease_id IS NULL "
            "AND lease_acquired_at IS NULL AND lease_expires_at IS NULL AND heartbeat_at IS NULL)",
            name="lease_matches_status",
        ),
        sa.CheckConstraint("lease_owner IS NULL OR btrim(lease_owner) <> ''", name="lease_owner_not_blank"),
        sa.CheckConstraint(
            "lease_expires_at IS NULL OR lease_expires_at > lease_acquired_at", name="lease_expiry_after_acquired"
        ),
        sa.CheckConstraint("heartbeat_at IS NULL OR heartbeat_at >= lease_acquired_at", name="heartbeat_after_acquired"),
        sa.CheckConstraint("heartbeat_at IS NULL OR heartbeat_at < lease_expires_at", name="heartbeat_before_expiry"),
        sa.CheckConstraint("cancel_reason_code IS NULL OR cancel_requested_at IS NOT NULL", name="cancel_reason_requires_time"),
        sa.CheckConstraint(
            "cancel_reason_code IS NULL OR cancel_reason_code ~ '^[a-z][a-z0-9_]*$'",
            name="cancel_reason_code_valid",
        ),
        sa.CheckConstraint("status <> 'cancelled' OR cancel_requested_at IS NOT NULL", name="cancelled_requires_request"),
        sa.CheckConstraint(
            "(last_error_category IS NULL AND last_error_code IS NULL) OR "
            "(last_error_category IS NOT NULL AND last_error_code IS NOT NULL)",
            name="error_pair_consistent",
        ),
        sa.CheckConstraint(
            "last_error_category IS NULL OR last_error_category IN "
            "('configuration', 'authentication', 'authorization', 'rate_limit', 'source_read', "
            "'extraction', 'persistence', 'embedding', 'permission', 'internal')",
            name="error_category_valid",
        ),
        sa.CheckConstraint(
            "last_error_code IS NULL OR last_error_code ~ '^[a-z][a-z0-9_]*$'", name="error_code_valid"
        ),
        sa.CheckConstraint(
            "(status = 'quarantined' AND quarantine_reason_code IS NOT NULL "
            "AND last_error_category IS NOT NULL) OR "
            "(status <> 'quarantined' AND quarantine_reason_code IS NULL)",
            name="quarantine_state_consistent",
        ),
        sa.CheckConstraint(
            "quarantine_reason_code IS NULL OR quarantine_reason_code ~ '^[a-z][a-z0-9_]*$'",
            name="quarantine_reason_valid",
        ),
        sa.CheckConstraint(
            "downloaded_bytes BETWEEN 0 AND 1073741824 "
            "AND extracted_characters BETWEEN 0 AND 100000000 "
            "AND chunk_count BETWEEN 0 AND 100000 "
            "AND embedding_batch_count BETWEEN 0 AND 100000",
            name="counters_bounded",
        ),
        sa.CheckConstraint(
            "(status IN ('succeeded', 'skipped', 'quarantined', 'failed', 'cancelled') "
            "AND terminal_at IS NOT NULL) OR "
            "(status IN ('pending', 'running', 'retry_wait') AND terminal_at IS NULL)",
            name="terminal_state_consistent",
        ),
        sa.CheckConstraint("updated_at >= created_at", name="updated_after_created"),
        sa.CheckConstraint("terminal_at IS NULL OR terminal_at >= created_at", name="terminal_after_created"),
    )
    op.create_index(
        "ix_sync_file_work_claimable", "connector_sync_file_work_items",
        ["organization_id", "generation_id", "next_attempt_at", "id"],
        postgresql_where=sa.text("status IN ('pending', 'retry_wait')"),
    )
    op.create_index(
        "ix_sync_file_work_expired", "connector_sync_file_work_items",
        ["organization_id", "generation_id", "lease_expires_at", "id"],
        postgresql_where=sa.text("status = 'running'"),
    )
    op.create_index(
        "ix_sync_file_work_generation_barrier", "connector_sync_file_work_items",
        ["organization_id", "generation_id", "status"],
    )
    op.create_index(
        "ix_sync_file_work_scope_terminal", "connector_sync_file_work_items",
        ["organization_id", "connector_scope_id", "terminal_at", "id"],
        postgresql_where=sa.text("terminal_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_table("connector_sync_file_work_items")
    op.drop_table("connector_sync_generations")
