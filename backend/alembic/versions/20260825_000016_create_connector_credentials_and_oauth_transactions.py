"""Create connector credentials and OAuth authorization transactions.

Revision ID: 20260825_000016
Revises: 20260824_000015
Create Date: 2026-08-25 00:00:16
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260825_000016"
down_revision = "20260824_000015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM connectors WHERE secret_reference IS NOT NULL) THEN
            RAISE EXCEPTION 'legacy connector secret references require explicit migration';
        END IF;
        END $$"""
    )
    op.drop_index("ix_connectors_org_credential_status", table_name="connectors")
    op.drop_constraint(
        op.f("ck_connectors_credential_expiry_after_created"), "connectors", type_="check"
    )
    op.drop_constraint(
        op.f("ck_connectors_secret_reference_not_blank"), "connectors", type_="check"
    )
    op.drop_constraint(
        op.f("ck_connectors_credential_status_valid"), "connectors", type_="check"
    )
    op.drop_column("connectors", "credential_expires_at")
    op.drop_column("connectors", "credential_status")
    op.drop_column("connectors", "secret_reference")

    op.create_table(
        "connector_credentials",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("provider_key", sa.String(64), nullable=False),
        sa.Column("auth_scheme", sa.String(32), nullable=False),
        sa.Column("secret_reference", sa.String(1024), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default=sa.text("'active'")),
        sa.Column("external_subject", sa.String(255), nullable=True),
        sa.Column("display_label", sa.String(255), nullable=True),
        sa.Column("granted_scopes", postgresql.JSONB(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("validated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_connector_credentials"),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_connector_credentials_organization", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id"],
            ["connectors.organization_id", "connectors.id"],
            name="fk_connector_credentials_connector_tenant", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "created_by_user_id"],
            ["users.organization_id", "users.id"],
            name="fk_connector_credentials_creator_tenant",
            ondelete="SET NULL (created_by_user_id)",
        ),
        sa.UniqueConstraint("organization_id", "id", name="uq_connector_credentials_org_id"),
        sa.UniqueConstraint(
            "organization_id", "connector_id", name="uq_connector_credentials_connector"
        ),
        sa.CheckConstraint("provider_key ~ '^[a-z][a-z0-9_]*$'", name="provider_key_valid"),
        sa.CheckConstraint(
            "auth_scheme IN ('oauth2', 'api_token', 'service_account', 'app_installation')",
            name="auth_scheme_valid",
        ),
        sa.CheckConstraint("btrim(secret_reference) <> ''", name="secret_reference_not_blank"),
        sa.CheckConstraint(
            "status IN ('active', 'expired', 'revoked', 'invalid')", name="status_valid"
        ),
        sa.CheckConstraint(
            "external_subject IS NULL OR btrim(external_subject) <> ''",
            name="external_subject_not_blank",
        ),
        sa.CheckConstraint(
            "display_label IS NULL OR btrim(display_label) <> ''", name="display_label_not_blank"
        ),
        sa.CheckConstraint("jsonb_typeof(granted_scopes) = 'array'", name="scopes_array"),
        sa.CheckConstraint("jsonb_array_length(granted_scopes) <= 100", name="scopes_bounded"),
        sa.CheckConstraint(
            "NOT jsonb_path_exists(granted_scopes, "
            "'$[*] ? (@.type() != \"string\" || @ like_regex \"^\\s*$\")')",
            name="scopes_nonblank_strings",
        ),
        sa.CheckConstraint(
            "octet_length(granted_scopes::text) <= 32768", name="scopes_storage_bounded"
        ),
        sa.CheckConstraint("expires_at IS NULL OR expires_at > created_at", name="expiry_after_created"),
        sa.CheckConstraint(
            "validated_at IS NULL OR validated_at >= created_at", name="validation_after_created"
        ),
        sa.CheckConstraint(
            "(status = 'revoked' AND revoked_at IS NOT NULL) OR "
            "(status <> 'revoked' AND revoked_at IS NULL)",
            name="revocation_consistent",
        ),
        sa.CheckConstraint(
            "status <> 'expired' OR expires_at IS NOT NULL", name="expired_requires_expiry"
        ),
        sa.CheckConstraint("revoked_at IS NULL OR revoked_at >= created_at", name="revoked_after_created"),
        sa.CheckConstraint("schema_version > 0", name="schema_version_positive"),
        sa.CheckConstraint("updated_at >= created_at", name="updated_after_created"),
    )
    op.create_index(
        "ix_connector_credentials_org_status", "connector_credentials", ["organization_id", "status"]
    )

    op.create_table(
        "oauth_authorization_transactions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connector_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("initiating_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("provider_key", sa.String(64), nullable=False),
        sa.Column("state_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("pkce_verifier_secret_reference", sa.String(1024), nullable=True),
        sa.Column("callback_identifier", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_code", sa.String(128), nullable=True),
        sa.Column("schema_version", sa.SmallInteger(), nullable=False, server_default=sa.text("1")),
        sa.PrimaryKeyConstraint("id", name="pk_oauth_authorization_transactions"),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_oauth_transactions_organization", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "connector_id"],
            ["connectors.organization_id", "connectors.id"],
            name="fk_oauth_transactions_connector_tenant", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "initiating_user_id"],
            ["users.organization_id", "users.id"],
            name="fk_oauth_transactions_user_tenant",
            ondelete="SET NULL (initiating_user_id)",
        ),
        sa.UniqueConstraint("organization_id", "id", name="uq_oauth_transactions_org_id"),
        sa.UniqueConstraint("state_hash", name="uq_oauth_transactions_state_hash"),
        sa.CheckConstraint("provider_key ~ '^[a-z][a-z0-9_]*$'", name="provider_key_valid"),
        sa.CheckConstraint("octet_length(state_hash) = 32", name="state_hash_sha256"),
        sa.CheckConstraint(
            "pkce_verifier_secret_reference IS NULL OR "
            "btrim(pkce_verifier_secret_reference) <> ''",
            name="pkce_reference_not_blank",
        ),
        sa.CheckConstraint(
            "callback_identifier ~ '^[a-z][a-z0-9_]*$'", name="callback_identifier_valid"
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'consumed', 'expired', 'failed')", name="status_valid"
        ),
        sa.CheckConstraint("expires_at > created_at", name="expiry_after_created"),
        sa.CheckConstraint(
            "expires_at <= created_at + interval '20 minutes'", name="lifetime_bounded"
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND consumed_at IS NULL AND failure_code IS NULL) OR "
            "(status = 'consumed' AND consumed_at IS NOT NULL AND failure_code IS NULL) OR "
            "(status = 'expired' AND consumed_at IS NULL AND failure_code IS NULL) OR "
            "(status = 'failed' AND consumed_at IS NULL AND failure_code IS NOT NULL)",
            name="lifecycle_consistent",
        ),
        sa.CheckConstraint(
            "consumed_at IS NULL OR consumed_at >= created_at", name="consumed_after_created"
        ),
        sa.CheckConstraint(
            "failure_code IS NULL OR failure_code ~ '^[a-z][a-z0-9_]*$'",
            name="failure_code_valid",
        ),
        sa.CheckConstraint("schema_version > 0", name="schema_version_positive"),
    )
    op.create_index(
        "ix_oauth_transactions_pending_state", "oauth_authorization_transactions", ["state_hash"],
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_oauth_transactions_pending_expiry", "oauth_authorization_transactions",
        ["expires_at", "id"], postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_oauth_transactions_org_connector_created", "oauth_authorization_transactions",
        ["organization_id", "connector_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_oauth_transactions_org_connector_created",
        table_name="oauth_authorization_transactions",
    )
    op.drop_index("ix_oauth_transactions_pending_expiry", table_name="oauth_authorization_transactions")
    op.drop_index("ix_oauth_transactions_pending_state", table_name="oauth_authorization_transactions")
    op.drop_table("oauth_authorization_transactions")
    op.drop_index("ix_connector_credentials_org_status", table_name="connector_credentials")
    op.drop_table("connector_credentials")

    op.add_column("connectors", sa.Column("secret_reference", sa.String(1024), nullable=True))
    op.add_column(
        "connectors",
        sa.Column(
            "credential_status", sa.String(32), nullable=False,
            server_default=sa.text("'not_configured'"),
        ),
    )
    op.add_column(
        "connectors", sa.Column("credential_expires_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.create_check_constraint(
        "credential_status_valid", "connectors",
        "credential_status IN ('not_configured', 'validating', 'valid', 'expiring', "
        "'expired', 'revoked', 'invalid')",
    )
    op.create_check_constraint(
        "secret_reference_not_blank", "connectors",
        "secret_reference IS NULL OR btrim(secret_reference) <> ''",
    )
    op.create_check_constraint(
        "credential_expiry_after_created", "connectors",
        "credential_expires_at IS NULL OR credential_expires_at > created_at",
    )
    op.create_index(
        "ix_connectors_org_credential_status", "connectors",
        ["organization_id", "credential_status"],
    )