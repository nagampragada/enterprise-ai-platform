"""Create identity and authentication tables.

Revision ID: 20260802_000002
Revises: 20260802_000001
Create Date: 2026-08-02 00:00:02
"""

from __future__ import annotations

import uuid

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260802_000002"
down_revision = "20260802_000001"
branch_labels = None
depends_on = None


ROLE_ORGANIZATION_ADMIN_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
ROLE_EMPLOYEE_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")


def upgrade() -> None:
    op.create_table(
        "organization_settings",
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("default_locale", sa.String(length=32), nullable=False, server_default=sa.text("'en-US'")),
        sa.Column("timezone", sa.String(length=64), nullable=False, server_default=sa.text("'UTC'")),
        sa.Column("retention_days", sa.Integer(), nullable=False, server_default=sa.text("365")),
        sa.Column("ai_model_name", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("organization_id", name="pk_organization_settings"),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_organization_settings_organization_id_organizations",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("btrim(default_locale) <> ''", name="organization_settings_default_locale_not_blank"),
        sa.CheckConstraint("btrim(timezone) <> ''", name="organization_settings_timezone_not_blank"),
        sa.CheckConstraint("retention_days BETWEEN 1 AND 3650", name="organization_settings_retention_days_range"),
        sa.CheckConstraint(
            "ai_model_name IS NULL OR btrim(ai_model_name) <> ''",
            name="organization_settings_ai_model_name_not_blank",
        ),
    )

    op.create_table(
        "roles",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_system_role", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_roles"),
        sa.UniqueConstraint("name", name="uq_roles_name"),
        sa.CheckConstraint("btrim(name) <> ''", name="roles_name_not_blank"),
    )

    roles_table = sa.table(
        "roles",
        sa.column("id", postgresql.UUID(as_uuid=True)),
        sa.column("name", sa.String(length=128)),
        sa.column("description", sa.Text()),
        sa.column("is_system_role", sa.Boolean()),
    )
    op.bulk_insert(
        roles_table,
        [
            {
                "id": ROLE_ORGANIZATION_ADMIN_ID,
                "name": "organization_admin",
                "description": "Organization administrator role",
                "is_system_role": True,
            },
            {
                "id": ROLE_EMPLOYEE_ID,
                "name": "employee",
                "description": "Standard organization employee role",
                "is_system_role": True,
            },
        ],
    )

    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("normalized_email", sa.String(length=320), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("first_name", sa.String(length=100), nullable=True),
        sa.Column("last_name", sa.String(length=100), nullable=True),
        # Version 1 requires a non-null display_name.
        sa.Column("display_name", sa.String(length=200), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'active'")),
        sa.Column("email_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_users"),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_users_organization_id_organizations",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("organization_id", "normalized_email", name="uq_users_organization_id_normalized_email"),
        sa.UniqueConstraint("organization_id", "id", name="uq_users_organization_id_id"),
        sa.CheckConstraint("normalized_email = lower(btrim(email))", name="users_normalized_email_matches_email"),
        sa.CheckConstraint("status IN ('active', 'suspended', 'disabled')", name="users_status_valid"),
        sa.CheckConstraint("btrim(password_hash) <> ''", name="users_password_hash_not_blank"),
    )
    op.create_index(
        "ix_users_organization_id_status",
        "users",
        ["organization_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_users_organization_id_last_login_at",
        "users",
        ["organization_id", "last_login_at"],
        unique=False,
    )

    op.create_table(
        "user_roles",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("assigned_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        # Audit metadata only in Version 1; keeping this as a nullable UUID avoids cross-tenant FK coupling.
        sa.Column("assigned_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_user_roles"),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_user_roles_organization_id_organizations",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "user_id"],
            ["users.organization_id", "users.id"],
            name="fk_user_roles_organization_id_user_id_users",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["role_id"],
            ["roles.id"],
            name="fk_user_roles_role_id_roles",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("organization_id", "user_id", "role_id", name="uq_user_roles_organization_id_user_id_role_id"),
    )
    op.create_index("ix_user_roles_organization_id_user_id", "user_roles", ["organization_id", "user_id"], unique=False)
    op.create_index("ix_user_roles_organization_id_role_id", "user_roles", ["organization_id", "role_id"], unique=False)
    op.create_index("ix_user_roles_assigned_by_user_id", "user_roles", ["assigned_by_user_id"], unique=False)

    op.create_table(
        "authentication_sessions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("refresh_token_hash", postgresql.BYTEA(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ip_address", postgresql.INET(), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_authentication_sessions"),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_authentication_sessions_organization_id_organizations",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "user_id"],
            ["users.organization_id", "users.id"],
            name="fk_authentication_sessions_organization_id_user_id_users",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("refresh_token_hash", name="uq_authentication_sessions_refresh_token_hash"),
        sa.CheckConstraint("expires_at > created_at", name="authentication_sessions_expires_after_created_at"),
        sa.CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= created_at",
            name="authentication_sessions_revoked_at_after_created_at",
        ),
        sa.CheckConstraint(
            "last_used_at IS NULL OR last_used_at >= created_at",
            name="authentication_sessions_last_used_at_after_created_at",
        ),
    )
    op.create_index(
        "ix_authentication_sessions_org_user_active",
        "authentication_sessions",
        ["organization_id", "user_id", "expires_at"],
        unique=False,
        postgresql_where=sa.text("revoked_at IS NULL"),
    )
    op.create_index(
        "ix_authentication_sessions_expires_at",
        "authentication_sessions",
        ["expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_authentication_sessions_expires_at", table_name="authentication_sessions")
    op.drop_index("ix_authentication_sessions_org_user_active", table_name="authentication_sessions")
    op.drop_table("authentication_sessions")

    op.drop_index("ix_user_roles_assigned_by_user_id", table_name="user_roles")
    op.drop_index("ix_user_roles_organization_id_role_id", table_name="user_roles")
    op.drop_index("ix_user_roles_organization_id_user_id", table_name="user_roles")
    op.drop_table("user_roles")

    op.drop_index("ix_users_organization_id_last_login_at", table_name="users")
    op.drop_index("ix_users_organization_id_status", table_name="users")
    op.drop_table("users")

    op.drop_table("roles")

    op.drop_table("organization_settings")