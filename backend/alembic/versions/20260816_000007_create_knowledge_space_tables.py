"""Create tenant-safe knowledge space tables.

Revision ID: 20260816_000007
Revises: 20260815_000006
Create Date: 2026-08-16 00:00:07
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260816_000007"
down_revision = "20260815_000006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "knowledge_spaces",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'active'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_knowledge_spaces"),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], name="fk_knowledge_spaces_organization_id_organizations", ondelete="CASCADE"),
        sa.UniqueConstraint("organization_id", "id", name="uq_knowledge_spaces_organization_id_id"),
        sa.UniqueConstraint("organization_id", "slug", name="uq_knowledge_spaces_organization_id_slug"),
        sa.CheckConstraint("btrim(name) <> ''", name="knowledge_spaces_name_not_blank"),
        sa.CheckConstraint("slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'", name="knowledge_spaces_slug_kebab_case"),
        sa.CheckConstraint("status IN ('active', 'inactive', 'archived')", name="knowledge_spaces_status_valid"),
        sa.CheckConstraint(
            "(status = 'archived' AND archived_at IS NOT NULL) OR (status <> 'archived' AND archived_at IS NULL)",
            name="knowledge_spaces_archived_at_consistent",
        ),
    )
    op.create_index("ix_knowledge_spaces_organization_id_status", "knowledge_spaces", ["organization_id", "status"])

    _create_grant_table("knowledge_space_organization_grants", None, None, None)
    _create_grant_table("knowledge_space_department_grants", "department_id", "departments", "department")
    _create_grant_table("knowledge_space_team_grants", "team_id", "teams", "team")
    _create_grant_table("knowledge_space_user_grants", "user_id", "users", "user")


def _create_grant_table(
    table_name: str, target_column: str | None, target_table: str | None, target_name: str | None
) -> None:
    columns = [
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("knowledge_space_id", postgresql.UUID(as_uuid=True), nullable=False),
    ]
    if target_column is not None:
        columns.append(sa.Column(target_column, postgresql.UUID(as_uuid=True), nullable=False))
    columns.extend(
        [
            sa.Column("permission_level", sa.String(length=32), nullable=False),
            sa.Column("granted_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column("granted_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("reason", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.PrimaryKeyConstraint("id", name=f"pk_{table_name}"),
            sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], name=f"fk_{table_name.replace('knowledge_space_', 'ks_')}_organization", ondelete="CASCADE"),
            sa.ForeignKeyConstraint(
                ["organization_id", "knowledge_space_id"],
                ["knowledge_spaces.organization_id", "knowledge_spaces.id"],
                name=f"fk_{table_name.replace('knowledge_space_', 'ks_')}_space_tenant",
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["organization_id", "granted_by_user_id"],
                ["users.organization_id", "users.id"],
                name=f"fk_{table_name.replace('knowledge_space_', 'ks_')}_creator",
                ondelete="SET NULL (granted_by_user_id)",
            ),
            sa.CheckConstraint(
                "permission_level IN ('viewer', 'contributor', 'manager')",
                name="p_valid",
            ),
            sa.CheckConstraint(
                "expires_at IS NULL OR expires_at > granted_at",
                name="expiry_after_granted",
            ),
            sa.CheckConstraint(
                "revoked_at IS NULL OR revoked_at >= granted_at",
                name="revoked_after_granted",
            ),
        ]
    )
    if target_column is None:
        columns.append(sa.UniqueConstraint("organization_id", "knowledge_space_id", name="uq_ks_organization_grants_space"))
    else:
        columns.append(
            sa.ForeignKeyConstraint(
                ["organization_id", target_column],
                [f"{target_table}.organization_id", f"{target_table}.id"],
                name=f"fk_ks_{target_name}_grants_{target_name}_tenant",
                ondelete="CASCADE",
            )
        )
        columns.append(
            sa.UniqueConstraint(
                "organization_id", "knowledge_space_id", target_column, name=f"uq_ks_{target_name}_grants_space_{target_name}"
            )
        )
    op.create_table(table_name, *columns)
    if target_column is not None:
        op.create_index(
            f"ix_ks_{target_name}_grants_org_{target_name}",
            table_name,
            ["organization_id", target_column, "knowledge_space_id"],
        )


def downgrade() -> None:
    op.drop_index("ix_ks_user_grants_org_user", table_name="knowledge_space_user_grants")
    op.drop_table("knowledge_space_user_grants")
    op.drop_index("ix_ks_team_grants_org_team", table_name="knowledge_space_team_grants")
    op.drop_table("knowledge_space_team_grants")
    op.drop_index("ix_ks_department_grants_org_department", table_name="knowledge_space_department_grants")
    op.drop_table("knowledge_space_department_grants")
    op.drop_table("knowledge_space_organization_grants")
    op.drop_index("ix_knowledge_spaces_organization_id_status", table_name="knowledge_spaces")
    op.drop_table("knowledge_spaces")