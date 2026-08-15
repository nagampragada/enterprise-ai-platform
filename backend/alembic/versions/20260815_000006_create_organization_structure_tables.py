"""Create optional tenant-safe organization structure tables.

Revision ID: 20260815_000006
Revises: 20260814_000005
Create Date: 2026-08-15 00:00:06
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260815_000006"
down_revision = "20260814_000005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "departments",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("parent_department_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'active'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_departments"),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], name="fk_departments_organization_id_organizations", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["organization_id", "parent_department_id"],
            ["departments.organization_id", "departments.id"],
            name="fk_departments_organization_id_parent_department_id_departments",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("organization_id", "id", name="uq_departments_organization_id_id"),
        sa.UniqueConstraint("organization_id", "slug", name="uq_departments_organization_id_slug"),
        sa.CheckConstraint("btrim(name) <> ''", name="departments_name_not_blank"),
        sa.CheckConstraint("slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'", name="departments_slug_kebab_case"),
        sa.CheckConstraint("status IN ('active', 'inactive', 'archived')", name="departments_status_valid"),
        sa.CheckConstraint("parent_department_id IS NULL OR parent_department_id <> id", name="departments_parent_not_self"),
        sa.CheckConstraint(
            "(status = 'archived' AND archived_at IS NOT NULL) OR (status <> 'archived' AND archived_at IS NULL)",
            name="departments_archived_at_consistent",
        ),
    )
    op.create_index("ix_departments_organization_id_status", "departments", ["organization_id", "status"])
    op.create_index("ix_departments_organization_id_parent_department_id", "departments", ["organization_id", "parent_department_id"])

    op.create_table(
        "teams",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'active'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_teams"),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], name="fk_teams_organization_id_organizations", ondelete="CASCADE"),
        sa.UniqueConstraint("organization_id", "id", name="uq_teams_organization_id_id"),
        sa.UniqueConstraint("organization_id", "slug", name="uq_teams_organization_id_slug"),
        sa.CheckConstraint("btrim(name) <> ''", name="teams_name_not_blank"),
        sa.CheckConstraint("slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'", name="teams_slug_kebab_case"),
        sa.CheckConstraint("status IN ('active', 'inactive', 'archived')", name="teams_status_valid"),
        sa.CheckConstraint(
            "(status = 'archived' AND archived_at IS NOT NULL) OR (status <> 'archived' AND archived_at IS NULL)",
            name="teams_archived_at_consistent",
        ),
    )
    op.create_index("ix_teams_organization_id_status", "teams", ["organization_id", "status"])

    _create_membership_table(
        table_name="department_memberships",
        entity_name="department",
        entity_table="departments",
        responsibilities="'member', 'manager'",
        indexes=("department_id", "department"),
    )
    _create_membership_table(
        table_name="team_memberships",
        entity_name="team",
        entity_table="teams",
        responsibilities="'member', 'lead', 'manager', 'owner'",
        indexes=("team_id", "team"),
    )


def _create_membership_table(
    *, table_name: str, entity_name: str, entity_table: str, responsibilities: str, indexes: tuple[str, str]
) -> None:
    op.create_table(
        table_name,
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(f"{entity_name}_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("responsibility", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'active'")),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name=f"pk_{table_name}"),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], name=f"fk_{table_name}_organization_id_organizations", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["organization_id", f"{entity_name}_id"], [f"{entity_table}.organization_id", f"{entity_table}.id"],
            name=f"fk_{table_name}_{entity_name}_tenant", ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "user_id"], ["users.organization_id", "users.id"],
            name=f"fk_{table_name}_organization_id_user_id_users", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["organization_id", "created_by_user_id"], ["users.organization_id", "users.id"],
            name=f"fk_{table_name}_creator_tenant", ondelete="SET NULL (created_by_user_id)",
        ),
        sa.UniqueConstraint("organization_id", f"{entity_name}_id", "user_id", name=f"uq_{table_name}_entity_user"),
        sa.CheckConstraint(f"responsibility IN ({responsibilities})", name=f"{table_name}_responsibility_valid"),
        sa.CheckConstraint(f"status IN ('active', 'inactive', 'revoked')", name=f"{table_name}_status_valid"),
        sa.CheckConstraint(f"expires_at IS NULL OR expires_at > effective_from", name=f"{table_name}_expiry_after_effective"),
        sa.CheckConstraint(f"revoked_at IS NULL OR revoked_at >= effective_from", name=f"{table_name}_revoked_after_effective"),
        sa.CheckConstraint(
            "(status = 'revoked' AND revoked_at IS NOT NULL) OR (status <> 'revoked' AND revoked_at IS NULL)",
            name=f"{table_name}_revocation_consistent",
        ),
    )
    op.create_index(f"ix_{table_name}_organization_id_user_id_status", table_name, ["organization_id", "user_id", "status"])
    op.create_index(f"ix_{table_name}_organization_id_{indexes[0]}_status", table_name, ["organization_id", indexes[0], "status"])


def downgrade() -> None:
    op.drop_index("ix_team_memberships_organization_id_team_id_status", table_name="team_memberships")
    op.drop_index("ix_team_memberships_organization_id_user_id_status", table_name="team_memberships")
    op.drop_table("team_memberships")
    op.drop_index("ix_department_memberships_organization_id_department_id_status", table_name="department_memberships")
    op.drop_index("ix_department_memberships_organization_id_user_id_status", table_name="department_memberships")
    op.drop_table("department_memberships")
    op.drop_index("ix_teams_organization_id_status", table_name="teams")
    op.drop_table("teams")
    op.drop_index("ix_departments_organization_id_parent_department_id", table_name="departments")
    op.drop_index("ix_departments_organization_id_status", table_name="departments")
    op.drop_table("departments")