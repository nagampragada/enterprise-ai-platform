"""Create industries and organizations tables.

Revision ID: 20260802_000001
Revises:
Create Date: 2026-08-02 00:00:01
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260802_000001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "industries",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("btrim(code) <> ''", name="ck_industries_industries_code_not_blank"),
        sa.CheckConstraint("btrim(name) <> ''", name="ck_industries_industries_name_not_blank"),
        sa.PrimaryKeyConstraint("id", name="pk_industries"),
        sa.UniqueConstraint("code", name="uq_industries_code"),
        sa.UniqueConstraint("name", name="uq_industries_name"),
    )
    op.create_index("ix_industries_is_active", "industries", ["is_active"], unique=False)

    op.create_table(
        "organizations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("industry_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('active', 'inactive', 'suspended')",
            name="ck_organizations_organizations_status_valid",
        ),
        sa.ForeignKeyConstraint(["industry_id"], ["industries.id"], name="fk_organizations_industry_id_industries", ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id", name="pk_organizations"),
        sa.UniqueConstraint("slug", name="uq_organizations_slug"),
    )
    op.create_index("ix_organizations_industry_id", "organizations", ["industry_id"], unique=False)
    op.create_index("ix_organizations_status", "organizations", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_organizations_status", table_name="organizations")
    op.drop_index("ix_organizations_industry_id", table_name="organizations")
    op.drop_table("organizations")

    op.drop_index("ix_industries_is_active", table_name="industries")
    op.drop_table("industries")