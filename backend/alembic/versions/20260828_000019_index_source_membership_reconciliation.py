"""Index bounded source-membership reconciliation scans.

Revision ID: 20260828_000019
Revises: 20260827_000018
Create Date: 2026-08-28
"""

from alembic import op
import sqlalchemy as sa


revision = "20260828_000019"
down_revision = "20260827_000018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_source_scope_memberships_reconciliation",
        "source_item_scope_memberships",
        [
            "organization_id",
            "connector_id",
            "connector_scope_id",
            "last_seen_at",
            "id",
        ],
        postgresql_where=sa.text("status = 'active' AND removed_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_source_scope_memberships_reconciliation",
        table_name="source_item_scope_memberships",
    )
