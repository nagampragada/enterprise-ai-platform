"""Enable the PostgreSQL vector extension.

Revision ID: 20260814_000004
Revises: 20260814_000003
Create Date: 2026-08-14 00:00:04
"""

from __future__ import annotations

from alembic import op


revision = "20260814_000004"
down_revision = "20260814_000003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")


def downgrade() -> None:
    # The extension is shared database infrastructure. Leave it installed so a
    # downgrade cannot invalidate future vector-dependent objects.
    pass