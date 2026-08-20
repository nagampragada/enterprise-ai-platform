"""Correlate an untrusted GitHub installation candidate with OAuth state.

Revision ID: 20260827_000018
Revises: 20260826_000017
"""

from alembic import op
import sqlalchemy as sa


revision = "20260827_000018"
down_revision = "20260826_000017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "oauth_authorization_transactions",
        sa.Column("provider_candidate_installation_id", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "oauth_authorization_transactions",
        sa.Column("provider_setup_completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "setup_correlation_valid",
        "oauth_authorization_transactions",
        "(provider_candidate_installation_id IS NULL AND provider_setup_completed_at IS NULL) OR "
        "(provider_key = 'github' AND provider_candidate_installation_id > 0 AND "
        "provider_setup_completed_at IS NOT NULL)",
    )
    op.create_check_constraint(
        "setup_time_valid",
        "oauth_authorization_transactions",
        "provider_setup_completed_at IS NULL OR "
        "(provider_setup_completed_at >= created_at AND provider_setup_completed_at < expires_at)",
    )


def downgrade() -> None:
    op.drop_constraint(
        op.f("ck_oauth_authorization_transactions_setup_time_valid"),
        "oauth_authorization_transactions",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_oauth_authorization_transactions_setup_correlation_valid"),
        "oauth_authorization_transactions",
        type_="check",
    )
    op.drop_column("oauth_authorization_transactions", "provider_setup_completed_at")
    op.drop_column("oauth_authorization_transactions", "provider_candidate_installation_id")
