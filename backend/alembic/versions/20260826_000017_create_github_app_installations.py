"""Create verified GitHub App installation bindings.

Revision ID: 20260826_000017
Revises: 20260825_000016
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision="20260826_000017"
down_revision="20260825_000016"
branch_labels=None
depends_on=None

def upgrade() -> None:
    op.alter_column("connector_credentials","secret_reference",existing_type=sa.String(1024),nullable=True)
    op.drop_constraint(op.f("ck_connector_credentials_secret_reference_not_blank"),"connector_credentials",type_="check")
    op.create_check_constraint("secret_reference_consistent","connector_credentials",
        "(auth_scheme = 'app_installation' AND provider_key = 'github' AND secret_reference IS NULL) OR (auth_scheme <> 'app_installation' AND secret_reference IS NOT NULL AND btrim(secret_reference) <> '')")
    op.create_unique_constraint("uq_connector_credentials_org_connector_id","connector_credentials",["organization_id","connector_id","id"])
    op.create_table("github_app_installations",
        sa.Column("id",postgresql.UUID(as_uuid=True),nullable=False),sa.Column("organization_id",postgresql.UUID(as_uuid=True),nullable=False),
        sa.Column("connector_id",postgresql.UUID(as_uuid=True),nullable=False),sa.Column("credential_id",postgresql.UUID(as_uuid=True),nullable=False),
        sa.Column("github_app_id",sa.BigInteger(),nullable=False),sa.Column("github_installation_id",sa.BigInteger(),nullable=False),
        sa.Column("account_id",sa.BigInteger(),nullable=False),sa.Column("account_login",sa.String(255),nullable=False),
        sa.Column("account_type",sa.String(32),nullable=False),sa.Column("repository_selection",sa.String(16),nullable=False),
        sa.Column("status",sa.String(32),nullable=False),sa.Column("provider_created_at",sa.DateTime(timezone=True),nullable=False),
        sa.Column("provider_updated_at",sa.DateTime(timezone=True),nullable=False),sa.Column("last_verified_at",sa.DateTime(timezone=True),nullable=False),
        sa.Column("disconnected_at",sa.DateTime(timezone=True),nullable=True),sa.Column("created_at",sa.DateTime(timezone=True),nullable=False),
        sa.Column("updated_at",sa.DateTime(timezone=True),nullable=False),
        sa.PrimaryKeyConstraint("id",name="pk_github_app_installations"),
        sa.ForeignKeyConstraint(["organization_id"],["organizations.id"],name="fk_github_installations_organization",ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id","connector_id"],["connectors.organization_id","connectors.id"],name="fk_github_installations_connector_tenant",ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["organization_id","connector_id","credential_id"],["connector_credentials.organization_id","connector_credentials.connector_id","connector_credentials.id"],name="fk_github_installations_credential_connector_tenant",ondelete="CASCADE"),
        sa.UniqueConstraint("organization_id","id",name="uq_github_installations_org_id"),
        sa.UniqueConstraint("organization_id","connector_id",name="uq_github_installations_connector"),
        sa.UniqueConstraint("github_app_id","github_installation_id",name="uq_github_installations_app_installation"),
        sa.CheckConstraint("github_app_id > 0 AND github_installation_id > 0 AND account_id > 0",name="github_installation_ids_positive"),
        sa.CheckConstraint("btrim(account_login) <> ''",name="github_installation_login_nonblank"),
        sa.CheckConstraint("account_type = 'Organization'",name="github_installation_account_type_valid"),
        sa.CheckConstraint("repository_selection IN ('all', 'selected')",name="github_installation_selection_valid"),
        sa.CheckConstraint("status IN ('connected', 'disconnected')",name="github_installation_status_valid"),
        sa.CheckConstraint("updated_at >= created_at",name="github_installation_updated_after_created"))
    op.create_index("ix_github_installations_org_status","github_app_installations",["organization_id","status"])

def downgrade() -> None:
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM connector_credentials WHERE auth_scheme = 'app_installation' AND secret_reference IS NULL) THEN
            RAISE EXCEPTION 'cannot downgrade while app installation credentials exist';
        END IF;
    END $$""")
    op.drop_index("ix_github_installations_org_status",table_name="github_app_installations")
    op.drop_table("github_app_installations")
    op.drop_constraint("uq_connector_credentials_org_connector_id","connector_credentials",type_="unique")
    op.drop_constraint(op.f("ck_connector_credentials_secret_reference_consistent"),"connector_credentials",type_="check")
    op.create_check_constraint("secret_reference_not_blank","connector_credentials","btrim(secret_reference) <> ''")
    op.alter_column("connector_credentials","secret_reference",existing_type=sa.String(1024),nullable=False)
