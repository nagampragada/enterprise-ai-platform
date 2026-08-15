"""SQLAlchemy 2.x models for the platform database metadata."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    text,
    func,
)
from sqlalchemy.dialects.postgresql import BYTEA, INET, JSONB, UUID
from pgvector.sqlalchemy import Vector
from sqlalchemy.orm import Mapped, mapped_column, relationship

from infrastructure.db.base import Base


class Industry(Base):
    __tablename__ = "industries"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_industries"),
        CheckConstraint("btrim(code) <> ''", name="industries_code_not_blank"),
        CheckConstraint("btrim(name) <> ''", name="industries_name_not_blank"),
        UniqueConstraint("code", name="uq_industries_code"),
        UniqueConstraint("name", name="uq_industries_name"),
        Index("ix_industries_is_active", "is_active"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    organizations: Mapped[list["Organization"]] = relationship(back_populates="industry")


class Organization(Base):
    __tablename__ = "organizations"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_organizations"),
        ForeignKeyConstraint(["industry_id"], ["industries.id"], name="fk_organizations_industry_id_industries", ondelete="RESTRICT"),
        CheckConstraint("status IN ('active', 'inactive', 'suspended')", name="organizations_status_valid"),
        UniqueConstraint("slug", name="uq_organizations_slug"),
        Index("ix_organizations_industry_id", "industry_id"),
        Index("ix_organizations_status", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    industry_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'active'"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    industry: Mapped[Industry | None] = relationship(back_populates="organizations")
    settings: Mapped[OrganizationSettings | None] = relationship(back_populates="organization", uselist=False)
    users: Mapped[list["User"]] = relationship(back_populates="organization")
    documents: Mapped[list["Document"]] = relationship(back_populates="organization")
    departments: Mapped[list["Department"]] = relationship(back_populates="organization")
    teams: Mapped[list["Team"]] = relationship(back_populates="organization")


class Department(Base):
    __tablename__ = "departments"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_departments"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_departments_organization_id_organizations",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "parent_department_id"],
            ["departments.organization_id", "departments.id"],
            name="fk_departments_organization_id_parent_department_id_departments",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("organization_id", "id", name="uq_departments_organization_id_id"),
        UniqueConstraint("organization_id", "slug", name="uq_departments_organization_id_slug"),
        CheckConstraint("btrim(name) <> ''", name="departments_name_not_blank"),
        CheckConstraint(
            "slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'",
            name="departments_slug_kebab_case",
        ),
        CheckConstraint(
            "status IN ('active', 'inactive', 'archived')",
            name="departments_status_valid",
        ),
        CheckConstraint(
            "parent_department_id IS NULL OR parent_department_id <> id",
            name="departments_parent_not_self",
        ),
        CheckConstraint(
            "(status = 'archived' AND archived_at IS NOT NULL) OR "
            "(status <> 'archived' AND archived_at IS NULL)",
            name="departments_archived_at_consistent",
        ),
        Index("ix_departments_organization_id_status", "organization_id", "status"),
        Index("ix_departments_organization_id_parent_department_id", "organization_id", "parent_department_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    parent_department_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'active'"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    organization: Mapped[Organization] = relationship(back_populates="departments")
    memberships: Mapped[list["DepartmentMembership"]] = relationship(back_populates="department")


class Team(Base):
    __tablename__ = "teams"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_teams"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_teams_organization_id_organizations",
            ondelete="CASCADE",
        ),
        UniqueConstraint("organization_id", "id", name="uq_teams_organization_id_id"),
        UniqueConstraint("organization_id", "slug", name="uq_teams_organization_id_slug"),
        CheckConstraint("btrim(name) <> ''", name="teams_name_not_blank"),
        CheckConstraint("slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'", name="teams_slug_kebab_case"),
        CheckConstraint("status IN ('active', 'inactive', 'archived')", name="teams_status_valid"),
        CheckConstraint(
            "(status = 'archived' AND archived_at IS NOT NULL) OR "
            "(status <> 'archived' AND archived_at IS NULL)",
            name="teams_archived_at_consistent",
        ),
        Index("ix_teams_organization_id_status", "organization_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'active'"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    organization: Mapped[Organization] = relationship(back_populates="teams")
    memberships: Mapped[list["TeamMembership"]] = relationship(back_populates="team")


class KnowledgeSpace(Base):
    __tablename__ = "knowledge_spaces"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_knowledge_spaces"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_knowledge_spaces_organization_id_organizations",
            ondelete="CASCADE",
        ),
        UniqueConstraint("organization_id", "id", name="uq_knowledge_spaces_organization_id_id"),
        UniqueConstraint("organization_id", "slug", name="uq_knowledge_spaces_organization_id_slug"),
        CheckConstraint("btrim(name) <> ''", name="knowledge_spaces_name_not_blank"),
        CheckConstraint("slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'", name="knowledge_spaces_slug_kebab_case"),
        CheckConstraint("status IN ('active', 'inactive', 'archived')", name="knowledge_spaces_status_valid"),
        CheckConstraint(
            "(status = 'archived' AND archived_at IS NOT NULL) OR "
            "(status <> 'archived' AND archived_at IS NULL)",
            name="knowledge_spaces_archived_at_consistent",
        ),
        Index("ix_knowledge_spaces_organization_id_status", "organization_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'active'"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class KnowledgeSpaceOrganizationGrant(Base):
    __tablename__ = "knowledge_space_organization_grants"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_knowledge_space_organization_grants"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_ks_organization_grants_organization",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "knowledge_space_id"],
            ["knowledge_spaces.organization_id", "knowledge_spaces.id"],
            name="fk_ks_organization_grants_space_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "granted_by_user_id"],
            ["users.organization_id", "users.id"],
            name="fk_ks_organization_grants_creator",
            ondelete="SET NULL (granted_by_user_id)",
        ),
        UniqueConstraint("organization_id", "knowledge_space_id", name="uq_ks_organization_grants_space"),
        CheckConstraint(
            "permission_level IN ('viewer', 'contributor', 'manager')",
            name="p_valid",
        ),
        CheckConstraint(
            "expires_at IS NULL OR expires_at > granted_at",
            name="expiry_after_granted",
        ),
        CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= granted_at",
            name="revoked_after_granted",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    knowledge_space_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    permission_level: Mapped[str] = mapped_column(String(32), nullable=False)
    granted_by_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    granted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class KnowledgeSpaceDepartmentGrant(Base):
    __tablename__ = "knowledge_space_department_grants"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_knowledge_space_department_grants"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_ks_department_grants_organization",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "knowledge_space_id"],
            ["knowledge_spaces.organization_id", "knowledge_spaces.id"],
            name="fk_ks_department_grants_space_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "department_id"],
            ["departments.organization_id", "departments.id"],
            name="fk_ks_department_grants_department_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "granted_by_user_id"],
            ["users.organization_id", "users.id"],
            name="fk_ks_department_grants_creator",
            ondelete="SET NULL (granted_by_user_id)",
        ),
        UniqueConstraint(
            "organization_id", "knowledge_space_id", "department_id", name="uq_ks_department_grants_space_department"
        ),
        CheckConstraint("permission_level IN ('viewer', 'contributor', 'manager')", name="p_valid"),
        CheckConstraint("expires_at IS NULL OR expires_at > granted_at", name="expiry_after_granted"),
        CheckConstraint("revoked_at IS NULL OR revoked_at >= granted_at", name="revoked_after_granted"),
        Index("ix_ks_department_grants_org_department", "organization_id", "department_id", "knowledge_space_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    knowledge_space_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    department_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    permission_level: Mapped[str] = mapped_column(String(32), nullable=False)
    granted_by_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    granted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class KnowledgeSpaceTeamGrant(Base):
    __tablename__ = "knowledge_space_team_grants"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_knowledge_space_team_grants"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_ks_team_grants_organization",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "knowledge_space_id"],
            ["knowledge_spaces.organization_id", "knowledge_spaces.id"],
            name="fk_ks_team_grants_space_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "team_id"],
            ["teams.organization_id", "teams.id"],
            name="fk_ks_team_grants_team_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "granted_by_user_id"],
            ["users.organization_id", "users.id"],
            name="fk_ks_team_grants_creator",
            ondelete="SET NULL (granted_by_user_id)",
        ),
        UniqueConstraint("organization_id", "knowledge_space_id", "team_id", name="uq_ks_team_grants_space_team"),
        CheckConstraint("permission_level IN ('viewer', 'contributor', 'manager')", name="p_valid"),
        CheckConstraint("expires_at IS NULL OR expires_at > granted_at", name="expiry_after_granted"),
        CheckConstraint("revoked_at IS NULL OR revoked_at >= granted_at", name="revoked_after_granted"),
        Index("ix_ks_team_grants_org_team", "organization_id", "team_id", "knowledge_space_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    knowledge_space_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    team_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    permission_level: Mapped[str] = mapped_column(String(32), nullable=False)
    granted_by_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    granted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class KnowledgeSpaceUserGrant(Base):
    __tablename__ = "knowledge_space_user_grants"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_knowledge_space_user_grants"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_ks_user_grants_organization",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "knowledge_space_id"],
            ["knowledge_spaces.organization_id", "knowledge_spaces.id"],
            name="fk_ks_user_grants_space_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "user_id"],
            ["users.organization_id", "users.id"],
            name="fk_ks_user_grants_user_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "granted_by_user_id"],
            ["users.organization_id", "users.id"],
            name="fk_ks_user_grants_creator",
            ondelete="SET NULL (granted_by_user_id)",
        ),
        UniqueConstraint("organization_id", "knowledge_space_id", "user_id", name="uq_ks_user_grants_space_user"),
        CheckConstraint("permission_level IN ('viewer', 'contributor', 'manager')", name="p_valid"),
        CheckConstraint("expires_at IS NULL OR expires_at > granted_at", name="expiry_after_granted"),
        CheckConstraint("revoked_at IS NULL OR revoked_at >= granted_at", name="revoked_after_granted"),
        Index("ix_ks_user_grants_org_user", "organization_id", "user_id", "knowledge_space_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    knowledge_space_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    permission_level: Mapped[str] = mapped_column(String(32), nullable=False)
    granted_by_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    granted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class Connector(Base):
    __tablename__ = "connectors"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_connectors"),
        ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_connectors_organization_id_organizations", ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "created_by_user_id"], ["users.organization_id", "users.id"],
            name="fk_connectors_creator_tenant", ondelete="SET NULL (created_by_user_id)",
        ),
        UniqueConstraint("organization_id", "id", name="uq_connectors_organization_id_id"),
        UniqueConstraint("organization_id", "slug", name="uq_connectors_organization_id_slug"),
        CheckConstraint("connector_type ~ '^[a-z][a-z0-9_]*$'", name="type_code_valid"),
        CheckConstraint("btrim(display_name) <> ''", name="display_name_not_blank"),
        CheckConstraint("slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'", name="slug_kebab_case"),
        CheckConstraint(
            "status IN ('draft', 'validating', 'active', 'degraded', 'auth_failed', 'paused', 'archived')",
            name="status_valid",
        ),
        CheckConstraint("acl_support IN ('none', 'partial', 'complete')", name="acl_support_valid"),
        CheckConstraint(
            "credential_status IN ('not_configured', 'validating', 'valid', 'expiring', 'expired', 'revoked', 'invalid')",
            name="credential_status_valid",
        ),
        CheckConstraint("jsonb_typeof(capabilities) = 'object'", name="capabilities_object"),
        CheckConstraint("jsonb_typeof(safe_config) = 'object'", name="safe_config_object"),
        CheckConstraint("config_schema_version > 0", name="config_version_positive"),
        CheckConstraint("secret_reference IS NULL OR btrim(secret_reference) <> ''", name="secret_reference_not_blank"),
        CheckConstraint(
            "(status = 'archived' AND archived_at IS NOT NULL) OR "
            "(status <> 'archived' AND archived_at IS NULL)",
            name="archive_consistent",
        ),
        CheckConstraint(
            "credential_expires_at IS NULL OR credential_expires_at > created_at",
            name="credential_expiry_after_created",
        ),
        Index("ix_connectors_organization_id_status", "organization_id", "status"),
        Index("ix_connectors_org_type_status", "organization_id", "connector_type", "status"),
        Index("ix_connectors_org_credential_status", "organization_id", "credential_status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'draft'"))
    acl_support: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'none'"))
    capabilities: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    safe_config: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    config_schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("1"))
    secret_reference: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    credential_status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'not_configured'"))
    credential_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    last_validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ConnectorScope(Base):
    __tablename__ = "connector_scopes"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_connector_scopes"),
        ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_connector_scopes_organization", ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "connector_id"], ["connectors.organization_id", "connectors.id"],
            name="fk_connector_scopes_connector_tenant", ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "knowledge_space_id"],
            ["knowledge_spaces.organization_id", "knowledge_spaces.id"],
            name="fk_connector_scopes_space_tenant", ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["organization_id", "created_by_user_id"], ["users.organization_id", "users.id"],
            name="fk_connector_scopes_creator_tenant", ondelete="SET NULL (created_by_user_id)",
        ),
        UniqueConstraint("organization_id", "id", name="uq_connector_scopes_organization_id_id"),
        UniqueConstraint(
            "organization_id", "connector_id", "id", name="uq_connector_scopes_org_connector_id"
        ),
        UniqueConstraint("organization_id", "connector_id", "slug", name="uq_connector_scopes_connector_slug"),
        UniqueConstraint(
            "organization_id", "connector_id", "external_scope_key",
            name="uq_connector_scopes_connector_external_key",
        ),
        CheckConstraint("btrim(display_name) <> ''", name="display_name_not_blank"),
        CheckConstraint("slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'", name="slug_kebab_case"),
        CheckConstraint("scope_type ~ '^[a-z][a-z0-9_]*$'", name="type_code_valid"),
        CheckConstraint("btrim(external_scope_key) <> ''", name="external_key_not_blank"),
        CheckConstraint(
            "access_mode IN ('platform_managed', 'source_acl', 'hybrid')", name="access_mode_valid"
        ),
        CheckConstraint(
            "status IN ('draft', 'validating', 'active', 'invalid', 'paused', 'removed')", name="status_valid"
        ),
        CheckConstraint("jsonb_typeof(safe_config) = 'object'", name="safe_config_object"),
        CheckConstraint("config_schema_version > 0", name="config_version_positive"),
        CheckConstraint(
            "(status = 'removed' AND removed_at IS NOT NULL) OR "
            "(status <> 'removed' AND removed_at IS NULL)",
            name="removal_consistent",
        ),
        Index("ix_connector_scopes_org_connector_status", "organization_id", "connector_id", "status"),
        Index("ix_connector_scopes_org_space_status", "organization_id", "knowledge_space_id", "status"),
        Index("ix_connector_scopes_org_access_status", "organization_id", "access_mode", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    knowledge_space_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(255), nullable=False)
    scope_type: Mapped[str] = mapped_column(String(64), nullable=False)
    external_scope_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    access_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'draft'"))
    safe_config: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    config_schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("1"))
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    last_validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    removed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SourceItem(Base):
    __tablename__ = "source_items"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_source_items"),
        ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_source_items_organization", ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "connector_id"], ["connectors.organization_id", "connectors.id"],
            name="fk_source_items_connector_tenant", ondelete="CASCADE",
        ),
        UniqueConstraint(
            "organization_id", "connector_id", "id", name="uq_source_items_connector_id"
        ),
        UniqueConstraint(
            "organization_id", "connector_id", "source_item_key", name="uq_source_items_connector_key"
        ),
        CheckConstraint("btrim(source_item_key) <> ''", name="key_not_blank"),
        CheckConstraint(
            "parent_source_item_key IS NULL OR btrim(parent_source_item_key) <> ''",
            name="parent_key_not_blank",
        ),
        CheckConstraint(
            "parent_source_item_key IS NULL OR parent_source_item_key <> source_item_key",
            name="parent_key_not_self",
        ),
        CheckConstraint("source_item_type ~ '^[a-z][a-z0-9_]*$'", name="type_code_valid"),
        CheckConstraint("btrim(title) <> ''", name="title_not_blank"),
        CheckConstraint("source_url IS NULL OR btrim(source_url) <> ''", name="url_not_blank"),
        CheckConstraint("mime_type IS NULL OR btrim(mime_type) <> ''", name="mime_type_not_blank"),
        CheckConstraint(
            "source_checksum IS NULL OR btrim(source_checksum) <> ''", name="checksum_not_blank"
        ),
        CheckConstraint(
            "source_version IS NULL OR btrim(source_version) <> ''", name="version_not_blank"
        ),
        CheckConstraint("size_bytes IS NULL OR size_bytes >= 0", name="size_nonnegative"),
        CheckConstraint("last_seen_at >= first_seen_at", name="seen_order_valid"),
        CheckConstraint("status IN ('active', 'deleted', 'unavailable')", name="status_valid"),
        CheckConstraint(
            "(status = 'deleted' AND deleted_at IS NOT NULL) OR "
            "(status <> 'deleted' AND deleted_at IS NULL)",
            name="deletion_consistent",
        ),
        CheckConstraint("jsonb_typeof(metadata) = 'object'", name="metadata_object"),
        CheckConstraint("metadata_schema_version > 0", name="metadata_version_positive"),
        Index("ix_source_items_org_connector_status", "organization_id", "connector_id", "status"),
        Index("ix_source_items_org_connector_type", "organization_id", "connector_id", "source_item_type"),
        Index("ix_source_items_org_connector_seen", "organization_id", "connector_id", "last_seen_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_item_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    parent_source_item_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    source_item_type: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(String(1024), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_checksum: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_version: Mapped[str | None] = mapped_column(String(255), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_modified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'active'"))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_metadata: Mapped[dict[str, object]] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    metadata_schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class SourceItemScopeMembership(Base):
    __tablename__ = "source_item_scope_memberships"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_source_item_scope_memberships"),
        ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_source_scope_memberships_organization", ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "connector_id", "source_item_id"],
            ["source_items.organization_id", "source_items.connector_id", "source_items.id"],
            name="fk_source_scope_memberships_item_tenant", ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "connector_id", "connector_scope_id"],
            ["connector_scopes.organization_id", "connector_scopes.connector_id", "connector_scopes.id"],
            name="fk_source_scope_memberships_scope_tenant", ondelete="CASCADE",
        ),
        UniqueConstraint(
            "organization_id", "source_item_id", "connector_scope_id",
            name="uq_source_scope_memberships_item_scope",
        ),
        CheckConstraint("status IN ('active', 'removed')", name="status_valid"),
        CheckConstraint("last_seen_at >= first_discovered_at", name="seen_order_valid"),
        CheckConstraint(
            "(status = 'removed' AND removed_at IS NOT NULL) OR "
            "(status <> 'removed' AND removed_at IS NULL)",
            name="removal_consistent",
        ),
        Index(
            "ix_source_scope_memberships_org_scope_status",
            "organization_id", "connector_scope_id", "status",
        ),
        Index(
            "ix_source_scope_memberships_org_item_status",
            "organization_id", "source_item_id", "status",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_scope_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'active'"))
    first_discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    removed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class ConnectorSyncRun(Base):
    __tablename__ = "connector_sync_runs"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_connector_sync_runs"),
        ForeignKeyConstraint(["organization_id"], ["organizations.id"], name="fk_sync_runs_organization", ondelete="CASCADE"),
        ForeignKeyConstraint(["organization_id", "connector_id"], ["connectors.organization_id", "connectors.id"], name="fk_sync_runs_connector_tenant", ondelete="CASCADE"),
        ForeignKeyConstraint(["organization_id", "connector_id", "connector_scope_id"], ["connector_scopes.organization_id", "connector_scopes.connector_id", "connector_scopes.id"], name="fk_sync_runs_scope_tenant", ondelete="CASCADE"),
        ForeignKeyConstraint(["organization_id", "connector_id", "connector_scope_id", "parent_run_id"], ["connector_sync_runs.organization_id", "connector_sync_runs.connector_id", "connector_sync_runs.connector_scope_id", "connector_sync_runs.id"], name="fk_sync_runs_parent_tenant", ondelete="SET NULL (parent_run_id)"),
        ForeignKeyConstraint(["organization_id", "initiated_by_user_id"], ["users.organization_id", "users.id"], name="fk_sync_runs_initiator_tenant", ondelete="SET NULL (initiated_by_user_id)"),
        UniqueConstraint("organization_id", "connector_id", "connector_scope_id", "id", name="uq_sync_runs_scope_id"),
        UniqueConstraint("organization_id", "id", name="uq_sync_runs_organization_id_id"),
        CheckConstraint("mode IN ('initial', 'incremental', 'retry', 'reconciliation')", name="mode_valid"),
        CheckConstraint("trigger_type IN ('manual', 'scheduled', 'webhook', 'retry', 'system')", name="trigger_valid"),
        CheckConstraint("status IN ('queued', 'running', 'cancelling', 'cancelled', 'completed', 'completed_with_errors', 'failed')", name="status_valid"),
        CheckConstraint(
            "(status = 'queued' AND started_at IS NULL AND finished_at IS NULL) OR "
            "(status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL) OR "
            "(status = 'cancelling' AND started_at IS NOT NULL AND cancel_requested_at IS NOT NULL AND finished_at IS NULL) OR "
            "(status IN ('cancelled', 'completed', 'completed_with_errors', 'failed') AND started_at IS NOT NULL AND finished_at IS NOT NULL)",
            name="timestamps_match_status",
        ),
        CheckConstraint("finished_at IS NULL OR finished_at >= started_at", name="finished_order_valid"),
        CheckConstraint("heartbeat_at IS NULL OR started_at IS NULL OR heartbeat_at >= started_at", name="heartbeat_order_valid"),
        CheckConstraint("cancel_requested_at IS NULL OR (started_at IS NOT NULL AND cancel_requested_at >= started_at)", name="cancel_order_valid"),
        CheckConstraint("error_summary IS NULL OR btrim(error_summary) <> ''", name="error_summary_not_blank"),
        CheckConstraint("jsonb_typeof(run_metadata) = 'object'", name="metadata_object"),
        CheckConstraint("items_discovered >= 0 AND items_new >= 0 AND items_changed >= 0 AND items_unchanged >= 0 AND items_deleted >= 0 AND items_skipped >= 0 AND items_succeeded >= 0 AND items_failed >= 0", name="counters_nonnegative"),
        CheckConstraint("parent_run_id IS NULL OR parent_run_id <> id", name="parent_not_self"),
        Index("ix_sync_runs_org_scope_created", "organization_id", "connector_scope_id", "created_at"),
        Index("ix_sync_runs_org_connector_status", "organization_id", "connector_id", "status"),
        Index("ix_sync_runs_org_status_created", "organization_id", "status", "created_at"),
        Index("uq_sync_runs_org_scope_active", "organization_id", "connector_scope_id", unique=True, postgresql_where=text("status IN ('running', 'cancelling')")),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_scope_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    parent_run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    mode: Mapped[str] = mapped_column(String(32), nullable=False)
    trigger_type: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'queued'"))
    initiated_by_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    run_metadata: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    items_discovered: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    items_new: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    items_changed: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    items_unchanged: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    items_deleted: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    items_skipped: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    items_succeeded: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    items_failed: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class ConnectorSyncItem(Base):
    __tablename__ = "connector_sync_items"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_connector_sync_items"),
        ForeignKeyConstraint(["organization_id", "connector_id", "connector_scope_id", "sync_run_id"], ["connector_sync_runs.organization_id", "connector_sync_runs.connector_id", "connector_sync_runs.connector_scope_id", "connector_sync_runs.id"], name="fk_sync_items_run_tenant", ondelete="CASCADE"),
        ForeignKeyConstraint(["organization_id", "connector_id", "source_item_id"], ["source_items.organization_id", "source_items.connector_id", "source_items.id"], name="fk_sync_items_source_tenant", ondelete="SET NULL (source_item_id)"),
        UniqueConstraint("organization_id", "sync_run_id", "id", name="uq_sync_items_run_id"),
        UniqueConstraint("organization_id", "sync_run_id", "source_item_key", name="uq_sync_items_run_key"),
        CheckConstraint("btrim(source_item_key) <> ''", name="key_not_blank"),
        CheckConstraint("change_type IN ('new', 'changed', 'unchanged', 'deleted', 'unknown')", name="change_type_valid"),
        CheckConstraint("processing_status IN ('pending', 'processing', 'succeeded', 'skipped', 'failed')", name="status_valid"),
        CheckConstraint("previous_checksum IS NULL OR btrim(previous_checksum) <> ''", name="previous_checksum_not_blank"),
        CheckConstraint("current_checksum IS NULL OR btrim(current_checksum) <> ''", name="current_checksum_not_blank"),
        CheckConstraint("attempt_count >= 0", name="attempt_nonnegative"),
        CheckConstraint("(processing_status = 'pending' AND started_at IS NULL AND finished_at IS NULL) OR (processing_status = 'processing' AND started_at IS NOT NULL AND finished_at IS NULL) OR (processing_status IN ('succeeded', 'skipped', 'failed') AND started_at IS NOT NULL AND finished_at IS NOT NULL)", name="timestamps_match_status"),
        CheckConstraint("finished_at IS NULL OR finished_at >= started_at", name="finished_order_valid"),
        Index("ix_sync_items_org_run_status", "organization_id", "sync_run_id", "processing_status"),
        Index("ix_sync_items_org_source", "organization_id", "source_item_id"),
        Index("ix_sync_items_org_scope_key", "organization_id", "connector_scope_id", "source_item_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_scope_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    sync_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_item_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    source_item_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    change_type: Mapped[str] = mapped_column(String(32), nullable=False)
    processing_status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'pending'"))
    previous_checksum: Mapped[str | None] = mapped_column(String(255), nullable=True)
    current_checksum: Mapped[str | None] = mapped_column(String(255), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())


class ConnectorSyncError(Base):
    __tablename__ = "connector_sync_errors"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_connector_sync_errors"),
        ForeignKeyConstraint(["organization_id", "connector_id", "connector_scope_id", "sync_run_id"], ["connector_sync_runs.organization_id", "connector_sync_runs.connector_id", "connector_sync_runs.connector_scope_id", "connector_sync_runs.id"], name="fk_sync_errors_run_tenant", ondelete="CASCADE"),
        ForeignKeyConstraint(["organization_id", "sync_run_id", "sync_item_id"], ["connector_sync_items.organization_id", "connector_sync_items.sync_run_id", "connector_sync_items.id"], name="fk_sync_errors_item_tenant", ondelete="SET NULL (sync_item_id)"),
        CheckConstraint("error_category IN ('configuration', 'authentication', 'authorization', 'rate_limit', 'source_read', 'extraction', 'persistence', 'embedding', 'permission', 'internal')", name="category_valid"),
        CheckConstraint("error_code ~ '^[a-z][a-z0-9_]*$'", name="code_valid"),
        CheckConstraint("btrim(message) <> ''", name="message_not_blank"),
        CheckConstraint("attempt_number > 0", name="attempt_positive"),
        CheckConstraint("jsonb_typeof(details) = 'object'", name="details_object"),
        CheckConstraint("retry_after_at IS NULL OR retry_after_at >= occurred_at", name="retry_order_valid"),
        CheckConstraint("resolved_at IS NULL OR resolved_at >= occurred_at", name="resolved_order_valid"),
        Index("ix_sync_errors_org_run_occurred", "organization_id", "sync_run_id", "occurred_at"),
        Index("ix_sync_errors_org_item_occurred", "organization_id", "sync_item_id", "occurred_at"),
        Index("ix_sync_errors_org_retry_resolved", "organization_id", "retryable", "resolved_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_scope_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    sync_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    sync_item_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    error_category: Mapped[str] = mapped_column(String(32), nullable=False)
    error_code: Mapped[str] = mapped_column(String(128), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    retryable: Mapped[bool] = mapped_column(Boolean, nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    details: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    retry_after_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class ConnectorSyncCursor(Base):
    __tablename__ = "connector_sync_cursors"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_connector_sync_cursors"),
        ForeignKeyConstraint(["organization_id", "connector_id", "connector_scope_id", "created_by_run_id"], ["connector_sync_runs.organization_id", "connector_sync_runs.connector_id", "connector_sync_runs.connector_scope_id", "connector_sync_runs.id"], name="fk_sync_cursors_run_tenant", ondelete="RESTRICT"),
        UniqueConstraint("organization_id", "connector_scope_id", "cursor_version", name="uq_sync_cursors_scope_version"),
        CheckConstraint("cursor_version > 0", name="version_positive"),
        CheckConstraint("cursor_type ~ '^[a-z][a-z0-9_]*$'", name="type_code_valid"),
        CheckConstraint("state IN ('active', 'superseded', 'invalid')", name="state_valid"),
        CheckConstraint("(safe_cursor IS NOT NULL AND secret_reference IS NULL) OR (safe_cursor IS NULL AND secret_reference IS NOT NULL)", name="storage_exactly_one"),
        CheckConstraint("safe_cursor IS NULL OR jsonb_typeof(safe_cursor) = 'object'", name="safe_cursor_object"),
        CheckConstraint("secret_reference IS NULL OR btrim(secret_reference) <> ''", name="secret_reference_not_blank"),
        CheckConstraint("(state = 'active' AND retired_at IS NULL) OR (state IN ('superseded', 'invalid') AND retired_at IS NOT NULL)", name="retirement_matches_state"),
        CheckConstraint("retired_at IS NULL OR retired_at >= activated_at", name="retired_order_valid"),
        Index("uq_sync_cursors_org_scope_active", "organization_id", "connector_scope_id", unique=True, postgresql_where=text("state = 'active'")),
        Index("ix_sync_cursors_org_scope_version", "organization_id", "connector_scope_id", "cursor_version"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_scope_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    created_by_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    cursor_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    cursor_type: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    safe_cursor: Mapped[dict[str, object] | None] = mapped_column(JSONB, nullable=True)
    secret_reference: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    source_watermark_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    activated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class DocumentVersion(Base):
    __tablename__ = "document_versions"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_document_versions"),
        ForeignKeyConstraint(
            ["organization_id", "connector_id", "source_item_id"],
            ["source_items.organization_id", "source_items.connector_id", "source_items.id"],
            name="fk_document_versions_source_item_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("organization_id", "id", name="uq_document_versions_organization_id_id"),
        UniqueConstraint(
            "organization_id", "source_item_id", "version_number",
            name="uq_document_versions_source_version",
        ),
        CheckConstraint("version_number > 0", name="version_number_positive"),
        CheckConstraint(
            "provider_version_id IS NULL OR btrim(provider_version_id) <> ''",
            name="provider_version_not_blank",
        ),
        CheckConstraint(
            "(content_checksum IS NULL AND checksum_algorithm IS NULL) OR "
            "(content_checksum IS NOT NULL AND btrim(content_checksum) <> '' "
            "AND checksum_algorithm IS NOT NULL)",
            name="checksum_pair_valid",
        ),
        CheckConstraint(
            "checksum_algorithm IS NULL OR checksum_algorithm ~ '^[a-z][a-z0-9_]*$'",
            name="checksum_algorithm_valid",
        ),
        CheckConstraint("source_size_bytes IS NULL OR source_size_bytes >= 0", name="size_nonnegative"),
        CheckConstraint("content_type IS NULL OR btrim(content_type) <> ''", name="content_type_not_blank"),
        CheckConstraint("file_extension IS NULL OR btrim(file_extension) <> ''", name="extension_not_blank"),
        CheckConstraint(
            "version_cause IN ('discovered', 'content_changed', 'metadata_changed', 'restored', 'tombstone', 'manual_backfill')",
            name="cause_valid",
        ),
        CheckConstraint("lifecycle IN ('available', 'unavailable', 'deleted')", name="lifecycle_valid"),
        CheckConstraint(
            "version_cause <> 'tombstone' OR "
            "(lifecycle IN ('unavailable', 'deleted') AND content_checksum IS NULL "
            "AND checksum_algorithm IS NULL AND source_size_bytes IS NULL)",
            name="tombstone_consistent",
        ),
        CheckConstraint("jsonb_typeof(version_metadata) = 'object'", name="metadata_object"),
        CheckConstraint("metadata_schema_version > 0", name="metadata_version_positive"),
        Index(
            "uq_document_versions_current_source",
            "organization_id", "source_item_id",
            unique=True,
            postgresql_where=text("is_current"),
        ),
        Index("ix_document_versions_org_source_number", "organization_id", "source_item_id", "version_number"),
        Index("ix_document_versions_org_checksum", "organization_id", "content_checksum"),
        Index("ix_document_versions_org_provider_version", "organization_id", "provider_version_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_item_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    version_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    provider_version_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    content_checksum: Mapped[str | None] = mapped_column(String(255), nullable=True)
    checksum_algorithm: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_modified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    content_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    file_extension: Mapped[str | None] = mapped_column(String(64), nullable=True)
    version_cause: Mapped[str] = mapped_column(String(32), nullable=False)
    lifecycle: Mapped[str] = mapped_column(String(32), nullable=False)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    version_metadata: Mapped[dict[str, object]] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    metadata_schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("1"))


class DocumentVersionDocument(Base):
    __tablename__ = "document_version_documents"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_document_version_documents"),
        ForeignKeyConstraint(
            ["organization_id", "document_version_id"],
            ["document_versions.organization_id", "document_versions.id"],
            name="fk_version_documents_version_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "document_id"],
            ["documents.organization_id", "documents.id"],
            name="fk_version_documents_document_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "organization_id", "document_version_id", name="uq_version_documents_version"
        ),
        UniqueConstraint("organization_id", "document_id", name="uq_version_documents_document"),
        Index("ix_version_documents_org_document", "organization_id", "document_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    document_version_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    document_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    linked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class DocumentIndexingState(Base):
    __tablename__ = "document_indexing_states"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_document_indexing_states"),
        ForeignKeyConstraint(
            ["organization_id", "document_version_id"],
            ["document_versions.organization_id", "document_versions.id"],
            name="fk_indexing_states_version_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("organization_id", "id", name="uq_indexing_states_organization_id_id"),
        UniqueConstraint(
            "organization_id", "document_version_id", "profile_fingerprint",
            name="uq_indexing_states_version_profile",
        ),
        CheckConstraint(
            "extraction_profile ~ '^[a-z0-9][a-z0-9._:/-]*$' AND "
            "extraction_version ~ '^[a-z0-9][a-z0-9._:/-]*$' AND "
            "chunking_profile ~ '^[a-z0-9][a-z0-9._:/-]*$' AND "
            "chunking_version ~ '^[a-z0-9][a-z0-9._:/-]*$' AND "
            "embedding_provider ~ '^[a-z0-9][a-z0-9._:/-]*$' AND "
            "embedding_model ~ '^[a-z0-9][a-z0-9._:/-]*$' AND "
            "profile_fingerprint ~ '^[a-z0-9][a-z0-9._:/-]*$'",
            name="profile_identifiers_valid",
        ),
        CheckConstraint("embedding_dimensions > 0", name="embedding_dimensions_positive"),
        CheckConstraint("desired_generation > 0", name="desired_generation_positive"),
        CheckConstraint(
            "indexed_generation IS NULL OR "
            "(indexed_generation > 0 AND indexed_generation <= desired_generation)",
            name="indexed_generation_valid",
        ),
        CheckConstraint(
            "status IN ('pending', 'processing', 'indexed', 'stale', 'failed', 'cancelled')",
            name="status_valid",
        ),
        CheckConstraint(
            "reason IN ('new_version', 'content_changed', 'profile_changed', "
            "'embedding_model_changed', 'manual_backfill', 'repair')",
            name="reason_valid",
        ),
        CheckConstraint("attempt_count >= 0", name="attempt_count_nonnegative"),
        CheckConstraint(
            "(last_error_category IS NULL AND last_error_code IS NULL) OR "
            "(last_error_category ~ '^[a-z][a-z0-9_]*$' "
            "AND last_error_code ~ '^[a-z][a-z0-9_]*$')",
            name="error_pair_valid",
        ),
        CheckConstraint(
            "(status = 'pending' AND started_at IS NULL AND completed_at IS NULL) OR "
            "(status = 'processing' AND started_at IS NOT NULL AND completed_at IS NULL) OR "
            "(status = 'indexed' AND completed_at IS NOT NULL "
            "AND indexed_generation = desired_generation AND next_retry_at IS NULL) OR "
            "(status = 'failed' AND completed_at IS NOT NULL "
            "AND last_error_category IS NOT NULL AND last_error_code IS NOT NULL) OR "
            "status IN ('stale', 'cancelled')",
            name="status_state_valid",
        ),
        CheckConstraint(
            "next_retry_at IS NULL OR status IN ('pending', 'failed')",
            name="retry_status_valid",
        ),
        CheckConstraint(
            "completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at",
            name="completed_order_valid",
        ),
        Index("ix_indexing_states_org_status_requested", "organization_id", "status", "requested_at"),
        Index("ix_indexing_states_org_retry", "organization_id", "next_retry_at"),
        Index("ix_indexing_states_org_profile", "organization_id", "profile_fingerprint", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    document_version_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    extraction_profile: Mapped[str] = mapped_column(String(128), nullable=False)
    extraction_version: Mapped[str] = mapped_column(String(64), nullable=False)
    chunking_profile: Mapped[str] = mapped_column(String(128), nullable=False)
    chunking_version: Mapped[str] = mapped_column(String(64), nullable=False)
    embedding_provider: Mapped[str] = mapped_column(String(128), nullable=False)
    embedding_model: Mapped[str] = mapped_column(String(255), nullable=False)
    embedding_dimensions: Mapped[int] = mapped_column(Integer, nullable=False)
    profile_fingerprint: Mapped[str] = mapped_column(String(255), nullable=False)
    desired_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    indexed_generation: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'pending'"))
    reason: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_error_category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class DocumentIndexingAttempt(Base):
    __tablename__ = "document_indexing_attempts"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_document_indexing_attempts"),
        ForeignKeyConstraint(
            ["organization_id", "indexing_state_id"],
            ["document_indexing_states.organization_id", "document_indexing_states.id"],
            name="fk_indexing_attempts_state_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "connector_sync_run_id"],
            ["connector_sync_runs.organization_id", "connector_sync_runs.id"],
            name="fk_indexing_attempts_run_tenant",
            ondelete="SET NULL (connector_sync_run_id)",
        ),
        ForeignKeyConstraint(
            ["organization_id", "connector_sync_run_id", "connector_sync_item_id"],
            ["connector_sync_items.organization_id", "connector_sync_items.sync_run_id", "connector_sync_items.id"],
            name="fk_indexing_attempts_item_tenant",
            ondelete="SET NULL (connector_sync_item_id)",
        ),
        UniqueConstraint(
            "organization_id", "indexing_state_id", "attempt_number",
            name="uq_indexing_attempts_state_number",
        ),
        CheckConstraint("attempt_number > 0", name="attempt_number_positive"),
        CheckConstraint(
            "connector_sync_item_id IS NULL OR connector_sync_run_id IS NOT NULL",
            name="item_requires_run",
        ),
        CheckConstraint(
            "trigger_type IN ('sync', 'retry', 'manual_backfill', 'scheduled_backfill', 'repair')",
            name="trigger_valid",
        ),
        CheckConstraint(
            "status IN ('running', 'succeeded', 'failed', 'cancelled')", name="status_valid"
        ),
        CheckConstraint("worker_reference IS NULL OR btrim(worker_reference) <> ''", name="worker_not_blank"),
        CheckConstraint(
            "(error_category IS NULL AND error_code IS NULL) OR "
            "(error_category ~ '^[a-z][a-z0-9_]*$' AND error_code ~ '^[a-z][a-z0-9_]*$')",
            name="error_pair_valid",
        ),
        CheckConstraint(
            "(status = 'running' AND completed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed', 'cancelled') AND completed_at IS NOT NULL)",
            name="completion_matches_status",
        ),
        CheckConstraint(
            "status <> 'succeeded' OR (error_category IS NULL AND error_code IS NULL)",
            name="success_has_no_error",
        ),
        CheckConstraint(
            "status <> 'failed' OR (error_category IS NOT NULL AND error_code IS NOT NULL)",
            name="failure_has_error",
        ),
        CheckConstraint("completed_at IS NULL OR completed_at >= started_at", name="completed_order_valid"),
        CheckConstraint("jsonb_typeof(summary) = 'object'", name="summary_object"),
        CheckConstraint("summary_schema_version > 0", name="summary_version_positive"),
        Index("ix_indexing_attempts_org_state_number", "organization_id", "indexing_state_id", "attempt_number"),
        Index("ix_indexing_attempts_org_sync_run", "organization_id", "connector_sync_run_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    indexing_state_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    connector_sync_run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    connector_sync_item_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    trigger_type: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    worker_reference: Mapped[str | None] = mapped_column(String(255), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    retryable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    summary: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    summary_schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class Document(Base):
    __tablename__ = "documents"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_documents"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_documents_organization_id_organizations",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "organization_id",
            "source_type",
            "source_document_key",
            name="uq_documents_organization_id_source_type_source_document_key",
        ),
        UniqueConstraint("organization_id", "id", name="uq_documents_organization_id_id"),
        CheckConstraint("btrim(source_type) <> ''", name="documents_source_type_not_blank"),
        CheckConstraint("btrim(title) <> ''", name="documents_title_not_blank"),
        CheckConstraint(
            "status IN ('pending', 'processing', 'ready', 'failed')",
            name="documents_status_valid",
        ),
        Index("ix_documents_organization_id_status", "organization_id", "status"),
        Index("ix_documents_organization_id_source_type", "organization_id", "source_type"),
        Index("ix_documents_organization_id_deleted_at", "organization_id", "deleted_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_document_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    checksum_latest: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'pending'"))
    source_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    organization: Mapped[Organization] = relationship(back_populates="documents")
    chunks: Mapped[list["DocumentChunk"]] = relationship(back_populates="document")


class DocumentChunk(Base):
    __tablename__ = "document_chunks"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_document_chunks"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_document_chunks_organization_id_organizations",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "document_id"],
            ["documents.organization_id", "documents.id"],
            name="fk_document_chunks_organization_id_document_id_documents",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "organization_id",
            "document_id",
            "chunk_index",
            name="uq_document_chunks_organization_id_document_id_chunk_index",
        ),
        CheckConstraint("chunk_index >= 0", name="document_chunks_chunk_index_nonnegative"),
        CheckConstraint(
            "token_count IS NULL OR token_count >= 0",
            name="document_chunks_token_count_nonnegative",
        ),
        CheckConstraint("btrim(chunk_text) <> ''", name="document_chunks_chunk_text_not_blank"),
        CheckConstraint("btrim(content_hash) <> ''", name="document_chunks_content_hash_not_blank"),
        CheckConstraint(
            "embedding_model IS NULL OR btrim(embedding_model) <> ''",
            name="document_chunks_embedding_model_not_blank",
        ),
        CheckConstraint(
            "(embedding IS NULL AND embedding_model IS NULL) OR "
            "(embedding IS NOT NULL AND embedding_model IS NOT NULL)",
            name="document_chunks_embedding_model_pair",
        ),
        Index("ix_document_chunks_organization_id_document_id", "organization_id", "document_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    document_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    chunk_text: Mapped[str] = mapped_column(Text, nullable=False)
    token_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1536), nullable=True)
    embedding_model: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    document: Mapped[Document] = relationship(back_populates="chunks")


class OrganizationSettings(Base):
    __tablename__ = "organization_settings"
    __table_args__ = (
        PrimaryKeyConstraint("organization_id", name="pk_organization_settings"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_organization_settings_organization_id_organizations",
            ondelete="CASCADE",
        ),
        CheckConstraint("btrim(default_locale) <> ''", name="organization_settings_default_locale_not_blank"),
        CheckConstraint("btrim(timezone) <> ''", name="organization_settings_timezone_not_blank"),
        CheckConstraint(
            "retention_days BETWEEN 1 AND 3650",
            name="organization_settings_retention_days_range",
        ),
        CheckConstraint(
            "ai_model_name IS NULL OR btrim(ai_model_name) <> ''",
            name="organization_settings_ai_model_name_not_blank",
        ),
    )

    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    default_locale: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'en-US'"))
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, server_default=text("'UTC'"))
    retention_days: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("365"))
    ai_model_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    organization: Mapped[Organization] = relationship(back_populates="settings", foreign_keys=[organization_id])


class Role(Base):
    __tablename__ = "roles"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_roles"),
        UniqueConstraint("name", name="uq_roles_name"),
        CheckConstraint("btrim(name) <> ''", name="roles_name_not_blank"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_system_role: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    user_roles: Mapped[list["UserRole"]] = relationship(back_populates="role")


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_users"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_users_organization_id_organizations",
            ondelete="CASCADE",
        ),
        UniqueConstraint("organization_id", "normalized_email", name="uq_users_organization_id_normalized_email"),
        UniqueConstraint("organization_id", "id", name="uq_users_organization_id_id"),
        CheckConstraint("normalized_email = lower(btrim(email))", name="users_normalized_email_matches_email"),
        CheckConstraint("status IN ('active', 'suspended', 'disabled')", name="users_status_valid"),
        CheckConstraint("btrim(password_hash) <> ''", name="users_password_hash_not_blank"),
        Index("ix_users_organization_id_status", "organization_id", "status"),
        Index("ix_users_organization_id_last_login_at", "organization_id", "last_login_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    normalized_email: Mapped[str] = mapped_column(String(320), nullable=False)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    first_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    # Version 1 requires a non-null display_name.
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'active'"))
    email_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    organization: Mapped[Organization] = relationship(back_populates="users", foreign_keys=[organization_id])
    role_assignments: Mapped[list["UserRole"]] = relationship(back_populates="user", foreign_keys="UserRole.user_id")
    auth_sessions: Mapped[list["AuthenticationSession"]] = relationship(back_populates="user", foreign_keys="AuthenticationSession.user_id")
    department_memberships: Mapped[list["DepartmentMembership"]] = relationship(
        back_populates="user", foreign_keys="DepartmentMembership.user_id"
    )
    team_memberships: Mapped[list["TeamMembership"]] = relationship(
        back_populates="user", foreign_keys="TeamMembership.user_id"
    )


class DepartmentMembership(Base):
    __tablename__ = "department_memberships"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_department_memberships"),
        ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_department_memberships_organization_id_organizations", ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "department_id"], ["departments.organization_id", "departments.id"],
            name="fk_department_memberships_department_tenant", ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["organization_id", "user_id"], ["users.organization_id", "users.id"],
            name="fk_department_memberships_organization_id_user_id_users", ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "created_by_user_id"], ["users.organization_id", "users.id"],
            name="fk_department_memberships_creator_tenant", ondelete="SET NULL (created_by_user_id)",
        ),
        UniqueConstraint(
            "organization_id", "department_id", "user_id",
            name="uq_department_memberships_entity_user",
        ),
        CheckConstraint("responsibility IN ('member', 'manager')", name="department_memberships_responsibility_valid"),
        CheckConstraint("status IN ('active', 'inactive', 'revoked')", name="department_memberships_status_valid"),
        CheckConstraint("expires_at IS NULL OR expires_at > effective_from", name="department_memberships_expiry_after_effective"),
        CheckConstraint("revoked_at IS NULL OR revoked_at >= effective_from", name="department_memberships_revoked_after_effective"),
        CheckConstraint(
            "(status = 'revoked' AND revoked_at IS NOT NULL) OR "
            "(status <> 'revoked' AND revoked_at IS NULL)",
            name="department_memberships_revocation_consistent",
        ),
        Index("ix_department_memberships_organization_id_user_id_status", "organization_id", "user_id", "status"),
        Index("ix_department_memberships_organization_id_department_id_status", "organization_id", "department_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    department_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    responsibility: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'active'"))
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    department: Mapped[Department] = relationship(back_populates="memberships")
    user: Mapped[User] = relationship(back_populates="department_memberships", foreign_keys=[user_id])


class TeamMembership(Base):
    __tablename__ = "team_memberships"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_team_memberships"),
        ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_team_memberships_organization_id_organizations", ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "team_id"], ["teams.organization_id", "teams.id"],
            name="fk_team_memberships_team_tenant", ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["organization_id", "user_id"], ["users.organization_id", "users.id"],
            name="fk_team_memberships_organization_id_user_id_users", ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "created_by_user_id"], ["users.organization_id", "users.id"],
            name="fk_team_memberships_creator_tenant", ondelete="SET NULL (created_by_user_id)",
        ),
        UniqueConstraint("organization_id", "team_id", "user_id", name="uq_team_memberships_entity_user"),
        CheckConstraint("responsibility IN ('member', 'lead', 'manager', 'owner')", name="team_memberships_responsibility_valid"),
        CheckConstraint("status IN ('active', 'inactive', 'revoked')", name="team_memberships_status_valid"),
        CheckConstraint("expires_at IS NULL OR expires_at > effective_from", name="team_memberships_expiry_after_effective"),
        CheckConstraint("revoked_at IS NULL OR revoked_at >= effective_from", name="team_memberships_revoked_after_effective"),
        CheckConstraint(
            "(status = 'revoked' AND revoked_at IS NOT NULL) OR "
            "(status <> 'revoked' AND revoked_at IS NULL)",
            name="team_memberships_revocation_consistent",
        ),
        Index("ix_team_memberships_organization_id_user_id_status", "organization_id", "user_id", "status"),
        Index("ix_team_memberships_organization_id_team_id_status", "organization_id", "team_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    team_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    responsibility: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default=text("'active'"))
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    team: Mapped[Team] = relationship(back_populates="memberships")
    user: Mapped[User] = relationship(back_populates="team_memberships", foreign_keys=[user_id])


class AuditEvent(Base):
    __tablename__ = "audit_events"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_audit_events"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_audit_events_organization_id_organizations",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["organization_id", "actor_user_id"],
            ["users.organization_id", "users.id"],
            name="fk_audit_events_actor_user_tenant",
            ondelete="RESTRICT",
        ),
        CheckConstraint("actor_type IN ('user', 'system', 'service')", name="actor_type_valid"),
        CheckConstraint(
            "(actor_type = 'user' AND actor_user_id IS NOT NULL) OR "
            "(actor_type IN ('system', 'service') AND actor_user_id IS NULL "
            "AND actor_reference IS NOT NULL AND btrim(actor_reference) <> '')",
            name="actor_consistent",
        ),
        CheckConstraint("btrim(action) <> ''", name="action_not_blank"),
        CheckConstraint("btrim(resource_type) <> ''", name="resource_type_not_blank"),
        CheckConstraint("outcome IN ('success', 'failure', 'denied')", name="outcome_valid"),
        CheckConstraint("reason IS NULL OR btrim(reason) <> ''", name="reason_not_blank"),
        CheckConstraint("request_id IS NULL OR btrim(request_id) <> ''", name="request_id_not_blank"),
        CheckConstraint("actor_reference IS NULL OR btrim(actor_reference) <> ''", name="actor_reference_not_blank"),
        CheckConstraint("schema_version > 0", name="schema_version_positive"),
        CheckConstraint("jsonb_typeof(change_summary) = 'object'", name="change_summary_object"),
        CheckConstraint("jsonb_typeof(context) = 'object'", name="context_object"),
        Index("ix_audit_events_organization_id_occurred_at", "organization_id", "occurred_at"),
        Index("ix_audit_events_org_actor_occurred", "organization_id", "actor_user_id", "occurred_at"),
        Index("ix_audit_events_org_resource_occurred", "organization_id", "resource_type", "resource_id", "occurred_at"),
        Index("ix_audit_events_org_action_occurred", "organization_id", "action", "occurred_at"),
        Index(
            "ix_audit_events_org_correlation",
            "organization_id",
            "correlation_id",
            postgresql_where=text("correlation_id IS NOT NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    actor_type: Mapped[str] = mapped_column(String(32), nullable=False)
    actor_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    actor_reference: Mapped[str | None] = mapped_column(String(255), nullable=True)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(128), nullable=False)
    resource_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    correlation_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    change_summary: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    context: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("1"))


class UserRole(Base):
    __tablename__ = "user_roles"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_user_roles"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_user_roles_organization_id_organizations",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "user_id"],
            ["users.organization_id", "users.id"],
            name="fk_user_roles_organization_id_user_id_users",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["role_id"],
            ["roles.id"],
            name="fk_user_roles_role_id_roles",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("organization_id", "user_id", "role_id", name="uq_user_roles_organization_id_user_id_role_id"),
        Index("ix_user_roles_organization_id_user_id", "organization_id", "user_id"),
        Index("ix_user_roles_organization_id_role_id", "organization_id", "role_id"),
        Index("ix_user_roles_assigned_by_user_id", "assigned_by_user_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    role_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    assigned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    # Audit metadata only in Version 1; keeping this as a nullable UUID avoids cross-tenant FK coupling.
    assigned_by_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)

    organization: Mapped[Organization] = relationship(foreign_keys=[organization_id])
    user: Mapped[User] = relationship(back_populates="role_assignments", foreign_keys=[user_id])
    role: Mapped[Role] = relationship(back_populates="user_roles")


class AuthenticationSession(Base):
    __tablename__ = "authentication_sessions"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_authentication_sessions"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_authentication_sessions_organization_id_organizations",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["organization_id", "user_id"],
            ["users.organization_id", "users.id"],
            name="fk_authentication_sessions_organization_id_user_id_users",
            ondelete="CASCADE",
        ),
        UniqueConstraint("refresh_token_hash", name="uq_authentication_sessions_refresh_token_hash"),
        CheckConstraint(
            "expires_at > created_at",
            name="authentication_sessions_expires_after_created_at",
        ),
        CheckConstraint(
            "revoked_at IS NULL OR revoked_at >= created_at",
            name="authentication_sessions_revoked_at_after_created_at",
        ),
        CheckConstraint(
            "last_used_at IS NULL OR last_used_at >= created_at",
            name="authentication_sessions_last_used_at_after_created_at",
        ),
        Index(
            "ix_authentication_sessions_org_user_active",
            "organization_id",
            "user_id",
            "expires_at",
            postgresql_where=text("revoked_at IS NULL"),
        ),
        Index("ix_authentication_sessions_expires_at", "expires_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    refresh_token_hash: Mapped[bytes] = mapped_column(BYTEA, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ip_address: Mapped[str | None] = mapped_column(INET, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)

    organization: Mapped[Organization] = relationship(foreign_keys=[organization_id])
    user: Mapped[User] = relationship(back_populates="auth_sessions", foreign_keys=[user_id])
