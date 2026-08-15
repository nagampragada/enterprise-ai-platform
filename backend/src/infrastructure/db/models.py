"""SQLAlchemy 2.x models for the platform database metadata."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    text,
    func,
)
from sqlalchemy.dialects.postgresql import BYTEA, INET, UUID
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
