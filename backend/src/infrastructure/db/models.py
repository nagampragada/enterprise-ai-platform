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
