"""User repository implementation using SQLAlchemy 2.x."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import exists, select
from sqlalchemy.orm import Session

from infrastructure.db.models import Organization, Role, User, UserRole


class UserRepository:
    """Repository for tenant-scoped user persistence operations."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get_by_id(self, organization_id: UUID, user_id: UUID) -> User | None:
        """Return a user by tenant and user identifier."""
        statement = select(User).where(
            User.organization_id == organization_id,
            User.id == user_id,
        )
        return self._session.execute(statement).scalar_one_or_none()

    def get_by_normalized_email(self, organization_id: UUID, normalized_email: str) -> User | None:
        """Return a user by tenant and normalized email address."""
        statement = select(User).where(
            User.organization_id == organization_id,
            User.normalized_email == normalized_email,
        )
        return self._session.execute(statement).scalar_one_or_none()

    def is_active_organization_admin(self, organization_id: UUID, user_id: UUID) -> bool:
        """Return whether an active tenant user has the committed administrator role."""
        statement = select(
            exists().where(
                Organization.id == organization_id,
                Organization.id == User.organization_id,
                Organization.status == "active",
                Organization.deleted_at.is_(None),
                User.organization_id == organization_id,
                User.id == user_id,
                User.status == "active",
                UserRole.organization_id == organization_id,
                UserRole.organization_id == User.organization_id,
                UserRole.user_id == user_id,
                UserRole.user_id == User.id,
                Role.id == UserRole.role_id,
                Role.name == "organization_admin",
            )
        )
        return bool(self._session.execute(statement).scalar_one())

    def add(self, user: User) -> User:
        """Add a new user entity to the active session."""
        self._session.add(user)
        return user

    def update_last_login(self, organization_id: UUID, user_id: UUID, last_login_at: datetime) -> User | None:
        """Update last login time for a tenant-scoped user."""
        user = self.get_by_id(organization_id=organization_id, user_id=user_id)
        if user is None:
            return None

        user.last_login_at = last_login_at
        return user

    def update_status(self, organization_id: UUID, user_id: UUID, status: str) -> User | None:
        """Update status for a tenant-scoped user."""
        user = self.get_by_id(organization_id=organization_id, user_id=user_id)
        if user is None:
            return None

        user.status = status
        return user
