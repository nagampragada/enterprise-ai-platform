"""Development bootstrap script to create the first organization administrator."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from typing import TypeAlias
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from infrastructure.db.models import Organization, Role, User, UserRole
from infrastructure.db.session import session_scope
from infrastructure.security.passwords import hash_password


ADMIN_ROLE_NAME = "organization_admin"


class AdminBootstrapError(Exception):
    """Safe, user-facing bootstrap failure."""


SessionContextFactory: TypeAlias = Callable[[], AbstractContextManager[Session]]


def normalize_email(email: str) -> str:
    return email.strip().lower()


def _build_display_name(first_name: str, last_name: str) -> str:
    return f"{first_name.strip()} {last_name.strip()}".strip()


def _get_organization_by_slug(session: Session, organization_slug: str) -> Organization | None:
    statement = select(Organization).where(Organization.slug == organization_slug)
    return session.execute(statement).scalar_one_or_none()


def _get_role_by_name(session: Session, role_name: str) -> Role | None:
    statement = select(Role).where(Role.name == role_name)
    return session.execute(statement).scalar_one_or_none()


def _get_user_by_normalized_email(
    session: Session,
    organization_id,
    normalized_email: str,
) -> User | None:
    statement = select(User).where(
        User.organization_id == organization_id,
        User.normalized_email == normalized_email,
    )
    return session.execute(statement).scalar_one_or_none()


def create_admin_user(
    session: Session,
    *,
    organization_name: str,
    organization_slug: str,
    admin_email: str,
    admin_password: str,
    first_name: str,
    last_name: str,
) -> None:
    normalized_email = normalize_email(admin_email)

    organization = _get_organization_by_slug(session, organization_slug)
    if organization is None:
        organization = Organization(
            id=uuid4(),
            name=organization_name,
            slug=organization_slug,
            status="active",
        )
        session.add(organization)

    admin_role = _get_role_by_name(session, ADMIN_ROLE_NAME)
    if admin_role is None:
        raise AdminBootstrapError("Required organization administrator role is missing.")

    existing_user = _get_user_by_normalized_email(
        session,
        organization_id=organization.id,
        normalized_email=normalized_email,
    )
    if existing_user is not None:
        raise AdminBootstrapError("An administrator with that email already exists for this organization.")

    password_hash = hash_password(admin_password)
    display_name = _build_display_name(first_name=first_name, last_name=last_name)

    user = User(
        id=uuid4(),
        organization_id=organization.id,
        email=normalized_email,
        normalized_email=normalized_email,
        password_hash=password_hash,
        first_name=first_name.strip() or None,
        last_name=last_name.strip() or None,
        display_name=display_name,
        status="active",
    )
    session.add(user)

    user_role = UserRole(
        id=uuid4(),
        organization_id=organization.id,
        user_id=user.id,
        role_id=admin_role.id,
        assigned_by_user_id=None,
    )
    session.add(user_role)


def run_bootstrap(
    *,
    organization_name: str,
    organization_slug: str,
    admin_email: str,
    admin_password: str,
    first_name: str,
    last_name: str,
    session_context_factory: SessionContextFactory = session_scope,
) -> int:
    try:
        with session_context_factory() as session:
            create_admin_user(
                session,
                organization_name=organization_name,
                organization_slug=organization_slug,
                admin_email=admin_email,
                admin_password=admin_password,
                first_name=first_name,
                last_name=last_name,
            )
    except AdminBootstrapError as exc:
        print(f"Error: {exc}")
        return 1
    except Exception:
        print("Error: Could not create administrator user.")
        return 1

    print("Administrator user created successfully.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create the first organization administrator.")
    parser.add_argument("--organization-name", required=True, help="Organization display name")
    parser.add_argument("--organization-slug", required=True, help="Organization slug")
    parser.add_argument("--admin-email", required=True, help="Administrator email")
    parser.add_argument("--admin-password", required=True, help="Administrator password")
    parser.add_argument("--first-name", required=True, help="Administrator first name")
    parser.add_argument("--last-name", required=True, help="Administrator last name")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return run_bootstrap(
        organization_name=args.organization_name,
        organization_slug=args.organization_slug,
        admin_email=args.admin_email,
        admin_password=args.admin_password,
        first_name=args.first_name,
        last_name=args.last_name,
    )


if __name__ == "__main__":
    raise SystemExit(main())
