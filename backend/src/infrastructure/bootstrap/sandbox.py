"""One-time, fail-closed bootstrap for the controlled sandbox only."""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from uuid import UUID, uuid4

from pydantic import EmailStr, TypeAdapter, ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import load_database_settings
from infrastructure.db.models import (
    KnowledgeSpace,
    KnowledgeSpaceUserGrant,
    Organization,
    Role,
    User,
    UserRole,
)
from infrastructure.db.session import SessionLocal
from infrastructure.security.passwords import (
    hash_password,
    validate_password_strength,
    verify_password,
)


ADMIN_ROLE_NAME = "organization_admin"
BOOTSTRAP_PERMISSION = "manager"
_SLUG = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
_EMAIL = TypeAdapter(EmailStr)


class SandboxBootstrapError(Exception):
    """Fixed-safe operational failure."""


@dataclass(frozen=True, repr=False)
class SandboxBootstrapInput:
    organization_name: str
    organization_slug: str
    admin_email: str
    admin_password: str
    admin_first_name: str
    admin_last_name: str
    knowledge_space_name: str
    knowledge_space_slug: str


@dataclass(frozen=True)
class SandboxBootstrapResult:
    outcome: str
    organization_id: UUID
    user_id: UUID
    knowledge_space_id: UUID


def load_bootstrap_input(
    environ: Mapping[str, str] | None = None,
) -> SandboxBootstrapInput:
    values = os.environ if environ is None else environ
    database = load_database_settings(values)
    if database.runtime_environment != "sandbox":
        raise SandboxBootstrapError("Sandbox bootstrap is not authorized")
    if values.get("ALLOW_SANDBOX_BOOTSTRAP") != "true":
        raise SandboxBootstrapError("Sandbox bootstrap is not authorized")
    required = {
        "organization_name": "SANDBOX_BOOTSTRAP_ORGANIZATION_NAME",
        "organization_slug": "SANDBOX_BOOTSTRAP_ORGANIZATION_SLUG",
        "admin_email": "SANDBOX_BOOTSTRAP_ADMIN_EMAIL",
        "admin_password": "SANDBOX_BOOTSTRAP_ADMIN_PASSWORD",
        "admin_first_name": "SANDBOX_BOOTSTRAP_ADMIN_FIRST_NAME",
        "admin_last_name": "SANDBOX_BOOTSTRAP_ADMIN_LAST_NAME",
        "knowledge_space_name": "SANDBOX_BOOTSTRAP_KNOWLEDGE_SPACE_NAME",
        "knowledge_space_slug": "SANDBOX_BOOTSTRAP_KNOWLEDGE_SPACE_SLUG",
    }
    if any(not values.get(variable) for variable in required.values()):
        raise SandboxBootstrapError("Sandbox bootstrap configuration is invalid")
    try:
        email = str(_EMAIL.validate_python(values[required["admin_email"]])).lower()
        password = validate_password_strength(values[required["admin_password"]])
        organization_name = _name(values[required["organization_name"]], 255)
        organization_slug = _slug(values[required["organization_slug"]])
        first_name = _name(values[required["admin_first_name"]], 100)
        last_name = _name(values[required["admin_last_name"]], 100)
        space_name = _name(values[required["knowledge_space_name"]], 255)
        space_slug = _slug(values[required["knowledge_space_slug"]])
    except (ValueError, ValidationError) as exc:
        raise SandboxBootstrapError(
            "Sandbox bootstrap configuration is invalid"
        ) from exc
    return SandboxBootstrapInput(
        organization_name,
        organization_slug,
        email,
        password,
        first_name,
        last_name,
        space_name,
        space_slug,
    )


def bootstrap_sandbox(
    session: Session, values: SandboxBootstrapInput
) -> SandboxBootstrapResult:
    role = session.execute(
        select(Role).where(Role.name == ADMIN_ROLE_NAME)
    ).scalar_one_or_none()
    if role is None or role.is_system_role is not True:
        raise SandboxBootstrapError("Sandbox bootstrap state conflicts")

    organization = session.execute(
        select(Organization).where(Organization.slug == values.organization_slug)
    ).scalar_one_or_none()
    users = session.execute(
        select(User).where(User.normalized_email == values.admin_email)
    ).scalars().all()
    space = None
    if organization is not None:
        space = session.execute(
            select(KnowledgeSpace).where(
                KnowledgeSpace.organization_id == organization.id,
                KnowledgeSpace.slug == values.knowledge_space_slug,
            )
        ).scalar_one_or_none()

    if organization is None and not users and space is None:
        return _create_state(session, values, role)
    if organization is None or len(users) != 1 or space is None:
        raise SandboxBootstrapError("Sandbox bootstrap state conflicts")
    return _verify_state(session, values, role, organization, users[0], space)


def _create_state(
    session: Session, values: SandboxBootstrapInput, role: Role
) -> SandboxBootstrapResult:
    organization = Organization(
        id=uuid4(),
        name=values.organization_name,
        slug=values.organization_slug,
        status="active",
    )
    session.add(organization)
    session.flush()

    user = User(
        id=uuid4(),
        organization_id=organization.id,
        email=values.admin_email,
        normalized_email=values.admin_email,
        password_hash=hash_password(values.admin_password),
        first_name=values.admin_first_name,
        last_name=values.admin_last_name,
        display_name=f"{values.admin_first_name} {values.admin_last_name}",
        status="active",
    )
    space = KnowledgeSpace(
        id=uuid4(),
        organization_id=organization.id,
        name=values.knowledge_space_name,
        slug=values.knowledge_space_slug,
        status="active",
    )
    session.add_all([user, space])
    session.flush()

    session.add_all(
        [
            UserRole(
                id=uuid4(),
                organization_id=organization.id,
                user_id=user.id,
                role_id=role.id,
                assigned_by_user_id=None,
            ),
            KnowledgeSpaceUserGrant(
                id=uuid4(),
                organization_id=organization.id,
                knowledge_space_id=space.id,
                user_id=user.id,
                permission_level=BOOTSTRAP_PERMISSION,
                granted_by_user_id=user.id,
                reason="sandbox_bootstrap",
            ),
        ]
    )
    session.flush()
    return SandboxBootstrapResult("created", organization.id, user.id, space.id)


def _verify_state(
    session: Session,
    values: SandboxBootstrapInput,
    role: Role,
    organization: Organization,
    user: User,
    space: KnowledgeSpace,
) -> SandboxBootstrapResult:
    expected_display_name = f"{values.admin_first_name} {values.admin_last_name}"
    role_assignment = session.execute(
        select(UserRole).where(
            UserRole.organization_id == organization.id,
            UserRole.user_id == user.id,
            UserRole.role_id == role.id,
        )
    ).scalar_one_or_none()
    grant = session.execute(
        select(KnowledgeSpaceUserGrant).where(
            KnowledgeSpaceUserGrant.organization_id == organization.id,
            KnowledgeSpaceUserGrant.knowledge_space_id == space.id,
            KnowledgeSpaceUserGrant.user_id == user.id,
        )
    ).scalar_one_or_none()
    exact = (
        organization.name == values.organization_name
        and organization.status == "active"
        and organization.deleted_at is None
        and user.organization_id == organization.id
        and user.email == values.admin_email
        and user.status == "active"
        and user.first_name == values.admin_first_name
        and user.last_name == values.admin_last_name
        and user.display_name == expected_display_name
        and verify_password(values.admin_password, user.password_hash)
        and role_assignment is not None
        and role_assignment.assigned_by_user_id is None
        and space.name == values.knowledge_space_name
        and space.status == "active"
        and space.archived_at is None
        and grant is not None
        and grant.permission_level == BOOTSTRAP_PERMISSION
        and grant.granted_by_user_id == user.id
        and grant.revoked_at is None
        and grant.expires_at is None
        and grant.reason == "sandbox_bootstrap"
    )
    if not exact:
        raise SandboxBootstrapError("Sandbox bootstrap state conflicts")
    return SandboxBootstrapResult("verified", organization.id, user.id, space.id)


def run(
    argv: Sequence[str],
    *,
    environ: Mapping[str, str] | None = None,
    session_factory=SessionLocal,
) -> int:
    if argv:
        print("Sandbox bootstrap failed")
        return 1
    session = None
    try:
        values = load_bootstrap_input(environ)
        session = session_factory()
        result = bootstrap_sandbox(session, values)
        session.commit()
    except Exception:
        if session is not None:
            session.rollback()
        print("Sandbox bootstrap failed")
        return 1
    finally:
        if session is not None:
            session.close()
    print(
        f"bootstrap_status={result.outcome} "
        f"organization_id={result.organization_id} user_id={result.user_id} "
        f"knowledge_space_id={result.knowledge_space_id}"
    )
    return 0


def _name(value: str, maximum: int) -> str:
    if value != value.strip() or not value or len(value) > maximum:
        raise ValueError
    return value


def _slug(value: str) -> str:
    if len(value) > 255 or _SLUG.fullmatch(value) is None:
        raise ValueError
    return value


def main(argv: Sequence[str] | None = None) -> int:
    return run(tuple(sys.argv[1:] if argv is None else argv))


if __name__ == "__main__":
    raise SystemExit(main())
