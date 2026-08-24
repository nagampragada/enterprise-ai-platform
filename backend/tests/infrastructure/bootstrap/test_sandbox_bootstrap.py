from __future__ import annotations

from uuid import uuid4

import pytest

from infrastructure.bootstrap.sandbox import (
    SandboxBootstrapError,
    SandboxBootstrapInput,
    bootstrap_sandbox,
    load_bootstrap_input,
    run,
)
from infrastructure.db.models import (
    KnowledgeSpace,
    KnowledgeSpaceUserGrant,
    Organization,
    Role,
    User,
    UserRole,
)
from infrastructure.security.passwords import hash_password


PASSWORD = "Strong-Sandbox-Password-123!"
DATABASE = "postgresql+psycopg://sandbox:S4ndboxDbValue9284@database:5432/platform"


def _environment(runtime: str = "sandbox") -> dict[str, str]:
    return {
        "APP_ENVIRONMENT": runtime,
        "DATABASE_URL": DATABASE,
        "ALLOW_SANDBOX_BOOTSTRAP": "true",
        "SANDBOX_BOOTSTRAP_ORGANIZATION_NAME": "Controlled Sandbox",
        "SANDBOX_BOOTSTRAP_ORGANIZATION_SLUG": "controlled-sandbox",
        "SANDBOX_BOOTSTRAP_ADMIN_EMAIL": "Admin@example.com",
        "SANDBOX_BOOTSTRAP_ADMIN_PASSWORD": PASSWORD,
        "SANDBOX_BOOTSTRAP_ADMIN_FIRST_NAME": "Sandbox",
        "SANDBOX_BOOTSTRAP_ADMIN_LAST_NAME": "Admin",
        "SANDBOX_BOOTSTRAP_KNOWLEDGE_SPACE_NAME": "GitHub Test",
        "SANDBOX_BOOTSTRAP_KNOWLEDGE_SPACE_SLUG": "github-test",
    }


def _input() -> SandboxBootstrapInput:
    return load_bootstrap_input(_environment())


class Result:
    def __init__(self, *, one=None, many=None):
        self.one = one
        self.many = [] if many is None else many

    def scalar_one_or_none(self):
        return self.one

    def scalars(self):
        return self

    def all(self):
        return self.many


class Session:
    def __init__(self, results):
        self.results = iter(results)
        self.added = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def execute(self, _statement):
        return next(self.results)

    def add_all(self, values):
        self.added.extend(values)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def _role() -> Role:
    return Role(id=uuid4(), name="organization_admin", is_system_role=True)


def test_bootstrap_refuses_non_sandbox_and_requires_exact_opt_in() -> None:
    for runtime in ("development", "test", "production"):
        with pytest.raises(SandboxBootstrapError):
            load_bootstrap_input(_environment(runtime))
    values = _environment()
    values["ALLOW_SANDBOX_BOOTSTRAP"] = "TRUE"
    with pytest.raises(SandboxBootstrapError):
        load_bootstrap_input(values)


def test_bootstrap_never_accepts_command_line_password(capsys) -> None:
    assert run(["--admin-password", PASSWORD], environ=_environment()) == 1
    assert PASSWORD not in capsys.readouterr().out


def test_bootstrap_creates_complete_state_without_secret_output(capsys) -> None:
    session = Session([Result(one=_role()), Result(one=None), Result(many=[])])

    assert run([], environ=_environment(), session_factory=lambda: session) == 0

    assert session.committed and session.closed and not session.rolled_back
    assert len(session.added) == 5
    assert {type(value) for value in session.added} == {
        Organization,
        User,
        UserRole,
        KnowledgeSpace,
        KnowledgeSpaceUserGrant,
    }
    assert PASSWORD not in capsys.readouterr().out


def test_exact_existing_state_is_idempotently_verified() -> None:
    values = _input()
    role = _role()
    organization = Organization(
        id=uuid4(), name=values.organization_name, slug=values.organization_slug,
        status="active", deleted_at=None,
    )
    user = User(
        id=uuid4(), organization_id=organization.id, email=values.admin_email,
        normalized_email=values.admin_email, password_hash=hash_password(PASSWORD),
        first_name=values.admin_first_name, last_name=values.admin_last_name,
        display_name="Sandbox Admin", status="active",
    )
    space = KnowledgeSpace(
        id=uuid4(), organization_id=organization.id, name=values.knowledge_space_name,
        slug=values.knowledge_space_slug, status="active", archived_at=None,
    )
    assignment = UserRole(
        id=uuid4(), organization_id=organization.id, user_id=user.id,
        role_id=role.id, assigned_by_user_id=None,
    )
    grant = KnowledgeSpaceUserGrant(
        id=uuid4(), organization_id=organization.id, knowledge_space_id=space.id,
        user_id=user.id, permission_level="manager", granted_by_user_id=user.id,
        revoked_at=None, expires_at=None, reason="sandbox_bootstrap",
    )
    session = Session(
        [
            Result(one=role), Result(one=organization), Result(many=[user]),
            Result(one=space), Result(one=assignment), Result(one=grant),
        ]
    )

    result = bootstrap_sandbox(session, values)

    assert result.outcome == "verified"
    assert session.added == []


def test_conflicting_state_rolls_back_and_returns_fixed_safe_error(capsys) -> None:
    role = _role()
    conflicting = Organization(
        id=uuid4(), name="Different", slug="controlled-sandbox", status="active"
    )
    session = Session(
        [Result(one=role), Result(one=conflicting), Result(many=[]), Result(one=None)]
    )

    assert run([], environ=_environment(), session_factory=lambda: session) == 1

    assert session.rolled_back and session.closed and not session.committed
    assert capsys.readouterr().out.strip() == "Sandbox bootstrap failed"
