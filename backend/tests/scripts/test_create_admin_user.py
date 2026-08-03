from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import uuid4

from infrastructure.db.models import Organization, Role, User, UserRole
from scripts.create_admin_user import (
    ADMIN_ROLE_NAME,
    AdminBootstrapError,
    create_admin_user,
    run_bootstrap,
)


@dataclass
class FakeSession:
    commit_calls: int = 0
    rollback_calls: int = 0
    close_calls: int = 0
    added: list[object] | None = None

    def __post_init__(self) -> None:
        if self.added is None:
            self.added = []

    def add(self, obj: object) -> None:
        self.added.append(obj)

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.close_calls += 1


def _standard_args() -> dict[str, str]:
    return {
        "organization_name": "Acme Corp",
        "organization_slug": "acme",
        "admin_email": "  Admin@Example.COM  ",
        "admin_password": "correct-horse-battery-staple",
        "first_name": "Ada",
        "last_name": "Lovelace",
    }


def test_creates_organization_when_missing(monkeypatch) -> None:
    session = FakeSession()
    role = Role(id=uuid4(), name=ADMIN_ROLE_NAME, is_system_role=True)

    monkeypatch.setattr("scripts.create_admin_user._get_organization_by_slug", lambda _session, _slug: None)
    monkeypatch.setattr("scripts.create_admin_user._get_role_by_name", lambda _session, _name: role)
    monkeypatch.setattr("scripts.create_admin_user._get_user_by_normalized_email", lambda *_args, **_kwargs: None)

    create_admin_user(session, **_standard_args())

    organizations = [obj for obj in session.added if isinstance(obj, Organization)]
    assert len(organizations) == 1
    assert organizations[0].slug == "acme"


def test_reuses_existing_organization(monkeypatch) -> None:
    session = FakeSession()
    organization = Organization(id=uuid4(), name="Acme", slug="acme", status="active")
    role = Role(id=uuid4(), name=ADMIN_ROLE_NAME, is_system_role=True)

    monkeypatch.setattr("scripts.create_admin_user._get_organization_by_slug", lambda _session, _slug: organization)
    monkeypatch.setattr("scripts.create_admin_user._get_role_by_name", lambda _session, _name: role)
    monkeypatch.setattr("scripts.create_admin_user._get_user_by_normalized_email", lambda *_args, **_kwargs: None)

    create_admin_user(session, **_standard_args())

    organizations = [obj for obj in session.added if isinstance(obj, Organization)]
    assert organizations == []


def test_creates_active_user(monkeypatch) -> None:
    session = FakeSession()
    organization = Organization(id=uuid4(), name="Acme", slug="acme", status="active")
    role = Role(id=uuid4(), name=ADMIN_ROLE_NAME, is_system_role=True)

    monkeypatch.setattr("scripts.create_admin_user._get_organization_by_slug", lambda _session, _slug: organization)
    monkeypatch.setattr("scripts.create_admin_user._get_role_by_name", lambda _session, _name: role)
    monkeypatch.setattr("scripts.create_admin_user._get_user_by_normalized_email", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("scripts.create_admin_user.hash_password", lambda _password: "hashed")

    create_admin_user(session, **_standard_args())

    users = [obj for obj in session.added if isinstance(obj, User)]
    assert len(users) == 1
    assert users[0].status == "active"
    assert users[0].display_name == "Ada Lovelace"


def test_hashes_password(monkeypatch) -> None:
    session = FakeSession()
    organization = Organization(id=uuid4(), name="Acme", slug="acme", status="active")
    role = Role(id=uuid4(), name=ADMIN_ROLE_NAME, is_system_role=True)
    called = {}

    def _fake_hash(password: str) -> str:
        called["password"] = password
        return "hashed-password"

    monkeypatch.setattr("scripts.create_admin_user._get_organization_by_slug", lambda _session, _slug: organization)
    monkeypatch.setattr("scripts.create_admin_user._get_role_by_name", lambda _session, _name: role)
    monkeypatch.setattr("scripts.create_admin_user._get_user_by_normalized_email", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("scripts.create_admin_user.hash_password", _fake_hash)

    create_admin_user(session, **_standard_args())

    users = [obj for obj in session.added if isinstance(obj, User)]
    assert called["password"] == "correct-horse-battery-staple"
    assert users[0].password_hash == "hashed-password"


def test_normalizes_email(monkeypatch) -> None:
    session = FakeSession()
    organization = Organization(id=uuid4(), name="Acme", slug="acme", status="active")
    role = Role(id=uuid4(), name=ADMIN_ROLE_NAME, is_system_role=True)
    captured = {}

    def _fake_lookup(_session, organization_id, normalized_email):
        captured["org"] = organization_id
        captured["email"] = normalized_email
        return None

    monkeypatch.setattr("scripts.create_admin_user._get_organization_by_slug", lambda _session, _slug: organization)
    monkeypatch.setattr("scripts.create_admin_user._get_role_by_name", lambda _session, _name: role)
    monkeypatch.setattr("scripts.create_admin_user._get_user_by_normalized_email", _fake_lookup)
    monkeypatch.setattr("scripts.create_admin_user.hash_password", lambda _password: "hashed")

    create_admin_user(session, **_standard_args())

    users = [obj for obj in session.added if isinstance(obj, User)]
    assert captured["email"] == "admin@example.com"
    assert users[0].email == "admin@example.com"
    assert users[0].normalized_email == "admin@example.com"


def test_assigns_organization_admin_role(monkeypatch) -> None:
    session = FakeSession()
    organization = Organization(id=uuid4(), name="Acme", slug="acme", status="active")
    role = Role(id=uuid4(), name=ADMIN_ROLE_NAME, is_system_role=True)

    monkeypatch.setattr("scripts.create_admin_user._get_organization_by_slug", lambda _session, _slug: organization)
    monkeypatch.setattr("scripts.create_admin_user._get_role_by_name", lambda _session, _name: role)
    monkeypatch.setattr("scripts.create_admin_user._get_user_by_normalized_email", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("scripts.create_admin_user.hash_password", lambda _password: "hashed")

    create_admin_user(session, **_standard_args())

    user_roles = [obj for obj in session.added if isinstance(obj, UserRole)]
    assert len(user_roles) == 1
    assert user_roles[0].role_id == role.id
    assert user_roles[0].organization_id == organization.id


def test_refuses_duplicate_user(monkeypatch) -> None:
    session = FakeSession()
    organization = Organization(id=uuid4(), name="Acme", slug="acme", status="active")
    role = Role(id=uuid4(), name=ADMIN_ROLE_NAME, is_system_role=True)
    existing_user = SimpleNamespace(id=uuid4())

    monkeypatch.setattr("scripts.create_admin_user._get_organization_by_slug", lambda _session, _slug: organization)
    monkeypatch.setattr("scripts.create_admin_user._get_role_by_name", lambda _session, _name: role)
    monkeypatch.setattr("scripts.create_admin_user._get_user_by_normalized_email", lambda *_args, **_kwargs: existing_user)

    try:
        create_admin_user(session, **_standard_args())
        assert False, "Expected AdminBootstrapError"
    except AdminBootstrapError as exc:
        assert "already exists" in str(exc)


def test_fails_safely_when_admin_role_missing(monkeypatch) -> None:
    session = FakeSession()
    organization = Organization(id=uuid4(), name="Acme", slug="acme", status="active")

    monkeypatch.setattr("scripts.create_admin_user._get_organization_by_slug", lambda _session, _slug: organization)
    monkeypatch.setattr("scripts.create_admin_user._get_role_by_name", lambda _session, _name: None)

    try:
        create_admin_user(session, **_standard_args())
        assert False, "Expected AdminBootstrapError"
    except AdminBootstrapError as exc:
        assert str(exc) == "Required organization administrator role is missing."


def test_does_not_expose_password_or_hash_in_output(monkeypatch, capsys) -> None:
    session = FakeSession()

    @contextmanager
    def _failing_scope():
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(
        "scripts.create_admin_user.create_admin_user",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AdminBootstrapError("Could not create admin user.")),
    )

    exit_code = run_bootstrap(session_context_factory=_failing_scope, **_standard_args())
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "correct-horse-battery-staple" not in output
    assert "hashed" not in output


def test_transaction_rolls_back_on_failure(monkeypatch) -> None:
    session = FakeSession()

    @contextmanager
    def _scope_with_transaction_behavior():
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    monkeypatch.setattr(
        "scripts.create_admin_user.create_admin_user",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("db failed")),
    )

    exit_code = run_bootstrap(session_context_factory=_scope_with_transaction_behavior, **_standard_args())

    assert exit_code == 1
    assert session.rollback_calls == 1
    assert session.commit_calls == 0
    assert session.close_calls == 1
