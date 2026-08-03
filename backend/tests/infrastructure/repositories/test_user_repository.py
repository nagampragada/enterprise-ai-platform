from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from infrastructure.db.models import Organization, User
from infrastructure.repositories.user_repository import UserRepository


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PROJECT_VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
TEST_DATABASE_URL_ENV_VAR = "TEST_DATABASE_URL"
DATABASE_URL_ENV_VAR = "DATABASE_URL"
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
DEFAULT_TEST_DATABASE_URL = (
    "postgresql+psycopg://enterprise_ai_platform:enterprise_ai_platform@127.0.0.1:15432/"
    "enterprise_ai_platform_test"
)


def _database_identity(database_url: str) -> tuple[str, str | None, int | None, str | None]:
    url = make_url(database_url)
    return url.drivername, url.host, url.port, url.database


def _test_database_url() -> str:
    return os.getenv(TEST_DATABASE_URL_ENV_VAR, DEFAULT_TEST_DATABASE_URL)


def _run_alembic_upgrade(database_url: str) -> None:
    environment = os.environ.copy()
    environment[DATABASE_URL_ENV_VAR] = database_url
    subprocess.run(
        [str(PROJECT_VENV_PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", "head"],
        check=True,
        cwd=str(PROJECT_ROOT),
        env=environment,
    )


@pytest.fixture(scope="module")
def migrated_engine():
    test_database_url = _test_database_url()
    development_database_url = os.environ.get(DATABASE_URL_ENV_VAR)
    if development_database_url and _database_identity(development_database_url) == _database_identity(test_database_url):
        raise RuntimeError("TEST_DATABASE_URL must point to a different database than DATABASE_URL")

    reset_engine = create_engine(test_database_url, future=True)
    with reset_engine.begin() as conn:
        current_user = conn.execute(text("SELECT current_user")).scalar_one()
        if current_user and current_user != "public":
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{current_user}" CASCADE'))
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    reset_engine.dispose()

    _run_alembic_upgrade(test_database_url)

    engine = create_engine(test_database_url, future=True)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(autouse=True)
def _clean_test_data(migrated_engine) -> None:
    # Keep seeded roles untouched while clearing test-created rows.
    with migrated_engine.begin() as conn:
        conn.execute(text("SET search_path TO public"))
        conn.execute(text("DELETE FROM authentication_sessions"))
        conn.execute(text("DELETE FROM user_roles"))
        conn.execute(text("DELETE FROM users"))
        conn.execute(text("DELETE FROM organization_settings"))
        conn.execute(text("DELETE FROM organizations"))
        conn.execute(text("DELETE FROM industries"))


@pytest.fixture()
def session_factory(migrated_engine):
    return sessionmaker(bind=migrated_engine, autoflush=False, expire_on_commit=False, class_=Session)


@pytest.fixture()
def db_session(session_factory):
    session = session_factory()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


def _create_organization(session: Session, *, name: str, slug: str) -> UUID:
    organization_id = uuid.uuid4()
    session.add(
        Organization(
            id=organization_id,
            industry_id=None,
            name=name,
            slug=slug,
            status="active",
        )
    )
    return organization_id


def _build_user(*, organization_id: UUID, email: str, normalized_email: str, display_name: str) -> User:
    return User(
        id=uuid.uuid4(),
        organization_id=organization_id,
        email=email,
        normalized_email=normalized_email,
        password_hash="argon2id$demo-hash",
        display_name=display_name,
        status="active",
    )


def test_add_persists_user_after_outer_commit(db_session: Session, session_factory) -> None:
    organization_id = _create_organization(db_session, name="Acme Corp", slug="acme-corp")
    user = _build_user(
        organization_id=organization_id,
        email="alice@acme.example",
        normalized_email="alice@acme.example",
        display_name="Alice",
    )

    repository = UserRepository(db_session)
    repository.add(user)
    db_session.commit()

    with session_factory() as verify_session:
        stored_user = verify_session.execute(
            select(User).where(
                User.organization_id == organization_id,
                User.id == user.id,
            )
        ).scalar_one_or_none()

    assert stored_user is not None


def test_add_does_not_commit_internally(db_session: Session, session_factory) -> None:
    organization_id = _create_organization(db_session, name="Beta LLC", slug="beta-llc")
    user = _build_user(
        organization_id=organization_id,
        email="bob@beta.example",
        normalized_email="bob@beta.example",
        display_name="Bob",
    )

    repository = UserRepository(db_session)
    repository.add(user)

    with session_factory() as verify_session:
        stored_user = verify_session.execute(
            select(User).where(
                User.organization_id == organization_id,
                User.id == user.id,
            )
        ).scalar_one_or_none()

    assert stored_user is None


def test_get_by_id_returns_user_for_correct_organization(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Gamma Inc", slug="gamma-inc")
    user = _build_user(
        organization_id=organization_id,
        email="carol@gamma.example",
        normalized_email="carol@gamma.example",
        display_name="Carol",
    )
    db_session.add(user)
    db_session.commit()

    repository = UserRepository(db_session)
    found_user = repository.get_by_id(organization_id=organization_id, user_id=user.id)

    assert found_user is not None
    assert found_user.id == user.id


def test_get_by_id_returns_none_for_wrong_organization(db_session: Session) -> None:
    org_a = _create_organization(db_session, name="Delta Org", slug="delta-org")
    org_b = _create_organization(db_session, name="Epsilon Org", slug="epsilon-org")
    user = _build_user(
        organization_id=org_a,
        email="dana@delta.example",
        normalized_email="dana@delta.example",
        display_name="Dana",
    )
    db_session.add(user)
    db_session.commit()

    repository = UserRepository(db_session)
    found_user = repository.get_by_id(organization_id=org_b, user_id=user.id)

    assert found_user is None


def test_get_by_normalized_email_returns_correct_user(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Zeta Org", slug="zeta-org")
    user = _build_user(
        organization_id=organization_id,
        email="ellen@zeta.example",
        normalized_email="ellen@zeta.example",
        display_name="Ellen",
    )
    db_session.add(user)
    db_session.commit()

    repository = UserRepository(db_session)
    found_user = repository.get_by_normalized_email(
        organization_id=organization_id,
        normalized_email="ellen@zeta.example",
    )

    assert found_user is not None
    assert found_user.id == user.id


def test_get_by_normalized_email_is_tenant_scoped(db_session: Session) -> None:
    org_a = _create_organization(db_session, name="Eta Org", slug="eta-org")
    org_b = _create_organization(db_session, name="Theta Org", slug="theta-org")
    user_a = _build_user(
        organization_id=org_a,
        email="shared@corp.example",
        normalized_email="shared@corp.example",
        display_name="Frank",
    )
    user_b = _build_user(
        organization_id=org_b,
        email="shared@corp.example",
        normalized_email="shared@corp.example",
        display_name="Fran",
    )
    db_session.add(user_a)
    db_session.add(user_b)
    db_session.commit()

    repository = UserRepository(db_session)
    found_user_a = repository.get_by_normalized_email(
        organization_id=org_a,
        normalized_email="shared@corp.example",
    )
    found_user_b = repository.get_by_normalized_email(
        organization_id=org_b,
        normalized_email="shared@corp.example",
    )

    assert found_user_a is not None
    assert found_user_b is not None
    assert found_user_a.id == user_a.id
    assert found_user_b.id == user_b.id


def test_update_last_login_updates_correct_tenant_user(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Iota Org", slug="iota-org")
    user = _build_user(
        organization_id=organization_id,
        email="gina@iota.example",
        normalized_email="gina@iota.example",
        display_name="Gina",
    )
    db_session.add(user)
    db_session.commit()

    login_time = datetime.now(timezone.utc).replace(microsecond=0)
    repository = UserRepository(db_session)
    updated_user = repository.update_last_login(
        organization_id=organization_id,
        user_id=user.id,
        last_login_at=login_time,
    )
    db_session.commit()

    assert updated_user is not None

    db_session.refresh(user)
    assert user.last_login_at == login_time


def test_update_last_login_returns_none_for_wrong_organization(db_session: Session) -> None:
    org_a = _create_organization(db_session, name="Kappa Org", slug="kappa-org")
    org_b = _create_organization(db_session, name="Lambda Org", slug="lambda-org")
    user = _build_user(
        organization_id=org_a,
        email="henry@kappa.example",
        normalized_email="henry@kappa.example",
        display_name="Henry",
    )
    db_session.add(user)
    db_session.commit()

    original_last_login = user.last_login_at
    repository = UserRepository(db_session)
    updated_user = repository.update_last_login(
        organization_id=org_b,
        user_id=user.id,
        last_login_at=datetime.now(timezone.utc),
    )

    assert updated_user is None

    db_session.refresh(user)
    assert user.last_login_at == original_last_login


def test_update_status_updates_correct_tenant_user(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Mu Org", slug="mu-org")
    user = _build_user(
        organization_id=organization_id,
        email="ivy@mu.example",
        normalized_email="ivy@mu.example",
        display_name="Ivy",
    )
    db_session.add(user)
    db_session.commit()

    repository = UserRepository(db_session)
    updated_user = repository.update_status(
        organization_id=organization_id,
        user_id=user.id,
        status="suspended",
    )
    db_session.commit()

    assert updated_user is not None

    db_session.refresh(user)
    assert user.status == "suspended"


def test_update_status_returns_none_for_wrong_organization(db_session: Session) -> None:
    org_a = _create_organization(db_session, name="Nu Org", slug="nu-org")
    org_b = _create_organization(db_session, name="Xi Org", slug="xi-org")
    user = _build_user(
        organization_id=org_a,
        email="john@nu.example",
        normalized_email="john@nu.example",
        display_name="John",
    )
    db_session.add(user)
    db_session.commit()

    original_status = user.status
    repository = UserRepository(db_session)
    updated_user = repository.update_status(
        organization_id=org_b,
        user_id=user.id,
        status="disabled",
    )

    assert updated_user is None

    db_session.refresh(user)
    assert user.status == original_status
