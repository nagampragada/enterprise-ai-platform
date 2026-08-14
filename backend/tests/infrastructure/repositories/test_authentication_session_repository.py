from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from infrastructure.db.models import AuthenticationSession, Organization, User
from infrastructure.repositories.authentication_session_repository import AuthenticationSessionRepository


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


def _create_user(
    session: Session,
    *,
    organization_id: UUID,
    email: str,
    display_name: str,
) -> User:
    normalized_email = email.strip().lower()
    user = User(
        id=uuid.uuid4(),
        organization_id=organization_id,
        email=email,
        normalized_email=normalized_email,
        password_hash="argon2id$demo-hash",
        display_name=display_name,
        status="active",
    )
    session.add(user)
    return user


def _build_authentication_session(
    *,
    organization_id: UUID,
    user_id: UUID,
    refresh_token_hash: str,
    expires_in_hours: int = 4,
) -> AuthenticationSession:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    return AuthenticationSession(
        id=uuid.uuid4(),
        organization_id=organization_id,
        user_id=user_id,
        refresh_token_hash=refresh_token_hash.encode("utf-8"),
        created_at=now,
        expires_at=now + timedelta(hours=expires_in_hours),
        revoked_at=None,
        last_used_at=None,
        ip_address="127.0.0.1",
        user_agent="pytest",
    )


def test_add_persists_after_outer_transaction_commit(db_session: Session, session_factory) -> None:
    organization_id = _create_organization(db_session, name="Acme Org", slug="acme-org")
    user = _create_user(
        db_session,
        organization_id=organization_id,
        email="alice@acme.example",
        display_name="Alice",
    )
    db_session.commit()

    auth_session = _build_authentication_session(
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash="hash-add-persist",
    )

    repository = AuthenticationSessionRepository(db_session)
    repository.add(auth_session)
    db_session.commit()

    with session_factory() as verify_session:
        stored_session = verify_session.execute(
            select(AuthenticationSession).where(
                AuthenticationSession.organization_id == organization_id,
                AuthenticationSession.id == auth_session.id,
            )
        ).scalar_one_or_none()

    assert stored_session is not None


def test_add_does_not_commit_internally(db_session: Session, session_factory) -> None:
    organization_id = _create_organization(db_session, name="Beta Org", slug="beta-org")
    user = _create_user(
        db_session,
        organization_id=organization_id,
        email="bob@beta.example",
        display_name="Bob",
    )
    db_session.commit()

    auth_session = _build_authentication_session(
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash="hash-add-no-commit",
    )

    repository = AuthenticationSessionRepository(db_session)
    repository.add(auth_session)

    with session_factory() as verify_session:
        stored_session = verify_session.execute(
            select(AuthenticationSession).where(AuthenticationSession.id == auth_session.id)
        ).scalar_one_or_none()

    assert stored_session is None


def test_get_by_id_returns_session_for_correct_organization(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Gamma Org", slug="gamma-org")
    user = _create_user(
        db_session,
        organization_id=organization_id,
        email="carol@gamma.example",
        display_name="Carol",
    )
    auth_session = _build_authentication_session(
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash="hash-get-by-id",
    )
    db_session.add(auth_session)
    db_session.commit()

    repository = AuthenticationSessionRepository(db_session)
    found = repository.get_by_id(organization_id=organization_id, session_id=auth_session.id)

    assert found is not None
    assert found.id == auth_session.id


def test_get_by_id_returns_none_for_wrong_organization(db_session: Session) -> None:
    org_a = _create_organization(db_session, name="Delta Org", slug="delta-org")
    org_b = _create_organization(db_session, name="Epsilon Org", slug="epsilon-org")
    user = _create_user(
        db_session,
        organization_id=org_a,
        email="dana@delta.example",
        display_name="Dana",
    )
    auth_session = _build_authentication_session(
        organization_id=org_a,
        user_id=user.id,
        refresh_token_hash="hash-get-by-id-wrong-org",
    )
    db_session.add(auth_session)
    db_session.commit()

    repository = AuthenticationSessionRepository(db_session)
    found = repository.get_by_id(organization_id=org_b, session_id=auth_session.id)

    assert found is None


def test_get_by_refresh_token_hash_returns_matching_session(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Zeta Org", slug="zeta-org")
    user = _create_user(
        db_session,
        organization_id=organization_id,
        email="ellen@zeta.example",
        display_name="Ellen",
    )
    token_hash = "hash-refresh-match"
    auth_session = _build_authentication_session(
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash=token_hash,
    )
    db_session.add(auth_session)
    db_session.commit()

    repository = AuthenticationSessionRepository(db_session)
    found = repository.get_by_refresh_token_hash(refresh_token_hash=token_hash)

    assert found is not None
    assert found.id == auth_session.id


def test_get_by_refresh_token_hash_returns_none_for_unknown_hash(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Eta Org", slug="eta-org")
    user = _create_user(
        db_session,
        organization_id=organization_id,
        email="frank@eta.example",
        display_name="Frank",
    )
    auth_session = _build_authentication_session(
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash="hash-refresh-known",
    )
    db_session.add(auth_session)
    db_session.commit()

    repository = AuthenticationSessionRepository(db_session)
    found = repository.get_by_refresh_token_hash(refresh_token_hash="hash-refresh-unknown")

    assert found is None


def test_revoke_sets_revoked_at_for_correct_organization(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Theta Org", slug="theta-org")
    user = _create_user(
        db_session,
        organization_id=organization_id,
        email="gina@theta.example",
        display_name="Gina",
    )
    auth_session = _build_authentication_session(
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash="hash-revoke-correct-org",
    )
    db_session.add(auth_session)
    db_session.commit()

    revoked_at = datetime.now(timezone.utc).replace(microsecond=0)
    repository = AuthenticationSessionRepository(db_session)
    revoked_session = repository.revoke(
        organization_id=organization_id,
        user_id=user.id,
        session_id=auth_session.id,
        revoked_at=revoked_at,
    )
    db_session.commit()

    assert revoked_session is not None

    db_session.refresh(auth_session)
    assert auth_session.revoked_at == revoked_at


def test_revoke_returns_none_for_wrong_organization(db_session: Session) -> None:
    org_a = _create_organization(db_session, name="Iota Org", slug="iota-org")
    org_b = _create_organization(db_session, name="Kappa Org", slug="kappa-org")
    user = _create_user(
        db_session,
        organization_id=org_a,
        email="harry@iota.example",
        display_name="Harry",
    )
    auth_session = _build_authentication_session(
        organization_id=org_a,
        user_id=user.id,
        refresh_token_hash="hash-revoke-wrong-org",
    )
    db_session.add(auth_session)
    db_session.commit()

    repository = AuthenticationSessionRepository(db_session)
    revoked = repository.revoke(
        organization_id=org_b,
        user_id=user.id,
        session_id=auth_session.id,
        revoked_at=datetime.now(timezone.utc),
    )

    assert revoked is None

    db_session.refresh(auth_session)
    assert auth_session.revoked_at is None


def test_revoke_returns_none_for_wrong_user_in_same_organization(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Lambda Two Org", slug="lambda-two-org")
    owner = _create_user(
        db_session,
        organization_id=organization_id,
        email="owner@lambda-two.example",
        display_name="Owner",
    )
    other_user = _create_user(
        db_session,
        organization_id=organization_id,
        email="other@lambda-two.example",
        display_name="Other",
    )
    auth_session = _build_authentication_session(
        organization_id=organization_id,
        user_id=owner.id,
        refresh_token_hash="hash-revoke-wrong-user",
    )
    db_session.add(auth_session)
    db_session.commit()

    repository = AuthenticationSessionRepository(db_session)
    revoked = repository.revoke(
        organization_id=organization_id,
        user_id=other_user.id,
        session_id=auth_session.id,
        revoked_at=datetime.now(timezone.utc),
    )

    assert revoked is None
    db_session.refresh(auth_session)
    assert auth_session.revoked_at is None


def test_revoke_all_for_user_revokes_only_target_users_sessions_in_organization(db_session: Session) -> None:
    org_a = _create_organization(db_session, name="Lambda Org", slug="lambda-org")
    org_b = _create_organization(db_session, name="Mu Org", slug="mu-org")

    target_user = _create_user(
        db_session,
        organization_id=org_a,
        email="target@lambda.example",
        display_name="Target",
    )
    other_user_same_org = _create_user(
        db_session,
        organization_id=org_a,
        email="other@lambda.example",
        display_name="Other",
    )
    target_user_other_org = _create_user(
        db_session,
        organization_id=org_b,
        email="target@mu.example",
        display_name="Target Other Org",
    )

    target_session_1 = _build_authentication_session(
        organization_id=org_a,
        user_id=target_user.id,
        refresh_token_hash="hash-bulk-target-1",
    )
    target_session_2 = _build_authentication_session(
        organization_id=org_a,
        user_id=target_user.id,
        refresh_token_hash="hash-bulk-target-2",
    )
    other_same_org_session = _build_authentication_session(
        organization_id=org_a,
        user_id=other_user_same_org.id,
        refresh_token_hash="hash-bulk-other-same-org",
    )
    other_org_session = _build_authentication_session(
        organization_id=org_b,
        user_id=target_user_other_org.id,
        refresh_token_hash="hash-bulk-other-org",
    )

    db_session.add_all([
        target_session_1,
        target_session_2,
        other_same_org_session,
        other_org_session,
    ])
    db_session.commit()

    revoked_at = datetime.now(timezone.utc).replace(microsecond=0)
    repository = AuthenticationSessionRepository(db_session)
    affected = repository.revoke_all_for_user(
        organization_id=org_a,
        user_id=target_user.id,
        revoked_at=revoked_at,
    )
    db_session.commit()

    assert affected == 2

    db_session.refresh(target_session_1)
    db_session.refresh(target_session_2)
    db_session.refresh(other_same_org_session)
    db_session.refresh(other_org_session)

    assert target_session_1.revoked_at == revoked_at
    assert target_session_2.revoked_at == revoked_at
    assert other_same_org_session.revoked_at is None
    assert other_org_session.revoked_at is None


def test_revoke_all_for_user_returns_affected_row_count(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Nu Org", slug="nu-org")
    user = _create_user(
        db_session,
        organization_id=organization_id,
        email="count@nu.example",
        display_name="Count",
    )

    first = _build_authentication_session(
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash="hash-count-1",
    )
    second = _build_authentication_session(
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash="hash-count-2",
    )
    db_session.add_all([first, second])
    db_session.commit()

    repository = AuthenticationSessionRepository(db_session)
    affected = repository.revoke_all_for_user(
        organization_id=organization_id,
        user_id=user.id,
        revoked_at=datetime.now(timezone.utc),
    )

    assert affected == 2


def test_update_last_used_updates_correct_tenant_session(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Xi Org", slug="xi-org")
    user = _create_user(
        db_session,
        organization_id=organization_id,
        email="ivy@xi.example",
        display_name="Ivy",
    )
    auth_session = _build_authentication_session(
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash="hash-last-used",
    )
    db_session.add(auth_session)
    db_session.commit()

    last_used_at = datetime.now(timezone.utc).replace(microsecond=0)
    repository = AuthenticationSessionRepository(db_session)
    updated = repository.update_last_used(
        organization_id=organization_id,
        session_id=auth_session.id,
        last_used_at=last_used_at,
    )
    db_session.commit()

    assert updated is not None

    db_session.refresh(auth_session)
    assert auth_session.last_used_at == last_used_at


def test_update_last_used_returns_none_for_wrong_organization(db_session: Session) -> None:
    org_a = _create_organization(db_session, name="Omicron Org", slug="omicron-org")
    org_b = _create_organization(db_session, name="Pi Org", slug="pi-org")
    user = _create_user(
        db_session,
        organization_id=org_a,
        email="jane@omicron.example",
        display_name="Jane",
    )
    auth_session = _build_authentication_session(
        organization_id=org_a,
        user_id=user.id,
        refresh_token_hash="hash-last-used-wrong-org",
    )
    db_session.add(auth_session)
    db_session.commit()

    repository = AuthenticationSessionRepository(db_session)
    updated = repository.update_last_used(
        organization_id=org_b,
        session_id=auth_session.id,
        last_used_at=datetime.now(timezone.utc),
    )

    assert updated is None

    db_session.refresh(auth_session)
    assert auth_session.last_used_at is None


def test_delete_expired_deletes_only_sessions_before_cutoff_and_returns_count(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Rho Org", slug="rho-org")
    user = _create_user(
        db_session,
        organization_id=organization_id,
        email="kate@rho.example",
        display_name="Kate",
    )

    now = datetime.now(timezone.utc).replace(microsecond=0)
    expired_one = AuthenticationSession(
        id=uuid.uuid4(),
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash=b"hash-expired-one",
        created_at=now - timedelta(days=5),
        expires_at=now - timedelta(days=2),
        revoked_at=None,
        last_used_at=None,
        ip_address="127.0.0.1",
        user_agent="pytest",
    )
    expired_two = AuthenticationSession(
        id=uuid.uuid4(),
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash=b"hash-expired-two",
        created_at=now - timedelta(days=4),
        expires_at=now - timedelta(hours=12),
        revoked_at=None,
        last_used_at=None,
        ip_address="127.0.0.1",
        user_agent="pytest",
    )
    not_expired = AuthenticationSession(
        id=uuid.uuid4(),
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash=b"hash-not-expired",
        created_at=now - timedelta(days=1),
        expires_at=now + timedelta(days=1),
        revoked_at=None,
        last_used_at=now,
        ip_address="127.0.0.1",
        user_agent="pytest",
    )
    db_session.add_all([expired_one, expired_two, not_expired])
    db_session.commit()

    repository = AuthenticationSessionRepository(db_session)
    affected = repository.delete_expired(expires_before=now)
    db_session.commit()

    assert affected == 2

    remaining_ids = {
        row[0]
        for row in db_session.execute(
            select(AuthenticationSession.id).where(AuthenticationSession.organization_id == organization_id)
        ).all()
    }
    assert expired_one.id not in remaining_ids
    assert expired_two.id not in remaining_ids
    assert not_expired.id in remaining_ids


def test_active_non_expired_sessions_remain_after_delete_expired(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Sigma Org", slug="sigma-org")
    user = _create_user(
        db_session,
        organization_id=organization_id,
        email="lucas@sigma.example",
        display_name="Lucas",
    )

    now = datetime.now(timezone.utc).replace(microsecond=0)
    active_session = AuthenticationSession(
        id=uuid.uuid4(),
        organization_id=organization_id,
        user_id=user.id,
        refresh_token_hash=b"hash-active-remains",
        created_at=now - timedelta(hours=3),
        expires_at=now + timedelta(hours=3),
        revoked_at=None,
        last_used_at=now - timedelta(minutes=10),
        ip_address="127.0.0.1",
        user_agent="pytest",
    )
    db_session.add(active_session)
    db_session.commit()

    repository = AuthenticationSessionRepository(db_session)
    affected = repository.delete_expired(expires_before=now)
    db_session.commit()

    assert affected == 0

    still_there = db_session.execute(
        select(AuthenticationSession).where(AuthenticationSession.id == active_session.id)
    ).scalar_one_or_none()
    assert still_there is not None
