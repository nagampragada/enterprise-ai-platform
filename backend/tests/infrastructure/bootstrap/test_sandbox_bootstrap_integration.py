from __future__ import annotations

import os
from uuid import uuid4

from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from infrastructure.bootstrap.sandbox import (
    SandboxBootstrapInput,
    _create_state,
    bootstrap_sandbox,
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


EXPECTED_REVISION = "20260911_000025"
VALIDATED_DATABASE_SETTING = (
    "postgresql+psycopg://bootstrap:Safe-Test-Value-123!"
    "@database.internal:5432/platform"
)


def _database_identity(database_url: str) -> tuple[object, ...]:
    value = make_url(database_url)
    return value.drivername, value.host, value.port, value.database, value.query


def _test_database_url() -> str:
    value = os.environ["TEST_DATABASE_URL"]
    parsed = make_url(value)
    development = os.environ.get("DATABASE_URL")
    if development and _database_identity(development) == _database_identity(value):
        raise RuntimeError("test database must differ from development database")
    if not parsed.drivername.startswith("postgresql") or not parsed.database:
        raise RuntimeError("test database must be PostgreSQL and named")
    if "test" not in parsed.database.casefold():
        raise RuntimeError("test database name must be test-designated")
    return value


def _count(session: Session, model, criterion) -> int:
    return session.scalar(
        select(func.count()).select_from(model).where(criterion)
    )


def _environment(database_url: str, values: SandboxBootstrapInput) -> dict[str, str]:
    return {
        "APP_ENVIRONMENT": "sandbox",
        "DATABASE_URL": database_url,
        "ALLOW_SANDBOX_BOOTSTRAP": "true",
        "SANDBOX_BOOTSTRAP_ORGANIZATION_NAME": values.organization_name,
        "SANDBOX_BOOTSTRAP_ORGANIZATION_SLUG": values.organization_slug,
        "SANDBOX_BOOTSTRAP_ADMIN_EMAIL": values.admin_email,
        "SANDBOX_BOOTSTRAP_ADMIN_PASSWORD": values.admin_password,
        "SANDBOX_BOOTSTRAP_ADMIN_FIRST_NAME": values.admin_first_name,
        "SANDBOX_BOOTSTRAP_ADMIN_LAST_NAME": values.admin_last_name,
        "SANDBOX_BOOTSTRAP_KNOWLEDGE_SPACE_NAME": values.knowledge_space_name,
        "SANDBOX_BOOTSTRAP_KNOWLEDGE_SPACE_SLUG": values.knowledge_space_slug,
    }


def _input(token: str) -> SandboxBootstrapInput:
    return SandboxBootstrapInput(
        organization_name=f"Bootstrap Integration {token}",
        organization_slug=f"bootstrap-integration-{token}",
        admin_email=f"bootstrap-{token}@example.com",
        admin_password="Strong-Integration-Password-123!",
        admin_first_name="Bootstrap",
        admin_last_name="Integration",
        knowledge_space_name=f"Bootstrap Space {token}",
        knowledge_space_slug=f"bootstrap-space-{token}",
    )


def test_postgresql_fk_ordering_replay_and_outer_rollback() -> None:
    engine = create_engine(_test_database_url(), future=True)
    values = _input(uuid4().hex)

    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection, expire_on_commit=False)
    try:
        revision = session.execute(
            text("SELECT version_num FROM alembic_version LIMIT 1")
        ).scalar_one()
        assert revision == EXPECTED_REVISION
        role = session.execute(
            select(Role).where(
                Role.name == "organization_admin",
                Role.is_system_role.is_(True),
            )
        ).scalar_one()

        created = _create_state(session, values, role)

        assert created.outcome == "created"
        assert _count(
            session, Organization, Organization.slug == values.organization_slug
        ) == 1
        assert _count(session, User, User.normalized_email == values.admin_email) == 1
        assert _count(
            session,
            KnowledgeSpace,
            KnowledgeSpace.slug == values.knowledge_space_slug,
        ) == 1
        assert _count(
            session,
            UserRole,
            UserRole.organization_id == created.organization_id,
        ) == 1
        assert _count(
            session,
            KnowledgeSpaceUserGrant,
            KnowledgeSpaceUserGrant.organization_id == created.organization_id,
        ) == 1

        replay = bootstrap_sandbox(session, values)
        assert replay.outcome == "verified"
        assert replay == created.__class__(
            "verified", created.organization_id, created.user_id, created.knowledge_space_id
        )
    finally:
        if transaction.is_active:
            transaction.rollback()
        session.close()
        connection.close()

    with engine.connect() as verification:
        assert verification.scalar(
            select(func.count()).select_from(Organization).where(
                Organization.slug == values.organization_slug
            )
        ) == 0
        assert verification.scalar(
            select(func.count()).select_from(User).where(
                User.normalized_email == values.admin_email
            )
        ) == 0
        assert verification.scalar(
            select(func.count()).select_from(KnowledgeSpace).where(
                KnowledgeSpace.slug == values.knowledge_space_slug
            )
        ) == 0
    engine.dispose()


def test_postgresql_late_failure_rolls_back_every_insert(capsys) -> None:
    class FinalFlushFailureSession(Session):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.flush_count = 0
            self.rolled_back = False

        def flush(self, objects=None):
            super().flush(objects)
            self.flush_count += 1
            if self.flush_count == 3:
                raise RuntimeError("simulated failure after dependent rows")

        def rollback(self):
            self.rolled_back = True
            super().rollback()

    engine = create_engine(_test_database_url(), future=True)
    values = _input(uuid4().hex)
    session = FinalFlushFailureSession(bind=engine, expire_on_commit=False)

    assert run(
        [],
        environ=_environment(VALIDATED_DATABASE_SETTING, values),
        session_factory=lambda: session,
    ) == 1

    assert session.flush_count == 3
    assert session.rolled_back
    assert capsys.readouterr().out.strip() == "Sandbox bootstrap failed"
    with engine.connect() as verification:
        assert verification.scalar(
            select(func.count()).select_from(Organization).where(
                Organization.slug == values.organization_slug
            )
        ) == 0
        assert verification.scalar(
            select(func.count()).select_from(User).where(
                User.normalized_email == values.admin_email
            )
        ) == 0
        assert verification.scalar(
            select(func.count()).select_from(KnowledgeSpace).where(
                KnowledgeSpace.slug == values.knowledge_space_slug
            )
        ) == 0
    engine.dispose()
