from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from infrastructure.db.models import Document, Organization
from infrastructure.repositories.document_repository import DocumentRepository, MAX_DOCUMENT_LIST_LIMIT


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
    with migrated_engine.begin() as conn:
        conn.execute(text("SET search_path TO public"))
        conn.execute(text("DELETE FROM documents"))
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
    session.add(Organization(id=organization_id, name=name, slug=slug, status="active"))
    session.flush()
    return organization_id


def _build_document(
    *,
    organization_id: UUID,
    source_key: str,
    title: str = "Policy",
    status: str = "pending",
    created_at: datetime | None = None,
) -> Document:
    return Document(
        id=uuid.uuid4(),
        organization_id=organization_id,
        source_type="local_folder",
        source_document_key=source_key,
        title=title,
        source_url="file:///documents/policy.txt",
        mime_type="text/plain",
        checksum_latest="checksum-before",
        status=status,
        source_created_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
        source_updated_at=datetime(2026, 8, 2, tzinfo=timezone.utc),
        created_at=created_at,
    )


def test_create_and_retrieve_document_by_tenant_and_id(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Acme", slug="acme")
    document = _build_document(organization_id=organization_id, source_key="policy.txt")
    repository = DocumentRepository(db_session)

    repository.add(organization_id, document)
    db_session.commit()

    found = repository.get_by_id(organization_id, document.id)
    assert found is not None
    assert found.id == document.id


def test_wrong_organization_cannot_retrieve_document_by_id(db_session: Session) -> None:
    organization_a = _create_organization(db_session, name="Alpha", slug="alpha")
    organization_b = _create_organization(db_session, name="Beta", slug="beta")
    document = _build_document(organization_id=organization_a, source_key="shared.txt")
    db_session.add(document)
    db_session.commit()

    assert DocumentRepository(db_session).get_by_id(organization_b, document.id) is None


def test_source_identity_is_tenant_scoped(db_session: Session) -> None:
    organization_a = _create_organization(db_session, name="Gamma", slug="gamma")
    organization_b = _create_organization(db_session, name="Delta", slug="delta")
    document_a = _build_document(organization_id=organization_a, source_key="shared.txt")
    document_b = _build_document(organization_id=organization_b, source_key="shared.txt")
    db_session.add_all([document_a, document_b])
    db_session.commit()
    repository = DocumentRepository(db_session)

    assert repository.get_by_source_identity(organization_a, "local_folder", "shared.txt").id == document_a.id
    assert repository.get_by_source_identity(organization_b, "local_folder", "shared.txt").id == document_b.id


def test_list_is_tenant_scoped_excludes_deleted_and_has_deterministic_order(db_session: Session) -> None:
    organization_a = _create_organization(db_session, name="Epsilon", slug="epsilon")
    organization_b = _create_organization(db_session, name="Zeta", slug="zeta")
    created_at = datetime(2026, 8, 10, tzinfo=timezone.utc)
    first = _build_document(organization_id=organization_a, source_key="first.txt", created_at=created_at)
    second = _build_document(organization_id=organization_a, source_key="second.txt", created_at=created_at)
    deleted = _build_document(organization_id=organization_a, source_key="deleted.txt", created_at=created_at)
    other = _build_document(organization_id=organization_b, source_key="other.txt", created_at=created_at)
    db_session.add_all([first, second, deleted, other])
    db_session.flush()
    deleted.deleted_at = datetime.now(timezone.utc)
    db_session.commit()

    documents = DocumentRepository(db_session).list_for_organization(organization_a)
    assert [document.id for document in documents] == sorted([first.id, second.id])
    assert deleted.id not in {document.id for document in documents}
    assert other.id not in {document.id for document in documents}


def test_list_supports_status_filter_and_enforces_maximum_limit(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Eta", slug="eta")
    repository = DocumentRepository(db_session)
    for index in range(MAX_DOCUMENT_LIST_LIMIT + 5):
        repository.add(
            organization_id,
            _build_document(organization_id=organization_id, source_key=f"{index}.txt", status="ready"),
        )
    db_session.commit()

    assert len(repository.list_for_organization(organization_id, limit=MAX_DOCUMENT_LIST_LIMIT + 50)) == MAX_DOCUMENT_LIST_LIMIT
    assert len(repository.list_for_organization(organization_id, status="processing")) == 0
    with pytest.raises(ValueError):
        repository.list_for_organization(organization_id, limit=0)


def test_controlled_update_changes_sync_fields_without_internal_commit(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Theta", slug="theta")
    document = _build_document(organization_id=organization_id, source_key="sync.txt")
    db_session.add(document)
    db_session.commit()
    repository = DocumentRepository(db_session)
    source_updated_at = datetime(2026, 8, 14, tzinfo=timezone.utc)

    updated = repository.update(
        organization_id,
        document.id,
        title="Updated title",
        source_url="file:///documents/sync.txt",
        mime_type="text/markdown",
        checksum_latest="checksum-after",
        status="ready",
        source_updated_at=source_updated_at,
    )

    assert updated is not None
    assert updated.title == "Updated title"
    assert updated.mime_type == "text/markdown"
    assert updated.checksum_latest == "checksum-after"
    assert updated.status == "ready"
    assert updated.source_updated_at == source_updated_at
    db_session.rollback()
    assert repository.get_by_id(organization_id, document.id).checksum_latest == "checksum-before"


def test_wrong_organization_cannot_update_or_soft_delete_document(db_session: Session) -> None:
    organization_a = _create_organization(db_session, name="Iota", slug="iota")
    organization_b = _create_organization(db_session, name="Kappa", slug="kappa")
    document = _build_document(organization_id=organization_a, source_key="protected.txt")
    db_session.add(document)
    db_session.commit()
    repository = DocumentRepository(db_session)

    assert repository.update(organization_b, document.id, checksum_latest="tampered") is None
    assert repository.soft_delete(organization_b, document.id, datetime.now(timezone.utc)) is None
    db_session.rollback()
    assert repository.get_by_id(organization_a, document.id).deleted_at is None
    assert repository.get_by_id(organization_a, document.id).checksum_latest == "checksum-before"


def test_soft_delete_excludes_then_restore_reactivates_document(db_session: Session) -> None:
    organization_id = _create_organization(db_session, name="Lambda", slug="lambda")
    document = _build_document(organization_id=organization_id, source_key="restore.txt")
    db_session.add(document)
    db_session.commit()
    repository = DocumentRepository(db_session)

    deleted = repository.soft_delete(organization_id, document.id, datetime.now(timezone.utc))
    assert deleted is not None
    db_session.commit()
    assert repository.list_for_organization(organization_id) == []
    assert repository.get_by_source_identity(organization_id, "local_folder", "restore.txt") is not None

    restored = repository.restore(organization_id, document.id)
    assert restored is not None
    assert restored.deleted_at is None
    db_session.commit()
    assert [item.id for item in repository.list_for_organization(organization_id)] == [document.id]