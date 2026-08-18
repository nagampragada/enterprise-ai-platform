from __future__ import annotations

import os
from pathlib import Path
import subprocess
import threading
import uuid

import pytest
from fastapi import Depends
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from app.dependencies import (
    CurrentUser,
    get_connector_management_service,
    get_current_user,
    get_db_session,
)
from app.main import app
from application.services.connector_management_service import ConnectorManagementService
from infrastructure.db.models import Connector, ConnectorScope, ConnectorSyncJob

ROOT = Path(__file__).resolve().parents[5]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
ADMIN_ROLE_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
EMPLOYEE_ROLE_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")


def _identity(url: str):
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database


@pytest.fixture(scope="module")
def engine():
    url = os.environ["TEST_DATABASE_URL"]
    development = os.environ.get("DATABASE_URL")
    if development and _identity(development) == _identity(url):
        raise RuntimeError("test database must differ from development database")
    reset = create_engine(url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    reset.dispose()
    environment = os.environ.copy(); environment["DATABASE_URL"] = url
    subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(INI), "upgrade", "head"],
        check=True, cwd=str(ROOT), env=environment,
    )
    value = create_engine(url, future=True)
    try:
        yield value
    finally:
        value.dispose()


@pytest.fixture(autouse=True)
def clean(engine):
    with engine.begin() as connection:
        for table in (
            "source_acl_entries", "source_acl_snapshots", "external_group_memberships",
            "external_directory_states", "user_external_identity_links", "external_principals",
            "document_indexing_attempts", "document_indexing_states", "document_version_documents",
            "document_versions", "connector_sync_cursors", "connector_sync_errors",
            "connector_sync_items", "connector_sync_runs", "connector_sync_jobs",
            "source_item_scope_memberships", "source_items", "connector_scopes", "connectors",
            "audit_events", "knowledge_space_user_grants", "knowledge_space_team_grants",
            "knowledge_space_department_grants", "knowledge_space_organization_grants",
            "knowledge_spaces", "team_memberships", "department_memberships", "teams",
            "departments", "document_chunks", "documents", "authentication_sessions", "user_roles",
            "users", "organization_settings", "organizations", "industries",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


@pytest.fixture
def factory(engine):
    return sessionmaker(bind=engine, class_=Session, autoflush=False, expire_on_commit=False)


@pytest.fixture(autouse=True)
def clear_overrides():
    yield
    app.dependency_overrides.clear()


def _identity_rows(factory, name: str, *, admin: bool, active: bool = True):
    organization_id, user_id, space_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    session = factory()
    session.execute(
        text("INSERT INTO organizations (id,name,slug,status) VALUES (:id,:name,:slug,:status)"),
        {"id": organization_id, "name": name, "slug": f"{name.lower()}-{organization_id}", "status": "active"},
    )
    session.execute(
        text("""INSERT INTO users
              (id,organization_id,email,normalized_email,password_hash,display_name,status)
              VALUES (:id,:org,:email,:email,'hash',:name,:status)"""),
        {"id": user_id, "org": organization_id, "email": f"{user_id}@example.com", "name": name, "status": "active" if active else "disabled"},
    )
    session.execute(
        text("INSERT INTO user_roles (id,organization_id,user_id,role_id) VALUES (:id,:org,:user,:role)"),
        {"id": uuid.uuid4(), "org": organization_id, "user": user_id, "role": ADMIN_ROLE_ID if admin else EMPLOYEE_ROLE_ID},
    )
    session.execute(
        text("INSERT INTO knowledge_spaces (id,organization_id,name,slug,status) VALUES (:id,:org,:name,:slug,'active')"),
        {"id": space_id, "org": organization_id, "name": f"{name} space", "slug": f"space-{space_id}"},
    )
    session.commit(); session.close()
    return organization_id, user_id, space_id


def _configure_app(factory, organization_id, user_id):
    def session_override():
        session = factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db_session] = session_override
    app.dependency_overrides[get_current_user] = lambda: CurrentUser(
        user_id, organization_id, f"{user_id}@example.com", "Administrator"
    )


def test_real_role_policy_denies_employee_and_disabled_admin(factory):
    employee_org, employee_id, _ = _identity_rows(factory, "Employee", admin=False)
    _configure_app(factory, employee_org, employee_id)
    with TestClient(app) as client:
        assert client.get("/api/v1/connectors").status_code == 403


def test_business_team_owner_responsibility_does_not_grant_connector_admin(factory):
    organization_id, user_id, _ = _identity_rows(factory, "BusinessOwner", admin=False)
    session = factory(); team_id = uuid.uuid4()
    session.execute(
        text("INSERT INTO teams (id,organization_id,name,slug,status) VALUES (:id,:org,'Owners',:slug,'active')"),
        {"id": team_id, "org": organization_id, "slug": f"owners-{team_id}"},
    )
    session.execute(
        text("""INSERT INTO team_memberships
              (id,organization_id,team_id,user_id,responsibility,status,effective_from)
              VALUES (:id,:org,:team,:user,'owner','active',CURRENT_TIMESTAMP)"""),
        {"id": uuid.uuid4(), "org": organization_id, "team": team_id, "user": user_id},
    )
    session.commit(); session.close()
    _configure_app(factory, organization_id, user_id)
    with TestClient(app) as client:
        assert client.get("/api/v1/connectors").status_code == 403


def test_inactive_organization_admin_is_forbidden(factory):
    organization_id, user_id, _ = _identity_rows(factory, "InactiveOrg", admin=True)
    session = factory()
    session.execute(
        text("UPDATE organizations SET status='suspended' WHERE id=:id"),
        {"id": organization_id},
    )
    session.commit(); session.close()
    _configure_app(factory, organization_id, user_id)
    with TestClient(app) as client:
        assert client.get("/api/v1/connectors").status_code == 403
    app.dependency_overrides.clear()
    disabled_org, disabled_id, _ = _identity_rows(factory, "Disabled", admin=True, active=False)
    _configure_app(factory, disabled_org, disabled_id)
    with TestClient(app) as client:
        assert client.get("/api/v1/connectors").status_code == 403


def test_full_api_flow_persists_defaults_redacts_and_coalesces(factory, tmp_path):
    organization_id, user_id, space_id = _identity_rows(factory, "Admin", admin=True)
    _configure_app(factory, organization_id, user_id)
    root = str((tmp_path / "not-scanned-during-api").resolve())
    assert not Path(root).exists()
    with TestClient(app) as client:
        created = client.post(
            "/api/v1/connectors",
            json={"connector_type": "local_folder", "display_name": "Local", "slug": "local"},
        )
        connector_id = created.json()["connector_id"]
        listing = client.get("/api/v1/connectors?limit=1")
        detail = client.get(f"/api/v1/connectors/{connector_id}")
        scope = client.post(
            f"/api/v1/connectors/{connector_id}/scopes",
            json={
                "knowledge_space_id": str(space_id), "display_name": "Folder", "slug": "folder",
                "configuration": {"root_path": root, "follow_symlinks": False},
            },
        )
        scope_id = scope.json()["scope_id"]
        scopes = client.get(f"/api/v1/connectors/{connector_id}/scopes")
        first_job = client.post(f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs")
        second_job = client.post(f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs")
        jobs = client.get(f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs")
        job_detail = client.get(
            f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs/{first_job.json()['job_id']}"
        )
    assert created.status_code == 201 and listing.status_code == detail.status_code == 200
    assert scope.status_code == 201 and scopes.status_code == 200
    assert first_job.status_code == second_job.status_code == 202
    assert first_job.json()["job_id"] == second_job.json()["job_id"]
    assert first_job.json()["coalesced"] is False and second_job.json()["coalesced"] is True
    assert jobs.status_code == job_detail.status_code == 200
    for response in (created, listing, detail, scope, scopes, first_job, second_job, jobs, job_detail):
        body = response.text
        assert root not in body and "secret_reference" not in body and "safe_config" not in body
        assert "lease_id" not in body and "lease_owner" not in body
    session = factory()
    connector = session.get(Connector, uuid.UUID(connector_id))
    persisted_scope = session.get(ConnectorScope, uuid.UUID(scope_id))
    assert connector.organization_id == organization_id and connector.created_by_user_id == user_id
    assert connector.status == "active" and connector.acl_support == "none"
    assert persisted_scope.external_scope_key == root and persisted_scope.created_by_user_id == user_id
    assert session.scalar(select(func.count()).select_from(ConnectorSyncJob)) == 1
    session.close()


def test_cross_tenant_resources_are_concealed(factory):
    org_a, user_a, space_a = _identity_rows(factory, "TenantA", admin=True)
    org_b, user_b, _ = _identity_rows(factory, "TenantB", admin=True)
    session = factory()
    service = ConnectorManagementService(session)
    connector = service.create_local_folder_connector(org_a, user_a, display_name="A", slug="a")
    scope = service.create_local_folder_scope(
        org_a, user_a, connector.id, knowledge_space_id=space_a, display_name="A", slug="a",
        root_path="C:\\tenant-a", follow_symlinks=False,
    )
    session.commit(); session.close()
    _configure_app(factory, org_b, user_b)
    with TestClient(app) as client:
        assert client.get(f"/api/v1/connectors/{connector.id}").status_code == 404
        assert client.get(f"/api/v1/connectors/{connector.id}/scopes").status_code == 404
        assert client.post(
            f"/api/v1/connectors/{connector.id}/scopes/{scope.id}/sync-jobs"
        ).status_code == 404


def test_cross_tenant_knowledge_space_and_inactive_scope_rejected(factory):
    org_a, user_a, _ = _identity_rows(factory, "OwnerA", admin=True)
    _, _, foreign_space = _identity_rows(factory, "OwnerB", admin=True)
    _configure_app(factory, org_a, user_a)
    with TestClient(app) as client:
        connector = client.post(
            "/api/v1/connectors",
            json={"connector_type": "local_folder", "display_name": "A", "slug": "a"},
        )
        connector_id = connector.json()["connector_id"]
        response = client.post(
            f"/api/v1/connectors/{connector_id}/scopes",
            json={
                "knowledge_space_id": str(foreign_space), "display_name": "Foreign", "slug": "foreign",
                "configuration": {"root_path": "C:\\foreign"},
            },
        )
    assert response.status_code == 404


def test_inactive_connector_or_scope_cannot_enqueue(factory):
    organization_id, user_id, space_id = _identity_rows(factory, "Inactive", admin=True)
    _configure_app(factory, organization_id, user_id)
    with TestClient(app) as client:
        connector = client.post(
            "/api/v1/connectors",
            json={"connector_type": "local_folder", "display_name": "A", "slug": "a"},
        )
        connector_id = connector.json()["connector_id"]
        scope = client.post(
            f"/api/v1/connectors/{connector_id}/scopes",
            json={
                "knowledge_space_id": str(space_id), "display_name": "A", "slug": "a",
                "configuration": {"root_path": "C:\\inactive"},
            },
        )
        scope_id = scope.json()["scope_id"]
    session = factory(); session.get(ConnectorScope, uuid.UUID(scope_id)).status = "paused"
    session.commit(); session.close()
    with TestClient(app) as client:
        response = client.post(f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs")
    assert response.status_code == 409
    session = factory(); session.get(ConnectorScope, uuid.UUID(scope_id)).status = "active"
    session.get(Connector, uuid.UUID(connector_id)).status = "paused"
    session.commit(); session.close()
    with TestClient(app) as client:
        response = client.post(f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs")
    assert response.status_code == 409


def test_completed_and_failed_job_history_remains_readable(factory):
    organization_id, user_id, space_id = _identity_rows(factory, "History", admin=True)
    session = factory(); service = ConnectorManagementService(session)
    connector = service.create_local_folder_connector(
        organization_id, user_id, display_name="History", slug="history"
    )
    scope = service.create_local_folder_scope(
        organization_id, user_id, connector.id, knowledge_space_id=space_id,
        display_name="History", slug="history", root_path="C:\\history", follow_symlinks=False,
    )
    first, _ = service.enqueue_sync_job(organization_id, user_id, connector.id, scope.id)
    session.flush()
    first_row = session.get(ConnectorSyncJob, first.job_id)
    first_row.status = "cancelled"; first_row.next_attempt_at = None
    first_row.cancel_requested_at = first_row.created_at; first_row.completed_at = first_row.created_at
    session.commit()
    second, _ = service.enqueue_sync_job(organization_id, user_id, connector.id, scope.id)
    session.flush(); second_row = session.get(ConnectorSyncJob, second.job_id)
    second_row.status = "failed"; second_row.attempt_count = 1; second_row.fencing_token = 1
    second_row.next_attempt_at = None; second_row.completed_at = second_row.created_at
    second_row.last_error_category = "internal"; second_row.last_error_code = "unknown_internal"
    session.commit(); session.close()
    _configure_app(factory, organization_id, user_id)
    with TestClient(app) as client:
        listing = client.get(f"/api/v1/connectors/{connector.id}/scopes/{scope.id}/sync-jobs")
        detail = client.get(
            f"/api/v1/connectors/{connector.id}/scopes/{scope.id}/sync-jobs/{second.job_id}"
        )
    assert listing.status_code == detail.status_code == 200
    assert {item["status"] for item in listing.json()["items"]} == {"cancelled", "failed"}
    assert detail.json()["last_error_category"] == "internal"
    assert "summary" not in detail.text


def test_concurrent_service_enqueue_creates_one_active_job(factory):
    organization_id, user_id, space_id = _identity_rows(factory, "Concurrent", admin=True)
    setup = factory(); service = ConnectorManagementService(setup)
    connector = service.create_local_folder_connector(
        organization_id, user_id, display_name="Concurrent", slug="concurrent"
    )
    scope = service.create_local_folder_scope(
        organization_id, user_id, connector.id, knowledge_space_id=space_id,
        display_name="Folder", slug="folder", root_path="C:\\concurrent", follow_symlinks=False,
    )
    setup.commit(); setup.close()
    barrier = threading.Barrier(2); job_ids=[]; errors=[]
    def enqueue():
        session = factory()
        try:
            barrier.wait()
            result, _ = ConnectorManagementService(session).enqueue_sync_job(
                organization_id, user_id, connector.id, scope.id
            )
            session.commit(); job_ids.append(result.job_id)
        except Exception as exc:
            session.rollback(); errors.append(exc)
        finally:
            session.close()
    threads = [threading.Thread(target=enqueue) for _ in range(2)]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    assert not errors and len(set(job_ids)) == 1
    session = factory()
    assert session.scalar(select(func.count()).select_from(ConnectorSyncJob)) == 1
    session.close()


def test_request_transaction_rolls_back_partial_connector(factory):
    organization_id, user_id, _ = _identity_rows(factory, "Rollback", admin=True)
    _configure_app(factory, organization_id, user_id)
    class FailingService:
        def __init__(self, session):
            self.inner = ConnectorManagementService(session)
        def create_local_folder_connector(self, *args, **kwargs):
            self.inner.create_local_folder_connector(*args, **kwargs)
            raise RuntimeError("controlled post-flush failure")
    def failing_service(db_session: Session = Depends(get_db_session)):
        return FailingService(db_session)
    app.dependency_overrides[get_connector_management_service] = failing_service
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/connectors",
            json={"connector_type": "local_folder", "display_name": "Rollback", "slug": "rollback"},
        )
    assert response.status_code == 500 and "controlled" not in response.text
    session = factory()
    assert session.scalar(select(func.count()).select_from(Connector)) == 0
    session.close()