from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess
import threading
import uuid
from unittest.mock import Mock

import pytest
from fastapi import Depends
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from app.dependencies import (
    CurrentUser,
    get_connector_management_service,
    get_github_repository_discovery_service,
    get_current_user,
    get_db_session,
    get_connector_sync_schedule_service,
)
from app.main import app
from application.services.connector_management_service import ConnectorManagementService
from application.services.connector_sync_schedule_service import ConnectorSyncScheduleService
from domain.embeddings.exceptions import RetryableEmbeddingProviderError
from domain.embeddings.models import EmbeddingProfile, EmbeddingRequest, EmbeddingResult
from domain.embeddings.provider import EmbeddingProvider
from infrastructure.db.models import (
    Connector,
    ConnectorScope,
    ConnectorSyncJob,
    ConnectorSyncRun,
    ConnectorSyncSchedule,
    Document,
    DocumentChunk,
    DocumentIndexingState,
    DocumentVersion,
    SourceItem,
    SourceItemScopeMembership,
)
from infrastructure.workers.local_folder_sync_worker_host import (
    LocalFolderHostExitCode,
    LocalFolderWorkerHostSettings,
    compose_local_folder_sync_worker_host,
)
from infrastructure.workers.connector_sync_scheduler_host import (
    ConnectorSyncSchedulerHostSettings,
    SchedulerHostExitCode,
    compose_connector_sync_scheduler_host,
)

ROOT = Path(__file__).resolve().parents[5]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
ADMIN_ROLE_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
EMPLOYEE_ROLE_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
DIMENSION = 1536


class HostTestEmbeddingProvider(EmbeddingProvider):
    def __init__(self) -> None:
        self.calls = 0
        self.transient_failures = 0

    @property
    def profile(self) -> EmbeddingProfile:
        return EmbeddingProfile("host-test", "host-test", DIMENSION, "host-test:1536", 64)

    def embed_batch(self, requests: Sequence[EmbeddingRequest]) -> tuple[EmbeddingResult, ...]:
        self.calls += 1
        if self.transient_failures > 0:
            self.transient_failures -= 1
            raise RetryableEmbeddingProviderError("controlled transient failure")
        return tuple(
            EmbeddingResult(
                request.input_index,
                (float(request.input_index + 1),) * DIMENSION,
                self.profile.model_identifier,
                DIMENSION,
            )
            for request in requests
        )


def _host_settings(worker_id: str) -> LocalFolderWorkerHostSettings:
    return LocalFolderWorkerHostSettings(
        worker_id=worker_id,
        idle_interval=timedelta(seconds=1),
        lease_duration=timedelta(minutes=15),
        heartbeat_interval=timedelta(minutes=1),
        maximum_consecutive_failures=3,
        minimum_backoff=timedelta(seconds=1),
        maximum_backoff=timedelta(seconds=4),
        backoff_jitter=0.0,
        graceful_shutdown_timeout=timedelta(minutes=5),
        one_shot=True,
        expired_recovery_limit=10,
    )


def _scheduler_settings(scheduler_id: str) -> ConnectorSyncSchedulerHostSettings:
    return ConnectorSyncSchedulerHostSettings(
        scheduler_id=scheduler_id,
        poll_interval=timedelta(seconds=1),
        maximum_consecutive_failures=3,
        minimum_backoff=timedelta(seconds=1),
        maximum_backoff=timedelta(seconds=4),
        backoff_jitter=0.0,
        graceful_shutdown_timeout=timedelta(seconds=30),
        one_shot=True,
    )


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
            "connector_sync_items", "connector_sync_runs", "connector_sync_schedules",
            "connector_sync_jobs",
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
    app.dependency_overrides[get_github_repository_discovery_service] = Mock
    with TestClient(app) as client:
        assert client.get("/api/v1/connectors").status_code == 403
        assert client.post(
            "/api/v1/connectors",
            json={"connector_type": "github", "display_name": "GitHub", "slug": "github"},
        ).status_code == 403
        assert client.get(
            f"/api/v1/connectors/{uuid.uuid4()}/github/repositories"
        ).status_code == 403


def test_admin_creates_tenant_bound_draft_github_connector(factory):
    organization_id, user_id, _ = _identity_rows(factory, "GitHubAdmin", admin=True)
    _configure_app(factory, organization_id, user_id)
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/connectors",
            json={"connector_type": "github", "display_name": "GitHub", "slug": "github"},
        )
        unsupported = client.post(
            "/api/v1/connectors",
            json={"connector_type": "google_drive", "display_name": "Drive", "slug": "drive"},
        )
        forced_active = client.post(
            "/api/v1/connectors",
            json={
                "connector_type": "github", "display_name": "Unsafe", "slug": "unsafe",
                "status": "active",
            },
        )
        listing = client.get("/api/v1/connectors")
    assert response.status_code == 201
    body = response.json()
    assert body["connector_type"] == "github" and body["status"] == "draft"
    assert body["acl_support"] == "none"
    assert body["capabilities"]["supports_repository_discovery"] is True
    assert body["capabilities"]["supports_repository_selection"] is True
    assert {
        value
        for key, value in body["capabilities"].items()
        if key not in {"supports_repository_discovery", "supports_repository_selection"}
    } == {False}
    assert unsupported.status_code == forced_active.status_code == 422
    assert [item["connector_id"] for item in listing.json()["items"]] == [body["connector_id"]]
    session = factory()
    row = session.get(Connector, uuid.UUID(body["connector_id"]))
    assert row.organization_id == organization_id and row.created_by_user_id == user_id
    session.close()


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


def test_api_enqueued_job_runs_end_to_end_through_one_shot_host(factory, tmp_path):
    organization_id, user_id, space_id = _identity_rows(factory, "Hosted", admin=True)
    _configure_app(factory, organization_id, user_id)
    root = tmp_path / "hosted"
    root.mkdir()
    document_path = root / "document.txt"
    document_path.write_text("initial content", encoding="utf-8")
    removable_path = root / "removable.txt"
    removable_path.write_text("retained until complete scan", encoding="utf-8")
    with TestClient(app) as client:
        connector = client.post(
            "/api/v1/connectors",
            json={"connector_type": "local_folder", "display_name": "Hosted", "slug": "hosted"},
        )
        connector_id = connector.json()["connector_id"]
        scope = client.post(
            f"/api/v1/connectors/{connector_id}/scopes",
            json={
                "knowledge_space_id": str(space_id),
                "display_name": "Hosted",
                "slug": "hosted",
                "configuration": {"root_path": str(root.resolve()), "follow_symlinks": False},
            },
        )
        scope_id = scope.json()["scope_id"]
        queued = client.post(
            f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs"
        )
    assert queued.status_code == 202

    provider = HostTestEmbeddingProvider()
    host = compose_local_folder_sync_worker_host(
        _host_settings("host-e2e-one"),
        session_factory=factory,
        embedding_provider_factory=lambda: provider,
        random_uniform=lambda low, high: high / 2,
    )
    assert host.run() == LocalFolderHostExitCode.SUCCESS
    assert provider.calls == 2
    session = factory()
    job = session.get(ConnectorSyncJob, uuid.UUID(queued.json()["job_id"]))
    assert job is not None and job.status == "succeeded" and job.attempt_count == 1
    assert session.scalar(select(func.count()).select_from(ConnectorSyncRun)) == 1
    assert session.scalar(select(func.count()).select_from(SourceItem)) == 2
    assert session.scalar(select(func.count()).select_from(DocumentVersion)) == 2
    assert session.scalar(select(func.count()).select_from(Document)) == 2
    assert session.scalar(select(func.count()).select_from(DocumentChunk)) == 2
    assert session.scalar(
        select(func.count()).select_from(DocumentIndexingState).where(
            DocumentIndexingState.status == "indexed"
        )
    ) == 2
    session.close()

    second_host = compose_local_folder_sync_worker_host(
        _host_settings("host-e2e-two"),
        session_factory=factory,
        embedding_provider_factory=lambda: provider,
        random_uniform=lambda low, high: high / 2,
    )
    assert second_host.run() == LocalFolderHostExitCode.NO_WORK

    with TestClient(app) as client:
        unchanged = client.post(
            f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs"
        )
    assert unchanged.status_code == 202
    assert second_host.run() == LocalFolderHostExitCode.SUCCESS
    assert provider.calls == 2

    document_path.write_text("changed content", encoding="utf-8")
    with TestClient(app) as client:
        changed = client.post(
            f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs"
        )
    assert changed.status_code == 202
    assert second_host.run() == LocalFolderHostExitCode.SUCCESS
    assert provider.calls == 3
    session = factory()
    assert session.scalar(select(func.count()).select_from(DocumentVersion)) == 3
    assert session.scalar(select(func.count()).select_from(DocumentIndexingState)) == 3
    session.close()

    removable_path.unlink()
    document_path.write_text("retryable content", encoding="utf-8")
    provider.transient_failures = 1
    with TestClient(app) as client:
        retryable = client.post(
            f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs"
        )
    assert retryable.status_code == 202
    assert second_host.run() == LocalFolderHostExitCode.RETRY_SCHEDULED
    session = factory()
    retry_job = session.get(ConnectorSyncJob, uuid.UUID(retryable.json()["job_id"]))
    assert retry_job is not None and retry_job.status == "retry_wait"
    memberships = session.scalars(select(SourceItemScopeMembership)).all()
    assert len(memberships) == 2 and all(item.status == "active" for item in memberships)
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


def test_recurring_schedule_runs_api_to_scheduler_to_worker_without_network(factory, tmp_path):
    organization_id, user_id, space_id = _identity_rows(factory, "Scheduled", admin=True)
    _configure_app(factory, organization_id, user_id)
    clock_value = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)

    def schedule_service(db_session: Session = Depends(get_db_session)):
        return ConnectorSyncScheduleService(db_session, clock=lambda: clock_value)

    app.dependency_overrides[get_connector_sync_schedule_service] = schedule_service
    root = tmp_path / "scheduled"
    root.mkdir()
    (root / "document.txt").write_text("scheduled content", encoding="utf-8")
    with TestClient(app) as client:
        connector = client.post(
            "/api/v1/connectors",
            json={"connector_type": "local_folder", "display_name": "Scheduled", "slug": "scheduled"},
        )
        connector_id = connector.json()["connector_id"]
        scope = client.post(
            f"/api/v1/connectors/{connector_id}/scopes",
            json={
                "knowledge_space_id": str(space_id), "display_name": "Scheduled", "slug": "scheduled",
                "configuration": {"root_path": str(root.resolve()), "follow_symlinks": False},
            },
        )
        scope_id = scope.json()["scope_id"]
        schedule = client.put(
            f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/schedule",
            json={"interval_seconds": 3600, "first_run_at": clock_value.isoformat()},
        )
    assert schedule.status_code == 200

    scheduler = compose_connector_sync_scheduler_host(
        _scheduler_settings("scheduler-e2e"), session_factory=factory, clock=lambda: clock_value,
        random_uniform=lambda low, high: high,
    )
    assert scheduler.run() == SchedulerHostExitCode.SUCCESS
    assert scheduler.run() == SchedulerHostExitCode.NO_WORK
    session = factory()
    job = session.scalar(select(ConnectorSyncJob))
    persisted_schedule = session.scalar(select(ConnectorSyncSchedule))
    assert job is not None and job.trigger_type == "scheduled" and job.status == "queued"
    assert persisted_schedule.next_run_at == clock_value + timedelta(hours=1)
    session.close()

    provider = HostTestEmbeddingProvider()
    worker = compose_local_folder_sync_worker_host(
        _host_settings("worker-scheduled"), session_factory=factory,
        embedding_provider_factory=lambda: provider, clock=lambda: clock_value,
        random_uniform=lambda low, high: high,
    )
    assert worker.run() == LocalFolderHostExitCode.SUCCESS and provider.calls == 1

    clock_value += timedelta(hours=3, minutes=20)
    assert scheduler.run() == SchedulerHostExitCode.SUCCESS
    session = factory()
    active_job = session.scalar(
        select(ConnectorSyncJob).where(ConnectorSyncJob.status == "queued")
    )
    persisted_schedule = session.scalar(select(ConnectorSyncSchedule))
    assert active_job is not None
    assert persisted_schedule.next_run_at == datetime(2026, 8, 24, 16, tzinfo=timezone.utc)
    clock_value = persisted_schedule.next_run_at
    session.close()
    assert scheduler.run() == SchedulerHostExitCode.SUCCESS
    session = factory()
    assert session.scalar(
        select(func.count()).select_from(ConnectorSyncJob).where(
            ConnectorSyncJob.status.in_(("queued", "running", "retry_wait"))
        )
    ) == 1
    session.close()

    with TestClient(app) as client:
        paused = client.patch(
            f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/schedule",
            json={"action": "pause"},
        )
        assert paused.status_code == 200 and paused.json()["status"] == "paused"
        resumed = client.patch(
            f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/schedule",
            json={"action": "resume"},
        )
        assert resumed.status_code == 200 and resumed.json()["next_run_at"] > clock_value.isoformat()
        deleted = client.delete(
            f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/schedule"
        )
        manual = client.post(
            f"/api/v1/connectors/{connector_id}/scopes/{scope_id}/sync-jobs"
        )
    assert deleted.status_code == 204 and manual.status_code == 202
    session = factory()
    assert session.scalar(select(func.count()).select_from(ConnectorSyncSchedule)) == 0
    assert session.get(ConnectorSyncJob, active_job.id) is not None
    session.close()
