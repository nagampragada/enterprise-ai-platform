from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess
import threading
import uuid

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from application.services.connector_sync_execution_service import ConnectorSyncExecutionService
from application.services.connector_sync_retry_policy import ConnectorSyncRetryPolicy
from application.services.staged_local_folder_synchronization_service import (
    LocalFolderPreparationService,
    StagedLocalFolderSynchronizationService,
)
from domain.embeddings.exceptions import PermanentEmbeddingProviderError, RetryableEmbeddingProviderError
from domain.embeddings.models import EmbeddingProfile, EmbeddingRequest, EmbeddingResult
from domain.embeddings.provider import EmbeddingProvider
from infrastructure.connectors.local.connector import LocalFolderConnector
from infrastructure.content_chunking.text_chunker import DeterministicTextChunker
from infrastructure.content_extraction.registry import create_default_content_extractor_registry
from infrastructure.db.models import (
    Connector,
    ConnectorScope,
    ConnectorSyncJob,
    ConnectorSyncRun,
    Document,
    DocumentChunk,
    DocumentIndexingAttempt,
    DocumentVersion,
    KnowledgeSpace,
    Organization,
    SourceItem,
    SourceItemScopeMembership,
)
from infrastructure.repositories.connector_sync_job_repository import ConnectorSyncJobRepository
from infrastructure.workers.local_folder_sync_worker import LocalFolderSyncWorker

ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
DIMENSION = 1536
MODEL_IDENTIFIER = "worker-fake:1536"
START = datetime.now(timezone.utc).replace(microsecond=0)


class MutableClock:
    def __init__(self) -> None:
        self.value = START

    def __call__(self) -> datetime:
        return self.value

    def advance(self, delta: timedelta) -> None:
        self.value += delta


class DeterministicWorkerEmbeddingProvider(EmbeddingProvider):
    def __init__(self, *, transient_failures: int = 0, permanent: bool = False) -> None:
        self.transient_failures = transient_failures
        self.permanent = permanent
        self.calls = 0

    @property
    def profile(self) -> EmbeddingProfile:
        return EmbeddingProfile("worker-fake", "worker-fake", DIMENSION, MODEL_IDENTIFIER, 64)

    def embed_batch(self, requests: Sequence[EmbeddingRequest]) -> tuple[EmbeddingResult, ...]:
        self.calls += 1
        if self.permanent:
            raise PermanentEmbeddingProviderError("controlled permanent failure")
        if self.calls <= self.transient_failures:
            raise RetryableEmbeddingProviderError("controlled transient failure")
        return tuple(
            EmbeddingResult(
                request.input_index,
                (float(request.input_index + 1),) * DIMENSION,
                MODEL_IDENTIFIER,
                DIMENSION,
            )
            for request in requests
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
    environment = os.environ.copy()
    environment["DATABASE_URL"] = url
    subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(INI), "upgrade", "head"],
        check=True,
        cwd=str(ROOT),
        env=environment,
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


def _session_factory(engine):
    return sessionmaker(bind=engine, class_=Session, autoflush=False, expire_on_commit=False)


def _configured_scope(factory, root: Path, name: str):
    session = factory()
    organization_id, connector_id, space_id, scope_id = (uuid.uuid4() for _ in range(4))
    provider = LocalFolderConnector(organization_id, connector_id, root)
    session.add(Organization(id=organization_id, name=name, slug=f"{name.lower()}-{organization_id}"))
    session.flush()
    session.add(KnowledgeSpace(id=space_id, organization_id=organization_id, name=name, slug=f"space-{space_id}"))
    session.flush()
    session.add(
        Connector(
            id=connector_id,
            organization_id=organization_id,
            connector_type="local_folder",
            display_name=name,
            slug=f"connector-{connector_id}",
            status="active",
            acl_support="none",
            capabilities=asdict(provider.capabilities),
            safe_config={},
            config_schema_version=1,
            credential_status="not_configured",
        )
    )
    session.flush()
    session.add(
        ConnectorScope(
            id=scope_id,
            organization_id=organization_id,
            connector_id=connector_id,
            knowledge_space_id=space_id,
            display_name=name,
            slug=f"scope-{scope_id}",
            scope_type="folder",
            external_scope_key=str(root.resolve()),
            access_mode="platform_managed",
            status="active",
            safe_config={"follow_symlinks": False},
            config_schema_version=1,
        )
    )
    session.commit()
    session.close()
    return organization_id, connector_id, scope_id


def _worker(factory, clock: MutableClock, provider: EmbeddingProvider, *, worker="worker-one"):
    policy = ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2)
    preparation = LocalFolderPreparationService(
        create_default_content_extractor_registry(), DeterministicTextChunker(), provider
    )
    return LocalFolderSyncWorker(
        factory,
        lambda session: ConnectorSyncExecutionService(
            ConnectorSyncJobRepository(session), policy, clock=clock
        ),
        lambda session: StagedLocalFolderSynchronizationService(
            session,
            ConnectorSyncExecutionService(
                ConnectorSyncJobRepository(session), policy, clock=clock
            ),
            preparation.profile,
        ),
        preparation,
        worker_id=worker,
        steps_per_invocation=1,
        batch_size=1,
        clock=clock,
    )


def _enqueue(factory, clock, organization_id, connector_id, scope_id, *, maximum=3):
    session = factory()
    service = ConnectorSyncExecutionService(
        ConnectorSyncJobRepository(session),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=clock,
    )
    result = service.enqueue(
        organization_id,
        connector_id,
        scope_id,
        mode="incremental",
        trigger_type="manual",
        max_attempts=maximum,
    )
    session.commit()
    session.close()
    return result.job_id


def _finish(worker: LocalFolderSyncWorker, context, *, maximum=20):
    results = []
    for _ in range(maximum):
        result = worker.execute(context)
        results.append(result)
        if result.outcome != "in_progress":
            return results
    raise AssertionError("worker did not finish within bounded test calls")


def _count(factory, model, *predicates):
    session = factory()
    try:
        return session.scalar(select(func.count()).select_from(model).where(*predicates))
    finally:
        session.close()


def test_success_and_unchanged_rerun_reuse_committed_indexing(engine, tmp_path):
    factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / "success"
    root.mkdir()
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    (root / "b.md").write_text("# beta", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(factory, root, "Success")
    provider = DeterministicWorkerEmbeddingProvider()
    worker = _worker(factory, clock, provider)
    first_job = _enqueue(factory, clock, organization_id, connector_id, scope_id)
    context = worker.claim_one(organization_id)
    assert context is not None and context.attempt_number == 1
    results = _finish(worker, context)
    assert results[-1].outcome == "completed"
    first_calls = provider.calls
    assert first_calls == 2
    assert _count(factory, SourceItem) == 2
    assert _count(factory, DocumentVersion) == 2
    assert _count(factory, Document) == 2
    assert _count(factory, DocumentChunk) >= 2
    assert _count(factory, ConnectorSyncRun, ConnectorSyncRun.sync_job_id == first_job) == 1
    session = factory()
    assert all(len(row.embedding) == DIMENSION for row in session.scalars(select(DocumentChunk)).all())
    session.close()

    second_job = _enqueue(factory, clock, organization_id, connector_id, scope_id)
    second = worker.claim_one(organization_id)
    assert second is not None and second.job_id == second_job
    assert _finish(worker, second)[-1].outcome == "completed"
    assert provider.calls == first_calls
    assert _count(factory, DocumentVersion) == 2
    assert _count(factory, DocumentIndexingAttempt) == 2


def test_changed_file_creates_exactly_one_new_version(engine, tmp_path):
    factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / "changed"; root.mkdir()
    path = root / "a.txt"; path.write_text("alpha", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(factory, root, "Changed")
    provider = DeterministicWorkerEmbeddingProvider(); worker = _worker(factory, clock, provider)
    _enqueue(factory, clock, organization_id, connector_id, scope_id)
    first = worker.claim_one(organization_id); assert first is not None
    assert _finish(worker, first)[-1].outcome == "completed"
    path.write_text("beta", encoding="utf-8")
    _enqueue(factory, clock, organization_id, connector_id, scope_id)
    second = worker.claim_one(organization_id); assert second is not None
    assert _finish(worker, second)[-1].outcome == "completed"
    assert _count(factory, DocumentVersion) == 2
    assert _count(factory, DocumentIndexingAttempt) == 2
    assert provider.calls == 2


def test_missing_item_is_removed_only_after_complete_scan_reconciliation(engine, tmp_path):
    factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / "removed"; root.mkdir()
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    removed = root / "b.txt"; removed.write_text("beta", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(factory, root, "Removed")
    worker = _worker(factory, clock, DeterministicWorkerEmbeddingProvider())
    _enqueue(factory, clock, organization_id, connector_id, scope_id)
    first = worker.claim_one(organization_id); assert first is not None
    assert _finish(worker, first)[-1].outcome == "completed"
    removed.unlink()
    clock.advance(timedelta(seconds=1))
    _enqueue(factory, clock, organization_id, connector_id, scope_id)
    second = worker.claim_one(organization_id); assert second is not None
    assert worker.execute(second).outcome == "in_progress"
    session = factory()
    memberships = session.scalars(select(SourceItemScopeMembership)).all()
    assert sum(row.status == "active" for row in memberships) == 2
    session.close()
    assert _finish(worker, second)[-1].outcome == "completed"
    session = factory(); memberships = session.scalars(select(SourceItemScopeMembership)).all()
    assert sum(row.status == "removed" for row in memberships) == 1
    session.close()


def test_embedding_failure_before_complete_scan_never_removes_unseen_item(engine, tmp_path):
    factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / "partial"; root.mkdir()
    changed = root / "a.txt"; changed.write_text("alpha", encoding="utf-8")
    missing = root / "b.txt"; missing.write_text("beta", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(factory, root, "Partial")
    initial = _worker(factory, clock, DeterministicWorkerEmbeddingProvider())
    _enqueue(factory, clock, organization_id, connector_id, scope_id)
    first = initial.claim_one(organization_id); assert first is not None
    assert _finish(initial, first)[-1].outcome == "completed"
    changed.write_text("changed", encoding="utf-8"); missing.unlink()
    failing = _worker(factory, clock, DeterministicWorkerEmbeddingProvider(transient_failures=1))
    _enqueue(factory, clock, organization_id, connector_id, scope_id)
    attempt = failing.claim_one(organization_id); assert attempt is not None
    assert failing.execute(attempt).outcome == "retry_scheduled"
    session = factory(); memberships = session.scalars(select(SourceItemScopeMembership)).all()
    assert all(row.status == "active" for row in memberships)
    session.close()


def test_crash_after_one_persisted_item_resumes_without_duplicate_artifacts(engine, tmp_path):
    factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / "resume"; root.mkdir()
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    (root / "b.txt").write_text("beta", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(factory, root, "Resume")
    provider = DeterministicWorkerEmbeddingProvider(); worker = _worker(factory, clock, provider)
    _enqueue(factory, clock, organization_id, connector_id, scope_id)
    context = worker.claim_one(organization_id); assert context is not None
    assert worker.execute(context).outcome == "in_progress"
    assert _count(factory, SourceItem) == 1 and _count(factory, DocumentVersion) == 1
    resumed = _worker(factory, clock, provider)
    assert _finish(resumed, context)[-1].outcome == "completed"
    assert _count(factory, SourceItem) == 2
    assert _count(factory, DocumentVersion) == 2
    assert _count(factory, DocumentIndexingAttempt) == 2
    assert provider.calls == 2


def test_stale_prepared_item_cannot_overwrite_newer_persisted_source(engine, tmp_path):
    factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / "stale-prepared"; root.mkdir()
    path = root / "a.txt"; path.write_text("alpha", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(factory, root, "StalePrepared")
    provider = DeterministicWorkerEmbeddingProvider(); worker = _worker(factory, clock, provider)
    _enqueue(factory, clock, organization_id, connector_id, scope_id)
    first = worker.claim_one(organization_id); assert first is not None
    assert _finish(worker, first)[-1].outcome == "completed"
    path.write_text("beta", encoding="utf-8")
    _enqueue(factory, clock, organization_id, connector_id, scope_id)
    second = worker.claim_one(organization_id); assert second is not None
    original = worker._preparation.prepare_item
    def prepare_then_advance(*args):
        prepared = original(*args)
        session = factory(); source = session.scalar(select(SourceItem))
        source.source_checksum = "b" * 64; session.commit(); session.close()
        return prepared
    worker._preparation.prepare_item = prepare_then_advance  # type: ignore[method-assign]
    assert worker.execute(second).outcome == "retry_scheduled"
    assert _count(factory, DocumentVersion) == 1


def test_transient_retry_allocates_new_lease_fence_and_run_then_succeeds(engine, tmp_path):
    factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / "retry"
    root.mkdir()
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(factory, root, "Retry")
    provider = DeterministicWorkerEmbeddingProvider(transient_failures=1)
    worker = _worker(factory, clock, provider)
    job_id = _enqueue(factory, clock, organization_id, connector_id, scope_id)
    first = worker.claim_one(organization_id)
    assert first is not None
    assert worker.execute(first).outcome == "retry_scheduled"
    assert worker.claim_one(organization_id) is None
    session = factory()
    job = session.get(ConnectorSyncJob, job_id)
    retry_at = job.next_attempt_at
    session.close()
    assert retry_at is not None
    clock.value = retry_at
    second = worker.claim_one(organization_id)
    assert second is not None
    assert second.attempt_number == second.fencing_token == 2
    assert second.lease_id != first.lease_id
    assert _finish(worker, second)[-1].outcome == "completed"
    assert provider.calls == 2
    assert _count(factory, ConnectorSyncRun, ConnectorSyncRun.sync_job_id == job_id) == 2


@pytest.mark.parametrize("permanent", (True,))
def test_permanent_failure_is_terminal_after_one_attempt(engine, tmp_path, permanent):
    factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / "permanent"
    root.mkdir()
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(factory, root, "Permanent")
    provider = DeterministicWorkerEmbeddingProvider(permanent=permanent)
    worker = _worker(factory, clock, provider)
    job_id = _enqueue(factory, clock, organization_id, connector_id, scope_id)
    context = worker.claim_one(organization_id)
    assert context is not None
    assert worker.execute(context).outcome == "failed"
    session = factory()
    job = session.get(ConnectorSyncJob, job_id)
    assert job.status == "failed" and job.next_attempt_at is None and job.attempt_count == 1
    session.close()
    assert worker.claim_one(organization_id) is None


def test_three_retryable_failures_exhaust_exactly_three_attempts(engine, tmp_path):
    factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / "exhaust"
    root.mkdir()
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(factory, root, "Exhaust")
    provider = DeterministicWorkerEmbeddingProvider(transient_failures=10)
    worker = _worker(factory, clock, provider)
    job_id = _enqueue(factory, clock, organization_id, connector_id, scope_id, maximum=3)
    for attempt in range(1, 4):
        context = worker.claim_one(organization_id)
        assert context is not None and context.attempt_number == attempt
        expected = "failed" if attempt == 3 else "retry_scheduled"
        assert worker.execute(context).outcome == expected
        if attempt < 3:
            session = factory()
            clock.value = session.get(ConnectorSyncJob, job_id).next_attempt_at
            session.close()
    assert worker.claim_one(organization_id) is None
    assert provider.calls == 3
    assert _count(factory, ConnectorSyncRun, ConnectorSyncRun.sync_job_id == job_id) == 3


def test_cancellation_before_claim_after_claim_and_between_steps(engine, tmp_path):
    factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / "cancel"
    root.mkdir()
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    (root / "b.txt").write_text("beta", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(factory, root, "Cancel")
    provider = DeterministicWorkerEmbeddingProvider()
    worker = _worker(factory, clock, provider)
    queued = _enqueue(factory, clock, organization_id, connector_id, scope_id)
    session = factory()
    ConnectorSyncExecutionService(
        ConnectorSyncJobRepository(session),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=clock,
    ).request_cancellation(organization_id, queued)
    session.commit(); session.close()
    assert worker.claim_one(organization_id) is None

    job = _enqueue(factory, clock, organization_id, connector_id, scope_id)
    context = worker.claim_one(organization_id)
    assert context is not None
    session = factory()
    control = ConnectorSyncExecutionService(
        ConnectorSyncJobRepository(session),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=clock,
    )
    control.request_cancellation(organization_id, job)
    session.commit(); session.close()
    assert worker.execute(context).outcome == "cancelled"
    assert provider.calls == 0

    third = _enqueue(factory, clock, organization_id, connector_id, scope_id)
    context = worker.claim_one(organization_id)
    assert context is not None
    assert worker.execute(context).outcome == "in_progress"
    session = factory()
    ConnectorSyncExecutionService(
        ConnectorSyncJobRepository(session),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=clock,
    ).request_cancellation(organization_id, third)
    session.commit(); session.close()
    assert worker.execute(context).outcome == "cancelled"


def test_stale_worker_cannot_continue_after_recovery_and_reassignment(engine, tmp_path):
    factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / "stale"
    root.mkdir()
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(factory, root, "Stale")
    provider = DeterministicWorkerEmbeddingProvider()
    first_worker = _worker(factory, clock, provider, worker="worker-a")
    _enqueue(factory, clock, organization_id, connector_id, scope_id)
    first = first_worker.claim_one(organization_id)
    assert first is not None
    clock.value = first.lease_expires_at
    session = factory()
    control = ConnectorSyncExecutionService(
        ConnectorSyncJobRepository(session),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=clock,
    )
    recovered = control.recover_expired(limit=1, organization_id=organization_id)
    session.commit(); session.close()
    assert len(recovered) == 1 and recovered[0].status == "retry_wait"
    session = factory(); job = session.get(ConnectorSyncJob, first.job_id); clock.value = job.next_attempt_at; session.close()
    second_worker = _worker(factory, clock, provider, worker="worker-b")
    second = second_worker.claim_one(organization_id)
    assert second is not None and second.fencing_token == 2
    assert first_worker.execute(first).outcome == "lost_lease"
    assert _finish(second_worker, second)[-1].outcome == "completed"


def test_provider_response_then_commit_failure_rolls_back_progress_and_fails_safely(engine, tmp_path):
    base_factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / "rollback"
    root.mkdir()
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(base_factory, root, "Rollback")
    provider = DeterministicWorkerEmbeddingProvider()
    normal = _worker(base_factory, clock, provider)
    job_id = _enqueue(base_factory, clock, organization_id, connector_id, scope_id)
    context = normal.claim_one(organization_id)
    assert context is not None

    calls = 0
    def failing_factory():
        nonlocal calls
        calls += 1
        session = base_factory()
        if calls == 4:
            original_commit = session.commit
            def fail_before_commit():
                raise RuntimeError("controlled pre-commit failure")
            session.commit = fail_before_commit  # type: ignore[method-assign]
            session._original_commit = original_commit  # type: ignore[attr-defined]
        return session

    worker = _worker(failing_factory, clock, provider)
    assert worker.execute(context).outcome == "failed"
    assert provider.calls == 1
    assert _count(base_factory, SourceItem) == 0
    assert _count(base_factory, DocumentVersion) == 0
    session = base_factory(); job = session.get(ConnectorSyncJob, job_id)
    assert job.status == "failed" and job.lease_id is None
    run = session.scalar(select(ConnectorSyncRun).where(ConnectorSyncRun.sync_job_id == job_id))
    assert run is not None and run.status == "failed"
    session.close()


@pytest.mark.parametrize(
    ("connector_type", "connector_status", "scope_status"),
    (
        ("google_drive", "active", "active"),
        ("local_folder", "paused", "active"),
        ("local_folder", "active", "paused"),
    ),
)
def test_unsupported_or_disabled_dispatch_fails_permanently(
    engine, tmp_path, connector_type, connector_status, scope_status
):
    factory, clock = _session_factory(engine), MutableClock()
    root = tmp_path / f"dispatch-{connector_type}-{connector_status}-{scope_status}"
    root.mkdir()
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(factory, root, "Dispatch")
    provider = DeterministicWorkerEmbeddingProvider()
    worker = _worker(factory, clock, provider)
    job_id = _enqueue(factory, clock, organization_id, connector_id, scope_id)
    context = worker.claim_one(organization_id)
    assert context is not None
    session = factory()
    connector = session.get(Connector, connector_id)
    scope = session.get(ConnectorScope, scope_id)
    connector.connector_type = connector_type
    connector.status = connector_status
    scope.status = scope_status
    session.commit(); session.close()
    assert worker.execute(context).outcome == "failed"
    assert provider.calls == 0
    session = factory(); job = session.get(ConnectorSyncJob, job_id)
    assert job.status == "failed" and job.next_attempt_at is None
    session.close()


def test_two_workers_claim_once_and_tenant_dispatch_cannot_cross(engine, tmp_path):
    factory, clock = _session_factory(engine), MutableClock()
    first_root, second_root = tmp_path / "tenant-a", tmp_path / "tenant-b"
    first_root.mkdir(); second_root.mkdir()
    (first_root / "a.txt").write_text("alpha", encoding="utf-8")
    (second_root / "b.txt").write_text("beta", encoding="utf-8")
    first_org, first_connector, first_scope = _configured_scope(factory, first_root, "TenantA")
    second_org, _, _ = _configured_scope(factory, second_root, "TenantB")
    _enqueue(factory, clock, first_org, first_connector, first_scope)
    workers = [
        _worker(factory, clock, DeterministicWorkerEmbeddingProvider(), worker=f"worker-{index}")
        for index in range(2)
    ]
    barrier = threading.Barrier(2)
    contexts = []
    def claim(worker):
        barrier.wait()
        contexts.append(worker.claim_one(first_org))
    threads = [threading.Thread(target=claim, args=(worker,)) for worker in workers]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    assert len([context for context in contexts if context is not None]) == 1
    assert all(worker.claim_one(second_org) is None for worker in workers)