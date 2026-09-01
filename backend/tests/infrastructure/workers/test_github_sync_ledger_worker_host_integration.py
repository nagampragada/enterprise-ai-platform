from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import subprocess
import threading
import time
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from domain.connectors.sync_work_ledger import (
    FileWorkCounters,
    FileWorkManifestEntry,
    FileWorkMaterialization,
    FileWorkMaterializationChunk,
    FileWorkStatus,
    RepositoryGenerationRegistration,
)
from infrastructure.db.models import (
    ConnectorSyncFileMaterialization,
    ConnectorSyncFileMaterializationChunk,
    ConnectorSyncFileWorkItem,
    ConnectorSyncGeneration,
)
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    ConnectorSyncWorkLedgerRepository,
    StaleFileWorkFence,
)
from infrastructure.workers.github_sync_ledger_worker_host import (
    GitHubLedgerHostExitCode,
    GitHubLedgerWorkerSettings,
    GitHubSyncLedgerWorkerHost,
)
from infrastructure.workers.github_sync_work_item_worker import (
    GitHubFileWorkExecution,
)
from infrastructure.workers.lease_heartbeat import FileWorkLeaseHeartbeat


ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
PROFILE = "github:extract-v1:chunk-v2:embed-v1"


def _identity(url: str) -> tuple[object, ...]:
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database, value.query


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
    value = create_engine(url, future=True, pool_size=20, max_overflow=0)
    try:
        yield value
    finally:
        value.dispose()


@pytest.fixture(autouse=True)
def clean_database(engine):
    with engine.begin() as connection:
        for table in (
            "connector_sync_file_materialization_chunks",
            "connector_sync_file_materializations",
            "connector_sync_file_work_items",
            "connector_sync_generations",
            "connector_sync_runs",
            "connector_sync_jobs",
            "connector_scopes",
            "connectors",
            "knowledge_spaces",
            "organizations",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


def _queue(engine, item_count: int):
    organization_id, connector_id, space_id, scope_id, job_id = (
        uuid4() for _ in range(5)
    )
    now = datetime.now(UTC)
    with Session(engine, expire_on_commit=False) as session:
        session.execute(
            text("INSERT INTO organizations (id,name,slug) VALUES (:id,:name,:slug)"),
            {"id": organization_id, "name": "Ledger Test", "slug": f"org-{organization_id}"},
        )
        session.execute(
            text(
                "INSERT INTO connectors "
                "(id,organization_id,connector_type,display_name,slug,status) "
                "VALUES (:id,:org,'github',:name,:slug,'active')"
            ),
            {
                "id": connector_id,
                "org": organization_id,
                "name": "Ledger Test",
                "slug": f"connector-{connector_id}",
            },
        )
        session.execute(
            text(
                "INSERT INTO knowledge_spaces (id,organization_id,name,slug) "
                "VALUES (:id,:org,:name,:slug)"
            ),
            {
                "id": space_id,
                "org": organization_id,
                "name": "Ledger Test",
                "slug": f"space-{space_id}",
            },
        )
        session.execute(
            text(
                "INSERT INTO connector_scopes "
                "(id,organization_id,connector_id,knowledge_space_id,display_name,slug,"
                "scope_type,external_scope_key,access_mode,status) "
                "VALUES (:id,:org,:connector,:space,:name,:slug,'repository',:key,"
                "'platform_managed','active')"
            ),
            {
                "id": scope_id,
                "org": organization_id,
                "connector": connector_id,
                "space": space_id,
                "name": "Ledger Test",
                "slug": f"scope-{scope_id}",
                "key": f"github:repository:{scope_id.int}",
            },
        )
        session.execute(
            text(
                "INSERT INTO connector_sync_jobs "
                "(id,organization_id,connector_id,connector_scope_id,mode,trigger_type,status) "
                "VALUES (:id,:org,:connector,:scope,'incremental','manual','queued')"
            ),
            {
                "id": job_id,
                "org": organization_id,
                "connector": connector_id,
                "scope": scope_id,
            },
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        generation, _ = repository.register_generation(
            RepositoryGenerationRegistration(
                organization_id,
                connector_id,
                scope_id,
                job_id,
                "github",
                "github:repository:123456",
                "main",
                "a" * 40,
                "b" * 40,
                PROFILE,
                now,
            )
        )
        entries = tuple(
            FileWorkManifestEntry(
                f"github:repository:123456:path:documents/file-{index}.md",
                f"documents/file-{index}.md",
                f"{index + 1:040x}",
                "a" * 40,
                PROFILE,
                85,
                ".md",
                "text/markdown",
            )
            for index in range(item_count)
        )
        registered = repository.register_manifest(
            organization_id, generation.generation_id, entries, now=now
        )
        repository.mark_discovery_complete(
            organization_id, generation.generation_id, now=now
        )
        session.commit()
    return organization_id, generation.generation_id, registered.work_item_ids


class PostgreSQLFileWorker:
    """Deterministic provider-free processor using production ledger operations."""

    def __init__(self, sessions, worker_id, claims, lock, *, pause=0.005):
        self._sessions = sessions
        self._worker_id = worker_id
        self._claims = claims
        self._lock = lock
        self._pause = pause

    def execute_one_result(self, *, claim_allowed):
        session = self._sessions()
        now = datetime.now(UTC)
        try:
            repository = ConnectorSyncWorkLedgerRepository(session)
            repository.recover_expired_available(
                provider_key="github",
                profile_fingerprint=PROFILE,
                now=now,
                limit=10,
            )
            lease = None
            if claim_allowed():
                lease = repository.claim_next_available(
                    provider_key="github",
                    profile_fingerprint=PROFILE,
                    worker_id=self._worker_id,
                    now=now,
                    lease_duration=timedelta(seconds=30),
                )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        if lease is None:
            return GitHubFileWorkExecution("no_work")
        with self._lock:
            self._claims.append(lease.work_item_id)
        time.sleep(self._pause)
        session = self._sessions()
        try:
            repository = ConnectorSyncWorkLedgerRepository(session)
            generation = repository.get_generation(
                lease.organization_id, lease.generation_id
            )
            work = repository.get_work_item(
                lease.organization_id, lease.generation_id, lease.work_item_id
            )
            assert generation is not None and work is not None
            counters = FileWorkCounters(85, 85, 1, 1)
            completed, _persisted, _created = (
                repository.stage_materialization_and_complete(
                    lease,
                    worker_id=self._worker_id,
                    generation=generation,
                    work_item=work,
                    materialization=_materialization(generation, work),
                    counters=counters,
                    now=datetime.now(UTC),
                )
            )
            session.commit()
            return GitHubFileWorkExecution(
                "completed",
                lease.work_item_id,
                lease.attempt_number,
                completed.counters,
            )
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def _materialization(generation, work):
    return FileWorkMaterialization(
        generation.repository_identity,
        generation.branch_name,
        generation.root_tree_object_id,
        work.source_item_key,
        work.repository_path,
        work.provider_blob_id,
        work.provider_revision_id,
        work.profile_fingerprint,
        "c" * 64,
        "file",
        "text/markdown",
        "openai:text-embedding-3-small:1536",
        (
            FileWorkMaterializationChunk(
                0,
                "bounded deterministic content",
                "d" * 64,
                (0.0,) * 1536,
                "openai:text-embedding-3-small:1536",
            ),
        ),
    )


def _settings(worker_id: str, *, max_items=100):
    return GitHubLedgerWorkerSettings(
        True,
        worker_id,
        max_items,
        timedelta(seconds=10),
        timedelta(seconds=1),
        timedelta(seconds=30),
        timedelta(milliseconds=100),
        timedelta(milliseconds=10),
        1,
        timedelta(seconds=1),
        10,
        timedelta(milliseconds=50),
    )


def _host(sessions, worker_id, claims, lock, *, max_items=100, shutdown=None):
    worker = PostgreSQLFileWorker(sessions, worker_id, claims, lock)
    return GitHubSyncLedgerWorkerHost(
        worker,
        _settings(worker_id, max_items=max_items),
        shutdown_event=shutdown,
    )


def test_multiple_hosts_use_skip_locked_and_converge_without_duplicates(engine):
    organization_id, generation_id, work_ids = _queue(engine, 10)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    claims = []
    lock = threading.Lock()
    hosts = [
        _host(sessions, f"worker-{index}", claims, lock) for index in range(3)
    ]

    with ThreadPoolExecutor(max_workers=3) as pool:
        exit_codes = tuple(pool.map(lambda host: host.run(), hosts))

    assert all(code == 0 for code in exit_codes)
    assert len(claims) == 10
    assert len(set(claims)) == 10
    assert set(claims) == set(work_ids)
    with Session(engine) as session:
        statuses = session.scalars(
            select(ConnectorSyncFileWorkItem.status).where(
                ConnectorSyncFileWorkItem.organization_id == organization_id,
                ConnectorSyncFileWorkItem.generation_id == generation_id,
            )
        ).all()
        assert statuses == [FileWorkStatus.SUCCEEDED.value] * 10
        assert session.scalar(select(func.count(ConnectorSyncFileMaterialization.id))) == 10
        assert session.scalar(select(func.count(ConnectorSyncFileMaterializationChunk.id))) == 10
        generation = session.scalar(
            select(ConnectorSyncGeneration).where(
                ConnectorSyncGeneration.organization_id == organization_id,
                ConnectorSyncGeneration.id == generation_id,
            )
        )
        assert generation.status == "processing"
        assert generation.reconciliation_eligible is False


def test_more_hosts_than_items_produces_disjoint_claims_and_empty_hosts(engine):
    _organization_id, _generation_id, work_ids = _queue(engine, 2)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    claims = []
    lock = threading.Lock()
    hosts = [
        _host(sessions, f"worker-{index}", claims, lock) for index in range(5)
    ]

    with ThreadPoolExecutor(max_workers=5) as pool:
        exit_codes = tuple(pool.map(lambda host: host.run(), hosts))

    assert len(claims) == len(set(claims)) == 2
    assert set(claims) == set(work_ids)
    assert all(code == GitHubLedgerHostExitCode.SUCCESS for code in exit_codes)


def test_item_limits_apply_independently_per_host(engine):
    _organization_id, _generation_id, _work_ids = _queue(engine, 6)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    claims = []
    lock = threading.Lock()
    first = _host(sessions, "worker-one", claims, lock, max_items=1)
    second = _host(sessions, "worker-two", claims, lock, max_items=2)

    with ThreadPoolExecutor(max_workers=2) as pool:
        exit_codes = tuple(pool.map(lambda host: host.run(), (first, second)))

    assert exit_codes == (0, 0)
    assert first.last_summary.items_claimed == 1
    assert second.last_summary.items_claimed == 2
    assert len(claims) == len(set(claims)) == 3
    with Session(engine) as session:
        pending = session.scalar(
            select(func.count(ConnectorSyncFileWorkItem.id)).where(
                ConnectorSyncFileWorkItem.status == FileWorkStatus.PENDING.value
            )
        )
        assert pending == 3


def test_one_stopped_host_does_not_block_another(engine):
    _organization_id, _generation_id, work_ids = _queue(engine, 4)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    claims = []
    lock = threading.Lock()
    stopped = threading.Event()
    stopped.set()
    first = _host(sessions, "stopped-worker", claims, lock, shutdown=stopped)
    second = _host(sessions, "active-worker", claims, lock)

    with ThreadPoolExecutor(max_workers=2) as pool:
        exit_codes = tuple(pool.map(lambda host: host.run(), (first, second)))

    assert exit_codes[0] == GitHubLedgerHostExitCode.SHUTDOWN
    assert exit_codes[1] == GitHubLedgerHostExitCode.CLEAN_DRAIN
    assert set(claims) == set(work_ids)


def test_processing_writes_only_staging_and_ledger_state(engine):
    organization_id, generation_id, _work_ids = _queue(engine, 1)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    claims = []
    host = _host(sessions, "isolated-worker", claims, threading.Lock())
    legacy_tables = (
        "source_items",
        "source_item_scope_memberships",
        "documents",
        "document_versions",
        "document_version_documents",
        "document_indexing_states",
        "document_indexing_attempts",
        "document_chunks",
    )
    with engine.connect() as connection:
        before = {
            table: connection.scalar(text(f"SELECT COUNT(*) FROM {table}"))
            for table in legacy_tables
        }

    assert host.run() == GitHubLedgerHostExitCode.CLEAN_DRAIN

    with engine.connect() as connection:
        after = {
            table: connection.scalar(text(f"SELECT COUNT(*) FROM {table}"))
            for table in legacy_tables
        }
    assert after == before
    with Session(engine) as session:
        assert session.scalar(select(func.count(ConnectorSyncFileMaterialization.id))) == 1
        generation = session.scalar(
            select(ConnectorSyncGeneration).where(
                ConnectorSyncGeneration.organization_id == organization_id,
                ConnectorSyncGeneration.id == generation_id,
            )
        )
        assert generation.status == "processing"
        assert generation.reconciliation_eligible is False


def test_expired_lease_recovery_advances_fence_and_rejects_stale_completion(engine):
    organization_id, generation_id, _work_ids = _queue(engine, 1)
    claim_now = datetime.now(UTC)
    with Session(engine, expire_on_commit=False) as session:
        repository = ConnectorSyncWorkLedgerRepository(session)
        stale = repository.claim_next_available(
            provider_key="github",
            profile_fingerprint=PROFILE,
            worker_id="stale-worker",
            now=claim_now,
            lease_duration=timedelta(seconds=1),
        )
        session.commit()
    with Session(engine, expire_on_commit=False) as session:
        repository = ConnectorSyncWorkLedgerRepository(session)
        recovered = repository.recover_expired_available(
            provider_key="github",
            profile_fingerprint=PROFILE,
            now=claim_now + timedelta(seconds=2),
            limit=10,
        )
        replacement = repository.claim_next_available(
            provider_key="github",
            profile_fingerprint=PROFILE,
            worker_id="replacement-worker",
            now=claim_now + timedelta(seconds=2),
            lease_duration=timedelta(seconds=30),
        )
        session.commit()
    assert len(recovered) == 1
    assert replacement.fencing_token == stale.fencing_token + 1
    with Session(engine) as session:
        with pytest.raises(StaleFileWorkFence):
            ConnectorSyncWorkLedgerRepository(session).complete(
                stale,
                worker_id="stale-worker",
                outcome=FileWorkStatus.SUCCEEDED,
                counters=FileWorkCounters(),
                now=datetime.now(UTC),
            )
        session.rollback()
        row = ConnectorSyncWorkLedgerRepository(session).get_work_item(
            organization_id, generation_id, replacement.work_item_id
        )
        assert row.status is FileWorkStatus.RUNNING
        assert row.attempt_count == 2


def test_independent_heartbeat_renews_active_file_lease(engine):
    _organization_id, _generation_id, _work_ids = _queue(engine, 1)
    sessions = sessionmaker(bind=engine, expire_on_commit=False)
    now = datetime.now(UTC)
    with Session(engine, expire_on_commit=False) as session:
        lease = ConnectorSyncWorkLedgerRepository(session).claim_next_available(
            provider_key="github",
            profile_fingerprint=PROFILE,
            worker_id="heartbeat-worker",
            now=now,
            lease_duration=timedelta(seconds=1),
        )
        session.commit()

    with FileWorkLeaseHeartbeat(
        sessions,
        lease,
        worker_id="heartbeat-worker",
        lease_duration=timedelta(seconds=1),
        interval=timedelta(milliseconds=100),
        shutdown_timeout=timedelta(seconds=1),
    ):
        time.sleep(0.25)

    with Session(engine) as session:
        row = session.get(ConnectorSyncFileWorkItem, lease.work_item_id)
        assert row.heartbeat_at > now
        assert row.lease_expires_at > lease.lease_expires_at
