from __future__ import annotations

import json
import math
import os
import statistics
import subprocess
import threading
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from domain.connectors.sync_work_ledger import (
    FileWorkCounters,
    FileWorkManifestEntry,
    FileWorkStatus,
    RepositoryGenerationRegistration,
)
from infrastructure.db.models import ConnectorSyncFileWorkItem
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    ConnectorSyncWorkLedgerRepository,
    FileWorkCancellationConflict,
    InvalidSyncWorkLedgerRequest,
    LostFileWorkLease,
    StaleFileWorkFence,
    SyncWorkLedgerConflict,
    SyncWorkLedgerNotFound,
)


ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
TEST_URL = "TEST_DATABASE_URL"
DEV_URL = "DATABASE_URL"
NOW = datetime(2026, 8, 28, 12, tzinfo=timezone.utc)
LEASE = timedelta(minutes=5)
PROFILE = "github:extract-v1:chunk-v2:embed-v1"


def _identity(url: str) -> tuple[object, ...]:
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database, value.query


@pytest.fixture(scope="module")
def engine():
    url = os.environ[TEST_URL]
    development = os.environ.get(DEV_URL)
    if development and _identity(development) == _identity(url):
        raise RuntimeError("test database must differ from development database")
    reset = create_engine(url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    reset.dispose()
    environment = os.environ.copy()
    environment[DEV_URL] = url
    subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(INI), "upgrade", "head"],
        check=True,
        cwd=str(ROOT),
        env=environment,
    )
    value = create_engine(url, future=True, pool_size=40, max_overflow=0)
    try:
        yield value
    finally:
        value.dispose()


@pytest.fixture(autouse=True)
def clean_database(engine):
    with engine.begin() as connection:
        for table in (
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


def _setup(session: Session, label: str) -> tuple[UUID, UUID, UUID, UUID]:
    organization_id, connector_id, space_id, scope_id, job_id = (
        uuid4() for _ in range(5)
    )
    session.execute(
        text("INSERT INTO organizations (id,name,slug) VALUES (:id,:name,:slug)"),
        {"id": organization_id, "name": label, "slug": f"org-{organization_id}"},
    )
    session.execute(
        text(
            """INSERT INTO connectors
               (id,organization_id,connector_type,display_name,slug,status)
               VALUES (:id,:org,'github',:name,:slug,'active')"""
        ),
        {
            "id": connector_id,
            "org": organization_id,
            "name": label,
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
            "name": label,
            "slug": f"space-{space_id}",
        },
    )
    session.execute(
        text(
            """INSERT INTO connector_scopes
               (id,organization_id,connector_id,knowledge_space_id,display_name,slug,
                scope_type,external_scope_key,access_mode,status)
               VALUES (:id,:org,:connector,:space,:name,:slug,'repository',:key,
                       'platform_managed','active')"""
        ),
        {
            "id": scope_id,
            "org": organization_id,
            "connector": connector_id,
            "space": space_id,
            "name": label,
            "slug": f"scope-{scope_id}",
            "key": f"github:repository:{scope_id.int}",
        },
    )
    session.execute(
        text(
            """INSERT INTO connector_sync_jobs
               (id,organization_id,connector_id,connector_scope_id,mode,trigger_type,status)
               VALUES (:id,:org,:connector,:scope,'incremental','manual','queued')"""
        ),
        {
            "id": job_id,
            "org": organization_id,
            "connector": connector_id,
            "scope": scope_id,
        },
    )
    session.commit()
    return organization_id, connector_id, scope_id, job_id


def _generation_request(context: tuple[UUID, UUID, UUID, UUID]):
    organization_id, connector_id, scope_id, job_id = context
    return RepositoryGenerationRegistration(
        organization_id=organization_id,
        connector_id=connector_id,
        connector_scope_id=scope_id,
        sync_job_id=job_id,
        provider_key="github",
        repository_identity="github:repository:123456",
        branch_name="main",
        commit_object_id="a" * 40,
        root_tree_object_id="b" * 40,
        profile_fingerprint=PROFILE,
        created_at=NOW,
    )


def _entry(index: int, *, max_attempts: int = 3) -> FileWorkManifestEntry:
    path = f"documents/file-{index:06d}.md"
    return FileWorkManifestEntry(
        source_item_key=f"github:file:{path}",
        repository_path=path,
        provider_blob_id=f"{index:040x}",
        provider_revision_id="a" * 40,
        profile_fingerprint=PROFILE,
        file_size_bytes=100 + index,
        file_extension=".md",
        mime_type="text/markdown",
        max_attempts=max_attempts,
    )


def _generation(session: Session, label: str = "Ledger"):
    context = _setup(session, label)
    generation, created = ConnectorSyncWorkLedgerRepository(session).register_generation(
        _generation_request(context)
    )
    session.commit()
    assert created
    return context, generation


def _register(
    session: Session, organization_id: UUID, generation_id: UUID, entries
):
    result = ConnectorSyncWorkLedgerRepository(session).register_manifest(
        organization_id, generation_id, entries, now=NOW
    )
    session.commit()
    return result


def test_generation_and_duplicate_manifest_registration_are_idempotent(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context = _setup(session, "Idempotent")
        repository = ConnectorSyncWorkLedgerRepository(session)
        first, created = repository.register_generation(_generation_request(context))
        session.commit()
        replay, replay_created = repository.register_generation(
            _generation_request(context)
        )
        assert created is True
        assert replay_created is False
        assert replay == first

        initial = repository.register_manifest(
            context[0], first.generation_id, [_entry(1), _entry(2)], now=NOW
        )
        replayed = repository.register_manifest(
            context[0], first.generation_id, [_entry(1), _entry(2)], now=NOW
        )
        session.commit()

        assert (initial.created_count, initial.existing_count) == (2, 0)
        assert (replayed.created_count, replayed.existing_count) == (0, 2)
        assert replayed.work_item_ids == initial.work_item_ids
        persisted = repository.get_generation(context[0], first.generation_id)
        assert persisted is not None
        assert (persisted.items_discovered, persisted.items_registered) == (2, 2)
        with pytest.raises(SyncWorkLedgerConflict):
            repository.register_manifest(
                context[0],
                first.generation_id,
                [replace(_entry(1), mime_type="text/plain")],
                now=NOW,
            )
        session.rollback()
        with pytest.raises(InvalidSyncWorkLedgerRequest):
            repository.register_manifest(
                context[0], first.generation_id,
                [_entry(index) for index in range(501)], now=NOW,
            )
        session.rollback()


def test_tenant_qualified_foreign_keys_uniqueness_and_reads_fail_closed(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "TenantOne")
        other = _setup(session, "TenantTwo")
        item = _register(session, context[0], generation.generation_id, [_entry(1)])
        repository = ConnectorSyncWorkLedgerRepository(session)

        assert repository.get_generation(other[0], generation.generation_id) is None
        assert (
            repository.get_work_item(
                other[0], generation.generation_id, item.work_item_ids[0]
            )
            is None
        )
        assert (
            repository.claim_next(
                other[0], generation.generation_id,
                worker_id="foreign-worker", now=NOW, lease_duration=LEASE,
            )
            is None
        )
        with pytest.raises(SyncWorkLedgerNotFound):
            repository.register_generation(
                replace(_generation_request(other), sync_job_id=context[3])
            )
        session.rollback()


def test_thirty_two_workers_claim_unique_items_with_skip_locked(engine) -> None:
    with Session(engine, expire_on_commit=False) as setup:
        context, generation = _generation(setup, "Concurrency")
        _register(
            setup,
            context[0],
            generation.generation_id,
            [_entry(index) for index in range(64)],
        )

    start = threading.Barrier(32)
    leases: list[object] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def claim(index: int) -> None:
        with Session(engine, expire_on_commit=False) as worker:
            try:
                start.wait(timeout=20)
                lease = ConnectorSyncWorkLedgerRepository(worker).claim_next(
                    context[0], generation.generation_id,
                    worker_id=f"worker-{index:02d}", now=NOW, lease_duration=LEASE,
                )
                worker.commit()
                with lock:
                    leases.append(lease)
            except BaseException as exc:  # pragma: no cover - asserted below
                worker.rollback()
                with lock:
                    errors.append(exc)

    threads = [threading.Thread(target=claim, args=(index,)) for index in range(32)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert len(leases) == 32
    assert all(lease is not None for lease in leases)
    assert len({lease.work_item_id for lease in leases if lease is not None}) == 32
    assert len({lease.lease_id for lease in leases if lease is not None}) == 32


def test_heartbeat_expiry_recovery_and_stale_fence_rejection(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "Recovery")
        _register(session, context[0], generation.generation_id, [_entry(1)])
        repository = ConnectorSyncWorkLedgerRepository(session)
        first = repository.claim_next(
            context[0], generation.generation_id,
            worker_id="worker-one", now=NOW, lease_duration=LEASE,
        )
        assert first is not None
        session.commit()
        assert (
            repository.claim_next(
                context[0], generation.generation_id,
                worker_id="worker-two", now=NOW, lease_duration=LEASE,
            )
            is None
        )
        session.rollback()
        extended = repository.heartbeat(
            first,
            worker_id="worker-one",
            now=NOW + timedelta(minutes=1),
            lease_duration=LEASE,
        )
        assert extended.lease_expires_at == NOW + timedelta(minutes=6)
        session.commit()
        with pytest.raises(LostFileWorkLease):
            repository.heartbeat(
                replace(extended, lease_id=uuid4()),
                worker_id="worker-one",
                now=NOW + timedelta(minutes=2),
                lease_duration=LEASE,
            )
        session.rollback()

        recovered = repository.recover_expired(
            context[0], generation.generation_id,
            now=NOW + timedelta(minutes=7), limit=10,
        )
        session.commit()
        assert len(recovered) == 1
        assert recovered[0].status is FileWorkStatus.RETRY_WAIT
        with pytest.raises(LostFileWorkLease):
            repository.complete(
                first,
                worker_id="worker-one",
                outcome=FileWorkStatus.SUCCEEDED,
                counters=FileWorkCounters(),
                now=NOW + timedelta(minutes=7),
            )
        session.rollback()

        second = repository.claim_next(
            context[0], generation.generation_id,
            worker_id="worker-two", now=NOW + timedelta(minutes=7), lease_duration=LEASE,
        )
        assert second is not None
        assert second.fencing_token == first.fencing_token + 1
        session.commit()
        with pytest.raises(StaleFileWorkFence):
            repository.complete(
                first,
                worker_id="worker-one",
                outcome=FileWorkStatus.SUCCEEDED,
                counters=FileWorkCounters(),
                now=NOW + timedelta(minutes=8),
            )
        session.rollback()


def test_retry_max_attempt_quarantine_and_barrier_semantics(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "Barrier")
        result = _register(
            session,
            context[0],
            generation.generation_id,
            [_entry(1, max_attempts=2), _entry(2), _entry(3)],
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        assert repository.barrier_summary(context[0], generation.generation_id).barrier_open is False

        leases = []
        for index in range(3):
            lease = repository.claim_next(
                context[0], generation.generation_id,
                worker_id=f"barrier-worker-{index}", now=NOW, lease_duration=LEASE,
            )
            assert lease is not None
            leases.append(lease)
        session.commit()
        retry_lease = next(lease for lease in leases if lease.max_attempts == 2)
        success_lease, quarantine_lease = (
            lease for lease in leases if lease.work_item_id != retry_lease.work_item_id
        )

        retry = repository.record_failure(
            retry_lease, worker_id=retry_lease.worker_id,
            error_category="rate_limit", error_code="provider_throttled",
            retry_at=NOW + timedelta(minutes=10), now=NOW,
        )
        repository.complete(
            success_lease, worker_id=success_lease.worker_id,
            outcome=FileWorkStatus.SUCCEEDED,
            counters=FileWorkCounters(200, 1000, 2, 1), now=NOW,
        )
        quarantined = repository.record_failure(
            quarantine_lease, worker_id=quarantine_lease.worker_id,
            error_category="extraction", error_code="unsupported_payload",
            quarantine_reason_code="unsupported_payload", now=NOW,
        )
        session.commit()
        assert retry.status is FileWorkStatus.RETRY_WAIT
        assert quarantined.status is FileWorkStatus.QUARANTINED

        repository.mark_discovery_complete(context[0], generation.generation_id, now=NOW)
        assert repository.barrier_summary(context[0], generation.generation_id).barrier_open is False
        final_lease = repository.claim_next(
            context[0], generation.generation_id,
            worker_id="retry-worker", now=NOW + timedelta(minutes=10), lease_duration=LEASE,
        )
        assert final_lease is not None
        terminal = repository.record_failure(
            final_lease, worker_id="retry-worker",
            error_category="source_read", error_code="provider_unavailable",
            retry_at=NOW + timedelta(minutes=20), now=NOW + timedelta(minutes=10),
        )
        session.commit()
        assert terminal.status is FileWorkStatus.FAILED

        barrier = repository.barrier_summary(context[0], generation.generation_id)
        assert barrier.barrier_open is True
        assert (barrier.succeeded_items, barrier.quarantined_items, barrier.failed_items) == (
            1, 1, 1
        )
        assert barrier.nonterminal_items == 0
        assert set(result.work_item_ids)


def test_cancellation_and_cross_tenant_or_tampered_lease_mutations_fail_closed(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "Cancellation")
        other = _setup(session, "ForeignCancellation")
        result = _register(
            session, context[0], generation.generation_id, [_entry(1), _entry(2)]
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        running = repository.claim_next(
            context[0], generation.generation_id,
            worker_id="cancellation-worker", now=NOW, lease_duration=LEASE,
        )
        assert running is not None
        pending_id = next(item for item in result.work_item_ids if item != running.work_item_id)
        with pytest.raises(SyncWorkLedgerNotFound):
            repository.request_cancellation(
                other[0], generation.generation_id, pending_id,
                reason_code="operator_request", now=NOW,
            )
        cancelled = repository.request_cancellation(
            context[0], generation.generation_id, pending_id,
            reason_code="operator_request", now=NOW,
        )
        requested = repository.request_cancellation(
            context[0], generation.generation_id, running.work_item_id,
            reason_code="operator_request", now=NOW,
        )
        session.commit()
        assert cancelled.status is FileWorkStatus.CANCELLED
        assert requested.status is FileWorkStatus.RUNNING
        assert requested.cancellation_requested

        with pytest.raises(FileWorkCancellationConflict):
            repository.complete(
                running,
                worker_id="cancellation-worker",
                outcome=FileWorkStatus.SUCCEEDED,
                counters=FileWorkCounters(), now=NOW + timedelta(seconds=1),
            )
        session.rollback()
        acknowledged = repository.acknowledge_cancellation(
            running,
            worker_id="cancellation-worker",
            now=NOW + timedelta(seconds=1),
        )
        session.commit()
        assert acknowledged.status is FileWorkStatus.CANCELLED

        with pytest.raises(LostFileWorkLease):
            repository.heartbeat(
                replace(running, organization_id=other[0]),
                worker_id="cancellation-worker",
                now=NOW + timedelta(seconds=2), lease_duration=LEASE,
            )
        session.rollback()


def test_discovery_empty_barrier_and_durable_follow_up_intent(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "EmptyBarrier")
        repository = ConnectorSyncWorkLedgerRepository(session)
        assert repository.barrier_summary(context[0], generation.generation_id).barrier_open is False
        completed = repository.mark_discovery_complete(
            context[0], generation.generation_id, now=NOW
        )
        follow_up = repository.require_follow_up(
            context[0], generation.generation_id, now=NOW + timedelta(seconds=1)
        )
        replay = repository.require_follow_up(
            context[0], generation.generation_id, now=NOW + timedelta(seconds=2)
        )
        session.commit()

        assert completed.discovery_complete
        assert repository.barrier_summary(context[0], generation.generation_id).barrier_open
        persisted = repository.get_generation(context[0], generation.generation_id)
        assert persisted is not None
        assert persisted.reconciliation_eligible is False
        assert follow_up.resync_required
        assert replay.resync_requested_at == follow_up.resync_requested_at


def test_claim_and_barrier_queries_use_dedicated_indexes(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "Plans")
        _register(session, context[0], generation.generation_id, [_entry(1), _entry(2)])
        session.execute(text("SET LOCAL enable_seqscan = off"))
        claim_plan = "\n".join(
            row[0]
            for row in session.execute(
                text(
                    """EXPLAIN (COSTS OFF)
                       SELECT id FROM connector_sync_file_work_items
                       WHERE organization_id=:org AND generation_id=:generation
                         AND status IN ('pending','retry_wait') AND next_attempt_at <= :now
                       ORDER BY next_attempt_at,id LIMIT 1 FOR UPDATE SKIP LOCKED"""
                ),
                {"org": context[0], "generation": generation.generation_id, "now": NOW},
            )
        )
        barrier_plan = "\n".join(
            row[0]
            for row in session.execute(
                text(
                    """EXPLAIN (COSTS OFF)
                       SELECT status,count(*) FROM connector_sync_file_work_items
                       WHERE organization_id=:org AND generation_id=:generation
                       GROUP BY status"""
                ),
                {"org": context[0], "generation": generation.generation_id},
            )
        )
        assert "ix_sync_file_work_claimable" in claim_plan
        assert "ix_sync_file_work_generation_barrier" in barrier_plan


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1)]


def _peak_process_memory_bytes() -> int:
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        current_process = ctypes.windll.kernel32.GetCurrentProcess
        current_process.restype = wintypes.HANDLE
        get_memory = ctypes.windll.psapi.GetProcessMemoryInfo
        get_memory.argtypes = (
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCounters),
            wintypes.DWORD,
        )
        get_memory.restype = wintypes.BOOL
        if not get_memory(
            current_process(), ctypes.byref(counters), counters.cb
        ):
            raise OSError("process memory measurement failed")
        return int(counters.PeakWorkingSetSize)
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak * (1 if os.uname().sysname == "Darwin" else 1024))


def test_ten_thousand_item_ledger_benchmark_is_bounded_and_unique(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "Benchmark")
        memory_before = _peak_process_memory_bytes()
        started = time.perf_counter()
        for start in range(0, 10_000, 500):
            result = ConnectorSyncWorkLedgerRepository(session).register_manifest(
                context[0],
                generation.generation_id,
                [_entry(index) for index in range(start, start + 500)],
                now=NOW,
            )
            assert result.created_count == 500
            session.commit()
        insert_seconds = time.perf_counter() - started

        total, distinct_id, distinct_identity = session.execute(
            select(
                func.count(),
                func.count(func.distinct(ConnectorSyncFileWorkItem.id)),
                func.count(func.distinct(ConnectorSyncFileWorkItem.source_key_hash)),
            ).where(ConnectorSyncFileWorkItem.generation_id == generation.generation_id)
        ).one()
        assert (total, distinct_id, distinct_identity) == (10_000, 10_000, 10_000)

        claim_latencies = []
        for index in range(128):
            claim_started = time.perf_counter()
            lease = ConnectorSyncWorkLedgerRepository(session).claim_next(
                context[0], generation.generation_id,
                worker_id=f"benchmark-{index}", now=NOW, lease_duration=LEASE,
            )
            session.commit()
            claim_latencies.append((time.perf_counter() - claim_started) * 1000)
            assert lease is not None

        barrier_latencies = []
        for _ in range(50):
            barrier_started = time.perf_counter()
            summary = ConnectorSyncWorkLedgerRepository(session).barrier_summary(
                context[0], generation.generation_id
            )
            barrier_latencies.append((time.perf_counter() - barrier_started) * 1000)
            assert summary.total_items == 10_000

        table_bytes, index_bytes = session.execute(
            text(
                """SELECT pg_relation_size('connector_sync_file_work_items'),
                          pg_indexes_size('connector_sync_file_work_items')"""
            )
        ).one()
        memory_peak = _peak_process_memory_bytes()
        metrics = {
            "items": 10_000,
            "insert_items_per_second": round(10_000 / insert_seconds, 2),
            "claim_p50_ms": round(statistics.median(claim_latencies), 3),
            "claim_p95_ms": round(_percentile(claim_latencies, 0.95), 3),
            "barrier_p50_ms": round(statistics.median(barrier_latencies), 3),
            "barrier_p95_ms": round(_percentile(barrier_latencies, 0.95), 3),
            "peak_process_memory_bytes": memory_peak,
            "peak_process_memory_growth_bytes": max(0, memory_peak - memory_before),
            "table_bytes": int(table_bytes),
            "index_bytes": int(index_bytes),
        }
        print(f"sync_work_ledger_benchmark={json.dumps(metrics, sort_keys=True)}")
        assert metrics["peak_process_memory_growth_bytes"] < 512 * 1024 * 1024
