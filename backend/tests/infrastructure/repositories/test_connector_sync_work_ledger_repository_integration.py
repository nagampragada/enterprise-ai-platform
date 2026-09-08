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
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from application.services.github_repository_content_service import (
    GitHubRepositoryContentAuthorization,
    GitHubRepositoryEntry,
    GitHubRepositorySnapshot,
)
from application.services.github_sync_work_planning_service import (
    GitHubSyncWorkPlanningService,
)
from domain.connectors.sync_work_ledger import (
    FileWorkCounters,
    FileWorkManifestEntry,
    FileWorkMaterialization,
    FileWorkMaterializationChunk,
    FileWorkStatus,
    GenerationObservationDisposition,
    GenerationCitationProjectionProfile,
    GenerationPromotionRequest,
    GenerationReconciliationRequest,
    GenerationSourceObservation,
    RepositoryGenerationRegistration,
)
from infrastructure.db.models import (
    ConnectorSyncFileMaterialization,
    ConnectorSyncFileMaterializationChunk,
    ConnectorSyncFileWorkItem,
    ConnectorSyncGeneration,
    ConnectorSyncGenerationActivation,
    ConnectorSyncGenerationObservation,
    ConnectorSyncOrganizationClaimSchedule,
    ConnectorScope,
    Document,
    DocumentChunk,
    DocumentIndexingState,
    DocumentVersion,
    SourceItem,
    SourceItemScopeMembership,
    Organization,
)
from infrastructure.repositories.connector_sync_job_repository import (
    ConnectorSyncJobRepository,
)
from infrastructure.repositories.connector_scope_repository import ConnectorScopeRepository
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    ConnectorSyncWorkLedgerRepository,
    FileWorkCancellationConflict,
    InvalidSyncWorkLedgerRequest,
    LostFileWorkLease,
    StaleFileWorkFence,
    SyncWorkLedgerConflict,
    SyncWorkLedgerNotFound,
    SyncWorkLedgerPersistenceError,
)
from infrastructure.repositories.permission_aware_document_chunk_search_repository import (
    PermissionAwareDocumentChunkSearchRepository,
)
from infrastructure.repositories.source_item_repository import SourceItemRepository


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


def _github_planning_context(context: tuple[UUID, UUID, UUID, UUID]):
    organization_id, connector_id, scope_id, job_id = context
    repository_id = 123456
    repository_identity = f"github:repository:{repository_id}"
    authorization = GitHubRepositoryContentAuthorization(
        organization_id,
        connector_id,
        scope_id,
        uuid4(),
        uuid4(),
        101,
        202,
        303,
        "sandbox-org",
        repository_id,
        "repository",
        "sandbox-org/repository",
        "sandbox-org",
        repository_identity,
        "main",
    )
    snapshot = GitHubRepositorySnapshot(
        connector_id,
        scope_id,
        repository_id,
        repository_identity,
        "main",
        "a" * 40,
        "b" * 40,
    )
    return organization_id, connector_id, scope_id, job_id, authorization, snapshot


def _github_entry(snapshot: GitHubRepositorySnapshot, index: int):
    path = f"documents/file-{index:06d}.md"
    return GitHubRepositoryEntry(
        snapshot.connector_id,
        snapshot.scope_id,
        snapshot.repository_id,
        snapshot.canonical_repository_identity,
        snapshot.commit_object_id,
        snapshot.root_tree_object_id,
        snapshot.root_tree_object_id,
        path.rsplit("/", 1)[-1],
        path,
        "regular_blob",
        f"{index:040x}",
        100 + index,
        False,
    )


def _plan(service, context, entries):
    organization_id, connector_id, scope_id, job_id, authorization, snapshot = context
    return service.register_manifest_batch(
        organization_id=organization_id,
        connector_id=connector_id,
        connector_scope_id=scope_id,
        sync_job_id=job_id,
        authorization=authorization,
        snapshot=snapshot,
        profile_fingerprint=PROFILE,
        entries=entries,
        now=NOW,
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
    entries = tuple(entries)
    observations = tuple(
        GenerationSourceObservation(
            entry.source_item_key,
            entry.repository_path,
            entry.provider_blob_id,
            entry.provider_revision_id,
            entry.profile_fingerprint,
            "regular_blob",
            GenerationObservationDisposition.ELIGIBLE,
            entry.file_size_bytes,
        )
        for entry in entries
    )
    result = ConnectorSyncWorkLedgerRepository(session).register_discovery_batch(
        organization_id, generation_id, observations, entries, now=NOW
    )
    session.commit()
    return result


def _materialization(generation, work, *, text_value="alpha"):
    content_hash = "d" * 64
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
        "File",
        work.mime_type,
        "fake:model:1536",
        (
            FileWorkMaterializationChunk(
                0,
                text_value,
                content_hash,
                (1.0,) * 1536,
                "fake:model:1536",
            ),
        ),
    )


def _promotion_request(generation) -> GenerationPromotionRequest:
    return GenerationPromotionRequest(
        generation.organization_id,
        generation.connector_id,
        generation.connector_scope_id,
        generation.generation_id,
        generation.sync_job_id,
        generation.provider_key,
        generation.repository_identity,
        generation.branch_name,
        generation.commit_object_id,
        generation.root_tree_object_id,
        generation.profile_fingerprint,
    )


def _projection_profile(generation) -> GenerationCitationProjectionProfile:
    return GenerationCitationProjectionProfile(
        "github",
        "v1",
        "deterministic",
        "v2",
        "test",
        "fake:model:1536",
        1536,
        generation.profile_fingerprint,
    )


def _reconciliation_request(generation) -> GenerationReconciliationRequest:
    return GenerationReconciliationRequest(
        generation.organization_id,
        generation.connector_id,
        generation.connector_scope_id,
        generation.generation_id,
        generation.sync_job_id,
        generation.provider_key,
        generation.repository_identity,
        generation.branch_name,
        generation.commit_object_id,
        generation.root_tree_object_id,
        generation.profile_fingerprint,
    )


def _add_absent_legacy_source(
    session: Session,
    context,
    generation,
    *,
    path: str,
    additional_scope_id: UUID | None = None,
):
    source_id, version_id, document_id, chunk_id = (uuid4() for _ in range(4))
    source_key = f"{generation.repository_identity}:path:{path}"
    session.add(
        SourceItem(
            id=source_id,
            organization_id=context[0],
            connector_id=context[1],
            source_item_key=source_key,
            parent_source_item_key=None,
            source_item_type="file",
            title=path.rsplit("/", 1)[-1],
            source_url=None,
            mime_type="text/markdown",
            source_checksum="e" * 64,
            source_version="f" * 40,
            size_bytes=10,
            source_created_at=None,
            source_modified_at=None,
            first_seen_at=NOW,
            last_seen_at=NOW,
            status="active",
            deleted_at=None,
            source_metadata={
                "provider": "github",
                "repository_identity": generation.repository_identity,
                "repository_path": path,
                "blob_object_id": "f" * 40,
                "snapshot_commit_id": "0" * 40,
            },
            metadata_schema_version=1,
        )
    )
    for scope_id in (context[2], additional_scope_id):
        if scope_id is not None:
            session.add(
                SourceItemScopeMembership(
                    id=uuid4(),
                    organization_id=context[0],
                    connector_id=context[1],
                    source_item_id=source_id,
                    connector_scope_id=scope_id,
                    status="active",
                    first_discovered_at=NOW,
                    last_seen_at=NOW,
                    removed_at=None,
                )
            )
    # These fixtures intentionally use independent ORM objects rather than
    # relationships.  Flush the tenant-qualified source graph before adding
    # its version so PostgreSQL, rather than ORM insertion ordering, remains
    # the authoritative foreign-key check.
    session.flush()
    session.add(
        DocumentVersion(
            id=version_id,
            organization_id=context[0],
            connector_id=context[1],
            source_item_id=source_id,
            version_number=1,
            provider_version_id="f" * 40,
            content_checksum="e" * 64,
            checksum_algorithm="sha256",
            source_modified_at=None,
            source_size_bytes=10,
            content_type="text/markdown",
            file_extension=".md",
            version_cause="discovered",
            lifecycle="available",
            is_current=True,
            discovered_at=NOW,
            version_metadata={},
            metadata_schema_version=1,
        )
    )
    session.add(
        Document(
            id=document_id,
            organization_id=context[0],
            source_type="github",
            source_document_key=source_key,
            title=path,
            source_url=None,
            mime_type="text/markdown",
            checksum_latest="e" * 64,
            status="ready",
            source_created_at=None,
            source_updated_at=None,
            deleted_at=None,
        )
    )
    session.flush()
    session.execute(
        text(
            "INSERT INTO document_version_documents "
            "(id,organization_id,document_version_id,document_id) "
            "VALUES (:id,:org,:version,:document)"
        ),
        {
            "id": uuid4(),
            "org": context[0],
            "version": version_id,
            "document": document_id,
        },
    )
    session.add(
        DocumentIndexingState(
            id=uuid4(),
            organization_id=context[0],
            document_version_id=version_id,
            extraction_profile="github",
            extraction_version="v1",
            chunking_profile="deterministic",
            chunking_version="v2",
            embedding_provider="test",
            embedding_model="fake:model:1536",
            embedding_dimensions=1536,
            profile_fingerprint=generation.profile_fingerprint,
            desired_generation=1,
            indexed_generation=1,
            status="indexed",
            reason="new_version",
            attempt_count=1,
            requested_at=NOW,
            started_at=NOW,
            completed_at=NOW,
        )
    )
    session.add(
        DocumentChunk(
            id=chunk_id,
            organization_id=context[0],
            document_id=document_id,
            chunk_index=0,
            chunk_text="legacy absent chunk",
            content_hash="9" * 64,
            token_count=None,
            embedding=[0.0] * 1536,
            embedding_model="fake:model:1536",
        )
    )
    session.commit()
    return source_id, version_id, document_id, chunk_id


def _retrieval_user(session: Session, organization_id: UUID, scope_id: UUID) -> UUID:
    user_id = uuid4()
    knowledge_space_id = session.scalar(
        select(ConnectorScope.knowledge_space_id).where(ConnectorScope.id == scope_id)
    )
    assert knowledge_space_id is not None
    session.execute(
        text(
            "INSERT INTO users "
            "(id,organization_id,email,normalized_email,password_hash,display_name) "
            "VALUES (:id,:org,:email,:email,'hash','Reconciliation Reader')"
        ),
        {
            "id": user_id,
            "org": organization_id,
            "email": f"{user_id}@example.test",
        },
    )
    session.execute(
        text(
            "INSERT INTO knowledge_space_user_grants "
            "(id,organization_id,knowledge_space_id,user_id,permission_level,granted_at) "
            "VALUES (:id,:org,:space,:user,'viewer',:now)"
        ),
        {
            "id": uuid4(),
            "org": organization_id,
            "space": knowledge_space_id,
            "user": user_id,
            "now": NOW,
        },
    )
    session.commit()
    return user_id


def _retrieval_chunk_ids(
    session: Session, organization_id: UUID, user_id: UUID
) -> set[UUID]:
    rows = PermissionAwareDocumentChunkSearchRepository(session).search(
        organization_id,
        user_id,
        [1.0] + [0.0] * 1535,
        "fake:model:1536",
        100,
        source_item_types=("file",),
    )
    return {row.chunk_id for row in rows}


def _ready_promotion(session: Session, label: str = "Promotion"):
    context, generation = _generation(session, label)
    _register(session, context[0], generation.generation_id, (_entry(1),))
    repository = ConnectorSyncWorkLedgerRepository(session)
    repository.mark_discovery_complete(context[0], generation.generation_id, now=NOW)
    session.commit()
    lease = repository.claim_next(
        context[0], generation.generation_id,
        worker_id="promotion-worker", now=NOW, lease_duration=LEASE,
    )
    session.commit()
    assert lease is not None
    generation = repository.get_generation(context[0], generation.generation_id)
    work = repository.get_work_item(context[0], generation.generation_id, lease.work_item_id)
    assert generation is not None and work is not None
    materialization = _materialization(generation, work)
    repository.stage_materialization_and_complete(
        lease,
        worker_id="promotion-worker",
        generation=generation,
        work_item=work,
        materialization=materialization,
        counters=FileWorkCounters(5, 5, 1, 1),
        now=NOW,
    )
    source_id, version_id, document_id = uuid4(), uuid4(), uuid4()
    session.execute(
        text("UPDATE connector_scopes SET external_scope_key=:key WHERE id=:scope"),
        {"key": generation.repository_identity, "scope": context[2]},
    )
    session.execute(text("""INSERT INTO source_items
        (id,organization_id,connector_id,source_item_key,source_item_type,title,
         mime_type,source_checksum,source_version,size_bytes,first_seen_at,last_seen_at,
         status,metadata)
        VALUES (:id,:org,:connector,:key,'file','File',:mime,:checksum,:blob,101,
                :now,:now,'active',CAST(:metadata AS jsonb))"""), {
        "id": source_id, "org": context[0], "connector": context[1],
        "key": work.source_item_key, "mime": work.mime_type,
        "checksum": materialization.content_checksum, "blob": work.provider_blob_id,
        "now": NOW, "metadata": json.dumps({
            "provider": "github", "repository_identity": generation.repository_identity,
            "repository_path": work.repository_path, "blob_object_id": work.provider_blob_id,
            "snapshot_commit_id": generation.commit_object_id,
        }),
    })
    session.execute(text("""INSERT INTO source_item_scope_memberships
        (id,organization_id,connector_id,source_item_id,connector_scope_id,status,
         first_discovered_at,last_seen_at)
        VALUES (:id,:org,:connector,:source,:scope,'active',:now,:now)"""), {
        "id": uuid4(), "org": context[0], "connector": context[1], "source": source_id,
        "scope": context[2], "now": NOW,
    })
    session.execute(text("""INSERT INTO document_versions
        (id,organization_id,connector_id,source_item_id,version_number,provider_version_id,
         content_checksum,checksum_algorithm,source_size_bytes,content_type,file_extension,
         version_cause,lifecycle,is_current,discovered_at,metadata)
        VALUES (:id,:org,:connector,:source,1,:blob,:checksum,'sha256',101,:mime,'.md',
                'discovered','available',true,:now,CAST(:metadata AS jsonb))"""), {
        "id": version_id, "org": context[0], "connector": context[1], "source": source_id,
        "blob": work.provider_blob_id, "checksum": materialization.content_checksum,
        "mime": work.mime_type, "now": NOW,
        "metadata": json.dumps({
            "provider": "github",
            "commit_object_id": generation.commit_object_id,
            "blob_object_id": work.provider_blob_id,
        }),
    })
    session.execute(text("""INSERT INTO documents
        (id,organization_id,source_type,source_document_key,title,mime_type,checksum_latest,status)
        VALUES (:id,:org,'github',:key,'File',:mime,:checksum,'ready')"""), {
        "id": document_id, "org": context[0], "key": work.source_item_key,
        "mime": work.mime_type, "checksum": materialization.content_checksum,
    })
    session.execute(text("""INSERT INTO document_version_documents
        (id,organization_id,document_version_id,document_id)
        VALUES (:id,:org,:version,:document)"""), {
        "id": uuid4(), "org": context[0], "version": version_id, "document": document_id,
    })
    session.execute(text("""INSERT INTO document_indexing_states
        (id,organization_id,document_version_id,extraction_profile,extraction_version,
         chunking_profile,chunking_version,embedding_provider,embedding_model,
         embedding_dimensions,profile_fingerprint,desired_generation,indexed_generation,
         status,reason,attempt_count,requested_at,started_at,completed_at)
        VALUES (:id,:org,:version,'github','v1','deterministic','v2','test',:model,1536,
                :profile,1,1,'indexed','new_version',1,:now,:now,:now)"""), {
        "id": uuid4(), "org": context[0], "version": version_id,
        "model": materialization.embedding_model, "profile": generation.profile_fingerprint,
        "now": NOW,
    })
    session.execute(text("""UPDATE connector_sync_jobs
        SET status='succeeded',attempt_count=1,fencing_token=1,next_attempt_at=NULL,
            completed_at=created_at WHERE id=:job"""), {"job": context[3]})
    session.commit()
    return context, generation, work, source_id, version_id, document_id


def _new_generation_same_scope(session: Session, context, *, created_at, discovered=0):
    job_id, generation_id = uuid4(), uuid4()
    session.execute(text("""INSERT INTO connector_sync_jobs
        (id,organization_id,connector_id,connector_scope_id,mode,trigger_type,status,
         attempt_count,fencing_token,next_attempt_at,completed_at,created_at,updated_at)
        VALUES (:id,:org,:connector,:scope,'incremental','manual','succeeded',1,1,NULL,
                :now,:now,:now)"""), {
        "id": job_id, "org": context[0], "connector": context[1],
        "scope": context[2], "now": created_at,
    })
    session.execute(text("""INSERT INTO connector_sync_generations
        (id,organization_id,connector_id,connector_scope_id,sync_job_id,provider_key,
         repository_identity,branch_name,commit_object_id,root_tree_object_id,
         profile_fingerprint,status,discovery_complete,discovery_completed_at,
         reconciliation_eligible,resync_required,items_discovered,items_registered,
         declared_bytes,created_at,updated_at)
        VALUES (:id,:org,:connector,:scope,:job,'github','github:repository:123456',
                'main',:commit,:tree,:profile,'processing',true,:now,false,false,
                :count,:count,0,:now,:now)"""), {
        "id": generation_id, "org": context[0], "connector": context[1],
        "scope": context[2], "job": job_id, "commit": "f" * 40,
        "tree": "e" * 40, "profile": PROFILE, "count": discovered, "now": created_at,
    })
    session.commit()
    row = ConnectorSyncWorkLedgerRepository(session).get_generation(context[0], generation_id)
    assert row is not None
    return row


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


def test_discovery_batch_failure_cannot_leave_partial_observations(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "AtomicDiscovery")
        first = _entry(1)
        first_result = _register(
            session, context[0], generation.generation_id, (first,)
        )
        second = _entry(2)
        second_observation = GenerationSourceObservation(
            second.source_item_key,
            second.repository_path,
            second.provider_blob_id,
            second.provider_revision_id,
            second.profile_fingerprint,
            "regular_blob",
            GenerationObservationDisposition.ELIGIBLE,
            second.file_size_bytes,
        )
        repository = ConnectorSyncWorkLedgerRepository(
            session,
            work_item_id_factory=lambda: first_result.work_item_ids[0],
        )
        with pytest.raises(SyncWorkLedgerPersistenceError, match="manifest registration"):
            repository.register_discovery_batch(
                context[0],
                generation.generation_id,
                (second_observation,),
                (second,),
                now=NOW,
            )
        session.rollback()
        assert session.scalar(
            select(func.count(ConnectorSyncGenerationObservation.id)).where(
                ConnectorSyncGenerationObservation.generation_id
                == generation.generation_id
            )
        ) == 1
        persisted = repository.get_generation(context[0], generation.generation_id)
        assert persisted is not None
        assert (persisted.items_discovered, persisted.items_registered) == (1, 1)


def test_completed_discovery_rejects_new_observation_before_any_write(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "ImmutableDiscovery")
        first = _entry(1)
        first_result = _register(
            session, context[0], generation.generation_id, (first,)
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.mark_discovery_complete(
            context[0], generation.generation_id, now=NOW
        )
        session.commit()

        replay = repository.register_discovery_batch(
            context[0],
            generation.generation_id,
            (
                GenerationSourceObservation(
                    first.source_item_key,
                    first.repository_path,
                    first.provider_blob_id,
                    first.provider_revision_id,
                    first.profile_fingerprint,
                    "regular_blob",
                    GenerationObservationDisposition.ELIGIBLE,
                    first.file_size_bytes,
                ),
            ),
            (first,),
            now=NOW,
        )
        assert replay.created_observation_count == replay.created_work_count == 0
        assert replay.observation_ids == first_result.observation_ids

        second = _entry(2)
        with pytest.raises(
            SyncWorkLedgerConflict,
            match="completed discovery cannot accept new observations",
        ):
            repository.register_discovery_batch(
                context[0],
                generation.generation_id,
                (
                    GenerationSourceObservation(
                        second.source_item_key,
                        second.repository_path,
                        second.provider_blob_id,
                        second.provider_revision_id,
                        second.profile_fingerprint,
                        "regular_blob",
                        GenerationObservationDisposition.ELIGIBLE,
                        second.file_size_bytes,
                    ),
                ),
                (second,),
                now=NOW,
            )
        # Even an exception-catching caller cannot commit the rejected mutation.
        session.commit()
        assert session.scalar(
            select(func.count(ConnectorSyncGenerationObservation.id)).where(
                ConnectorSyncGenerationObservation.generation_id
                == generation.generation_id
            )
        ) == 1
        assert session.scalar(
            select(func.count(ConnectorSyncFileWorkItem.id)).where(
                ConnectorSyncFileWorkItem.generation_id == generation.generation_id
            )
        ) == 1


def test_cross_tenant_provider_claim_is_single_bounded_and_requires_completed_discovery(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        first_context, first_generation = _generation(session, "First")
        second_context, second_generation = _generation(session, "Second")
        _register(
            session,
            first_context[0],
            first_generation.generation_id,
            (_entry(1), _entry(2)),
        )
        _register(
            session,
            second_context[0],
            second_generation.generation_id,
            (_entry(3),),
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.mark_discovery_complete(
            second_context[0], second_generation.generation_id, now=NOW
        )
        session.commit()

        lease = repository.claim_next_available(
            provider_key="github",
            profile_fingerprint=second_generation.profile_fingerprint,
            worker_id="phase3-worker",
            now=NOW,
            lease_duration=LEASE,
        )
        session.commit()

        assert lease is not None
        assert lease.organization_id == second_context[0]
        assert lease.generation_id == second_generation.generation_id
        assert repository.get_work_item(
            lease.organization_id, lease.generation_id, lease.work_item_id
        ).status is FileWorkStatus.RUNNING
        assert repository.claim_next_available(
            provider_key="github",
            profile_fingerprint=second_generation.profile_fingerprint,
            worker_id="phase3-worker",
            now=NOW,
            lease_duration=LEASE,
        ) is None
        session.rollback()


def _make_fair_generation(session: Session, label: str, item_count: int):
    context, generation = _generation(session, label)
    _register(
        session,
        context[0],
        generation.generation_id,
        tuple(_entry(index) for index in range(item_count)),
    )
    ConnectorSyncWorkLedgerRepository(session).mark_discovery_complete(
        context[0], generation.generation_id, now=NOW
    )
    session.commit()
    return context, generation


def _fair_claim(session: Session, worker_id: str, *, now: datetime = NOW):
    lease = ConnectorSyncWorkLedgerRepository(session).claim_next_available_fair(
        provider_key="github",
        profile_fingerprint=PROFILE,
        worker_id=worker_id,
        now=now,
        lease_duration=LEASE,
    )
    session.commit()
    return lease


def _fair_schedule_state(session: Session, organization_id: UUID):
    row = session.get(ConnectorSyncOrganizationClaimSchedule, organization_id)
    if row is None:
        return None
    return (
        row.last_claim_sequence,
        row.claim_count,
        row.last_claimed_at,
        row.created_at,
        row.updated_at,
    )


def _fair_work_states(session: Session, generation_id: UUID):
    return tuple(
        session.execute(
            select(
                ConnectorSyncFileWorkItem.id,
                ConnectorSyncFileWorkItem.status,
                ConnectorSyncFileWorkItem.attempt_count,
                ConnectorSyncFileWorkItem.fencing_token,
                ConnectorSyncFileWorkItem.lease_id,
            )
            .where(ConnectorSyncFileWorkItem.generation_id == generation_id)
            .order_by(ConnectorSyncFileWorkItem.id)
        ).all()
    )


def test_fair_claim_preserves_single_organization_item_order(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _make_fair_generation(session, "FairSingle", 3)
        expected = tuple(
            session.scalars(
                select(ConnectorSyncFileWorkItem.id)
                .where(
                    ConnectorSyncFileWorkItem.organization_id == context[0],
                    ConnectorSyncFileWorkItem.generation_id == generation.generation_id,
                )
                .order_by(
                    ConnectorSyncFileWorkItem.next_attempt_at,
                    ConnectorSyncFileWorkItem.id,
                )
            ).all()
        )
        leases = tuple(_fair_claim(session, f"fair-single-{index}") for index in range(3))

        assert all(lease is not None for lease in leases)
        assert tuple(lease.work_item_id for lease in leases) == expected
        assert tuple(lease.fairness_claim_sequence for lease in leases) == tuple(
            sorted(lease.fairness_claim_sequence for lease in leases)
        )
        assert all(lease.organization_id == context[0] for lease in leases)
        schedule = session.get(ConnectorSyncOrganizationClaimSchedule, context[0])
        assert schedule is not None
        assert schedule.claim_count == 3


def test_two_organizations_alternate_and_small_backlog_does_not_starve(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        first = _generation(session, "FairBacklogA")
        second = _generation(session, "FairBacklogB")
        ordered = sorted((first, second), key=lambda value: value[0][0])
        large, small = ordered
        _register(
            session,
            large[0][0],
            large[1].generation_id,
            tuple(_entry(index) for index in range(8)),
        )
        _register(session, small[0][0], small[1].generation_id, (_entry(100),))
        repository = ConnectorSyncWorkLedgerRepository(session)
        for context, generation in (large, small):
            repository.mark_discovery_complete(
                context[0], generation.generation_id, now=NOW
            )
        session.commit()

        claims = tuple(_fair_claim(session, f"fair-skew-{index}") for index in range(4))

        assert [lease.organization_id for lease in claims] == [
            large[0][0],
            small[0][0],
            large[0][0],
            large[0][0],
        ]
        assert len({lease.work_item_id for lease in claims}) == 4


@pytest.mark.parametrize(
    ("counts", "expected_prefix"),
    (
        ((100, 1, 1), "ABC"),
        ((100, 5, 3), "ABCABCABCABAB"),
    ),
)
def test_sequential_sustained_backlog_has_exact_committed_turn_history(
    engine, counts, expected_prefix
) -> None:
    with Session(engine, expire_on_commit=False) as session:
        created = tuple(_generation(session, f"FairHistory{index}") for index in range(3))
        ordered = tuple(sorted(created, key=lambda value: value[0][0]))
        repository = ConnectorSyncWorkLedgerRepository(session)
        for (context, generation), count in zip(ordered, counts, strict=True):
            _register(
                session,
                context[0],
                generation.generation_id,
                tuple(_entry(index) for index in range(count)),
            )
            repository.mark_discovery_complete(
                context[0], generation.generation_id, now=NOW
            )
        session.commit()
        labels = {value[0][0]: chr(ord("A") + index) for index, value in enumerate(ordered)}

        leases = tuple(
            _fair_claim(session, f"fair-history-{index}")
            for index in range(sum(counts))
        )
        history = "".join(labels[lease.organization_id] for lease in leases)

        assert history.startswith(expected_prefix)
        if counts == (100, 1, 1):
            assert history == "ABC" + ("A" * 99)
        else:
            assert history == "ABCABCABCABAB" + ("A" * 95)
        assert len({lease.work_item_id for lease in leases}) == len(leases)
        assert tuple(lease.fairness_claim_sequence for lease in leases) == tuple(
            sorted(lease.fairness_claim_sequence for lease in leases)
        )


def test_committed_claims_do_not_impose_an_active_per_organization_cap(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _make_fair_generation(session, "FairNoActiveCap", 2)

        first = _fair_claim(session, "fair-active-1")
        second = _fair_claim(session, "fair-active-2")

        assert first.organization_id == second.organization_id == context[0]
        assert first.work_item_id != second.work_item_id
        assert _fair_work_states(session, generation.generation_id) == tuple(
            sorted(
                _fair_work_states(session, generation.generation_id),
                key=lambda value: value[0],
            )
        )
        assert all(row.status == FileWorkStatus.RUNNING.value for row in session.scalars(
            select(ConnectorSyncFileWorkItem).where(
                ConnectorSyncFileWorkItem.generation_id == generation.generation_id
            )
        ))


def test_locked_never_served_item_is_skipped_and_reconsidered(engine) -> None:
    with Session(engine, expire_on_commit=False) as setup:
        candidates = tuple(
            _make_fair_generation(setup, f"FairItemLock{index}", 1)
            for index in range(2)
        )
        first, second = sorted(candidates, key=lambda value: value[0][0])

    with Session(engine, expire_on_commit=False) as blocker:
        blocker.execute(text("SET LOCAL lock_timeout = '5s'"))
        blocker.execute(text("SET LOCAL statement_timeout = '5s'"))
        blocker.scalar(
            select(ConnectorSyncFileWorkItem)
            .where(
                ConnectorSyncFileWorkItem.generation_id == first[1].generation_id
            )
            .with_for_update()
        )
        with Session(engine, expire_on_commit=False) as worker:
            worker.execute(text("SET LOCAL lock_timeout = '5s'"))
            worker.execute(text("SET LOCAL statement_timeout = '5s'"))
            lease = _fair_claim(worker, "fair-item-lock")
            assert lease.organization_id == second[0][0]
        blocker.rollback()

    with Session(engine, expire_on_commit=False) as worker:
        reconsidered = _fair_claim(worker, "fair-item-reconsidered")
        assert reconsidered.organization_id == first[0][0]


def test_locked_served_schedule_is_skipped_and_reconsidered(engine) -> None:
    with Session(engine, expire_on_commit=False) as setup:
        candidates = tuple(
            _make_fair_generation(setup, f"FairScheduleLock{index}", 2)
            for index in range(2)
        )
        first, second = sorted(candidates, key=lambda value: value[0][0])
        assert _fair_claim(setup, "fair-schedule-initialize-1").organization_id == first[0][0]
        assert _fair_claim(setup, "fair-schedule-initialize-2").organization_id == second[0][0]

    with Session(engine, expire_on_commit=False) as blocker:
        blocker.execute(text("SET LOCAL lock_timeout = '5s'"))
        blocker.execute(text("SET LOCAL statement_timeout = '5s'"))
        blocker.scalar(
            select(ConnectorSyncOrganizationClaimSchedule)
            .where(
                ConnectorSyncOrganizationClaimSchedule.organization_id == first[0][0]
            )
            .with_for_update()
        )
        with Session(engine, expire_on_commit=False) as worker:
            worker.execute(text("SET LOCAL lock_timeout = '5s'"))
            worker.execute(text("SET LOCAL statement_timeout = '5s'"))
            lease = _fair_claim(worker, "fair-schedule-lock")
            assert lease.organization_id == second[0][0]
        blocker.rollback()

    with Session(engine, expire_on_commit=False) as worker:
        reconsidered = _fair_claim(worker, "fair-schedule-reconsidered")
        assert reconsidered.organization_id == first[0][0]


def test_workers_committing_at_different_speeds_preserve_durable_turn_order(
    engine,
) -> None:
    with Session(engine, expire_on_commit=False) as setup:
        candidates = tuple(
            _make_fair_generation(setup, f"FairCommitSpeed{index}", 2)
            for index in range(2)
        )
        first, second = sorted(candidates, key=lambda value: value[0][0])

    first_claimed = threading.Event()
    allow_first_commit = threading.Event()
    committed: list[UUID] = []
    errors: list[BaseException] = []
    result_lock = threading.Lock()

    def slow_first() -> None:
        with Session(engine, expire_on_commit=False) as worker:
            try:
                worker.execute(text("SET LOCAL lock_timeout = '5s'"))
                worker.execute(text("SET LOCAL statement_timeout = '5s'"))
                lease = ConnectorSyncWorkLedgerRepository(
                    worker
                ).claim_next_available_fair(
                    provider_key="github",
                    profile_fingerprint=PROFILE,
                    worker_id="fair-slow-commit",
                    now=NOW,
                    lease_duration=LEASE,
                )
                assert lease.organization_id == first[0][0]
                first_claimed.set()
                assert allow_first_commit.wait(timeout=10)
                worker.commit()
                with result_lock:
                    committed.append(lease.organization_id)
            except BaseException as exc:  # pragma: no cover - asserted below
                worker.rollback()
                first_claimed.set()
                with result_lock:
                    errors.append(exc)

    thread = threading.Thread(target=slow_first)
    thread.start()
    assert first_claimed.wait(timeout=10)
    with Session(engine, expire_on_commit=False) as fast_worker:
        lease = _fair_claim(fast_worker, "fair-fast-commit")
        assert lease.organization_id == second[0][0]
        committed.append(lease.organization_id)
    allow_first_commit.set()
    thread.join(15)

    assert not thread.is_alive()
    assert errors == []
    assert committed == [second[0][0], first[0][0]]
    with Session(engine, expire_on_commit=False) as later:
        next_lease = _fair_claim(later, "fair-after-slow-commit")
        assert next_lease.organization_id == first[0][0]
        first_schedule = _fair_schedule_state(later, first[0][0])
        second_schedule = _fair_schedule_state(later, second[0][0])
        assert first_schedule[0] > second_schedule[0]


def test_retry_wait_and_expired_recovery_reenter_normal_fair_order(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        one = _make_fair_generation(session, "FairRetryA", 1)
        two = _make_fair_generation(session, "FairRetryB", 1)
        expected_first, expected_second = sorted((one, two), key=lambda value: value[0][0])
        first = _fair_claim(session, "fair-retry-first")
        assert first.organization_id == expected_first[0][0]
        ConnectorSyncWorkLedgerRepository(session).record_failure(
            first,
            worker_id=first.worker_id,
            error_category="rate_limit",
            error_code="provider_throttled",
            now=NOW,
            retry_at=NOW + timedelta(minutes=10),
        )
        session.commit()

        second = _fair_claim(session, "fair-retry-second", now=NOW)
        assert second.organization_id == expected_second[0][0]
        assert _fair_claim(session, "fair-retry-early", now=NOW) is None
        retried = _fair_claim(
            session, "fair-retry-ready", now=NOW + timedelta(minutes=10)
        )
        assert retried.organization_id == expected_first[0][0]
        assert retried.fencing_token == first.fencing_token + 1
        schedule = session.get(
            ConnectorSyncOrganizationClaimSchedule, expected_first[0][0]
        )
        assert schedule.claim_count == 2


def test_newly_eligible_organization_enters_before_previously_served_backlogs(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        first = _make_fair_generation(session, "FairExistingA", 3)
        second = _make_fair_generation(session, "FairExistingB", 3)
        existing_claims = (
            _fair_claim(session, "fair-existing-0"),
            _fair_claim(session, "fair-existing-1"),
        )
        assert {lease.organization_id for lease in existing_claims} == {
            first[0][0], second[0][0]
        }

        newcomer = _make_fair_generation(session, "FairNewcomer", 1)
        next_lease = _fair_claim(session, "fair-newcomer")

        assert next_lease.organization_id == newcomer[0][0]


def test_fairness_state_rolls_back_with_uncommitted_item_claim(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _make_fair_generation(session, "FairRollback", 1)
        lease = ConnectorSyncWorkLedgerRepository(session).claim_next_available_fair(
            provider_key="github",
            profile_fingerprint=PROFILE,
            worker_id="fair-rollback",
            now=NOW,
            lease_duration=LEASE,
        )
        assert lease is not None
        session.rollback()

        assert session.get(ConnectorSyncOrganizationClaimSchedule, context[0]) is None
        item = session.scalar(
            select(ConnectorSyncFileWorkItem).where(
                ConnectorSyncFileWorkItem.generation_id == generation.generation_id
            )
        )
        assert item.status == FileWorkStatus.PENDING.value
        assert item.attempt_count == 0


@pytest.mark.parametrize(
    "boundary",
    (
        "after_organization_selection",
        "after_schedule_initialization",
        "after_schedule_lock",
        "after_work_item_lock",
        "after_lease_fence_mutation",
        "after_fairness_mutation",
        "immediately_before_commit",
    ),
)
def test_forced_failure_at_each_fair_claim_boundary_rolls_back_all_durable_state(
    engine, monkeypatch, boundary
) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _make_fair_generation(
            session,
            f"FairBoundary{boundary}",
            2 if boundary == "after_schedule_lock" else 1,
        )
        if boundary == "after_schedule_lock":
            assert _fair_claim(session, "fair-boundary-initial") is not None
        before_schedule = _fair_schedule_state(session, context[0])
        before_work = _fair_work_states(session, generation.generation_id)
        repository = ConnectorSyncWorkLedgerRepository(session)

        if boundary in {"after_organization_selection", "after_schedule_lock"}:
            original = repository._select_fair_organization

            def fail_after_selection(**kwargs):
                selected = original(**kwargs)
                assert selected is not None
                if boundary == "after_schedule_lock":
                    assert selected[1] is not None
                raise RuntimeError(boundary)

            monkeypatch.setattr(repository, "_select_fair_organization", fail_after_selection)
        elif boundary == "after_schedule_initialization":
            original_execute = session.execute

            def fail_after_schedule_insert(statement, *args, **kwargs):
                result = original_execute(statement, *args, **kwargs)
                if (
                    getattr(getattr(statement, "table", None), "name", None)
                    == ConnectorSyncOrganizationClaimSchedule.__tablename__
                ):
                    raise RuntimeError(boundary)
                return result

            monkeypatch.setattr(session, "execute", fail_after_schedule_insert)
        elif boundary == "after_work_item_lock":
            def fail_after_item_lock(statement, **kwargs):
                assert repository._one(statement, "forced item lock") is not None
                raise RuntimeError(boundary)

            monkeypatch.setattr(repository, "_claim_one", fail_after_item_lock)
        elif boundary == "after_lease_fence_mutation":
            original_claim = repository._claim_fair_organization_item

            def fail_after_claim(**kwargs):
                assert original_claim(**kwargs) is not None
                raise RuntimeError(boundary)

            monkeypatch.setattr(repository, "_claim_fair_organization_item", fail_after_claim)
        elif boundary == "after_fairness_mutation":
            original_flush = repository._flush

            def fail_before_fair_flush(message):
                if message == "fair organization claim could not be advanced":
                    raise RuntimeError(boundary)
                original_flush(message)

            monkeypatch.setattr(repository, "_flush", fail_before_fair_flush)

        try:
            lease = repository.claim_next_available_fair(
                provider_key="github",
                profile_fingerprint=PROFILE,
                worker_id=f"fair-boundary-{boundary}",
                now=NOW,
                lease_duration=LEASE,
            )
            if boundary == "immediately_before_commit":
                assert lease is not None
                raise RuntimeError(boundary)
        except RuntimeError as exc:
            assert str(exc) == boundary
            session.rollback()
        else:  # pragma: no cover - every injected boundary must fail closed
            pytest.fail(f"{boundary} did not fail")

        assert _fair_schedule_state(session, context[0]) == before_schedule
        assert _fair_work_states(session, generation.generation_id) == before_work


def test_rolled_back_sequence_value_leaves_gap_but_no_committed_turn(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _make_fair_generation(session, "FairSequenceGap", 1)
        rolled_back = ConnectorSyncWorkLedgerRepository(
            session
        ).claim_next_available_fair(
            provider_key="github",
            profile_fingerprint=PROFILE,
            worker_id="fair-gap-rollback",
            now=NOW,
            lease_duration=LEASE,
        )
        assert rolled_back is not None
        session.rollback()
        assert _fair_schedule_state(session, context[0]) is None
        assert _fair_work_states(session, generation.generation_id)[0][1:] == (
            FileWorkStatus.PENDING.value,
            0,
            0,
            None,
        )

        committed = _fair_claim(session, "fair-gap-commit")
        assert committed.fairness_claim_sequence > rolled_back.fairness_claim_sequence
        schedule = session.get(ConnectorSyncOrganizationClaimSchedule, context[0])
        assert schedule.claim_count == 1
        assert schedule.last_claim_sequence == committed.fairness_claim_sequence


def test_stale_never_served_snapshot_atomically_advances_existing_schedule(
    engine, monkeypatch
) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _make_fair_generation(
            session, "FairFirstTurnRace", 2
        )
        first = _fair_claim(session, "fair-first-turn-winner")
        assert first is not None
        schedule = session.get(
            ConnectorSyncOrganizationClaimSchedule, context[0]
        )
        assert schedule.claim_count == 1
        first_sequence = schedule.last_claim_sequence

        repository = ConnectorSyncWorkLedgerRepository(session)
        monkeypatch.setattr(
            repository,
            "_select_fair_organization",
            lambda **_kwargs: (context[0], None),
        )
        second = repository.claim_next_available_fair(
            provider_key="github",
            profile_fingerprint=PROFILE,
            worker_id="fair-stale-never-served-snapshot",
            now=NOW,
            lease_duration=LEASE,
        )
        session.commit()

        assert second is not None
        assert second.organization_id == context[0]
        assert second.generation_id == generation.generation_id
        assert second.fairness_claim_sequence > first_sequence
        session.expire_all()
        schedule = session.get(
            ConnectorSyncOrganizationClaimSchedule, context[0]
        )
        assert schedule.claim_count == 2
        assert schedule.last_claim_sequence == second.fairness_claim_sequence


def test_repeated_rollback_for_one_organization_does_not_corrupt_other_schedules(
    engine,
) -> None:
    with Session(engine, expire_on_commit=False) as session:
        candidates = tuple(
            _make_fair_generation(session, f"FairRepeatedRollback{index}", 1)
            for index in range(2)
        )
        first, second = sorted(candidates, key=lambda value: value[0][0])
        rolled_back_sequences = []
        for index in range(3):
            lease = ConnectorSyncWorkLedgerRepository(
                session
            ).claim_next_available_fair(
                provider_key="github",
                profile_fingerprint=PROFILE,
                worker_id=f"fair-repeat-rollback-{index}",
                now=NOW,
                lease_duration=LEASE,
            )
            rolled_back_sequences.append(lease.fairness_claim_sequence)
            session.rollback()
        assert rolled_back_sequences == sorted(rolled_back_sequences)
        assert _fair_schedule_state(session, first[0][0]) is None
        assert _fair_schedule_state(session, second[0][0]) is None

    with Session(engine, expire_on_commit=False) as blocker:
        blocker.execute(text("SET LOCAL lock_timeout = '5s'"))
        blocker.execute(text("SET LOCAL statement_timeout = '5s'"))
        blocker.scalar(
            select(Organization)
            .where(Organization.id == first[0][0])
            .with_for_update()
        )
        with Session(engine, expire_on_commit=False) as worker:
            lease = _fair_claim(worker, "fair-repeat-other")
            assert lease.organization_id == second[0][0]
        blocker.rollback()

    with Session(engine, expire_on_commit=False) as verification:
        assert _fair_schedule_state(verification, first[0][0]) is None
        assert _fair_schedule_state(verification, second[0][0])[1] == 1
        first_committed = _fair_claim(verification, "fair-repeat-first")
        assert first_committed.organization_id == first[0][0]
        assert first_committed.fairness_claim_sequence > max(rolled_back_sequences)


def test_fair_claim_excludes_ineligible_generation_and_work_states(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        eligible = _make_fair_generation(session, "FairEligible", 1)

        empty = _generation(session, "FairEmpty")
        ConnectorSyncWorkLedgerRepository(session).mark_discovery_complete(
            empty[0][0], empty[1].generation_id, now=NOW
        )
        session.commit()

        incomplete = _generation(session, "FairIncomplete")
        _register(
            session, incomplete[0][0], incomplete[1].generation_id, (_entry(20),)
        )

        incompatible_context = _setup(session, "FairIncompatible")
        incompatible_request = replace(
            _generation_request(incompatible_context),
            profile_fingerprint="github:incompatible-profile",
        )
        incompatible_generation, _ = ConnectorSyncWorkLedgerRepository(
            session
        ).register_generation(incompatible_request)
        ConnectorSyncWorkLedgerRepository(session).register_manifest(
            incompatible_context[0],
            incompatible_generation.generation_id,
            (
                replace(
                    _entry(21), profile_fingerprint="github:incompatible-profile"
                ),
            ),
            now=NOW,
        )
        ConnectorSyncWorkLedgerRepository(session).mark_discovery_complete(
            incompatible_context[0], incompatible_generation.generation_id, now=NOW
        )
        session.commit()

        cancelled = _make_fair_generation(session, "FairCancelled", 1)
        cancelled_item = session.scalar(
            select(ConnectorSyncFileWorkItem).where(
                ConnectorSyncFileWorkItem.generation_id
                == cancelled[1].generation_id
            )
        )
        ConnectorSyncWorkLedgerRepository(session).request_cancellation(
            cancelled[0][0],
            cancelled[1].generation_id,
            cancelled_item.id,
            reason_code="operator_cancelled",
            now=NOW,
        )
        session.commit()

        quarantined = _make_fair_generation(session, "FairQuarantined", 1)
        quarantine_lease = ConnectorSyncWorkLedgerRepository(session).claim_next(
            quarantined[0][0],
            quarantined[1].generation_id,
            worker_id="quarantine-setup",
            now=NOW,
            lease_duration=LEASE,
        )
        ConnectorSyncWorkLedgerRepository(session).record_failure(
            quarantine_lease,
            worker_id="quarantine-setup",
            error_category="extraction",
            error_code="unsupported_payload",
            quarantine_reason_code="unsupported_payload",
            now=NOW,
        )
        session.commit()

        terminal = _make_fair_generation(session, "FairTerminal", 1)
        terminal_lease = ConnectorSyncWorkLedgerRepository(session).claim_next(
            terminal[0][0],
            terminal[1].generation_id,
            worker_id="terminal-setup",
            now=NOW,
            lease_duration=LEASE,
        )
        ConnectorSyncWorkLedgerRepository(session).complete(
            terminal_lease,
            worker_id="terminal-setup",
            outcome=FileWorkStatus.SUCCEEDED,
            counters=FileWorkCounters(),
            now=NOW,
        )
        session.commit()

        lease = _fair_claim(session, "fair-only-eligible")
        assert lease.organization_id == eligible[0][0]
        assert _fair_claim(session, "fair-no-more") is None
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncOrganizationClaimSchedule)
        ) == 1


def test_expired_lease_recovery_does_not_rewind_consumed_fair_turn(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        one = _make_fair_generation(session, "FairExpiryA", 1)
        two = _make_fair_generation(session, "FairExpiryB", 1)
        expected_first, expected_second = sorted((one, two), key=lambda value: value[0][0])
        first = _fair_claim(session, "fair-expired-first")
        first_schedule = session.get(
            ConnectorSyncOrganizationClaimSchedule, expected_first[0][0]
        )
        consumed_sequence = first_schedule.last_claim_sequence

        recovered = ConnectorSyncWorkLedgerRepository(session).recover_expired_available(
            provider_key="github",
            profile_fingerprint=PROFILE,
            now=NOW + LEASE,
            limit=10,
        )
        session.commit()
        assert len(recovered) == 1
        assert session.get(
            ConnectorSyncOrganizationClaimSchedule, expected_first[0][0]
        ).last_claim_sequence == consumed_sequence

        second = _fair_claim(session, "fair-expired-second", now=NOW + LEASE)
        assert second.organization_id == expected_second[0][0]
        replacement = _fair_claim(
            session, "fair-expired-replacement", now=NOW + LEASE
        )
        assert replacement.organization_id == expected_first[0][0]
        assert replacement.fencing_token == first.fencing_token + 1


def test_concurrent_fair_claims_drain_sustained_skew_without_starvation(engine) -> None:
    with Session(engine, expire_on_commit=False) as setup:
        tenants = tuple(
            _make_fair_generation(setup, f"FairConcurrent{index}", 12 if index == 0 else 2)
            for index in range(3)
        )
    start = threading.Barrier(4)
    leases: list[object] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def claim(index: int) -> None:
        with Session(engine, expire_on_commit=False) as worker:
            try:
                start.wait(timeout=20)
                while True:
                    lease = ConnectorSyncWorkLedgerRepository(
                        worker
                    ).claim_next_available_fair(
                        provider_key="github",
                        profile_fingerprint=PROFILE,
                        worker_id=f"fair-concurrent-{index}",
                        now=NOW,
                        lease_duration=LEASE,
                    )
                    worker.commit()
                    if lease is None:
                        break
                    with lock:
                        leases.append(lease)
            except BaseException as exc:  # pragma: no cover - asserted below
                worker.rollback()
                with lock:
                    errors.append(exc)

    threads = [threading.Thread(target=claim, args=(index,)) for index in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert len(leases) == 16
    assert len({lease.work_item_id for lease in leases}) == len(leases)
    assert len({lease.lease_id for lease in leases}) == len(leases)
    assert {lease.organization_id for lease in leases} == {
        tenant[0][0] for tenant in tenants
    }
    assert len({lease.fairness_claim_sequence for lease in leases}) == len(leases)
    with Session(engine) as verification:
        schedules = {
            row.organization_id: row.claim_count
            for row in verification.scalars(
                select(ConnectorSyncOrganizationClaimSchedule)
            )
        }
        assert schedules == {
            tenants[0][0][0]: 12,
            tenants[1][0][0]: 2,
            tenants[2][0][0]: 2,
        }


def test_fewer_workers_converge_across_more_eligible_organizations(engine) -> None:
    with Session(engine, expire_on_commit=False) as setup:
        tenants = tuple(
            _make_fair_generation(setup, f"FairManyTenants{index}", 1)
            for index in range(5)
        )
    start = threading.Barrier(2)
    leases: list[object] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def drain(index: int) -> None:
        with Session(engine, expire_on_commit=False) as worker:
            try:
                start.wait(timeout=20)
                while True:
                    lease = ConnectorSyncWorkLedgerRepository(
                        worker
                    ).claim_next_available_fair(
                        provider_key="github",
                        profile_fingerprint=PROFILE,
                        worker_id=f"fair-fewer-workers-{index}",
                        now=NOW,
                        lease_duration=LEASE,
                    )
                    worker.commit()
                    if lease is None:
                        return
                    with lock:
                        leases.append(lease)
            except BaseException as exc:  # pragma: no cover - asserted below
                worker.rollback()
                with lock:
                    errors.append(exc)

    threads = [threading.Thread(target=drain, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert len(leases) == 5
    assert len({lease.work_item_id for lease in leases}) == 5
    assert {lease.organization_id for lease in leases} == {
        tenant[0][0] for tenant in tenants
    }


def test_generation_scoped_materialization_and_completion_are_atomic(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "Materialization")
        _register(session, context[0], generation.generation_id, (_entry(1),))
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.mark_discovery_complete(context[0], generation.generation_id, now=NOW)
        session.commit()
        lease = repository.claim_next_available(
            provider_key="github", profile_fingerprint=PROFILE,
            worker_id="phase3-worker", now=NOW, lease_duration=LEASE,
        )
        session.commit()
        assert lease is not None
        generation = repository.get_generation(context[0], generation.generation_id)
        work = repository.get_work_item(context[0], lease.generation_id, lease.work_item_id)
        assert generation is not None and work is not None
        completed, staged, created = repository.stage_materialization_and_complete(
            lease,
            worker_id="phase3-worker",
            generation=generation,
            work_item=work,
            materialization=_materialization(generation, work),
            counters=FileWorkCounters(5, 5, 1, 1),
            now=NOW,
        )
        session.commit()
        assert created is True
        assert completed.status is FileWorkStatus.SUCCEEDED
        assert staged.generation_id == generation.generation_id
        assert staged.work_item_id == work.work_item_id
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncFileMaterialization)
        ) == 1
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncFileMaterializationChunk)
        ) == 1


def test_concurrent_same_lease_completion_converges_to_one_materialization(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "ConcurrentMaterialization")
        _register(session, context[0], generation.generation_id, (_entry(1),))
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.mark_discovery_complete(
            context[0], generation.generation_id, now=NOW
        )
        session.commit()
        lease = repository.claim_next_available(
            provider_key="github",
            profile_fingerprint=PROFILE,
            worker_id="phase3-worker",
            now=NOW,
            lease_duration=LEASE,
        )
        session.commit()
        assert lease is not None
        generation_view = repository.get_generation(
            context[0], generation.generation_id
        )
        work_view = repository.get_work_item(
            context[0], lease.generation_id, lease.work_item_id
        )
        assert generation_view is not None and work_view is not None

    gate = threading.Barrier(2)
    outcomes: list[str] = []

    def complete_once() -> None:
        with Session(engine, expire_on_commit=False) as session:
            repository = ConnectorSyncWorkLedgerRepository(session)
            gate.wait(timeout=10)
            try:
                repository.stage_materialization_and_complete(
                    lease,
                    worker_id="phase3-worker",
                    generation=generation_view,
                    work_item=work_view,
                    materialization=_materialization(generation_view, work_view),
                    counters=FileWorkCounters(5, 5, 1, 1),
                    now=NOW,
                )
                session.commit()
                outcomes.append("completed")
            except LostFileWorkLease:
                session.rollback()
                outcomes.append("lost_lease")

    workers = [threading.Thread(target=complete_once) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=15)
        assert not worker.is_alive()

    assert sorted(outcomes) == ["completed", "lost_lease"]
    with Session(engine) as session:
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncFileMaterialization)
        ) == 1
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncFileMaterializationChunk)
        ) == 1
        work = session.get(ConnectorSyncFileWorkItem, lease.work_item_id)
        assert work.status == FileWorkStatus.SUCCEEDED.value


def test_same_path_different_blobs_are_isolated_between_generations(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        first_context, first_generation = _generation(session, "FirstGeneration")
        second_context = _setup(session, "SecondGeneration")
        second_generation, created = ConnectorSyncWorkLedgerRepository(
            session
        ).register_generation(
            replace(
                _generation_request(second_context),
                commit_object_id="f" * 40,
                root_tree_object_id="e" * 40,
            )
        )
        session.commit()
        assert created
        first_entry = _entry(1)
        second_entry = replace(
            first_entry,
            provider_blob_id="e" * 40,
            provider_revision_id="f" * 40,
        )
        _register(
            session,
            first_context[0],
            first_generation.generation_id,
            (first_entry,),
        )
        _register(
            session,
            second_context[0],
            second_generation.generation_id,
            (second_entry,),
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.mark_discovery_complete(
            first_context[0], first_generation.generation_id, now=NOW
        )
        repository.mark_discovery_complete(
            second_context[0], second_generation.generation_id, now=NOW
        )
        session.commit()

        persisted = []
        for context, generation, worker_id in (
            (first_context, first_generation, "first-worker"),
            (second_context, second_generation, "second-worker"),
        ):
            lease = repository.claim_next(
                context[0],
                generation.generation_id,
                worker_id=worker_id,
                now=NOW,
                lease_duration=LEASE,
            )
            session.commit()
            assert lease is not None
            generation_view = repository.get_generation(
                context[0], generation.generation_id
            )
            work_view = repository.get_work_item(
                context[0], generation.generation_id, lease.work_item_id
            )
            assert generation_view is not None and work_view is not None
            completed, materialization, created = (
                repository.stage_materialization_and_complete(
                    lease,
                    worker_id=worker_id,
                    generation=generation_view,
                    work_item=work_view,
                    materialization=_materialization(generation_view, work_view),
                    counters=FileWorkCounters(5, 5, 1, 1),
                    now=NOW,
                )
            )
            session.commit()
            assert completed.status is FileWorkStatus.SUCCEEDED
            assert created
            persisted.append(materialization)

        assert persisted[0].generation_id != persisted[1].generation_id
        assert persisted[0].provider_blob_id != persisted[1].provider_blob_id
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncFileMaterialization)
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncFileMaterializationChunk)
        ) == 2


def test_materialization_rollback_preserves_running_work_and_no_staged_rows(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "MaterializationRollback")
        _register(session, context[0], generation.generation_id, (_entry(1),))
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.mark_discovery_complete(context[0], generation.generation_id, now=NOW)
        session.commit()
        lease = repository.claim_next_available(
            provider_key="github", profile_fingerprint=PROFILE,
            worker_id="phase3-worker", now=NOW, lease_duration=LEASE,
        )
        session.commit()
        assert lease is not None
        generation = repository.get_generation(context[0], generation.generation_id)
        work = repository.get_work_item(context[0], lease.generation_id, lease.work_item_id)
        assert generation is not None and work is not None
        repository.stage_materialization_and_complete(
            lease,
            worker_id="phase3-worker",
            generation=generation,
            work_item=work,
            materialization=_materialization(generation, work),
            counters=FileWorkCounters(5, 5, 1, 1),
            now=NOW,
        )
        session.rollback()

    with Session(engine) as session:
        work_row = session.get(ConnectorSyncFileWorkItem, lease.work_item_id)
        assert work_row.status == FileWorkStatus.RUNNING.value
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncFileMaterialization)
        ) == 0
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncFileMaterializationChunk)
        ) == 0


def test_complete_generation_promotion_is_atomic_and_idempotent(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, _work, *_ = _ready_promotion(session)
        repository = ConnectorSyncWorkLedgerRepository(session)
        request = _promotion_request(generation)
        first = repository.promote_generation(request, now=NOW + timedelta(minutes=1))
        session.commit()
        replay = repository.promote_generation(request, now=NOW + timedelta(minutes=2))
        session.commit()
        assert first.promoted is True
        assert replay.promoted is False
        assert replay.activation.activation_id == first.activation.activation_id
        assert (first.materialization_count, first.chunk_count) == (1, 1)
        assert session.scalar(select(func.count()).select_from(ConnectorSyncGenerationActivation)) == 1
        persisted = repository.get_generation(context[0], generation.generation_id)
        assert persisted is not None
        assert persisted.status.value == "completed"


def test_projection_builds_missing_citations_and_promotes_in_one_commit(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, _work, source_id, _version_id, document_id = (
            _ready_promotion(session, "ProjectedPromotion")
        )
        session.execute(
            text("DELETE FROM source_items WHERE organization_id=:org AND id=:id"),
            {"org": context[0], "id": source_id},
        )
        session.execute(
            text("DELETE FROM documents WHERE organization_id=:org AND id=:id"),
            {"org": context[0], "id": document_id},
        )
        session.commit()
        repository = ConnectorSyncWorkLedgerRepository(session)

        result = repository.project_citations_and_promote_generation(
            _promotion_request(generation),
            _projection_profile(generation),
            now=NOW + timedelta(minutes=1),
        )
        session.commit()

        assert result.promoted is True
        assert (result.materialization_count, result.chunk_count) == (1, 1)
        assert session.scalar(
            select(func.count()).select_from(SourceItem).where(
                SourceItem.organization_id == context[0],
                SourceItem.source_item_key == _entry(1).source_item_key,
            )
        ) == 1
        assert session.scalar(
            select(func.count()).select_from(DocumentChunk).where(
                DocumentChunk.organization_id == context[0]
            )
        ) == 0
        replay = repository.project_citations_and_promote_generation(
            _promotion_request(generation),
            _projection_profile(generation),
            now=NOW + timedelta(minutes=2),
        )
        session.commit()
        assert replay.promoted is False
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncGenerationActivation)
        ) == 1


def test_projection_failure_savepoint_prevents_partial_commit(engine, monkeypatch) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, _work, source_id, _version_id, document_id = (
            _ready_promotion(session, "ProjectionRollback")
        )
        session.execute(
            text("DELETE FROM source_items WHERE organization_id=:org AND id=:id"),
            {"org": context[0], "id": source_id},
        )
        session.execute(
            text("DELETE FROM documents WHERE organization_id=:org AND id=:id"),
            {"org": context[0], "id": document_id},
        )
        session.commit()
        repository = ConnectorSyncWorkLedgerRepository(session)
        monkeypatch.setattr(
            repository,
            "promote_generation",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                SyncWorkLedgerConflict("forced activation failure")
            ),
        )

        with pytest.raises(SyncWorkLedgerConflict, match="forced activation"):
            repository.project_citations_and_promote_generation(
                _promotion_request(generation),
                _projection_profile(generation),
                now=NOW + timedelta(minutes=1),
            )
        # The caller deliberately catches the validation error and commits.
        session.commit()

        assert session.scalar(
            select(func.count()).select_from(SourceItem).where(
                SourceItem.organization_id == context[0]
            )
        ) == 0
        assert session.scalar(
            select(func.count()).select_from(Document).where(
                Document.organization_id == context[0]
            )
        ) == 0
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncGenerationActivation)
        ) == 0


def test_concurrent_projection_and_promotion_converges_without_duplicate_citations(
    engine,
) -> None:
    with Session(engine, expire_on_commit=False) as setup:
        context, generation, _work, source_id, _version_id, document_id = (
            _ready_promotion(setup, "ConcurrentProjection")
        )
        setup.execute(
            text("DELETE FROM source_items WHERE organization_id=:org AND id=:id"),
            {"org": context[0], "id": source_id},
        )
        setup.execute(
            text("DELETE FROM documents WHERE organization_id=:org AND id=:id"),
            {"org": context[0], "id": document_id},
        )
        setup.commit()
        request = _promotion_request(generation)
        profile = _projection_profile(generation)

    barrier = threading.Barrier(2)
    outcomes: list[bool] = []
    errors: list[BaseException] = []

    def project() -> None:
        with Session(engine) as session:
            try:
                barrier.wait(timeout=10)
                result = ConnectorSyncWorkLedgerRepository(
                    session
                ).project_citations_and_promote_generation(
                    request, profile, now=NOW + timedelta(minutes=1)
                )
                session.commit()
                outcomes.append(result.promoted)
            except BaseException as exc:  # pragma: no cover - asserted below
                session.rollback()
                errors.append(exc)

    workers = [threading.Thread(target=project) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(20)
        assert not worker.is_alive()

    assert errors == []
    assert sorted(outcomes) == [False, True]
    with Session(engine) as verification:
        assert verification.scalar(
            select(func.count()).select_from(ConnectorSyncGenerationActivation)
        ) == 1
        assert verification.scalar(
            select(func.count()).select_from(SourceItem).where(
                SourceItem.organization_id == context[0]
            )
        ) == 1
        assert verification.scalar(
            select(func.count()).select_from(DocumentVersion).where(
                DocumentVersion.organization_id == context[0]
            )
        ) == 1
        assert verification.scalar(
            select(func.count()).select_from(DocumentIndexingState).where(
                DocumentIndexingState.organization_id == context[0]
            )
        ) == 1
        assert verification.scalar(
            select(func.count()).select_from(DocumentChunk).where(
                DocumentChunk.organization_id == context[0]
            )
        ) == 0


def test_projection_preserves_shared_membership_retrieval_and_scope_uniqueness(
    engine,
) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, work, source_id, version_id, document_id = (
            _ready_promotion(session, "SharedProjection")
        )
        repository = ConnectorSyncWorkLedgerRepository(session)

        other_space, other_scope = uuid4(), uuid4()
        session.execute(
            text(
                "INSERT INTO knowledge_spaces (id,organization_id,name,slug) "
                "VALUES (:id,:org,'Shared',:slug)"
            ),
            {"id": other_space, "org": context[0], "slug": f"space-{other_space}"},
        )
        session.execute(
            text(
                """INSERT INTO connector_scopes
                (id,organization_id,connector_id,knowledge_space_id,display_name,slug,
                 scope_type,external_scope_key,access_mode,status)
                VALUES (:id,:org,:connector,:space,'Shared',:slug,'repository',
                        :key,'platform_managed','active')"""
            ),
            {
                "id": other_scope,
                "org": context[0],
                "connector": context[1],
                "space": other_space,
                "slug": f"scope-{other_scope}",
                "key": f"github:repository:{other_scope.int}",
            },
        )
        session.add(
            SourceItemScopeMembership(
                id=uuid4(),
                organization_id=context[0],
                connector_id=context[1],
                source_item_id=source_id,
                connector_scope_id=other_scope,
                status="active",
                first_discovered_at=NOW,
                last_seen_at=NOW,
                removed_at=None,
            )
        )
        legacy_chunk_id = uuid4()
        session.add(
            DocumentChunk(
                id=legacy_chunk_id,
                organization_id=context[0],
                document_id=document_id,
                chunk_index=0,
                chunk_text="shared legacy content",
                content_hash="7" * 64,
                token_count=None,
                embedding=[1.0] * 1536,
                embedding_model="fake:model:1536",
            )
        )
        session.commit()
        other_user = _retrieval_user(session, context[0], other_scope)
        assert _retrieval_chunk_ids(session, context[0], other_user) == {
            legacy_chunk_id
        }

        # The staged generation now carries changed content. Projection for
        # the target scope must not rewrite the global legacy current/link or
        # chunk state observed through the independent shared scope.
        changed_blob = "f" * 40
        changed_checksum = "8" * 64
        session.execute(
            text(
                "UPDATE connector_sync_file_work_items "
                "SET provider_blob_id=:blob WHERE id=:work"
            ),
            {"blob": changed_blob, "work": work.work_item_id},
        )
        session.execute(
            text(
                "UPDATE connector_sync_file_materializations "
                "SET provider_blob_id=:blob,content_checksum=:checksum "
                "WHERE work_item_id=:work"
            ),
            {
                "blob": changed_blob,
                "checksum": changed_checksum,
                "work": work.work_item_id,
            },
        )
        session.execute(
            text(
                "UPDATE connector_sync_generation_observations "
                "SET provider_object_id=:blob WHERE generation_id=:generation"
            ),
            {"blob": changed_blob, "generation": generation.generation_id},
        )
        session.commit()

        result = repository.project_citations_and_promote_generation(
            _promotion_request(generation),
            _projection_profile(generation),
            now=NOW + timedelta(minutes=1),
        )
        session.commit()
        assert result.promoted is True
        assert _retrieval_chunk_ids(session, context[0], other_user) == {
            legacy_chunk_id
        }
        source = session.get(SourceItem, source_id)
        assert source is not None
        assert source.source_version == _entry(1).provider_blob_id
        assert source.source_checksum == "c" * 64
        assert session.get(DocumentVersion, version_id).is_current is True
        assert session.scalar(
            select(func.count()).select_from(DocumentVersion).where(
                DocumentVersion.source_item_id == source_id
            )
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(DocumentChunk).where(
                DocumentChunk.document_id == document_id
            )
        ) == 1

        # One connector cannot represent the same repository as two scopes;
        # the database invariant rules out two active generations sharing this
        # source identity while the explicit shared membership remains safe.
        duplicate_scope = uuid4()
        with pytest.raises(IntegrityError):
            with session.begin_nested():
                session.execute(
                    text(
                        """INSERT INTO connector_scopes
                        (id,organization_id,connector_id,knowledge_space_id,
                         display_name,slug,scope_type,external_scope_key,
                         access_mode,status)
                        VALUES (:id,:org,:connector,:space,'Duplicate',:slug,
                                'repository',:key,'platform_managed','active')"""
                    ),
                    {
                        "id": duplicate_scope,
                        "org": context[0],
                        "connector": context[1],
                        "space": other_space,
                        "slug": f"scope-{duplicate_scope}",
                        "key": generation.repository_identity,
                    },
                )

        replay = repository.project_citations_and_promote_generation(
            _promotion_request(generation),
            _projection_profile(generation),
            now=NOW + timedelta(minutes=2),
        )
        session.commit()
        assert replay.promoted is False
        assert _retrieval_chunk_ids(session, context[0], other_user) == {
            legacy_chunk_id
        }


@pytest.mark.parametrize(
    "status",
    ("pending", "retry_wait", "failed", "cancelled", "quarantined"),
)
def test_promotion_rejects_every_unsuccessful_work_state(engine, status: str) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, work, *_ = _ready_promotion(session, f"Reject-{status}")
        values = {
            "status": status,
            "next": NOW if status in {"pending", "retry_wait"} else None,
            "terminal": None if status in {"pending", "retry_wait"} else NOW,
            "category": "internal" if status in {"failed", "quarantined"} else None,
            "code": "forced_failure" if status in {"failed", "quarantined"} else None,
            "quarantine": "forced_failure" if status == "quarantined" else None,
            "cancel": NOW if status == "cancelled" else None,
            "cancel_reason": "operator_cancelled" if status == "cancelled" else None,
            "work": work.work_item_id,
        }
        session.execute(text("""UPDATE connector_sync_file_work_items SET
            status=:status,next_attempt_at=:next,terminal_at=:terminal,
            last_error_category=:category,last_error_code=:code,
            quarantine_reason_code=:quarantine,cancel_requested_at=:cancel,
            cancel_reason_code=:cancel_reason
            WHERE id=:work"""), values)
        session.commit()
        with pytest.raises(SyncWorkLedgerConflict, match="completely successful"):
            ConnectorSyncWorkLedgerRepository(session).promote_generation(
                _promotion_request(generation), now=NOW + timedelta(minutes=1)
            )
        session.rollback()
        assert session.scalar(select(func.count()).select_from(ConnectorSyncGenerationActivation)) == 0


def test_promotion_rejects_missing_and_mismatched_staging(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        _context, generation, work, *_ = _ready_promotion(session, "MissingStaging")
        session.execute(text("DELETE FROM connector_sync_file_materializations WHERE work_item_id=:id"), {"id": work.work_item_id})
        session.commit()
        with pytest.raises(SyncWorkLedgerConflict, match="incomplete"):
            ConnectorSyncWorkLedgerRepository(session).promote_generation(
                _promotion_request(generation), now=NOW + timedelta(minutes=1)
            )
        session.rollback()

    with Session(engine, expire_on_commit=False) as session:
        _context, generation, work, *_ = _ready_promotion(session, "MismatchedStaging")
        session.execute(text("UPDATE connector_sync_file_materializations SET repository_identity='github:repository:999' WHERE work_item_id=:id"), {"id": work.work_item_id})
        session.commit()
        with pytest.raises(SyncWorkLedgerConflict, match="attribution"):
            ConnectorSyncWorkLedgerRepository(session).promote_generation(
                _promotion_request(generation), now=NOW + timedelta(minutes=1)
            )
        session.rollback()


def test_duplicate_promotion_staging_is_rejected_by_database_identity(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        _context, _generation, work, *_ = _ready_promotion(
            session, "DuplicateStaging"
        )
        with pytest.raises(IntegrityError):
            with session.begin_nested():
                session.execute(
                    text(
                        """INSERT INTO connector_sync_file_materializations
                        SELECT :id,organization_id,connector_id,connector_scope_id,
                               generation_id,work_item_id,repository_identity,branch_name,
                               root_tree_object_id,source_item_key,source_key_hash,
                               repository_path,provider_blob_id,provider_revision_id,
                               profile_fingerprint,content_checksum,title,mime_type,
                               embedding_model,chunk_count,created_at
                        FROM connector_sync_file_materializations
                        WHERE work_item_id=:work"""
                    ),
                    {"id": uuid4(), "work": work.work_item_id},
                )


def test_promotion_rollback_and_later_failure_preserve_previous_activation(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, *_ = _ready_promotion(session, "RollbackPromotion")
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.promote_generation(_promotion_request(generation), now=NOW + timedelta(minutes=1))
        session.rollback()
        assert session.scalar(select(func.count()).select_from(ConnectorSyncGenerationActivation)) == 0
        assert repository.get_generation(context[0], generation.generation_id).status.value == "processing"

        repository.promote_generation(_promotion_request(generation), now=NOW + timedelta(minutes=1))
        session.commit()
        newer = _new_generation_same_scope(
            session, context, created_at=NOW + timedelta(hours=1), discovered=1
        )
        replay = repository.promote_generation(
            _promotion_request(generation), now=NOW + timedelta(hours=2)
        )
        assert replay.promoted is False
        with pytest.raises(SyncWorkLedgerConflict):
            repository.promote_generation(
                _promotion_request(newer), now=NOW + timedelta(hours=2)
            )
        session.rollback()
        active = session.scalar(select(ConnectorSyncGenerationActivation).where(ConnectorSyncGenerationActivation.status == "active"))
        assert active is not None and active.generation_id == generation.generation_id


def test_successful_cutover_retires_previous_generation_atomically(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, first, work, source_id, version_id, _document_id = _ready_promotion(
            session, "RetirePrevious"
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.promote_generation(
            _promotion_request(first), now=NOW + timedelta(minutes=1)
        )
        session.commit()
        second = _new_generation_same_scope(
            session, context, created_at=NOW + timedelta(hours=1), discovered=1
        )
        second_work, second_materialization = uuid4(), uuid4()
        source_key = work.source_item_key
        path = work.repository_path
        blob, checksum = "f" * 40, "9" * 64
        session.execute(text("""INSERT INTO connector_sync_file_work_items
            (id,organization_id,connector_id,connector_scope_id,generation_id,
             source_item_key,source_key_hash,repository_path,provider_blob_id,
             provider_revision_id,profile_fingerprint,status,attempt_count,max_attempts,
             fencing_token,downloaded_bytes,extracted_characters,chunk_count,
             embedding_batch_count,created_at,updated_at,terminal_at)
            VALUES (:id,:org,:connector,:scope,:generation,:key,:hash,:path,:blob,
                    :revision,:profile,'succeeded',1,3,1,10,10,1,1,:now,:now,:now)"""), {
            "id": second_work, "org": context[0], "connector": context[1],
            "scope": context[2], "generation": second.generation_id, "key": source_key,
            "hash": "8" * 64, "path": path, "blob": blob,
            "revision": second.commit_object_id, "profile": PROFILE,
            "now": NOW + timedelta(hours=1),
        })
        session.execute(text("""INSERT INTO connector_sync_file_materializations
            (id,organization_id,connector_id,connector_scope_id,generation_id,work_item_id,
             repository_identity,branch_name,root_tree_object_id,source_item_key,
             source_key_hash,repository_path,provider_blob_id,provider_revision_id,
             profile_fingerprint,content_checksum,title,mime_type,embedding_model,
             chunk_count,created_at)
            VALUES (:id,:org,:connector,:scope,:generation,:work,:repository,'main',:tree,
                    :key,:hash,:path,:blob,:revision,:profile,:checksum,'File',
                    'text/markdown','fake:model:1536',1,:now)"""), {
            "id": second_materialization, "org": context[0], "connector": context[1],
            "scope": context[2], "generation": second.generation_id, "work": second_work,
            "repository": second.repository_identity, "tree": second.root_tree_object_id,
            "key": source_key, "hash": "8" * 64, "path": path, "blob": blob,
            "revision": second.commit_object_id, "profile": PROFILE, "checksum": checksum,
            "now": NOW + timedelta(hours=1),
        })
        session.execute(text("""INSERT INTO connector_sync_file_materialization_chunks
            (id,organization_id,generation_id,materialization_id,chunk_index,chunk_text,
             content_hash,embedding,embedding_model,created_at)
            VALUES (:id,:org,:generation,:materialization,0,'new content',:hash,
                    CAST(:embedding AS vector),'fake:model:1536',:now)"""), {
            "id": uuid4(), "org": context[0], "generation": second.generation_id,
            "materialization": second_materialization, "hash": "7" * 64,
            "embedding": "[" + ",".join("1" for _ in range(1536)) + "]",
            "now": NOW + timedelta(hours=1),
        })
        metadata = json.dumps({
            "provider": "github", "repository_identity": second.repository_identity,
            "repository_path": path, "blob_object_id": blob,
            "snapshot_commit_id": second.commit_object_id,
        })
        session.execute(text("UPDATE source_items SET source_version=:blob,source_checksum=:checksum,metadata=CAST(:metadata AS jsonb) WHERE id=:id"), {"blob": blob, "checksum": checksum, "metadata": metadata, "id": source_id})
        version_metadata = json.dumps(
            {
                "provider": "github",
                "commit_object_id": second.commit_object_id,
                "blob_object_id": blob,
            }
        )
        session.execute(
            text(
                "UPDATE document_versions SET provider_version_id=:blob,"
                "content_checksum=:checksum,metadata=CAST(:metadata AS jsonb) "
                "WHERE id=:id"
            ),
            {
                "blob": blob,
                "checksum": checksum,
                "metadata": version_metadata,
                "id": version_id,
            },
        )
        session.commit()

        result = repository.promote_generation(
            _promotion_request(second), now=NOW + timedelta(hours=2)
        )
        session.commit()
        rows = session.scalars(
            select(ConnectorSyncGenerationActivation).order_by(
                ConnectorSyncGenerationActivation.activated_at
            )
        ).all()
        assert result.retired_generation_id == first.generation_id
        assert [(row.generation_id, row.status) for row in rows] == [
            (first.generation_id, "retired"),
            (second.generation_id, "active"),
        ]
        assert rows[0].retired_at == NOW + timedelta(hours=2)


def test_stale_and_cross_tenant_generation_promotion_fail_closed(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, *_ = _ready_promotion(session, "StalePromotion")
        _new_generation_same_scope(session, context, created_at=NOW + timedelta(hours=1))
        with pytest.raises(SyncWorkLedgerConflict, match="stale"):
            ConnectorSyncWorkLedgerRepository(session).promote_generation(
                _promotion_request(generation), now=NOW + timedelta(hours=2)
            )
        session.rollback()
        wrong = replace(_promotion_request(generation), organization_id=uuid4())
        with pytest.raises(SyncWorkLedgerNotFound):
            ConnectorSyncWorkLedgerRepository(session).promote_generation(
                wrong, now=NOW + timedelta(hours=2)
            )
        session.rollback()
        wrong_scope = replace(
            _promotion_request(generation), connector_scope_id=uuid4()
        )
        with pytest.raises(SyncWorkLedgerNotFound):
            ConnectorSyncWorkLedgerRepository(session).promote_generation(
                wrong_scope, now=NOW + timedelta(hours=2)
            )
        session.rollback()


def test_concurrent_duplicate_promotion_converges_to_one_activation(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        _context, generation, *_ = _ready_promotion(session, "ConcurrentPromotion")
        request = _promotion_request(generation)
    barrier = threading.Barrier(2)
    outcomes: list[bool] = []
    errors: list[BaseException] = []

    def promote() -> None:
        with Session(engine) as session:
            try:
                barrier.wait(timeout=10)
                result = ConnectorSyncWorkLedgerRepository(session).promote_generation(
                    request, now=NOW + timedelta(minutes=1)
                )
                session.commit()
                outcomes.append(result.promoted)
            except BaseException as exc:  # pragma: no cover - asserted below
                session.rollback()
                errors.append(exc)

    threads = [threading.Thread(target=promote) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(20)
        assert not thread.is_alive()
    assert errors == []
    assert sorted(outcomes) == [False, True]
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(ConnectorSyncGenerationActivation)) == 1


def test_new_generation_registration_serializes_before_stale_promotion(engine) -> None:
    with Session(engine, expire_on_commit=False) as setup:
        context, generation, *_ = _ready_promotion(
            setup, "RegistrationPromotionRace"
        )
        newer_job_id = uuid4()
        setup.execute(
            text(
                """INSERT INTO connector_sync_jobs
                (id,organization_id,connector_id,connector_scope_id,mode,
                 trigger_type,status,created_at,updated_at)
                VALUES (:id,:org,:connector,:scope,'incremental','manual',
                        'queued',:now,:now)"""
            ),
            {
                "id": newer_job_id,
                "org": context[0],
                "connector": context[1],
                "scope": context[2],
                "now": NOW + timedelta(hours=1),
            },
        )
        setup.commit()

    newer_request = replace(
        _generation_request((context[0], context[1], context[2], newer_job_id)),
        commit_object_id="f" * 40,
        root_tree_object_id="e" * 40,
        created_at=NOW + timedelta(hours=1),
    )
    registration_ready = threading.Event()
    release_registration = threading.Event()
    promotion_started = threading.Event()
    registration_errors: list[BaseException] = []
    promotion_errors: list[BaseException] = []

    def register_newer() -> None:
        with Session(engine, expire_on_commit=False) as session:
            try:
                ConnectorSyncWorkLedgerRepository(session).register_generation(
                    newer_request
                )
                registration_ready.set()
                if not release_registration.wait(10):
                    raise TimeoutError("registration release timed out")
                session.commit()
            except BaseException as exc:  # pragma: no cover - asserted below
                session.rollback()
                registration_errors.append(exc)
                registration_ready.set()

    def promote_older() -> None:
        with Session(engine, expire_on_commit=False) as session:
            try:
                session.execute(
                    text(
                        "SET LOCAL application_name = "
                        "'phase3-slice4-promotion-race'"
                    )
                )
                promotion_started.set()
                ConnectorSyncWorkLedgerRepository(session).promote_generation(
                    _promotion_request(generation), now=NOW + timedelta(hours=2)
                )
                session.commit()
            except BaseException as exc:  # pragma: no cover - asserted below
                session.rollback()
                promotion_errors.append(exc)

    registrar = threading.Thread(target=register_newer)
    registrar.start()
    assert registration_ready.wait(10)
    assert registration_errors == []

    promoter = threading.Thread(target=promote_older)
    promoter.start()
    assert promotion_started.wait(10)
    blocked = False
    with engine.connect() as observation:
        for _ in range(100):
            blocked = bool(
                observation.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE application_name = "
                        "'phase3-slice4-promotion-race' "
                        "AND wait_event_type = 'Lock'"
                    )
                )
            )
            if blocked:
                break
            time.sleep(0.02)
    assert blocked
    release_registration.set()
    registrar.join(10)
    promoter.join(10)
    assert not registrar.is_alive()
    assert not promoter.is_alive()
    assert registration_errors == []
    assert len(promotion_errors) == 1
    assert isinstance(promotion_errors[0], SyncWorkLedgerConflict)
    assert str(promotion_errors[0]) == "stale generation cannot be promoted"

    with Session(engine) as verification:
        assert verification.scalar(
            select(func.count()).select_from(ConnectorSyncGenerationActivation)
        ) == 0
        assert verification.scalar(
            select(func.count())
            .select_from(ConnectorSyncGeneration)
            .where(ConnectorSyncGeneration.sync_job_id == newer_job_id)
        ) == 1


def test_global_claim_rejects_profile_mismatch_and_cancelled_job(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "ClaimRestrictions")
        _register(session, context[0], generation.generation_id, (_entry(1),))
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.mark_discovery_complete(context[0], generation.generation_id, now=NOW)
        session.commit()
        assert repository.claim_next_available(
            provider_key="github", profile_fingerprint="other:profile",
            worker_id="phase3-worker", now=NOW, lease_duration=LEASE,
        ) is None
        session.execute(
            text(
                    "UPDATE connector_sync_jobs SET cancel_requested_at=created_at, "
                    "cancel_reason_code='operator_cancelled' WHERE id=:job"
                ),
                {"job": context[3]},
        )
        session.commit()
        assert repository.claim_next_available(
            provider_key="github", profile_fingerprint=PROFILE,
            worker_id="phase3-worker", now=NOW, lease_duration=LEASE,
        ) is None


def test_github_planner_persists_multiple_bounded_batches_resumes_and_replays(
    engine,
) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context = _github_planning_context(_setup(session, "GitHubPlanner"))
        service = GitHubSyncWorkPlanningService(
            ConnectorSyncWorkLedgerRepository(session)
        )
        first = _plan(
            service,
            context,
            tuple(_github_entry(context[-1], index) for index in range(500)),
        )
        session.commit()
        assert (first.created_count, first.existing_count) == (500, 0)
        generation_id = first.generation_id

    # A new transaction/session models resumption after an interrupted worker.
    with Session(engine, expire_on_commit=False) as session:
        service = GitHubSyncWorkPlanningService(
            ConnectorSyncWorkLedgerRepository(session)
        )
        resumed = _plan(
            service,
            context,
            tuple(_github_entry(context[-1], index) for index in range(500, 537)),
        )
        session.commit()
        assert resumed.generation_id == generation_id
        assert resumed.generation_created is False
        assert (resumed.created_count, resumed.existing_count) == (37, 0)

        completed = service.mark_discovery_complete(
            organization_id=context[0],
            connector_id=context[1],
            connector_scope_id=context[2],
            sync_job_id=context[3],
            authorization=context[4],
            snapshot=context[5],
            profile_fingerprint=PROFILE,
            now=NOW,
        )
        session.commit()
        assert completed.discovery_complete is True
        assert completed.reconciliation_eligible is False

        replay = _plan(
            service,
            context,
            tuple(_github_entry(context[-1], index) for index in range(500)),
        )
        session.commit()
        assert replay.generation_id == generation_id
        assert (replay.created_count, replay.existing_count) == (0, 500)

        repository = ConnectorSyncWorkLedgerRepository(session)
        summary = repository.barrier_summary(context[0], generation_id)
        assert summary.discovery_complete is True
        assert summary.total_items == summary.pending_items == 537
        assert summary.barrier_open is False
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncFileWorkItem).where(
                ConnectorSyncFileWorkItem.generation_id == generation_id
            )
        ) == 537


def test_github_planner_concurrent_replay_is_unique_and_conflicts_fail_closed(
    engine,
) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context = _github_planning_context(_setup(session, "GitHubConcurrent"))
    entries = tuple(_github_entry(context[-1], index) for index in range(100))
    start = threading.Barrier(8)
    results: list[object] = []
    errors: list[BaseException] = []
    guard = threading.Lock()

    def plan() -> None:
        with Session(engine, expire_on_commit=False) as session:
            try:
                start.wait(timeout=20)
                result = _plan(
                    GitHubSyncWorkPlanningService(
                        ConnectorSyncWorkLedgerRepository(session)
                    ),
                    context,
                    entries,
                )
                session.commit()
                with guard:
                    results.append(result)
            except BaseException as exc:  # pragma: no cover - asserted below
                session.rollback()
                with guard:
                    errors.append(exc)

    threads = [threading.Thread(target=plan) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert len(results) == 8
    assert sum(result.created_count for result in results) == 100
    assert len({result.generation_id for result in results}) == 1

    with Session(engine, expire_on_commit=False) as session:
        generation_id = results[0].generation_id
        rows = session.scalars(
            select(ConnectorSyncFileWorkItem).where(
                ConnectorSyncFileWorkItem.generation_id == generation_id
            )
        ).all()
        assert len(rows) == len({row.id for row in rows}) == 100
        service = GitHubSyncWorkPlanningService(
            ConnectorSyncWorkLedgerRepository(session)
        )
        conflicting = replace(entries[0], object_id="f" * 40)
        with pytest.raises(SyncWorkLedgerConflict, match="observation identity"):
            _plan(service, context, (conflicting,))
        session.rollback()
        assert session.scalar(
            select(func.count()).select_from(ConnectorSyncFileWorkItem).where(
                ConnectorSyncFileWorkItem.generation_id == generation_id
            )
        ) == 100


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
        assert repository.claim_next(
            context[0], generation.generation_id,
            worker_id="early-platform-retry", now=NOW + timedelta(minutes=9),
            lease_duration=LEASE,
        ) is None
        session.rollback()

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
        _register(
            session,
            context[0],
            generation.generation_id,
            tuple(_entry(index) for index in range(500)),
        )
        ConnectorSyncWorkLedgerRepository(session).mark_discovery_complete(
            context[0], generation.generation_id, now=NOW
        )
        session.commit()
        assert _fair_claim(session, "fair-plan-seed") is not None
        session.execute(text("SET LOCAL enable_seqscan = off"))
        session.execute(text("SET LOCAL enable_sort = off"))
        session.execute(text("SET LOCAL enable_bitmapscan = off"))
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
        fairness_plan = "\n".join(
            row[0]
            for row in session.execute(
                text(
                    """EXPLAIN (COSTS OFF)
                       SELECT schedule.organization_id
                       FROM connector_sync_organization_claim_schedules AS schedule
                       WHERE (
                         SELECT work.id
                         FROM connector_sync_file_work_items AS work
                         JOIN connector_sync_generations AS generation
                           ON generation.organization_id = work.organization_id
                          AND generation.connector_id = work.connector_id
                          AND generation.connector_scope_id = work.connector_scope_id
                          AND generation.id = work.generation_id
                          AND generation.profile_fingerprint = work.profile_fingerprint
                         JOIN connector_sync_jobs AS job
                           ON job.organization_id = generation.organization_id
                          AND job.connector_id = generation.connector_id
                          AND job.connector_scope_id = generation.connector_scope_id
                          AND job.id = generation.sync_job_id
                         WHERE work.organization_id = schedule.organization_id
                           AND generation.provider_key = 'github'
                           AND generation.profile_fingerprint = :profile
                           AND work.profile_fingerprint = :profile
                           AND generation.status = 'processing'
                           AND generation.discovery_complete IS TRUE
                           AND job.status != 'cancelled'
                           AND job.cancel_requested_at IS NULL
                           AND work.status IN ('pending','retry_wait')
                           AND work.next_attempt_at <= :now
                           AND work.cancel_requested_at IS NULL
                           AND work.attempt_count < work.max_attempts
                         LIMIT 1
                       ) IS NOT NULL
                       ORDER BY schedule.last_claim_sequence, schedule.organization_id
                       LIMIT 1 FOR UPDATE OF schedule SKIP LOCKED"""
                ),
                {"profile": PROFILE, "now": NOW},
            )
        )
        fair_probe_plan = "\n".join(
            row[0]
            for row in session.execute(
                text(
                    """EXPLAIN (COSTS OFF)
                       SELECT id FROM connector_sync_file_work_items
                       WHERE organization_id=:org
                         AND profile_fingerprint=:profile
                         AND status IN ('pending','retry_wait')
                         AND next_attempt_at <= :now
                         AND cancel_requested_at IS NULL
                         AND attempt_count < max_attempts
                       ORDER BY next_attempt_at,generation_id,id
                       LIMIT 1"""
                ),
                {"org": context[0], "profile": PROFILE, "now": NOW},
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
        assert "ix_sync_org_claim_schedules_fair_order" in fairness_plan
        assert "Seq Scan on connector_sync_file_work_items" not in fairness_plan
        assert "ix_sync_file_work_fair_eligible" in fair_probe_plan
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

        replay_started = time.perf_counter()
        for start in range(0, 10_000, 500):
            replay = ConnectorSyncWorkLedgerRepository(session).register_manifest(
                context[0],
                generation.generation_id,
                [_entry(index) for index in range(start, start + 500)],
                now=NOW,
            )
            assert (replay.created_count, replay.existing_count) == (0, 500)
            session.commit()
        replay_seconds = time.perf_counter() - replay_started

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
            "registration_batch_count": 20,
            "maximum_batch_size": 500,
            "insert_items_per_second": round(10_000 / insert_seconds, 2),
            "replay_items_per_second": round(10_000 / replay_seconds, 2),
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


def test_generation_reconciliation_retires_absent_lifecycle_and_replays(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, *_ = _ready_promotion(session, "ReconcileAbsent")
        absent_source, absent_version, absent_document, absent_chunk = (
            _add_absent_legacy_source(
                session, context, generation, path="documents/removed.md"
            )
        )
        user_id = _retrieval_user(session, context[0], context[2])
        assert absent_chunk in _retrieval_chunk_ids(session, context[0], user_id)
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.promote_generation(
            _promotion_request(generation), now=NOW + timedelta(hours=1)
        )
        session.commit()
        activated_ids = _retrieval_chunk_ids(session, context[0], user_id)
        assert absent_chunk not in activated_ids
        assert len(activated_ids) == 1

        result = repository.reconcile_generation(
            _reconciliation_request(generation),
            now=NOW + timedelta(hours=2),
            limit=10,
        )
        session.commit()
        assert result.completed is True and result.replayed is False
        assert (
            result.memberships_retired,
            result.sources_retired,
            result.documents_retired,
        ) == (1, 1, 1)
        membership = session.scalar(
            select(SourceItemScopeMembership).where(
                SourceItemScopeMembership.connector_scope_id == context[2],
                SourceItemScopeMembership.source_item_id == absent_source,
            )
        )
        source = session.get(SourceItem, absent_source)
        document = session.get(Document, absent_document)
        versions = session.scalars(
            select(DocumentVersion)
            .where(DocumentVersion.source_item_id == absent_source)
            .order_by(DocumentVersion.version_number)
        ).all()
        assert membership is not None and membership.status == "removed"
        assert source is not None and source.status == "deleted"
        assert document is not None and document.deleted_at is not None
        assert [(row.version_number, row.lifecycle, row.is_current) for row in versions] == [
            (1, "available", False),
            (2, "deleted", True),
        ]
        assert session.get(DocumentChunk, absent_chunk) is not None
        assert session.get(DocumentVersion, absent_version) is not None
        assert session.scalar(
            select(func.count(DocumentIndexingState.id)).where(
                DocumentIndexingState.document_version_id == absent_version
            )
        ) == 1
        assert _retrieval_chunk_ids(session, context[0], user_id) == activated_ids
        generation_row = session.get(ConnectorSyncGeneration, generation.generation_id)
        assert generation_row is not None
        assert generation_row.reconciliation_eligible is True
        assert generation_row.reconciliation_completed_at is not None
        assert generation_row.reconciled_membership_count == 1

        replay = repository.reconcile_generation(
            _reconciliation_request(generation),
            now=NOW + timedelta(hours=3),
            limit=10,
        )
        session.commit()
        assert replay.completed is replay.replayed is True
        assert replay.memberships_retired == 0
        assert session.scalar(
            select(func.count(DocumentVersion.id)).where(
                DocumentVersion.source_item_id == absent_source
            )
        ) == 2


def test_reconciliation_preserves_shared_scope_and_rolls_back_atomically(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, *_ = _ready_promotion(session, "ReconcileShared")
        other_space, other_scope = uuid4(), uuid4()
        session.execute(
            text(
                "INSERT INTO knowledge_spaces (id,organization_id,name,slug) "
                "VALUES (:id,:org,'Shared','shared-space')"
            ),
            {"id": other_space, "org": context[0]},
        )
        session.execute(
            text(
                "INSERT INTO connector_scopes "
                "(id,organization_id,connector_id,knowledge_space_id,display_name,slug,"
                "scope_type,external_scope_key,access_mode,status) VALUES "
                "(:id,:org,:connector,:space,'Shared','shared-scope','repository',"
                ":key,'platform_managed','active')"
            ),
            {
                "id": other_scope,
                "org": context[0],
                "connector": context[1],
                "space": other_space,
                "key": "github:repository:999999",
            },
        )
        session.commit()
        absent_source, absent_version, absent_document, shared_chunk = _add_absent_legacy_source(
            session,
            context,
            generation,
            path="documents/shared.md",
            additional_scope_id=other_scope,
        )
        shared_user = _retrieval_user(session, context[0], other_scope)
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.promote_generation(
            _promotion_request(generation), now=NOW + timedelta(hours=1)
        )
        session.commit()
        prepared = repository.reconcile_generation(
            _reconciliation_request(generation), now=NOW + timedelta(hours=2)
        )
        assert prepared.memberships_retired == 1
        session.rollback()
        assert session.scalar(
            select(SourceItemScopeMembership.status).where(
                SourceItemScopeMembership.source_item_id == absent_source,
                SourceItemScopeMembership.connector_scope_id == context[2],
            )
        ) == "active"

        result = repository.reconcile_generation(
            _reconciliation_request(generation), now=NOW + timedelta(hours=2)
        )
        session.commit()
        assert (result.memberships_retired, result.sources_retired) == (1, 0)
        memberships = session.scalars(
            select(SourceItemScopeMembership).where(
                SourceItemScopeMembership.source_item_id == absent_source
            )
        ).all()
        assert {row.connector_scope_id: row.status for row in memberships} == {
            context[2]: "removed",
            other_scope: "active",
        }
        assert session.get(SourceItem, absent_source).status == "active"
        assert session.get(Document, absent_document).deleted_at is None
        assert session.get(DocumentVersion, absent_version).is_current is True
        assert shared_chunk in _retrieval_chunk_ids(
            session, context[0], shared_user
        )


def test_empty_repository_reconciles_bounded_and_stale_generation_fails(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "ReconcileEmpty")
        session.execute(
            text("UPDATE connector_scopes SET external_scope_key=:key WHERE id=:scope"),
            {"key": generation.repository_identity, "scope": context[2]},
        )
        session.execute(
            text(
                "UPDATE connector_sync_jobs SET status='succeeded',attempt_count=1,"
                "fencing_token=1,next_attempt_at=NULL,completed_at=created_at WHERE id=:job"
            ),
            {"job": context[3]},
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.mark_discovery_complete(context[0], generation.generation_id, now=NOW)
        session.commit()
        source_ids = [
            _add_absent_legacy_source(
                session,
                context,
                generation,
                path=f"documents/removed-{index}.md",
            )[0]
            for index in range(3)
        ]
        repository.promote_generation(
            _promotion_request(generation), now=NOW + timedelta(hours=1)
        )
        session.commit()
        first = repository.reconcile_generation(
            _reconciliation_request(generation),
            now=NOW + timedelta(hours=2),
            limit=2,
        )
        session.commit()
        assert first.completed is False and first.memberships_retired == 2
        _new_generation_same_scope(
            session, context, created_at=NOW + timedelta(hours=3)
        )
        with pytest.raises(SyncWorkLedgerConflict, match="stale generation"):
            repository.reconcile_generation(
                _reconciliation_request(generation), now=NOW + timedelta(hours=4)
            )
        session.rollback()
        assert sum(session.get(SourceItem, source_id).status == "deleted" for source_id in source_ids) == 2


def test_reconciliation_over_500_candidates_commits_resumes_and_replays(
    engine, monkeypatch
) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "ReconcileLarge")
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.mark_discovery_complete(
            context[0], generation.generation_id, now=NOW
        )
        session.execute(
            text("UPDATE connector_scopes SET external_scope_key=:key WHERE id=:scope"),
            {"key": generation.repository_identity, "scope": context[2]},
        )
        session.execute(
            text(
                "UPDATE connector_sync_jobs SET status='succeeded',attempt_count=1,"
                "fencing_token=1,next_attempt_at=NULL,completed_at=created_at WHERE id=:job"
            ),
            {"job": context[3]},
        )
        source_ids = [uuid4() for _ in range(501)]
        session.execute(
            SourceItem.__table__.insert(),
            [
                {
                    "id": source_id,
                    "organization_id": context[0],
                    "connector_id": context[1],
                    "source_item_key": (
                        f"{generation.repository_identity}:path:removed-{index:04d}.md"
                    ),
                    "source_item_type": "file",
                    "title": f"removed-{index:04d}.md",
                    "first_seen_at": NOW,
                    "last_seen_at": NOW,
                    "status": "active",
                    "metadata": {
                        "provider": "github",
                        "repository_identity": generation.repository_identity,
                        "repository_path": f"removed-{index:04d}.md",
                    },
                    "metadata_schema_version": 1,
                }
                for index, source_id in enumerate(source_ids)
            ],
        )
        session.execute(
            SourceItemScopeMembership.__table__.insert(),
            [
                {
                    "id": uuid4(),
                    "organization_id": context[0],
                    "connector_id": context[1],
                    "source_item_id": source_id,
                    "connector_scope_id": context[2],
                    "status": "active",
                    "first_discovered_at": NOW,
                    "last_seen_at": NOW,
                }
                for source_id in source_ids
            ],
        )
        repository.promote_generation(
            _promotion_request(generation), now=NOW + timedelta(hours=1)
        )
        session.commit()
        projection_validations = 0
        original_validate = repository._validate_reconciliation_projection

        def count_projection_validation(persisted_generation):
            nonlocal projection_validations
            projection_validations += 1
            return original_validate(persisted_generation)

        monkeypatch.setattr(
            repository,
            "_validate_reconciliation_projection",
            count_projection_validation,
        )

        rolled_back = repository.reconcile_generation(
            _reconciliation_request(generation),
            now=NOW + timedelta(hours=2),
            limit=500,
        )
        assert rolled_back.completed is False
        assert rolled_back.memberships_retired == 500
        session.rollback()
        assert session.scalar(
            select(func.count(SourceItemScopeMembership.id)).where(
                SourceItemScopeMembership.connector_scope_id == context[2],
                SourceItemScopeMembership.status == "active",
            )
        ) == 501
        persisted = session.get(
            ConnectorSyncGeneration, generation.generation_id
        )
        assert persisted.reconciled_membership_count == 0
        assert persisted.reconciliation_started_at is None

        first = repository.reconcile_generation(
            _reconciliation_request(generation),
            now=NOW + timedelta(hours=2),
            limit=500,
        )
        session.commit()
        assert first.completed is False
        assert (
            first.memberships_retired,
            first.sources_retired,
            first.documents_retired,
        ) == (500, 500, 0)
        second = repository.reconcile_generation(
            _reconciliation_request(generation),
            now=NOW + timedelta(hours=3),
            limit=500,
        )
        session.commit()
        assert second.completed is True
        assert (
            second.memberships_retired,
            second.sources_retired,
            second.documents_retired,
        ) == (1, 1, 0)
        assert (
            second.total_memberships_retired,
            second.total_sources_retired,
            second.total_documents_retired,
        ) == (501, 501, 0)
        assert session.scalar(
            select(func.count(DocumentVersion.id)).where(
                DocumentVersion.source_item_id.in_(source_ids),
                DocumentVersion.lifecycle == "deleted",
                DocumentVersion.is_current.is_(True),
            )
        ) == 501
        replay = repository.reconcile_generation(
            _reconciliation_request(generation),
            now=NOW + timedelta(hours=4),
            limit=500,
        )
        session.commit()
        assert replay.completed is replay.replayed is True
        assert replay.memberships_retired == replay.sources_retired == 0
        assert replay.total_memberships_retired == 501
        # Rollback forces the initial proof to run again; committed progress
        # avoids an O(generation-size) rescan on each continuation and replay.
        assert projection_validations == 2


def test_observed_unindexable_source_is_not_treated_as_deleted(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation = _generation(session, "ReconcileUnsupported")
        path = "code/present.py"
        source_id, version_id, document_id, chunk_id = _add_absent_legacy_source(
            session, context, generation, path=path
        )
        user_id = _retrieval_user(session, context[0], context[2])
        assert chunk_id in _retrieval_chunk_ids(session, context[0], user_id)
        observation = GenerationSourceObservation(
            f"{generation.repository_identity}:path:{path}",
            path,
            "f" * 40,
            generation.commit_object_id,
            generation.profile_fingerprint,
            "regular_blob",
            GenerationObservationDisposition.UNSUPPORTED_FORMAT,
            10,
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.register_discovery_batch(
            context[0], generation.generation_id, (observation,), (), now=NOW
        )
        repository.mark_discovery_complete(context[0], generation.generation_id, now=NOW)
        session.execute(
            text("UPDATE connector_scopes SET external_scope_key=:key WHERE id=:scope"),
            {"key": generation.repository_identity, "scope": context[2]},
        )
        session.execute(
            text(
                "UPDATE connector_sync_jobs SET status='succeeded',attempt_count=1,"
                "fencing_token=1,next_attempt_at=NULL,completed_at=created_at WHERE id=:job"
            ),
            {"job": context[3]},
        )
        session.commit()
        repository.promote_generation(
            _promotion_request(generation), now=NOW + timedelta(hours=1)
        )
        session.commit()
        assert chunk_id not in _retrieval_chunk_ids(session, context[0], user_id)
        result = repository.reconcile_generation(
            _reconciliation_request(generation), now=NOW + timedelta(hours=2)
        )
        session.commit()
        assert result.completed is True and result.memberships_retired == 0
        assert session.get(SourceItem, source_id).status == "active"
        assert session.get(DocumentVersion, version_id).is_current is True
        assert session.get(Document, document_id).deleted_at is None
        assert chunk_id not in _retrieval_chunk_ids(session, context[0], user_id)


def test_reconciliation_rejects_projection_drift_and_cross_tenant_request(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, work, *_ = _ready_promotion(
            session, "ReconcileFailClosed"
        )
        source_id, *_ = _add_absent_legacy_source(
            session, context, generation, path="documents/unchanged-on-failure.md"
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.promote_generation(
            _promotion_request(generation), now=NOW + timedelta(hours=1)
        )
        session.commit()

        with pytest.raises(SyncWorkLedgerConflict, match="time moved backward"):
            repository.reconcile_generation(
                _reconciliation_request(generation), now=NOW
            )
        session.rollback()
        with pytest.raises(SyncWorkLedgerNotFound):
            repository.reconcile_generation(
                replace(_reconciliation_request(generation), organization_id=uuid4()),
                now=NOW + timedelta(hours=2),
            )
        session.rollback()
        session.execute(
            text(
                "UPDATE connector_sync_file_work_items SET status='failed', "
                "last_error_category='internal', last_error_code='forced_drift' "
                "WHERE id=:work"
            ),
            {"work": work.work_item_id},
        )
        session.flush()
        with pytest.raises(SyncWorkLedgerConflict, match="not completely successful"):
            repository.reconcile_generation(
                _reconciliation_request(generation), now=NOW + timedelta(hours=2)
            )
        session.rollback()
        observation_id = session.scalar(
            select(ConnectorSyncGenerationObservation.id).where(
                ConnectorSyncGenerationObservation.generation_id
                == generation.generation_id
            )
        )
        assert observation_id is not None
        session.execute(
            ConnectorSyncGenerationObservation.__table__.delete().where(
                ConnectorSyncGenerationObservation.id == observation_id
            )
        )
        with pytest.raises(SyncWorkLedgerConflict, match="observations are incomplete"):
            repository.reconcile_generation(
                _reconciliation_request(generation), now=NOW + timedelta(hours=2)
            )
        session.rollback()
        chunk_id = session.scalar(
            select(ConnectorSyncFileMaterializationChunk.id).where(
                ConnectorSyncFileMaterializationChunk.generation_id
                == generation.generation_id
            )
        )
        assert chunk_id is not None
        session.execute(
            ConnectorSyncFileMaterializationChunk.__table__.delete().where(
                ConnectorSyncFileMaterializationChunk.id == chunk_id
            )
        )
        with pytest.raises(
            SyncWorkLedgerConflict, match="materialization chunks are incomplete"
        ):
            repository.reconcile_generation(
                _reconciliation_request(generation), now=NOW + timedelta(hours=2)
            )
        session.rollback()
        membership_status = session.scalar(
            select(SourceItemScopeMembership.status).where(
                SourceItemScopeMembership.source_item_id == source_id,
                SourceItemScopeMembership.connector_scope_id == context[2],
            )
        )
        assert membership_status == "active"


def test_concurrent_reconciliation_serializes_and_creates_one_tombstone(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, *_ = _ready_promotion(
            session, "ReconcileConcurrent"
        )
        source_id, *_ = _add_absent_legacy_source(
            session, context, generation, path="documents/concurrent-delete.md"
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.promote_generation(
            _promotion_request(generation), now=NOW + timedelta(hours=1)
        )
        session.commit()

    start = threading.Barrier(2)
    results: list[object] = []
    errors: list[BaseException] = []
    guard = threading.Lock()

    def reconcile() -> None:
        with Session(engine, expire_on_commit=False) as session:
            try:
                start.wait(timeout=20)
                result = ConnectorSyncWorkLedgerRepository(
                    session
                ).reconcile_generation(
                    _reconciliation_request(generation),
                    now=NOW + timedelta(hours=2),
                )
                session.commit()
                with guard:
                    results.append(result)
            except BaseException as exc:  # pragma: no cover - asserted below
                session.rollback()
                with guard:
                    errors.append(exc)

    threads = [threading.Thread(target=reconcile) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert len(results) == 2
    assert sum(result.memberships_retired for result in results) == 1
    assert sum(result.replayed for result in results) == 1

    with Session(engine, expire_on_commit=False) as session:
        versions = session.scalars(
            select(DocumentVersion)
            .where(DocumentVersion.source_item_id == source_id)
            .order_by(DocumentVersion.version_number)
        ).all()
        assert [(row.version_number, row.lifecycle) for row in versions] == [
            (1, "available"),
            (2, "deleted"),
        ]


def test_newer_synchronization_fails_closed_before_lifecycle_changes(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, *_ = _ready_promotion(session, "ReconcileNewerSync")
        source_id, *_ = _add_absent_legacy_source(
            session, context, generation, path="documents/newer-sync.md"
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.promote_generation(
            _promotion_request(generation), now=NOW + timedelta(hours=1)
        )
        session.commit()
        newer_job = uuid4()
        original_job_created_at = session.scalar(
            text("SELECT created_at FROM connector_sync_jobs WHERE id=:job"),
            {"job": context[3]},
        )
        assert original_job_created_at is not None
        newer_job_created_at = original_job_created_at + timedelta(seconds=1)
        session.execute(
            text(
                "INSERT INTO connector_sync_jobs "
                "(id,organization_id,connector_id,connector_scope_id,mode,trigger_type,"
                "status,attempt_count,fencing_token,next_attempt_at,completed_at,"
                "created_at,updated_at) VALUES "
                "(:id,:org,:connector,:scope,'incremental','manual','succeeded',1,1,"
                "NULL,:created,:created,:created)"
            ),
            {
                "id": newer_job,
                "org": context[0],
                "connector": context[1],
                "scope": context[2],
                "created": newer_job_created_at,
            },
        )
        session.commit()
        with pytest.raises(SyncWorkLedgerConflict, match="newer synchronization"):
            repository.reconcile_generation(
                _reconciliation_request(generation),
                now=NOW + timedelta(hours=3),
            )
        session.rollback()
        assert session.scalar(
            select(SourceItemScopeMembership.status).where(
                SourceItemScopeMembership.source_item_id == source_id,
                SourceItemScopeMembership.connector_scope_id == context[2],
            )
        ) == "active"


def test_concurrent_enqueue_serializes_before_reconciliation_decision(engine) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, *_ = _ready_promotion(
            session, "ReconcileConcurrentEnqueue"
        )
        source_id, *_ = _add_absent_legacy_source(
            session, context, generation, path="documents/concurrent-enqueue.md"
        )
        repository = ConnectorSyncWorkLedgerRepository(session)
        repository.promote_generation(
            _promotion_request(generation), now=NOW + timedelta(hours=1)
        )
        session.commit()

    enqueue_locked = threading.Event()
    allow_enqueue_commit = threading.Event()
    reconcile_started = threading.Event()
    errors: list[BaseException] = []
    reconciliation_errors: list[BaseException] = []

    def enqueue() -> None:
        with Session(engine, expire_on_commit=False) as session:
            try:
                result = ConnectorSyncJobRepository(session).enqueue_or_coalesce(
                    context[0],
                    context[1],
                    context[2],
                    mode="incremental",
                    trigger_type="manual",
                    now=NOW + timedelta(hours=2),
                )
                assert result.coalesced is False
                enqueue_locked.set()
                assert allow_enqueue_commit.wait(timeout=20)
                session.commit()
            except BaseException as exc:  # pragma: no cover - asserted below
                session.rollback()
                errors.append(exc)

    def reconcile() -> None:
        with Session(engine, expire_on_commit=False) as session:
            try:
                reconcile_started.set()
                ConnectorSyncWorkLedgerRepository(session).reconcile_generation(
                    _reconciliation_request(generation),
                    now=NOW + timedelta(hours=3),
                )
                session.commit()
            except BaseException as exc:  # expected conflict after enqueue commits
                session.rollback()
                reconciliation_errors.append(exc)

    enqueue_thread = threading.Thread(target=enqueue)
    enqueue_thread.start()
    assert enqueue_locked.wait(timeout=20)
    reconcile_thread = threading.Thread(target=reconcile)
    reconcile_thread.start()
    assert reconcile_started.wait(timeout=20)
    time.sleep(0.1)
    assert reconcile_thread.is_alive()
    allow_enqueue_commit.set()
    enqueue_thread.join(20)
    reconcile_thread.join(20)

    assert not enqueue_thread.is_alive() and not reconcile_thread.is_alive()
    assert errors == []
    assert len(reconciliation_errors) == 1
    assert isinstance(reconciliation_errors[0], SyncWorkLedgerConflict)
    assert "active synchronization" in str(reconciliation_errors[0])
    with Session(engine) as session:
        assert session.scalar(
            select(SourceItemScopeMembership.status).where(
                SourceItemScopeMembership.source_item_id == source_id,
                SourceItemScopeMembership.connector_scope_id == context[2],
            )
        ) == "active"
        persisted = session.get(
            ConnectorSyncGeneration, generation.generation_id
        )
        assert persisted.reconciliation_started_at is None
        assert persisted.reconciled_membership_count == 0


def test_concurrent_shared_membership_creation_precedes_last_membership_decision(
    engine,
) -> None:
    with Session(engine, expire_on_commit=False) as session:
        context, generation, *_ = _ready_promotion(
            session, "ReconcileConcurrentMembership"
        )
        source_id, version_id, document_id, _ = _add_absent_legacy_source(
            session, context, generation, path="documents/concurrent-shared.md"
        )
        other_space, other_scope = uuid4(), uuid4()
        session.execute(
            text(
                "INSERT INTO knowledge_spaces (id,organization_id,name,slug) "
                "VALUES (:id,:org,'Concurrent Shared',:slug)"
            ),
            {
                "id": other_space,
                "org": context[0],
                "slug": f"concurrent-shared-{other_space}",
            },
        )
        session.execute(
            text(
                "INSERT INTO connector_scopes "
                "(id,organization_id,connector_id,knowledge_space_id,display_name,slug,"
                "scope_type,external_scope_key,access_mode,status) VALUES "
                "(:id,:org,:connector,:space,'Concurrent Shared',:slug,'repository',"
                ":key,'platform_managed','active')"
            ),
            {
                "id": other_scope,
                "org": context[0],
                "connector": context[1],
                "space": other_space,
                "slug": f"concurrent-shared-{other_scope}",
                "key": f"github:repository:{other_scope.int}",
            },
        )
        ConnectorSyncWorkLedgerRepository(session).promote_generation(
            _promotion_request(generation), now=NOW + timedelta(hours=1)
        )
        session.commit()

    source_locked = threading.Event()
    allow_membership_commit = threading.Event()
    reconciliation_started = threading.Event()
    creator_errors: list[BaseException] = []
    reconciliation_results: list[object] = []
    reconciliation_errors: list[BaseException] = []

    def create_membership() -> None:
        with Session(engine, expire_on_commit=False) as session:
            try:
                assert ConnectorScopeRepository(session).lock_by_id(
                    context[0], other_scope
                ) is not None
                sources = SourceItemRepository(session)
                assert sources.lock_by_id(context[0], context[1], source_id) is not None
                source_locked.set()
                assert allow_membership_commit.wait(timeout=20)
                sources.add_membership(
                    context[0],
                    context[1],
                    SourceItemScopeMembership(
                        id=uuid4(),
                        organization_id=context[0],
                        connector_id=context[1],
                        source_item_id=source_id,
                        connector_scope_id=other_scope,
                        status="active",
                        first_discovered_at=NOW + timedelta(hours=2),
                        last_seen_at=NOW + timedelta(hours=2),
                        removed_at=None,
                    ),
                )
                session.commit()
            except BaseException as exc:  # pragma: no cover - asserted below
                session.rollback()
                creator_errors.append(exc)

    def reconcile() -> None:
        with Session(engine, expire_on_commit=False) as session:
            try:
                reconciliation_started.set()
                result = ConnectorSyncWorkLedgerRepository(
                    session
                ).reconcile_generation(
                    _reconciliation_request(generation),
                    now=NOW + timedelta(hours=3),
                )
                session.commit()
                reconciliation_results.append(result)
            except BaseException as exc:  # pragma: no cover - asserted below
                session.rollback()
                reconciliation_errors.append(exc)

    creator = threading.Thread(target=create_membership)
    creator.start()
    assert source_locked.wait(timeout=20)
    reconciler = threading.Thread(target=reconcile)
    reconciler.start()
    assert reconciliation_started.wait(timeout=20)
    time.sleep(0.1)
    assert reconciler.is_alive()
    allow_membership_commit.set()
    creator.join(20)
    reconciler.join(20)

    assert not creator.is_alive() and not reconciler.is_alive()
    assert creator_errors == reconciliation_errors == []
    assert len(reconciliation_results) == 1
    assert (
        reconciliation_results[0].memberships_retired,
        reconciliation_results[0].sources_retired,
        reconciliation_results[0].documents_retired,
    ) == (1, 0, 0)
    with Session(engine) as session:
        memberships = session.scalars(
            select(SourceItemScopeMembership).where(
                SourceItemScopeMembership.source_item_id == source_id
            )
        ).all()
        assert {row.connector_scope_id: row.status for row in memberships} == {
            context[2]: "removed",
            other_scope: "active",
        }
        assert session.get(SourceItem, source_id).status == "active"
        assert session.get(DocumentVersion, version_id).is_current is True
        assert session.get(Document, document_id).deleted_at is None
