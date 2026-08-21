from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import subprocess
from threading import Barrier
import uuid

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from application.services.connector_sync_execution_service import ConnectorSyncExecutionService
from application.services.connector_sync_retry_policy import ConnectorSyncRetryPolicy
from application.services.github_repository_content_service import (
    MAX_GITHUB_BLOB_BYTES,
    GitHubRepositoryContentService,
    GitHubRepositoryEntry,
    GitHubRepositorySnapshot,
)
from application.services.github_staged_synchronization_service import (
    GitHubDiscoveredFile,
    GitHubDiscoveryBatch,
    GitHubRunBudget,
    GitHubStagedSynchronizationService,
    GitHubSynchronizationPreparationService,
    GitHubTraversalCursor,
    GitHubTraversalFrame,
    PreparedGitHubBatch,
    PreparedGitHubChunk,
    PreparedGitHubFile,
    StalePreparedGitHubBatch,
)
from application.services.local_document_indexing_service import LocalDocumentIndexingProfile
from domain.embeddings.models import EmbeddingProfile
from infrastructure.db.models import (
    ConnectorSyncItem,
    ConnectorSyncCursor,
    ConnectorSyncError,
    ConnectorSyncRun,
    Document,
    DocumentChunk,
    DocumentIndexingAttempt,
    DocumentIndexingState,
    DocumentVersion,
    DocumentVersionDocument,
    SourceItem,
    SourceItemScopeMembership,
)
from infrastructure.repositories.connector_sync_job_repository import ConnectorSyncJobRepository
from infrastructure.repositories.connector_sync_job_repository import (
    LostSyncJobLease,
    StaleSyncJobFence,
    SyncJobCancellationConflict,
)
from infrastructure.repositories.permission_aware_document_chunk_search_repository import (
    PermissionAwareDocumentChunkSearchRepository,
)


ROOT = Path(__file__).resolve().parents[3]
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
INI = ROOT / "alembic.ini"
NOW = datetime(2026, 8, 21, 12, tzinfo=timezone.utc)
COMMIT = "a" * 40
TREE = "b" * 40
BLOB = "c" * 40
CONTENT = b"alpha"
CHECKSUM = hashlib.sha256(CONTENT).hexdigest()
DEFAULT_TEST_DATABASE_URL = (
    "postgresql+psycopg://enterprise_ai_platform:enterprise_ai_platform@127.0.0.1:15432/"
    "enterprise_ai_platform_test"
)


class Client:
    app_id = 123


def _identity(url):
    value = make_url(url)
    return value.drivername, value.host, value.port, value.database


@pytest.fixture(scope="module")
def engine():
    url = os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)
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
            "document_indexing_attempts",
            "document_indexing_states",
            "document_version_documents",
            "document_versions",
            "connector_sync_cursors",
            "connector_sync_errors",
            "connector_sync_items",
            "connector_sync_runs",
            "connector_sync_jobs",
            "source_item_scope_memberships",
            "source_items",
            "document_chunks",
            "documents",
            "connector_scopes",
            "github_app_installations",
            "connector_credentials",
            "connectors",
            "knowledge_spaces",
            "users",
            "organization_settings",
            "organizations",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


def _factory(engine):
    return sessionmaker(
        bind=engine, class_=Session, autoflush=False, expire_on_commit=False
    )


def _seed(factory, *, installation_id=77, account_id=99):
    organization_id, user_id, space_id, connector_id, credential_id, scope_id = (
        uuid.uuid4() for _ in range(6)
    )
    with factory() as session:
        session.execute(
            text("INSERT INTO organizations(id,name,slug) VALUES(:id,'Alpha',:slug)"),
            {"id": organization_id, "slug": str(organization_id)},
        )
        session.execute(
            text(
                "INSERT INTO users(id,organization_id,email,normalized_email,password_hash,display_name) "
                "VALUES(:id,:org,:email,:email,'hash','Admin')"
            ),
            {"id": user_id, "org": organization_id, "email": f"{user_id}@example.test"},
        )
        session.execute(
            text(
                "INSERT INTO knowledge_spaces(id,organization_id,name,slug,status) "
                "VALUES(:id,:org,'Docs',:slug,'active')"
            ),
            {"id": space_id, "org": organization_id, "slug": str(space_id)},
        )
        session.execute(
            text(
                "INSERT INTO connectors(id,organization_id,connector_type,display_name,slug,status,acl_support,capabilities,created_by_user_id) "
                "VALUES(:id,:org,'github','GitHub',:slug,'active','none',CAST(:capabilities AS jsonb),:user)"
            ),
            {
                "id": connector_id,
                "org": organization_id,
                "slug": str(connector_id),
                "capabilities": '{"supports_repository_discovery":true,"supports_repository_selection":true,"supports_bounded_content_reading":true,"supports_staged_synchronization":true}',
                "user": user_id,
            },
        )
        session.execute(
            text(
                "INSERT INTO connector_credentials(id,organization_id,connector_id,provider_key,auth_scheme,status,external_subject,display_label,granted_scopes,created_by_user_id) "
                "VALUES(:id,:org,:connector,'github','app_installation','active',:subject,'fake-org',CAST(:scopes AS jsonb),:user)"
            ),
            {
                "id": credential_id,
                "org": organization_id,
                "connector": connector_id,
                "subject": str(installation_id),
                "scopes": '["contents:read","metadata:read"]',
                "user": user_id,
            },
        )
        session.execute(
            text(
                "INSERT INTO github_app_installations(id,organization_id,connector_id,credential_id,github_app_id,github_installation_id,account_id,account_login,account_type,repository_selection,status,provider_created_at,provider_updated_at,last_verified_at,created_at,updated_at) "
                "VALUES(:id,:org,:connector,:credential,123,:installation,:account,'fake-org','Organization','selected','connected',:now,:now,:now,:now,:now)"
            ),
            {
                "id": uuid.uuid4(),
                "org": organization_id,
                "connector": connector_id,
                "credential": credential_id,
                "installation": installation_id,
                "account": account_id,
                "now": NOW,
            },
        )
        session.execute(
            text(
                "INSERT INTO connector_scopes(id,organization_id,connector_id,knowledge_space_id,display_name,slug,scope_type,external_scope_key,access_mode,status,safe_config,config_schema_version,created_by_user_id,last_validated_at) "
                "VALUES(:id,:org,:connector,:space,'fake-org/docs',:slug,'repository','github:repository:501','platform_managed','active',CAST(:config AS jsonb),1,:user,:now)"
            ),
            {
                "id": scope_id,
                "org": organization_id,
                "connector": connector_id,
                "space": space_id,
                "slug": str(scope_id),
                "config": '{"repository_id":501,"repository_name":"docs","repository_full_name":"fake-org/docs","owner_login":"fake-org","private":true,"visibility":"private","archived":false,"disabled":false,"default_branch":"main"}',
                "user": user_id,
                "now": NOW,
            },
        )
        session.commit()
    return organization_id, connector_id, scope_id


def _execution(session, now=NOW):
    return ConnectorSyncExecutionService(
        ConnectorSyncJobRepository(session),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=lambda: now,
    )


def _acquire(factory, organization_id, connector_id, scope_id, *, now=NOW):
    with factory() as session:
        execution = _execution(session, now)
        execution.enqueue(
            organization_id,
            connector_id,
            scope_id,
            mode="incremental",
            trigger_type="manual",
        )
        session.commit()
    with factory() as session:
        attempt = _execution(session, now).acquire_one(
            organization_id,
            worker_id="github-worker",
            lease_duration=timedelta(minutes=15),
        )
        assert attempt is not None
        session.commit()
        return attempt


def _profile():
    return LocalDocumentIndexingProfile(
        "content_extraction",
        "e" * 64,
        "deterministic_text_chunker",
        "c" * 64,
        "fake",
        "fake:model:1536",
        1536,
        "f" * 64,
    )


def _staged(session, profile, now=NOW):
    content = GitHubRepositoryContentService(session, Client())
    return GitHubStagedSynchronizationService(
        session, _execution(session, now), content, profile
    )


def _candidate(cursor, authorization, *, path="file.txt", blob=BLOB, size=len(CONTENT)):
    snapshot = cursor.snapshot
    entry = GitHubRepositoryEntry(
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
        blob,
        size,
        False,
    )
    after = replace(
        cursor,
        frames=(),
        totals=GitHubRunBudget(entries_examined=1),
        scan_complete=True,
    )
    return GitHubDiscoveredFile(entry, cursor, after, None)


def _prepared(discovered, source_snapshot):
    target = replace_totals(
        discovered.cursor_after,
        supported_files=1,
        downloaded_bytes=len(CONTENT),
        prepared_chunks=1,
    )
    item = PreparedGitHubFile(
        discovered,
        source_snapshot.source_item_id,
        source_snapshot.persisted_blob_id,
        "indexed",
        CHECKSUM,
        "Alpha",
        "text/plain",
        (PreparedGitHubChunk(0, "alpha", CHECKSUM, (1.0,) * 1536),),
        "fake:model:1536",
    )
    return PreparedGitHubBatch((item,), target, len(CONTENT), 1)


def replace_totals(cursor, **changes):
    from dataclasses import replace

    return replace(cursor, totals=GitHubRunBudget(cursor.totals.entries_examined, **changes))


def _persist_single_file_traversal(
    factory,
    organization_id,
    connector_id,
    scope_id,
    profile,
    *,
    now,
    path,
    blob=BLOB,
):
    attempt = _acquire(
        factory, organization_id, connector_id, scope_id, now=now
    )
    with factory() as session:
        staged = _staged(session, profile, now)
        snapshot = staged.snapshot(
            attempt.lease, attempt.sync_run_id, worker_id="github-worker"
        )
        session.rollback()
    cursor = GitHubTraversalCursor.initial(
        GitHubRepositorySnapshot(
            connector_id,
            scope_id,
            501,
            "github:repository:501",
            "main",
            COMMIT,
            TREE,
        ),
        snapshot.authorization,
    )
    with factory() as session:
        _staged(session, profile, now).pin_snapshot(
            attempt.lease,
            snapshot,
            cursor,
            worker_id="github-worker",
            now=now,
        )
        session.commit()
    discovered = _candidate(cursor, snapshot.authorization, path=path, blob=blob)
    batch = GitHubDiscoveryBatch((discovered,), discovered.cursor_after, 1, 1)
    with factory() as session:
        item_snapshot = _staged(session, profile, now).item_snapshots(
            attempt.lease,
            snapshot,
            batch,
            worker_id="github-worker",
        )[0]
        session.rollback()
    prepared = _prepared(discovered, item_snapshot)
    with factory() as session:
        outcome = _staged(session, profile, now).persist_batch(
            attempt.lease,
            snapshot,
            prepared,
            worker_id="github-worker",
            now=now,
        )
        assert outcome.phase == "reconciliation"
        session.commit()
    return attempt, snapshot


def _start_empty_reconciliation(
    factory,
    organization_id,
    connector_id,
    scope_id,
    profile,
    *,
    now,
    commit="d" * 40,
    tree="e" * 40,
):
    attempt = _acquire(
        factory, organization_id, connector_id, scope_id, now=now
    )
    with factory() as session:
        staged = _staged(session, profile, now)
        snapshot = staged.snapshot(
            attempt.lease, attempt.sync_run_id, worker_id="github-worker"
        )
        session.rollback()
    cursor = GitHubTraversalCursor.initial(
        GitHubRepositorySnapshot(
            connector_id,
            scope_id,
            501,
            "github:repository:501",
            "main",
            commit,
            tree,
        ),
        snapshot.authorization,
    )
    with factory() as session:
        staged = _staged(session, profile, now)
        staged.pin_snapshot(
            attempt.lease,
            snapshot,
            cursor,
            worker_id="github-worker",
            now=now,
        )
        session.commit()
    terminal = replace(cursor, frames=(), scan_complete=True)
    with factory() as session:
        outcome = _staged(session, profile, now).persist_batch(
            attempt.lease,
            snapshot,
            PreparedGitHubBatch((), terminal, 0, 0),
            worker_id="github-worker",
            now=now,
        )
        assert outcome.phase == "reconciliation"
        session.commit()
    return attempt, snapshot


def _persist_file_set_traversal(
    factory,
    organization_id,
    connector_id,
    scope_id,
    profile,
    *,
    now,
    paths,
):
    attempt = _acquire(
        factory, organization_id, connector_id, scope_id, now=now
    )
    with factory() as session:
        snapshot = _staged(session, profile, now).snapshot(
            attempt.lease, attempt.sync_run_id, worker_id="github-worker"
        )
        session.rollback()
    cursor = GitHubTraversalCursor.initial(
        GitHubRepositorySnapshot(
            connector_id,
            scope_id,
            501,
            "github:repository:501",
            "main",
            COMMIT,
            TREE,
        ),
        snapshot.authorization,
    )
    with factory() as session:
        _staged(session, profile, now).pin_snapshot(
            attempt.lease,
            snapshot,
            cursor,
            worker_id="github-worker",
            now=now,
        )
        session.commit()
    discovered = []
    before = cursor
    for index, path in enumerate(paths, start=1):
        final = index == len(paths)
        after = replace(
            before,
            frames=() if final else (GitHubTraversalFrame("", TREE, index),),
            totals=GitHubRunBudget(entries_examined=index),
            scan_complete=final,
        )
        candidate = _candidate(before, snapshot.authorization, path=path)
        discovered.append(replace(candidate, cursor_after=after))
        before = after
    batch = GitHubDiscoveryBatch(
        tuple(discovered), before, 1, len(discovered)
    )
    with factory() as session:
        item_snapshots = _staged(session, profile, now).item_snapshots(
            attempt.lease,
            snapshot,
            batch,
            worker_id="github-worker",
        )
        session.rollback()
    prepared_files = tuple(
        PreparedGitHubFile(
            candidate,
            item_snapshot.source_item_id,
            item_snapshot.persisted_blob_id,
            "indexed",
            CHECKSUM,
            candidate.entry.name,
            "text/plain",
            (PreparedGitHubChunk(0, "alpha", CHECKSUM, (1.0,) * 1536),),
            "fake:model:1536",
        )
        for candidate, item_snapshot in zip(
            discovered, item_snapshots, strict=True
        )
    )
    prepared_cursor = replace(
        before,
        totals=GitHubRunBudget(
            len(discovered),
            len(discovered),
            len(CONTENT) * len(discovered),
            len(discovered),
        ),
    )
    with factory() as session:
        outcome = _staged(session, profile, now).persist_batch(
            attempt.lease,
            snapshot,
            PreparedGitHubBatch(
                prepared_files,
                prepared_cursor,
                len(CONTENT) * len(discovered),
                len(discovered),
            ),
            worker_id="github-worker",
            now=now,
        )
        assert outcome.phase == "reconciliation"
        session.commit()
    return attempt, snapshot


def _grant_and_search_context(factory, organization_id, scope_id):
    with factory() as session:
        user_id = session.execute(
            text("SELECT id FROM users WHERE organization_id = :org"),
            {"org": organization_id},
        ).scalar_one()
        space_id = session.execute(
            text(
                "SELECT knowledge_space_id FROM connector_scopes "
                "WHERE organization_id = :org AND id = :scope"
            ),
            {"org": organization_id, "scope": scope_id},
        ).scalar_one()
        session.execute(
            text(
                "INSERT INTO knowledge_space_organization_grants "
                "(id,organization_id,knowledge_space_id,permission_level,granted_at) "
                "VALUES(:id,:org,:space,'viewer',:now)"
            ),
            {
                "id": uuid.uuid4(),
                "org": organization_id,
                "space": space_id,
                "now": NOW,
            },
        )
        session.commit()
    return user_id, space_id


def _search(factory, organization_id, user_id):
    with factory() as session:
        return PermissionAwareDocumentChunkSearchRepository(session).search(
            organization_id,
            user_id,
            (1.0,) * 1536,
            "fake:model:1536",
            10,
        )


def test_new_then_unchanged_run_persists_once_and_completes_without_deletion(engine):
    factory = _factory(engine)
    organization_id, connector_id, scope_id = _seed(factory)
    profile = _profile()

    first = _acquire(factory, organization_id, connector_id, scope_id)
    with factory() as session:
        staged = _staged(session, profile)
        snapshot = staged.snapshot(first.lease, first.sync_run_id, worker_id="github-worker")
        assert snapshot.cursor is None
        session.rollback()

    from application.services.github_repository_content_service import GitHubRepositorySnapshot

    cursor = GitHubTraversalCursor.initial(
        GitHubRepositorySnapshot(
            connector_id,
            scope_id,
            501,
            "github:repository:501",
            "main",
            COMMIT,
            TREE,
        ),
        snapshot.authorization,
    )
    with factory() as session:
        staged = _staged(session, profile)
        staged.pin_snapshot(first.lease, snapshot, cursor, worker_id="github-worker", now=NOW)
        session.commit()
    discovered = _candidate(cursor, snapshot.authorization)
    batch = GitHubDiscoveryBatch((discovered,), discovered.cursor_after, 1, 1)
    with factory() as session:
        staged = _staged(session, profile)
        source_snapshot = staged.item_snapshots(
            first.lease, snapshot, batch, worker_id="github-worker"
        )[0]
        session.rollback()
    prepared = _prepared(discovered, source_snapshot)
    with factory() as session:
        outcome = _staged(session, profile).persist_batch(
            first.lease, snapshot, prepared, worker_id="github-worker", now=NOW
        )
        assert outcome.outcome == "in_progress"
        assert outcome.phase == "reconciliation"
        session.commit()
    with factory() as session:
        outcome = _staged(session, profile).reconcile(
            first.lease,
            snapshot,
            worker_id="github-worker",
            now=NOW,
        )
        assert outcome.outcome == "completed"
        session.commit()

    with factory() as session:
        assert session.scalar(select(func.count()).select_from(SourceItem)) == 1
        assert session.scalar(select(func.count()).select_from(SourceItemScopeMembership)) == 1
        assert session.scalar(select(func.count()).select_from(Document)) == 1
        assert session.scalar(select(func.count()).select_from(DocumentVersion)) == 1
        assert session.scalar(select(func.count()).select_from(DocumentVersionDocument)) == 1
        assert session.scalar(select(func.count()).select_from(DocumentIndexingState)) == 1
        assert session.scalar(select(func.count()).select_from(DocumentIndexingAttempt)) == 1
        assert session.scalar(select(func.count()).select_from(DocumentChunk)) == 1
        assert session.scalar(select(func.count()).select_from(ConnectorSyncItem)) == 1
        source = session.scalar(select(SourceItem))
        version = session.scalar(select(DocumentVersion))
        run = session.scalar(
            select(ConnectorSyncRun).where(ConnectorSyncRun.id == first.sync_run_id)
        )
        assert source.source_version == version.provider_version_id == BLOB
        assert source.source_checksum == version.content_checksum == CHECKSUM
        assert version.is_current and run.status == "completed" and run.items_deleted == 0

    second = _acquire(factory, organization_id, connector_id, scope_id)
    with factory() as session:
        staged = _staged(session, profile)
        second_snapshot = staged.snapshot(
            second.lease, second.sync_run_id, worker_id="github-worker"
        )
        session.rollback()
    second_cursor = GitHubTraversalCursor.initial(
        cursor.snapshot, second_snapshot.authorization
    )
    with factory() as session:
        _staged(session, profile).pin_snapshot(
            second.lease,
            second_snapshot,
            second_cursor,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()
    second_discovered = _candidate(second_cursor, second_snapshot.authorization)
    second_batch = GitHubDiscoveryBatch(
        (second_discovered,), second_discovered.cursor_after, 1, 1
    )
    with factory() as session:
        staged = _staged(session, profile)
        item_snapshot = staged.item_snapshots(
            second.lease, second_snapshot, second_batch, worker_id="github-worker"
        )
        session.rollback()
    preparation = GitHubSynchronizationPreparationService(MockContent(), MockRegistry(), MockChunker(), MockEmbedding())
    unchanged = preparation.prepare_batch(
        second_snapshot.authorization, item_snapshot, second_batch
    )
    assert unchanged.files[0].outcome == "unchanged"
    with factory() as session:
        _staged(session, profile).persist_batch(
            second.lease, second_snapshot, unchanged, worker_id="github-worker", now=NOW
        )
        session.commit()
    with factory() as session:
        _staged(session, profile).reconcile(
            second.lease,
            second_snapshot,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()
    with factory() as session:
        assert session.scalar(select(func.count()).select_from(DocumentVersion)) == 1
        assert session.scalar(select(func.count()).select_from(DocumentIndexingAttempt)) == 1
        assert session.scalar(select(func.count()).select_from(SourceItemScopeMembership)) == 1


def test_cursor_failure_rolls_back_all_file_version_index_and_chunk_state(engine):
    factory = _factory(engine)
    organization_id, connector_id, scope_id = _seed(factory)
    profile = _profile()
    attempt = _acquire(factory, organization_id, connector_id, scope_id)
    with factory() as session:
        staged = _staged(session, profile)
        snapshot = staged.snapshot(attempt.lease, attempt.sync_run_id, worker_id="github-worker")
        session.rollback()
    from application.services.github_repository_content_service import GitHubRepositorySnapshot

    cursor = GitHubTraversalCursor.initial(
        GitHubRepositorySnapshot(
            connector_id, scope_id, 501, "github:repository:501", "main", COMMIT, TREE
        ),
        snapshot.authorization,
    )
    with factory() as session:
        _staged(session, profile).pin_snapshot(
            attempt.lease, snapshot, cursor, worker_id="github-worker", now=NOW
        )
        session.commit()
    discovered = _candidate(cursor, snapshot.authorization)
    batch = GitHubDiscoveryBatch((discovered,), discovered.cursor_after, 1, 1)
    with factory() as session:
        staged = _staged(session, profile)
        source_snapshot = staged.item_snapshots(
            attempt.lease, snapshot, batch, worker_id="github-worker"
        )[0]
        session.rollback()
    prepared = _prepared(discovered, source_snapshot)
    with factory() as session:
        staged = _staged(session, profile)
        staged._sync.replace_active_cursor = MockFailure()  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="controlled cursor failure"):
            staged.persist_batch(
                attempt.lease, snapshot, prepared, worker_id="github-worker", now=NOW
            )
        session.rollback()
    with factory() as session:
        for model in (
            SourceItem,
            SourceItemScopeMembership,
            Document,
            DocumentVersion,
            DocumentVersionDocument,
            DocumentIndexingState,
            DocumentIndexingAttempt,
            DocumentChunk,
            ConnectorSyncItem,
        ):
            assert session.scalar(select(func.count()).select_from(model)) == 0
        run = session.scalar(
            select(ConnectorSyncRun).where(ConnectorSyncRun.id == attempt.sync_run_id)
        )
        assert run.status == "running"


def test_rename_creates_new_identity_then_reconciliation_retires_old_history(engine):
    factory = _factory(engine)
    organization_id, connector_id, scope_id = _seed(factory)
    profile = _profile()
    first, first_snapshot = _persist_single_file_traversal(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=NOW,
        path="old/file.txt",
    )
    with factory() as session:
        outcome = _staged(session, profile).reconcile(
            first.lease,
            first_snapshot,
            worker_id="github-worker",
            now=NOW,
        )
        assert outcome.outcome == "completed"
        session.commit()

    later = NOW + timedelta(hours=1)
    second, second_snapshot = _persist_single_file_traversal(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=later,
        path="new/file.txt",
        blob=BLOB,
    )
    with factory() as session:
        outcome = _staged(session, profile, later).reconcile(
            second.lease,
            second_snapshot,
            worker_id="github-worker",
            now=later,
            limit=1,
        )
        assert outcome.outcome == "completed"
        assert outcome.files_persisted == 1
        session.commit()

    old_key = "github:repository:501:path:old/file.txt"
    new_key = "github:repository:501:path:new/file.txt"
    with factory() as session:
        old = session.scalar(select(SourceItem).where(SourceItem.source_item_key == old_key))
        new = session.scalar(select(SourceItem).where(SourceItem.source_item_key == new_key))
        assert old is not None and new is not None and old.id != new.id
        assert old.status == "deleted" and old.deleted_at == later
        assert new.status == "active" and new.deleted_at is None
        old_membership = session.scalar(
            select(SourceItemScopeMembership).where(
                SourceItemScopeMembership.source_item_id == old.id
            )
        )
        new_membership = session.scalar(
            select(SourceItemScopeMembership).where(
                SourceItemScopeMembership.source_item_id == new.id
            )
        )
        assert old_membership.status == "removed"
        assert new_membership.status == "active"
        old_versions = session.scalars(
            select(DocumentVersion)
            .where(DocumentVersion.source_item_id == old.id)
            .order_by(DocumentVersion.version_number)
        ).all()
        assert [version.lifecycle for version in old_versions] == ["available", "deleted"]
        assert old_versions[-1].version_cause == "tombstone"
        assert old_versions[-1].version_metadata["reason"] == "provider_deleted"
        old_document = session.scalar(
            select(Document).where(Document.source_document_key == old_key)
        )
        assert old_document.deleted_at == later
        assert session.scalar(
            select(func.count())
            .select_from(DocumentVersionDocument)
            .where(DocumentVersionDocument.document_id == old_document.id)
        ) == 1
        deletion = session.scalar(
            select(ConnectorSyncItem).where(
                ConnectorSyncItem.sync_run_id == second.sync_run_id,
                ConnectorSyncItem.source_item_key == old_key,
            )
        )
        assert deletion.change_type == "deleted"
        run = session.get(ConnectorSyncRun, second.sync_run_id)
        cursor = session.scalar(
            select(ConnectorSyncCursor).where(ConnectorSyncCursor.state == "active")
        )
        assert run.status == "completed" and run.items_deleted == 1
        assert cursor.safe_cursor["phase"] == "complete"
        assert cursor.safe_cursor["completion_marker"] is True


@pytest.mark.parametrize(
    ("classification", "path", "entry_type", "size", "skip_reason"),
    (
        ("unsupported_format", "file.py", "regular_blob", len(CONTENT), "unsupported_format"),
        ("oversized", "file.txt", "regular_blob", MAX_GITHUB_BLOB_BYTES + 1, "oversized"),
        ("unsupported_object_type", "file.txt", "symlink", len(CONTENT), "unsupported_object_type"),
        ("unsupported_object_type", "file.txt", "submodule", None, "unsupported_object_type"),
        ("git_lfs_unsupported", "file.txt", "regular_blob", len(CONTENT), None),
    ),
)
def test_unindexable_current_path_retires_searchable_state_without_deletion(
    engine, classification, path, entry_type, size, skip_reason
):
    factory = _factory(engine)
    organization_id, connector_id, scope_id = _seed(factory)
    profile = _profile()
    first, first_snapshot = _persist_single_file_traversal(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=NOW,
        path="file.txt" if classification == "unsupported_format" else path,
    )
    with factory() as session:
        _staged(session, profile).reconcile(
            first.lease,
            first_snapshot,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()

    if classification == "unsupported_format":
        with factory() as session:
            old_key = "github:repository:501:path:file.txt"
            new_key = "github:repository:501:path:file.py"
            source = session.scalar(
                select(SourceItem).where(SourceItem.source_item_key == old_key)
            )
            document = session.scalar(
                select(Document).where(Document.source_document_key == old_key)
            )
            source.source_item_key = new_key
            document.source_document_key = new_key
            session.commit()

    later = NOW + timedelta(hours=1)
    attempt = _acquire(
        factory, organization_id, connector_id, scope_id, now=later
    )
    with factory() as session:
        snapshot = _staged(session, profile, later).snapshot(
            attempt.lease, attempt.sync_run_id, worker_id="github-worker"
        )
        session.rollback()
    cursor = GitHubTraversalCursor.initial(
        GitHubRepositorySnapshot(
            connector_id,
            scope_id,
            501,
            "github:repository:501",
            "main",
            "d" * 40,
            "e" * 40,
        ),
        snapshot.authorization,
    )
    with factory() as session:
        _staged(session, profile, later).pin_snapshot(
            attempt.lease,
            snapshot,
            cursor,
            worker_id="github-worker",
            now=later,
        )
        session.commit()
    discovered = _candidate(
        cursor,
        snapshot.authorization,
        path=path,
        blob="f" * 40,
        size=size,
    )
    discovered = replace(
        discovered,
        entry=replace(discovered.entry, entry_type=entry_type, size_bytes=size),
        skip_reason=skip_reason,
    )
    batch = GitHubDiscoveryBatch((discovered,), discovered.cursor_after, 1, 1)
    with factory() as session:
        item_snapshot = _staged(session, profile, later).item_snapshots(
            attempt.lease,
            snapshot,
            batch,
            worker_id="github-worker",
        )[0]
        session.rollback()
    prepared_item = PreparedGitHubFile(
        discovered,
        item_snapshot.source_item_id,
        item_snapshot.persisted_blob_id,
        "unsupported",
        None,
        None,
        None,
        (),
        None,
        classification,
    )
    downloaded_bytes = len(CONTENT) if classification == "git_lfs_unsupported" else 0
    prepared = PreparedGitHubBatch(
        (prepared_item,),
        replace_totals(
            discovered.cursor_after,
            supported_files=1,
            downloaded_bytes=downloaded_bytes,
            prepared_chunks=0,
        ),
        downloaded_bytes,
        0,
    )
    with factory() as session:
        _staged(session, profile, later).persist_batch(
            attempt.lease,
            snapshot,
            prepared,
            worker_id="github-worker",
            now=later,
        )
        session.commit()
    with factory() as session:
        outcome = _staged(session, profile, later).reconcile(
            attempt.lease,
            snapshot,
            worker_id="github-worker",
            now=later,
        )
        assert outcome.files_persisted == 0
        session.commit()

    key = f"github:repository:501:path:{path}"
    with factory() as session:
        source = session.scalar(select(SourceItem).where(SourceItem.source_item_key == key))
        membership = session.scalar(
            select(SourceItemScopeMembership).where(
                SourceItemScopeMembership.source_item_id == source.id
            )
        )
        versions = session.scalars(
            select(DocumentVersion)
            .where(DocumentVersion.source_item_id == source.id)
            .order_by(DocumentVersion.version_number)
        ).all()
        document = session.scalar(
            select(Document).where(Document.source_document_key == key)
        )
        error = session.scalar(
            select(ConnectorSyncError).where(
                ConnectorSyncError.sync_run_id == attempt.sync_run_id
            )
        )
        assert source.status == "unavailable" and source.deleted_at is None
        assert source.source_metadata["availability_reason"] == classification
        assert membership.status == "active" and membership.last_seen_at == later
        assert [version.lifecycle for version in versions] == ["available", "unavailable"]
        assert versions[-1].version_metadata["reason"] == classification
        assert document.deleted_at == later
        assert error.error_code == classification
        assert error.message == "GitHub content is not indexable"
        assert path not in error.message and path not in repr(error.details)


def test_reconciliation_keyset_resumes_in_bounded_fenced_batches(engine):
    factory = _factory(engine)
    organization_id, connector_id, scope_id = _seed(factory)
    profile = _profile()
    first = _acquire(factory, organization_id, connector_id, scope_id)
    with factory() as session:
        snapshot = _staged(session, profile).snapshot(
            first.lease, first.sync_run_id, worker_id="github-worker"
        )
        session.rollback()
    cursor = GitHubTraversalCursor.initial(
        GitHubRepositorySnapshot(
            connector_id,
            scope_id,
            501,
            "github:repository:501",
            "main",
            COMMIT,
            TREE,
        ),
        snapshot.authorization,
    )
    with factory() as session:
        _staged(session, profile).pin_snapshot(
            first.lease,
            snapshot,
            cursor,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()
    middle = replace(
        cursor,
        frames=(GitHubTraversalFrame("", TREE, 1),),
        totals=GitHubRunBudget(entries_examined=1),
    )
    terminal = replace(
        middle,
        frames=(),
        totals=GitHubRunBudget(entries_examined=2),
        scan_complete=True,
    )
    first_entry = _candidate(cursor, snapshot.authorization, path="one.txt")
    first_entry = replace(first_entry, cursor_after=middle)
    second_entry = _candidate(middle, snapshot.authorization, path="two.txt")
    second_entry = replace(second_entry, cursor_after=terminal)
    batch = GitHubDiscoveryBatch((first_entry, second_entry), terminal, 1, 2)
    with factory() as session:
        item_snapshots = _staged(session, profile).item_snapshots(
            first.lease,
            snapshot,
            batch,
            worker_id="github-worker",
        )
        session.rollback()
    prepared_files = tuple(
        PreparedGitHubFile(
            discovered,
            item_snapshot.source_item_id,
            item_snapshot.persisted_blob_id,
            "indexed",
            CHECKSUM,
            discovered.entry.name,
            "text/plain",
            (PreparedGitHubChunk(0, "alpha", CHECKSUM, (1.0,) * 1536),),
            "fake:model:1536",
        )
        for discovered, item_snapshot in zip(
            (first_entry, second_entry), item_snapshots, strict=True
        )
    )
    prepared = PreparedGitHubBatch(
        prepared_files,
        replace(
            terminal,
            totals=GitHubRunBudget(2, 2, len(CONTENT) * 2, 2),
        ),
        len(CONTENT) * 2,
        2,
    )
    with factory() as session:
        _staged(session, profile).persist_batch(
            first.lease,
            snapshot,
            prepared,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()
    with factory() as session:
        _staged(session, profile).reconcile(
            first.lease,
            snapshot,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()

    later = NOW + timedelta(hours=1)
    second = _acquire(
        factory, organization_id, connector_id, scope_id, now=later
    )
    with factory() as session:
        empty_snapshot = _staged(session, profile, later).snapshot(
            second.lease, second.sync_run_id, worker_id="github-worker"
        )
        session.rollback()
    empty_cursor = GitHubTraversalCursor.initial(
        GitHubRepositorySnapshot(
            connector_id,
            scope_id,
            501,
            "github:repository:501",
            "main",
            "d" * 40,
            "e" * 40,
        ),
        empty_snapshot.authorization,
    )
    with factory() as session:
        _staged(session, profile, later).pin_snapshot(
            second.lease,
            empty_snapshot,
            empty_cursor,
            worker_id="github-worker",
            now=later,
        )
        session.commit()
    with factory() as session:
        _staged(session, profile, later).persist_batch(
            second.lease,
            empty_snapshot,
            PreparedGitHubBatch(
                (), replace(empty_cursor, frames=(), scan_complete=True), 0, 0
            ),
            worker_id="github-worker",
            now=later,
        )
        session.commit()
    with factory() as session:
        outcome = _staged(session, profile, later).reconcile(
            second.lease,
            empty_snapshot,
            worker_id="github-worker",
            now=later,
            limit=1,
        )
        assert outcome.outcome == "in_progress" and outcome.files_persisted == 1
        session.commit()
    with factory() as session:
        resumed = _staged(session, profile, later).snapshot(
            second.lease, second.sync_run_id, worker_id="github-worker"
        )
        assert resumed.cursor.phase == "reconciliation"
        assert resumed.cursor.reconciled_items == 1
        assert resumed.cursor.reconciliation_cursor is not None
        session.rollback()
    with factory() as session:
        outcome = _staged(session, profile, later).reconcile(
            second.lease,
            resumed,
            worker_id="github-worker",
            now=later,
            limit=1,
        )
        assert outcome.outcome == "completed" and outcome.files_persisted == 1
        session.commit()
    with factory() as session:
        run = session.get(ConnectorSyncRun, second.sync_run_id)
        cursor_row = session.scalar(
            select(ConnectorSyncCursor).where(ConnectorSyncCursor.state == "active")
        )
        assert run.status == "completed" and run.items_deleted == 2
        assert cursor_row.safe_cursor["phase"] == "complete"
        assert cursor_row.safe_cursor["reconciliation"]["items_reconciled"] == 2


def test_cancellation_is_locked_and_rejected_before_reconciliation_mutation(engine):
    factory = _factory(engine)
    organization_id, connector_id, scope_id = _seed(factory)
    profile = _profile()
    first, first_snapshot = _persist_single_file_traversal(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=NOW,
        path="file.txt",
    )
    with factory() as session:
        _staged(session, profile).reconcile(
            first.lease,
            first_snapshot,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()
    later = NOW + timedelta(hours=1)
    attempt, snapshot = _start_empty_reconciliation(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=later,
    )
    with factory() as session:
        _execution(session, later).request_cancellation(
            organization_id, attempt.lease.job_id
        )
        session.commit()
    with factory() as session:
        with pytest.raises(SyncJobCancellationConflict):
            _staged(session, profile, later).reconcile(
                attempt.lease,
                snapshot,
                worker_id="github-worker",
                now=later,
            )
        session.rollback()
    with factory() as session:
        source = session.scalar(select(SourceItem))
        membership = session.scalar(select(SourceItemScopeMembership))
        document = session.scalar(select(Document))
        run = session.get(ConnectorSyncRun, attempt.sync_run_id)
        cursor = session.scalar(
            select(ConnectorSyncCursor).where(ConnectorSyncCursor.state == "active")
        )
        assert source.status == "active" and source.deleted_at is None
        assert membership.status == "active" and membership.removed_at is None
        assert document.deleted_at is None
        assert run.status == "running" and run.items_deleted == 0
        assert cursor.safe_cursor["phase"] == "reconciliation"
        assert cursor.safe_cursor["reconciliation"]["items_reconciled"] == 0


def test_stale_lease_variants_and_forged_run_boundary_cannot_retire(engine):
    factory = _factory(engine)
    organization_id, connector_id, scope_id = _seed(factory)
    profile = _profile()
    first, first_snapshot = _persist_single_file_traversal(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=NOW,
        path="file.txt",
    )
    with factory() as session:
        _staged(session, profile).reconcile(
            first.lease,
            first_snapshot,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()
    later = NOW + timedelta(hours=1)
    attempt, snapshot = _start_empty_reconciliation(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=later,
    )
    invalid_calls = (
        (attempt.lease, "other-worker", later, LostSyncJobLease),
        (replace(attempt.lease, lease_id=uuid.uuid4()), "github-worker", later, LostSyncJobLease),
        (
            replace(attempt.lease, attempt_number=2, fencing_token=2),
            "github-worker",
            later,
            StaleSyncJobFence,
        ),
        (
            attempt.lease,
            "github-worker",
            attempt.lease.lease_expires_at,
            LostSyncJobLease,
        ),
    )
    for lease, worker_id, clock, error in invalid_calls:
        with factory() as session:
            with pytest.raises(error):
                _staged(session, profile, clock).reconcile(
                    lease,
                    snapshot,
                    worker_id=worker_id,
                    now=clock,
                )
            session.rollback()
    with factory() as session:
        with pytest.raises(StalePreparedGitHubBatch, match="run changed"):
            _staged(session, profile, later).reconcile(
                attempt.lease,
                replace(snapshot, run_started_at=NOW - timedelta(days=1)),
                worker_id="github-worker",
                now=later,
            )
        session.rollback()
    with factory() as session:
        source = session.scalar(select(SourceItem))
        run = session.get(ConnectorSyncRun, attempt.sync_run_id)
        assert source.status == "active"
        assert run.status == "running" and run.items_deleted == 0


def test_second_retirement_failure_rolls_back_memberships_documents_counters_and_cursor(engine):
    factory = _factory(engine)
    organization_id, connector_id, scope_id = _seed(factory)
    user_id, _space_id = _grant_and_search_context(
        factory, organization_id, scope_id
    )
    profile = _profile()
    first, first_snapshot = _persist_file_set_traversal(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=NOW,
        paths=("one.txt", "two.txt"),
    )
    with factory() as session:
        _staged(session, profile).reconcile(
            first.lease,
            first_snapshot,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()
    later = NOW + timedelta(hours=1)
    attempt, snapshot = _start_empty_reconciliation(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=later,
    )
    with factory() as session:
        staged = _staged(session, profile, later)
        original = staged._retire_unseen
        calls = 0

        def fail_second(*args, **kwargs):
            nonlocal calls
            calls += 1
            result = original(*args, **kwargs)
            if calls == 2:
                raise RuntimeError("controlled second retirement failure")
            return result

        staged._retire_unseen = fail_second  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="second retirement"):
            staged.reconcile(
                attempt.lease,
                snapshot,
                worker_id="github-worker",
                now=later,
                limit=2,
            )
        session.rollback()
    with factory() as session:
        assert {row.status for row in session.scalars(select(SourceItem))} == {"active"}
        assert {row.status for row in session.scalars(select(SourceItemScopeMembership))} == {"active"}
        assert all(row.deleted_at is None for row in session.scalars(select(Document)))
        run = session.get(ConnectorSyncRun, attempt.sync_run_id)
        cursor = session.scalar(
            select(ConnectorSyncCursor).where(ConnectorSyncCursor.state == "active")
        )
        assert run.items_deleted == 0 and run.items_succeeded == 0
        assert cursor.safe_cursor["phase"] == "reconciliation"
        assert cursor.safe_cursor["reconciliation"]["items_reconciled"] == 0
        assert cursor.safe_cursor["reconciliation"]["batches_completed"] == 0
    assert len(_search(factory, organization_id, user_id)) == 2


def test_reconciliation_is_provider_free_and_identical_blobs_remain_distinct(engine):
    factory = _factory(engine)
    organization_id, connector_id, scope_id = _seed(factory)
    profile = _profile()
    first, first_snapshot = _persist_file_set_traversal(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=NOW,
        paths=("one.txt", "two.txt"),
    )
    with factory() as session:
        _staged(session, profile).reconcile(
            first.lease,
            first_snapshot,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()
    with factory() as session:
        sources = session.scalars(select(SourceItem).order_by(SourceItem.source_item_key)).all()
        assert len(sources) == 2 and sources[0].id != sources[1].id
        assert sources[0].source_version == sources[1].source_version == BLOB
    later = NOW + timedelta(hours=1)
    attempt, snapshot = _start_empty_reconciliation(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=later,
    )
    with factory() as session:
        staged = _staged(session, profile, later)
        client = staged._content._client
        client.get_branch_reference = MockFailure()
        client.get_commit_reference = MockFailure()
        client.get_tree = MockFailure()
        client.get_blob = MockFailure()
        outcome = staged.reconcile(
            attempt.lease,
            snapshot,
            worker_id="github-worker",
            now=later,
            limit=2,
        )
        assert outcome.outcome == "completed" and outcome.files_persisted == 2
        session.commit()


def test_retirement_blocks_retrieval_and_safe_reactivation_restores_same_identity(engine):
    factory = _factory(engine)
    organization_id, connector_id, scope_id = _seed(factory)
    user_id, _space_id = _grant_and_search_context(
        factory, organization_id, scope_id
    )
    profile = _profile()
    first, first_snapshot = _persist_single_file_traversal(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=NOW,
        path="file.txt",
    )
    with factory() as session:
        _staged(session, profile).reconcile(
            first.lease,
            first_snapshot,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()
    before = _search(factory, organization_id, user_id)
    assert len(before) == 1
    original_source_id = before[0].source_item_id
    original_document_id = before[0].document_id

    later = NOW + timedelta(hours=1)
    deletion, deletion_snapshot = _start_empty_reconciliation(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=later,
    )
    with factory() as session:
        _staged(session, profile, later).reconcile(
            deletion.lease,
            deletion_snapshot,
            worker_id="github-worker",
            now=later,
        )
        session.commit()
    assert _search(factory, organization_id, user_id) == ()
    with factory() as session:
        before_run = session.get(ConnectorSyncRun, deletion.sync_run_id)
        before_cursor = session.scalar(
            select(ConnectorSyncCursor).where(ConnectorSyncCursor.state == "active")
        ).safe_cursor
        expected_counters = (
            before_run.items_deleted,
            before_run.items_succeeded,
        )
        with pytest.raises(LostSyncJobLease):
            _staged(session, profile, later).reconcile(
                deletion.lease,
                deletion_snapshot,
                worker_id="github-worker",
                now=later,
            )
        session.rollback()
    with factory() as session:
        replay_run = session.get(ConnectorSyncRun, deletion.sync_run_id)
        replay_cursor = session.scalar(
            select(ConnectorSyncCursor).where(ConnectorSyncCursor.state == "active")
        ).safe_cursor
        assert (replay_run.items_deleted, replay_run.items_succeeded) == expected_counters
        assert replay_cursor == before_cursor

    restored_at = later + timedelta(hours=1)
    restoration, restoration_snapshot = _persist_single_file_traversal(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=restored_at,
        path="file.txt",
    )
    with factory() as session:
        _staged(session, profile, restored_at).reconcile(
            restoration.lease,
            restoration_snapshot,
            worker_id="github-worker",
            now=restored_at,
        )
        session.commit()
    after = _search(factory, organization_id, user_id)
    assert len(after) == 1
    assert after[0].source_item_id == original_source_id
    assert after[0].document_id == original_document_id
    with factory() as session:
        versions = session.scalars(
            select(DocumentVersion)
            .where(DocumentVersion.source_item_id == original_source_id)
            .order_by(DocumentVersion.version_number)
        ).all()
        assert [version.lifecycle for version in versions] == [
            "available",
            "deleted",
            "available",
        ]
        assert versions[-1].version_cause == "restored"


def test_other_active_scope_membership_preserves_source_document_and_retrieval(engine):
    factory = _factory(engine)
    organization_id, connector_id, scope_id = _seed(factory)
    user_id, space_id = _grant_and_search_context(
        factory, organization_id, scope_id
    )
    profile = _profile()
    first, first_snapshot = _persist_single_file_traversal(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=NOW,
        path="file.txt",
    )
    with factory() as session:
        _staged(session, profile).reconcile(
            first.lease,
            first_snapshot,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()
    second_scope = uuid.uuid4()
    with factory() as session:
        user_id_db = session.execute(
            text("SELECT id FROM users WHERE organization_id = :org"),
            {"org": organization_id},
        ).scalar_one()
        source = session.scalar(select(SourceItem))
        session.execute(
            text(
                "INSERT INTO connector_scopes "
                "(id,organization_id,connector_id,knowledge_space_id,display_name,slug,"
                "scope_type,external_scope_key,access_mode,status,safe_config,"
                "config_schema_version,created_by_user_id,last_validated_at) "
                "VALUES(:id,:org,:connector,:space,'Secondary',:slug,'repository',"
                "'github:repository:502','platform_managed','active',CAST(:config AS jsonb),"
                "1,:user,:now)"
            ),
            {
                "id": second_scope,
                "org": organization_id,
                "connector": connector_id,
                "space": space_id,
                "slug": str(second_scope),
                "config": '{"repository_id":502,"repository_name":"other","repository_full_name":"fake-org/other","owner_login":"fake-org","private":true,"visibility":"private","archived":false,"disabled":false,"default_branch":"main"}',
                "user": user_id_db,
                "now": NOW,
            },
        )
        session.add(
            SourceItemScopeMembership(
                id=uuid.uuid4(),
                organization_id=organization_id,
                connector_id=connector_id,
                source_item_id=source.id,
                connector_scope_id=second_scope,
                status="active",
                first_discovered_at=NOW,
                last_seen_at=NOW,
            )
        )
        session.commit()
    later = NOW + timedelta(hours=1)
    deletion, deletion_snapshot = _start_empty_reconciliation(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=later,
    )
    with factory() as session:
        outcome = _staged(session, profile, later).reconcile(
            deletion.lease,
            deletion_snapshot,
            worker_id="github-worker",
            now=later,
        )
        assert outcome.files_persisted == 1
        session.commit()
    results = _search(factory, organization_id, user_id)
    assert len(results) == 1 and results[0].connector_scope_id == second_scope
    with factory() as session:
        source = session.scalar(select(SourceItem))
        document = session.scalar(select(Document))
        memberships = session.scalars(
            select(SourceItemScopeMembership).order_by(
                SourceItemScopeMembership.connector_scope_id
            )
        ).all()
        assert source.status == "active" and source.deleted_at is None
        assert document.deleted_at is None
        assert {row.status for row in memberships} == {"active", "removed"}
        assert session.scalar(
            select(func.count())
            .select_from(DocumentVersion)
            .where(DocumentVersion.source_item_id == source.id)
        ) == 1


def test_reconciliation_is_tenant_isolated(engine):
    factory = _factory(engine)
    org_a, connector_a, scope_a = _seed(factory)
    org_b, connector_b, scope_b = _seed(
        factory, installation_id=78, account_id=100
    )
    profile = _profile()
    first_a, snapshot_a = _persist_single_file_traversal(
        factory, org_a, connector_a, scope_a, profile, now=NOW, path="a.txt"
    )
    first_b, snapshot_b = _persist_single_file_traversal(
        factory, org_b, connector_b, scope_b, profile, now=NOW, path="b.txt"
    )
    with factory() as session:
        _staged(session, profile).reconcile(
            first_a.lease, snapshot_a, worker_id="github-worker", now=NOW
        )
        session.commit()
    with factory() as session:
        _staged(session, profile).reconcile(
            first_b.lease, snapshot_b, worker_id="github-worker", now=NOW
        )
        session.commit()
    later = NOW + timedelta(hours=1)
    deletion, deletion_snapshot = _start_empty_reconciliation(
        factory, org_a, connector_a, scope_a, profile, now=later
    )
    with factory() as session:
        _staged(session, profile, later).reconcile(
            deletion.lease,
            deletion_snapshot,
            worker_id="github-worker",
            now=later,
        )
        session.commit()
    with factory() as session:
        source_a = session.scalar(
            select(SourceItem).where(SourceItem.organization_id == org_a)
        )
        source_b = session.scalar(
            select(SourceItem).where(SourceItem.organization_id == org_b)
        )
        document_b = session.scalar(
            select(Document).where(Document.organization_id == org_b)
        )
        assert source_a.status == "deleted"
        assert source_b.status == "active" and source_b.deleted_at is None
        assert document_b.deleted_at is None


def test_concurrent_workers_produce_one_effective_terminal_retirement(engine):
    factory = _factory(engine)
    organization_id, connector_id, scope_id = _seed(factory)
    profile = _profile()
    first, first_snapshot = _persist_single_file_traversal(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=NOW,
        path="file.txt",
    )
    with factory() as session:
        _staged(session, profile).reconcile(
            first.lease,
            first_snapshot,
            worker_id="github-worker",
            now=NOW,
        )
        session.commit()
    later = NOW + timedelta(hours=1)
    attempt, snapshot = _start_empty_reconciliation(
        factory,
        organization_id,
        connector_id,
        scope_id,
        profile,
        now=later,
    )
    barrier = Barrier(2)

    def invoke(worker_id):
        with factory() as session:
            barrier.wait()
            try:
                outcome = _staged(session, profile, later).reconcile(
                    attempt.lease,
                    snapshot,
                    worker_id=worker_id,
                    now=later,
                )
                session.commit()
                return outcome.outcome
            except LostSyncJobLease:
                session.rollback()
                return "lost"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(
            pool.map(invoke, ("github-worker", "competing-worker"))
        )
    assert set(results) == {"completed", "lost"}
    with factory() as session:
        run = session.get(ConnectorSyncRun, attempt.sync_run_id)
        assert run.status == "completed"
        assert run.items_deleted == 1 and run.items_succeeded == 1
        assert session.scalar(
            select(func.count())
            .select_from(ConnectorSyncItem)
            .where(
                ConnectorSyncItem.sync_run_id == attempt.sync_run_id,
                ConnectorSyncItem.change_type == "deleted",
            )
        ) == 1


class MockContent:
    def download_blob(self, *args, **kwargs):
        raise AssertionError("unchanged file must not download")


class MockRegistry:
    extractors = {}

    def extract(self, *args, **kwargs):
        raise AssertionError("unchanged file must not extract")


class MockChunker:
    def chunk(self, *args, **kwargs):
        raise AssertionError("unchanged file must not chunk")


class MockEmbedding:
    profile = EmbeddingProfile("fake", "fake", 1536, "fake:model:1536", 2)

    def embed_batch(self, *args, **kwargs):
        raise AssertionError("unchanged file must not embed")


class MockFailure:
    def __call__(self, *args, **kwargs):
        raise RuntimeError("controlled cursor failure")
