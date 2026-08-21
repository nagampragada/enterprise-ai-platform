from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import subprocess
import uuid

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from application.services.connector_sync_execution_service import ConnectorSyncExecutionService
from application.services.connector_sync_retry_policy import ConnectorSyncRetryPolicy
from application.services.github_repository_content_service import (
    GitHubRepositoryContentService,
    GitHubRepositoryEntry,
)
from application.services.github_staged_synchronization_service import (
    GitHubDiscoveredFile,
    GitHubDiscoveryBatch,
    GitHubRunBudget,
    GitHubStagedSynchronizationService,
    GitHubSynchronizationPreparationService,
    GitHubTraversalCursor,
    PreparedGitHubBatch,
    PreparedGitHubChunk,
    PreparedGitHubFile,
)
from application.services.local_document_indexing_service import LocalDocumentIndexingProfile
from domain.embeddings.models import EmbeddingProfile
from infrastructure.db.models import (
    ConnectorSyncItem,
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


def _seed(factory):
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
                "VALUES(:id,:org,:connector,'github','app_installation','active','77','fake-org',CAST(:scopes AS jsonb),:user)"
            ),
            {
                "id": credential_id,
                "org": organization_id,
                "connector": connector_id,
                "scopes": '["contents:read","metadata:read"]',
                "user": user_id,
            },
        )
        session.execute(
            text(
                "INSERT INTO github_app_installations(id,organization_id,connector_id,credential_id,github_app_id,github_installation_id,account_id,account_login,account_type,repository_selection,status,provider_created_at,provider_updated_at,last_verified_at,created_at,updated_at) "
                "VALUES(:id,:org,:connector,:credential,123,77,99,'fake-org','Organization','selected','connected',:now,:now,:now,:now,:now)"
            ),
            {
                "id": uuid.uuid4(),
                "org": organization_id,
                "connector": connector_id,
                "credential": credential_id,
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


def _execution(session):
    return ConnectorSyncExecutionService(
        ConnectorSyncJobRepository(session),
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=lambda: NOW,
    )


def _acquire(factory, organization_id, connector_id, scope_id):
    with factory() as session:
        execution = _execution(session)
        execution.enqueue(
            organization_id,
            connector_id,
            scope_id,
            mode="incremental",
            trigger_type="manual",
        )
        session.commit()
    with factory() as session:
        attempt = _execution(session).acquire_one(
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


def _staged(session, profile):
    content = GitHubRepositoryContentService(session, Client())
    return GitHubStagedSynchronizationService(session, _execution(session), content, profile)


def _candidate(cursor, authorization):
    snapshot = cursor.snapshot
    entry = GitHubRepositoryEntry(
        snapshot.connector_id,
        snapshot.scope_id,
        snapshot.repository_id,
        snapshot.canonical_repository_identity,
        snapshot.commit_object_id,
        snapshot.root_tree_object_id,
        snapshot.root_tree_object_id,
        "file.txt",
        "file.txt",
        "regular_blob",
        BLOB,
        len(CONTENT),
        False,
    )
    after = GitHubTraversalCursor(
        snapshot,
        (),
        GitHubRunBudget(entries_examined=1),
        True,
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
        )
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
    second_cursor = GitHubTraversalCursor.initial(cursor.snapshot)
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
        )
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
