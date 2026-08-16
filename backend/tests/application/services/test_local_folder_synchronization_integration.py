from __future__ import annotations

from dataclasses import asdict
import os
from pathlib import Path
import subprocess
import threading
import uuid
from collections.abc import Sequence

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from application.services.document_chunk_embedding_service import DocumentChunkEmbeddingService
from application.services.local_document_indexing_service import LocalDocumentIndexingService
from application.services.local_document_ingestion_service import LocalDocumentIngestionService
from application.services.local_folder_synchronization_service import (
    LocalFolderSynchronizationRequest,
    LocalFolderSynchronizationPersistenceError,
    LocalFolderSynchronizationService,
    LocalFolderSynchronizationUnavailable,
)
from domain.embeddings.exceptions import RetryableEmbeddingProviderError
from domain.embeddings.models import EmbeddingProfile, EmbeddingRequest, EmbeddingResult
from domain.embeddings.provider import EmbeddingProvider
from infrastructure.connectors.local.connector import LocalFolderConnector
from infrastructure.content_chunking.text_chunker import DeterministicTextChunker
from infrastructure.content_extraction.registry import create_default_content_extractor_registry
from infrastructure.db.models import (
    Connector,
    ConnectorScope,
    ConnectorSyncRun,
    Document,
    DocumentChunk,
    DocumentIndexingAttempt,
    DocumentIndexingState,
    DocumentVersion,
    DocumentVersionDocument,
    KnowledgeSpace,
    Organization,
    SourceItem,
    SourceItemScopeMembership,
)
from infrastructure.repositories.connector_repository import ConnectorRepository
from infrastructure.repositories.connector_scope_repository import ConnectorScopeRepository
from infrastructure.repositories.connector_sync_repository import ConnectorSyncRepository
from infrastructure.repositories.document_chunk_repository import DocumentChunkRepository
from infrastructure.repositories.document_indexing_repository import DocumentIndexingRepository
from infrastructure.repositories.document_repository import DocumentRepository
from infrastructure.repositories.document_version_repository import DocumentVersionRepository
from infrastructure.repositories.source_item_repository import SourceItemRepository

PROJECT_ROOT = Path(__file__).resolve().parents[3]
PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
DIMENSION = 1536
MODEL_IDENTIFIER = "local-sync-fake:1536"


class DeterministicSynchronizationEmbeddingProvider(EmbeddingProvider):
    def __init__(self, *, fail_on_batch: int | None = None) -> None:
        self.fail_on_batch = fail_on_batch
        self.calls = 0

    @property
    def profile(self) -> EmbeddingProfile:
        return EmbeddingProfile("local-sync-fake", "local-sync-fake", DIMENSION, MODEL_IDENTIFIER, 64)

    def embed_batch(self, requests: Sequence[EmbeddingRequest]) -> tuple[EmbeddingResult, ...]:
        self.calls += 1
        if self.fail_on_batch == self.calls:
            raise RetryableEmbeddingProviderError("controlled synchronization failure")
        return tuple(
            EmbeddingResult(
                request.input_index,
                (float(request.input_index + 1),) * DIMENSION,
                MODEL_IDENTIFIER,
                DIMENSION,
            )
            for request in requests
        )


def _database_identity(database_url: str):
    value = make_url(database_url)
    return value.drivername, value.host, value.port, value.database


@pytest.fixture(scope="module")
def engine():
    test_url = os.environ["TEST_DATABASE_URL"]
    development_url = os.environ.get("DATABASE_URL")
    if development_url and _database_identity(development_url) == _database_identity(test_url):
        raise RuntimeError("TEST_DATABASE_URL must differ from DATABASE_URL")
    reset = create_engine(test_url, future=True)
    with reset.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
    reset.dispose()
    environment = os.environ.copy()
    environment["DATABASE_URL"] = test_url
    subprocess.run(
        [str(PYTHON), "-m", "alembic", "-c", str(ALEMBIC_INI), "upgrade", "head"],
        check=True,
        cwd=str(PROJECT_ROOT),
        env=environment,
    )
    value = create_engine(test_url, future=True)
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
            "document_versions", "connector_sync_cursors", "connector_sync_errors", "connector_sync_items",
            "connector_sync_runs", "source_item_scope_memberships", "source_items", "connector_scopes",
            "connectors", "audit_events", "knowledge_space_user_grants", "knowledge_space_team_grants",
            "knowledge_space_department_grants", "knowledge_space_organization_grants", "knowledge_spaces",
            "team_memberships", "department_memberships", "teams", "departments", "document_chunks",
            "documents", "authentication_sessions", "user_roles", "users", "organization_settings",
            "organizations", "industries",
        ):
            connection.execute(text(f"DELETE FROM {table}"))


@pytest.fixture
def session(engine):
    value = sessionmaker(bind=engine, class_=Session, autoflush=False, expire_on_commit=False)()
    try:
        yield value
    finally:
        value.rollback()
        value.close()


def _configured_scope(session: Session, root: Path, name: str, *, organization_id=None, connector_id=None):
    create_organization = organization_id is None
    create_connector = connector_id is None
    organization_id = organization_id or uuid.uuid4()
    connector_id = connector_id or uuid.uuid4()
    space_id, scope_id = uuid.uuid4(), uuid.uuid4()
    provider = LocalFolderConnector(organization_id, connector_id, root)
    if create_organization:
        session.add(Organization(id=organization_id, name=name, slug=f"{name.lower()}-{organization_id}"))
        session.flush()
    session.add(KnowledgeSpace(id=space_id, organization_id=organization_id, name=name, slug=f"space-{scope_id}"))
    session.flush()
    if create_connector:
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
    session.flush()
    return organization_id, connector_id, scope_id


def _service(session: Session, provider: EmbeddingProvider) -> LocalFolderSynchronizationService:
    chunks = DocumentChunkRepository(session)
    indexing_service = LocalDocumentIndexingService(
        LocalDocumentIngestionService(
            create_default_content_extractor_registry(),
            DeterministicTextChunker(),
            DocumentRepository(session),
            chunks,
        ),
        chunks,
        DocumentChunkEmbeddingService(provider, chunks),
    )
    return LocalFolderSynchronizationService(
        ConnectorRepository(session),
        ConnectorScopeRepository(session),
        SourceItemRepository(session),
        ConnectorSyncRepository(session),
        DocumentVersionRepository(session),
        DocumentIndexingRepository(session),
        indexing_service,
    )


def _run_to_completion(
    session: Session,
    service: LocalFolderSynchronizationService,
    organization_id,
    connector_id,
    scope_id,
    *,
    batch_size: int = 1,
):
    results = []
    run_id = None
    for _ in range(20):
        result = service.synchronize(
            LocalFolderSynchronizationRequest(
                organization_id,
                connector_id,
                scope_id,
                sync_run_id=run_id,
                batch_size=batch_size,
            )
        )
        results.append(result)
        run_id = result.sync_run_id
        session.commit()
        if result.outcome == "completed":
            return results
    raise AssertionError("synchronization did not complete")


def _count(session: Session, model, *predicates) -> int:
    return session.scalar(select(func.count()).select_from(model).where(*predicates))


def test_initial_unchanged_and_change_add_remove_lifecycle(session: Session, tmp_path: Path) -> None:
    root = tmp_path / "root"
    (root / "nested").mkdir(parents=True)
    (root / "a.txt").write_text("alpha", encoding="utf-8")
    (root / "nested" / "b.md").write_text("# beta", encoding="utf-8")
    (root / "ignored.exe").write_bytes(b"ignored")
    organization_id, connector_id, scope_id = _configured_scope(session, root, "Initial")
    session.commit()
    provider = DeterministicSynchronizationEmbeddingProvider()
    service = _service(session, provider)

    initial = _run_to_completion(session, service, organization_id, connector_id, scope_id)
    assert sum(result.new_source_items for result in initial) == 2
    assert sum(result.versions_indexed for result in initial) == 2
    assert initial[-1].discovery_completed and initial[-1].reconciliation_completed
    sources = list(session.scalars(select(SourceItem).where(SourceItem.organization_id == organization_id)).all())
    assert {source.source_item_key for source in sources} == {"a.txt", "nested/b.md"}
    assert all(source.source_url is None for source in sources)
    assert all(not Path(str(source.source_metadata["relative_path"])).is_absolute() for source in sources)
    assert _count(session, SourceItemScopeMembership, SourceItemScopeMembership.connector_scope_id == scope_id) == 2
    assert _count(session, DocumentVersion, DocumentVersion.organization_id == organization_id) == 2
    assert _count(session, DocumentVersionDocument, DocumentVersionDocument.organization_id == organization_id) == 2
    assert _count(session, DocumentIndexingState, DocumentIndexingState.status == "indexed") == 2
    assert _count(session, DocumentIndexingAttempt, DocumentIndexingAttempt.status == "succeeded") == 2
    states = list(session.scalars(select(DocumentIndexingState).where(DocumentIndexingState.organization_id == organization_id)).all())
    assert len({state.profile_fingerprint for state in states}) == 1
    assert all(len(state.profile_fingerprint) == 64 for state in states)
    assert all(state.embedding_model == MODEL_IDENTIFIER and state.embedding_dimensions == DIMENSION for state in states)
    chunks = list(session.scalars(select(DocumentChunk).where(DocumentChunk.organization_id == organization_id)).all())
    assert chunks and all(chunk.embedding is not None and len(chunk.embedding) == DIMENSION for chunk in chunks)
    assert all(chunk.embedding_model == MODEL_IDENTIFIER for chunk in chunks)
    assert all("root" not in run.run_metadata for run in session.scalars(select(ConnectorSyncRun)).all())

    versions_before = _count(session, DocumentVersion, DocumentVersion.organization_id == organization_id)
    attempts_before = _count(session, DocumentIndexingAttempt, DocumentIndexingAttempt.organization_id == organization_id)
    unchanged = _run_to_completion(session, service, organization_id, connector_id, scope_id)
    assert sum(result.unchanged_files for result in unchanged) == 2
    assert sum(result.versions_created for result in unchanged) == 0
    assert _count(session, DocumentVersion, DocumentVersion.organization_id == organization_id) == versions_before
    assert _count(session, DocumentIndexingAttempt, DocumentIndexingAttempt.organization_id == organization_id) == attempts_before

    (root / "a.txt").write_text("alpha changed", encoding="utf-8")
    (root / "nested" / "b.md").unlink()
    (root / "new.markdown").write_text("# new", encoding="utf-8")
    changed = _run_to_completion(session, service, organization_id, connector_id, scope_id)
    assert sum(result.changed_source_items for result in changed) == 1
    assert sum(result.new_source_items for result in changed) == 1
    assert sum(result.missing_memberships_reconciled for result in changed) == 1
    a_source = session.scalar(select(SourceItem).where(SourceItem.organization_id == organization_id, SourceItem.source_item_key == "a.txt"))
    b_source = session.scalar(select(SourceItem).where(SourceItem.organization_id == organization_id, SourceItem.source_item_key == "nested/b.md"))
    new_source = session.scalar(select(SourceItem).where(SourceItem.organization_id == organization_id, SourceItem.source_item_key == "new.markdown"))
    assert [row.version_number for row in session.scalars(select(DocumentVersion).where(DocumentVersion.source_item_id == a_source.id).order_by(DocumentVersion.version_number)).all()] == [1, 2]
    assert _count(session, DocumentVersion, DocumentVersion.source_item_id == new_source.id) == 1
    assert b_source.status == "unavailable"
    membership = session.scalar(select(SourceItemScopeMembership).where(SourceItemScopeMembership.source_item_id == b_source.id, SourceItemScopeMembership.connector_scope_id == scope_id))
    assert membership.status == "removed"
    current = session.scalar(select(DocumentVersion).where(DocumentVersion.source_item_id == a_source.id, DocumentVersion.is_current.is_(True)))
    mapping = session.scalar(select(DocumentVersionDocument).where(DocumentVersionDocument.document_version_id == current.id))
    assert mapping is not None


def test_indexing_failure_caller_rollback_preserves_last_materialization_and_retry(session: Session, tmp_path: Path) -> None:
    root = tmp_path / "rollback"
    root.mkdir()
    path = root / "document.txt"
    path.write_text("original", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(session, root, "Rollback")
    session.commit()
    healthy = _service(session, DeterministicSynchronizationEmbeddingProvider())
    _run_to_completion(session, healthy, organization_id, connector_id, scope_id, batch_size=10)
    source = session.scalar(select(SourceItem).where(SourceItem.organization_id == organization_id))
    committed_version = session.scalar(select(DocumentVersion).where(DocumentVersion.source_item_id == source.id, DocumentVersion.is_current.is_(True)))
    committed_mapping = session.scalar(select(DocumentVersionDocument).where(DocumentVersionDocument.document_version_id == committed_version.id))
    committed_hashes = tuple(session.scalars(select(DocumentChunk.content_hash).where(DocumentChunk.document_id == committed_mapping.document_id).order_by(DocumentChunk.chunk_index)).all())
    run_count = _count(session, ConnectorSyncRun, ConnectorSyncRun.organization_id == organization_id)

    path.write_text("changed content", encoding="utf-8")
    failing = _service(session, DeterministicSynchronizationEmbeddingProvider(fail_on_batch=1))
    with pytest.raises(Exception) as raised:
        failing.synchronize(LocalFolderSynchronizationRequest(organization_id, connector_id, scope_id, batch_size=10))
    assert str(root) not in str(raised.value)
    session.rollback()
    session.expire_all()
    current = session.scalar(select(DocumentVersion).where(DocumentVersion.source_item_id == source.id, DocumentVersion.is_current.is_(True)))
    assert current.id == committed_version.id
    mapping = session.scalar(select(DocumentVersionDocument).where(DocumentVersionDocument.document_version_id == current.id))
    assert mapping.document_id == committed_mapping.document_id
    assert tuple(session.scalars(select(DocumentChunk.content_hash).where(DocumentChunk.document_id == mapping.document_id).order_by(DocumentChunk.chunk_index)).all()) == committed_hashes
    assert _count(session, ConnectorSyncRun, ConnectorSyncRun.organization_id == organization_id) == run_count

    retry = _run_to_completion(session, healthy, organization_id, connector_id, scope_id, batch_size=10)
    assert sum(result.versions_created for result in retry) == 1
    assert _count(session, DocumentVersion, DocumentVersion.source_item_id == source.id) == 2


def test_tenant_and_symlink_isolation(session: Session, tmp_path: Path) -> None:
    first_root, second_root = tmp_path / "first", tmp_path / "second"
    first_root.mkdir();second_root.mkdir()
    (first_root / "same.txt").write_text("first", encoding="utf-8")
    (second_root / "same.txt").write_text("second", encoding="utf-8")
    outside = tmp_path / "outside.txt";outside.write_text("secret", encoding="utf-8")
    link = first_root / "escape.txt"
    symlink_available = True
    try:link.symlink_to(outside)
    except (OSError,NotImplementedError):symlink_available=False
    first = _configured_scope(session, first_root, "First")
    second = _configured_scope(session, second_root, "Second")
    session.commit()
    service = _service(session, DeterministicSynchronizationEmbeddingProvider())
    _run_to_completion(session, service, *first, batch_size=10)
    _run_to_completion(session, service, *second, batch_size=10)
    assert _count(session, SourceItem, SourceItem.organization_id == first[0]) == 1
    assert _count(session, SourceItem, SourceItem.organization_id == second[0]) == 1
    if symlink_available:
        assert session.scalar(select(SourceItem).where(SourceItem.organization_id == first[0], SourceItem.source_item_key == "escape.txt")) is None
    second_versions = _count(session, DocumentVersion, DocumentVersion.organization_id == second[0])
    with pytest.raises(LocalFolderSynchronizationUnavailable):
        service.synchronize(LocalFolderSynchronizationRequest(first[0], second[1], second[2]))
    session.rollback()
    assert _count(session, DocumentVersion, DocumentVersion.organization_id == second[0]) == second_versions


def test_multi_scope_removal_preserves_canonical_source_and_other_membership(session: Session, tmp_path: Path) -> None:
    first_root, second_root = tmp_path / "scope-one", tmp_path / "scope-two"
    first_root.mkdir();second_root.mkdir()
    (first_root / "shared.txt").write_text("shared", encoding="utf-8")
    (second_root / "shared.txt").write_text("shared", encoding="utf-8")
    organization_id, connector_id, first_scope = _configured_scope(session, first_root, "Shared")
    _, _, second_scope = _configured_scope(session, second_root, "SharedTwo", organization_id=organization_id, connector_id=connector_id)
    session.commit()
    service = _service(session, DeterministicSynchronizationEmbeddingProvider())
    _run_to_completion(session, service, organization_id, connector_id, first_scope, batch_size=10)
    _run_to_completion(session, service, organization_id, connector_id, second_scope, batch_size=10)
    source = session.scalar(select(SourceItem).where(SourceItem.organization_id == organization_id, SourceItem.source_item_key == "shared.txt"))
    assert _count(session, SourceItem, SourceItem.organization_id == organization_id, SourceItem.source_item_key == "shared.txt") == 1
    assert _count(session, SourceItemScopeMembership, SourceItemScopeMembership.source_item_id == source.id, SourceItemScopeMembership.status == "active") == 2

    (first_root / "shared.txt").unlink()
    _run_to_completion(session, service, organization_id, connector_id, first_scope, batch_size=10)
    session.refresh(source)
    assert source.status == "active"
    memberships = list(session.scalars(select(SourceItemScopeMembership).where(SourceItemScopeMembership.source_item_id == source.id)).all())
    assert {membership.connector_scope_id: membership.status for membership in memberships} == {first_scope: "removed", second_scope: "active"}
    assert _count(session, DocumentVersion, DocumentVersion.source_item_id == source.id) == 1


def test_concurrent_same_scope_start_preserves_single_active_run(engine, session: Session, tmp_path: Path) -> None:
    root = tmp_path / "concurrent"
    root.mkdir()
    (root / "a.txt").write_text("a", encoding="utf-8")
    (root / "b.txt").write_text("b", encoding="utf-8")
    organization_id, connector_id, scope_id = _configured_scope(session, root, "Concurrent")
    session.commit()
    factory = sessionmaker(bind=engine, class_=Session, autoflush=False, expire_on_commit=False)
    first_session, second_session = factory(), factory()
    started = threading.Event()
    failures: list[BaseException] = []
    try:
        first_result = _service(
            first_session, DeterministicSynchronizationEmbeddingProvider()
        ).synchronize(
            LocalFolderSynchronizationRequest(
                organization_id, connector_id, scope_id, batch_size=1
            )
        )
        assert first_result.outcome == "running"

        def start_competing_run() -> None:
            started.set()
            try:
                _service(
                    second_session, DeterministicSynchronizationEmbeddingProvider()
                ).synchronize(
                    LocalFolderSynchronizationRequest(
                        organization_id, connector_id, scope_id, batch_size=1
                    )
                )
            except BaseException as exc:
                failures.append(exc)

        thread = threading.Thread(target=start_competing_run)
        thread.start()
        assert started.wait(5)
        first_session.commit()
        thread.join(10)
        assert not thread.is_alive()
        assert len(failures) == 1
        assert isinstance(failures[0], LocalFolderSynchronizationPersistenceError)
    finally:
        first_session.rollback();second_session.rollback();first_session.close();second_session.close()
    assert _count(
        session,
        ConnectorSyncRun,
        ConnectorSyncRun.organization_id == organization_id,
        ConnectorSyncRun.connector_scope_id == scope_id,
        ConnectorSyncRun.status == "running",
    ) == 1
