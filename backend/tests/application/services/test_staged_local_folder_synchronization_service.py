from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from application.services.local_document_indexing_service import LocalDocumentIndexingProfile
from application.services.staged_local_folder_synchronization_service import (
    InvalidStagedLocalFolderRequest,
    LocalFolderDiscoveredEntry,
    LocalFolderItemSnapshot,
    LocalFolderPreparationService,
    LocalFolderSynchronizationSnapshot,
    PreparedLocalFolderChunk,
    StalePreparedLocalFolderItem,
)
from domain.content_chunking.models import ChunkResult
from domain.content_extraction.models import ExtractedContent
from domain.embeddings.models import EmbeddingProfile, EmbeddingResult

NOW = datetime(2026, 8, 19, tzinfo=timezone.utc)


def _profile():
    return LocalDocumentIndexingProfile(
        "extract", "v1", "chunk", "v1", "fake", "fake:model:1536", 1536, "f" * 64
    )


def _snapshot(root: Path):
    return LocalFolderSynchronizationSnapshot(
        uuid4(), uuid4(), uuid4(), uuid4(), NOW, root,
        "discovery", None, None, _profile(),
    )


def _entry(checksum="a" * 64):
    return LocalFolderDiscoveredEntry(
        "file.txt", "file.txt", "text/plain", checksum, 5, NOW, NOW, False
    )


def _service():
    registry, chunker, provider = Mock(), Mock(), Mock()
    provider.profile = EmbeddingProfile("fake", "fake", 1536, "fake:model:1536", 64)
    service = LocalFolderPreparationService(registry, chunker, provider)
    return service, registry, chunker, provider


def test_snapshot_and_prepared_contracts_are_immutable(tmp_path):
    snapshot = _snapshot(tmp_path)
    with pytest.raises(FrozenInstanceError):
        snapshot.phase = "completed"  # type: ignore[misc]
    chunk = PreparedLocalFolderChunk(0, "text", "a" * 64, (1.0,) * 1536)
    with pytest.raises(FrozenInstanceError):
        chunk.chunk_text = "other"  # type: ignore[misc]


def test_completed_run_item_and_profile_complete_unchanged_skip_provider(tmp_path):
    service, registry, chunker, provider = _service()
    entry = _entry()
    completed = LocalFolderItemSnapshot(uuid4(), entry.checksum, entry.checksum, False, "succeeded", entry.checksum)
    result = service.prepare_item(_snapshot(tmp_path), completed, entry)
    assert result.outcome == "already_complete" and not result.chunks
    unchanged = LocalFolderItemSnapshot(uuid4(), entry.checksum, entry.checksum, True, None, None)
    result = service.prepare_item(_snapshot(tmp_path), unchanged, entry)
    assert result.outcome == "unchanged" and not result.chunks
    registry.extract.assert_not_called(); chunker.chunk.assert_not_called(); provider.embed_batch.assert_not_called()


def test_changed_content_prepares_chunks_and_vectors_without_persistence(tmp_path):
    path = tmp_path / "file.txt"; path.write_text("alpha", encoding="utf-8")
    checksum = __import__("hashlib").sha256(b"alpha").hexdigest()
    service, registry, chunker, provider = _service()
    registry.extract.return_value = ExtractedContent(
        text="alpha", title="Alpha", mime_type="text/plain", metadata={}, warnings=()
    )
    chunker.chunk.return_value = (
        ChunkResult(0, "alpha", checksum, 5, 0, 5),
    )
    provider.embed_batch.return_value = (
        EmbeddingResult(0, (1.0,) * 1536, "fake:model:1536", 1536),
    )
    snapshot = _snapshot(tmp_path)
    entry = _entry(checksum)
    with patch(
        "application.services.staged_local_folder_synchronization_service.LocalFolderConnector.resolve_content_path",
        return_value=path,
    ):
        prepared = service.prepare_item(
            snapshot, LocalFolderItemSnapshot(None, None, None, False, None, None), entry
        )
    assert prepared.outcome == "indexed"
    assert prepared.document_title == "Alpha"
    assert len(prepared.chunks) == 1 and len(prepared.chunks[0].embedding) == 1536


def test_file_change_during_read_is_rejected_once_without_loop(tmp_path):
    path = tmp_path / "file.txt"; path.write_text("alpha", encoding="utf-8")
    checksum = __import__("hashlib").sha256(b"alpha").hexdigest()
    service, registry, chunker, provider = _service()
    def change_during_extract(_):
        path.write_text("changed", encoding="utf-8")
        return ExtractedContent(title="Changed", text="changed", mime_type="text/plain")

    registry.extract.side_effect = change_during_extract
    with patch(
        "application.services.staged_local_folder_synchronization_service.LocalFolderConnector.resolve_content_path",
        return_value=path,
    ):
        with pytest.raises(StalePreparedLocalFolderItem):
            service.prepare_item(
                _snapshot(tmp_path),
                LocalFolderItemSnapshot(None, None, None, False, None, None),
                _entry(checksum),
            )
    assert registry.extract.call_count == 1
    provider.embed_batch.assert_not_called()


@pytest.mark.parametrize(
    "vector",
    ((1.0,) * 1535, (float("nan"),) + (1.0,) * 1535),
)
def test_prepared_chunk_rejects_invalid_vectors(vector):
    with pytest.raises(InvalidStagedLocalFolderRequest):
        PreparedLocalFolderChunk(0, "text", "a" * 64, vector)


def test_worker_preparation_runs_after_all_snapshot_sessions_close():
    from infrastructure.workers.local_folder_sync_worker import LocalFolderSyncWorker
    from tests.infrastructure.workers.test_local_folder_sync_worker import (
        SessionFactory, _context, _entry as worker_entry, _execution, _prepared, _snapshot as worker_snapshot,
    )

    sessions, execution, staged, preparation = SessionFactory(), _execution(), Mock(), Mock()
    context = _context()
    staged.snapshot.return_value = worker_snapshot(context)
    staged.item_snapshot.return_value = LocalFolderItemSnapshot(None, None, None, False, None, None)
    staged.persist_discovery.return_value = Mock(outcome="persisted")

    def discover(snapshot):
        assert all(session.closed for session in sessions.sessions)
        return worker_entry()

    def prepare(snapshot, item_snapshot, entry):
        assert all(session.closed for session in sessions.sessions)
        return _prepared(entry)

    preparation.discover_next.side_effect = discover
    preparation.prepare_item.side_effect = prepare
    worker = LocalFolderSyncWorker(
        sessions, lambda session: execution, lambda session: staged, preparation,
        worker_id="worker-one", batch_size=1, clock=lambda: NOW,
    )
    assert worker.execute(context).outcome == "in_progress"