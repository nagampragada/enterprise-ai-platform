from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.dependencies import CurrentUser, get_current_user, get_db_session, get_local_document_indexing_service
from app.api.v1.documents.router import _validate_filename
from app.main import app
from application.services.document_chunk_embedding_service import DocumentChunkEmbeddingPersistenceError
from application.services.local_document_indexing_service import LocalDocumentIndexingSummary
from application.services.local_document_indexing_service import NonProgressingDocumentChunkPageError
from application.services.local_document_ingestion_service import LocalDocumentIngestionPersistenceError
from domain.content_extraction.exceptions import ContentParseError, ContentTooLargeError, EncryptedContentError
from domain.embeddings.exceptions import (
    EmbeddingProviderAuthenticationError,
    PermanentEmbeddingProviderError,
    RetryableEmbeddingProviderError,
)


@dataclass
class FakeSession:
    commit_calls: int = 0
    rollback_calls: int = 0
    close_calls: int = 0

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.close_calls += 1


def _summary(organization_id, filename="document.txt") -> LocalDocumentIndexingSummary:
    return LocalDocumentIndexingSummary(
        organization_id=organization_id,
        document_id=uuid4(),
        source_type="manual_upload",
        source_document_key=filename,
        ingestion_outcome="created",
        content_checksum="a" * 64,
        chunks_seen=3,
        chunks_embedded=3,
        chunks_skipped=0,
        provider_batches=1,
        embedded_chunk_ids=(uuid4(), uuid4(), uuid4()),
    )


def _setup(service, session, organization_id):
    def db_session_override():
        yield session

    app.dependency_overrides[get_db_session] = db_session_override
    app.dependency_overrides[get_current_user] = lambda: CurrentUser(
        user_id=uuid4(), organization_id=organization_id, email="user@example.com", display_name="User"
    )
    app.dependency_overrides[get_local_document_indexing_service] = lambda: service


@pytest.fixture(autouse=True)
def clear_overrides():
    yield
    app.dependency_overrides.clear()


def test_unauthenticated_upload_is_rejected() -> None:
    with TestClient(app) as client:
        response = client.post("/api/v1/documents/ingest", files={"file": ("document.txt", b"content", "text/plain")})
    assert response.status_code == 401


def test_authenticated_upload_commits_and_deletes_temporary_file() -> None:
    organization_id = uuid4()
    service = Mock()
    service.index.return_value = _summary(organization_id)
    session = FakeSession()
    seen_paths: list[Path] = []

    def index(**kwargs):
        seen_paths.append(kwargs["path"])
        assert kwargs["organization_id"] == organization_id
        assert kwargs["source_type"] == "manual_upload"
        assert kwargs["source_document_key"] == "document.txt"
        assert kwargs["path"].exists()
        return _summary(organization_id)

    service.index.side_effect = index
    _setup(service, session, organization_id)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/documents/ingest",
            files={"file": ("document.txt", b"content", "text/plain")},
        )

    assert response.status_code == 200
    assert response.json()["chunks_seen"] == 3
    assert "document_id" in response.json()
    assert "content_checksum" not in response.json()
    assert session.commit_calls == 1
    assert session.rollback_calls == 0
    assert seen_paths and not seen_paths[0].exists()


def test_request_cannot_choose_tenant_and_unsupported_extensions_are_rejected() -> None:
    organization_id = uuid4()
    service = Mock()
    session = FakeSession()
    _setup(service, session, organization_id)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/documents/ingest?organization_id=other",
            files={"file": ("document.csv", b"content", "text/plain")},
        )

    assert response.status_code == 415
    service.index.assert_not_called()


@pytest.mark.parametrize("filename", ["", "../document.txt", "folder\\document.txt"])
def test_invalid_filenames_are_rejected(filename: str) -> None:
    organization_id = uuid4()
    service = Mock()
    session = FakeSession()
    _setup(service, session, organization_id)

    with TestClient(app) as client:
        response = client.post("/api/v1/documents/ingest", files={"file": (filename, b"content", "text/plain")})

    assert response.status_code == 422
    service.index.assert_not_called()


def test_control_character_filename_is_rejected_by_route_validation() -> None:
    with pytest.raises(Exception):
        _validate_filename("bad\x01.txt")


def test_oversized_upload_rolls_back_without_coordinator_call() -> None:
    organization_id = uuid4()
    service = Mock()
    session = FakeSession()
    _setup(service, session, organization_id)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/documents/ingest",
            files={"file": ("large.txt", b"x" * (25 * 1024 * 1024 + 1), "text/plain")},
        )

    assert response.status_code == 413
    service.index.assert_not_called()
    assert session.commit_calls == 0
    assert session.rollback_calls == 1


def test_coordinator_failure_rolls_back_and_removes_temporary_file() -> None:
    organization_id = uuid4()
    service = Mock()
    session = FakeSession()
    seen_paths: list[Path] = []

    def fail(**kwargs):
        seen_paths.append(kwargs["path"])
        assert kwargs["path"].exists()
        raise RuntimeError("controlled failure")

    service.index.side_effect = fail
    _setup(service, session, organization_id)

    with TestClient(app) as client:
        response = client.post("/api/v1/documents/ingest", files={"file": ("document.md", b"# content", "text/markdown")})

    assert response.status_code == 500
    assert session.commit_calls == 0
    assert session.rollback_calls == 1
    assert seen_paths and not seen_paths[0].exists()


@pytest.mark.parametrize("extension", [".txt", ".md", ".markdown", ".docx", ".pdf"])
def test_all_supported_extensions_are_accepted(extension: str) -> None:
    organization_id = uuid4()
    service = Mock()
    service.index.return_value = _summary(organization_id, f"document{extension}")
    session = FakeSession()
    _setup(service, session, organization_id)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/documents/ingest",
            files={"file": (f"document{extension}", b"content", "application/octet-stream")},
        )

    assert response.status_code == 200
    service.index.assert_called_once()


def test_mime_type_cannot_make_unsupported_extension_valid() -> None:
    organization_id = uuid4()
    service = Mock()
    session = FakeSession()
    _setup(service, session, organization_id)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/documents/ingest",
            files={"file": ("document.exe", b"content", "text/plain")},
        )

    assert response.status_code == 415
    service.index.assert_not_called()


@pytest.mark.parametrize(
    ("error", "status_code"),
    [
        (ContentParseError("bad document"), 422),
        (EncryptedContentError("encrypted"), 422),
        (ContentTooLargeError("too large"), 413),
        (EmbeddingProviderAuthenticationError("auth"), 503),
        (RetryableEmbeddingProviderError("retry"), 503),
        (PermanentEmbeddingProviderError("permanent"), 502),
        (LocalDocumentIngestionPersistenceError("persistence"), 500),
        (DocumentChunkEmbeddingPersistenceError("chunk persistence"), 500),
        (NonProgressingDocumentChunkPageError("pagination"), 500),
        (RuntimeError("database secret and stack"), 500),
    ],
)
def test_known_and_unexpected_failures_map_to_safe_responses(error: Exception, status_code: int) -> None:
    organization_id = uuid4()
    service = Mock()
    service.index.side_effect = error
    session = FakeSession()
    _setup(service, session, organization_id)

    with TestClient(app) as client:
        response = client.post("/api/v1/documents/ingest", files={"file": ("document.txt", b"content", "text/plain")})

    assert response.status_code == status_code
    assert "secret" not in response.text
    assert "stack" not in response.text
    assert session.commit_calls == 0
    assert session.rollback_calls == 1


def test_provider_configuration_dependency_failure_returns_503_without_openai_call(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.dependencies import get_local_document_indexing_service
    monkeypatch.setattr(
        "app.dependencies.OpenAIEmbeddingProvider",
        lambda: (_ for _ in ()).throw(EmbeddingProviderAuthenticationError("missing key")),
    )
    app.dependency_overrides[get_current_user] = lambda: CurrentUser(uuid4(), uuid4(), "u@example.com", "User")
    app.dependency_overrides[get_db_session] = lambda: (value for value in (FakeSession(),))

    with TestClient(app) as client:
        response = client.post("/api/v1/documents/ingest", files={"file": ("document.txt", b"content", "text/plain")})

    assert response.status_code == 503
    assert "missing key" not in response.text
    assert get_local_document_indexing_service not in app.dependency_overrides
