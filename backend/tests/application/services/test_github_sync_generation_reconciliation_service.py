from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import logging
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.services.github_sync_generation_reconciliation_service import (
    GitHubSyncGenerationReconciliationDisabled,
    GitHubSyncGenerationReconciliationService,
    InvalidGitHubSyncGenerationReconciliationRequest,
)
from domain.connectors.sync_work_ledger import (
    GenerationReconciliationRequest,
    GenerationReconciliationResult,
)
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    ConnectorSyncWorkLedgerRepository,
)


NOW = datetime(2026, 9, 5, 12, tzinfo=timezone.utc)


def _request() -> GenerationReconciliationRequest:
    return GenerationReconciliationRequest(
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
        "github",
        "github:repository:123",
        "main",
        "a" * 40,
        "b" * 40,
        "github:extract-v1:chunk-v2:embed-v1",
    )


def test_reconciliation_is_default_off() -> None:
    repository = Mock(spec=ConnectorSyncWorkLedgerRepository)
    service = GitHubSyncGenerationReconciliationService(repository)
    with pytest.raises(GitHubSyncGenerationReconciliationDisabled):
        service.reconcile(_request(), now=NOW)
    repository.reconcile_generation.assert_not_called()


def test_enabled_reconciliation_delegates_and_logs_only_safe_identity(caplog) -> None:
    request = _request()
    result = GenerationReconciliationResult(
        request.generation_id, True, False, 2, 1, 1, 2, 1, 1
    )
    repository = Mock(spec=ConnectorSyncWorkLedgerRepository)
    repository.reconcile_generation.return_value = result
    service = GitHubSyncGenerationReconciliationService(repository, enabled=True)
    with caplog.at_level(logging.INFO):
        assert service.reconcile(request, now=NOW, limit=25) == result
    repository.reconcile_generation.assert_called_once_with(
        request, now=NOW, limit=25
    )
    assert "event=github_ledger_generation_reconciliation_prepared" in caplog.text
    assert request.repository_identity not in caplog.text
    assert request.commit_object_id not in caplog.text
    assert request.profile_fingerprint not in caplog.text


def test_reconciliation_constructor_rejects_nonboolean_gate() -> None:
    repository = Mock(spec=ConnectorSyncWorkLedgerRepository)
    with pytest.raises(ValueError, match="flag"):
        GitHubSyncGenerationReconciliationService(
            repository, enabled="true"  # type: ignore[arg-type]
        )


def test_non_github_request_is_rejected_before_repository_call() -> None:
    request = replace(_request(), provider_key="local_folder")
    repository = Mock(spec=ConnectorSyncWorkLedgerRepository)
    service = GitHubSyncGenerationReconciliationService(repository, enabled=True)

    with pytest.raises(InvalidGitHubSyncGenerationReconciliationRequest):
        service.reconcile(request, now=NOW)

    repository.reconcile_generation.assert_not_called()
