from __future__ import annotations

from datetime import datetime, timezone
import logging
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.services.github_sync_generation_promotion_service import (
    GitHubSyncGenerationPromotionDisabled,
    GitHubSyncGenerationPromotionService,
)
from domain.connectors.sync_work_ledger import (
    GenerationActivationStatus,
    GenerationActivationView,
    GenerationPromotionRequest,
    GenerationPromotionResult,
)
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    ConnectorSyncWorkLedgerRepository,
)


NOW = datetime(2026, 9, 4, 12, tzinfo=timezone.utc)


def _request() -> GenerationPromotionRequest:
    return GenerationPromotionRequest(
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


def _result(request: GenerationPromotionRequest) -> GenerationPromotionResult:
    return GenerationPromotionResult(
        GenerationActivationView(
            uuid4(),
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            request.generation_id,
            request.repository_identity,
            request.commit_object_id,
            request.profile_fingerprint,
            GenerationActivationStatus.ACTIVE,
            NOW,
            None,
            NOW,
            NOW,
        ),
        True,
        None,
        2,
        3,
    )


def test_promotion_is_default_off_and_does_not_touch_repository() -> None:
    repository = Mock(spec=ConnectorSyncWorkLedgerRepository)
    service = GitHubSyncGenerationPromotionService(repository)
    with pytest.raises(GitHubSyncGenerationPromotionDisabled):
        service.promote(_request(), now=NOW)
    repository.promote_generation.assert_not_called()


def test_enabled_promotion_delegates_once_and_logs_only_safe_identity(caplog) -> None:
    request = _request()
    result = _result(request)
    repository = Mock(spec=ConnectorSyncWorkLedgerRepository)
    repository.promote_generation.return_value = result
    service = GitHubSyncGenerationPromotionService(repository, enabled=True)
    with caplog.at_level(logging.INFO):
        assert service.promote(request, now=NOW) == result
    repository.promote_generation.assert_called_once_with(request, now=NOW)
    assert "event=github_ledger_generation_promotion_prepared" in caplog.text
    assert request.repository_identity not in caplog.text
    assert request.commit_object_id not in caplog.text
    assert request.profile_fingerprint not in caplog.text


def test_promotion_constructor_rejects_nonboolean_gate() -> None:
    repository = Mock(spec=ConnectorSyncWorkLedgerRepository)
    with pytest.raises(ValueError, match="flag"):
        GitHubSyncGenerationPromotionService(repository, enabled="true")  # type: ignore[arg-type]
