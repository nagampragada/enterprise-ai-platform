"""Feature-gated bounded lifecycle reconciliation for active GitHub generations."""

from __future__ import annotations

from datetime import datetime
import logging

from domain.connectors.sync_work_ledger import (
    GenerationReconciliationRequest,
    GenerationReconciliationResult,
)
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    ConnectorSyncWorkLedgerRepository,
)


LOGGER = logging.getLogger(__name__)
GITHUB_SYNC_LEDGER_RECONCILIATION_ENVIRONMENT_VARIABLE = (
    "GITHUB_SYNC_LEDGER_RECONCILIATION_ENABLED"
)


class GitHubSyncGenerationReconciliationDisabled(RuntimeError):
    """Raised when reconciliation is requested while its rollout gate is disabled."""


class InvalidGitHubSyncGenerationReconciliationRequest(ValueError):
    """Raised when a reconciliation request is outside the GitHub boundary."""


class GitHubSyncGenerationReconciliationService:
    """Prepare one caller-committed, bounded lifecycle-retirement transaction."""

    def __init__(
        self,
        repository: ConnectorSyncWorkLedgerRepository,
        *,
        enabled: bool = False,
        logger: logging.Logger = LOGGER,
    ) -> None:
        if not isinstance(repository, ConnectorSyncWorkLedgerRepository):
            raise ValueError("GitHub generation-reconciliation repository is invalid")
        if not isinstance(enabled, bool):
            raise ValueError("GitHub generation-reconciliation flag is invalid")
        self._repository = repository
        self._enabled = enabled
        self._logger = logger

    def reconcile(
        self,
        request: GenerationReconciliationRequest,
        *,
        now: datetime,
        limit: int = 100,
    ) -> GenerationReconciliationResult:
        if not self._enabled:
            raise GitHubSyncGenerationReconciliationDisabled(
                "GitHub generation reconciliation is disabled"
            )
        if (
            not isinstance(request, GenerationReconciliationRequest)
            or request.provider_key != "github"
        ):
            raise InvalidGitHubSyncGenerationReconciliationRequest(
                "GitHub generation reconciliation request is invalid"
            )
        result = self._repository.reconcile_generation(
            request, now=now, limit=limit
        )
        self._logger.info(
            "event=github_ledger_generation_reconciliation_prepared "
            "organization_id=%s connector_id=%s connector_scope_id=%s "
            "generation_id=%s completed=%s replayed=%s "
            "memberships_retired=%d sources_retired=%d documents_retired=%d",
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            request.generation_id,
            str(result.completed).lower(),
            str(result.replayed).lower(),
            result.memberships_retired,
            result.sources_retired,
            result.documents_retired,
        )
        return result
