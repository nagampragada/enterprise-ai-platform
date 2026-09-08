"""Feature-gated atomic activation of complete GitHub ledger generations."""

from __future__ import annotations

from datetime import datetime
import logging

from domain.connectors.sync_work_ledger import (
    GenerationCitationProjectionProfile,
    GenerationPromotionRequest,
    GenerationPromotionResult,
)
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    ConnectorSyncWorkLedgerRepository,
)


LOGGER = logging.getLogger(__name__)
GITHUB_SYNC_LEDGER_PROMOTION_ENVIRONMENT_VARIABLE = (
    "GITHUB_SYNC_LEDGER_PROMOTION_ENABLED"
)


class GitHubSyncGenerationPromotionDisabled(RuntimeError):
    """Raised when promotion is requested while its rollout gate is disabled."""


class GitHubSyncGenerationPromotionService:
    """Validate and stage one caller-committed atomic retrieval cutover."""

    def __init__(
        self,
        repository: ConnectorSyncWorkLedgerRepository,
        *,
        enabled: bool = False,
        logger: logging.Logger = LOGGER,
    ) -> None:
        if not isinstance(repository, ConnectorSyncWorkLedgerRepository):
            raise ValueError("GitHub generation-promotion repository is invalid")
        if not isinstance(enabled, bool):
            raise ValueError("GitHub generation-promotion flag is invalid")
        self._repository = repository
        self._enabled = enabled
        self._logger = logger

    def promote(
        self, request: GenerationPromotionRequest, *, now: datetime
    ) -> GenerationPromotionResult:
        if not self._enabled:
            raise GitHubSyncGenerationPromotionDisabled(
                "GitHub generation promotion is disabled"
            )
        result = self._repository.promote_generation(request, now=now)
        self._logger.info(
            "event=github_ledger_generation_promotion_prepared "
            "organization_id=%s connector_id=%s connector_scope_id=%s "
            "generation_id=%s promoted=%s retired_generation_id=%s "
            "materializations=%d chunks=%d",
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            request.generation_id,
            str(result.promoted).lower(),
            result.retired_generation_id,
            result.materialization_count,
            result.chunk_count,
        )
        return result

    def project_and_promote(
        self,
        request: GenerationPromotionRequest,
        profile: GenerationCitationProjectionProfile,
        *,
        now: datetime,
    ) -> GenerationPromotionResult:
        """Project provider-free citations and prepare one atomic cutover."""
        if not self._enabled:
            raise GitHubSyncGenerationPromotionDisabled(
                "GitHub generation promotion is disabled"
            )
        result = self._repository.project_citations_and_promote_generation(
            request, profile, now=now
        )
        self._logger.info(
            "event=github_ledger_generation_projection_promotion_prepared "
            "organization_id=%s connector_id=%s connector_scope_id=%s "
            "generation_id=%s promoted=%s materializations=%d chunks=%d",
            request.organization_id,
            request.connector_id,
            request.connector_scope_id,
            request.generation_id,
            str(result.promoted).lower(),
            result.materialization_count,
            result.chunk_count,
        )
        return result
