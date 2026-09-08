"""Dedicated, provider-read-only host for bounded GitHub ledger discovery."""

from __future__ import annotations

import logging
import signal
import threading
import time
from types import FrameType
from uuid import uuid4

from sqlalchemy.orm import Session

from app.config import (
    GitHubPlannerProcessSettings,
    validate_github_planner_process_environment,
)
from application.services.connector_sync_execution_service import (
    ConnectorSyncExecutionService,
)
from application.services.connector_sync_retry_policy import ConnectorSyncRetryPolicy
from application.services.github_repository_content_service import (
    GitHubRepositoryContentService,
)
from application.services.github_staged_synchronization_service import (
    GitHubStagedSynchronizationService,
    GitHubSynchronizationPreparationService,
)
from domain.embeddings.models import EmbeddingProfile
from domain.embeddings.provider import EmbeddingProvider
from infrastructure.connectors.github.client import GitHubAppRestClient
from infrastructure.content_chunking.text_chunker import DeterministicTextChunker
from infrastructure.content_extraction.registry import create_default_content_extractor_registry
from infrastructure.db.session import SessionLocal
from infrastructure.embeddings.openai.provider import (
    OPENAI_DIMENSION,
    OPENAI_MAX_BATCH_SIZE,
    OPENAI_MODEL,
)
from infrastructure.repositories.connector_sync_job_repository import (
    ConnectorSyncJobRepository,
)
from infrastructure.secrets.google_secret_manager import GoogleSecretManagerSecretStore
from infrastructure.workers.connector_sync_worker_host import ConnectorWorkerSettings
from infrastructure.workers.github_sync_ledger_planner_worker import (
    GitHubSyncLedgerPlannerWorker,
)
from infrastructure.workers.local_folder_sync_worker import LocalFolderAttemptContext


LOGGER = logging.getLogger(__name__)


class _ProfileOnlyEmbeddingProvider(EmbeddingProvider):
    """Expose the immutable production profile without constructing OpenAI."""

    @property
    def profile(self) -> EmbeddingProfile:
        return EmbeddingProfile(
            provider_name="openai",
            model_name=OPENAI_MODEL,
            dimension=OPENAI_DIMENSION,
            model_identifier=f"openai:{OPENAI_MODEL}:{OPENAI_DIMENSION}",
            max_batch_size=OPENAI_MAX_BATCH_SIZE,
        )

    def embed_batch(self, requests):
        raise RuntimeError("planner cannot execute embedding operations")


class GitHubSyncLedgerPlannerHost:
    """Claim only GitHub jobs and drive ledger discovery to a bounded terminal state."""

    def __init__(
        self,
        session_factory,
        execution_factory,
        worker: GitHubSyncLedgerPlannerWorker | None,
        settings: ConnectorWorkerSettings,
        *,
        planning_enabled: bool,
        shutdown_event=None,
        monotonic=time.monotonic,
        logger=LOGGER,
    ) -> None:
        if not isinstance(planning_enabled, bool):
            raise ValueError("GitHub ledger-planning flag is invalid")
        self._sessions = session_factory
        self._execution = execution_factory
        self._worker = worker
        self._settings = settings
        self._planning_enabled = planning_enabled
        self._shutdown = shutdown_event or threading.Event()
        self._monotonic = monotonic
        self._logger = logger

    @property
    def shutdown_event(self):
        return self._shutdown

    def run(self) -> int:
        if not self._planning_enabled:
            self._logger.info("event=github_ledger_planner_disabled")
            return 0
        if self._worker is None:
            raise RuntimeError("GitHub ledger planner is unavailable")
        acquired = self._recover_and_claim()
        if acquired is None:
            self._logger.info("event=github_ledger_planner_empty")
            return 0
        context = _attempt_context(acquired, self._settings.worker_id)
        self._logger.info(
            "event=github_ledger_planner_job_claimed organization_id=%s "
            "connector_id=%s connector_scope_id=%s job_id=%s attempt=%d",
            context.organization_id,
            context.connector_id,
            context.connector_scope_id,
            context.job_id,
            context.attempt_number,
        )
        batches = 0
        deadline = (
            self._monotonic()
            + self._settings.planner_max_execution_duration.total_seconds()
        )
        while not self._shutdown.is_set():
            if batches >= self._settings.planner_max_batches_per_execution:
                self._logger.info(
                    "event=github_ledger_planner_summary job_id=%s "
                    "outcome=resumable stop_reason=batch_limit batches=%d",
                    context.job_id,
                    batches,
                )
                return 0
            if self._monotonic() >= deadline:
                self._logger.info(
                    "event=github_ledger_planner_summary job_id=%s "
                    "outcome=resumable stop_reason=runtime_limit batches=%d",
                    context.job_id,
                    batches,
                )
                return 0
            result = self._worker.execute(context)
            batches += 1
            if result.outcome != "in_progress":
                self._logger.info(
                    "event=github_ledger_planner_summary job_id=%s outcome=%s batches=%d",
                    context.job_id,
                    result.outcome,
                    batches,
                )
                return 0 if result.outcome in {"completed", "cancelled"} else 1
        self._logger.info(
            "event=github_ledger_planner_summary job_id=%s outcome=shutdown batches=%d",
            context.job_id,
            batches,
        )
        return 0

    def _recover_and_claim(self):
        session = self._sessions()
        try:
            execution = self._execution(session)
            recovered = execution.recover_expired_github(
                limit=self._settings.recovery_limit
            )
            if recovered:
                self._logger.info(
                    "event=github_ledger_planner_expired_jobs_recovered count=%d",
                    len(recovered),
                )
            acquired = None
            if not self._shutdown.is_set():
                acquired = execution.acquire_one_github(
                    worker_id=self._settings.worker_id,
                    lease_duration=self._settings.lease_duration,
                )
            session.commit()
            return acquired
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def compose_github_sync_ledger_planner_host(
    settings: ConnectorWorkerSettings,
    *,
    process_settings: GitHubPlannerProcessSettings,
    session_factory=SessionLocal,
    shutdown_event=None,
) -> GitHubSyncLedgerPlannerHost:
    if not process_settings.github_sync_ledger_planning_enabled:
        raise ValueError("GitHub ledger planning is disabled")
    secret_store = GoogleSecretManagerSecretStore(process_settings.secret_manager)
    github_client = GitHubAppRestClient(process_settings.github, secret_store)
    retry = ConnectorSyncRetryPolicy()

    def execution(session: Session):
        from datetime import UTC, datetime

        return ConnectorSyncExecutionService(
            ConnectorSyncJobRepository(session),
            retry,
            clock=lambda: datetime.now(UTC),
        )

    preparation = GitHubSynchronizationPreparationService(
        GitHubRepositoryContentService(None, github_client),
        create_default_content_extractor_registry(),
        DeterministicTextChunker(),
        _ProfileOnlyEmbeddingProvider(),
    )

    def staged(session: Session):
        return GitHubStagedSynchronizationService(
            session,
            execution(session),
            GitHubRepositoryContentService(session, github_client),
            preparation.profile,
            ledger_planning_enabled=True,
        )

    worker = GitHubSyncLedgerPlannerWorker(
        session_factory,
        execution,
        staged,
        preparation,
        worker_id=settings.worker_id,
        lease_duration=settings.lease_duration,
        heartbeat_interval=settings.heartbeat_interval,
        heartbeat_shutdown_timeout=settings.shutdown_timeout,
    )
    return GitHubSyncLedgerPlannerHost(
        session_factory,
        execution,
        worker,
        settings,
        planning_enabled=True,
        shutdown_event=shutdown_event,
    )


def _attempt_context(acquired, worker_id: str) -> LocalFolderAttemptContext:
    lease = acquired.lease
    return LocalFolderAttemptContext(
        lease.organization_id,
        lease.job_id,
        lease.connector_id,
        lease.connector_scope_id,
        acquired.sync_run_id,
        lease.attempt_number,
        worker_id,
        lease.lease_id,
        lease.fencing_token,
        lease.lease_expires_at,
        lease.mode,
        lease.trigger_type,
        lease.max_attempts,
    )


def install_shutdown_signal_handlers(event: threading.Event) -> None:
    def stop(_signum: int, _frame: FrameType | None) -> None:
        event.set()

    signal.signal(signal.SIGINT, stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, stop)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        process_settings = validate_github_planner_process_environment()
        settings = ConnectorWorkerSettings.from_environment(argv)
        shutdown = threading.Event()
        install_shutdown_signal_handlers(shutdown)
        if not process_settings.github_sync_ledger_planning_enabled:
            return GitHubSyncLedgerPlannerHost(
                SessionLocal,
                lambda session: None,
                None,
                settings,
                planning_enabled=False,
                shutdown_event=shutdown,
            ).run()
        return compose_github_sync_ledger_planner_host(
            settings,
            process_settings=process_settings,
            shutdown_event=shutdown,
        ).run()
    except Exception as error:
        LOGGER.error(
            "event=github_ledger_planner_startup_failed error_type=%s",
            type(error).__name__,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
