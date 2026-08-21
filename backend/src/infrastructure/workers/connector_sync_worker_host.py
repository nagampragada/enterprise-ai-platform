"""Production host routing claimed Local Folder and GitHub synchronization jobs."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging
import os
import signal
import threading
from types import FrameType
from uuid import uuid4

from sqlalchemy.orm import Session

from app.config import (
    load_github_app_settings_from_environment,
    load_google_secret_manager_settings_from_environment,
)
from application.services.connector_sync_execution_service import (
    AcquiredRoutedSyncAttempt,
    ConnectorSyncExecutionService,
)
from application.services.connector_sync_retry_policy import ConnectorSyncRetryPolicy
from application.services.connector_sync_retry_policy import SyncFailureKind
from application.services.github_repository_content_service import GitHubRepositoryContentService
from application.services.github_staged_synchronization_service import (
    GitHubStagedSynchronizationService,
    GitHubSynchronizationPreparationService,
)
from application.services.staged_local_folder_synchronization_service import (
    LocalFolderPreparationService,
    StagedLocalFolderSynchronizationService,
)
from infrastructure.connectors.github.client import GitHubAppRestClient
from infrastructure.content_chunking.text_chunker import DeterministicTextChunker
from infrastructure.content_extraction.registry import create_default_content_extractor_registry
from infrastructure.db.session import SessionLocal
from infrastructure.embeddings.openai import OpenAIEmbeddingProvider
from infrastructure.repositories.connector_sync_job_repository import ConnectorSyncJobRepository
from infrastructure.secrets.google_secret_manager import GoogleSecretManagerSecretStore
from infrastructure.workers.github_sync_worker import GitHubSyncWorker
from infrastructure.workers.local_folder_sync_worker import LocalFolderSyncWorker

LOGGER = logging.getLogger(__name__)
SUPPORTED_CONNECTOR_TYPES = frozenset({"local_folder", "github"})


@dataclass(frozen=True)
class ConnectorWorkerSettings:
    worker_id: str
    lease_duration: timedelta
    heartbeat_interval: timedelta
    idle_interval: timedelta
    shutdown_timeout: timedelta
    recovery_limit: int
    one_shot: bool = False

    def __post_init__(self) -> None:
        if not self.worker_id or len(self.worker_id) > 255:
            raise ValueError("worker identity is invalid")
        for name, value, maximum in (
            ("lease_duration", self.lease_duration, 3600),
            ("heartbeat_interval", self.heartbeat_interval, 1800),
            ("idle_interval", self.idle_interval, 300),
            ("shutdown_timeout", self.shutdown_timeout, 3600),
        ):
            if not isinstance(value, timedelta) or not 0 < value.total_seconds() <= maximum:
                raise ValueError(f"{name} is outside the allowed range")
        if self.heartbeat_interval.total_seconds() * 2 >= self.lease_duration.total_seconds():
            raise ValueError("heartbeat interval must leave one full renewal margin")
        if isinstance(self.recovery_limit, bool) or not 1 <= self.recovery_limit <= 100:
            raise ValueError("recovery_limit is outside the allowed range")

    @classmethod
    def from_environment(cls, argv: Sequence[str] | None = None,
                         environ: Mapping[str, str] | None = None):
        parser = argparse.ArgumentParser(description="Run connector synchronization worker")
        parser.add_argument("--once", action="store_true")
        args = parser.parse_args(argv)
        values = os.environ if environ is None else environ
        duration = lambda name, default: timedelta(seconds=float(values.get(name, default)))
        return cls(
            values.get("CONNECTOR_WORKER_ID", f"connector-{uuid4().hex}"),
            duration("CONNECTOR_WORKER_LEASE_SECONDS", 900),
            duration("CONNECTOR_WORKER_HEARTBEAT_SECONDS", 60),
            duration("CONNECTOR_WORKER_IDLE_SECONDS", 5),
            duration("CONNECTOR_WORKER_SHUTDOWN_SECONDS", 300),
            int(values.get("CONNECTOR_WORKER_RECOVERY_LIMIT", "10")),
            args.once,
        )


class ConnectorSyncWorkerHost:
    def __init__(self, session_factory, execution_factory, local_worker, github_worker,
                 settings, *, shutdown_event=None, wait=None, logger=LOGGER):
        self._sessions = session_factory
        self._execution = execution_factory
        self._local = local_worker
        self._github = github_worker
        self._settings = settings
        self._shutdown = shutdown_event or threading.Event()
        self._wait = wait or self._shutdown.wait
        self._logger = logger

    @property
    def shutdown_event(self):
        return self._shutdown

    def run(self) -> int:
        while not self._shutdown.is_set():
            result = self.run_cycle()
            if self._settings.one_shot:
                return 0 if result in {"completed", "no_work"} else 1
            if result == "no_work" and self._wait(self._settings.idle_interval.total_seconds()):
                break
        return 0

    def run_cycle(self) -> str:
        if self._shutdown.is_set():
            return "shutdown"
        acquired = self._recover_and_claim()
        if acquired is None:
            return "no_work"
        if self._shutdown.is_set():
            return "shutdown"
        context = self._local.attempt_context(acquired)
        worker = {"local_folder": self._local, "github": self._github}.get(
            acquired.connector_type
        )
        if worker is None:
            return self._fail_unsupported(context)
        self._logger.info(
            "event=job_claimed connector_type=%s job_id=%s attempt=%d",
            acquired.connector_type, context.job_id, context.attempt_number,
        )
        while not self._shutdown.is_set():
            result = worker.execute(context)
            if result.outcome != "in_progress":
                return result.outcome
        return "shutdown"

    def _fail_unsupported(self, context) -> str:
        session = self._sessions()
        try:
            result = self._execution(session).fail_attempt(
                _context_lease(context),
                worker_id=self._settings.worker_id,
                kind=SyncFailureKind.VALIDATION,
            )
            session.commit()
            return "retry_scheduled" if result.status == "retry_wait" else "failed"
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _recover_and_claim(self) -> AcquiredRoutedSyncAttempt | None:
        session = self._sessions()
        try:
            execution = self._execution(session)
            recovered = execution.recover_expired_routed(limit=self._settings.recovery_limit)
            if recovered:
                self._logger.info("event=expired_jobs_recovered count=%d", len(recovered))
            acquired = None
            if not self._shutdown.is_set():
                acquired = execution.acquire_one_routed(
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


def compose_connector_sync_worker_host(settings: ConnectorWorkerSettings,
                                       *, session_factory=SessionLocal,
                                       shutdown_event=None):
    secret_store = GoogleSecretManagerSecretStore(
        load_google_secret_manager_settings_from_environment()
    )
    github_client = GitHubAppRestClient(
        load_github_app_settings_from_environment(), secret_store
    )
    embedding = OpenAIEmbeddingProvider()
    extractors = create_default_content_extractor_registry()
    chunker = DeterministicTextChunker()
    retry = ConnectorSyncRetryPolicy()

    def execution(session: Session):
        return ConnectorSyncExecutionService(
            ConnectorSyncJobRepository(session), retry, clock=lambda: datetime.now(UTC)
        )

    local_preparation = LocalFolderPreparationService(extractors, chunker, embedding)
    local = LocalFolderSyncWorker(
        session_factory, execution,
        lambda session: StagedLocalFolderSynchronizationService(
            session, execution(session), local_preparation.profile
        ),
        local_preparation,
        worker_id=settings.worker_id,
        lease_duration=settings.lease_duration,
        heartbeat_target=settings.heartbeat_interval,
    )
    github_preparation = GitHubSynchronizationPreparationService(
        GitHubRepositoryContentService(None, github_client),
        extractors, chunker, embedding,
    )
    github = GitHubSyncWorker(
        session_factory, execution,
        lambda session: GitHubStagedSynchronizationService(
            session, execution(session), GitHubRepositoryContentService(session, github_client),
            github_preparation.profile,
        ),
        github_preparation,
        worker_id=settings.worker_id,
        lease_duration=settings.lease_duration,
        heartbeat_interval=settings.heartbeat_interval,
        heartbeat_shutdown_timeout=settings.shutdown_timeout,
    )
    return ConnectorSyncWorkerHost(
        session_factory, execution, local, github, settings, shutdown_event=shutdown_event
    )


def install_shutdown_signal_handlers(event: threading.Event) -> None:
    def stop(_signum: int, _frame: FrameType | None) -> None:
        event.set()
    signal.signal(signal.SIGINT, stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, stop)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        settings = ConnectorWorkerSettings.from_environment(argv)
        shutdown = threading.Event()
        install_shutdown_signal_handlers(shutdown)
        return compose_connector_sync_worker_host(settings, shutdown_event=shutdown).run()
    except Exception as error:
        LOGGER.error("event=worker_startup_failed error_type=%s", type(error).__name__)
        return 1


def _context_lease(context):
    from infrastructure.repositories.connector_sync_job_repository import SyncJobLease
    return SyncJobLease(
        context.organization_id, context.job_id, context.connector_id,
        context.connector_scope_id, context.mode, context.trigger_type,
        context.attempt_number, context.max_attempts, context.lease_id,
        context.fencing_token, context.lease_expires_at,
    )


if __name__ == "__main__":
    raise SystemExit(main())
