"""Dedicated bounded host for isolated GitHub ledger file processing."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum
import logging
import os
import random
import re
import signal
import threading
import time
from types import FrameType
from uuid import uuid4

from openai import OpenAI
from sqlalchemy.orm import Session

from app.config import WorkerProcessSettings, validate_worker_process_environment
from application.services.connector_sync_retry_policy import ConnectorSyncRetryPolicy
from application.services.github_repository_content_service import (
    GitHubRepositoryContentService,
)
from application.services.github_staged_synchronization_service import (
    GitHubSynchronizationPreparationService,
)
from application.services.github_sync_work_processing_service import (
    GitHubSyncWorkProcessingService,
)
from domain.connectors.sync_work_ledger import FileWorkCounters
from infrastructure.connectors.github.client import GitHubAppRestClient
from infrastructure.content_chunking.text_chunker import DeterministicTextChunker
from infrastructure.content_extraction.registry import (
    create_default_content_extractor_registry,
)
from infrastructure.db.session import SessionLocal
from infrastructure.embeddings.openai import OpenAIEmbeddingProvider
from infrastructure.repositories.connector_sync_work_ledger_repository import (
    ConnectorSyncWorkLedgerRepository,
)
from infrastructure.secrets.google_secret_manager import (
    GoogleSecretManagerSecretStore,
)
from infrastructure.workers.github_sync_work_item_worker import (
    FileWorkGracefulShutdownExpired,
    GitHubFileWorkExecution,
    GitHubSyncWorkItemWorker,
)


LOGGER = logging.getLogger(__name__)
OPENAI_PROVIDER_CALL_TIMEOUT_SECONDS = 600.0


class GitHubLedgerHostExitCode(IntEnum):
    SUCCESS = 0
    FAILURE = 1
    CLEAN_DRAIN = 0
    FATAL = 1
    DISABLED = 0
    EMPTY_QUEUE = 0
    RETRY_SCHEDULED = 0
    LEASE_OR_FENCE_LOST = 1
    ITEM_FAILED = 1
    SHUTDOWN = 0


class GitHubLedgerShutdownState:
    """Signal state with a monotonic active-item grace deadline."""

    def __init__(
        self,
        graceful_timeout: timedelta,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._event = threading.Event()
        self._grace_seconds = graceful_timeout.total_seconds()
        self._monotonic = monotonic
        self._requested_at: float | None = None
        self._lock = threading.Lock()

    def set(self) -> None:
        with self._lock:
            if self._requested_at is None:
                self._requested_at = self._monotonic()
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float) -> bool:
        return self._event.wait(timeout)

    def raise_if_grace_expired(self) -> None:
        with self._lock:
            requested_at = self._requested_at
        if (
            requested_at is not None
            and self._monotonic() - requested_at >= self._grace_seconds
        ):
            raise FileWorkGracefulShutdownExpired(
                "GitHub ledger graceful shutdown window expired"
            )


@dataclass(frozen=True)
class GitHubLedgerWorkerSettings:
    processing_enabled: bool
    worker_id: str
    max_items_per_execution: int
    max_execution_duration: timedelta
    minimum_claim_time: timedelta
    lease_duration: timedelta
    heartbeat_interval: timedelta
    idle_interval: timedelta
    empty_poll_limit: int
    shutdown_timeout: timedelta
    recovery_limit: int
    provider_call_timeout: timedelta = timedelta(
        seconds=OPENAI_PROVIDER_CALL_TIMEOUT_SECONDS
    )

    def __post_init__(self) -> None:
        if not isinstance(self.processing_enabled, bool):
            raise ValueError("processing_enabled must be a boolean")
        if not isinstance(self.worker_id, str) or re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,254}", self.worker_id
        ) is None:
            raise ValueError("worker identity is invalid")
        _bounded_integer(
            "max_items_per_execution", self.max_items_per_execution, 500
        )
        _bounded_integer("empty_poll_limit", self.empty_poll_limit, 100)
        _bounded_integer("recovery_limit", self.recovery_limit, 500)
        for name, value, maximum in (
            ("max_execution_duration", self.max_execution_duration, 3600),
            ("minimum_claim_time", self.minimum_claim_time, 1800),
            ("lease_duration", self.lease_duration, 3600),
            ("heartbeat_interval", self.heartbeat_interval, 1800),
            ("idle_interval", self.idle_interval, 60),
            ("shutdown_timeout", self.shutdown_timeout, 3600),
            ("provider_call_timeout", self.provider_call_timeout, 600),
        ):
            _bounded_duration(name, value, maximum)
        if self.heartbeat_interval * 2 >= self.lease_duration:
            raise ValueError("heartbeat interval must leave one full renewal margin")
        safe_provider_window = self.provider_call_timeout + self.heartbeat_interval * 2
        if self.minimum_claim_time < safe_provider_window:
            raise ValueError("minimum claim time is incompatible with provider timeout")
        if self.minimum_claim_time > self.max_execution_duration:
            raise ValueError("minimum claim time exceeds execution duration")
        if self.lease_duration <= safe_provider_window:
            raise ValueError("lease duration is incompatible with provider timeout")
        if self.shutdown_timeout > self.lease_duration:
            raise ValueError("shutdown timeout exceeds lease duration")

    @classmethod
    def from_environment(
        cls,
        argv: Sequence[str] | None = None,
        environ: Mapping[str, str] | None = None,
        *,
        processing_enabled: bool,
    ) -> GitHubLedgerWorkerSettings:
        parser = argparse.ArgumentParser(
            description="Drain isolated GitHub synchronization ledger work",
            exit_on_error=False,
        )
        parser.add_argument("--worker-id")
        parser.add_argument("--max-items")
        parser.add_argument("--max-seconds")
        parser.add_argument("--minimum-claim-seconds")
        parser.add_argument("--lease-seconds")
        parser.add_argument("--heartbeat-seconds")
        parser.add_argument("--idle-seconds")
        parser.add_argument("--empty-polls")
        parser.add_argument("--shutdown-seconds")
        parser.add_argument("--recovery-limit")
        args = parser.parse_args(argv)
        values = os.environ if environ is None else environ

        def setting(argument: str | None, name: str, default: str) -> str:
            return argument if argument is not None else values.get(name, default)

        return cls(
            processing_enabled=processing_enabled,
            worker_id=setting(
                args.worker_id,
                "GITHUB_LEDGER_WORKER_ID",
                f"github-ledger-{uuid4().hex}",
            ),
            max_items_per_execution=_strict_positive_integer(
                setting(
                    args.max_items,
                    "GITHUB_LEDGER_WORKER_MAX_ITEMS_PER_EXECUTION",
                    "25",
                )
            ),
            max_execution_duration=_seconds(
                setting(
                    args.max_seconds,
                    "GITHUB_LEDGER_WORKER_MAX_EXECUTION_SECONDS",
                    "1200",
                )
            ),
            minimum_claim_time=_seconds(
                setting(
                    args.minimum_claim_seconds,
                    "GITHUB_LEDGER_WORKER_MINIMUM_CLAIM_SECONDS",
                    "720",
                )
            ),
            lease_duration=_seconds(
                setting(
                    args.lease_seconds,
                    "GITHUB_LEDGER_WORKER_LEASE_SECONDS",
                    "900",
                )
            ),
            heartbeat_interval=_seconds(
                setting(
                    args.heartbeat_seconds,
                    "GITHUB_LEDGER_WORKER_HEARTBEAT_SECONDS",
                    "60",
                )
            ),
            idle_interval=_seconds(
                setting(
                    args.idle_seconds,
                    "GITHUB_LEDGER_WORKER_IDLE_SECONDS",
                    "5",
                )
            ),
            empty_poll_limit=_strict_positive_integer(
                setting(
                    args.empty_polls,
                    "GITHUB_LEDGER_WORKER_EMPTY_POLL_LIMIT",
                    "1",
                )
            ),
            shutdown_timeout=_seconds(
                setting(
                    args.shutdown_seconds,
                    "GITHUB_LEDGER_WORKER_SHUTDOWN_SECONDS",
                    "300",
                )
            ),
            recovery_limit=_strict_positive_integer(
                setting(
                    args.recovery_limit,
                    "GITHUB_LEDGER_WORKER_RECOVERY_LIMIT",
                    "10",
                )
            ),
            provider_call_timeout=timedelta(
                seconds=OPENAI_PROVIDER_CALL_TIMEOUT_SECONDS
            ),
        )


@dataclass(frozen=True)
class GitHubLedgerExecutionSummary:
    run_id: str
    items_examined: int
    items_claimed: int
    succeeded: int
    retry_scheduled: int
    quarantined: int
    cancelled: int
    failed: int
    lease_or_fence_lost: int
    empty_queue: bool
    downloaded_bytes: int
    extracted_characters: int
    chunks: int
    embedding_batches: int
    duration_seconds: float
    stop_reason: str
    run_status: str
    graceful_shutdown: bool
    partial_drain: bool
    exit_code: int


class GitHubSyncLedgerWorkerHost:
    """Drain only isolated GitHub file work under per-execution bounds."""

    def __init__(
        self,
        worker: GitHubSyncWorkItemWorker | None,
        settings: GitHubLedgerWorkerSettings,
        *,
        shutdown_event: GitHubLedgerShutdownState | threading.Event | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        wait: Callable[[float], bool] | None = None,
        run_id_factory: Callable[[], object] = uuid4,
        logger: logging.Logger = LOGGER,
    ) -> None:
        self._worker = worker
        self._settings = settings
        self._shutdown = shutdown_event or threading.Event()
        self._monotonic = monotonic
        self._wait = wait or self._shutdown.wait
        self._run_id_factory = run_id_factory
        self._logger = logger
        self.last_summary: GitHubLedgerExecutionSummary | None = None

    @property
    def shutdown_event(self) -> GitHubLedgerShutdownState | threading.Event:
        return self._shutdown

    def run(self) -> int:
        started = self._monotonic()
        run_id = str(self._run_id_factory())
        counts = {
            "items_examined": 0,
            "items_claimed": 0,
            "succeeded": 0,
            "retry_scheduled": 0,
            "quarantined": 0,
            "cancelled": 0,
            "failed": 0,
            "lease_or_fence_lost": 0,
            "downloaded_bytes": 0,
            "extracted_characters": 0,
            "chunks": 0,
            "embedding_batches": 0,
        }
        empty_polls = 0
        stop_reason = "disabled"
        exit_code = GitHubLedgerHostExitCode.DISABLED
        try:
            if not self._settings.processing_enabled:
                return self._finish(
                    run_id, started, counts, False, stop_reason, exit_code
                )
            if self._worker is None:
                raise RuntimeError("GitHub ledger worker is unavailable")
            deadline = started + self._settings.max_execution_duration.total_seconds()
            while True:
                if self._shutdown.is_set():
                    stop_reason = "shutdown_requested"
                    exit_code = GitHubLedgerHostExitCode.SHUTDOWN
                    break
                if counts["items_claimed"] >= self._settings.max_items_per_execution:
                    stop_reason = "item_limit"
                    exit_code = GitHubLedgerHostExitCode.CLEAN_DRAIN
                    break
                if not self._has_claim_time(deadline):
                    stop_reason = "runtime_limit"
                    exit_code = GitHubLedgerHostExitCode.CLEAN_DRAIN
                    break

                counts["items_examined"] += 1
                item_started = self._monotonic()
                result = self._worker.execute_one_result(
                    claim_allowed=lambda: (
                        not self._shutdown.is_set()
                        and counts["items_claimed"]
                        < self._settings.max_items_per_execution
                        and self._has_claim_time(deadline)
                    )
                )
                if result.outcome == "no_work":
                    if self._shutdown.is_set():
                        stop_reason = "shutdown_requested"
                        exit_code = GitHubLedgerHostExitCode.SHUTDOWN
                        break
                    if not self._has_claim_time(deadline):
                        stop_reason = "runtime_limit"
                        exit_code = GitHubLedgerHostExitCode.CLEAN_DRAIN
                        break
                    empty_polls += 1
                    if empty_polls >= self._settings.empty_poll_limit:
                        stop_reason = (
                            "empty_queue"
                            if counts["items_claimed"] == 0
                            else "queue_drained"
                        )
                        exit_code = (
                            GitHubLedgerHostExitCode.EMPTY_QUEUE
                            if counts["items_claimed"] == 0
                            else GitHubLedgerHostExitCode.CLEAN_DRAIN
                        )
                        break
                    wait_seconds = min(
                        self._settings.idle_interval.total_seconds(),
                        max(
                            0.0,
                            deadline
                            - self._monotonic()
                            - self._settings.minimum_claim_time.total_seconds(),
                        ),
                    )
                    if wait_seconds <= 0.0:
                        stop_reason = "runtime_limit"
                        exit_code = GitHubLedgerHostExitCode.CLEAN_DRAIN
                        break
                    if self._wait(wait_seconds):
                        stop_reason = "shutdown_requested"
                        exit_code = GitHubLedgerHostExitCode.SHUTDOWN
                        break
                    continue

                empty_polls = 0
                counts["items_claimed"] += 1
                self._record(result, counts)
                self._logger.info(
                    "event=github_ledger_item_finished run_id=%s work_item_id=%s "
                    "attempt=%s outcome=%s duration_seconds=%.3f",
                    run_id,
                    result.work_item_id,
                    result.attempt_number,
                    result.outcome,
                    max(0.0, self._monotonic() - item_started),
                )
                if result.outcome == "retry_scheduled":
                    if result.reason_code == "shutdown_grace_expired":
                        stop_reason = "shutdown_grace_expired"
                        exit_code = GitHubLedgerHostExitCode.SHUTDOWN
                    else:
                        stop_reason = "retry_scheduled"
                        exit_code = GitHubLedgerHostExitCode.RETRY_SCHEDULED
                    break
                if result.outcome == "lost_lease":
                    stop_reason = "lease_or_fence_lost"
                    exit_code = GitHubLedgerHostExitCode.LEASE_OR_FENCE_LOST
                    break
                if result.outcome == "failed":
                    stop_reason = "item_failed"
                    exit_code = GitHubLedgerHostExitCode.ITEM_FAILED
                    break
        except Exception as error:
            self._logger.error(
                "event=github_ledger_host_failed run_id=%s error_type=%s",
                run_id,
                type(error).__name__,
            )
            stop_reason = "fatal_error"
            exit_code = GitHubLedgerHostExitCode.FATAL
        return self._finish(
            run_id,
            started,
            counts,
            stop_reason == "empty_queue",
            stop_reason,
            exit_code,
        )

    def _has_claim_time(self, deadline: float) -> bool:
        return (
            deadline - self._monotonic()
            > self._settings.minimum_claim_time.total_seconds()
        )

    @staticmethod
    def _record(result: GitHubFileWorkExecution, counts: dict[str, int]) -> None:
        if (
            result.work_item_id is None
            or isinstance(result.attempt_number, bool)
            or not isinstance(result.attempt_number, int)
            or result.attempt_number < 1
        ):
            raise RuntimeError("GitHub ledger worker returned an invalid identity")
        outcome_field = {
            "completed": "succeeded",
            "retry_scheduled": "retry_scheduled",
            "quarantined": "quarantined",
            "cancelled": "cancelled",
            "failed": "failed",
            "lost_lease": "lease_or_fence_lost",
        }.get(result.outcome)
        if outcome_field is None:
            raise RuntimeError("GitHub ledger worker returned an invalid outcome")
        if result.outcome != "completed" and result.counters != FileWorkCounters():
            raise RuntimeError("GitHub ledger worker returned contradictory counters")
        counts[outcome_field] += 1
        counts["downloaded_bytes"] += result.counters.downloaded_bytes
        counts["extracted_characters"] += result.counters.extracted_characters
        counts["chunks"] += result.counters.chunk_count
        counts["embedding_batches"] += result.counters.embedding_batch_count

    def _finish(
        self,
        run_id: str,
        started: float,
        counts: dict[str, int],
        empty_queue: bool,
        stop_reason: str,
        exit_code: GitHubLedgerHostExitCode,
    ) -> int:
        summary = GitHubLedgerExecutionSummary(
            run_id=run_id,
            items_examined=counts["items_examined"],
            items_claimed=counts["items_claimed"],
            succeeded=counts["succeeded"],
            retry_scheduled=counts["retry_scheduled"],
            quarantined=counts["quarantined"],
            cancelled=counts["cancelled"],
            failed=counts["failed"],
            lease_or_fence_lost=counts["lease_or_fence_lost"],
            empty_queue=empty_queue,
            downloaded_bytes=counts["downloaded_bytes"],
            extracted_characters=counts["extracted_characters"],
            chunks=counts["chunks"],
            embedding_batches=counts["embedding_batches"],
            duration_seconds=max(0.0, self._monotonic() - started),
            stop_reason=stop_reason,
            run_status=_run_status(stop_reason),
            graceful_shutdown=self._shutdown.is_set(),
            partial_drain=(
                stop_reason
                in {
                    "item_limit",
                    "runtime_limit",
                    "retry_scheduled",
                    "shutdown_requested",
                    "shutdown_grace_expired",
                    "lease_or_fence_lost",
                    "item_failed",
                }
                or (stop_reason == "fatal_error" and counts["items_claimed"] > 0)
            ),
            exit_code=int(exit_code),
        )
        self.last_summary = summary
        self._logger.info(
            "event=github_ledger_execution_summary run_id=%s items_examined=%d "
            "items_claimed=%d succeeded=%d retry_scheduled=%d quarantined=%d "
            "cancelled=%d failed=%d lease_or_fence_lost=%d empty_queue=%s "
            "downloaded_bytes=%d extracted_characters=%d chunks=%d "
            "embedding_batches=%d duration_seconds=%.3f stop_reason=%s "
            "run_status=%s graceful_shutdown=%s partial_drain=%s exit_code=%d",
            summary.run_id,
            summary.items_examined,
            summary.items_claimed,
            summary.succeeded,
            summary.retry_scheduled,
            summary.quarantined,
            summary.cancelled,
            summary.failed,
            summary.lease_or_fence_lost,
            str(summary.empty_queue).lower(),
            summary.downloaded_bytes,
            summary.extracted_characters,
            summary.chunks,
            summary.embedding_batches,
            summary.duration_seconds,
            summary.stop_reason,
            summary.run_status,
            str(summary.graceful_shutdown).lower(),
            str(summary.partial_drain).lower(),
            summary.exit_code,
        )
        return summary.exit_code


def compose_github_sync_ledger_worker_host(
    settings: GitHubLedgerWorkerSettings,
    *,
    process_settings: WorkerProcessSettings,
    session_factory=SessionLocal,
    shutdown_event: GitHubLedgerShutdownState | threading.Event | None = None,
    random_uniform: Callable[[float, float], float] = random.SystemRandom().uniform,
) -> GitHubSyncLedgerWorkerHost:
    if (
        not settings.processing_enabled
        or process_settings.github_sync_ledger_processing_enabled is not True
    ):
        raise ValueError("GitHub ledger processing is disabled")
    secret_store = GoogleSecretManagerSecretStore(process_settings.secret_manager)
    github_client = GitHubAppRestClient(process_settings.github, secret_store)
    preparation = GitHubSynchronizationPreparationService(
        GitHubRepositoryContentService(None, github_client),
        create_default_content_extractor_registry(),
        DeterministicTextChunker(),
        OpenAIEmbeddingProvider(
            OpenAI(
                timeout=OPENAI_PROVIDER_CALL_TIMEOUT_SECONDS,
                max_retries=0,
            )
        ),
    )
    retry = ConnectorSyncRetryPolicy(random_uniform=random_uniform)

    def service(session: Session) -> GitHubSyncWorkProcessingService:
        return GitHubSyncWorkProcessingService(
            ConnectorSyncWorkLedgerRepository(session),
            GitHubRepositoryContentService(session, github_client),
            preparation.profile,
        )

    worker = GitHubSyncWorkItemWorker(
        session_factory,
        service,
        preparation,
        retry,
        worker_id=settings.worker_id,
        lease_duration=settings.lease_duration,
        heartbeat_interval=settings.heartbeat_interval,
        heartbeat_shutdown_timeout=settings.shutdown_timeout,
        recovery_limit=settings.recovery_limit,
        progress_check=getattr(
            shutdown_event, "raise_if_grace_expired", lambda: None
        ),
    )
    return GitHubSyncLedgerWorkerHost(
        worker, settings, shutdown_event=shutdown_event
    )


def install_shutdown_signal_handlers(event) -> None:
    def stop(_signum: int, _frame: FrameType | None) -> None:
        event.set()

    signal.signal(signal.SIGINT, stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, stop)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        process_settings = validate_worker_process_environment()
        settings = GitHubLedgerWorkerSettings.from_environment(
            argv,
            processing_enabled=(
                process_settings.github_sync_ledger_processing_enabled
            ),
        )
        shutdown = GitHubLedgerShutdownState(settings.shutdown_timeout)
        install_shutdown_signal_handlers(shutdown)
        if not settings.processing_enabled:
            return GitHubSyncLedgerWorkerHost(
                None,
                settings,
                shutdown_event=shutdown,
            ).run()
        return compose_github_sync_ledger_worker_host(
            settings,
            process_settings=process_settings,
            shutdown_event=shutdown,
        ).run()
    except (Exception, SystemExit) as error:
        LOGGER.error(
            "event=github_ledger_worker_startup_failed error_type=%s",
            type(error).__name__,
        )
        LOGGER.info(
            "event=github_ledger_execution_summary run_id=%s items_examined=0 "
            "items_claimed=0 succeeded=0 retry_scheduled=0 quarantined=0 "
            "cancelled=0 failed=1 lease_or_fence_lost=0 empty_queue=false "
            "downloaded_bytes=0 extracted_characters=0 chunks=0 "
            "embedding_batches=0 duration_seconds=0.000 "
            "stop_reason=startup_failure run_status=failed "
            "graceful_shutdown=false partial_drain=false exit_code=1",
            uuid4(),
        )
        return int(GitHubLedgerHostExitCode.FATAL)


def _strict_positive_integer(value: object) -> int:
    if not isinstance(value, str) or re.fullmatch(r"[1-9][0-9]*", value) is None:
        raise ValueError("GitHub ledger worker configuration is invalid")
    return int(value)


def _seconds(value: object) -> timedelta:
    return timedelta(seconds=_strict_positive_integer(value))


def _bounded_integer(name: str, value: object, maximum: int) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= maximum
    ):
        raise ValueError(f"{name} is outside the allowed range")


def _bounded_duration(name: str, value: object, maximum: int) -> None:
    if (
        not isinstance(value, timedelta)
        or not 0 < value.total_seconds() <= maximum
    ):
        raise ValueError(f"{name} is outside the allowed range")


def _run_status(stop_reason: str) -> str:
    if stop_reason == "disabled":
        return "disabled"
    if stop_reason in {"empty_queue", "queue_drained"}:
        return "succeeded"
    if stop_reason in {"item_limit", "runtime_limit"}:
        return "partial"
    if stop_reason == "retry_scheduled":
        return "retry_scheduled"
    if stop_reason == "shutdown_requested":
        return "stopped"
    if stop_reason == "shutdown_grace_expired":
        return "retry_scheduled"
    if stop_reason in {"lease_or_fence_lost", "item_failed", "fatal_error"}:
        return "failed"
    raise ValueError("GitHub ledger host stop reason is invalid")


if __name__ == "__main__":
    raise SystemExit(main())
