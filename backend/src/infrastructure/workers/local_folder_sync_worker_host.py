"""Continuous and one-shot process host for staged Local Folder synchronization."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import IntEnum
import logging
import math
import os
import random
import re
import signal
import threading
import time
from types import FrameType
from uuid import UUID, uuid4

from sqlalchemy.orm import Session

from application.services.connector_sync_execution_service import ConnectorSyncExecutionService
from application.services.connector_sync_retry_policy import ConnectorSyncRetryPolicy
from application.services.staged_local_folder_synchronization_service import (
    LocalFolderPreparationService,
    StagedLocalFolderSynchronizationService,
)
from domain.embeddings.provider import EmbeddingProvider
from infrastructure.content_chunking.text_chunker import DeterministicTextChunker
from infrastructure.content_extraction.registry import create_default_content_extractor_registry
from infrastructure.db.session import SessionLocal
from infrastructure.embeddings.openai import OpenAIEmbeddingProvider
from infrastructure.repositories.connector_sync_job_repository import ConnectorSyncJobRepository
from infrastructure.workers.local_folder_sync_worker import (
    LocalFolderAttemptContext,
    LocalFolderSyncWorker,
    LocalFolderWorkerResult,
)

LOGGER = logging.getLogger(__name__)
CONNECTOR_TYPE = "local_folder"
_WORKER_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$")

DEFAULT_IDLE_SECONDS = 5.0
DEFAULT_LEASE_SECONDS = 15 * 60.0
DEFAULT_HEARTBEAT_SECONDS = 60.0
DEFAULT_MAX_HOST_FAILURES = 5
DEFAULT_BACKOFF_MIN_SECONDS = 1.0
DEFAULT_BACKOFF_MAX_SECONDS = 60.0
DEFAULT_BACKOFF_JITTER = 0.2
DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 5 * 60.0
DEFAULT_RECOVERY_LIMIT = 10

MAX_IDLE_SECONDS = 5 * 60.0
MAX_LEASE_SECONDS = 60 * 60.0
MAX_BACKOFF_SECONDS = 15 * 60.0
MAX_SHUTDOWN_SECONDS = 60 * 60.0
MAX_HOST_FAILURES = 100
MAX_RECOVERY_LIMIT = 100


class InvalidLocalFolderHostConfiguration(ValueError):
    """Raised when Local Folder host settings are malformed or unsafe."""


class LocalFolderHostExitCode(IntEnum):
    SUCCESS = 0
    HOST_FAILURE = 1
    NO_WORK = 2
    RETRY_SCHEDULED = 3
    TERMINAL_FAILURE = 4
    CANCELLED = 5
    LOST_LEASE = 6
    SHUTDOWN = 130


@dataclass(frozen=True, repr=False)
class LocalFolderWorkerHostSettings:
    worker_id: str
    idle_interval: timedelta
    lease_duration: timedelta
    heartbeat_interval: timedelta
    maximum_consecutive_failures: int
    minimum_backoff: timedelta
    maximum_backoff: timedelta
    backoff_jitter: float
    graceful_shutdown_timeout: timedelta
    one_shot: bool
    expired_recovery_limit: int

    def __post_init__(self) -> None:
        _worker_identifier(self.worker_id)
        for name, value, maximum in (
            ("idle_interval", self.idle_interval, MAX_IDLE_SECONDS),
            ("lease_duration", self.lease_duration, MAX_LEASE_SECONDS),
            ("heartbeat_interval", self.heartbeat_interval, MAX_LEASE_SECONDS),
            ("minimum_backoff", self.minimum_backoff, MAX_BACKOFF_SECONDS),
            ("maximum_backoff", self.maximum_backoff, MAX_BACKOFF_SECONDS),
            ("graceful_shutdown_timeout", self.graceful_shutdown_timeout, MAX_SHUTDOWN_SECONDS),
        ):
            if not isinstance(value, timedelta):
                raise InvalidLocalFolderHostConfiguration(f"{name} is invalid")
            seconds = value.total_seconds()
            if seconds <= 0.0 or seconds > maximum:
                raise InvalidLocalFolderHostConfiguration(f"{name} is outside the allowed range")
        if self.heartbeat_interval >= self.lease_duration:
            raise InvalidLocalFolderHostConfiguration(
                "heartbeat_interval must be less than lease_duration"
            )
        if self.maximum_backoff < self.minimum_backoff:
            raise InvalidLocalFolderHostConfiguration(
                "maximum_backoff must not be less than minimum_backoff"
            )
        if (
            isinstance(self.maximum_consecutive_failures, bool)
            or not isinstance(self.maximum_consecutive_failures, int)
            or not 1 <= self.maximum_consecutive_failures <= MAX_HOST_FAILURES
        ):
            raise InvalidLocalFolderHostConfiguration(
                "maximum_consecutive_failures is outside the allowed range"
            )
        if (
            isinstance(self.expired_recovery_limit, bool)
            or not isinstance(self.expired_recovery_limit, int)
            or not 1 <= self.expired_recovery_limit <= MAX_RECOVERY_LIMIT
        ):
            raise InvalidLocalFolderHostConfiguration(
                "expired_recovery_limit is outside the allowed range"
            )
        if (
            isinstance(self.backoff_jitter, bool)
            or not isinstance(self.backoff_jitter, (int, float))
            or not math.isfinite(float(self.backoff_jitter))
            or not 0.0 <= float(self.backoff_jitter) <= 1.0
        ):
            raise InvalidLocalFolderHostConfiguration("backoff_jitter is outside the allowed range")
        if not isinstance(self.one_shot, bool):
            raise InvalidLocalFolderHostConfiguration("one_shot must be boolean")

    @classmethod
    def from_environment(
        cls,
        argv: Sequence[str] | None = None,
        *,
        environ: Mapping[str, str] | None = None,
        worker_id_factory: Callable[[], str] = lambda: f"local-folder-{uuid4().hex}",
    ) -> LocalFolderWorkerHostSettings:
        parser = argparse.ArgumentParser(description="Run the Local Folder synchronization worker")
        parser.add_argument(
            "--once",
            action="store_true",
            help="perform one claim attempt, execute at most one job, and exit",
        )
        arguments = parser.parse_args(argv)
        values = os.environ if environ is None else environ
        worker_id = values.get("LOCAL_FOLDER_WORKER_ID") or worker_id_factory()
        settings = cls(
            worker_id=_worker_identifier(worker_id),
            idle_interval=_duration(
                values, "LOCAL_FOLDER_WORKER_IDLE_SECONDS", DEFAULT_IDLE_SECONDS, MAX_IDLE_SECONDS
            ),
            lease_duration=_duration(
                values, "LOCAL_FOLDER_WORKER_LEASE_SECONDS", DEFAULT_LEASE_SECONDS, MAX_LEASE_SECONDS
            ),
            heartbeat_interval=_duration(
                values,
                "LOCAL_FOLDER_WORKER_HEARTBEAT_SECONDS",
                DEFAULT_HEARTBEAT_SECONDS,
                MAX_LEASE_SECONDS,
            ),
            maximum_consecutive_failures=_integer(
                values,
                "LOCAL_FOLDER_WORKER_MAX_FAILURES",
                DEFAULT_MAX_HOST_FAILURES,
                MAX_HOST_FAILURES,
            ),
            minimum_backoff=_duration(
                values,
                "LOCAL_FOLDER_WORKER_BACKOFF_MIN_SECONDS",
                DEFAULT_BACKOFF_MIN_SECONDS,
                MAX_BACKOFF_SECONDS,
            ),
            maximum_backoff=_duration(
                values,
                "LOCAL_FOLDER_WORKER_BACKOFF_MAX_SECONDS",
                DEFAULT_BACKOFF_MAX_SECONDS,
                MAX_BACKOFF_SECONDS,
            ),
            backoff_jitter=_nonnegative_float(
                values,
                "LOCAL_FOLDER_WORKER_BACKOFF_JITTER",
                DEFAULT_BACKOFF_JITTER,
                maximum=1.0,
            ),
            graceful_shutdown_timeout=_duration(
                values,
                "LOCAL_FOLDER_WORKER_SHUTDOWN_TIMEOUT_SECONDS",
                DEFAULT_SHUTDOWN_TIMEOUT_SECONDS,
                MAX_SHUTDOWN_SECONDS,
            ),
            one_shot=arguments.once,
            expired_recovery_limit=_integer(
                values,
                "LOCAL_FOLDER_WORKER_RECOVERY_LIMIT",
                DEFAULT_RECOVERY_LIMIT,
                MAX_RECOVERY_LIMIT,
            ),
        )
        return settings


@dataclass(frozen=True)
class LocalFolderHostCycleResult:
    outcome: str
    job_id: UUID | None = None
    attempt_number: int | None = None
    shutdown_timeout_exceeded: bool = False


SessionFactory = Callable[[], Session]
ExecutionServiceFactory = Callable[[Session], ConnectorSyncExecutionService]


class LocalFolderSyncWorkerHost:
    """Poll, recover, claim and execute Local Folder work with bounded host failures."""

    def __init__(
        self,
        session_factory: SessionFactory,
        execution_service_factory: ExecutionServiceFactory,
        worker: LocalFolderSyncWorker,
        settings: LocalFolderWorkerHostSettings,
        *,
        shutdown_event: threading.Event | None = None,
        wait: Callable[[float], bool] | None = None,
        random_uniform: Callable[[float, float], float] = random.uniform,
        monotonic: Callable[[], float] = time.monotonic,
        logger: logging.Logger = LOGGER,
    ) -> None:
        self._session_factory = session_factory
        self._execution_service_factory = execution_service_factory
        self._worker = worker
        self._settings = settings
        self._shutdown_event = shutdown_event or threading.Event()
        self._wait = wait or self._shutdown_event.wait
        self._random_uniform = random_uniform
        self._monotonic = monotonic
        self._logger = logger

    @property
    def shutdown_event(self) -> threading.Event:
        return self._shutdown_event

    def run(self) -> int:
        if self._settings.one_shot:
            return int(self._one_shot_exit(self._run_one_shot()))
        failures = 0
        self._logger.info("event=worker_host_started worker_id=%s", self._settings.worker_id)
        while not self._shutdown_event.is_set():
            try:
                result = self.run_cycle()
            except Exception as error:
                failures += 1
                self._logger.error(
                    "event=worker_host_failure worker_id=%s failure_count=%d error_type=%s",
                    self._settings.worker_id,
                    failures,
                    type(error).__name__,
                )
                if failures >= self._settings.maximum_consecutive_failures:
                    return int(LocalFolderHostExitCode.HOST_FAILURE)
                if self._wait(self._backoff_seconds(failures)):
                    return int(LocalFolderHostExitCode.SUCCESS)
                continue
            failures = 0
            if result.outcome == "no_work":
                if self._wait(self._settings.idle_interval.total_seconds()):
                    return int(LocalFolderHostExitCode.SUCCESS)
            elif result.outcome == "shutdown":
                return int(
                    LocalFolderHostExitCode.HOST_FAILURE
                    if result.shutdown_timeout_exceeded
                    else LocalFolderHostExitCode.SUCCESS
                )
        return int(LocalFolderHostExitCode.SUCCESS)

    def run_cycle(self) -> LocalFolderHostCycleResult:
        if self._shutdown_event.is_set():
            return LocalFolderHostCycleResult("shutdown")
        context = self._recover_and_claim()
        if context is None:
            return LocalFolderHostCycleResult(
                "shutdown" if self._shutdown_event.is_set() else "no_work"
            )
        self._logger.info(
            "event=job_claimed worker_id=%s job_id=%s attempt=%d",
            self._settings.worker_id,
            context.job_id,
            context.attempt_number,
        )
        return self._execute_claimed(context)

    def _run_one_shot(self) -> LocalFolderHostCycleResult:
        try:
            return self.run_cycle()
        except Exception as error:
            self._logger.error(
                "event=worker_host_failure worker_id=%s failure_count=1 error_type=%s",
                self._settings.worker_id,
                type(error).__name__,
            )
            return LocalFolderHostCycleResult("host_failure")

    def _recover_and_claim(self) -> LocalFolderAttemptContext | None:
        session = self._session_factory()
        try:
            execution = self._execution_service_factory(session)
            recovered = execution.recover_expired_local_folder(
                limit=self._settings.expired_recovery_limit
            )
            for item in recovered:
                self._logger.info(
                    "event=expired_job_recovered worker_id=%s job_id=%s state=%s",
                    self._settings.worker_id,
                    item.job_id,
                    item.status,
                )
            acquired = None
            if not self._shutdown_event.is_set():
                acquired = execution.acquire_one_local_folder(
                    worker_id=self._settings.worker_id,
                    lease_duration=self._settings.lease_duration,
                )
            context = self._worker.attempt_context(acquired) if acquired is not None else None
            session.commit()
            return context
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _execute_claimed(
        self, context: LocalFolderAttemptContext
    ) -> LocalFolderHostCycleResult:
        if self._shutdown_event.is_set():
            return LocalFolderHostCycleResult(
                "shutdown",
                context.job_id,
                context.attempt_number,
            )
        while True:
            started = self._monotonic()
            result = self._worker.execute(context)
            self._log_job_outcome(result)
            elapsed = self._monotonic() - started
            if (
                self._shutdown_event.is_set()
                and elapsed > self._settings.graceful_shutdown_timeout.total_seconds()
            ):
                return LocalFolderHostCycleResult(
                    "shutdown",
                    context.job_id,
                    context.attempt_number,
                    True,
                )
            if result.outcome != "in_progress":
                return LocalFolderHostCycleResult(
                    result.outcome,
                    result.job_id,
                    result.attempt_number,
                )
            if self._shutdown_event.is_set():
                self._logger.info(
                    "event=shutdown_boundary worker_id=%s job_id=%s attempt=%d",
                    self._settings.worker_id,
                    context.job_id,
                    context.attempt_number,
                )
                return LocalFolderHostCycleResult(
                    "shutdown",
                    context.job_id,
                    context.attempt_number,
                )

    def _backoff_seconds(self, failures: int) -> float:
        minimum = self._settings.minimum_backoff.total_seconds()
        maximum = min(
            minimum * (2 ** (failures - 1)),
            self._settings.maximum_backoff.total_seconds(),
        )
        jitter = maximum * self._settings.backoff_jitter
        value = self._random_uniform(max(0.0, maximum - jitter), maximum)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise InvalidLocalFolderHostConfiguration("host random source returned invalid data")
        result = float(value)
        if not math.isfinite(result) or result < 0.0 or result > maximum:
            raise InvalidLocalFolderHostConfiguration("host random source returned invalid data")
        return result

    def _log_job_outcome(self, result: LocalFolderWorkerResult) -> None:
        if result.outcome == "in_progress":
            return
        self._logger.info(
            "event=job_outcome worker_id=%s job_id=%s attempt=%s state=%s",
            self._settings.worker_id,
            result.job_id,
            result.attempt_number,
            result.outcome,
        )

    @staticmethod
    def _one_shot_exit(result: LocalFolderHostCycleResult) -> LocalFolderHostExitCode:
        return {
            "completed": LocalFolderHostExitCode.SUCCESS,
            "no_work": LocalFolderHostExitCode.NO_WORK,
            "retry_scheduled": LocalFolderHostExitCode.RETRY_SCHEDULED,
            "failed": LocalFolderHostExitCode.TERMINAL_FAILURE,
            "cancelled": LocalFolderHostExitCode.CANCELLED,
            "lost_lease": LocalFolderHostExitCode.LOST_LEASE,
            "shutdown": LocalFolderHostExitCode.SHUTDOWN,
            "host_failure": LocalFolderHostExitCode.HOST_FAILURE,
        }.get(result.outcome, LocalFolderHostExitCode.HOST_FAILURE)


def compose_local_folder_sync_worker_host(
    settings: LocalFolderWorkerHostSettings,
    *,
    session_factory: SessionFactory = SessionLocal,
    embedding_provider_factory: Callable[[], EmbeddingProvider] = OpenAIEmbeddingProvider,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    random_uniform: Callable[[float, float], float] = random.uniform,
    shutdown_event: threading.Event | None = None,
    wait: Callable[[float], bool] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    logger: logging.Logger = LOGGER,
) -> LocalFolderSyncWorkerHost:
    """Compose production dependencies without creating a global database session."""
    provider = embedding_provider_factory()
    preparation = LocalFolderPreparationService(
        create_default_content_extractor_registry(),
        DeterministicTextChunker(),
        provider,
    )
    retry_policy = ConnectorSyncRetryPolicy(random_uniform=random_uniform)

    def execution_factory(session: Session) -> ConnectorSyncExecutionService:
        return ConnectorSyncExecutionService(
            ConnectorSyncJobRepository(session),
            retry_policy,
            clock=clock,
        )

    worker = LocalFolderSyncWorker(
        session_factory,
        execution_factory,
        lambda session: StagedLocalFolderSynchronizationService(
            session,
            execution_factory(session),
            preparation.profile,
        ),
        preparation,
        worker_id=settings.worker_id,
        lease_duration=settings.lease_duration,
        heartbeat_target=settings.heartbeat_interval,
        steps_per_invocation=1,
        clock=clock,
    )
    return LocalFolderSyncWorkerHost(
        session_factory,
        execution_factory,
        worker,
        settings,
        shutdown_event=shutdown_event,
        wait=wait,
        random_uniform=random_uniform,
        monotonic=monotonic,
        logger=logger,
    )


def install_shutdown_signal_handlers(shutdown_event: threading.Event) -> None:
    """Install handlers that only request cooperative shutdown."""

    def request_shutdown(_signum: int, _frame: FrameType | None) -> None:
        shutdown_event.set()

    signal.signal(signal.SIGINT, request_shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_shutdown)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        settings = LocalFolderWorkerHostSettings.from_environment(argv)
        shutdown_event = threading.Event()
        install_shutdown_signal_handlers(shutdown_event)
        host = compose_local_folder_sync_worker_host(settings, shutdown_event=shutdown_event)
        return host.run()
    except Exception as error:
        LOGGER.error("event=worker_host_startup_failed error_type=%s", type(error).__name__)
        return int(LocalFolderHostExitCode.HOST_FAILURE)


def _worker_identifier(value: object) -> str:
    if not isinstance(value, str) or not _WORKER_ID.fullmatch(value):
        raise InvalidLocalFolderHostConfiguration("LOCAL_FOLDER_WORKER_ID is invalid")
    return value


def _duration(
    values: Mapping[str, str],
    name: str,
    default: float,
    maximum: float,
) -> timedelta:
    seconds = _positive_float(values, name, default, maximum=maximum)
    return timedelta(seconds=seconds)


def _positive_float(
    values: Mapping[str, str],
    name: str,
    default: float,
    *,
    maximum: float,
) -> float:
    value = _float(values, name, default)
    if value <= 0.0 or value > maximum:
        raise InvalidLocalFolderHostConfiguration(f"{name} is outside the allowed range")
    return value


def _nonnegative_float(
    values: Mapping[str, str],
    name: str,
    default: float,
    *,
    maximum: float,
) -> float:
    value = _float(values, name, default)
    if value < 0.0 or value > maximum:
        raise InvalidLocalFolderHostConfiguration(f"{name} is outside the allowed range")
    return value


def _float(values: Mapping[str, str], name: str, default: float) -> float:
    raw = values.get(name)
    try:
        value = default if raw is None else float(raw)
    except (TypeError, ValueError) as exc:
        raise InvalidLocalFolderHostConfiguration(f"{name} is invalid") from exc
    if not math.isfinite(value):
        raise InvalidLocalFolderHostConfiguration(f"{name} is invalid")
    return value


def _integer(
    values: Mapping[str, str],
    name: str,
    default: int,
    maximum: int,
) -> int:
    raw = values.get(name)
    try:
        value = default if raw is None else int(raw)
    except (TypeError, ValueError) as exc:
        raise InvalidLocalFolderHostConfiguration(f"{name} is invalid") from exc
    if value < 1 or value > maximum:
        raise InvalidLocalFolderHostConfiguration(f"{name} is outside the allowed range")
    return value


if __name__ == "__main__":
    raise SystemExit(main())