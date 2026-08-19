"""Continuous and one-shot host for recurring connector synchronization schedules."""

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
from uuid import uuid4

from sqlalchemy.orm import Session

from application.services.connector_sync_schedule_service import (
    ConnectorSyncScheduleService,
    DueScheduleResult,
)
from infrastructure.db.session import SessionLocal

LOGGER = logging.getLogger(__name__)
_HOST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$")

DEFAULT_POLL_SECONDS = 5.0
DEFAULT_MAX_FAILURES = 5
DEFAULT_BACKOFF_MIN_SECONDS = 1.0
DEFAULT_BACKOFF_MAX_SECONDS = 60.0
DEFAULT_BACKOFF_JITTER = 0.2
DEFAULT_SHUTDOWN_SECONDS = 30.0
MAX_POLL_SECONDS = 300.0
MAX_FAILURES = 100
MAX_BACKOFF_SECONDS = 900.0
MAX_SHUTDOWN_SECONDS = 300.0


class InvalidSchedulerHostConfiguration(ValueError):
    """Raised when scheduler host settings are malformed or unsafe."""


class SchedulerHostExitCode(IntEnum):
    SUCCESS = 0
    HOST_FAILURE = 1
    NO_WORK = 2
    SHUTDOWN = 130


@dataclass(frozen=True, repr=False)
class ConnectorSyncSchedulerHostSettings:
    scheduler_id: str
    poll_interval: timedelta
    maximum_consecutive_failures: int
    minimum_backoff: timedelta
    maximum_backoff: timedelta
    backoff_jitter: float
    graceful_shutdown_timeout: timedelta
    one_shot: bool

    def __post_init__(self) -> None:
        _scheduler_id(self.scheduler_id)
        for name, value, maximum in (
            ("poll_interval", self.poll_interval, MAX_POLL_SECONDS),
            ("minimum_backoff", self.minimum_backoff, MAX_BACKOFF_SECONDS),
            ("maximum_backoff", self.maximum_backoff, MAX_BACKOFF_SECONDS),
            ("graceful_shutdown_timeout", self.graceful_shutdown_timeout, MAX_SHUTDOWN_SECONDS),
        ):
            if not isinstance(value, timedelta) or not 0 < value.total_seconds() <= maximum:
                raise InvalidSchedulerHostConfiguration(f"{name} is outside the allowed range")
        if self.maximum_backoff < self.minimum_backoff:
            raise InvalidSchedulerHostConfiguration("maximum_backoff must not be less than minimum_backoff")
        if (
            isinstance(self.maximum_consecutive_failures, bool)
            or not isinstance(self.maximum_consecutive_failures, int)
            or not 1 <= self.maximum_consecutive_failures <= MAX_FAILURES
        ):
            raise InvalidSchedulerHostConfiguration("maximum_consecutive_failures is invalid")
        if (
            isinstance(self.backoff_jitter, bool)
            or not isinstance(self.backoff_jitter, (int, float))
            or not math.isfinite(float(self.backoff_jitter))
            or not 0 <= float(self.backoff_jitter) <= 1
        ):
            raise InvalidSchedulerHostConfiguration("backoff_jitter is invalid")
        if not isinstance(self.one_shot, bool):
            raise InvalidSchedulerHostConfiguration("one_shot must be boolean")

    @classmethod
    def from_environment(
        cls,
        argv: Sequence[str] | None = None,
        *,
        environ: Mapping[str, str] | None = None,
        scheduler_id_factory: Callable[[], str] = lambda: f"sync-scheduler-{uuid4().hex}",
    ) -> ConnectorSyncSchedulerHostSettings:
        parser = argparse.ArgumentParser(description="Run recurring connector synchronization scheduling")
        parser.add_argument("--once", action="store_true", help="process at most one due schedule")
        arguments = parser.parse_args(argv)
        values = os.environ if environ is None else environ
        return cls(
            scheduler_id=_scheduler_id(
                values.get("CONNECTOR_SYNC_SCHEDULER_ID") or scheduler_id_factory()
            ),
            poll_interval=_duration(
                values, "CONNECTOR_SYNC_SCHEDULER_POLL_SECONDS", DEFAULT_POLL_SECONDS, MAX_POLL_SECONDS
            ),
            maximum_consecutive_failures=_integer(
                values, "CONNECTOR_SYNC_SCHEDULER_MAX_FAILURES", DEFAULT_MAX_FAILURES, MAX_FAILURES
            ),
            minimum_backoff=_duration(
                values, "CONNECTOR_SYNC_SCHEDULER_BACKOFF_MIN_SECONDS",
                DEFAULT_BACKOFF_MIN_SECONDS, MAX_BACKOFF_SECONDS,
            ),
            maximum_backoff=_duration(
                values, "CONNECTOR_SYNC_SCHEDULER_BACKOFF_MAX_SECONDS",
                DEFAULT_BACKOFF_MAX_SECONDS, MAX_BACKOFF_SECONDS,
            ),
            backoff_jitter=_float(
                values, "CONNECTOR_SYNC_SCHEDULER_BACKOFF_JITTER", DEFAULT_BACKOFF_JITTER, 0.0, 1.0
            ),
            graceful_shutdown_timeout=_duration(
                values, "CONNECTOR_SYNC_SCHEDULER_SHUTDOWN_SECONDS",
                DEFAULT_SHUTDOWN_SECONDS, MAX_SHUTDOWN_SECONDS,
            ),
            one_shot=arguments.once,
        )


SessionFactory = Callable[[], Session]
ServiceFactory = Callable[[Session], ConnectorSyncScheduleService]


class ConnectorSyncSchedulerHost:
    def __init__(
        self,
        session_factory: SessionFactory,
        service_factory: ServiceFactory,
        settings: ConnectorSyncSchedulerHostSettings,
        *,
        shutdown_event: threading.Event | None = None,
        wait: Callable[[float], bool] | None = None,
        random_uniform: Callable[[float, float], float] = random.uniform,
        monotonic: Callable[[], float] = time.monotonic,
        logger: logging.Logger = LOGGER,
    ) -> None:
        self._session_factory = session_factory
        self._service_factory = service_factory
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
            try:
                result = self.run_cycle()
            except Exception as error:
                self._log_failure(1, error)
                return int(SchedulerHostExitCode.HOST_FAILURE)
            if result.outcome == "no_work":
                return int(SchedulerHostExitCode.NO_WORK)
            if result.outcome == "shutdown":
                return int(SchedulerHostExitCode.SHUTDOWN)
            return int(SchedulerHostExitCode.SUCCESS)
        failures = 0
        self._logger.info("event=scheduler_started scheduler_id=%s", self._settings.scheduler_id)
        while not self._shutdown_event.is_set():
            started = self._monotonic()
            try:
                result = self.run_cycle()
            except Exception as error:
                failures += 1
                self._log_failure(failures, error)
                if failures >= self._settings.maximum_consecutive_failures:
                    return int(SchedulerHostExitCode.HOST_FAILURE)
                if self._wait(self._backoff_seconds(failures)):
                    return int(SchedulerHostExitCode.SUCCESS)
                continue
            failures = 0
            if (
                self._shutdown_event.is_set()
                and self._monotonic() - started
                > self._settings.graceful_shutdown_timeout.total_seconds()
            ):
                return int(SchedulerHostExitCode.HOST_FAILURE)
            if result.outcome == "no_work":
                if self._wait(self._settings.poll_interval.total_seconds()):
                    return int(SchedulerHostExitCode.SUCCESS)
            elif result.outcome == "shutdown":
                return int(SchedulerHostExitCode.SUCCESS)
        return int(SchedulerHostExitCode.SUCCESS)

    def run_cycle(self) -> DueScheduleResult:
        if self._shutdown_event.is_set():
            return DueScheduleResult("shutdown", None, None, None, False, None)
        session = self._session_factory()
        try:
            result = self._service_factory(session).process_one_due()
            session.commit()
            if result.schedule_id is not None:
                self._logger.info(
                    "event=schedule_processed scheduler_id=%s schedule_id=%s state=%s",
                    self._settings.scheduler_id,
                    result.schedule_id,
                    result.outcome,
                )
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _backoff_seconds(self, failures: int) -> float:
        minimum = self._settings.minimum_backoff.total_seconds()
        maximum = min(
            minimum * (2 ** (failures - 1)), self._settings.maximum_backoff.total_seconds()
        )
        jitter = maximum * self._settings.backoff_jitter
        value = self._random_uniform(max(0.0, maximum - jitter), maximum)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or not 0 <= float(value) <= maximum
        ):
            raise InvalidSchedulerHostConfiguration("random source returned invalid data")
        return float(value)

    def _log_failure(self, failures: int, error: BaseException) -> None:
        self._logger.error(
            "event=scheduler_failure scheduler_id=%s failure_count=%d error_type=%s",
            self._settings.scheduler_id,
            failures,
            type(error).__name__,
        )


def compose_connector_sync_scheduler_host(
    settings: ConnectorSyncSchedulerHostSettings,
    *,
    session_factory: SessionFactory = SessionLocal,
    shutdown_event: threading.Event | None = None,
    wait: Callable[[float], bool] | None = None,
    random_uniform: Callable[[float, float], float] = random.uniform,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    monotonic: Callable[[], float] = time.monotonic,
    logger: logging.Logger = LOGGER,
) -> ConnectorSyncSchedulerHost:
    return ConnectorSyncSchedulerHost(
        session_factory,
        lambda session: ConnectorSyncScheduleService(session, clock=clock),
        settings,
        shutdown_event=shutdown_event,
        wait=wait,
        random_uniform=random_uniform,
        monotonic=monotonic,
        logger=logger,
    )


def install_shutdown_signal_handlers(shutdown_event: threading.Event) -> None:
    def request_shutdown(_signum: int, _frame: FrameType | None) -> None:
        shutdown_event.set()

    signal.signal(signal.SIGINT, request_shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_shutdown)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        settings = ConnectorSyncSchedulerHostSettings.from_environment(argv)
        shutdown_event = threading.Event()
        install_shutdown_signal_handlers(shutdown_event)
        return compose_connector_sync_scheduler_host(
            settings, shutdown_event=shutdown_event
        ).run()
    except Exception as error:
        LOGGER.error("event=scheduler_startup_failed error_type=%s", type(error).__name__)
        return int(SchedulerHostExitCode.HOST_FAILURE)


def _scheduler_id(value: object) -> str:
    if not isinstance(value, str) or not _HOST_ID.fullmatch(value):
        raise InvalidSchedulerHostConfiguration("scheduler ID is invalid")
    return value


def _duration(values: Mapping[str, str], name: str, default: float, maximum: float) -> timedelta:
    return timedelta(seconds=_float(values, name, default, 0.0, maximum, lower_exclusive=True))


def _float(
    values: Mapping[str, str], name: str, default: float, minimum: float, maximum: float,
    *, lower_exclusive: bool = False,
) -> float:
    try:
        value = default if name not in values else float(values[name])
    except (TypeError, ValueError) as exc:
        raise InvalidSchedulerHostConfiguration(f"{name} is invalid") from exc
    if not math.isfinite(value) or value > maximum or value < minimum or (lower_exclusive and value == minimum):
        raise InvalidSchedulerHostConfiguration(f"{name} is outside the allowed range")
    return value


def _integer(values: Mapping[str, str], name: str, default: int, maximum: int) -> int:
    try:
        value = default if name not in values else int(values[name])
    except (TypeError, ValueError) as exc:
        raise InvalidSchedulerHostConfiguration(f"{name} is invalid") from exc
    if value < 1 or value > maximum:
        raise InvalidSchedulerHostConfiguration(f"{name} is outside the allowed range")
    return value


if __name__ == "__main__":
    raise SystemExit(main())