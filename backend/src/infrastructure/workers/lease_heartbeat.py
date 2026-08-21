"""Independent, execution-scoped synchronization lease heartbeat."""

from __future__ import annotations

import threading
from datetime import timedelta
from typing import Callable

from sqlalchemy.orm import Session

from application.services.connector_sync_execution_service import ConnectorSyncExecutionService
from infrastructure.repositories.connector_sync_job_repository import SyncJobLease


class LeaseHeartbeatFailure(RuntimeError):
    """Raised in the owner thread when independent renewal stopped safely."""


class LeaseHeartbeat:
    def __init__(
        self,
        session_factory: Callable[[], Session],
        execution_factory: Callable[[Session], ConnectorSyncExecutionService],
        lease: SyncJobLease,
        *,
        worker_id: str,
        lease_duration: timedelta,
        interval: timedelta,
        shutdown_timeout: timedelta,
    ) -> None:
        if interval >= lease_duration or interval.total_seconds() * 2 >= lease_duration.total_seconds():
            raise ValueError("heartbeat interval must leave one full renewal margin")
        self._session_factory = session_factory
        self._execution_factory = execution_factory
        self._lease = lease
        self._worker_id = worker_id
        self._lease_duration = lease_duration
        self._interval = interval.total_seconds()
        self._shutdown_timeout = shutdown_timeout.total_seconds()
        self._stop = threading.Event()
        self._failure: BaseException | None = None
        self._thread: threading.Thread | None = None

    def __enter__(self) -> LeaseHeartbeat:
        if self._thread is not None:
            raise RuntimeError("heartbeat is already running")
        self._thread = threading.Thread(
            target=self._run,
            name=f"sync-heartbeat-{self._lease.job_id}",
            daemon=False,
        )
        self._thread.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.stop()

    def raise_if_failed(self) -> None:
        if self._failure is not None:
            raise LeaseHeartbeatFailure("synchronization lease heartbeat stopped") from self._failure

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(self._shutdown_timeout)
            if thread.is_alive():
                raise LeaseHeartbeatFailure("synchronization lease heartbeat did not stop")
        self.raise_if_failed()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            session = self._session_factory()
            try:
                self._execution_factory(session).heartbeat(
                    self._lease,
                    worker_id=self._worker_id,
                    lease_duration=self._lease_duration,
                )
                session.commit()
            except BaseException as error:
                session.rollback()
                self._failure = error
                self._stop.set()
                return
            finally:
                session.close()
