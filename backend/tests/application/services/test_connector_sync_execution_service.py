from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.services.connector_sync_execution_service import ConnectorSyncExecutionService
from application.services.connector_sync_retry_policy import (
    ConnectorSyncRetryPolicy,
    RetryPolicyViolation,
    SyncFailureKind,
)
from infrastructure.db.models import ConnectorSyncRun
from infrastructure.repositories.connector_sync_job_repository import SyncJobLease

NOW = datetime(2026, 8, 24, tzinfo=timezone.utc)


def _lease(*, attempt: int = 1, maximum: int = 3) -> SyncJobLease:
    return SyncJobLease(
        uuid4(), uuid4(), uuid4(), uuid4(), "incremental", "manual",
        attempt, maximum, uuid4(), attempt, NOW + timedelta(minutes=5),
    )


def _service(repository: Mock) -> ConnectorSyncExecutionService:
    return ConnectorSyncExecutionService(
        repository,
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high / 2),
        clock=lambda: NOW,
    )


def test_acquire_handles_at_most_one_attempt_and_allocates_one_run():
    repository = Mock()
    lease = _lease()
    repository.acquire_next.return_value = lease
    repository.create_attempt_run.return_value = ConnectorSyncRun(id=uuid4())
    result = _service(repository).acquire_one(
        lease.organization_id,
        worker_id="worker-one",
        lease_duration=timedelta(minutes=5),
    )
    assert result is not None and result.lease == lease
    repository.acquire_next.assert_called_once()
    repository.create_attempt_run.assert_called_once()


def test_no_eligible_work_does_not_allocate_a_run():
    repository = Mock()
    repository.acquire_next.return_value = None
    assert _service(repository).acquire_one(
        uuid4(), worker_id="worker-one", lease_duration=timedelta(minutes=5)
    ) is None
    repository.create_attempt_run.assert_not_called()


def test_internal_local_folder_acquire_allocates_one_run_without_tenant_input():
    repository = Mock()
    lease = _lease()
    repository.acquire_next_local_folder.return_value = lease
    repository.create_attempt_run.return_value = ConnectorSyncRun(id=uuid4())
    result = _service(repository).acquire_one_local_folder(
        worker_id="worker-one",
        lease_duration=timedelta(minutes=5),
    )
    assert result is not None and result.lease.organization_id == lease.organization_id
    repository.acquire_next_local_folder.assert_called_once()
    repository.create_attempt_run.assert_called_once()


def test_retry_decision_passes_only_safe_controlled_error_fields():
    repository = Mock()
    lease = _lease()
    _service(repository).fail_attempt(
        lease,
        worker_id="worker-one",
        kind=SyncFailureKind.RATE_LIMITED,
        retry_after_seconds=120,
    )
    kwargs = repository.record_failure.call_args.kwargs
    assert kwargs["error_category"] == "rate_limit"
    assert kwargs["error_code"] == "provider_rate_limited"
    assert kwargs["retry_at"] == NOW + timedelta(seconds=120)
    assert "summary" not in kwargs


def test_exhausted_retry_is_terminal_and_cancelled_kind_requires_acknowledgement():
    repository = Mock()
    lease = _lease(attempt=3, maximum=3)
    _service(repository).fail_attempt(
        lease,
        worker_id="worker-one",
        kind=SyncFailureKind.RETRYABLE_PROVIDER,
    )
    assert repository.record_failure.call_args.kwargs["retry_at"] is None
    with pytest.raises(RetryPolicyViolation):
        _service(repository).fail_attempt(
            lease,
            worker_id="worker-one",
            kind=SyncFailureKind.CANCELLED,
        )


def test_recovery_is_bounded_by_repository_result_and_does_not_increment_attempts():
    repository = Mock()
    expired = Mock(attempt_count=1, max_attempts=3, cancellation_requested=False)
    repository.lock_expired.return_value = (expired,)
    repository.recover_expired.return_value = Mock()
    _service(repository).recover_expired(limit=1)
    repository.lock_expired.assert_called_once()
    repository.recover_expired.assert_called_once()
    assert repository.recover_expired.call_args.kwargs["retry_at"] == NOW + timedelta(seconds=15)


def test_internal_local_folder_recovery_uses_narrow_repository_query():
    repository = Mock()
    expired = Mock(attempt_count=1, max_attempts=3, cancellation_requested=False)
    repository.lock_expired_local_folder.return_value = (expired,)
    repository.recover_expired.return_value = Mock()
    recovered = _service(repository).recover_expired_local_folder(limit=4)
    assert len(recovered) == 1
    repository.lock_expired_local_folder.assert_called_once_with(now=NOW, limit=4)
    repository.lock_expired.assert_not_called()


def test_service_and_repository_source_have_no_sleep_commit_rollback_or_forever_loop():
    import inspect
    import application.services.connector_sync_execution_service as service_module
    import infrastructure.repositories.connector_sync_job_repository as repository_module

    source = inspect.getsource(service_module) + inspect.getsource(repository_module)
    assert "sleep(" not in source
    assert ".commit(" not in source
    assert ".rollback(" not in source
    assert "while True" not in source
