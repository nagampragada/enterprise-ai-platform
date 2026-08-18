from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import inf

import pytest
from sqlalchemy.exc import OperationalError

from application.services.connector_sync_retry_policy import (
    DEFAULT_MAX_ATTEMPTS,
    HARD_MAX_ATTEMPTS,
    RATE_LIMIT_CAP_SECONDS,
    ConnectorSyncRetryPolicy,
    RetryPolicyViolation,
    SyncFailureKind,
    classify_exception,
    validate_max_attempts,
)
from domain.connectors.exceptions import (
    ConnectorAuthenticationError,
    ConnectorAuthorizationError,
    ConnectorRateLimitError,
    ConnectorUnavailableError,
)
from domain.content_extraction.exceptions import EncryptedContentError, UnsupportedContentTypeError
from domain.embeddings.exceptions import (
    InvalidEmbeddingConfigurationError,
    InvalidEmbeddingInputError,
    PermanentEmbeddingProviderError,
    RetryableEmbeddingProviderError,
)

NOW = datetime(2026, 8, 24, tzinfo=timezone.utc)


def _policy(fraction: float = 0.5) -> ConnectorSyncRetryPolicy:
    return ConnectorSyncRetryPolicy(random_uniform=lambda low, high: low + (high - low) * fraction)


def test_attempt_limits_are_bounded_and_include_initial_execution():
    assert DEFAULT_MAX_ATTEMPTS == 3
    assert HARD_MAX_ATTEMPTS == 5
    assert validate_max_attempts(1) == 1
    assert validate_max_attempts(5) == 5
    for value in (None, 0, -1, 6, inf, "3", True):
        with pytest.raises(RetryPolicyViolation):
            validate_max_attempts(value)


def test_max_attempts_one_and_exhaustion_never_schedule_retry():
    decision = _policy().decide(
        kind=SyncFailureKind.RETRYABLE_PROVIDER,
        attempt_count=1,
        max_attempts=1,
        now=NOW,
    )
    assert decision.terminal_status == "failed"
    assert decision.retry_at is None


@pytest.mark.parametrize(
    "kind",
    (
        SyncFailureKind.AUTHENTICATION,
        SyncFailureKind.CONFIGURATION,
        SyncFailureKind.VALIDATION,
        SyncFailureKind.UNSUPPORTED_CONTENT,
        SyncFailureKind.PERMANENT_PROVIDER,
        SyncFailureKind.PROGRAMMING_ERROR,
        SyncFailureKind.UNKNOWN_INTERNAL,
    ),
)
def test_nonretryable_failures_fail_closed(kind: SyncFailureKind):
    decision = _policy().decide(kind=kind, attempt_count=1, max_attempts=3, now=NOW)
    assert decision.terminal_status == "failed"
    assert decision.retry_at is None
    assert decision.classification.retryable is False


def test_retryable_failure_schedules_only_while_attempts_remain():
    first = _policy().decide(
        kind=SyncFailureKind.TRANSIENT_NETWORK, attempt_count=1, max_attempts=3, now=NOW
    )
    exhausted = _policy().decide(
        kind=SyncFailureKind.TRANSIENT_NETWORK, attempt_count=3, max_attempts=3, now=NOW
    )
    assert first.retry_at == NOW + timedelta(seconds=15)
    assert first.terminal_status is None
    assert exhausted.terminal_status == "failed"
    assert exhausted.retry_at is None


def test_exponential_backoff_uses_first_retry_exponent_zero_and_injected_jitter():
    policy = _policy(0.5)
    assert policy.delay_seconds(kind=SyncFailureKind.RETRYABLE_PROVIDER, attempt_count=1) == 15
    assert policy.delay_seconds(kind=SyncFailureKind.RETRYABLE_PROVIDER, attempt_count=2) == 30
    assert policy.delay_seconds(kind=SyncFailureKind.RETRYABLE_PROVIDER, attempt_count=3) == 60
    assert ConnectorSyncRetryPolicy(
        random_uniform=lambda low, high: high
    ).delay_seconds(kind=SyncFailureKind.RETRYABLE_PROVIDER, attempt_count=10) == 900


def test_rate_limit_retry_after_is_validated_and_capped_without_jitter():
    policy = _policy(0.1)
    assert policy.delay_seconds(
        kind=SyncFailureKind.RATE_LIMITED, attempt_count=1, retry_after_seconds=120
    ) == 120
    assert policy.delay_seconds(
        kind=SyncFailureKind.RATE_LIMITED,
        attempt_count=1,
        retry_after_seconds=RATE_LIMIT_CAP_SECONDS * 2,
    ) == RATE_LIMIT_CAP_SECONDS
    for value in (-1, inf, "60", True):
        with pytest.raises(RetryPolicyViolation):
            policy.delay_seconds(
                kind=SyncFailureKind.RATE_LIMITED,
                attempt_count=1,
                retry_after_seconds=value,
            )


def test_retry_after_is_rejected_for_non_rate_limit_failures():
    with pytest.raises(RetryPolicyViolation):
        _policy().delay_seconds(
            kind=SyncFailureKind.RETRYABLE_PROVIDER,
            attempt_count=1,
            retry_after_seconds=30,
        )


def test_cancellation_prevents_retry_scheduling():
    decision = _policy().decide(
        kind=SyncFailureKind.RETRYABLE_PROVIDER,
        attempt_count=1,
        max_attempts=3,
        now=NOW,
        cancellation_requested=True,
    )
    assert decision.terminal_status == "cancelled"
    assert decision.retry_at is None


@pytest.mark.parametrize(
    ("error", "kind", "retryable"),
    (
        (ConnectorRateLimitError(), SyncFailureKind.RATE_LIMITED, True),
        (ConnectorUnavailableError(), SyncFailureKind.RETRYABLE_PROVIDER, True),
        (RetryableEmbeddingProviderError(), SyncFailureKind.RETRYABLE_PROVIDER, True),
        (ConnectorAuthenticationError(), SyncFailureKind.AUTHENTICATION, False),
        (ConnectorAuthorizationError(), SyncFailureKind.AUTHORIZATION, False),
        (TimeoutError(), SyncFailureKind.TRANSIENT_NETWORK, True),
        (ConnectionError(), SyncFailureKind.TRANSIENT_NETWORK, True),
        (InvalidEmbeddingConfigurationError(), SyncFailureKind.CONFIGURATION, False),
        (InvalidEmbeddingInputError(), SyncFailureKind.VALIDATION, False),
        (UnsupportedContentTypeError(), SyncFailureKind.UNSUPPORTED_CONTENT, False),
        (EncryptedContentError(), SyncFailureKind.UNSUPPORTED_CONTENT, False),
        (PermanentEmbeddingProviderError(), SyncFailureKind.PERMANENT_PROVIDER, False),
        (AssertionError(), SyncFailureKind.PROGRAMMING_ERROR, False),
        (RuntimeError(), SyncFailureKind.UNKNOWN_INTERNAL, False),
        (Exception(), SyncFailureKind.UNKNOWN_INTERNAL, False),
    ),
)
def test_exception_classification_is_explicit_and_unknown_fails_closed(error, kind, retryable):
    classification = classify_exception(error)
    assert classification.kind is kind
    assert classification.retryable is retryable


def test_policy_rejects_invalid_attempts_and_randomness():
    for attempt in (0, -1, True, 4):
        with pytest.raises(RetryPolicyViolation):
            _policy().decide(
                kind=SyncFailureKind.RETRYABLE_PROVIDER,
                attempt_count=attempt,
                max_attempts=3,
                now=NOW,
            )
    with pytest.raises(RetryPolicyViolation):
        ConnectorSyncRetryPolicy(random_uniform=lambda low, high: high + 1).delay_seconds(
            kind=SyncFailureKind.RETRYABLE_PROVIDER, attempt_count=1
        )


def test_only_replay_safe_postgresql_operational_errors_are_transient():
    class DatabaseError(Exception):
        def __init__(self, sqlstate):
            self.sqlstate = sqlstate

    for sqlstate in ("40001", "40P01"):
        classification = classify_exception(OperationalError("statement", {}, DatabaseError(sqlstate)))
        assert classification.kind is SyncFailureKind.TRANSIENT_PERSISTENCE
        assert classification.retryable is True
    generic = classify_exception(OperationalError("statement", {}, DatabaseError("08006")))
    assert generic.kind is SyncFailureKind.UNKNOWN_INTERNAL
    assert generic.retryable is False


def test_policy_contains_no_sleep_or_transaction_control():
    import inspect
    import application.services.connector_sync_retry_policy as module

    source = inspect.getsource(module)
    assert "sleep(" not in source
    assert ".commit(" not in source
    assert ".rollback(" not in source