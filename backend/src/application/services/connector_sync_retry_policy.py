"""Pure bounded retry policy for connector synchronization attempts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from math import isfinite
from typing import Callable

from sqlalchemy.exc import OperationalError

from domain.connectors.exceptions import (
    ConnectorAuthenticationError,
    ConnectorAuthorizationError,
    ConnectorCheckpointError,
    ConnectorContentError,
    ConnectorItemNotFoundError,
    ConnectorRateLimitError,
    ConnectorUnavailableError,
)
from domain.content_extraction.exceptions import (
    ContentParseError,
    ContentReadError,
    ContentTooLargeError,
    EncryptedContentError,
    UnsupportedContentTypeError,
)
from domain.embeddings.exceptions import (
    EmbeddingProviderAuthenticationError,
    InvalidEmbeddingConfigurationError,
    InvalidEmbeddingInputError,
    InvalidEmbeddingResultError,
    PermanentEmbeddingProviderError,
    RetryableEmbeddingProviderError,
)

DEFAULT_MAX_ATTEMPTS = 3
HARD_MAX_ATTEMPTS = 5
DEFAULT_BASE_DELAY_SECONDS = 30.0
DEFAULT_BACKOFF_CAP_SECONDS = 15 * 60.0
RATE_LIMIT_CAP_SECONDS = 60 * 60.0


class RetryPolicyViolation(ValueError):
    """Raised when retry policy input is invalid."""


class SyncFailureKind(StrEnum):
    RETRYABLE_PROVIDER = "retryable_provider"
    RATE_LIMITED = "rate_limited"
    TRANSIENT_NETWORK = "transient_network"
    TRANSIENT_PERSISTENCE = "transient_persistence"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    CONFIGURATION = "configuration"
    VALIDATION = "validation"
    UNSUPPORTED_CONTENT = "unsupported_content"
    PERMANENT_PROVIDER = "permanent_provider"
    CANCELLED = "cancelled"
    PROGRAMMING_ERROR = "programming_error"
    UNKNOWN_INTERNAL = "unknown_internal"


@dataclass(frozen=True)
class FailureClassification:
    kind: SyncFailureKind
    error_category: str
    error_code: str
    retryable: bool


@dataclass(frozen=True)
class RetryDecision:
    terminal_status: str | None
    retry_at: datetime | None
    classification: FailureClassification


_CLASSIFICATIONS = {
    SyncFailureKind.RETRYABLE_PROVIDER: FailureClassification(
        SyncFailureKind.RETRYABLE_PROVIDER, "source_read", "provider_temporarily_unavailable", True
    ),
    SyncFailureKind.RATE_LIMITED: FailureClassification(
        SyncFailureKind.RATE_LIMITED, "rate_limit", "provider_rate_limited", True
    ),
    SyncFailureKind.TRANSIENT_NETWORK: FailureClassification(
        SyncFailureKind.TRANSIENT_NETWORK, "source_read", "network_temporarily_unavailable", True
    ),
    SyncFailureKind.TRANSIENT_PERSISTENCE: FailureClassification(
        SyncFailureKind.TRANSIENT_PERSISTENCE, "persistence", "transaction_temporarily_unavailable", True
    ),
    SyncFailureKind.AUTHENTICATION: FailureClassification(
        SyncFailureKind.AUTHENTICATION, "authentication", "authentication_failed", False
    ),
    SyncFailureKind.AUTHORIZATION: FailureClassification(
        SyncFailureKind.AUTHORIZATION, "authorization", "authorization_denied", False
    ),
    SyncFailureKind.CONFIGURATION: FailureClassification(
        SyncFailureKind.CONFIGURATION, "configuration", "configuration_invalid", False
    ),
    SyncFailureKind.VALIDATION: FailureClassification(
        SyncFailureKind.VALIDATION, "configuration", "request_invalid", False
    ),
    SyncFailureKind.UNSUPPORTED_CONTENT: FailureClassification(
        SyncFailureKind.UNSUPPORTED_CONTENT, "extraction", "content_unsupported", False
    ),
    SyncFailureKind.PERMANENT_PROVIDER: FailureClassification(
        SyncFailureKind.PERMANENT_PROVIDER, "source_read", "provider_failure_permanent", False
    ),
    SyncFailureKind.CANCELLED: FailureClassification(
        SyncFailureKind.CANCELLED, "internal", "execution_cancelled", False
    ),
    SyncFailureKind.PROGRAMMING_ERROR: FailureClassification(
        SyncFailureKind.PROGRAMMING_ERROR, "internal", "programming_error", False
    ),
    SyncFailureKind.UNKNOWN_INTERNAL: FailureClassification(
        SyncFailureKind.UNKNOWN_INTERNAL, "internal", "unknown_internal", False
    ),
}


class ConnectorSyncRetryPolicy:
    def __init__(
        self,
        *,
        random_uniform: Callable[[float, float], float],
        base_delay_seconds: float = DEFAULT_BASE_DELAY_SECONDS,
        backoff_cap_seconds: float = DEFAULT_BACKOFF_CAP_SECONDS,
    ) -> None:
        self._random_uniform = random_uniform
        self._base_delay_seconds = _require_positive_finite("base_delay_seconds", base_delay_seconds)
        self._backoff_cap_seconds = _require_positive_finite(
            "backoff_cap_seconds", backoff_cap_seconds
        )

    def decide(
        self,
        *,
        kind: SyncFailureKind,
        attempt_count: int,
        max_attempts: int,
        now: datetime,
        cancellation_requested: bool = False,
        retry_after_seconds: float | None = None,
    ) -> RetryDecision:
        classification = classification_for(kind)
        validate_max_attempts(max_attempts)
        _require_attempt_count(attempt_count, max_attempts)
        _require_aware(now)
        if not isinstance(cancellation_requested, bool):
            raise RetryPolicyViolation("cancellation_requested must be boolean")
        if cancellation_requested or kind is SyncFailureKind.CANCELLED:
            return RetryDecision("cancelled", None, _CLASSIFICATIONS[SyncFailureKind.CANCELLED])
        if not classification.retryable or attempt_count >= max_attempts:
            return RetryDecision("failed", None, classification)
        delay = self.delay_seconds(
            attempt_count=attempt_count,
            kind=kind,
            retry_after_seconds=retry_after_seconds,
        )
        return RetryDecision(None, now + timedelta(seconds=delay), classification)

    def delay_seconds(
        self,
        *,
        attempt_count: int,
        kind: SyncFailureKind,
        retry_after_seconds: float | None = None,
    ) -> float:
        if isinstance(attempt_count, bool) or not isinstance(attempt_count, int) or attempt_count < 1:
            raise RetryPolicyViolation("attempt_count must be a positive integer")
        classification_for(kind)
        if retry_after_seconds is not None:
            if kind is not SyncFailureKind.RATE_LIMITED:
                raise RetryPolicyViolation("retry_after_seconds is only valid for rate limiting")
            return min(_require_nonnegative_finite("retry_after_seconds", retry_after_seconds), RATE_LIMIT_CAP_SECONDS)
        maximum = min(
            self._base_delay_seconds * (2 ** (attempt_count - 1)),
            self._backoff_cap_seconds,
        )
        jittered = self._random_uniform(0.0, maximum)
        if not isinstance(jittered, (int, float)) or isinstance(jittered, bool):
            raise RetryPolicyViolation("random source returned an invalid value")
        value = float(jittered)
        if not isfinite(value) or value < 0.0 or value > maximum:
            raise RetryPolicyViolation("random source returned an out-of-range value")
        return value


def validate_max_attempts(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= HARD_MAX_ATTEMPTS:
        raise RetryPolicyViolation(f"max_attempts must be between 1 and {HARD_MAX_ATTEMPTS}")
    return value


def classification_for(kind: SyncFailureKind) -> FailureClassification:
    if not isinstance(kind, SyncFailureKind):
        raise RetryPolicyViolation("failure kind is invalid")
    return _CLASSIFICATIONS[kind]


def classify_exception(error: BaseException) -> FailureClassification:
    if isinstance(error, (ConnectorAuthenticationError, EmbeddingProviderAuthenticationError)):
        return _CLASSIFICATIONS[SyncFailureKind.AUTHENTICATION]
    if isinstance(error, ConnectorAuthorizationError):
        return _CLASSIFICATIONS[SyncFailureKind.AUTHORIZATION]
    if isinstance(error, ConnectorRateLimitError):
        return _CLASSIFICATIONS[SyncFailureKind.RATE_LIMITED]
    if isinstance(error, (ConnectorUnavailableError, RetryableEmbeddingProviderError)):
        return _CLASSIFICATIONS[SyncFailureKind.RETRYABLE_PROVIDER]
    if isinstance(error, (TimeoutError, ConnectionError)):
        return _CLASSIFICATIONS[SyncFailureKind.TRANSIENT_NETWORK]
    if isinstance(error, OperationalError) and _sqlstate(error) in {"40001", "40P01"}:
        return _CLASSIFICATIONS[SyncFailureKind.TRANSIENT_PERSISTENCE]
    if isinstance(error, InvalidEmbeddingConfigurationError):
        return _CLASSIFICATIONS[SyncFailureKind.CONFIGURATION]
    if isinstance(error, (InvalidEmbeddingInputError, ConnectorCheckpointError)):
        return _CLASSIFICATIONS[SyncFailureKind.VALIDATION]
    if isinstance(
        error,
        (
            UnsupportedContentTypeError,
            ContentReadError,
            ContentParseError,
            ContentTooLargeError,
            EncryptedContentError,
            ConnectorContentError,
        ),
    ):
        return _CLASSIFICATIONS[SyncFailureKind.UNSUPPORTED_CONTENT]
    if isinstance(
        error,
        (ConnectorItemNotFoundError, PermanentEmbeddingProviderError, InvalidEmbeddingResultError),
    ):
        return _CLASSIFICATIONS[SyncFailureKind.PERMANENT_PROVIDER]
    if isinstance(error, (AssertionError, TypeError)):
        return _CLASSIFICATIONS[SyncFailureKind.PROGRAMMING_ERROR]
    return _CLASSIFICATIONS[SyncFailureKind.UNKNOWN_INTERNAL]


def _require_attempt_count(value: object, max_attempts: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= max_attempts:
        raise RetryPolicyViolation("attempt_count is invalid")
    return value


def _sqlstate(error: OperationalError) -> str | None:
    value = getattr(error.orig, "sqlstate", None)
    return value if isinstance(value, str) else None


def _require_aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise RetryPolicyViolation("now must be timezone-aware")
    return value


def _require_positive_finite(name: str, value: object) -> float:
    result = _require_nonnegative_finite(name, value)
    if result == 0.0:
        raise RetryPolicyViolation(f"{name} must be positive")
    return result


def _require_nonnegative_finite(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RetryPolicyViolation(f"{name} must be numeric")
    result = float(value)
    if not isfinite(result) or result < 0.0:
        raise RetryPolicyViolation(f"{name} must be finite and nonnegative")
    return result