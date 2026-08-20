"""Bounded Google Cloud Secret Manager implementation of the SecretStore port."""

from __future__ import annotations

from collections.abc import Callable
import random
import re
import secrets
import time
from typing import NoReturn, TypeVar

from google.api_core import exceptions as google_exceptions
from google.cloud import secretmanager
import google_crc32c

from app.config import GoogleSecretManagerSettings
from application.ports.secret_store import (
    InvalidSecretReference,
    SecretNotFound,
    SecretReference,
    SecretStoreAccessDenied,
    SecretStoreError,
    SecretStoreIntegrityError,
    SecretStoreUnavailable,
    SecretValue,
)


REFERENCE_SCHEME = "gcp-secret-manager"
MAX_PAYLOAD_BYTES = 65_536
PROVIDER_CALL_TIMEOUT_SECONDS = 5.0
READ_RETRY_DEADLINE_SECONDS = 12.0
READ_MAX_ATTEMPTS = 3
READ_RETRY_INITIAL_DELAY_SECONDS = 0.1
READ_RETRY_MAX_DELAY_SECONDS = 1.0
MAX_NAME_COLLISION_ATTEMPTS = 3
MAX_VERSION_NUMBER = 9_223_372_036_854_775_807

_MANAGED_LABELS = {
    "managed-by": "enterprise-ai-platform",
    "eap-secret-policy": "single-version",
}
_TRANSIENT_READ_ERRORS = (
    google_exceptions.DeadlineExceeded,
    google_exceptions.ServiceUnavailable,
    google_exceptions.ResourceExhausted,
)
_T = TypeVar("_T")


class GoogleSecretManagerSecretStore:
    """Store one value per random container and return an immutable opaque reference."""

    def __init__(
        self,
        settings: GoogleSecretManagerSettings,
        *,
        client: object | None = None,
        token_factory: Callable[[int], str] = secrets.token_hex,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        jitter: Callable[[float, float], float] = random.SystemRandom().uniform,
    ) -> None:
        if not isinstance(settings, GoogleSecretManagerSettings):
            raise ValueError("Google Secret Manager configuration is invalid")
        self._settings = settings
        self._token_factory = token_factory
        self._monotonic = monotonic
        self._sleep = sleeper
        self._jitter = jitter
        self._secret_id_pattern = re.compile(
            rf"{re.escape(settings.secret_prefix)}-sm-[0-9a-f]{{32}}"
        )
        try:
            self._client = (
                client if client is not None else secretmanager.SecretManagerServiceClient()
            )
        except Exception as exc:
            raise SecretStoreUnavailable() from exc

    def store(self, secret: SecretValue) -> SecretReference:
        if not isinstance(secret, SecretValue):
            raise SecretStoreError()
        try:
            payload = secret.value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise SecretStoreError() from exc
        if not payload or len(payload) > MAX_PAYLOAD_BYTES:
            raise SecretStoreError()
        checksum = google_crc32c.value(payload)
        secret_name: str | None = None
        last_collision: Exception | None = None

        for _ in range(MAX_NAME_COLLISION_ATTEMPTS):
            secret_id = self._new_secret_id()
            candidate_name = self._secret_name(secret_id)
            try:
                created = self._client.create_secret(
                    request={
                        "parent": self._project_name,
                        "secret_id": secret_id,
                        "secret": {
                            "replication": {"automatic": {}},
                            "labels": self._expected_labels,
                        },
                    },
                    retry=None,
                    timeout=PROVIDER_CALL_TIMEOUT_SECONDS,
                )
            except google_exceptions.AlreadyExists as exc:
                last_collision = exc
                continue
            except Exception as exc:
                self._raise_provider_error(exc, not_found=False)
            if getattr(created, "name", None) != candidate_name:
                self._cleanup_container(candidate_name)
                raise SecretStoreIntegrityError()
            secret_name = candidate_name
            break

        if secret_name is None:
            raise SecretStoreUnavailable() from last_collision

        try:
            version = self._client.add_secret_version(
                request={
                    "parent": secret_name,
                    "payload": {"data": payload, "data_crc32c": checksum},
                },
                retry=None,
                timeout=PROVIDER_CALL_TIMEOUT_SECONDS,
            )
            if getattr(version, "client_specified_payload_checksum", None) is not True:
                raise SecretStoreIntegrityError()
            resource_name = getattr(version, "name", None)
            if not isinstance(resource_name, str):
                raise SecretStoreIntegrityError()
            reference = SecretReference(f"{REFERENCE_SCHEME}://{resource_name}")
            try:
                parsed = self._parse_reference(reference)
            except InvalidSecretReference as exc:
                raise SecretStoreIntegrityError() from exc
            if parsed[0] != secret_name:
                raise SecretStoreIntegrityError()
            return reference
        except SecretStoreError:
            self._cleanup_container(secret_name)
            raise
        except Exception as exc:
            self._cleanup_container(secret_name)
            self._raise_provider_error(exc, not_found=False)

    def retrieve(self, reference: SecretReference) -> SecretValue:
        _, version_name = self._parse_reference(reference)
        try:
            response = self._read_with_retry(
                lambda timeout: self._client.access_secret_version(
                    request={"name": version_name}, retry=None, timeout=timeout
                )
            )
        except Exception as exc:
            self._raise_provider_error(exc, not_found=True)
        if getattr(response, "name", None) != version_name:
            raise SecretStoreIntegrityError()
        provider_payload = getattr(response, "payload", None)
        data = getattr(provider_payload, "data", None)
        if not isinstance(data, bytes) or not data or len(data) > MAX_PAYLOAD_BYTES:
            raise SecretStoreIntegrityError()
        supplied_checksum = _supplied_crc32c(provider_payload)
        if supplied_checksum is not None and supplied_checksum != google_crc32c.value(data):
            raise SecretStoreIntegrityError()
        try:
            return SecretValue(data.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise SecretStoreIntegrityError() from exc

    def delete(self, reference: SecretReference) -> None:
        secret_name, version_name = self._parse_reference(reference)
        try:
            metadata = self._read_with_retry(
                lambda timeout: self._client.get_secret(
                    request={"name": secret_name}, retry=None, timeout=timeout
                )
            )
        except google_exceptions.NotFound:
            return
        except Exception as exc:
            self._raise_provider_error(exc, not_found=False)
        if (
            getattr(metadata, "name", None) != secret_name
            or not self._is_adapter_managed(getattr(metadata, "labels", None))
        ):
            raise InvalidSecretReference()

        try:
            self._client.destroy_secret_version(
                request={"name": version_name},
                retry=None,
                timeout=PROVIDER_CALL_TIMEOUT_SECONDS,
            )
        except google_exceptions.NotFound:
            return
        except google_exceptions.FailedPrecondition:
            pass
        except Exception as exc:
            self._raise_provider_error(exc, not_found=False)

        try:
            self._client.delete_secret(
                request={"name": secret_name},
                retry=None,
                timeout=PROVIDER_CALL_TIMEOUT_SECONDS,
            )
        except google_exceptions.NotFound:
            return
        except Exception as exc:
            self._raise_provider_error(exc, not_found=False)

    def validate_reference(self, reference: SecretReference) -> None:
        """Validate configured references locally without contacting Google Cloud."""
        self._parse_reference(reference)

    @property
    def _project_name(self) -> str:
        return f"projects/{self._settings.project_id}"

    @property
    def _expected_labels(self) -> dict[str, str]:
        return {**_MANAGED_LABELS, "environment": self._settings.environment}

    def _new_secret_id(self) -> str:
        try:
            token = self._token_factory(16)
        except Exception as exc:
            raise SecretStoreUnavailable() from exc
        secret_id = f"{self._settings.secret_prefix}-sm-{token}"
        if self._secret_id_pattern.fullmatch(secret_id) is None or len(secret_id) > 255:
            raise SecretStoreUnavailable()
        return secret_id

    def _secret_name(self, secret_id: str) -> str:
        return f"{self._project_name}/secrets/{secret_id}"

    def _parse_reference(self, reference: SecretReference) -> tuple[str, str]:
        if not isinstance(reference, SecretReference):
            raise InvalidSecretReference()
        pattern = re.compile(
            rf"{REFERENCE_SCHEME}://projects/"
            rf"(?P<project>[a-z][a-z0-9-]{{4,28}}[a-z0-9])/secrets/"
            rf"(?P<secret>{self._secret_id_pattern.pattern})/versions/"
            rf"(?P<version>[1-9][0-9]*)"
        )
        match = pattern.fullmatch(reference.value)
        if match is None or match.group("project") != self._settings.project_id:
            raise InvalidSecretReference()
        try:
            version = int(match.group("version"))
        except ValueError as exc:
            raise InvalidSecretReference() from exc
        if version > MAX_VERSION_NUMBER:
            raise InvalidSecretReference()
        secret_name = self._secret_name(match.group("secret"))
        return secret_name, f"{secret_name}/versions/{version}"

    def _is_adapter_managed(self, labels: object) -> bool:
        if not isinstance(labels, dict):
            try:
                labels = dict(labels)
            except (TypeError, ValueError):
                return False
        return all(labels.get(key) == value for key, value in self._expected_labels.items())

    def _read_with_retry(self, operation: Callable[[float], _T]) -> _T:
        deadline = self._monotonic() + READ_RETRY_DEADLINE_SECONDS
        for attempt in range(READ_MAX_ATTEMPTS):
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise google_exceptions.DeadlineExceeded("read deadline exceeded")
            try:
                return operation(min(PROVIDER_CALL_TIMEOUT_SECONDS, remaining))
            except _TRANSIENT_READ_ERRORS:
                if attempt + 1 >= READ_MAX_ATTEMPTS:
                    raise
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    raise
                ceiling = min(
                    READ_RETRY_INITIAL_DELAY_SECONDS * (2**attempt),
                    READ_RETRY_MAX_DELAY_SECONDS,
                    remaining,
                )
                self._sleep(self._jitter(0.0, ceiling))
        raise google_exceptions.DeadlineExceeded("read deadline exceeded")

    def _cleanup_container(self, secret_name: str) -> None:
        try:
            self._client.delete_secret(
                request={"name": secret_name},
                retry=None,
                timeout=PROVIDER_CALL_TIMEOUT_SECONDS,
            )
        except Exception:
            pass

    @staticmethod
    def _raise_provider_error(exc: Exception, *, not_found: bool) -> NoReturn:
        if isinstance(exc, google_exceptions.NotFound):
            error: SecretStoreError = SecretNotFound() if not_found else SecretStoreError()
        elif isinstance(
            exc, (google_exceptions.PermissionDenied, google_exceptions.Unauthenticated)
        ):
            error = SecretStoreAccessDenied()
        elif isinstance(exc, _TRANSIENT_READ_ERRORS):
            error = SecretStoreUnavailable()
        elif isinstance(
            exc,
            (
                google_exceptions.AlreadyExists,
                google_exceptions.FailedPrecondition,
                google_exceptions.InvalidArgument,
            ),
        ):
            error = SecretStoreError()
        else:
            error = SecretStoreError()
        raise error from exc


def _supplied_crc32c(payload: object) -> int | None:
    if payload is None:
        return None
    protobuf = getattr(payload, "_pb", None)
    if protobuf is not None:
        try:
            if not protobuf.HasField("data_crc32c"):
                return None
        except (ValueError, AttributeError):
            pass
    value = getattr(payload, "data_crc32c", None)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 0xFFFFFFFF:
        raise SecretStoreIntegrityError()
    return value
