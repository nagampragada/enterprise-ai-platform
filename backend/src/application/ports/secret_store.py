"""Provider-neutral secret storage boundary with redacted value objects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class SecretStoreError(RuntimeError):
    """Base safe secret-store failure."""

    message = "secret store operation failed"

    def __init__(self, *_unsafe_details: object) -> None:
        super().__init__(self.message)


class SecretStoreUnavailable(SecretStoreError):
    message = "secret store is unavailable"


class SecretNotFound(SecretStoreError):
    message = "secret was not found"


class SecretStoreAccessDenied(SecretStoreError):
    message = "secret store access was denied"


class InvalidSecretReference(SecretStoreError):
    message = "secret reference is invalid"


class SecretStoreIntegrityError(SecretStoreError):
    message = "secret store integrity check failed"


@dataclass(frozen=True, repr=False)
class SecretValue:
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not self.value:
            raise ValueError("secret value is invalid")


@dataclass(frozen=True, repr=False)
class SecretReference:
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or not self.value.strip() or len(self.value) > 1024:
            raise InvalidSecretReference("secret reference is invalid")


class SecretStore(Protocol):
    def store(self, secret: SecretValue) -> SecretReference: ...
    def retrieve(self, reference: SecretReference) -> SecretValue: ...
    def delete(self, reference: SecretReference) -> None: ...
