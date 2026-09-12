"""Bounded capability ownership for controlled connector synchronization."""

from __future__ import annotations

import base64
import hashlib
import re
import secrets
from dataclasses import dataclass
from uuid import UUID, uuid4


MIN_RESERVATION_SECONDS = 300
MAX_RESERVATION_SECONDS = 7_200
_OWNER_TOKEN = re.compile(r"^[A-Za-z0-9_-]{43}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_OBJECT_ID = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_PROFILE = re.compile(r"^[a-z0-9][a-z0-9._:/-]{0,254}$")


@dataclass(frozen=True, repr=False)
class ControlReservationOwner:
    """Opaque reservation capability excluded from persistence and repr output."""

    reservation_id: UUID
    owner_token: str

    def __post_init__(self) -> None:
        if not isinstance(self.reservation_id, UUID):
            raise ValueError("reservation_id must be a UUID")
        _validate_owner_token(self.owner_token)

    @property
    def owner_token_hash(self) -> str:
        return hashlib.sha256(self.owner_token.encode("ascii")).hexdigest()


@dataclass(frozen=True, repr=False)
class ControlledSyncReservationRequest:
    """Reservation created by an authenticated organization administrator."""

    owner: ControlReservationOwner
    created_by_user_id: UUID
    expires_in_seconds: int
    target_source_key_hash: str
    target_provider_blob_id: str
    target_provider_revision_id: str
    target_profile_fingerprint: str

    def __post_init__(self) -> None:
        if not isinstance(self.owner, ControlReservationOwner):
            raise ValueError("reservation owner is invalid")
        if not isinstance(self.created_by_user_id, UUID):
            raise ValueError("reservation creator must be a UUID")
        if (
            isinstance(self.expires_in_seconds, bool)
            or not isinstance(self.expires_in_seconds, int)
            or not MIN_RESERVATION_SECONDS
            <= self.expires_in_seconds
            <= MAX_RESERVATION_SECONDS
        ):
            raise ValueError("reservation expiry is invalid")
        if not isinstance(self.target_source_key_hash, str) or not _SHA256.fullmatch(
            self.target_source_key_hash
        ):
            raise ValueError("reservation source identity is invalid")
        for value in (
            self.target_provider_blob_id,
            self.target_provider_revision_id,
        ):
            if not isinstance(value, str) or not _OBJECT_ID.fullmatch(value):
                raise ValueError("reservation provider identity is invalid")
        if not isinstance(self.target_profile_fingerprint, str) or not _PROFILE.fullmatch(
            self.target_profile_fingerprint
        ):
            raise ValueError("reservation profile is invalid")


def generate_control_reservation_owner(
    reservation_id: UUID | None = None,
) -> ControlReservationOwner:
    """Generate a fresh 256-bit bearer capability for a controlled operation."""

    identifier = uuid4() if reservation_id is None else reservation_id
    if not isinstance(identifier, UUID):
        raise ValueError("reservation_id must be a UUID")
    token = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii").rstrip("=")
    return ControlReservationOwner(identifier, token)


def _validate_owner_token(value: object) -> None:
    if not isinstance(value, str) or not _OWNER_TOKEN.fullmatch(value):
        raise ValueError("reservation owner token is invalid")
    try:
        decoded = base64.urlsafe_b64decode(value + "=")
    except (ValueError, TypeError) as exc:
        raise ValueError("reservation owner token is invalid") from exc
    canonical = base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=")
    if len(decoded) != 32 or canonical != value:
        raise ValueError("reservation owner token is invalid")
