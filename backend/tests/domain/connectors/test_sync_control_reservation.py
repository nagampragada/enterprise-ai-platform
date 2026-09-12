from __future__ import annotations

import base64
from uuid import uuid4

import pytest

import domain.connectors.sync_control_reservation as reservation_module
from domain.connectors.sync_control_reservation import (
    ControlReservationOwner,
    ControlledSyncReservationRequest,
    generate_control_reservation_owner,
)


def _token(byte: int = 1) -> str:
    return base64.urlsafe_b64encode(bytes([byte]) * 32).decode().rstrip("=")


def test_owner_is_an_opaque_capability_with_deterministic_hash():
    owner = ControlReservationOwner(uuid4(), _token())

    assert len(owner.owner_token_hash) == 64
    assert owner.owner_token not in repr(owner)
    assert owner.owner_token_hash == owner.owner_token_hash


def test_owner_generator_uses_exactly_256_bits_and_canonical_base64url(monkeypatch):
    requested_sizes: list[int] = []

    def token_bytes(size: int) -> bytes:
        requested_sizes.append(size)
        return bytes(range(size))

    monkeypatch.setattr(reservation_module.secrets, "token_bytes", token_bytes)
    reservation_id = uuid4()

    owner = generate_control_reservation_owner(reservation_id)

    assert requested_sizes == [32]
    assert owner.reservation_id == reservation_id
    assert owner.owner_token == _token_from_bytes(bytes(range(32)))
    assert len(owner.owner_token) == 43
    assert owner.owner_token not in repr(owner)


def _token_from_bytes(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


@pytest.mark.parametrize(
    "token",
    ("", "A" * 42, "A" * 44, "unsafe+capability" + "A" * 26),
)
def test_owner_rejects_noncanonical_capabilities(token):
    with pytest.raises(ValueError, match="owner token"):
        ControlReservationOwner(uuid4(), token)


def test_controlled_request_is_bounded_and_hides_capability():
    owner = ControlReservationOwner(uuid4(), _token(2))
    request = ControlledSyncReservationRequest(
        owner,
        uuid4(),
        300,
        "a" * 64,
        "b" * 40,
        "c" * 40,
        "github:extract-v1:chunk-v2:embed-v1",
    )

    assert request.expires_in_seconds == 300
    assert owner.owner_token not in repr(request)


@pytest.mark.parametrize("expiry", (299, 7201, True, 300.0))
def test_controlled_request_rejects_invalid_expiry(expiry):
    with pytest.raises(ValueError, match="expiry"):
        ControlledSyncReservationRequest(
            ControlReservationOwner(uuid4(), _token(3)),
            uuid4(),
            expiry,
            "a" * 64,
            "b" * 40,
            "c" * 40,
            "profile",
        )
