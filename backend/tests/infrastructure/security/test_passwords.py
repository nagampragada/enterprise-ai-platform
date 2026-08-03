from __future__ import annotations

import pytest

from infrastructure.security.passwords import hash_password, verify_password


def test_hash_is_not_plaintext() -> None:
    password = "CorrectHorseBatteryStaple"

    password_hash = hash_password(password)

    assert password_hash != password


def test_verify_correct_password_returns_true() -> None:
    password = "CorrectHorseBatteryStaple"
    password_hash = hash_password(password)

    assert verify_password(password, password_hash) is True


def test_verify_incorrect_password_returns_false() -> None:
    password_hash = hash_password("CorrectHorseBatteryStaple")

    assert verify_password("WrongPassword", password_hash) is False


def test_hashing_same_password_twice_produces_different_hashes() -> None:
    password = "CorrectHorseBatteryStaple"

    first_hash = hash_password(password)
    second_hash = hash_password(password)

    assert first_hash != second_hash


def test_empty_password_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Password must not be empty"):
        hash_password("")

    with pytest.raises(ValueError, match="Password must not be empty"):
        verify_password("", "$argon2id$v=19$m=65536,t=3,p=4$abc$def")


def test_malformed_password_hash_returns_false() -> None:
    assert verify_password("CorrectHorseBatteryStaple", "not-an-argon2-hash") is False
