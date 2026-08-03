"""Password hashing utilities based on Argon2."""

from __future__ import annotations

from typing import Final

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError


_PASSWORD_HASHER: Final[PasswordHasher] = PasswordHasher()


def hash_password(password: str) -> str:
    """Hash a plain-text password using Argon2.

    Args:
        password: The plain-text password to hash.

    Returns:
        The encoded Argon2 password hash.

    Raises:
        ValueError: If the password is empty.
    """
    if not password:
        raise ValueError("Password must not be empty")
    return _PASSWORD_HASHER.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a plain-text password against an Argon2 hash.

    Args:
        password: The plain-text password to verify.
        password_hash: The stored Argon2-encoded hash.

    Returns:
        True if the password matches the hash; otherwise False.

    Raises:
        ValueError: If the password is empty.
    """
    if not password:
        raise ValueError("Password must not be empty")
    if not password_hash:
        return False

    try:
        return _PASSWORD_HASHER.verify(password_hash, password)
    except (InvalidHashError, VerificationError):
        # Covers mismatches and malformed/invalid hashes.
        return False
