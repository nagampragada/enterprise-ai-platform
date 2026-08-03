from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import jwt
import pytest

from app.config import get_settings
from infrastructure.security.tokens import (
    create_access_token,
    decode_access_token,
    generate_refresh_token,
    hash_refresh_token,
    verify_refresh_token_hash,
)


@pytest.fixture(autouse=True)
def _reset_settings_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure each test controls its own configuration without leakage.
    monkeypatch.delenv("JWT_SECRET_KEY", raising=False)
    monkeypatch.delenv("ACCESS_TOKEN_LIFETIME_MINUTES", raising=False)
    monkeypatch.delenv("REFRESH_TOKEN_HASH_SECRET", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _set_token_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    jwt_secret: str = "test-jwt-secret-minimum-32-bytes!!",
    lifetime_minutes: int = 15,
    refresh_secret: str | None = "test-refresh-secret-minimum-32-bytes!",
) -> None:
    monkeypatch.setenv("JWT_SECRET_KEY", jwt_secret)
    monkeypatch.setenv("ACCESS_TOKEN_LIFETIME_MINUTES", str(lifetime_minutes))
    if refresh_secret is None:
        monkeypatch.delenv("REFRESH_TOKEN_HASH_SECRET", raising=False)
    else:
        monkeypatch.setenv("REFRESH_TOKEN_HASH_SECRET", refresh_secret)
    get_settings.cache_clear()


def test_create_access_token_returns_non_empty_jwt(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)
    token = create_access_token(user_id=uuid4(), organization_id=uuid4())

    assert isinstance(token, str)
    assert token


def test_decoded_payload_contains_correct_user_id(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)
    user_id = uuid4()
    organization_id = uuid4()

    token = create_access_token(user_id=user_id, organization_id=organization_id)
    payload = decode_access_token(token)

    assert payload is not None
    assert payload.user_id == user_id


def test_decoded_payload_contains_correct_organization_id(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)
    user_id = uuid4()
    organization_id = uuid4()

    token = create_access_token(user_id=user_id, organization_id=organization_id)
    payload = decode_access_token(token)

    assert payload is not None
    assert payload.organization_id == organization_id


def test_issued_at_and_expires_at_are_timezone_aware(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)
    token = create_access_token(user_id=uuid4(), organization_id=uuid4())

    payload = decode_access_token(token)

    assert payload is not None
    assert payload.issued_at.tzinfo is not None
    assert payload.expires_at.tzinfo is not None


def test_custom_expires_delta_is_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)
    token = create_access_token(
        user_id=uuid4(),
        organization_id=uuid4(),
        expires_delta=timedelta(minutes=2),
    )

    payload = decode_access_token(token)

    assert payload is not None
    assert int((payload.expires_at - payload.issued_at).total_seconds()) == 120


def test_expired_token_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)
    token = create_access_token(
        user_id=uuid4(),
        organization_id=uuid4(),
        expires_delta=timedelta(seconds=-1),
    )

    assert decode_access_token(token) is None


def test_malformed_token_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)

    assert decode_access_token("not-a-jwt") is None


def test_token_signed_with_wrong_secret_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch, jwt_secret="expected-secret-minimum-32-bytes!!")

    wrong_secret_token = jwt.encode(
        {
            "sub": str(uuid4()),
            "organization_id": str(uuid4()),
            "iat": 1700000000,
            "exp": 4700000000,
            "type": "access",
        },
        "wrong-secret-minimum-32-bytes!!!!!",
        algorithm="HS256",
    )

    assert decode_access_token(wrong_secret_token) is None


def test_wrong_token_type_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)
    settings = get_settings()

    wrong_type_token = jwt.encode(
        {
            "sub": str(uuid4()),
            "organization_id": str(uuid4()),
            "iat": 1700000000,
            "exp": 4700000000,
            "type": "refresh",
        },
        settings.jwt_secret_key,
        algorithm="HS256",
    )

    assert decode_access_token(wrong_type_token) is None


def test_empty_token_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)

    assert decode_access_token("") is None


def test_generated_refresh_token_is_non_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)

    token = generate_refresh_token()

    assert isinstance(token, str)
    assert token


def test_two_generated_refresh_tokens_differ(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)

    token_one = generate_refresh_token()
    token_two = generate_refresh_token()

    assert token_one != token_two


def test_generated_refresh_token_has_sufficient_length(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)

    token = generate_refresh_token()

    assert len(token) >= 64


def test_hash_refresh_token_differs_from_plaintext(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)

    refresh_token = generate_refresh_token()
    digest = hash_refresh_token(refresh_token)

    assert digest != refresh_token


def test_same_refresh_token_produces_same_hmac_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch, refresh_secret="shared-secret")

    refresh_token = "fixed-refresh-token"
    digest_one = hash_refresh_token(refresh_token)
    digest_two = hash_refresh_token(refresh_token)

    assert digest_one == digest_two


def test_correct_refresh_token_verifies(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)

    refresh_token = "refresh-token-to-verify"
    digest = hash_refresh_token(refresh_token)

    assert verify_refresh_token_hash(refresh_token, digest) is True


def test_wrong_refresh_token_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)

    digest = hash_refresh_token("token-a")

    assert verify_refresh_token_hash("token-b", digest) is False


def test_malformed_hash_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)

    assert verify_refresh_token_hash("some-token", "not-hex") is False


def test_empty_token_hashing_raises_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)

    with pytest.raises(ValueError):
        hash_refresh_token("")


def test_empty_verification_inputs_return_false(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch)

    assert verify_refresh_token_hash("", "abc") is False
    assert verify_refresh_token_hash("abc", "") is False


def test_settings_repr_does_not_expose_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch, jwt_secret="jwt-super-secret", refresh_secret="refresh-super-secret")

    settings = get_settings()
    rendered = repr(settings)

    assert "jwt-super-secret" not in rendered
    assert "refresh-super-secret" not in rendered


def test_refresh_token_hash_secret_fallback_behavior(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch, jwt_secret="fallback-jwt-secret", refresh_secret=None)

    settings = get_settings()

    assert settings.refresh_token_hash_secret == settings.jwt_secret_key


def test_access_token_lifetime_applied_from_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_token_env(monkeypatch, lifetime_minutes=42)

    token = create_access_token(user_id=uuid4(), organization_id=uuid4())
    payload = decode_access_token(token)

    assert payload is not None
    assert int((payload.expires_at - payload.issued_at).total_seconds()) == 42 * 60
