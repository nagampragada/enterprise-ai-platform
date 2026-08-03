from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, call
from uuid import UUID, uuid4

import pytest

from application.services.authentication_service import AuthenticationService, LoginResult
import application.services.authentication_service as auth_service_module


def _build_user(*, organization_id: UUID, status: str = "active") -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        organization_id=organization_id,
        email="user@example.com",
        normalized_email="user@example.com",
        password_hash="stored-password-hash",
        display_name="Example User",
        status=status,
        last_login_at=None,
    )


def _build_session(*, organization_id: UUID, user_id: UUID, revoked_at: datetime | None, expires_at: datetime) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        organization_id=organization_id,
        user_id=user_id,
        refresh_token_hash=b"stored-hash-bytes",
        revoked_at=revoked_at,
        expires_at=expires_at,
        last_used_at=None,
        ip_address=None,
        user_agent=None,
    )


@pytest.fixture()
def repos_and_service() -> tuple[AuthenticationService, Mock, Mock]:
    user_repository = Mock()
    authentication_session_repository = Mock()

    # Extra guard: service must never control transactions directly.
    user_repository.commit = Mock()
    user_repository.rollback = Mock()
    authentication_session_repository.commit = Mock()
    authentication_session_repository.rollback = Mock()

    service = AuthenticationService(
        user_repository=user_repository,
        authentication_session_repository=authentication_session_repository,
    )
    return service, user_repository, authentication_session_repository


def test_login_valid_active_user_creates_tokens_and_session(monkeypatch: pytest.MonkeyPatch, repos_and_service) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    user = _build_user(organization_id=organization_id, status="active")
    user_repository.get_by_normalized_email.return_value = user

    monkeypatch.setattr(auth_service_module, "verify_password", lambda password, password_hash: True)
    monkeypatch.setattr(auth_service_module, "create_access_token", lambda **kwargs: "access-token")
    monkeypatch.setattr(auth_service_module, "generate_refresh_token", lambda: "refresh-token")
    monkeypatch.setattr(auth_service_module, "hash_refresh_token", lambda token: f"hash:{token}")
    monkeypatch.setattr(auth_service_module, "_access_token_expires_delta", lambda: timedelta(minutes=5))

    result = service.login(
        organization_id=organization_id,
        email="  USER@EXAMPLE.COM  ",
        password="plaintext-password",
        ip_address="127.0.0.1",
        user_agent="pytest-agent",
    )

    user_repository.get_by_normalized_email.assert_called_once_with(
        organization_id=organization_id,
        normalized_email="user@example.com",
    )
    user_repository.update_last_login.assert_called_once()
    authentication_session_repository.add.assert_called_once()

    added_session = authentication_session_repository.add.call_args.args[0]
    assert added_session.organization_id == organization_id
    assert added_session.user_id == user.id
    assert added_session.refresh_token_hash == b"hash:refresh-token"
    assert added_session.refresh_token_hash != b"refresh-token"
    assert added_session.ip_address == "127.0.0.1"
    assert added_session.user_agent == "pytest-agent"
    assert added_session.created_at.tzinfo == timezone.utc
    assert added_session.last_used_at.tzinfo == timezone.utc
    assert added_session.expires_at.tzinfo == timezone.utc

    last_login_at = user_repository.update_last_login.call_args.kwargs["last_login_at"]
    assert last_login_at.tzinfo == timezone.utc

    assert isinstance(result, LoginResult)
    assert result is not None
    assert result.user.user_id == user.id
    assert result.user.organization_id == organization_id
    assert result.user.email == user.email
    assert result.user.display_name == user.display_name
    assert result.tokens.access_token == "access-token"
    assert result.tokens.refresh_token == "refresh-token"
    assert result.tokens.expires_in_seconds == 300


def test_login_unknown_user_returns_none(repos_and_service) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    user_repository.get_by_normalized_email.return_value = None

    result = service.login(organization_id=uuid4(), email="none@example.com", password="password")

    assert result is None
    authentication_session_repository.add.assert_not_called()


def test_login_incorrect_password_returns_none(monkeypatch: pytest.MonkeyPatch, repos_and_service) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    user_repository.get_by_normalized_email.return_value = _build_user(organization_id=organization_id)

    monkeypatch.setattr(auth_service_module, "verify_password", lambda password, password_hash: False)

    result = service.login(organization_id=organization_id, email="user@example.com", password="wrong")

    assert result is None
    authentication_session_repository.add.assert_not_called()


def test_login_inactive_user_returns_none(monkeypatch: pytest.MonkeyPatch, repos_and_service) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    user_repository.get_by_normalized_email.return_value = _build_user(
        organization_id=organization_id,
        status="disabled",
    )

    monkeypatch.setattr(auth_service_module, "verify_password", lambda password, password_hash: True)

    result = service.login(organization_id=organization_id, email="user@example.com", password="password")

    assert result is None
    authentication_session_repository.add.assert_not_called()


def test_failed_login_does_not_create_session(monkeypatch: pytest.MonkeyPatch, repos_and_service) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    user_repository.get_by_normalized_email.return_value = _build_user(organization_id=organization_id)
    monkeypatch.setattr(auth_service_module, "verify_password", lambda password, password_hash: False)

    result = service.login(organization_id=organization_id, email="user@example.com", password="wrong")

    assert result is None
    authentication_session_repository.add.assert_not_called()


def test_login_does_not_commit_or_rollback(repos_and_service) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    user_repository.get_by_normalized_email.return_value = None

    _ = service.login(organization_id=uuid4(), email="none@example.com", password="password")

    user_repository.commit.assert_not_called()
    user_repository.rollback.assert_not_called()
    authentication_session_repository.commit.assert_not_called()
    authentication_session_repository.rollback.assert_not_called()


def test_refresh_valid_token_rotates_hash_and_returns_new_tokens(
    monkeypatch: pytest.MonkeyPatch,
    repos_and_service,
) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    user = _build_user(organization_id=organization_id, status="active")
    session = _build_session(
        organization_id=organization_id,
        user_id=user.id,
        revoked_at=None,
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
    )

    authentication_session_repository.get_by_refresh_token_hash.return_value = session
    user_repository.get_by_id.return_value = user

    hash_mock = Mock(side_effect=lambda token: f"hash:{token}")
    monkeypatch.setattr(auth_service_module, "hash_refresh_token", hash_mock)
    monkeypatch.setattr(auth_service_module, "generate_refresh_token", lambda: "rotated-refresh-token")
    monkeypatch.setattr(auth_service_module, "create_access_token", lambda **kwargs: "rotated-access-token")
    monkeypatch.setattr(auth_service_module, "_access_token_expires_delta", lambda: timedelta(minutes=15))

    result = service.refresh("presented-refresh-token")

    hash_mock.assert_has_calls([call("presented-refresh-token"), call("rotated-refresh-token")])
    authentication_session_repository.get_by_refresh_token_hash.assert_called_once_with("hash:presented-refresh-token")
    user_repository.get_by_id.assert_called_once_with(
        organization_id=session.organization_id,
        user_id=session.user_id,
    )

    assert result is not None
    assert result.access_token == "rotated-access-token"
    assert result.refresh_token == "rotated-refresh-token"
    assert result.expires_in_seconds == 900

    assert session.refresh_token_hash == b"hash:rotated-refresh-token"
    assert session.refresh_token_hash != b"rotated-refresh-token"
    assert session.last_used_at is not None
    assert session.last_used_at.tzinfo == timezone.utc


def test_refresh_unknown_refresh_token_returns_none(monkeypatch: pytest.MonkeyPatch, repos_and_service) -> None:
    service, _, authentication_session_repository = repos_and_service

    monkeypatch.setattr(auth_service_module, "hash_refresh_token", lambda token: "hash:missing")
    authentication_session_repository.get_by_refresh_token_hash.return_value = None

    result = service.refresh("unknown-refresh-token")

    assert result is None


def test_refresh_revoked_session_returns_none(monkeypatch: pytest.MonkeyPatch, repos_and_service) -> None:
    service, _, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    session = _build_session(
        organization_id=organization_id,
        user_id=uuid4(),
        revoked_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
    )

    monkeypatch.setattr(auth_service_module, "hash_refresh_token", lambda token: "hash:revoked")
    authentication_session_repository.get_by_refresh_token_hash.return_value = session

    result = service.refresh("revoked-token")

    assert result is None


def test_refresh_expired_session_returns_none(monkeypatch: pytest.MonkeyPatch, repos_and_service) -> None:
    service, _, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    session = _build_session(
        organization_id=organization_id,
        user_id=uuid4(),
        revoked_at=None,
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    monkeypatch.setattr(auth_service_module, "hash_refresh_token", lambda token: "hash:expired")
    authentication_session_repository.get_by_refresh_token_hash.return_value = session

    result = service.refresh("expired-token")

    assert result is None


def test_refresh_missing_user_returns_none(monkeypatch: pytest.MonkeyPatch, repos_and_service) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    session = _build_session(
        organization_id=organization_id,
        user_id=uuid4(),
        revoked_at=None,
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )

    monkeypatch.setattr(auth_service_module, "hash_refresh_token", lambda token: "hash:present")
    authentication_session_repository.get_by_refresh_token_hash.return_value = session
    user_repository.get_by_id.return_value = None

    result = service.refresh("present-token")

    assert result is None


def test_refresh_inactive_user_returns_none(monkeypatch: pytest.MonkeyPatch, repos_and_service) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    inactive_user = _build_user(organization_id=organization_id, status="suspended")
    session = _build_session(
        organization_id=organization_id,
        user_id=inactive_user.id,
        revoked_at=None,
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )

    monkeypatch.setattr(auth_service_module, "hash_refresh_token", lambda token: "hash:present")
    authentication_session_repository.get_by_refresh_token_hash.return_value = session
    user_repository.get_by_id.return_value = inactive_user

    result = service.refresh("present-token")

    assert result is None


def test_refresh_never_assigns_raw_refresh_token_to_session(
    monkeypatch: pytest.MonkeyPatch,
    repos_and_service,
) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    user = _build_user(organization_id=organization_id, status="active")
    session = _build_session(
        organization_id=organization_id,
        user_id=user.id,
        revoked_at=None,
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
    )

    authentication_session_repository.get_by_refresh_token_hash.return_value = session
    user_repository.get_by_id.return_value = user

    monkeypatch.setattr(auth_service_module, "hash_refresh_token", lambda token: f"hash:{token}")
    monkeypatch.setattr(auth_service_module, "generate_refresh_token", lambda: "plain-rotated-token")
    monkeypatch.setattr(auth_service_module, "create_access_token", lambda **kwargs: "token")
    monkeypatch.setattr(auth_service_module, "_access_token_expires_delta", lambda: timedelta(minutes=1))

    _ = service.refresh("incoming-token")

    assert session.refresh_token_hash != b"plain-rotated-token"


def test_logout_returns_true_when_revoked(repos_and_service) -> None:
    service, _, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    session_id = uuid4()
    revoked_at = datetime.now(timezone.utc)
    authentication_session_repository.revoke.return_value = SimpleNamespace(id=session_id)

    result = service.logout(
        organization_id=organization_id,
        session_id=session_id,
        revoked_at=revoked_at,
    )

    assert result is True


def test_logout_returns_false_when_session_not_found(repos_and_service) -> None:
    service, _, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    session_id = uuid4()
    authentication_session_repository.revoke.return_value = None

    result = service.logout(
        organization_id=organization_id,
        session_id=session_id,
        revoked_at=datetime.now(timezone.utc),
    )

    assert result is False


def test_logout_all_returns_affected_row_count(repos_and_service) -> None:
    service, _, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    user_id = uuid4()
    revoked_at = datetime.now(timezone.utc)
    authentication_session_repository.revoke_all_for_user.return_value = 4

    result = service.logout_all(
        organization_id=organization_id,
        user_id=user_id,
        revoked_at=revoked_at,
    )

    assert result == 4


def test_generated_datetimes_are_timezone_aware_utc(monkeypatch: pytest.MonkeyPatch, repos_and_service) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    user = _build_user(organization_id=organization_id, status="active")
    user_repository.get_by_normalized_email.return_value = user

    monkeypatch.setattr(auth_service_module, "verify_password", lambda password, password_hash: True)
    monkeypatch.setattr(auth_service_module, "create_access_token", lambda **kwargs: "access-token")
    monkeypatch.setattr(auth_service_module, "generate_refresh_token", lambda: "refresh-token")
    monkeypatch.setattr(auth_service_module, "hash_refresh_token", lambda token: f"hash:{token}")
    monkeypatch.setattr(auth_service_module, "_access_token_expires_delta", lambda: timedelta(minutes=3))

    _ = service.login(organization_id=organization_id, email="user@example.com", password="password")

    login_session = authentication_session_repository.add.call_args.args[0]
    login_last_login = user_repository.update_last_login.call_args.kwargs["last_login_at"]

    refresh_session = _build_session(
        organization_id=organization_id,
        user_id=user.id,
        revoked_at=None,
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    authentication_session_repository.get_by_refresh_token_hash.return_value = refresh_session
    user_repository.get_by_id.return_value = user

    _ = service.refresh("refresh-token")

    assert login_session.created_at.tzinfo == timezone.utc
    assert login_session.expires_at.tzinfo == timezone.utc
    assert login_session.last_used_at.tzinfo == timezone.utc
    assert login_last_login.tzinfo == timezone.utc
    assert refresh_session.last_used_at is not None
    assert refresh_session.last_used_at.tzinfo == timezone.utc


def test_login_and_logout_preserve_tenant_scope(monkeypatch: pytest.MonkeyPatch, repos_and_service) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    session_id = uuid4()
    user = _build_user(organization_id=organization_id, status="active")
    user_repository.get_by_normalized_email.return_value = user
    authentication_session_repository.revoke.return_value = SimpleNamespace(id=session_id)

    monkeypatch.setattr(auth_service_module, "verify_password", lambda password, password_hash: True)
    monkeypatch.setattr(auth_service_module, "create_access_token", lambda **kwargs: "access-token")
    monkeypatch.setattr(auth_service_module, "generate_refresh_token", lambda: "refresh-token")
    monkeypatch.setattr(auth_service_module, "hash_refresh_token", lambda token: f"hash:{token}")
    monkeypatch.setattr(auth_service_module, "_access_token_expires_delta", lambda: timedelta(minutes=3))

    _ = service.login(
        organization_id=organization_id,
        email="Scoped@Example.com",
        password="password",
    )
    _ = service.logout(
        organization_id=organization_id,
        session_id=session_id,
        revoked_at=datetime.now(timezone.utc),
    )

    user_repository.get_by_normalized_email.assert_called_once_with(
        organization_id=organization_id,
        normalized_email="scoped@example.com",
    )
    authentication_session_repository.revoke.assert_called_once()
    assert authentication_session_repository.revoke.call_args.kwargs["organization_id"] == organization_id


def test_service_does_not_commit_or_rollback_across_flows(
    monkeypatch: pytest.MonkeyPatch,
    repos_and_service,
) -> None:
    service, user_repository, authentication_session_repository = repos_and_service
    organization_id = uuid4()
    user = _build_user(organization_id=organization_id, status="active")

    user_repository.get_by_normalized_email.return_value = user
    user_repository.get_by_id.return_value = user

    active_session = _build_session(
        organization_id=organization_id,
        user_id=user.id,
        revoked_at=None,
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    authentication_session_repository.get_by_refresh_token_hash.return_value = active_session
    authentication_session_repository.revoke.return_value = SimpleNamespace(id=uuid4())
    authentication_session_repository.revoke_all_for_user.return_value = 1

    monkeypatch.setattr(auth_service_module, "verify_password", lambda password, password_hash: True)
    monkeypatch.setattr(auth_service_module, "create_access_token", lambda **kwargs: "access-token")
    monkeypatch.setattr(auth_service_module, "generate_refresh_token", lambda: "refresh-token")
    monkeypatch.setattr(auth_service_module, "hash_refresh_token", lambda token: f"hash:{token}")
    monkeypatch.setattr(auth_service_module, "_access_token_expires_delta", lambda: timedelta(minutes=3))

    _ = service.login(organization_id=organization_id, email="user@example.com", password="password")
    _ = service.refresh("refresh-token")
    _ = service.logout(organization_id=organization_id, session_id=uuid4(), revoked_at=datetime.now(timezone.utc))
    _ = service.logout_all(organization_id=organization_id, user_id=user.id, revoked_at=datetime.now(timezone.utc))

    user_repository.commit.assert_not_called()
    user_repository.rollback.assert_not_called()
    authentication_session_repository.commit.assert_not_called()
    authentication_session_repository.rollback.assert_not_called()
