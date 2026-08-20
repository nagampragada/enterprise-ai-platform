from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
import re
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from google.api_core import exceptions as google_exceptions
import google_crc32c
import httpx
import jwt
import pytest

from app.config import GitHubAppSettings, GoogleSecretManagerSettings
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
from application.services.oauth_authorization_service import (
    LockedOAuthAuthorization,
    OAuthAuthorizationService,
)
from infrastructure.secrets.google_secret_manager import (
    MAX_NAME_COLLISION_ATTEMPTS,
    MAX_PAYLOAD_BYTES,
    PROVIDER_CALL_TIMEOUT_SECONDS,
    READ_MAX_ATTEMPTS,
    READ_RETRY_DEADLINE_SECONDS,
    GoogleSecretManagerSecretStore,
)
from infrastructure.connectors.github.client import GitHubAppRestClient


PROJECT = "platform-prod-1"
PREFIX = "eap"
ENVIRONMENT = "production"
TOKEN = "a" * 32
SECRET_ID = f"{PREFIX}-sm-{TOKEN}"
SECRET_NAME = f"projects/{PROJECT}/secrets/{SECRET_ID}"
VERSION_NAME = f"{SECRET_NAME}/versions/1"
REFERENCE = f"gcp-secret-manager://{VERSION_NAME}"
NOW = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
LABELS = {
    "managed-by": "enterprise-ai-platform",
    "eap-secret-policy": "single-version",
    "environment": ENVIRONMENT,
}


class FakeSecretManagerClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict, object, float]] = []
        self.secrets: dict[str, dict[str, object]] = {}
        self.create_errors: deque[Exception] = deque()
        self.add_error: Exception | None = None
        self.access_errors: deque[Exception] = deque()
        self.get_error: Exception | None = None
        self.destroy_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.checksum_acknowledged = True

    def _record(self, method, request, retry, timeout):
        self.calls.append((method, request, retry, timeout))

    def create_secret(self, *, request, retry, timeout):
        self._record("create", request, retry, timeout)
        if self.create_errors:
            raise self.create_errors.popleft()
        name = f"{request['parent']}/secrets/{request['secret_id']}"
        if name in self.secrets:
            raise google_exceptions.AlreadyExists("unsafe collision details")
        self.secrets[name] = {"labels": dict(request["secret"]["labels"]), "versions": {}}
        return SimpleNamespace(name=name)

    def add_secret_version(self, *, request, retry, timeout):
        self._record("add", request, retry, timeout)
        if self.add_error is not None:
            raise self.add_error
        item = self.secrets[request["parent"]]
        versions = item["versions"]
        version = len(versions) + 1
        versions[version] = bytes(request["payload"]["data"])
        return SimpleNamespace(
            name=f"{request['parent']}/versions/{version}",
            client_specified_payload_checksum=self.checksum_acknowledged,
        )

    def access_secret_version(self, *, request, retry, timeout):
        self._record("access", request, retry, timeout)
        if self.access_errors:
            raise self.access_errors.popleft()
        secret_name, _, version_text = request["name"].rpartition("/versions/")
        try:
            data = self.secrets[secret_name]["versions"][int(version_text)]
        except KeyError as exc:
            raise google_exceptions.NotFound("unsafe missing details") from exc
        return SimpleNamespace(
            name=request["name"],
            payload=SimpleNamespace(data=data, data_crc32c=google_crc32c.value(data)),
        )

    def get_secret(self, *, request, retry, timeout):
        self._record("get", request, retry, timeout)
        if self.get_error is not None:
            raise self.get_error
        try:
            item = self.secrets[request["name"]]
        except KeyError as exc:
            raise google_exceptions.NotFound("unsafe missing details") from exc
        return SimpleNamespace(name=request["name"], labels=dict(item["labels"]))

    def destroy_secret_version(self, *, request, retry, timeout):
        self._record("destroy", request, retry, timeout)
        if self.destroy_error is not None:
            raise self.destroy_error
        secret_name, _, version_text = request["name"].rpartition("/versions/")
        try:
            del self.secrets[secret_name]["versions"][int(version_text)]
        except KeyError as exc:
            raise google_exceptions.NotFound("unsafe missing details") from exc
        return SimpleNamespace(name=request["name"])

    def delete_secret(self, *, request, retry, timeout):
        self._record("delete", request, retry, timeout)
        if self.delete_error is not None:
            raise self.delete_error
        try:
            del self.secrets[request["name"]]
        except KeyError as exc:
            raise google_exceptions.NotFound("unsafe missing details") from exc


def settings(**overrides) -> GoogleSecretManagerSettings:
    values = {
        "project_id": PROJECT,
        "secret_prefix": PREFIX,
        "environment": ENVIRONMENT,
    }
    values.update(overrides)
    return GoogleSecretManagerSettings(**values)


def store(client=None, *, tokens=(TOKEN,), **kwargs):
    values = iter(tokens)
    return GoogleSecretManagerSecretStore(
        settings(),
        client=client or FakeSecretManagerClient(),
        token_factory=lambda _: next(values),
        sleeper=kwargs.pop("sleeper", lambda _: None),
        jitter=kwargs.pop("jitter", lambda _low, high: high),
        **kwargs,
    )


def provision(client: FakeSecretManagerClient, name=SECRET_NAME, data=b"hidden") -> None:
    client.secrets[name] = {"labels": dict(LABELS), "versions": {1: data}}


@pytest.mark.parametrize(
    "value",
    (
        "gcp-secret-manager://projects/another-prod-1/secrets/eap-sm-" + TOKEN + "/versions/1",
        "gcp-secret-manager://projects/platform-prod-1/secrets/other-sm-" + TOKEN + "/versions/1",
        "gcp-secret-manager://projects/platform-prod-1/secrets/eap-sm-" + TOKEN + "/versions/latest",
        "gcp-secret-manager://projects/platform-prod-1/secrets/eap-sm-" + TOKEN + "/versions/alias",
        "gcp-secret-manager://projects/platform-prod-1/secrets/eap-sm-" + TOKEN,
        REFERENCE + "?version=2",
        REFERENCE + "#fragment",
        REFERENCE + "/extra",
        REFERENCE.replace("/secrets/", "/secrets/%65"),
        REFERENCE.replace("/versions/1", "/versions/01"),
        REFERENCE.replace("/versions/1", "/versions/0"),
        REFERENCE.replace("gcp-secret-manager", "GCP-secret-manager"),
    ),
)
def test_reference_parser_rejects_noncanonical_attacker_controlled_values_before_calls(value):
    client = FakeSecretManagerClient()
    adapter = store(client)
    with pytest.raises(InvalidSecretReference, match="secret reference is invalid") as caught:
        adapter.retrieve(SecretReference(value))
    assert client.calls == []
    assert value not in str(caught.value)


def test_store_returns_numeric_version_pinned_reference_and_safe_random_name():
    client = FakeSecretManagerClient()
    adapter = store(client)
    reference = adapter.store(SecretValue("secret payload"))
    assert reference.value == REFERENCE
    create_request = client.calls[0][1]
    assert create_request["secret_id"] == SECRET_ID
    assert create_request["secret"]["labels"] == LABELS
    assert all(
        unsafe not in create_request["secret_id"]
        for unsafe in ("organization", "connector", "user", "email", "repository", "payload")
    )
    assert re.fullmatch(r"eap-sm-[0-9a-f]{32}", create_request["secret_id"])
    assert repr(reference).find(reference.value) == -1
    assert repr(adapter).find(PROJECT) == -1


def test_store_and_retrieve_preserve_unicode_and_multiline_pem_exactly_with_crc32c():
    value = "-----BEGIN PRIVATE KEY-----\nπ-secret\r\nline-three\n-----END PRIVATE KEY-----\n"
    client = FakeSecretManagerClient()
    adapter = store(client)
    reference = adapter.store(SecretValue(value))
    add_request = next(call[1] for call in client.calls if call[0] == "add")
    encoded = value.encode("utf-8")
    assert add_request["payload"] == {
        "data": encoded,
        "data_crc32c": google_crc32c.value(encoded),
    }
    assert adapter.retrieve(reference).value == value


def test_payload_limit_is_enforced_before_provider_call():
    client = FakeSecretManagerClient()
    adapter = store(client)
    adapter.store(SecretValue("x" * MAX_PAYLOAD_BYTES))
    assert client.calls
    client.calls.clear()
    with pytest.raises(SecretStoreError, match="secret store operation failed"):
        adapter.store(SecretValue("x" * (MAX_PAYLOAD_BYTES + 1)))
    assert client.calls == []

    with pytest.raises(SecretStoreError, match="secret store operation failed") as caught:
        adapter.store(SecretValue("invalid-surrogate-\ud800"))
    assert "surrogate" not in str(caught.value)
    assert client.calls == []


def test_unacknowledged_write_checksum_fails_safely_and_cleans_container():
    client = FakeSecretManagerClient()
    client.checksum_acknowledged = False
    adapter = store(client)
    with pytest.raises(SecretStoreIntegrityError, match="integrity check failed"):
        adapter.store(SecretValue("hidden"))
    assert [call[0] for call in client.calls] == ["create", "add", "delete"]
    assert client.secrets == {}


def test_retrieve_validates_provider_crc32c_and_utf8_without_leaking_data():
    client = FakeSecretManagerClient()
    provision(client, data=b"FAKE-SECRET")
    adapter = store(client)
    original = client.access_secret_version

    def corrupted(**kwargs):
        result = original(**kwargs)
        result.payload.data_crc32c += 1
        return result

    client.access_secret_version = corrupted
    with pytest.raises(SecretStoreIntegrityError) as caught:
        adapter.retrieve(SecretReference(REFERENCE))
    assert "FAKE-SECRET" not in str(caught.value)

    client.access_secret_version = lambda **_kwargs: SimpleNamespace(
        name=VERSION_NAME,
        payload=SimpleNamespace(data=b"\xff", data_crc32c=google_crc32c.value(b"\xff")),
    )
    with pytest.raises(SecretStoreIntegrityError):
        adapter.retrieve(SecretReference(REFERENCE))


@pytest.mark.parametrize(
    ("provider_error", "expected", "message"),
    (
        (google_exceptions.NotFound("FAKE resource"), SecretNotFound, "secret was not found"),
        (google_exceptions.PermissionDenied("FAKE project"), SecretStoreAccessDenied, "secret store access was denied"),
        (google_exceptions.Unauthenticated("FAKE credential"), SecretStoreAccessDenied, "secret store access was denied"),
        (google_exceptions.DeadlineExceeded("FAKE deadline"), SecretStoreUnavailable, "secret store is unavailable"),
        (google_exceptions.ServiceUnavailable("FAKE response"), SecretStoreUnavailable, "secret store is unavailable"),
        (google_exceptions.ResourceExhausted("FAKE quota"), SecretStoreUnavailable, "secret store is unavailable"),
        (google_exceptions.FailedPrecondition("FAKE state"), SecretStoreError, "secret store operation failed"),
        (RuntimeError("FAKE provider body"), SecretStoreError, "secret store operation failed"),
    ),
)
def test_provider_failures_map_to_fixed_safe_messages(provider_error, expected, message):
    client = FakeSecretManagerClient()
    provision(client)
    repeat = READ_MAX_ATTEMPTS if isinstance(
        provider_error,
        (google_exceptions.DeadlineExceeded, google_exceptions.ServiceUnavailable, google_exceptions.ResourceExhausted),
    ) else 1
    client.access_errors.extend(type(provider_error)("FAKE unsafe details") for _ in range(repeat))
    adapter = store(client)
    with pytest.raises(expected) as caught:
        adapter.retrieve(SecretReference(REFERENCE))
    assert str(caught.value) == message
    assert "FAKE" not in str(caught.value)
    assert caught.value.__cause__ is not None


def test_transient_read_retries_are_three_attempts_with_deadline_timeout_and_jitter():
    client = FakeSecretManagerClient()
    provision(client)
    client.access_errors.extend(
        [google_exceptions.ServiceUnavailable("one"), google_exceptions.DeadlineExceeded("two")]
    )
    sleeps = []
    adapter = store(client, sleeper=sleeps.append)
    assert adapter.retrieve(SecretReference(REFERENCE)).value == "hidden"
    access_calls = [call for call in client.calls if call[0] == "access"]
    assert len(access_calls) == READ_MAX_ATTEMPTS
    assert len(sleeps) == READ_MAX_ATTEMPTS - 1
    assert sleeps == [0.1, 0.2]
    assert all(call[2] is None and 0 < call[3] <= PROVIDER_CALL_TIMEOUT_SECONDS for call in access_calls)
    assert READ_RETRY_DEADLINE_SECONDS == 12.0


def test_mutating_add_is_never_retried_and_partial_store_cleanup_is_bounded():
    client = FakeSecretManagerClient()
    client.add_error = google_exceptions.ServiceUnavailable("FAKE ambiguous write")
    adapter = store(client)
    with pytest.raises(SecretStoreUnavailable):
        adapter.store(SecretValue("hidden"))
    assert [call[0] for call in client.calls] == ["create", "add", "delete"]
    assert all(call[2] is None for call in client.calls)
    assert all(call[3] == PROVIDER_CALL_TIMEOUT_SECONDS for call in client.calls)


def test_name_collisions_use_three_distinct_names_then_stop_without_add():
    client = FakeSecretManagerClient()
    client.create_errors.extend(
        google_exceptions.AlreadyExists("FAKE collision")
        for _ in range(MAX_NAME_COLLISION_ATTEMPTS)
    )
    tokens = ("a" * 32, "b" * 32, "c" * 32)
    adapter = store(client, tokens=tokens)
    with pytest.raises(SecretStoreUnavailable) as caught:
        adapter.store(SecretValue("hidden"))
    creates = [call for call in client.calls if call[0] == "create"]
    assert len(creates) == MAX_NAME_COLLISION_ATTEMPTS
    assert len({call[1]["secret_id"] for call in creates}) == 3
    assert "collision" not in str(caught.value)
    assert all(call[0] != "add" for call in client.calls)


def test_delete_is_exact_label_verified_and_removes_only_referenced_version_then_container():
    client = FakeSecretManagerClient()
    provision(client)
    adapter = store(client)
    adapter.delete(SecretReference(REFERENCE))
    assert [call[0] for call in client.calls] == ["get", "destroy", "delete"]
    assert client.calls[0][1] == {"name": SECRET_NAME}
    assert client.calls[1][1] == {"name": VERSION_NAME}
    assert client.calls[2][1] == {"name": SECRET_NAME}
    assert client.secrets == {}
    assert all(call[0] not in {"list_secrets", "list_secret_versions"} for call in client.calls)


def test_delete_missing_secret_or_version_is_idempotent_and_never_broadens_scope():
    client = FakeSecretManagerClient()
    adapter = store(client)
    adapter.delete(SecretReference(REFERENCE))
    assert [call[0] for call in client.calls] == ["get"]

    client.calls.clear()
    provision(client)
    attacker_reference = SecretReference(REFERENCE.replace("/versions/1", "/versions/2"))
    adapter.delete(attacker_reference)
    assert [call[0] for call in client.calls] == ["get", "destroy"]
    assert SECRET_NAME in client.secrets and client.secrets[SECRET_NAME]["versions"] == {1: b"hidden"}


def test_delete_already_destroyed_version_and_missing_container_are_successful_cleanup():
    client = FakeSecretManagerClient()
    provision(client)
    client.destroy_error = google_exceptions.FailedPrecondition("already destroyed")
    adapter = store(client)
    adapter.delete(SecretReference(REFERENCE))
    assert [call[0] for call in client.calls] == ["get", "destroy", "delete"]
    assert client.secrets == {}

    client.calls.clear()
    adapter.delete(SecretReference(REFERENCE))
    assert [call[0] for call in client.calls] == ["get"]


def test_delete_rejects_unmanaged_container_before_destructive_provider_call():
    client = FakeSecretManagerClient()
    provision(client)
    client.secrets[SECRET_NAME]["labels"] = {"environment": ENVIRONMENT}
    adapter = store(client)
    with pytest.raises(InvalidSecretReference) as caught:
        adapter.delete(SecretReference(REFERENCE))
    assert [call[0] for call in client.calls] == ["get"]
    assert REFERENCE not in str(caught.value)


def test_missing_retrieve_differs_from_missing_delete():
    client = FakeSecretManagerClient()
    adapter = store(client)
    with pytest.raises(SecretNotFound):
        adapter.retrieve(SecretReference(REFERENCE))
    adapter.delete(SecretReference(REFERENCE))


def test_oauth_pkce_uses_adapter_store_retrieve_and_delete_lifecycle():
    client = FakeSecretManagerClient()
    adapter = store(client)
    service = OAuthAuthorizationService(
        Mock(),
        adapter,
        clock=lambda: NOW,
        state_factory=lambda: "s" * 64,
        verifier_factory=lambda: "v" * 64,
    )
    service._connectors = Mock()
    service._connectors.get_by_id.return_value = SimpleNamespace(status="active", connector_type="github")
    service._users = Mock()
    service._users.get_by_id.return_value = SimpleNamespace(status="active")
    service._transactions = Mock()
    transaction_id = uuid4()
    service._transactions.create.return_value = SimpleNamespace(
        id=transaction_id, expires_at=NOW + timedelta(minutes=10)
    )
    organization_id, connector_id, user_id = uuid4(), uuid4(), uuid4()
    service.prepare(
        organization_id,
        connector_id,
        user_id,
        provider_key="github",
        callback_identifier="github_app_installation",
    )
    reference = SecretReference(service._transactions.create.call_args.kwargs["pkce_reference"])
    locked = LockedOAuthAuthorization(
        transaction_id,
        organization_id,
        connector_id,
        user_id,
        "github",
        "github_app_installation",
        reference,
        NOW + timedelta(minutes=10),
        None,
        None,
    )
    assert service.retrieve_pkce_verifier(locked).value == "v" * 64
    service.delete_pkce_verifier(locked)
    assert client.secrets == {}


def test_github_client_secret_and_private_key_are_retrieved_through_adapter(monkeypatch):
    client = FakeSecretManagerClient()
    provision(client, data=b"oauth-client-secret")
    private_name = SECRET_NAME.replace(TOKEN, "b" * 32)
    provision(client, name=private_name, data=b"private-key-pem")
    adapter = store(client)
    github_settings = GitHubAppSettings(
        app_id=12345,
        app_slug="enterprise-ai",
        client_id="Iv1.client-id",
        client_secret_reference=SecretReference(REFERENCE),
        private_key_reference=SecretReference(REFERENCE.replace(TOKEN, "b" * 32)),
        callback_url="https://platform.test/api/v1/connectors/github/callback",
        setup_url="https://platform.test/api/v1/connectors/github/setup",
    )

    def handler(request):
        if request.url.path == "/login/oauth/access_token":
            assert "oauth-client-secret" in request.content.decode()
            return httpx.Response(200, json={"access_token": "ghu_hidden", "token_type": "bearer"})
        return httpx.Response(
            200,
            json={
                "id": 77,
                "app_id": 12345,
                "account": {"id": 99, "login": "safe-org", "type": "Organization"},
                "repository_selection": "selected",
                "permissions": {"contents": "read", "metadata": "read"},
                "created_at": "2026-08-20T10:00:00Z",
                "updated_at": "2026-08-20T11:00:00Z",
            },
        )

    monkeypatch.setattr(jwt, "encode", lambda *_args, **_kwargs: "fake.jwt")
    github = GitHubAppRestClient(
        github_settings,
        adapter,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        clock=lambda: NOW,
    )
    assert github.exchange_authorization_code("temporary-code", "v" * 64).value == "ghu_hidden"
    assert github.verify_installation(77).installation_id == 77
    accessed = [call[1]["name"] for call in client.calls if call[0] == "access"]
    assert accessed == [VERSION_NAME, f"{private_name}/versions/1"]
    assert "oauth-client-secret" not in repr(github)
    assert "private-key-pem" not in repr(adapter)
