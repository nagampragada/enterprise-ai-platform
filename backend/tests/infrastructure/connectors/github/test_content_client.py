from datetime import datetime, timezone
import hashlib
from unittest.mock import Mock

import httpx
import jwt
import pytest

from app.config import GitHubAppSettings
from application.ports.github_app import (
    GitHubInstallationAccessToken,
    GitHubProviderAuthenticationError,
    GitHubProviderMalformedResponseError,
    GitHubProviderRedirectError,
    GitHubProviderResponseTooLargeError,
    GitHubProviderUnavailableError,
)
from application.ports.secret_store import SecretReference, SecretValue
from infrastructure.connectors.github.client import (
    GitHubAppRestClient,
    MAX_RESPONSE_BYTES,
)


NOW = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
COMMIT = "a" * 40
TREE = "b" * 64
BLOB = "c" * 40


class Store:
    def __init__(self):
        self.retrieved = []

    def retrieve(self, reference):
        self.retrieved.append(reference.value)
        return SecretValue("FAKE TEST KEY")


def settings(**changes):
    values = dict(
        app_id=12345,
        app_slug="enterprise-ai-test",
        client_id="Iv1.client-id",
        client_secret_reference=SecretReference("fake://github-client-secret"),
        private_key_reference=SecretReference("fake://github-app-key"),
        callback_url="https://platform.example.test/api/v1/connectors/github/callback",
        setup_url="https://platform.example.test/api/v1/connectors/github/setup",
    )
    values.update(changes)
    return GitHubAppSettings(**values)


def repository():
    return {
        "id": 501,
        "name": "docs",
        "full_name": "fake-org/docs",
        "owner": {"id": 99, "login": "fake-org"},
        "private": True,
        "visibility": "private",
        "archived": False,
        "disabled": False,
        "default_branch": "main",
        "html_url": "https://github.com/fake-org/docs",
        "updated_at": "2026-08-20T11:00:00Z",
    }


def token_body():
    return {
        "token": "ghs_temporary",
        "expires_at": "2026-08-20T13:00:00Z",
        "permissions": {"contents": "read", "metadata": "read"},
        "repository_selection": "selected",
        "repositories": [repository()],
    }


def client(handler, **changes):
    store = Store()
    http = httpx.Client(transport=httpx.MockTransport(handler))
    value = GitHubAppRestClient(
        settings(**changes), store, http_client=http, clock=lambda: NOW, sleeper=Mock()
    )
    return value, store


def token():
    return GitHubInstallationAccessToken(
        "ghs_temporary", datetime(2026, 8, 20, 13, tzinfo=timezone.utc)
    )


def test_content_token_is_one_repository_metadata_and_contents_only(monkeypatch):
    monkeypatch.setattr(jwt, "encode", lambda *args, **kwargs: "fake.jwt")
    calls = []

    def handler(request):
        calls.append(request)
        assert request.method == "POST"
        assert request.url.path == "/app/installations/77/access_tokens"
        assert request.read() == (
            b'{"repository_ids":[501],"permissions":'
            b'{"contents":"read","metadata":"read"}}'
        )
        return httpx.Response(201, json=token_body())

    value, store = client(handler, max_retries=3)
    grant = value.create_repository_content_access_token(
        77, 501, account_id=99, account_login="fake-org"
    )
    assert grant.repository.repository_id == 501 and len(calls) == 1
    assert store.retrieved == ["fake://github-app-key"]
    assert "ghs_temporary" not in repr(grant)


def test_branch_commit_and_tree_contracts_are_exact_and_non_recursive():
    calls = []

    def handler(request):
        calls.append(request)
        assert request.headers["Authorization"] == "Bearer ghs_temporary"
        if "/branches/" in request.url.path:
            return httpx.Response(200, json={
                "name": "feature/docs",
                "commit": {"sha": COMMIT, "commit": {"tree": {"sha": TREE}}},
            })
        if "/git/commits/" in request.url.path:
            return httpx.Response(200, json={"sha": COMMIT, "tree": {"sha": TREE}})
        assert request.url.query == b""
        return httpx.Response(200, json={
            "sha": TREE,
            "truncated": False,
            "tree": [
                {"path": "README.md", "mode": "100644", "type": "blob",
                 "sha": BLOB, "size": 5, "url": "https://evil.test/ignored"},
            ],
        })

    value, _ = client(handler)
    branch = value.get_branch_reference(
        token(), owner_login="fake-org", repository_name="docs",
        branch_name="feature/docs",
    )
    commit = value.get_commit_reference(
        token(), owner_login="fake-org", repository_name="docs",
        commit_object_id=branch.commit_object_id,
    )
    tree = value.get_tree(
        token(), owner_login="fake-org", repository_name="docs",
        tree_object_id=commit.root_tree_object_id,
    )
    assert branch.root_tree_object_id == commit.root_tree_object_id == TREE
    assert tree.entries[0].name == "README.md" and not hasattr(tree.entries[0], "url")
    assert len(calls) == 3 and "recursive" not in str(calls[-1].url)


@pytest.mark.parametrize("object_id", (
    "A" * 40, "sha:" + "a" * 40, "a" * 39, "a" * 65, "a" * 20 + "/x",
))
def test_noncanonical_object_ids_are_rejected_before_http(object_id):
    calls = []
    value, _ = client(lambda request: calls.append(request))
    with pytest.raises(ValueError):
        value.get_tree(
            token(), owner_login="fake-org", repository_name="docs",
            tree_object_id=object_id,
        )
    assert calls == []


def test_malformed_or_oversized_tree_response_is_rejected():
    value, _ = client(lambda request: httpx.Response(
        200, json={"sha": TREE, "truncated": "false", "tree": []}
    ))
    with pytest.raises(GitHubProviderMalformedResponseError):
        value.get_tree(
            token(), owner_login="fake-org", repository_name="docs", tree_object_id=TREE
        )
    value, _ = client(lambda request: httpx.Response(
        200, content=b"x" * (MAX_RESPONSE_BYTES + 1)
    ))
    with pytest.raises(GitHubProviderUnavailableError):
        value.get_tree(
            token(), owner_login="fake-org", repository_name="docs", tree_object_id=TREE
        )


class CountingStream(httpx.SyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.consumed = 0

    def __iter__(self):
        for chunk in self.chunks:
            self.consumed += 1
            yield chunk


def test_json_response_stream_stops_when_raw_cap_is_exceeded():
    stream = CountingStream([b"x" * 700_000, b"y" * 400_000, b"must-not-read"])
    value, _ = client(lambda request: httpx.Response(200, stream=stream))
    with pytest.raises(GitHubProviderUnavailableError):
        value.get_tree(
            token(), owner_login="fake-org", repository_name="docs", tree_object_id=TREE
        )
    assert stream.consumed == 2


def test_blob_stream_is_byte_exact_incremental_and_missing_length_is_bounded():
    stream = CountingStream([b"line one\n", "λ".encode(), b"\x00binary"])
    value, _ = client(lambda request: httpx.Response(200, stream=stream))
    result = value.download_blob(
        token(), owner_login="fake-org", repository_name="docs",
        blob_object_id=BLOB, maximum_bytes=1024,
    )
    expected = b"line one\n" + "λ".encode() + b"\x00binary"
    assert result.content == expected and result.byte_count == len(expected)
    assert result.sha256 == hashlib.sha256(expected).hexdigest()
    assert expected.decode(errors="ignore") not in repr(result)


def test_blob_requires_identity_content_encoding_to_avoid_decompression_ambiguity():
    calls = []

    def handler(request):
        calls.append(request)
        assert request.headers["Accept-Encoding"] == "identity"
        return httpx.Response(200, headers={"Content-Encoding": "gzip"}, content=b"x")

    value, _ = client(handler)
    with pytest.raises(GitHubProviderMalformedResponseError):
        value.download_blob(
            token(), owner_login="fake-org", repository_name="docs",
            blob_object_id=BLOB, maximum_bytes=5,
        )
    assert len(calls) == 1


def test_blob_stops_on_streamed_overflow_and_rejects_large_content_length_early():
    stream = CountingStream([b"1234", b"5678", b"must-not-read"])
    value, _ = client(lambda request: httpx.Response(200, stream=stream))
    with pytest.raises(GitHubProviderResponseTooLargeError):
        value.download_blob(
            token(), owner_login="fake-org", repository_name="docs",
            blob_object_id=BLOB, maximum_bytes=5,
        )
    assert stream.consumed == 2
    stream = CountingStream([b"must-not-read"])
    value, _ = client(lambda request: httpx.Response(
        200, headers={"Content-Length": "6"}, stream=stream
    ))
    with pytest.raises(GitHubProviderResponseTooLargeError):
        value.download_blob(
            token(), owner_login="fake-org", repository_name="docs",
            blob_object_id=BLOB, maximum_bytes=5,
        )
    assert stream.consumed == 0


def test_blob_redirects_and_auth_failures_are_not_followed_or_retried():
    calls = []
    value, _ = client(lambda request: (
        calls.append(request) or httpx.Response(302, headers={"Location": "https://evil.test/x"})
    ), max_retries=3)
    with pytest.raises(GitHubProviderRedirectError):
        value.download_blob(
            token(), owner_login="fake-org", repository_name="docs",
            blob_object_id=BLOB, maximum_bytes=5,
        )
    assert len(calls) == 1
    calls.clear()
    value, _ = client(lambda request: calls.append(request) or httpx.Response(401), max_retries=3)
    with pytest.raises(GitHubProviderAuthenticationError):
        value.download_blob(
            token(), owner_login="fake-org", repository_name="docs",
            blob_object_id=BLOB, maximum_bytes=5,
        )
    assert len(calls) == 1


def test_blob_get_transient_retry_cap_is_three():
    calls = []
    value, _ = client(
        lambda request: calls.append(request) or httpx.Response(503), max_retries=3
    )
    value._sleep = Mock()
    with pytest.raises(GitHubProviderUnavailableError):
        value.download_blob(
            token(), owner_login="fake-org", repository_name="docs",
            blob_object_id=BLOB, maximum_bytes=5,
        )
    assert len(calls) == 3 and value._sleep.call_count == 2
