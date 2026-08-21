from datetime import datetime, timedelta, timezone
import hashlib
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from application.ports.github_app import (
    GitHubBranchReference,
    GitHubCommitReference,
    GitHubGitTree,
    GitHubGitTreeEntry,
    GitHubInstallationAccessToken,
    GitHubRawBlob,
    GitHubRepository,
    GitHubRepositoryAccessGrant,
)
from application.services.github_repository_content_service import (
    GitHubRepositoryContentAuthorization,
    GitHubRepositoryContentConflict,
    GitHubRepositoryContentNotFound,
    GitHubRepositoryContentRejected,
    GitHubRepositoryContentService,
    GitHubRepositorySnapshot,
    MAX_GITHUB_TREE_ENTRIES,
)
from infrastructure.db.models import ConnectorScope
from infrastructure.repositories.connector_credential_repository import CredentialMetadata
from infrastructure.repositories.github_app_installation_repository import GitHubInstallationView


NOW = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
COMMIT_1 = "a" * 40
COMMIT_2 = "b" * 64
TREE_1 = "c" * 40
TREE_2 = "d" * 64
BLOB_1 = "e" * 40


def repository(default_branch="main"):
    return GitHubRepository(
        501, "docs", "fake-org/docs", "fake-org", True, "private",
        False, False, default_branch, "https://github.com/fake-org/docs", NOW,
    )


class Client:
    app_id = 123

    def __init__(self):
        self.branches = []
        self.commit_trees = {COMMIT_1: TREE_1, COMMIT_2: TREE_2}
        self.tree = GitHubGitTree(TREE_1, (), False)
        self.raw = GitHubRawBlob(b"hello", 5, hashlib.sha256(b"hello").hexdigest())
        self.token_calls = []
        self.branch_calls = []
        self.commit_calls = []
        self.tree_calls = []
        self.blob_calls = []

    def create_repository_content_access_token(
        self, installation_id, repository_id, *, account_id, account_login
    ):
        self.token_calls.append((installation_id, repository_id, account_id, account_login))
        return GitHubRepositoryAccessGrant(
            GitHubInstallationAccessToken("ghs_request_scoped", NOW + timedelta(hours=1)),
            repository(),
        )

    def get_branch_reference(self, token, **kwargs):
        self.branch_calls.append(kwargs)
        return self.branches.pop(0)

    def get_commit_reference(self, token, *, commit_object_id, **kwargs):
        self.commit_calls.append((commit_object_id, kwargs))
        return GitHubCommitReference(commit_object_id, self.commit_trees[commit_object_id])

    def get_tree(self, token, **kwargs):
        self.tree_calls.append(kwargs)
        return self.tree

    def download_blob(self, token, **kwargs):
        self.blob_calls.append(kwargs)
        return self.raw


def authorization():
    return GitHubRepositoryContentAuthorization(
        uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), 77, 123, 99,
        "fake-org", 501, "docs", "fake-org/docs", "fake-org",
        "github:repository:501", "main",
    )


def snapshot(auth):
    return GitHubRepositorySnapshot(
        auth.connector_id, auth.scope_id, 501, "github:repository:501",
        "main", COMMIT_1, TREE_1,
    )


def service(client=None, *, in_transaction=False):
    session = Mock()
    session.in_transaction.return_value = in_transaction
    return GitHubRepositoryContentService(session, client or Client()), session


def scope(auth, **changes):
    config = {
        "repository_id": 501,
        "repository_name": "docs",
        "repository_full_name": "fake-org/docs",
        "owner_login": "fake-org",
        "private": True,
        "visibility": "private",
        "archived": False,
        "disabled": False,
        "default_branch": "main",
    }
    config.update(changes.pop("config", {}))
    values = dict(
        id=auth.scope_id, organization_id=auth.organization_id,
        connector_id=auth.connector_id, knowledge_space_id=auth.knowledge_space_id,
        display_name="fake-org/docs", slug="github-repository-501",
        scope_type="repository", external_scope_key="github:repository:501",
        access_mode="platform_managed", status="active", safe_config=config,
        config_schema_version=1, created_by_user_id=uuid4(), last_validated_at=NOW,
        created_at=NOW, updated_at=NOW, removed_at=None,
    )
    values.update(changes)
    return ConnectorScope(**values)


def configured_authorization_service():
    auth = authorization()
    value, session = service()
    credential = CredentialMetadata(
        auth.credential_id, auth.connector_id, "github", "app_installation", "active",
        "77", "fake-org", ("contents:read", "metadata:read"), None, NOW, None,
        NOW, NOW,
    )
    installation = GitHubInstallationView(
        auth.connector_id, auth.credential_id, 123, 77, 99, "fake-org",
        "Organization", "selected", "connected", NOW, NOW, NOW, None, NOW, NOW,
    )
    value._connectors = Mock()
    value._scopes = Mock()
    value._credentials = Mock()
    value._installations = Mock()
    value._connectors.get_by_id.return_value = SimpleNamespace(
        connector_type="github", status="active"
    )
    value._scopes.get_by_id.return_value = scope(auth)
    value._credentials.get.return_value = credential
    value._installations.get.return_value = installation
    session.execute.return_value.scalar_one_or_none.return_value = auth.knowledge_space_id
    return value, session, auth


def test_authorize_copies_only_validated_active_tenant_scope_identifiers():
    value, _, auth = configured_authorization_service()
    result = value.authorize(auth.organization_id, auth.connector_id, auth.scope_id)
    assert result == auth
    assert "fake-org" not in repr(result) and "77" not in repr(result)
    value._scopes.get_by_id.assert_called_once_with(auth.organization_id, auth.scope_id)


@pytest.mark.parametrize(("change", "error"), (
    ("missing_scope", GitHubRepositoryContentNotFound),
    ("wrong_provider", GitHubRepositoryContentRejected),
    ("inactive_scope", GitHubRepositoryContentConflict),
    ("connector_mismatch", GitHubRepositoryContentNotFound),
    ("credential_mismatch", GitHubRepositoryContentConflict),
    ("repository_identity", GitHubRepositoryContentConflict),
    ("owner_mismatch", GitHubRepositoryContentConflict),
))
def test_authorize_rejects_cross_tenant_and_stale_boundaries(change, error):
    value, _, auth = configured_authorization_service()
    if change == "missing_scope":
        value._scopes.get_by_id.return_value = None
    elif change == "wrong_provider":
        value._connectors.get_by_id.return_value.connector_type = "local_folder"
    elif change == "inactive_scope":
        value._scopes.get_by_id.return_value.status = "paused"
    elif change == "connector_mismatch":
        value._scopes.get_by_id.return_value.connector_id = uuid4()
    elif change == "credential_mismatch":
        value._installations.get.return_value = SimpleNamespace(
            **{**value._installations.get.return_value.__dict__, "credential_id": uuid4()}
        )
    elif change == "repository_identity":
        value._scopes.get_by_id.return_value.external_scope_key = "github:repository:502"
    elif change == "owner_mismatch":
        value._scopes.get_by_id.return_value.safe_config["owner_login"] = "other-org"
        value._scopes.get_by_id.return_value.safe_config["repository_full_name"] = "other-org/docs"
    with pytest.raises(error):
        value.authorize(auth.organization_id, auth.connector_id, auth.scope_id)


def test_provider_access_requires_caller_to_end_database_transaction():
    client = Client()
    value, _ = service(client, in_transaction=True)
    with pytest.raises(GitHubRepositoryContentConflict, match="transaction"):
        value.resolve_default_branch_snapshot(authorization())
    assert client.token_calls == []


def test_snapshot_is_immutable_supports_sha1_and_sha256_and_uses_one_token():
    client = Client()
    client.branches = [
        GitHubBranchReference("main", COMMIT_2, TREE_2),
        GitHubBranchReference("main", COMMIT_2, TREE_2),
    ]
    value, _ = service(client)
    result = value.resolve_default_branch_snapshot(authorization())
    assert result.commit_object_id == COMMIT_2 and result.root_tree_object_id == TREE_2
    assert len(client.token_calls) == 1 and len(client.branch_calls) == 2
    assert len(client.commit_calls) == 1


def test_snapshot_retries_one_branch_move_then_succeeds():
    client = Client()
    client.branches = [
        GitHubBranchReference("main", COMMIT_1, TREE_1),
        GitHubBranchReference("main", COMMIT_2, TREE_2),
        GitHubBranchReference("main", COMMIT_2, TREE_2),
        GitHubBranchReference("main", COMMIT_2, TREE_2),
    ]
    value, _ = service(client)
    assert value.resolve_default_branch_snapshot(authorization()).commit_object_id == COMMIT_2
    assert len(client.token_calls) == 1 and len(client.commit_calls) == 2


def test_snapshot_rejects_repeated_movement_and_mixed_commit_tree():
    client = Client()
    client.branches = [
        GitHubBranchReference("main", COMMIT_1, TREE_1),
        GitHubBranchReference("main", COMMIT_2, TREE_2),
        GitHubBranchReference("main", COMMIT_1, TREE_1),
        GitHubBranchReference("main", COMMIT_2, TREE_2),
    ]
    value, _ = service(client)
    with pytest.raises(GitHubRepositoryContentConflict, match="moved"):
        value.resolve_default_branch_snapshot(authorization())
    client = Client()
    client.commit_trees[COMMIT_1] = TREE_2
    client.branches = [GitHubBranchReference("main", COMMIT_1, TREE_1)] * 2
    value, _ = service(client)
    with pytest.raises(GitHubRepositoryContentRejected, match="inconsistent"):
        value.resolve_default_branch_snapshot(authorization())


def test_tree_is_non_recursive_ordered_and_skips_symlink_and_submodule():
    auth = authorization()
    client = Client()
    client.tree = GitHubGitTree(TREE_1, (
        GitHubGitTreeEntry("folder", "040000", "tree", TREE_2, None),
        GitHubGitTreeEntry("readme.md", "100644", "blob", BLOB_1, 5),
        GitHubGitTreeEntry("run.txt", "100755", "blob", "f" * 40, 7),
        GitHubGitTreeEntry("link", "120000", "blob", "1" * 40, 4),
        GitHubGitTreeEntry("module", "160000", "commit", "2" * 40, None),
    ), False)
    value, _ = service(client)
    page = value.list_tree(auth, snapshot(auth), snapshot(auth).root_tree())
    assert [entry.name for entry in page.entries] == ["folder", "readme.md", "run.txt"]
    assert page.entries[0].entry_type == "tree"
    assert page.entries[2].executable is True
    assert client.tree_calls[0]["tree_object_id"] == TREE_1
    assert len(client.tree_calls) == 1 and len(client.token_calls) == 1


@pytest.mark.parametrize("entries,truncated,message", (
    ((), True, "truncated"),
    ((GitHubGitTreeEntry("../bad", "100644", "blob", BLOB_1, 1),), False, "path"),
    ((GitHubGitTreeEntry("bad\\name", "100644", "blob", BLOB_1, 1),), False, "path"),
    ((GitHubGitTreeEntry("bad\x00name", "100644", "blob", BLOB_1, 1),), False, "path"),
    ((GitHubGitTreeEntry("special", "100600", "blob", BLOB_1, 1),), False, "unsupported"),
    ((GitHubGitTreeEntry("same", "100644", "blob", BLOB_1, 1),
      GitHubGitTreeEntry("same", "100644", "blob", BLOB_1, 1)), False, "duplicate"),
    ((GitHubGitTreeEntry("é.md", "100644", "blob", BLOB_1, 1),
      GitHubGitTreeEntry("e\u0301.md", "100644", "blob", "f" * 40, 1)), False, "duplicate"),
))
def test_tree_rejects_truncation_malformed_paths_modes_and_collisions(
    entries, truncated, message
):
    auth = authorization()
    client = Client()
    client.tree = GitHubGitTree(TREE_1, entries, truncated)
    value, _ = service(client)
    with pytest.raises(GitHubRepositoryContentRejected, match=message):
        value.list_tree(auth, snapshot(auth), snapshot(auth).root_tree())


def test_tree_rejects_platform_count_and_guessed_root_identity():
    auth = authorization()
    client = Client()
    client.tree = GitHubGitTree(
        TREE_1,
        tuple(
            GitHubGitTreeEntry(f"f{i}.txt", "100644", "blob", BLOB_1, 1)
            for i in range(MAX_GITHUB_TREE_ENTRIES + 1)
        ),
        False,
    )
    value, _ = service(client)
    with pytest.raises(GitHubRepositoryContentRejected, match="too large"):
        value.list_tree(auth, snapshot(auth), snapshot(auth).root_tree())
    forged = snapshot(auth).root_tree()
    object.__setattr__(forged, "object_id", TREE_2)
    with pytest.raises(GitHubRepositoryContentRejected, match="descriptor"):
        value.list_tree(auth, snapshot(auth), forged)


def blob_entry(auth, *, path="readme.md", size=5):
    page_client = Client()
    page_client.tree = GitHubGitTree(
        TREE_1, (GitHubGitTreeEntry(path, "100644", "blob", BLOB_1, size),), False
    )
    value, _ = service(page_client)
    return value.list_tree(auth, snapshot(auth), snapshot(auth).root_tree()).entries[0]


def test_blob_preserves_exact_bytes_and_returns_platform_sha256():
    auth = authorization()
    content = "line one\nλ\x00".encode()
    client = Client()
    client.raw = GitHubRawBlob(content, len(content), hashlib.sha256(content).hexdigest())
    value, _ = service(client)
    result = value.download_blob(auth, snapshot(auth), blob_entry(auth, size=len(content)))
    assert result.content == content and result.byte_count == len(content)
    assert result.sha256 == hashlib.sha256(content).hexdigest()
    assert content.decode(errors="ignore") not in repr(result)
    assert client.blob_calls[0]["maximum_bytes"] == 10 * 1024 * 1024
    assert len(client.token_calls) == 1


def test_blob_rejects_unsupported_policy_size_mismatch_lfs_and_context_mismatch():
    auth = authorization()
    value, client = service()[0], None
    with pytest.raises(GitHubRepositoryContentRejected, match="unsupported"):
        value.download_blob(auth, snapshot(auth), blob_entry(auth, path="code.py"))
    client = Client()
    client.raw = GitHubRawBlob(b"tiny", 4, hashlib.sha256(b"tiny").hexdigest())
    value, _ = service(client)
    with pytest.raises(GitHubRepositoryContentRejected, match="size"):
        value.download_blob(auth, snapshot(auth), blob_entry(auth, size=5))
    pointer = _lfs_pointer()
    client = Client()
    client.raw = GitHubRawBlob(pointer, len(pointer), hashlib.sha256(pointer).hexdigest())
    value, _ = service(client)
    with pytest.raises(GitHubRepositoryContentRejected, match="LFS"):
        value.download_blob(auth, snapshot(auth), blob_entry(auth, size=len(pointer)))
    other = snapshot(auth)
    object.__setattr__(other, "commit_object_id", COMMIT_2)
    with pytest.raises(GitHubRepositoryContentRejected, match="descriptor"):
        value.download_blob(auth, other, blob_entry(auth))


def _lfs_pointer():
    return (
        b"version https://git-lfs.github.com/spec/v1\n"
        b"oid sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n"
        b"size 123\n"
    )
