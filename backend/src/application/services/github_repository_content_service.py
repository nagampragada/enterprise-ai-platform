"""Tenant-bound, immutable GitHub content identity and bounded byte reads."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import PurePosixPath
import re
import unicodedata
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from application.ports.github_app import (
    GitHubAppClient,
    GitHubBranchReference,
    GitHubCommitReference,
    GitHubGitTree,
    GitHubInstallationAccessToken,
    GitHubProviderAuthenticationError,
    GitHubProviderAuthorizationError,
    GitHubProviderMalformedResponseError,
    GitHubProviderNotFoundError,
    GitHubProviderRateLimitError,
    GitHubProviderRedirectError,
    GitHubProviderResponseTooLargeError,
    GitHubProviderUnavailableError,
    GitHubRawBlob,
    GitHubRepositoryAccessGrant,
)
from application.ports.secret_store import SecretStoreError
from application.services.github_repository_selection_service import (
    EXPECTED_GITHUB_SCOPES,
    GitHubRepositorySelectionConflict,
    validated_github_repository_scope_config,
)
from infrastructure.db.models import KnowledgeSpace
from infrastructure.repositories.connector_credential_repository import (
    ConnectorCredentialRepository,
)
from infrastructure.repositories.connector_repository import ConnectorRepository
from infrastructure.repositories.connector_scope_repository import ConnectorScopeRepository
from infrastructure.repositories.github_app_installation_repository import (
    GitHubAppInstallationRepository,
)


MAX_GITHUB_TREE_ENTRIES = 1_000
MAX_GITHUB_BLOB_BYTES = 10 * 1024 * 1024
MAX_REPOSITORY_PATH_BYTES = 1_024
MAX_REPOSITORY_PATH_SEGMENT_BYTES = 255
MAX_REPOSITORY_PATH_SEGMENTS = 64
SUPPORTED_CONTENT_EXTENSIONS = frozenset({".pdf", ".docx", ".txt", ".md", ".markdown"})
_OBJECT_ID = re.compile(r"[0-9a-f]+")
_ACCOUNT_LOGIN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,98}[A-Za-z0-9])?")
_REPOSITORY_NAME = re.compile(r"[A-Za-z0-9_.-]{1,100}")
_LFS_SIGNATURE = b"version https://git-lfs.github.com/spec/v1"


class GitHubRepositoryContentNotFound(RuntimeError):
    pass


class GitHubRepositoryContentRejected(RuntimeError):
    pass


class GitHubRepositoryContentConflict(RuntimeError):
    pass


class GitHubRepositoryContentUnavailable(RuntimeError):
    pass


class GitHubRepositoryContentUnsupported(GitHubRepositoryContentRejected):
    """A valid pinned blob exists but is not safely indexable."""

    def __init__(self, classification: str) -> None:
        if classification != "git_lfs_unsupported":
            raise ValueError("unsupported GitHub content classification is invalid")
        super().__init__("GitHub blob content is unsupported")
        self.classification = classification


@dataclass(frozen=True, repr=False)
class GitHubRepositoryContentAuthorization:
    organization_id: UUID
    connector_id: UUID
    scope_id: UUID
    knowledge_space_id: UUID
    credential_id: UUID
    installation_id: int
    app_id: int
    account_id: int
    account_login: str
    repository_id: int
    repository_name: str
    repository_full_name: str
    owner_login: str
    canonical_repository_identity: str
    default_branch_name: str


@dataclass(frozen=True)
class GitHubRepositorySnapshot:
    connector_id: UUID
    scope_id: UUID
    repository_id: int
    canonical_repository_identity: str
    default_branch_name: str
    commit_object_id: str
    root_tree_object_id: str

    def root_tree(self) -> GitHubTreeDescriptor:
        return GitHubTreeDescriptor(
            self.connector_id,
            self.scope_id,
            self.repository_id,
            self.canonical_repository_identity,
            self.commit_object_id,
            self.root_tree_object_id,
            "",
            self.root_tree_object_id,
        )


@dataclass(frozen=True)
class GitHubTreeDescriptor:
    connector_id: UUID
    scope_id: UUID
    repository_id: int
    canonical_repository_identity: str
    commit_object_id: str
    root_tree_object_id: str
    path: str
    object_id: str


@dataclass(frozen=True)
class GitHubRepositoryEntry:
    connector_id: UUID
    scope_id: UUID
    repository_id: int
    canonical_repository_identity: str
    commit_object_id: str
    root_tree_object_id: str
    parent_tree_object_id: str
    name: str
    path: str
    entry_type: str
    object_id: str
    size_bytes: int | None
    executable: bool

    def as_tree(self) -> GitHubTreeDescriptor:
        if self.entry_type != "tree":
            raise GitHubRepositoryContentRejected("GitHub tree descriptor is invalid")
        return GitHubTreeDescriptor(
            self.connector_id,
            self.scope_id,
            self.repository_id,
            self.canonical_repository_identity,
            self.commit_object_id,
            self.root_tree_object_id,
            self.path,
            self.object_id,
        )


@dataclass(frozen=True)
class GitHubTreePage:
    tree: GitHubTreeDescriptor
    entries: tuple[GitHubRepositoryEntry, ...]


@dataclass(frozen=True, repr=False)
class GitHubBlobContent:
    content: bytes
    byte_count: int
    sha256: str


class GitHubRepositoryContentService:
    """Stage DB authorization separately from request-scoped provider access."""

    def __init__(self, session: Session, client: GitHubAppClient) -> None:
        self._session = session
        self._client = client
        self._connectors = ConnectorRepository(session)
        self._credentials = ConnectorCredentialRepository(session)
        self._installations = GitHubAppInstallationRepository(session)
        self._scopes = ConnectorScopeRepository(session)

    def authorize(
        self, organization_id: UUID, connector_id: UUID, scope_id: UUID
    ) -> GitHubRepositoryContentAuthorization:
        connector = self._connectors.get_by_id(organization_id, connector_id)
        scope = self._scopes.get_by_id(organization_id, scope_id)
        credential = self._credentials.get(organization_id, connector_id)
        installation = self._installations.get(organization_id, connector_id)
        if connector is None or scope is None:
            raise GitHubRepositoryContentNotFound("GitHub repository scope was not found")
        if connector.connector_type != "github":
            raise GitHubRepositoryContentRejected("GitHub repository content is unavailable")
        if connector.status != "active" or scope.status != "active":
            raise GitHubRepositoryContentConflict("GitHub repository scope is unavailable")
        if scope.connector_id != connector_id:
            raise GitHubRepositoryContentNotFound("GitHub repository scope was not found")
        try:
            config = validated_github_repository_scope_config(scope)
        except GitHubRepositorySelectionConflict as exc:
            raise GitHubRepositoryContentConflict("GitHub repository scope is invalid") from exc
        if self._session.execute(
            select(KnowledgeSpace.id).where(
                KnowledgeSpace.organization_id == organization_id,
                KnowledgeSpace.id == scope.knowledge_space_id,
                KnowledgeSpace.status == "active",
            )
        ).scalar_one_or_none() is None:
            raise GitHubRepositoryContentNotFound("knowledge space was not found")
        if credential is None or installation is None:
            raise GitHubRepositoryContentNotFound("GitHub installation was not found")
        credential_id = credential.credential_id
        if (
            credential.status != "active"
            or credential.provider_key != "github"
            or credential.auth_scheme != "app_installation"
            or credential.external_subject != str(installation.github_installation_id)
            or frozenset(credential.granted_scopes) != EXPECTED_GITHUB_SCOPES
            or credential.expires_at is not None
            or installation.connector_id != connector_id
            or installation.credential_id != credential_id
            or installation.status != "connected"
            or installation.disconnected_at is not None
            or installation.account_type != "Organization"
            or installation.repository_selection not in {"all", "selected"}
            or installation.github_app_id != self._client.app_id
            or installation.github_installation_id < 1
            or installation.account_id < 1
            or _ACCOUNT_LOGIN.fullmatch(installation.account_login) is None
            or config["owner_login"].casefold() != installation.account_login.casefold()
        ):
            raise GitHubRepositoryContentConflict("GitHub installation is unavailable")
        branch = config["default_branch"]
        if not isinstance(branch, str) or not branch:
            raise GitHubRepositoryContentNotFound("GitHub default branch is unavailable")
        repository_id = config["repository_id"]
        canonical = f"github:repository:{repository_id}"
        return GitHubRepositoryContentAuthorization(
            organization_id,
            connector_id,
            scope_id,
            scope.knowledge_space_id,
            credential_id,
            installation.github_installation_id,
            installation.github_app_id,
            installation.account_id,
            installation.account_login,
            repository_id,
            config["repository_name"],
            config["repository_full_name"],
            config["owner_login"],
            canonical,
            branch,
        )

    def resolve_default_branch_snapshot(
        self, authorization: GitHubRepositoryContentAuthorization
    ) -> GitHubRepositorySnapshot:
        self._provider_boundary(authorization)
        grant = self._content_grant(authorization)
        for _attempt in range(2):
            try:
                first = self._client.get_branch_reference(
                    grant.token,
                    owner_login=grant.repository.owner_login,
                    repository_name=grant.repository.name,
                    branch_name=authorization.default_branch_name,
                )
                commit = self._client.get_commit_reference(
                    grant.token,
                    owner_login=grant.repository.owner_login,
                    repository_name=grant.repository.name,
                    commit_object_id=first.commit_object_id,
                )
                second = self._client.get_branch_reference(
                    grant.token,
                    owner_login=grant.repository.owner_login,
                    repository_name=grant.repository.name,
                    branch_name=authorization.default_branch_name,
                )
            except Exception as exc:
                self._raise_provider_error(exc)
            self._validate_snapshot_references(
                authorization.default_branch_name, first, commit, second
            )
            if first.commit_object_id != second.commit_object_id:
                continue
            return GitHubRepositorySnapshot(
                authorization.connector_id,
                authorization.scope_id,
                authorization.repository_id,
                authorization.canonical_repository_identity,
                authorization.default_branch_name,
                first.commit_object_id,
                commit.root_tree_object_id,
            )
        raise GitHubRepositoryContentConflict(
            "GitHub default branch moved during snapshot resolution"
        )

    def list_tree(
        self,
        authorization: GitHubRepositoryContentAuthorization,
        snapshot: GitHubRepositorySnapshot,
        tree: GitHubTreeDescriptor,
    ) -> GitHubTreePage:
        self._provider_boundary(authorization)
        self._validate_snapshot(authorization, snapshot)
        self._validate_tree_descriptor(snapshot, tree)
        grant = self._content_grant(authorization)
        try:
            provider_tree = self._client.get_tree(
                grant.token,
                owner_login=grant.repository.owner_login,
                repository_name=grant.repository.name,
                tree_object_id=tree.object_id,
            )
        except GitHubProviderResponseTooLargeError as exc:
            raise GitHubRepositoryContentRejected("GitHub tree is too large") from exc
        except Exception as exc:
            self._raise_provider_error(exc)
        return self._validated_tree_page(snapshot, tree, provider_tree)

    def download_blob(
        self,
        authorization: GitHubRepositoryContentAuthorization,
        snapshot: GitHubRepositorySnapshot,
        blob: GitHubRepositoryEntry,
    ) -> GitHubBlobContent:
        self._provider_boundary(authorization)
        self._validate_snapshot(authorization, snapshot)
        self._validate_blob_descriptor(snapshot, blob)
        if PurePosixPath(blob.path).suffix.casefold() not in SUPPORTED_CONTENT_EXTENSIONS:
            raise GitHubRepositoryContentRejected("GitHub blob content type is unsupported")
        if blob.size_bytes is None or blob.size_bytes > MAX_GITHUB_BLOB_BYTES:
            raise GitHubRepositoryContentRejected("GitHub blob is too large")
        grant = self._content_grant(authorization)
        try:
            raw = self._client.download_blob(
                grant.token,
                owner_login=grant.repository.owner_login,
                repository_name=grant.repository.name,
                blob_object_id=blob.object_id,
                maximum_bytes=MAX_GITHUB_BLOB_BYTES,
            )
        except GitHubProviderResponseTooLargeError as exc:
            raise GitHubRepositoryContentRejected("GitHub blob is too large") from exc
        except GitHubProviderRedirectError as exc:
            raise GitHubRepositoryContentRejected("GitHub blob redirect is unsupported") from exc
        except Exception as exc:
            self._raise_provider_error(exc)
        self._validate_raw_blob(raw, blob.size_bytes)
        if raw.content.splitlines()[:1] == [_LFS_SIGNATURE]:
            raise GitHubRepositoryContentUnsupported("git_lfs_unsupported")
        digest = hashlib.sha256(raw.content).hexdigest()
        if digest != raw.sha256:
            raise GitHubRepositoryContentRejected("GitHub blob content is invalid")
        return GitHubBlobContent(raw.content, raw.byte_count, digest)

    def _content_grant(
        self, authorization: GitHubRepositoryContentAuthorization
    ) -> GitHubRepositoryAccessGrant:
        try:
            grant = self._client.create_repository_content_access_token(
                authorization.installation_id,
                authorization.repository_id,
                account_id=authorization.account_id,
                account_login=authorization.account_login,
            )
        except Exception as exc:
            self._raise_provider_error(exc)
        if (
            not isinstance(grant, GitHubRepositoryAccessGrant)
            or not isinstance(grant.token, GitHubInstallationAccessToken)
            or grant.repository.repository_id != authorization.repository_id
            or grant.repository.owner_login.casefold() != authorization.account_login.casefold()
            or grant.repository.owner_login.casefold() != authorization.owner_login.casefold()
            or _ACCOUNT_LOGIN.fullmatch(grant.repository.owner_login) is None
            or _REPOSITORY_NAME.fullmatch(grant.repository.name) is None
            or grant.repository.full_name
            != f"{grant.repository.owner_login}/{grant.repository.name}"
            or grant.repository.default_branch != authorization.default_branch_name
        ):
            raise GitHubRepositoryContentRejected("GitHub repository is unavailable")
        return grant

    def _provider_boundary(
        self, authorization: GitHubRepositoryContentAuthorization
    ) -> None:
        if not isinstance(authorization, GitHubRepositoryContentAuthorization):
            raise GitHubRepositoryContentRejected("GitHub content authorization is invalid")
        if self._session.in_transaction():
            raise GitHubRepositoryContentConflict(
                "database transaction must end before GitHub provider access"
            )

    @staticmethod
    def _raise_provider_error(exc: Exception) -> None:
        if isinstance(exc, SecretStoreError):
            raise GitHubRepositoryContentUnavailable("GitHub secret is unavailable") from exc
        if isinstance(exc, GitHubProviderAuthenticationError):
            raise GitHubRepositoryContentRejected("GitHub authentication failed") from exc
        if isinstance(exc, (GitHubProviderAuthorizationError, GitHubProviderNotFoundError)):
            raise GitHubRepositoryContentRejected("GitHub repository is unavailable") from exc
        if isinstance(exc, GitHubProviderRateLimitError):
            raise GitHubRepositoryContentUnavailable("GitHub rate limit was reached") from exc
        if isinstance(exc, (GitHubProviderMalformedResponseError, GitHubProviderRedirectError)):
            raise GitHubRepositoryContentRejected("GitHub provider response is invalid") from exc
        if isinstance(exc, GitHubProviderResponseTooLargeError):
            raise GitHubRepositoryContentRejected("GitHub provider response is too large") from exc
        if isinstance(exc, GitHubProviderUnavailableError):
            raise GitHubRepositoryContentUnavailable("GitHub provider is unavailable") from exc
        raise GitHubRepositoryContentUnavailable("GitHub provider failed") from exc

    @staticmethod
    def _validate_snapshot_references(
        branch_name: str,
        first: GitHubBranchReference,
        commit: GitHubCommitReference,
        second: GitHubBranchReference,
    ) -> None:
        if (
            not isinstance(first, GitHubBranchReference)
            or not isinstance(commit, GitHubCommitReference)
            or not isinstance(second, GitHubBranchReference)
            or first.branch_name != branch_name
            or second.branch_name != branch_name
        ):
            raise GitHubRepositoryContentRejected("GitHub snapshot is invalid")
        for value in (
            first.commit_object_id,
            first.root_tree_object_id,
            commit.commit_object_id,
            commit.root_tree_object_id,
            second.commit_object_id,
            second.root_tree_object_id,
        ):
            _object_id(value)
        if (
            commit.commit_object_id != first.commit_object_id
            or commit.root_tree_object_id != first.root_tree_object_id
            or (
                first.commit_object_id == second.commit_object_id
                and second.root_tree_object_id != commit.root_tree_object_id
            )
        ):
            raise GitHubRepositoryContentRejected("GitHub snapshot identities are inconsistent")

    @staticmethod
    def _validate_snapshot(
        authorization: GitHubRepositoryContentAuthorization,
        snapshot: GitHubRepositorySnapshot,
    ) -> None:
        if (
            not isinstance(snapshot, GitHubRepositorySnapshot)
            or snapshot.connector_id != authorization.connector_id
            or snapshot.scope_id != authorization.scope_id
            or snapshot.repository_id != authorization.repository_id
            or snapshot.canonical_repository_identity
            != authorization.canonical_repository_identity
            or snapshot.default_branch_name != authorization.default_branch_name
        ):
            raise GitHubRepositoryContentRejected("GitHub snapshot context is invalid")
        _object_id(snapshot.commit_object_id)
        _object_id(snapshot.root_tree_object_id)

    @staticmethod
    def _validate_tree_descriptor(
        snapshot: GitHubRepositorySnapshot, tree: GitHubTreeDescriptor
    ) -> None:
        if (
            not isinstance(tree, GitHubTreeDescriptor)
            or tree.connector_id != snapshot.connector_id
            or tree.scope_id != snapshot.scope_id
            or tree.repository_id != snapshot.repository_id
            or tree.canonical_repository_identity != snapshot.canonical_repository_identity
            or tree.commit_object_id != snapshot.commit_object_id
            or tree.root_tree_object_id != snapshot.root_tree_object_id
        ):
            raise GitHubRepositoryContentRejected("GitHub tree descriptor is invalid")
        _path(tree.path, allow_empty=True)
        _object_id(tree.object_id)
        if tree.path == "" and tree.object_id != snapshot.root_tree_object_id:
            raise GitHubRepositoryContentRejected("GitHub tree descriptor is invalid")

    @staticmethod
    def _validated_tree_page(
        snapshot: GitHubRepositorySnapshot,
        tree: GitHubTreeDescriptor,
        provider_tree: GitHubGitTree,
    ) -> GitHubTreePage:
        if not isinstance(provider_tree, GitHubGitTree):
            raise GitHubRepositoryContentRejected("GitHub tree response is invalid")
        if provider_tree.object_id != tree.object_id:
            raise GitHubRepositoryContentRejected("GitHub tree identity is invalid")
        if provider_tree.truncated:
            raise GitHubRepositoryContentRejected("GitHub tree response is truncated")
        if len(provider_tree.entries) > MAX_GITHUB_TREE_ENTRIES:
            raise GitHubRepositoryContentRejected("GitHub tree is too large")
        results: list[GitHubRepositoryEntry] = []
        names: set[str] = set()
        identities: set[tuple[str, str, str]] = set()
        collision_keys: dict[str, str] = {}
        for item in provider_tree.entries:
            name = _path(item.name, allow_empty=False, single_segment=True)
            normalized = unicodedata.normalize("NFC", name).casefold()
            if name in names or (normalized in collision_keys and collision_keys[normalized] != name):
                raise GitHubRepositoryContentRejected("GitHub tree contains duplicate paths")
            names.add(name)
            collision_keys[normalized] = name
            path = _path(f"{tree.path}/{name}" if tree.path else name, allow_empty=False)
            _object_id(item.object_id)
            identity = (path, item.object_id, item.object_type)
            if identity in identities:
                raise GitHubRepositoryContentRejected("GitHub tree contains duplicate entries")
            identities.add(identity)
            if item.mode == "040000" and item.object_type == "tree":
                if item.size_bytes is not None:
                    raise GitHubRepositoryContentRejected("GitHub tree entry is invalid")
                entry_type, size, executable = "tree", None, False
            elif item.mode in {"100644", "100755"} and item.object_type == "blob":
                if (
                    isinstance(item.size_bytes, bool)
                    or not isinstance(item.size_bytes, int)
                    or item.size_bytes < 0
                ):
                    raise GitHubRepositoryContentRejected("GitHub blob descriptor is invalid")
                entry_type, size, executable = (
                    "regular_blob",
                    item.size_bytes,
                    item.mode == "100755",
                )
            elif item.mode == "120000" and item.object_type == "blob":
                if (
                    isinstance(item.size_bytes, bool)
                    or not isinstance(item.size_bytes, int)
                    or item.size_bytes < 0
                ):
                    raise GitHubRepositoryContentRejected("GitHub symlink descriptor is invalid")
                entry_type, size, executable = "symlink", item.size_bytes, False
            elif item.mode == "160000" and item.object_type == "commit":
                if item.size_bytes is not None:
                    raise GitHubRepositoryContentRejected("GitHub submodule descriptor is invalid")
                entry_type, size, executable = "submodule", None, False
            else:
                raise GitHubRepositoryContentRejected("GitHub tree entry type is unsupported")
            results.append(
                GitHubRepositoryEntry(
                    snapshot.connector_id,
                    snapshot.scope_id,
                    snapshot.repository_id,
                    snapshot.canonical_repository_identity,
                    snapshot.commit_object_id,
                    snapshot.root_tree_object_id,
                    tree.object_id,
                    name,
                    path,
                    entry_type,
                    item.object_id,
                    size,
                    executable,
                )
            )
        return GitHubTreePage(tree, tuple(results))

    @staticmethod
    def _validate_blob_descriptor(
        snapshot: GitHubRepositorySnapshot, blob: GitHubRepositoryEntry
    ) -> None:
        if (
            not isinstance(blob, GitHubRepositoryEntry)
            or blob.connector_id != snapshot.connector_id
            or blob.scope_id != snapshot.scope_id
            or blob.repository_id != snapshot.repository_id
            or blob.canonical_repository_identity != snapshot.canonical_repository_identity
            or blob.commit_object_id != snapshot.commit_object_id
            or blob.root_tree_object_id != snapshot.root_tree_object_id
            or blob.entry_type != "regular_blob"
            or blob.executable not in {True, False}
        ):
            raise GitHubRepositoryContentRejected("GitHub blob descriptor is invalid")
        _object_id(blob.parent_tree_object_id)
        _object_id(blob.object_id)
        _path(blob.name, allow_empty=False, single_segment=True)
        _path(blob.path, allow_empty=False)
        if not blob.path.endswith(f"/{blob.name}") and blob.path != blob.name:
            raise GitHubRepositoryContentRejected("GitHub blob path is invalid")

    @staticmethod
    def _validate_raw_blob(raw: GitHubRawBlob, declared_size: int) -> None:
        if (
            not isinstance(raw, GitHubRawBlob)
            or not isinstance(raw.content, bytes)
            or isinstance(raw.byte_count, bool)
            or not isinstance(raw.byte_count, int)
            or raw.byte_count != len(raw.content)
            or raw.byte_count != declared_size
            or raw.byte_count > MAX_GITHUB_BLOB_BYTES
            or not isinstance(raw.sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", raw.sha256) is None
        ):
            raise GitHubRepositoryContentRejected("GitHub blob size or content is invalid")


def _object_id(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) not in {40, 64}
        or _OBJECT_ID.fullmatch(value) is None
    ):
        raise GitHubRepositoryContentRejected("GitHub object identity is invalid")
    return value


def _path(
    value: object, *, allow_empty: bool, single_segment: bool = False
) -> str:
    if not isinstance(value, str):
        raise GitHubRepositoryContentRejected("GitHub repository path is invalid")
    if value == "" and allow_empty:
        return value
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise GitHubRepositoryContentRejected("GitHub repository path is invalid") from exc
    segments = value.split("/")
    if (
        not value
        or value.startswith("/")
        or value.endswith("/")
        or "\\" in value
        or len(encoded) > MAX_REPOSITORY_PATH_BYTES
        or len(segments) > MAX_REPOSITORY_PATH_SEGMENTS
        or (single_segment and len(segments) != 1)
        or any(
            segment in {"", ".", ".."}
            or len(segment.encode("utf-8")) > MAX_REPOSITORY_PATH_SEGMENT_BYTES
            for segment in segments
        )
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise GitHubRepositoryContentRejected("GitHub repository path is invalid")
    return value
