"""Tenant-safe staged persistence for explicitly selected GitHub repositories."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import re
from typing import Callable
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from application.ports.github_app import (
    GitHubAppClient,
    GitHubProviderAuthorizationError,
    GitHubProviderNotFoundError,
    GitHubRepository,
)
from infrastructure.db.models import ConnectorScope, KnowledgeSpace
from infrastructure.repositories.connector_credential_repository import (
    ConnectorCredentialRepository,
)
from infrastructure.repositories.connector_repository import ConnectorRepository
from infrastructure.repositories.connector_scope_repository import (
    ConnectorScopePage,
    ConnectorScopePageCursor,
    ConnectorScopeRepository,
)
from infrastructure.repositories.github_app_installation_repository import (
    GitHubAppInstallationRepository,
)


EXPECTED_GITHUB_SCOPES = frozenset({"contents:read", "metadata:read"})
REPOSITORY_SCOPE_TYPE = "repository"
REPOSITORY_CONFIG_KEYS = frozenset(
    {
        "repository_id",
        "repository_name",
        "repository_full_name",
        "owner_login",
        "private",
        "visibility",
        "archived",
        "disabled",
        "default_branch",
    }
)
_ACCOUNT_LOGIN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,98}[A-Za-z0-9])?")
_REPOSITORY_NAME = re.compile(r"[A-Za-z0-9_.-]{1,100}")
_DEFAULT_BRANCH = re.compile(r"[A-Za-z0-9._/-]{1,255}")


class GitHubRepositorySelectionNotFound(RuntimeError):
    pass


class GitHubRepositorySelectionRejected(RuntimeError):
    pass


class GitHubRepositorySelectionConflict(RuntimeError):
    pass


class GitHubRepositorySelectionPersistenceError(RuntimeError):
    pass


@dataclass(frozen=True, repr=False)
class GitHubRepositorySelectionContext:
    organization_id: UUID
    connector_id: UUID
    knowledge_space_id: UUID
    credential_id: UUID
    installation_id: int
    app_id: int
    account_id: int
    account_login: str
    repository_id: int


@dataclass(frozen=True)
class GitHubRepositoryScopeView:
    scope_id: UUID
    connector_id: UUID
    knowledge_space_id: UUID
    repository_id: int
    repository_name: str
    repository_full_name: str
    owner_login: str
    private: bool
    visibility: str | None
    archived: bool
    disabled: bool
    default_branch: str | None
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class GitHubRepositoryScopePage:
    items: tuple[GitHubRepositoryScopeView, ...]
    limit: int
    has_more: bool
    next_cursor: ConnectorScopePageCursor | None


class GitHubRepositorySelectionService:
    def __init__(
        self,
        session: Session,
        client: GitHubAppClient | None = None,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._session = session
        self._connectors = ConnectorRepository(session)
        self._credentials = ConnectorCredentialRepository(session)
        self._installations = GitHubAppInstallationRepository(session)
        self._scopes = ConnectorScopeRepository(session)
        self._client = client
        self._clock = clock

    def prepare(
        self,
        organization_id: UUID,
        connector_id: UUID,
        knowledge_space_id: UUID,
        repository_id: int,
    ) -> GitHubRepositorySelectionContext:
        repository_id = _repository_id(repository_id)
        if self._client is None:
            raise GitHubRepositorySelectionConflict("GitHub provider is unavailable")
        connector = self._connectors.get_by_id(organization_id, connector_id)
        credential = self._credentials.get(organization_id, connector_id)
        installation = self._installations.get(organization_id, connector_id)
        self._require_connector(connector)
        self._require_installation(credential, installation)
        self._require_active_knowledge_space(organization_id, knowledge_space_id, lock=False)
        return GitHubRepositorySelectionContext(
            organization_id,
            connector_id,
            knowledge_space_id,
            credential.credential_id,
            installation.github_installation_id,
            installation.github_app_id,
            installation.account_id,
            installation.account_login,
            repository_id,
        )

    def verify(
        self, context: GitHubRepositorySelectionContext
    ) -> GitHubRepository:
        if self._client is None or not isinstance(
            context, GitHubRepositorySelectionContext
        ):
            raise GitHubRepositorySelectionRejected(
                "GitHub repository selection request is invalid"
            )
        try:
            grant = self._client.create_repository_access_token(
                context.installation_id,
                context.repository_id,
                account_id=context.account_id,
                account_login=context.account_login,
            )
            page = self._client.list_installation_repositories(
                grant.token,
                page=1,
                page_size=1,
                account_id=context.account_id,
                account_login=context.account_login,
            )
        except (GitHubProviderAuthorizationError, GitHubProviderNotFoundError) as exc:
            raise GitHubRepositorySelectionRejected(
                "GitHub repository is unavailable"
            ) from exc
        if (
            grant.repository.repository_id != context.repository_id
            or page.page != 1
            or page.page_size != 1
            or page.has_next
            or page.total_count != 1
            or len(page.items) != 1
            or page.items[0] != grant.repository
        ):
            raise GitHubRepositorySelectionRejected(
                "GitHub repository is unavailable"
            )
        return grant.repository

    def persist(
        self,
        context: GitHubRepositorySelectionContext,
        repository: GitHubRepository,
        creator_user_id: UUID,
    ) -> GitHubRepositoryScopeView:
        if (
            not isinstance(context, GitHubRepositorySelectionContext)
            or not isinstance(repository, GitHubRepository)
            or repository.repository_id != context.repository_id
            or repository.owner_login.casefold() != context.account_login.casefold()
        ):
            raise GitHubRepositorySelectionRejected(
                "GitHub repository selection request is invalid"
            )
        connector = self._connectors.lock_by_id(
            context.organization_id, context.connector_id
        )
        credential = self._credentials.lock(
            context.organization_id, context.connector_id
        )
        installation = self._installations.lock(
            context.organization_id, context.connector_id
        )
        self._require_connector(connector)
        self._require_installation(credential, installation)
        self._require_active_knowledge_space(
            context.organization_id, context.knowledge_space_id, lock=True
        )
        if (
            credential.id != context.credential_id
            or installation.credential_id != context.credential_id
            or installation.github_installation_id != context.installation_id
            or installation.github_app_id != context.app_id
            or installation.account_id != context.account_id
            or installation.account_login != context.account_login
        ):
            raise GitHubRepositorySelectionConflict(
                "GitHub installation changed during repository verification"
            )

        now = self._now()
        external_key = _external_scope_key(context.repository_id)
        scope = self._scopes.lock_by_external_scope_key(
            context.organization_id, context.connector_id, external_key
        )
        config = _repository_config(repository)
        if scope is None:
            scope = ConnectorScope(
                id=uuid4(),
                organization_id=context.organization_id,
                connector_id=context.connector_id,
                knowledge_space_id=context.knowledge_space_id,
                display_name=repository.full_name,
                slug=f"github-repository-{context.repository_id}",
                scope_type=REPOSITORY_SCOPE_TYPE,
                external_scope_key=external_key,
                access_mode="platform_managed",
                status="active",
                safe_config=config,
                config_schema_version=1,
                created_by_user_id=creator_user_id,
                last_validated_at=now,
                removed_at=None,
            )
            self._scopes.add(context.organization_id, scope)
            return _scope_view(scope)

        _validated_scope_config(scope)
        if scope.knowledge_space_id != context.knowledge_space_id:
            raise GitHubRepositorySelectionConflict(
                "GitHub repository is already assigned"
            )
        scope.display_name = repository.full_name
        scope.safe_config = config
        scope.config_schema_version = 1
        scope.status = "active"
        scope.last_validated_at = now
        scope.updated_at = now
        scope.removed_at = None
        self._scopes.flush()
        return _scope_view(scope)

    def list(
        self,
        organization_id: UUID,
        connector_id: UUID,
        *,
        limit: int,
        cursor: ConnectorScopePageCursor | None = None,
    ) -> GitHubRepositoryScopePage:
        connector = self._connectors.get_by_id(organization_id, connector_id)
        if connector is None:
            raise GitHubRepositorySelectionNotFound("GitHub connector was not found")
        if connector.connector_type != "github":
            raise GitHubRepositorySelectionRejected(
                "GitHub repository selection is unavailable"
            )
        page: ConnectorScopePage = self._scopes.list_page(
            organization_id,
            connector_id=connector_id,
            scope_type=REPOSITORY_SCOPE_TYPE,
            limit=limit,
            cursor=cursor,
        )
        return GitHubRepositoryScopePage(
            tuple(_scope_view(item) for item in page.items),
            page.limit,
            page.has_more,
            page.next_cursor,
        )

    def deselect(
        self, organization_id: UUID, connector_id: UUID, scope_id: UUID
    ) -> GitHubRepositoryScopeView:
        connector = self._connectors.lock_by_id(organization_id, connector_id)
        if connector is None:
            raise GitHubRepositorySelectionNotFound("GitHub connector was not found")
        if connector.connector_type != "github":
            raise GitHubRepositorySelectionRejected(
                "GitHub repository selection is unavailable"
            )
        scope = self._scopes.lock_by_id(organization_id, scope_id)
        if (
            scope is None
            or scope.connector_id != connector_id
            or scope.scope_type != REPOSITORY_SCOPE_TYPE
        ):
            raise GitHubRepositorySelectionNotFound(
                "GitHub repository scope was not found"
            )
        _validated_scope_config(scope)
        if scope.status != "removed":
            now = self._now()
            scope.status = "removed"
            scope.removed_at = now
            scope.updated_at = now
            self._scopes.flush()
        return _scope_view(scope)

    def _require_connector(self, connector) -> None:
        if connector is None:
            raise GitHubRepositorySelectionNotFound("GitHub connector was not found")
        if connector.connector_type != "github":
            raise GitHubRepositorySelectionRejected(
                "GitHub repository selection is unavailable"
            )
        if connector.status != "active":
            raise GitHubRepositorySelectionConflict("GitHub connector is not active")

    def _require_installation(self, credential, installation) -> None:
        if credential is None or installation is None:
            raise GitHubRepositorySelectionNotFound("GitHub installation was not found")
        if self._client is None or (
            credential.status != "active"
            or credential.provider_key != "github"
            or credential.auth_scheme != "app_installation"
            or _credential_id(credential) != installation.credential_id
            or credential.external_subject != str(installation.github_installation_id)
            or frozenset(credential.granted_scopes) != EXPECTED_GITHUB_SCOPES
            or credential.expires_at is not None
            or installation.status != "connected"
            or installation.disconnected_at is not None
            or installation.account_type != "Organization"
            or installation.github_app_id != self._client.app_id
            or installation.github_installation_id < 1
            or installation.account_id < 1
            or _ACCOUNT_LOGIN.fullmatch(installation.account_login) is None
        ):
            raise GitHubRepositorySelectionConflict(
                "GitHub installation is unavailable"
            )

    def _require_active_knowledge_space(
        self, organization_id: UUID, knowledge_space_id: UUID, *, lock: bool
    ) -> None:
        statement = select(KnowledgeSpace.id).where(
            KnowledgeSpace.organization_id == organization_id,
            KnowledgeSpace.id == knowledge_space_id,
            KnowledgeSpace.status == "active",
        )
        if lock:
            statement = statement.with_for_update()
        try:
            found = self._session.execute(statement).scalar_one_or_none()
        except SQLAlchemyError as exc:
            raise GitHubRepositorySelectionPersistenceError(
                "knowledge space could not be verified"
            ) from exc
        if found is None:
            raise GitHubRepositorySelectionNotFound("knowledge space was not found")

    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise GitHubRepositorySelectionRejected("clock is invalid")
        return value


def _repository_id(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= 9_223_372_036_854_775_807
    ):
        raise GitHubRepositorySelectionRejected(
            "GitHub repository selection request is invalid"
        )
    return value


def _credential_id(credential) -> UUID:
    value = getattr(credential, "credential_id", getattr(credential, "id", None))
    if not isinstance(value, UUID):
        raise GitHubRepositorySelectionConflict("GitHub installation is unavailable")
    return value


def _external_scope_key(repository_id: int) -> str:
    return f"github:repository:{_repository_id(repository_id)}"


def _repository_config(repository: GitHubRepository) -> dict[str, object]:
    config: dict[str, object] = {
        "repository_id": repository.repository_id,
        "repository_name": repository.name,
        "repository_full_name": repository.full_name,
        "owner_login": repository.owner_login,
        "private": repository.private,
        "visibility": repository.visibility,
        "archived": repository.archived,
        "disabled": repository.disabled,
        "default_branch": repository.default_branch,
    }
    _validated_config_values(config)
    return config


def _validated_scope_config(scope: ConnectorScope) -> dict[str, object]:
    config = scope.safe_config
    if (
        scope.scope_type != REPOSITORY_SCOPE_TYPE
        or scope.access_mode != "platform_managed"
        or scope.config_schema_version != 1
        or not isinstance(config, dict)
        or set(config) != REPOSITORY_CONFIG_KEYS
        or config.get("repository_id") != _repository_id_from_key(scope.external_scope_key)
    ):
        raise GitHubRepositorySelectionConflict(
            "GitHub repository scope is invalid"
        )
    try:
        _validated_config_values(config)
    except GitHubRepositorySelectionRejected as exc:
        raise GitHubRepositorySelectionConflict(
            "GitHub repository scope is invalid"
        ) from exc
    return config


def _validated_config_values(config: dict[str, object]) -> None:
    repository_id = _repository_id(config.get("repository_id"))
    name = config.get("repository_name")
    owner = config.get("owner_login")
    full_name = config.get("repository_full_name")
    branch = config.get("default_branch")
    if (
        not isinstance(name, str)
        or _REPOSITORY_NAME.fullmatch(name) is None
        or not isinstance(owner, str)
        or _ACCOUNT_LOGIN.fullmatch(owner) is None
        or not isinstance(full_name, str)
        or len(full_name) > 255
        or full_name != f"{owner}/{name}"
        or not isinstance(config.get("private"), bool)
        or config.get("visibility") not in {None, "public", "private", "internal"}
        or not isinstance(config.get("archived"), bool)
        or not isinstance(config.get("disabled"), bool)
        or (
            branch is not None
            and (
                not isinstance(branch, str)
                or _DEFAULT_BRANCH.fullmatch(branch) is None
                or branch.startswith(("-", "/", "."))
                or branch.endswith(("/", ".", ".lock"))
                or any(part in branch for part in ("..", "//", "@{"))
            )
        )
        or repository_id < 1
    ):
        raise GitHubRepositorySelectionRejected(
            "GitHub repository selection request is invalid"
        )


def _repository_id_from_key(value: object) -> int:
    if not isinstance(value, str) or not value.startswith("github:repository:"):
        raise GitHubRepositorySelectionConflict("GitHub repository scope is invalid")
    try:
        return _repository_id(int(value.removeprefix("github:repository:")))
    except (ValueError, GitHubRepositorySelectionRejected) as exc:
        raise GitHubRepositorySelectionConflict(
            "GitHub repository scope is invalid"
        ) from exc


def _scope_view(scope: ConnectorScope) -> GitHubRepositoryScopeView:
    config = _validated_scope_config(scope)
    return GitHubRepositoryScopeView(
        scope.id,
        scope.connector_id,
        scope.knowledge_space_id,
        config["repository_id"],
        config["repository_name"],
        config["repository_full_name"],
        config["owner_login"],
        config["private"],
        config["visibility"],
        config["archived"],
        config["disabled"],
        config["default_branch"],
        scope.status,
        scope.created_at,
        scope.updated_at,
    )
