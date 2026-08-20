"""Tenant-safe staged GitHub repository discovery without persistence."""

from __future__ import annotations

from dataclasses import dataclass
import re
from uuid import UUID

from application.ports.github_app import GitHubAppClient, GitHubRepositoryPage
from infrastructure.repositories.connector_credential_repository import (
    ConnectorCredentialRepository,
)
from infrastructure.repositories.connector_repository import ConnectorRepository
from infrastructure.repositories.github_app_installation_repository import (
    GitHubAppInstallationRepository,
)


MAX_REPOSITORY_PAGE = 1_000
MAX_REPOSITORY_PAGE_SIZE = 100
EXPECTED_DISCOVERY_SCOPES = frozenset({"contents:read", "metadata:read"})
_ACCOUNT_LOGIN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,98}[A-Za-z0-9])?")


class GitHubRepositoryDiscoveryNotFound(RuntimeError):
    pass


class GitHubRepositoryDiscoveryRejected(RuntimeError):
    pass


class GitHubRepositoryDiscoveryConflict(RuntimeError):
    pass


@dataclass(frozen=True, repr=False)
class GitHubRepositoryDiscoveryContext:
    installation_id: int
    account_id: int
    account_login: str


class GitHubRepositoryDiscoveryService:
    def __init__(self, session, client: GitHubAppClient) -> None:
        self._connectors = ConnectorRepository(session)
        self._credentials = ConnectorCredentialRepository(session)
        self._installations = GitHubAppInstallationRepository(session)
        self._client = client

    def prepare(
        self, organization_id: UUID, connector_id: UUID
    ) -> GitHubRepositoryDiscoveryContext:
        connector = self._connectors.get_by_id(organization_id, connector_id)
        if connector is None:
            raise GitHubRepositoryDiscoveryNotFound("GitHub connector was not found")
        if connector.connector_type != "github":
            raise GitHubRepositoryDiscoveryRejected("GitHub repository discovery is unavailable")
        if connector.status != "active":
            raise GitHubRepositoryDiscoveryConflict("GitHub connector is not active")

        credential = self._credentials.get(organization_id, connector_id)
        installation = self._installations.get(organization_id, connector_id)
        if credential is None or installation is None:
            raise GitHubRepositoryDiscoveryNotFound("GitHub installation was not found")
        if (
            credential.status != "active"
            or credential.provider_key != "github"
            or credential.auth_scheme != "app_installation"
            or credential.credential_id != installation.credential_id
            or credential.external_subject != str(installation.github_installation_id)
            or frozenset(credential.granted_scopes) != EXPECTED_DISCOVERY_SCOPES
            or credential.expires_at is not None
            or installation.status != "connected"
            or installation.disconnected_at is not None
            or installation.account_type != "Organization"
            or installation.github_app_id != self._client.app_id
            or installation.github_installation_id < 1
            or installation.account_id < 1
            or _ACCOUNT_LOGIN.fullmatch(installation.account_login) is None
        ):
            raise GitHubRepositoryDiscoveryConflict(
                "GitHub installation is unavailable"
            )
        return GitHubRepositoryDiscoveryContext(
            installation.github_installation_id,
            installation.account_id,
            installation.account_login,
        )

    def discover(
        self,
        context: GitHubRepositoryDiscoveryContext,
        *,
        page: int,
        page_size: int,
    ) -> GitHubRepositoryPage:
        if not isinstance(context, GitHubRepositoryDiscoveryContext):
            raise GitHubRepositoryDiscoveryRejected(
                "GitHub repository discovery request is invalid"
            )
        _page(page)
        _page_size(page_size)
        token = self._client.create_installation_access_token(context.installation_id)
        return self._client.list_installation_repositories(
            token,
            page=page,
            page_size=page_size,
            account_id=context.account_id,
            account_login=context.account_login,
        )


def _page(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= MAX_REPOSITORY_PAGE
    ):
        raise GitHubRepositoryDiscoveryRejected(
            "GitHub repository discovery request is invalid"
        )
    return value


def _page_size(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= MAX_REPOSITORY_PAGE_SIZE
    ):
        raise GitHubRepositoryDiscoveryRejected(
            "GitHub repository discovery request is invalid"
        )
    return value
