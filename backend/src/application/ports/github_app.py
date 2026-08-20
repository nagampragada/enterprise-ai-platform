"""Narrow, token-redacting GitHub App application boundary."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


class GitHubProviderError(RuntimeError):
    def __init__(self, *_unsafe: object) -> None:
        super().__init__("GitHub provider request failed")


class GitHubProviderAuthenticationError(GitHubProviderError): pass
class GitHubProviderAuthorizationError(GitHubProviderError): pass
class GitHubProviderNotFoundError(GitHubProviderError): pass
class GitHubProviderRateLimitError(GitHubProviderError): pass
class GitHubProviderUnavailableError(GitHubProviderError): pass


@dataclass(frozen=True, repr=False)
class GitHubUserAccessToken:
    value: str

    def __post_init__(self) -> None:
        if (not isinstance(self.value,str) or not 1<=len(self.value)<=8192
            or any(character.isspace() for character in self.value)):
            raise ValueError("GitHub user access token is invalid")


@dataclass(frozen=True)
class GitHubUser:
    user_id: int
    login: str


@dataclass(frozen=True)
class GitHubInstallation:
    installation_id: int
    app_id: int
    account_id: int
    account_login: str
    account_type: str
    repository_selection: str
    permissions: tuple[tuple[str,str],...]
    created_at: datetime
    updated_at: datetime


class GitHubAppClient(Protocol):
    @property
    def app_id(self) -> int: ...
    @property
    def web_base_url(self) -> str: ...
    @property
    def client_id(self) -> str: ...
    @property
    def callback_url(self) -> str: ...
    def build_installation_url(self, state: str) -> str: ...
    def build_authorization_url(self, state: str, pkce_challenge: str) -> str: ...
    def exchange_authorization_code(self, code: str, pkce_verifier: str) -> GitHubUserAccessToken: ...
    def get_authenticated_user(self, token: GitHubUserAccessToken) -> GitHubUser: ...
    def list_user_installations(self, token: GitHubUserAccessToken) -> tuple[GitHubInstallation, ...]: ...
    def verify_installation(self, installation_id: int) -> GitHubInstallation: ...
