"""Configuration helpers for backend infrastructure."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from functools import lru_cache
from urllib.parse import urlparse

from application.ports.secret_store import SecretReference


@dataclass(frozen=True, repr=False)
class Settings:
    database_url: str
    jwt_secret_key: str
    access_token_lifetime_minutes: int
    refresh_token_hash_secret: str


@dataclass(frozen=True, repr=False)
class GitHubAppSettings:
    app_id: int
    app_slug: str
    client_id: str
    client_secret_reference: SecretReference
    private_key_reference: SecretReference
    callback_url: str
    setup_url: str
    api_base_url: str = "https://api.github.com"
    web_base_url: str = "https://github.com"
    request_timeout_seconds: float = 10.0
    max_retries: int = 2

    def __post_init__(self) -> None:
        if isinstance(self.app_id, bool) or not isinstance(self.app_id, int) or self.app_id < 1:
            raise ValueError("GitHub App ID is invalid")
        if not isinstance(self.app_slug, str) or not re.fullmatch(r"[A-Za-z0-9-]{1,100}",self.app_slug):
            raise ValueError("GitHub App slug is invalid")
        if not isinstance(self.client_id,str) or not re.fullmatch(r"[A-Za-z0-9._-]{6,255}",self.client_id):
            raise ValueError("GitHub App client ID is invalid")
        if self.client_id==str(self.app_id):raise ValueError("GitHub App identifiers must be distinct")
        if not isinstance(self.client_secret_reference,SecretReference) or not isinstance(self.private_key_reference,SecretReference):
            raise ValueError("GitHub secret configuration is invalid")
        for reference in (self.client_secret_reference,self.private_key_reference):
            if not re.fullmatch(r"[a-z][a-z0-9+.-]*://[^\s]+",reference.value):
                raise ValueError("GitHub secret reference is invalid")
        for value in (self.callback_url,self.setup_url,self.api_base_url,self.web_base_url):
            if not isinstance(value,str):raise ValueError("GitHub URL configuration is invalid")
            parsed = urlparse(value)
            if (parsed.scheme!="https" or not parsed.netloc
                or parsed.username or parsed.password or parsed.fragment or "*" in value):
                raise ValueError("GitHub URL configuration is invalid")
        callback=urlparse(self.callback_url)
        if callback.query or not callback.path or callback.path=="/":raise ValueError("GitHub callback URL is invalid")
        for value in (self.api_base_url,self.web_base_url):
            parsed=urlparse(value)
            if parsed.query:raise ValueError("GitHub base URL configuration is invalid")
        if not isinstance(self.request_timeout_seconds, (int, float)) or isinstance(self.request_timeout_seconds, bool) or not 0.1 <= self.request_timeout_seconds <= 60:
            raise ValueError("GitHub request timeout is invalid")
        if isinstance(self.max_retries, bool) or not isinstance(self.max_retries, int) or not 0 <= self.max_retries <= 3:
            raise ValueError("GitHub retry count is invalid")


def load_github_app_settings_from_environment()->GitHubAppSettings:
    """Load references only; secret values are resolved later by an injected SecretStore."""
    required=("GITHUB_APP_ID","GITHUB_APP_SLUG","GITHUB_APP_CLIENT_ID",
        "GITHUB_APP_CLIENT_SECRET_REFERENCE","GITHUB_APP_PRIVATE_KEY_REFERENCE",
        "GITHUB_APP_CALLBACK_URL","GITHUB_APP_SETUP_URL")
    if any(not os.getenv(name) for name in required):raise ValueError("GitHub App configuration is incomplete")
    try:app_id=int(os.environ["GITHUB_APP_ID"])
    except ValueError as exc:raise ValueError("GitHub App configuration is invalid") from exc
    return GitHubAppSettings(app_id=app_id,app_slug=os.environ["GITHUB_APP_SLUG"],
        client_id=os.environ["GITHUB_APP_CLIENT_ID"],
        client_secret_reference=SecretReference(os.environ["GITHUB_APP_CLIENT_SECRET_REFERENCE"]),
        private_key_reference=SecretReference(os.environ["GITHUB_APP_PRIVATE_KEY_REFERENCE"]),
        callback_url=os.environ["GITHUB_APP_CALLBACK_URL"],setup_url=os.environ["GITHUB_APP_SETUP_URL"],
        api_base_url=os.getenv("GITHUB_API_BASE_URL","https://api.github.com"),
        web_base_url=os.getenv("GITHUB_WEB_BASE_URL","https://github.com"),
        request_timeout_seconds=float(os.getenv("GITHUB_REQUEST_TIMEOUT_SECONDS","10")),
        max_retries=int(os.getenv("GITHUB_MAX_RETRIES","2")))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://enterprise_ai_platform:enterprise_ai_platform@localhost:5432/enterprise_ai_platform",
    )
    jwt_secret_key = os.getenv("JWT_SECRET_KEY", "development-jwt-secret-change-me")
    access_token_lifetime_minutes = int(os.getenv("ACCESS_TOKEN_LIFETIME_MINUTES", "15"))
    refresh_token_hash_secret = os.getenv("REFRESH_TOKEN_HASH_SECRET", jwt_secret_key)

    return Settings(
        database_url=database_url,
        jwt_secret_key=jwt_secret_key,
        access_token_lifetime_minutes=access_token_lifetime_minutes,
        refresh_token_hash_secret=refresh_token_hash_secret,
    )
