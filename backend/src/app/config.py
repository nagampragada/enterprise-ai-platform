"""Fail-closed, process-specific backend configuration helpers."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from urllib.parse import urlparse

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from application.ports.secret_store import SecretReference


APP_ENVIRONMENT_VARIABLE = "APP_ENVIRONMENT"
ALLOWED_RUNTIME_ENVIRONMENTS = frozenset(
    {"development", "test", "sandbox", "production"}
)
STRICT_RUNTIME_ENVIRONMENTS = frozenset({"sandbox", "production"})
DEVELOPMENT_DATABASE_URL = (
    "postgresql+psycopg://enterprise_ai_platform:enterprise_ai_platform@"
    "localhost:5432/enterprise_ai_platform"
)
DEVELOPMENT_JWT_SECRET = "development-jwt-secret-change-me"
DEFAULT_API_PORT = 8000
MINIMUM_SECRET_LENGTH = 32


class InvalidRuntimeConfiguration(ValueError):
    """Raised with a fixed safe message for invalid runtime configuration."""


@dataclass(frozen=True, repr=False)
class DatabaseSettings:
    runtime_environment: str
    database_url: str


@dataclass(frozen=True, repr=False)
class Settings:
    runtime_environment: str
    database_url: str
    jwt_secret_key: str
    access_token_lifetime_minutes: int
    refresh_token_hash_secret: str


@dataclass(frozen=True, repr=False)
class GitHubWorkerSettings:
    """GitHub fields consumed by provider reads in the connector worker."""

    app_id: int
    client_id: str
    private_key_reference: SecretReference
    api_base_url: str = "https://api.github.com"
    web_base_url: str = "https://github.com"
    request_timeout_seconds: float = 10.0
    max_retries: int = 2

    def __post_init__(self) -> None:
        _validate_github_provider_settings(self)


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
        _validate_github_provider_settings(self)
        if not isinstance(self.app_slug, str) or not re.fullmatch(
            r"[A-Za-z0-9-]{1,100}", self.app_slug
        ):
            raise ValueError("GitHub App slug is invalid")
        if not isinstance(self.client_secret_reference, SecretReference):
            raise ValueError("GitHub secret configuration is invalid")
        _validate_secret_reference(self.client_secret_reference)
        for value in (self.callback_url, self.setup_url):
            _validate_https_url(value)
        callback = urlparse(self.callback_url)
        setup = urlparse(self.setup_url)
        if (
            callback.query
            or callback.path != "/api/v1/connectors/github/callback"
            or setup.query
            or setup.path != "/api/v1/connectors/github/setup"
            or (callback.scheme, callback.netloc) != (setup.scheme, setup.netloc)
        ):
            raise ValueError("GitHub browser URL configuration is invalid")


@dataclass(frozen=True, repr=False)
class GoogleSecretManagerSettings:
    project_id: str
    secret_prefix: str
    environment: str

    def __post_init__(self) -> None:
        if not isinstance(self.project_id, str) or not re.fullmatch(
            r"[a-z][a-z0-9-]{4,28}[a-z0-9]", self.project_id
        ):
            raise ValueError("Google Secret Manager project configuration is invalid")
        if not isinstance(self.secret_prefix, str) or not re.fullmatch(
            r"[a-z][a-z0-9-]{0,30}[a-z0-9]|[a-z]", self.secret_prefix
        ):
            raise ValueError("Google Secret Manager prefix configuration is invalid")
        if not isinstance(self.environment, str) or not re.fullmatch(
            r"[a-z][a-z0-9_-]{0,62}", self.environment
        ):
            raise ValueError("Google Secret Manager environment configuration is invalid")


@dataclass(frozen=True, repr=False)
class ApiProcessSettings:
    application: Settings
    github: GitHubAppSettings | None
    secret_manager: GoogleSecretManagerSettings | None
    port: int


@dataclass(frozen=True, repr=False)
class WorkerProcessSettings:
    database: DatabaseSettings
    github: GitHubWorkerSettings
    secret_manager: GoogleSecretManagerSettings
    github_sync_ledger_planning_enabled: bool = False


def load_runtime_environment(environ: Mapping[str, str] | None = None) -> str:
    values = os.environ if environ is None else environ
    value = values.get(APP_ENVIRONMENT_VARIABLE, "development")
    if value not in ALLOWED_RUNTIME_ENVIRONMENTS:
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
    return value


def load_database_settings(
    environ: Mapping[str, str] | None = None,
) -> DatabaseSettings:
    values = os.environ if environ is None else environ
    runtime_environment = load_runtime_environment(values)
    supplied = values.get("DATABASE_URL")
    if runtime_environment in STRICT_RUNTIME_ENVIRONMENTS and not supplied:
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
    database_url = supplied or DEVELOPMENT_DATABASE_URL
    try:
        parsed = make_url(database_url)
    except (ArgumentError, TypeError, ValueError) as exc:
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid") from exc
    if not parsed.drivername.startswith("postgresql") or not parsed.database:
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
    if runtime_environment in STRICT_RUNTIME_ENVIRONMENTS and _is_development_database(
        parsed
    ):
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
    return DatabaseSettings(runtime_environment, database_url)


def load_application_settings(
    environ: Mapping[str, str] | None = None,
) -> Settings:
    values = os.environ if environ is None else environ
    database = load_database_settings(values)
    strict = database.runtime_environment in STRICT_RUNTIME_ENVIRONMENTS
    jwt_supplied = values.get("JWT_SECRET_KEY")
    refresh_supplied = values.get("REFRESH_TOKEN_HASH_SECRET")
    if strict and (not jwt_supplied or not refresh_supplied):
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
    jwt_secret = jwt_supplied or DEVELOPMENT_JWT_SECRET
    refresh_secret = refresh_supplied or jwt_secret
    if strict and (
        jwt_secret == DEVELOPMENT_JWT_SECRET
        or jwt_secret == refresh_secret
        or not _strong_secret(jwt_secret)
        or not _strong_secret(refresh_secret)
    ):
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
    try:
        access_lifetime = int(values.get("ACCESS_TOKEN_LIFETIME_MINUTES", "15"))
    except (TypeError, ValueError) as exc:
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid") from exc
    if not 1 <= access_lifetime <= 1_440:
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
    return Settings(
        database.runtime_environment,
        database.database_url,
        jwt_secret,
        access_lifetime,
        refresh_secret,
    )


def load_github_app_settings_from_environment(
    environ: Mapping[str, str] | None = None,
) -> GitHubAppSettings:
    values = os.environ if environ is None else environ
    required = (
        "GITHUB_APP_ID",
        "GITHUB_APP_SLUG",
        "GITHUB_APP_CLIENT_ID",
        "GITHUB_APP_CLIENT_SECRET_REFERENCE",
        "GITHUB_APP_PRIVATE_KEY_REFERENCE",
        "GITHUB_APP_CALLBACK_URL",
        "GITHUB_APP_SETUP_URL",
    )
    _require_values(values, required, "GitHub App configuration is incomplete")
    return GitHubAppSettings(
        app_id=_integer(values, "GITHUB_APP_ID", "GitHub App configuration is invalid"),
        app_slug=values["GITHUB_APP_SLUG"],
        client_id=values["GITHUB_APP_CLIENT_ID"],
        client_secret_reference=SecretReference(
            values["GITHUB_APP_CLIENT_SECRET_REFERENCE"]
        ),
        private_key_reference=SecretReference(
            values["GITHUB_APP_PRIVATE_KEY_REFERENCE"]
        ),
        callback_url=values["GITHUB_APP_CALLBACK_URL"],
        setup_url=values["GITHUB_APP_SETUP_URL"],
        api_base_url=values.get("GITHUB_API_BASE_URL", "https://api.github.com"),
        web_base_url=values.get("GITHUB_WEB_BASE_URL", "https://github.com"),
        request_timeout_seconds=_float(values, "GITHUB_REQUEST_TIMEOUT_SECONDS", 10.0),
        max_retries=_optional_integer(values, "GITHUB_MAX_RETRIES", 2),
    )


def load_github_worker_settings_from_environment(
    environ: Mapping[str, str] | None = None,
) -> GitHubWorkerSettings:
    values = os.environ if environ is None else environ
    required = (
        "GITHUB_APP_ID",
        "GITHUB_APP_CLIENT_ID",
        "GITHUB_APP_PRIVATE_KEY_REFERENCE",
    )
    _require_values(values, required, "GitHub worker configuration is incomplete")
    return GitHubWorkerSettings(
        app_id=_integer(values, "GITHUB_APP_ID", "GitHub worker configuration is invalid"),
        client_id=values["GITHUB_APP_CLIENT_ID"],
        private_key_reference=SecretReference(
            values["GITHUB_APP_PRIVATE_KEY_REFERENCE"]
        ),
        api_base_url=values.get("GITHUB_API_BASE_URL", "https://api.github.com"),
        web_base_url=values.get("GITHUB_WEB_BASE_URL", "https://github.com"),
        request_timeout_seconds=_float(values, "GITHUB_REQUEST_TIMEOUT_SECONDS", 10.0),
        max_retries=_optional_integer(values, "GITHUB_MAX_RETRIES", 2),
    )


def load_google_secret_manager_settings_from_environment(
    environ: Mapping[str, str] | None = None,
) -> GoogleSecretManagerSettings:
    values = os.environ if environ is None else environ
    required = (
        "GCP_SECRET_MANAGER_PROJECT_ID",
        "GCP_SECRET_MANAGER_SECRET_PREFIX",
        "GCP_SECRET_MANAGER_ENVIRONMENT",
    )
    _require_values(values, required, "Google Secret Manager configuration is incomplete")
    return GoogleSecretManagerSettings(
        project_id=values["GCP_SECRET_MANAGER_PROJECT_ID"],
        secret_prefix=values["GCP_SECRET_MANAGER_SECRET_PREFIX"],
        environment=values["GCP_SECRET_MANAGER_ENVIRONMENT"],
    )


def load_api_port(environ: Mapping[str, str] | None = None) -> int:
    values = os.environ if environ is None else environ
    runtime_environment = load_runtime_environment(values)
    supplied = values.get("PORT")
    if runtime_environment in STRICT_RUNTIME_ENVIRONMENTS and not supplied:
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
    if supplied is not None and re.fullmatch(r"[1-9][0-9]{0,4}", supplied) is None:
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
    try:
        port = DEFAULT_API_PORT if supplied is None else int(supplied)
    except (TypeError, ValueError) as exc:
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid") from exc
    if not 1 <= port <= 65_535:
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
    return port


def validate_api_process_environment(
    environ: Mapping[str, str] | None = None,
) -> ApiProcessSettings:
    values = os.environ if environ is None else environ
    application = load_application_settings(values)
    github: GitHubAppSettings | None = None
    secret_manager: GoogleSecretManagerSettings | None = None
    try:
        github = load_github_app_settings_from_environment(values)
        secret_manager = load_google_secret_manager_settings_from_environment(values)
        if (
            application.runtime_environment in STRICT_RUNTIME_ENVIRONMENTS
            and secret_manager.environment != application.runtime_environment
        ):
            raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
        _validate_google_reference(github.client_secret_reference, secret_manager)
        _validate_google_reference(github.private_key_reference, secret_manager)
    except (ValueError, InvalidRuntimeConfiguration):
        if application.runtime_environment in STRICT_RUNTIME_ENVIRONMENTS:
            raise InvalidRuntimeConfiguration("Runtime configuration is invalid") from None
    return ApiProcessSettings(application, github, secret_manager, load_api_port(values))


def validate_worker_process_environment(
    environ: Mapping[str, str] | None = None,
) -> WorkerProcessSettings:
    values = os.environ if environ is None else environ
    database = load_database_settings(values)
    github = load_github_worker_settings_from_environment(values)
    secret_manager = load_google_secret_manager_settings_from_environment(values)
    if (
        database.runtime_environment in STRICT_RUNTIME_ENVIRONMENTS
        and secret_manager.environment != database.runtime_environment
    ):
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
    _validate_google_reference(github.private_key_reference, secret_manager)
    if database.runtime_environment in STRICT_RUNTIME_ENVIRONMENTS:
        api_key = values.get("OPENAI_API_KEY")
        if (
            not isinstance(api_key, str)
            or len(api_key) < 20
            or any(character.isspace() for character in api_key)
        ):
            raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
    ledger_planning_enabled = _optional_strict_boolean(
        values,
        "GITHUB_SYNC_LEDGER_PLANNING_ENABLED",
        default=False,
    )
    return WorkerProcessSettings(
        database,
        github,
        secret_manager,
        ledger_planning_enabled,
    )


def validate_scheduler_process_environment(
    environ: Mapping[str, str] | None = None,
) -> DatabaseSettings:
    return load_database_settings(environ)


def validate_migration_process_environment(
    environ: Mapping[str, str] | None = None,
) -> DatabaseSettings:
    return load_database_settings(environ)


@lru_cache(maxsize=1)
def get_database_settings() -> DatabaseSettings:
    return load_database_settings()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_application_settings()


def _validate_github_provider_settings(
    value: GitHubAppSettings | GitHubWorkerSettings,
) -> None:
    if isinstance(value.app_id, bool) or not isinstance(value.app_id, int) or value.app_id < 1:
        raise ValueError("GitHub App ID is invalid")
    if not isinstance(value.client_id, str) or not re.fullmatch(
        r"[A-Za-z0-9._-]{6,255}", value.client_id
    ):
        raise ValueError("GitHub App client ID is invalid")
    if value.client_id == str(value.app_id):
        raise ValueError("GitHub App identifiers must be distinct")
    if not isinstance(value.private_key_reference, SecretReference):
        raise ValueError("GitHub secret configuration is invalid")
    _validate_secret_reference(value.private_key_reference)
    for url in (value.api_base_url, value.web_base_url):
        _validate_https_url(url)
        parsed = urlparse(url)
        if parsed.query or parsed.path not in {"", "/"}:
            raise ValueError("GitHub base URL configuration is invalid")
    if (
        isinstance(value.request_timeout_seconds, bool)
        or not isinstance(value.request_timeout_seconds, (int, float))
        or not 0.1 <= value.request_timeout_seconds <= 60
    ):
        raise ValueError("GitHub request timeout is invalid")
    if (
        isinstance(value.max_retries, bool)
        or not isinstance(value.max_retries, int)
        or not 0 <= value.max_retries <= 3
    ):
        raise ValueError("GitHub retry count is invalid")


def _validate_https_url(value: object) -> None:
    if not isinstance(value, str):
        raise ValueError("GitHub URL configuration is invalid")
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.fragment
        or "*" in value
    ):
        raise ValueError("GitHub URL configuration is invalid")


def _validate_secret_reference(reference: SecretReference) -> None:
    if not re.fullmatch(r"[a-z][a-z0-9+.-]*://[^\s]+", reference.value):
        raise ValueError("GitHub secret reference is invalid")


def _validate_google_reference(
    reference: SecretReference, settings: GoogleSecretManagerSettings
) -> None:
    pattern = re.compile(
        rf"gcp-secret-manager://projects/{re.escape(settings.project_id)}/secrets/"
        rf"{re.escape(settings.secret_prefix)}-sm-[0-9a-f]{{32}}/versions/[1-9][0-9]*"
    )
    if not pattern.fullmatch(reference.value):
        raise InvalidRuntimeConfiguration("Runtime configuration is invalid")


def _is_development_database(value: object) -> bool:
    try:
        candidate = (
            value
            if hasattr(value, "render_as_string")
            else make_url(str(value))
        )
        if candidate.host and candidate.host.casefold() in {
            "localhost",
            "127.0.0.1",
            "::1",
        }:
            return True
        rendered = candidate.render_as_string(hide_password=False).casefold()
        host = (candidate.host or "").casefold()
        password = (candidate.password or "").casefold()
        if (
            any(
                marker in rendered
                for marker in ("<", ">", "{", "}", "placeholder", "changeme")
            )
            or host in {"example.com", "example.net", "example.org"}
            or host.endswith((".example", ".invalid", ".test"))
            or password in {"password", "change-me", "secret", "example"}
        ):
            return True
        return candidate.render_as_string(hide_password=False) == make_url(
            DEVELOPMENT_DATABASE_URL
        ).render_as_string(hide_password=False)
    except (ArgumentError, TypeError, ValueError):
        return False


def _strong_secret(value: object) -> bool:
    if not isinstance(value, str) or len(value) < MINIMUM_SECRET_LENGTH:
        return False
    if any(character.isspace() for character in value):
        return False
    if any(
        marker in value.casefold()
        for marker in ("changeme", "change-me", "placeholder")
    ):
        return False
    classes = (
        any(character.islower() for character in value),
        any(character.isupper() for character in value),
        any(character.isdigit() for character in value),
        any(not character.isalnum() for character in value),
    )
    return sum(classes) >= 3


def _require_values(
    values: Mapping[str, str], names: tuple[str, ...], message: str
) -> None:
    if any(not values.get(name) for name in names):
        raise ValueError(message)


def _integer(values: Mapping[str, str], name: str, message: str) -> int:
    try:
        return int(values[name])
    except (TypeError, ValueError) as exc:
        raise ValueError(message) from exc


def _optional_integer(values: Mapping[str, str], name: str, default: int) -> int:
    try:
        return int(values.get(name, str(default)))
    except (TypeError, ValueError) as exc:
        raise ValueError("GitHub App configuration is invalid") from exc


def _float(values: Mapping[str, str], name: str, default: float) -> float:
    try:
        return float(values.get(name, str(default)))
    except (TypeError, ValueError) as exc:
        raise ValueError("GitHub App configuration is invalid") from exc


def _optional_strict_boolean(
    values: Mapping[str, str], name: str, *, default: bool
) -> bool:
    supplied = values.get(name)
    if supplied is None:
        return default
    if supplied == "true":
        return True
    if supplied == "false":
        return False
    raise InvalidRuntimeConfiguration("Runtime configuration is invalid")
