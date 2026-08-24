from __future__ import annotations

import pytest

from app.config import (
    DEFAULT_API_PORT,
    DEVELOPMENT_DATABASE_URL,
    InvalidRuntimeConfiguration,
    load_api_port,
    load_application_settings,
    load_database_settings,
    validate_api_process_environment,
    validate_migration_process_environment,
    validate_scheduler_process_environment,
    validate_worker_process_environment,
)


SAFE_DATABASE = "postgresql+psycopg://sandbox_user:S4ndboxDbValue9284@db.internal:5432/platform"
JWT_SECRET = "Sandbox-JWT-secret-value-1234567890!"
REFRESH_SECRET = "Sandbox-Refresh-secret-value-098765!"
TOKEN = "0123456789abcdef0123456789abcdef"


def _sandbox() -> dict[str, str]:
    return {
        "APP_ENVIRONMENT": "sandbox",
        "DATABASE_URL": SAFE_DATABASE,
        "JWT_SECRET_KEY": JWT_SECRET,
        "REFRESH_TOKEN_HASH_SECRET": REFRESH_SECRET,
        "PORT": "8080",
        "GITHUB_APP_ID": "12345",
        "GITHUB_APP_SLUG": "sandbox-app",
        "GITHUB_APP_CLIENT_ID": "Iv1.sandbox-client",
        "GITHUB_APP_CLIENT_SECRET_REFERENCE": (
            "gcp-secret-manager://projects/sandbox-proj-1/secrets/"
            f"eap-sm-{TOKEN}/versions/1"
        ),
        "GITHUB_APP_PRIVATE_KEY_REFERENCE": (
            "gcp-secret-manager://projects/sandbox-proj-1/secrets/"
            f"eap-sm-{TOKEN}/versions/2"
        ),
        "GITHUB_APP_CALLBACK_URL": (
            "https://sandbox-api.example.run.app/api/v1/connectors/github/callback"
        ),
        "GITHUB_APP_SETUP_URL": (
            "https://sandbox-api.example.run.app/api/v1/connectors/github/setup"
        ),
        "GCP_SECRET_MANAGER_PROJECT_ID": "sandbox-proj-1",
        "GCP_SECRET_MANAGER_SECRET_PREFIX": "eap",
        "GCP_SECRET_MANAGER_ENVIRONMENT": "sandbox",
        "OPENAI_API_KEY": "sk-sandbox-value-1234567890",
    }


def test_development_defaults_are_deliberate_and_api_composition_is_optional() -> None:
    settings = load_application_settings({"APP_ENVIRONMENT": "development"})
    assert settings.database_url == DEVELOPMENT_DATABASE_URL
    assert validate_api_process_environment({"APP_ENVIRONMENT": "development"}).port == DEFAULT_API_PORT


@pytest.mark.parametrize(
    "updates",
    (
        {"DATABASE_URL": None},
        {"DATABASE_URL": DEVELOPMENT_DATABASE_URL},
        {"JWT_SECRET_KEY": None},
        {"REFRESH_TOKEN_HASH_SECRET": None},
        {"REFRESH_TOKEN_HASH_SECRET": JWT_SECRET},
    ),
)
def test_sandbox_rejects_unsafe_core_configuration(updates) -> None:
    values = _sandbox()
    for name, value in updates.items():
        if value is None:
            values.pop(name)
        else:
            values[name] = value
    with pytest.raises(InvalidRuntimeConfiguration, match="Runtime configuration is invalid"):
        load_application_settings(values)


@pytest.mark.parametrize(
    "database_url",
    (
        "postgresql+psycopg://user:S4ndboxDbValue9284@database.invalid:5432/platform",
        "postgresql+psycopg://user:password@db.internal:5432/platform",
        "postgresql+psycopg://user:S4ndboxDbValue9284@placeholder-host:5432/platform",
    ),
)
def test_sandbox_rejects_placeholder_database_urls(database_url: str) -> None:
    values = _sandbox()
    values["DATABASE_URL"] = database_url
    with pytest.raises(InvalidRuntimeConfiguration):
        load_database_settings(values)


@pytest.mark.parametrize("value", ("0", "65536", "not-a-port", " 8080"))
def test_port_validation_rejects_malformed_or_out_of_range_values(value: str) -> None:
    values = _sandbox()
    values["PORT"] = value
    with pytest.raises(InvalidRuntimeConfiguration):
        load_api_port(values)


def test_safe_sandbox_api_and_worker_composition_passes_without_provider_calls() -> None:
    values = _sandbox()
    assert validate_api_process_environment(values).github is not None
    assert validate_worker_process_environment(values).github.private_key_reference.value.endswith(
        "/versions/2"
    )


def test_api_and_worker_require_only_their_relevant_sandbox_inputs() -> None:
    values = _sandbox()
    values.pop("GITHUB_APP_CLIENT_SECRET_REFERENCE")
    with pytest.raises(InvalidRuntimeConfiguration):
        validate_api_process_environment(values)
    assert validate_worker_process_environment(values).github.app_id == 12345

    values = _sandbox()
    values.pop("OPENAI_API_KEY")
    with pytest.raises(InvalidRuntimeConfiguration):
        validate_worker_process_environment(values)


def test_scheduler_and_migration_require_database_only() -> None:
    values = {"APP_ENVIRONMENT": "sandbox", "DATABASE_URL": SAFE_DATABASE}
    assert validate_scheduler_process_environment(values).database_url == SAFE_DATABASE
    assert validate_migration_process_environment(values).database_url == SAFE_DATABASE
    assert load_database_settings(values).runtime_environment == "sandbox"
