from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
import pytest

import app.main as main_module
from app.config import (
    GoogleSecretManagerSettings,
    load_google_secret_manager_settings_from_environment,
)
from application.ports.secret_store import SecretStoreUnavailable


PROJECT = "platform-prod-1"
TOKEN = "a" * 32
REFERENCE = (
    "gcp-secret-manager://projects/platform-prod-1/secrets/"
    f"eap-sm-{TOKEN}/versions/1"
)


def set_environment(monkeypatch) -> None:
    values = {
        "GCP_SECRET_MANAGER_PROJECT_ID": PROJECT,
        "GCP_SECRET_MANAGER_SECRET_PREFIX": "eap",
        "GCP_SECRET_MANAGER_ENVIRONMENT": "production",
        "GITHUB_APP_ID": "12345",
        "GITHUB_APP_SLUG": "enterprise-ai",
        "GITHUB_APP_CLIENT_ID": "Iv1.client-id",
        "GITHUB_APP_CLIENT_SECRET_REFERENCE": REFERENCE,
        "GITHUB_APP_PRIVATE_KEY_REFERENCE": REFERENCE.replace(TOKEN, "b" * 32),
        "GITHUB_APP_CALLBACK_URL": "https://platform.test/api/v1/connectors/github/callback",
        "GITHUB_APP_SETUP_URL": "https://platform.test/api/v1/connectors/github/setup",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def test_nonsecret_configuration_is_strict_and_redacted(monkeypatch):
    set_environment(monkeypatch)
    value = load_google_secret_manager_settings_from_environment()
    assert value == GoogleSecretManagerSettings(PROJECT, "eap", "production")
    assert PROJECT not in repr(value)
    assert set(value.__dict__) == {"project_id", "secret_prefix", "environment"}
    for bad in ("Project_One", "projects/project-one", "project-one?x"):
        with pytest.raises(ValueError, match="project configuration is invalid"):
            GoogleSecretManagerSettings(bad, "eap", "production")
    with pytest.raises(ValueError, match="prefix configuration is invalid"):
        GoogleSecretManagerSettings(PROJECT, "tenant/id", "production")
    with pytest.raises(ValueError, match="environment configuration is invalid"):
        GoogleSecretManagerSettings(PROJECT, "eap", "Production Env")


def test_missing_configuration_leaves_github_operations_fail_closed(monkeypatch):
    application = FastAPI()
    application.state.secret_store = object()
    application.state.github_app_settings = object()
    for name in (
        "GCP_SECRET_MANAGER_PROJECT_ID",
        "GCP_SECRET_MANAGER_SECRET_PREFIX",
        "GCP_SECRET_MANAGER_ENVIRONMENT",
    ):
        monkeypatch.delenv(name, raising=False)
    assert main_module.configure_github_app_from_environment(application) is False
    assert not hasattr(application.state, "secret_store")
    assert not hasattr(application.state, "github_app_settings")


def test_credentials_or_adapter_initialization_failure_is_fail_closed(monkeypatch):
    set_environment(monkeypatch)
    application = FastAPI()

    def unavailable(_settings):
        raise SecretStoreUnavailable("FAKE credential details")

    monkeypatch.setattr(main_module, "GoogleSecretManagerSecretStore", unavailable)
    assert main_module.configure_github_app_from_environment(application) is False
    assert not hasattr(application.state, "secret_store")


def test_complete_configuration_constructs_adc_adapter_and_validates_both_references(monkeypatch):
    set_environment(monkeypatch)
    application = FastAPI()
    captured = SimpleNamespace(settings=None, references=[])

    class Store:
        def __init__(self, settings):
            captured.settings = settings

        def validate_reference(self, reference):
            captured.references.append(reference.value)

        def store(self, value):
            raise AssertionError("no provider operation expected during composition")

        def retrieve(self, reference):
            raise AssertionError("no provider operation expected during composition")

        def delete(self, reference):
            raise AssertionError("no provider operation expected during composition")

    monkeypatch.setattr(main_module, "GoogleSecretManagerSecretStore", Store)
    assert main_module.configure_github_app_from_environment(application) is True
    assert captured.settings == GoogleSecretManagerSettings(PROJECT, "eap", "production")
    assert captured.references == [REFERENCE, REFERENCE.replace(TOKEN, "b" * 32)]
    assert isinstance(application.state.secret_store, Store)
    assert application.state.github_app_settings.private_key_reference.value.endswith("/versions/1")
