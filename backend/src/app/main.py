"""Minimal FastAPI application entrypoint."""

from __future__ import annotations

from fastapi import FastAPI

from app.api.router import api_router
from app.config import (
    GitHubAppSettings,
    STRICT_RUNTIME_ENVIRONMENTS,
    load_runtime_environment,
    load_github_app_settings_from_environment,
    load_google_secret_manager_settings_from_environment,
    validate_api_process_environment,
)
from application.ports.secret_store import SecretStore
from infrastructure.secrets.google_secret_manager import GoogleSecretManagerSecretStore

app = FastAPI(
    title="Enterprise AI Platform API",
    version="0.1.0",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy"}


app.include_router(api_router, prefix="/api/v1")


def configure_github_app(application:FastAPI,secret_store:SecretStore,*,
    settings:GitHubAppSettings|None=None)->None:
    """Install secure GitHub composition; production must inject its SecretStore adapter."""
    if any(not callable(getattr(secret_store,name,None)) for name in ("store","retrieve","delete")):
        raise ValueError("Secure secret storage is unavailable")
    resolved=settings or load_github_app_settings_from_environment()
    application.state.secret_store=secret_store
    application.state.github_app_settings=resolved
    application.state.github_composition_ready=True


def configure_github_app_from_environment(application: FastAPI) -> bool:
    """Compose production secrets with ADC, or leave GitHub fail-closed."""
    try:
        secret_settings = load_google_secret_manager_settings_from_environment()
        github_settings = load_github_app_settings_from_environment()
        secret_store = GoogleSecretManagerSecretStore(secret_settings)
        secret_store.validate_reference(github_settings.client_secret_reference)
        secret_store.validate_reference(github_settings.private_key_reference)
    except Exception:
        for name in ("secret_store", "github_app_settings"):
            if hasattr(application.state, name):
                delattr(application.state, name)
        application.state.github_composition_ready=False
        return False
    configure_github_app(application, secret_store, settings=github_settings)
    return True


_runtime_environment = load_runtime_environment()
if _runtime_environment in STRICT_RUNTIME_ENVIRONMENTS:
    try:
        validate_api_process_environment()
    except Exception:
        raise RuntimeError("API runtime configuration is invalid") from None
    if not configure_github_app_from_environment(app):
        raise RuntimeError("API runtime composition is unavailable")
else:
    configure_github_app_from_environment(app)
