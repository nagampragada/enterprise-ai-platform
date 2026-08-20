"""Minimal FastAPI application entrypoint."""

from __future__ import annotations

from fastapi import FastAPI

from app.api.router import api_router
from app.config import GitHubAppSettings,load_github_app_settings_from_environment
from application.ports.secret_store import SecretStore

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
