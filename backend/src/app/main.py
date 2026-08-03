"""Minimal FastAPI application entrypoint."""

from __future__ import annotations

from fastapi import FastAPI

from app.api.router import api_router

app = FastAPI(
    title="Enterprise AI Platform API",
    version="0.1.0",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy"}


app.include_router(api_router, prefix="/api/v1")
