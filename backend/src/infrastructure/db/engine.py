"""Reusable SQLAlchemy engine for the platform database."""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy import Engine, create_engine

from app.config import get_settings


@lru_cache(maxsize=1)
def _build_engine() -> Engine:
    settings = get_settings()
    return create_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        pool_recycle=1800,
        future=True,
    )


engine = _build_engine()
