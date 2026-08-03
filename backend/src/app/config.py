"""Configuration helpers for backend infrastructure."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True, repr=False)
class Settings:
    database_url: str
    jwt_secret_key: str
    access_token_lifetime_minutes: int
    refresh_token_hash_secret: str


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
