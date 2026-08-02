"""Configuration helpers for backend infrastructure."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True, repr=False)
class Settings:
    database_url: str


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://enterprise_ai_platform:enterprise_ai_platform@localhost:5432/enterprise_ai_platform",
    )
    return Settings(database_url=database_url)
