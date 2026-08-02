"""Database health checks for the platform database."""

from __future__ import annotations

from dataclasses import dataclass
import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from infrastructure.db.engine import engine


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatabaseHealthResult:
    healthy: bool
    message: str


def check_database_connection() -> DatabaseHealthResult:
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return DatabaseHealthResult(healthy=True, message="Database connection is healthy.")
    except SQLAlchemyError:
        logger.exception("Database connection health check failed")
        return DatabaseHealthResult(healthy=False, message="Database connection check failed.")
    except Exception:
        logger.exception("Unexpected database connection health check failure")
        return DatabaseHealthResult(healthy=False, message="Database connection check failed.")
