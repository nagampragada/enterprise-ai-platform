"""Database health checks for the platform database."""

from __future__ import annotations

from dataclasses import dataclass
import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from infrastructure.db.engine import engine


logger = logging.getLogger(__name__)
EXPECTED_ALEMBIC_REVISION = "20260831_000021"
READINESS_STATEMENT_TIMEOUT_MILLISECONDS = 2_000


@dataclass(frozen=True)
class DatabaseHealthResult:
    healthy: bool
    message: str
    schema_current: bool = False


def check_database_connection() -> DatabaseHealthResult:
    try:
        with engine.connect() as connection:
            connection.execute(
                text(
                    f"SET LOCAL statement_timeout = "
                    f"'{READINESS_STATEMENT_TIMEOUT_MILLISECONDS}ms'"
                )
            )
            connection.execute(text("SELECT 1"))
            revision = connection.execute(
                text("SELECT version_num FROM alembic_version LIMIT 1")
            ).scalar_one_or_none()
        return DatabaseHealthResult(
            healthy=True,
            message="Database connection is healthy.",
            schema_current=revision == EXPECTED_ALEMBIC_REVISION,
        )
    except SQLAlchemyError:
        logger.warning("event=database_readiness_failed")
        return DatabaseHealthResult(healthy=False, message="Database connection check failed.")
    except Exception:
        logger.warning("event=database_readiness_failed")
        return DatabaseHealthResult(healthy=False, message="Database connection check failed.")
