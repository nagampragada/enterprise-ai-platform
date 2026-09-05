"""Database health checks for the platform database."""

from __future__ import annotations

from dataclasses import dataclass
import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from infrastructure.db.engine import engine


logger = logging.getLogger(__name__)
EXPECTED_ALEMBIC_REVISION = "20260905_000024"
COMPATIBLE_ALEMBIC_REVISIONS = frozenset(
    {
        "20260904_000023",
        EXPECTED_ALEMBIC_REVISION,
    }
)
READINESS_STATEMENT_TIMEOUT_MILLISECONDS = 2_000


@dataclass(frozen=True)
class DatabaseHealthResult:
    healthy: bool
    message: str
    schema_compatible: bool = False
    schema_current: bool = False

    @property
    def migration_required(self) -> bool:
        return self.schema_compatible and not self.schema_current


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
            revisions = tuple(
                connection.execute(
                    text(
                        "SELECT version_num FROM alembic_version "
                        "ORDER BY version_num LIMIT 2"
                    )
                )
                .scalars()
                .all()
            )
        revision = revisions[0] if len(revisions) == 1 else None
        schema_compatible = revision in COMPATIBLE_ALEMBIC_REVISIONS
        return DatabaseHealthResult(
            healthy=True,
            message="Database connection is healthy.",
            schema_compatible=schema_compatible,
            schema_current=revision == EXPECTED_ALEMBIC_REVISION,
        )
    except SQLAlchemyError:
        logger.warning("event=database_readiness_failed")
        return DatabaseHealthResult(healthy=False, message="Database connection check failed.")
    except Exception:
        logger.warning("event=database_readiness_failed")
        return DatabaseHealthResult(healthy=False, message="Database connection check failed.")
