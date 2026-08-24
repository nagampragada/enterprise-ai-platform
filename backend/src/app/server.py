"""Validated single-process Uvicorn launcher for the backend API."""

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence

from app.config import validate_api_process_environment


LOGGER = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    """Start the API without shell interpolation or development-only options."""
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    if arguments:
        LOGGER.error("event=api_startup_failed error_type=InvalidArguments")
        return 1
    try:
        settings = validate_api_process_environment()
        _run_uvicorn(
            "app.main:app",
            host="0.0.0.0",
            port=settings.port,
            reload=False,
            workers=1,
        )
    except Exception as error:
        LOGGER.error("event=api_startup_failed error_type=%s", type(error).__name__)
        return 1
    return 0


def _run_uvicorn(application: str, **kwargs: object) -> None:
    import uvicorn

    uvicorn.run(application, **kwargs)


if __name__ == "__main__":
    raise SystemExit(main())
