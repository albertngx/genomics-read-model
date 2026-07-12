"""FastAPI application for the tracking service — the only writer to Postgres.

The application writes runs and specimens to Postgres and holds no reference to any
downstream store, log, or consumer (ARCHITECTURE.md §2). Derived stores are built from
Postgres's change stream, so a new consumer is added without modifying this package. The
constraint is verifiable by grepping this package for the downstream systems' names (see
story 02.2's acceptance criteria).

Swagger UI is served at ``/docs`` and ReDoc at ``/redoc`` (FastAPI defaults).

Run it with::

    make api                       # uvicorn on $API_PORT, with reload
    uv run uvicorn cdc_pipeline.api.app:app --reload
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response

from cdc_pipeline.api.routers import health, runs, specimens

logger = logging.getLogger("cdc_pipeline.api")


def _configure_logging() -> None:
    """Give the API logger its own handler at INFO so request lines show up regardless of
    how the app is launched (uvicorn configures the ``uvicorn`` logger, not ours).
    Idempotent, and ``propagate=False`` avoids double-printing under uvicorn."""
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


_DESCRIPTION = (
    "Source-of-truth tracking service for sequencing runs and specimens. The only writer "
    "to Postgres; holds no reference to any downstream store or consumer (ARCHITECTURE.md §2)."
)


def create_app() -> FastAPI:
    """Build the FastAPI application (factory so tests can construct fresh instances)."""
    _configure_logging()
    app = FastAPI(
        title="cdc-read-model tracking API",
        description=_DESCRIPTION,
        version="0.1.0",
    )

    @app.middleware("http")
    async def log_requests(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Log each request: method, path, response status, and wall-clock latency.

        The latency field is the source of the write-latency baseline recorded in
        docs/pipeline/tracking-service.md.
        """
        start = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "%s %s -> %d (%.1f ms)",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )
        return response

    app.include_router(health.router)
    app.include_router(runs.router)
    app.include_router(specimens.router)
    return app


# Module-level instance for `uvicorn cdc_pipeline.api.app:app`.
app = create_app()
