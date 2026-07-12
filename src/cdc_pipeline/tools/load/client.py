"""The async HTTP client that turns :class:`Operation`s into API calls, plus the startup
bootstrap that samples existing keys.

The client never touches Postgres — it speaks only HTTP to the tracking API, so the load
traverses the exact write path production traffic would (ARCHITECTURE.md §2).
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path

import httpx

from cdc_pipeline.config import load_dotenv
from cdc_pipeline.tools.load.model import Operation, OpKind, Result
from cdc_pipeline.tools.load.planner import Bootstrap

_REPO_ROOT = Path(__file__).resolve().parents[4]


def resolve_api_url(explicit: str | None = None) -> str:
    """Resolve the API base URL: ``--api-url`` > ``$API_URL`` > ``http://localhost:$API_PORT``.

    Honours the repo's "change a port once, in ``.env``" rule by falling back to
    ``API_PORT`` (default 8000) rather than hard-coding a URL.
    """
    if explicit:
        return explicit.rstrip("/")
    load_dotenv(_REPO_ROOT / ".env")
    if url := os.environ.get("API_URL"):
        return url.rstrip("/")
    port = os.environ.get("API_PORT", "8000")
    return f"http://localhost:{port}"


@dataclass
class LoadClient:
    """Thin async wrapper over :class:`httpx.AsyncClient` with per-op result mapping."""

    base_url: str
    concurrency: int
    timeout: float = 10.0
    _client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> LoadClient:
        # Size the connection pool to the worker count so concurrency isn't throttled by
        # httpx's default keepalive limit.
        limits = httpx.Limits(
            max_connections=self.concurrency + 8,
            max_keepalive_connections=self.concurrency + 8,
        )
        self._client = httpx.AsyncClient(
            base_url=self.base_url, timeout=self.timeout, limits=limits
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def execute(self, op: Operation) -> Result:
        assert self._client is not None, "use LoadClient as an async context manager"
        start = time.perf_counter()
        try:
            resp = await self._client.request(op.method, op.path, json=op.json)
        except httpx.HTTPError as exc:
            latency_ms = (time.perf_counter() - start) * 1000
            return Result(
                op.kind, ok=False, status_code=None, latency_ms=latency_ms, error=type(exc).__name__
            )
        latency_ms = (time.perf_counter() - start) * 1000
        ok = 200 <= resp.status_code < 300
        created_id: int | None = None
        if ok and op.kind is OpKind.SPECIMEN_INSERT:
            try:
                created_id = int(resp.json()["id"])
            except (ValueError, KeyError, TypeError):
                created_id = None
        return Result(
            op.kind,
            ok=ok,
            status_code=resp.status_code,
            latency_ms=latency_ms,
            created_id=created_id,
            error=None if ok else f"HTTP {resp.status_code}",
        )

    async def health_ok(self) -> bool:
        assert self._client is not None
        try:
            resp = await self._client.get("/health")
        except httpx.HTTPError:
            return False
        return resp.status_code == 200

    async def bootstrap(self, pool_size: int) -> Bootstrap:
        """Sample up to ``pool_size`` existing specimens (with statuses) and all runs.

        Pages the list endpoints (max page 500) so the Zipf universe reflects real seeded
        keys. Raises ``RuntimeError`` if the database looks empty — run ``make seed`` first.
        """
        assert self._client is not None
        specimen_ids: list[int] = []
        specimen_status: dict[int, str] = {}
        registered: list[int] = []
        offset = 0
        page = 500
        while len(specimen_ids) < pool_size:
            resp = await self._client.get("/specimens", params={"limit": page, "offset": offset})
            resp.raise_for_status()
            rows = resp.json()
            if not rows:
                break
            for row in rows:
                sid = int(row["id"])
                specimen_ids.append(sid)
                specimen_status[sid] = row["status"]
                if row["status"] == "registered":
                    registered.append(sid)
            offset += len(rows)
            if len(rows) < page:
                break

        run_ids: list[int] = []
        offset = 0
        while True:
            resp = await self._client.get("/runs", params={"limit": page, "offset": offset})
            resp.raise_for_status()
            rows = resp.json()
            if not rows:
                break
            run_ids.extend(int(row["id"]) for row in rows)
            offset += len(rows)
            if len(rows) < page:
                break

        if not specimen_ids:
            raise RuntimeError(
                "no specimens found via the API — is the stack seeded? Run `make seed`."
            )
        return Bootstrap(
            specimen_ids=specimen_ids,
            specimen_status=specimen_status,
            registered_ids=registered,
            run_ids=run_ids,
        )
