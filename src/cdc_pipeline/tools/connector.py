"""``cdc-connector`` — register and inspect the Debezium CDC connector (epic 03.1).

The connector config lives in ``deploy/connectors/genomics.json`` as a flat Kafka Connect
config map with ``${VAR}`` placeholders for credentials. This
module fills those from the repo-root ``.env`` (the single source of truth — no secret is
hardcoded in the JSON) and drives the Connect REST API on ``KAFKA_CONNECT_URL``.

Subcommands (see ``make connector`` / ``connector-status`` / ``connector-delete``)::

    cdc-connector register   # idempotent PUT /connectors/genomics/config
    cdc-connector status     # GET  /connectors/genomics/status  (connector + tasks)
    cdc-connector delete     # DELETE /connectors/genomics       (keeps the WAL slot)

``register`` is idempotent by construction: Connect's ``PUT …/config`` creates the
connector if absent and updates it in place if present, so re-running never errors and
never re-snapshots an existing slot.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from string import Template

import httpx
import typer

from cdc_pipeline.config import Settings, load_dotenv

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CONFIG_PATH = _REPO_ROOT / "deploy" / "connectors" / "genomics.json"

# The connector's REST name — also the basename of its config file and the slot's sibling.
CONNECTOR_NAME = "genomics"


class ConnectorError(RuntimeError):
    """A connector operation could not be completed (bad config, unreachable Connect)."""


def render_config(path: Path = _CONFIG_PATH, env: dict[str, str] | None = None) -> dict[str, str]:
    """Load the connector JSON and expand ``${VAR}`` placeholders in its string values.

    Substitution is confined to values (keys never contain placeholders) and is *strict*:
    a ``${VAR}`` with no matching environment variable raises rather than silently PUTting
    an empty credential. Returns the ready-to-POST flat config map.
    """
    env = dict(os.environ if env is None else env)
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError as exc:
        raise ConnectorError(f"connector config not found at {path}") from exc
    except json.JSONDecodeError as exc:
        raise ConnectorError(f"connector config at {path} is not valid JSON: {exc}") from exc

    rendered: dict[str, str] = {}
    for key, value in raw.items():
        if isinstance(value, str):
            try:
                value = Template(value).substitute(env)
            except KeyError as exc:
                missing = exc.args[0]
                raise ConnectorError(
                    f"connector config references ${{{missing}}} but it is not set. "
                    f"Copy .env.example to .env (scripts/bootstrap.sh does this) and set it."
                ) from exc
        rendered[key] = value
    return rendered


def _settings() -> Settings:
    """Load the repo-root ``.env`` (without clobbering) and build settings."""
    load_dotenv(_REPO_ROOT / ".env")
    return Settings.from_env()


def _connect_base(settings: Settings) -> str:
    return settings.kafka_connect_url.rstrip("/")


app = typer.Typer(help="Register and inspect the Debezium CDC connector.", no_args_is_help=True)


@app.command()
def register() -> None:
    """Idempotently register/update the connector (PUT /connectors/<name>/config)."""
    settings = _settings()
    config = render_config()
    url = f"{_connect_base(settings)}/connectors/{CONNECTOR_NAME}/config"
    try:
        resp = httpx.put(url, json=config, timeout=30.0)
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        # Connect returns a JSON body explaining *why* a config was rejected — surface it.
        raise ConnectorError(
            f"Connect rejected the config (HTTP {exc.response.status_code}): {exc.response.text}"
        ) from exc
    except httpx.HTTPError as exc:
        raise ConnectorError(
            f"cannot reach Kafka Connect at {url!r}: {exc}. Is the connect service up? "
            f"Check `make ps`."
        ) from exc
    created = resp.status_code == 201
    typer.echo(f"connector {CONNECTOR_NAME!r} {'created' if created else 'updated'} (idempotent).")
    typer.echo("Next: `make connector-status` and `make slot-status`.")


@app.command()
def status() -> None:
    """Show the connector's state and each task's state (GET /connectors/<name>/status)."""
    settings = _settings()
    url = f"{_connect_base(settings)}/connectors/{CONNECTOR_NAME}/status"
    try:
        resp = httpx.get(url, timeout=15.0)
    except httpx.HTTPError as exc:
        raise ConnectorError(f"cannot reach Kafka Connect at {url!r}: {exc}.") from exc
    if resp.status_code == 404:
        typer.echo(f"connector {CONNECTOR_NAME!r} is not registered. Run `make connector`.")
        raise typer.Exit(code=1)
    resp.raise_for_status()
    body = resp.json()
    typer.echo(json.dumps(body, indent=2))
    # A short human verdict on top of the raw JSON.
    conn_state = body.get("connector", {}).get("state", "UNKNOWN")
    task_states = [t.get("state", "UNKNOWN") for t in body.get("tasks", [])]
    healthy = conn_state == "RUNNING" and task_states and all(s == "RUNNING" for s in task_states)
    verdict = "RUNNING" if healthy else "NOT fully running"
    typer.echo(f"→ connector={conn_state} tasks={task_states or '[]'} — {verdict}")
    if not healthy:
        raise typer.Exit(code=1)


@app.command()
def delete() -> None:
    """Delete the connector instance. Leaves the Postgres replication slot behind.

    Deleting the connector does NOT drop its slot ``cdc_genomics`` — the slot keeps pinning
    WAL until dropped explicitly. To fully reset, `make nuke` (destroys the pg volume) or
    ``SELECT pg_drop_replication_slot('cdc_genomics')``.
    """
    settings = _settings()
    url = f"{_connect_base(settings)}/connectors/{CONNECTOR_NAME}"
    try:
        resp = httpx.delete(url, timeout=15.0)
    except httpx.HTTPError as exc:
        raise ConnectorError(f"cannot reach Kafka Connect at {url!r}: {exc}.") from exc
    if resp.status_code == 404:
        typer.echo(f"connector {CONNECTOR_NAME!r} was not registered — nothing to delete.")
        return
    resp.raise_for_status()
    typer.echo(
        f"connector {CONNECTOR_NAME!r} deleted. The replication slot 'cdc_genomics' still "
        f"exists (and still pins WAL) — see the docstring to drop it."
    )


def main() -> None:
    """Console-script entrypoint, with a clean one-line error instead of a traceback."""
    try:
        app()
    except ConnectorError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    main()
