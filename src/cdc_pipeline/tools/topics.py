"""``cdc-topics`` — create, inspect, and demonstrate the CDC topics (epic 03.3).

The topics that carry every change out of Postgres are **designed**, not defaulted. Their
shape lives in ``deploy/topics/topics.json`` (partitions + per-topic config) with the
rationale in the adjacent ``topics.notes.md``; this module is the tool that applies and
inspects it, exactly as :mod:`cdc_pipeline.tools.connector` does for the Debezium config.

Why explicit creation at all (ARCHITECTURE.md §4, decision D3): a topic Kafka auto-creates
gets the broker defaults — **one partition** and ``cleanup.policy=delete`` with a 7-day
retention. Both are silently wrong for us. Compaction is what keeps the latest event per
key *forever* so an offset-0 rebuild stays affordable (epic 06); the partition count is the
unit of ordering and the ceiling on consumer parallelism (epic 04). Neither failure is
visible until the day it matters, so the broker's ``auto.create.topics.enable`` is off in
compose and this tool is the only way the topics come into being.

Subcommands (see ``make topics`` / ``topics-describe`` / ``compaction-demo``)::

    cdc-topics create            # idempotent: create missing, converge config on existing
    cdc-topics describe          # live partitions + config, with drift against the spec

The compaction demo itself is its own script — :mod:`cdc_pipeline.tools.compaction_demo`
(``make compaction-demo``).

``create`` is idempotent by construction but deliberately **not** omnipotent: it will never
change a topic's partition count in place. Growing partitions re-hashes keys, so a row's
history would split across two partitions — per-key ordering and compaction both break, with
no error anywhere. It reports the conflict and points at ``--recreate`` instead.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import typer
from confluent_kafka import KafkaError, KafkaException, TopicCollection
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
)

from cdc_pipeline.config import Settings, load_dotenv

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SPEC_PATH = _REPO_ROOT / "deploy" / "topics" / "topics.json"


class TopicError(RuntimeError):
    """A topic operation could not be completed (bad spec, unreachable broker, conflict)."""


# ── The spec ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TopicSpec:
    """One topic's designed shape: name, partitions, replication, and config overrides."""

    name: str
    partitions: int
    replication_factor: int
    config: Mapping[str, str] = field(default_factory=dict)


def load_spec(path: Path = _SPEC_PATH) -> list[TopicSpec]:
    """Load and validate ``deploy/topics/topics.json``.

    Validation is strict and up-front: a malformed spec must fail here, loudly, rather than
    half-applying itself against a live broker.
    """
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError as exc:
        raise TopicError(f"topic spec not found at {path}") from exc
    except json.JSONDecodeError as exc:
        raise TopicError(f"topic spec at {path} is not valid JSON: {exc}") from exc

    entries = raw.get("topics") if isinstance(raw, dict) else None
    if not isinstance(entries, list) or not entries:
        raise TopicError(f"topic spec at {path} must have a non-empty 'topics' array.")

    specs: list[TopicSpec] = []
    seen: set[str] = set()
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise TopicError(f"topics[{i}] in {path} is not an object.")
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            raise TopicError(f"topics[{i}] in {path} has no 'name'.")
        if name in seen:
            raise TopicError(f"topic {name!r} is declared twice in {path}.")
        seen.add(name)
        partitions = entry.get("partitions")
        if not isinstance(partitions, int) or isinstance(partitions, bool) or partitions < 1:
            raise TopicError(f"topic {name!r}: 'partitions' must be an integer >= 1.")
        rf = entry.get("replication_factor", 1)
        if not isinstance(rf, int) or isinstance(rf, bool) or rf < 1:
            raise TopicError(f"topic {name!r}: 'replication_factor' must be an integer >= 1.")
        config_raw = entry.get("config", {})
        if not isinstance(config_raw, dict):
            raise TopicError(f"topic {name!r}: 'config' must be an object of string settings.")
        # Kafka's admin protocol is all strings; accept JSON numbers and normalise.
        config = {str(k): str(v) for k, v in config_raw.items()}
        specs.append(
            TopicSpec(name=name, partitions=partitions, replication_factor=rf, config=config)
        )
    return specs


# ── Planning (pure — unit-tested without a broker) ──────────────────────────────

ActionKind = Literal["create", "reconfigure", "partition-conflict"]


@dataclass(frozen=True)
class Action:
    """One planned change against the live cluster."""

    topic: str
    kind: ActionKind
    detail: str


def plan(specs: Sequence[TopicSpec], existing: Mapping[str, int]) -> list[Action]:
    """Diff the spec against the live topics (``name -> partition count``).

    Three outcomes per topic:

    - absent → ``create`` with the full spec;
    - present with the right partition count → ``reconfigure`` (config is safe to converge
      in place via an incremental alter; it changes no data layout);
    - present with a *different* partition count → ``partition-conflict``, never an
      automatic fix. Kafka can only grow a partition count, and growing one re-hashes the
      key space: existing records for key K stay in their old partition while new ones may
      route elsewhere. The key then has two "latest" values that compaction can never
      merge, and per-key ordering — the guarantee the whole read-model rests on — is gone,
      silently. Recreating the topic and re-snapshotting is the only correct repair.
    """
    actions: list[Action] = []
    for spec in specs:
        current = existing.get(spec.name)
        if current is None:
            actions.append(
                Action(
                    spec.name,
                    "create",
                    f"{spec.partitions} partitions, rf={spec.replication_factor}, "
                    f"{len(spec.config)} config overrides",
                )
            )
        elif current != spec.partitions:
            actions.append(
                Action(
                    spec.name,
                    "partition-conflict",
                    f"live topic has {current} partitions, spec wants {spec.partitions}",
                )
            )
        else:
            actions.append(
                Action(spec.name, "reconfigure", f"converge {len(spec.config)} config settings")
            )
    return actions


# ── Cluster plumbing ────────────────────────────────────────────────────────────


def _settings() -> Settings:
    load_dotenv(_REPO_ROOT / ".env")
    return Settings.from_env()


def _admin(settings: Settings) -> AdminClient:
    return AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})


def live_partition_counts(admin: AdminClient, names: Sequence[str]) -> dict[str, int]:
    """Return ``name -> partition count`` for those of ``names`` that exist.

    Uses the **DescribeTopics** RPC rather than ``list_topics()``. The metadata path was
    tried first and is not trustworthy here: right after a create — or a beat after the
    broker comes up — a client's metadata view can report a real 6-partition topic as
    having one partition, or report a topic that does not exist yet at all. Acting on that
    would have this tool refuse to run with a bogus "partition counts differ" conflict.
    DescribeTopics answers per topic and says *unknown topic* explicitly, so "absent" and
    "wrongly shaped" can never be confused.
    """
    try:
        futures = admin.describe_topics(TopicCollection(list(names)))
    except Exception as exc:  # noqa: BLE001 - surfaced as a readable TopicError
        raise TopicError(
            f"cannot reach Kafka: {exc}. Is the kafka service up? Check `make ps`."
        ) from exc
    counts: dict[str, int] = {}
    for name, fut in futures.items():
        try:
            counts[name] = len(fut.result(timeout=30).partitions)
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                continue  # not an error: the topic simply isn't there yet
            raise TopicError(f"could not describe topic {name!r}: {exc}") from exc
    return counts


def _create_topics(admin: AdminClient, specs: Sequence[TopicSpec], *, attempts: int = 5) -> None:
    """Create the given topics, retrying transient broker-not-ready errors.

    Compose's healthcheck (``kafka-broker-api-versions.sh``) goes green a beat before the
    KRaft controller will accept a create — so a `make up && make topics` one-liner can hit
    a spurious failure. Retry rather than make the caller re-run.
    """
    if not specs:
        return
    pending = list(specs)
    for attempt in range(1, attempts + 1):
        futures = admin.create_topics(
            [
                NewTopic(
                    s.name,
                    num_partitions=s.partitions,
                    replication_factor=s.replication_factor,
                    config=dict(s.config),
                )
                for s in pending
            ]
        )
        failures: dict[str, Exception] = {}
        for name, fut in futures.items():
            try:
                fut.result(timeout=30)
            except Exception as exc:  # noqa: BLE001 - one topic's failure, reported by name
                failures[name] = exc
        if not failures:
            return
        if attempt == attempts:
            name, exc = next(iter(failures.items()))
            raise TopicError(
                f"could not create topic {name!r} after {attempts} attempts: {exc}. "
                f"Is the kafka service up and settled? Check `make ps`."
            )
        by_name = {s.name: s for s in pending}
        pending = [by_name[n] for n in failures]
        time.sleep(2.0 * attempt)


def _alter_configs(admin: AdminClient, specs: Sequence[TopicSpec]) -> None:
    """Converge each topic's config with an *incremental* alter.

    Incremental (not the legacy whole-resource ``alter_configs``) because that one replaces
    the entire config map — anything not restated would silently revert to the broker
    default. We only ever assert the settings the spec names.
    """
    resources = []
    for s in specs:
        if not s.config:
            continue
        res = ConfigResource(ConfigResource.Type.TOPIC, s.name)
        for key, value in s.config.items():
            res.add_incremental_config(
                ConfigEntry(key, value, incremental_operation=AlterConfigOpType.SET)
            )
        resources.append(res)
    if not resources:
        return
    futures = admin.incremental_alter_configs(resources)
    for res, fut in futures.items():
        try:
            fut.result(timeout=30)
        except Exception as exc:  # noqa: BLE001
            raise TopicError(f"could not set config on topic {res.name!r}: {exc}") from exc


def _delete_topics(admin: AdminClient, names: Sequence[str]) -> None:
    futures = admin.delete_topics(list(names), operation_timeout=30)
    for name, fut in futures.items():
        try:
            fut.result(timeout=30)
        except Exception as exc:  # noqa: BLE001
            raise TopicError(f"could not delete topic {name!r}: {exc}") from exc
    # Deletion is asynchronous on the broker; wait for the metadata to actually drop it,
    # otherwise the immediate re-create races and fails with TOPIC_ALREADY_EXISTS.
    deadline = time.time() + 60
    while time.time() < deadline:
        if not live_partition_counts(admin, names):
            return
        time.sleep(1.0)
    raise TopicError(f"topics {list(names)} were not gone 60s after delete.")


def describe_topics(admin: AdminClient, names: Sequence[str]) -> dict[str, dict[str, str]]:
    """Return each existing topic's effective config as ``name -> {setting: value}``.

    Effective, not overridden-only: the broker reports every setting with the value actually
    in force, so a spec'd setting that was never applied shows up as the broker default
    rather than as absent. That is what makes drift detection meaningful.
    """
    present = live_partition_counts(admin, names)
    if not present:
        return {}
    resources = [ConfigResource(ConfigResource.Type.TOPIC, n) for n in present]
    futures = admin.describe_configs(resources)
    out: dict[str, dict[str, str]] = {}
    for res, fut in futures.items():
        entries = fut.result(timeout=30)
        out[res.name] = {k: e.value for k, e in entries.items()}
    return out


# ── Commands ────────────────────────────────────────────────────────────────────

app = typer.Typer(help="Create and inspect the designed CDC topics.", no_args_is_help=True)


@app.command()
def create(
    recreate: bool = typer.Option(
        False,
        "--recreate",
        help="DESTRUCTIVE: delete and rebuild topics whose partition count differs from the "
        "spec. Discards every event on them; refill by re-snapshotting (see the docs).",
    ),
) -> None:
    """Idempotently bring the cluster's topics in line with ``deploy/topics/topics.json``."""
    settings = _settings()
    specs = load_spec()
    admin = _admin(settings)
    existing = live_partition_counts(admin, [s.name for s in specs])
    actions = plan(specs, existing)
    by_name = {s.name: s for s in specs}

    conflicts = [a for a in actions if a.kind == "partition-conflict"]
    if conflicts and recreate:
        names = [a.topic for a in conflicts]
        for a in conflicts:
            typer.secho(
                f"  ✗ {a.topic}: {a.detail} — deleting and recreating.", fg=typer.colors.YELLOW
            )
        _delete_topics(admin, names)
        existing = live_partition_counts(admin, [s.name for s in specs])
        actions = plan(specs, existing)
        conflicts = []

    if conflicts:
        for a in conflicts:
            typer.secho(f"  ✗ {a.topic}: {a.detail}", fg=typer.colors.RED, err=True)
        raise TopicError(
            "partition counts differ from the spec, and changing them in place would re-hash "
            "the key space (a row's history splits across partitions — per-key ordering and "
            "compaction both break, silently). Re-run with `--recreate` to delete and rebuild "
            "these topics, then re-snapshot to refill them: "
            "`make nuke && make up && make topics && make seed && make connector`."
        )

    to_create = [by_name[a.topic] for a in actions if a.kind == "create"]
    to_configure = [by_name[a.topic] for a in actions if a.kind == "reconfigure"]
    _create_topics(admin, to_create)
    _alter_configs(admin, to_configure)

    for a in actions:
        if a.kind == "create":
            typer.echo(f"  + created  {a.topic}  ({a.detail})")
        else:
            typer.echo(f"  = existing {a.topic}  ({a.detail})")
    typer.echo(f"{len(specs)} topic(s) match the spec. Next: `make topics-describe`.")


@app.command()
def describe() -> None:
    """Show live partitions and config for every spec'd topic, flagging drift."""
    settings = _settings()
    specs = load_spec()
    admin = _admin(settings)
    existing = live_partition_counts(admin, [s.name for s in specs])
    live_configs = describe_topics(admin, [s.name for s in specs])

    drift = 0
    for spec in specs:
        if spec.name not in existing:
            typer.secho(f"{spec.name}: MISSING — run `make topics`.", fg=typer.colors.RED)
            drift += 1
            continue
        parts = existing[spec.name]
        mark = "ok" if parts == spec.partitions else f"DRIFT (spec {spec.partitions})"
        typer.echo(f"\n{spec.name}  partitions={parts}  [{mark}]")
        if parts != spec.partitions:
            drift += 1
        cfg = live_configs.get(spec.name, {})
        for key, want in sorted(spec.config.items()):
            got = cfg.get(key, "(unset)")
            status = "ok" if got == want else f"DRIFT (spec {want})"
            if got != want:
                drift += 1
            typer.echo(f"    {key:<28} {got:<10} [{status}]")
    typer.echo("")
    if drift:
        typer.secho(
            f"{drift} setting(s) drifted from the spec. Run `make topics`.", fg=typer.colors.RED
        )
        raise typer.Exit(code=1)
    typer.secho("every topic matches deploy/topics/topics.json.", fg=typer.colors.GREEN)


def main() -> None:
    """Console-script entrypoint, with a clean one-line error instead of a traceback."""
    try:
        app()
    except TopicError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    main()
