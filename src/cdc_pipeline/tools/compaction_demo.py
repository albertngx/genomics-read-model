"""``cdc-compaction-demo`` — make log compaction and the tombstone lifecycle observable (03.3).

Compaction is the setting the whole read-model design leans on (ARCHITECTURE.md §4, D3):
it keeps the **latest event per key forever** while dropping superseded ones, which is what
makes an offset-0 rebuild affordable in epic 06 and what stops the topic silently emptying
itself the way a ``delete``-policy topic does. Configuring it is one line; *seeing* it work
is the point of this script, because everything about it is invisible from the outside until
you count records.

Two demos, run back to back against the live stack::

    cdc-compaction-demo                 # 1000 updates, then a hard delete
    cdc-compaction-demo --updates 200   # quicker

**Demo 1 — retention by key.** Insert one specimen, update it ``--updates`` times, then
watch the partition go from 1 001 records for that key to 1.

**Demo 2 — the tombstone lifecycle.** Insert a ``registered`` specimen and hard-delete it.
Debezium emits two events (``op:"d"`` plus a null-valued tombstone on the same key), and
compaction reaps them in two distinct stages: first the delete event collapses into the
tombstone alone, then — only after ``delete.retention.ms`` — the tombstone itself vanishes
and the key leaves the log entirely.

**Why the script keeps writing while it waits.** The log cleaner never touches a partition's
*active* segment, and a segment only rolls when a record arrives after ``segment.ms`` has
elapsed. A quiet topic therefore never compacts, no matter how low you set the dirty ratio —
the single most confusing thing about compaction in a lab. So between polls the script
inserts and immediately hard-deletes a batch of throwaway specimens, which spreads appends
across every partition and forces the roll. They are cleaned up by construction.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass

import psycopg
import typer
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, Consumer, TopicPartition

from cdc_pipeline.config import Settings
from cdc_pipeline.tools.topics import TopicError, _settings

SPECIMENS_TOPIC = "cdc.public.specimens"


# ── Reading the log ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PartitionScan:
    """What one end-to-end read of a partition found."""

    low: int  # log start offset (compaction does NOT advance this; deletion would)
    high: int  # next offset to be written
    total: int  # records actually readable — the gap vs (high-low) IS the compacted-away set
    key_offsets: list[int]  # offsets still holding a record for the key we asked about
    key_tombstones: int  # how many of those are null-valued tombstones

    @property
    def key_count(self) -> int:
        return len(self.key_offsets)


def _consumer(settings: Settings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"cdc-compaction-demo-{uuid.uuid4().hex}",  # fresh group, no offsets
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def _partition_ids(consumer: Consumer, topic: str) -> list[int]:
    md = consumer.list_topics(topic, timeout=15)
    tmd = md.topics.get(topic)
    if tmd is None or tmd.error is not None:
        raise TopicError(f"topic {topic!r} not found — run `make topics` and `make connector`.")
    return sorted(tmd.partitions)


def scan_partition(
    settings: Settings, topic: str, partition: int, specimen_id: int, *, timeout: float = 120.0
) -> PartitionScan:
    """Read one partition from its log start to its high watermark, counting records.

    This is the measurement the whole script rests on. Note what compaction does to the
    offsets: it rewrites segments in place, so surviving records keep their original offsets
    and the log becomes **non-contiguous** — reading start-to-end returns fewer records than
    ``high - low``. That gap is the compacted-away set, and it is the number to quote.
    """
    consumer = _consumer(settings)
    try:
        probe = TopicPartition(topic, partition)
        low, high = consumer.get_watermark_offsets(probe, timeout=15, cached=False)
        if low >= high:
            return PartitionScan(low=low, high=high, total=0, key_offsets=[], key_tombstones=0)
        consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING)])
        total = 0
        key_offsets: list[int] = []
        tombstones = 0
        # "Read to the end" needs care on a compacted topic: the record at `high - 1` may
        # itself have been compacted away, so waiting to *see* that offset can hang forever.
        # Draining — a run of empty polls once we are at or past the watermark — is the only
        # reliable end condition here.
        empty_polls = 0
        deadline = time.time() + timeout
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                empty_polls += 1
                if empty_polls >= 3:
                    break
                continue
            empty_polls = 0
            if msg.error():
                continue
            total += 1
            if _key_id(msg.key()) == specimen_id:
                key_offsets.append(msg.offset())
                if msg.value() is None:
                    tombstones += 1
            if msg.offset() >= high - 1:
                break
        else:
            raise TopicError(f"reading {topic}-{partition} did not finish within {timeout:.0f}s.")
        return PartitionScan(
            low=low, high=high, total=total, key_offsets=key_offsets, key_tombstones=tombstones
        )
    finally:
        consumer.close()


def _key_id(key: bytes | None) -> int | None:
    """Extract the primary key from a Debezium message key (``{"id":123}``)."""
    if key is None:
        return None
    try:
        return json.loads(key).get("id")
    except (json.JSONDecodeError, AttributeError):
        return None


# ── Writing to the source ───────────────────────────────────────────────────────


def _insert_specimen(conn: psycopg.Connection, barcode: str) -> int:
    row = conn.execute(
        "INSERT INTO specimens (barcode, label, specimen_type, assay, status, volume_ul) "
        "VALUES (%s, %s, 'blood', 'wgs', 'registered', 100.00) RETURNING id",
        (barcode, f"compaction demo {barcode}"),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _locate_key_partition(
    settings: Settings, conn: psycopg.Connection, barcode: str, *, timeout: float = 30.0
) -> tuple[int, int]:
    """Insert a specimen while tailing every partition; return ``(id, partition)``.

    Tailing from the END *before* the write is how we learn the key→partition mapping
    without reimplementing Kafka's murmur2 partitioner — and it doubles as proof that
    Debezium is actually streaming.
    """
    consumer = _consumer(settings)
    try:
        parts = _partition_ids(consumer, SPECIMENS_TOPIC)
        consumer.assign([TopicPartition(SPECIMENS_TOPIC, p, OFFSET_END) for p in parts])
        consumer.poll(0)  # force the seek to take effect before we write

        specimen_id = _insert_specimen(conn, barcode)

        deadline = time.time() + timeout
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            if _key_id(msg.key()) == specimen_id:
                return specimen_id, msg.partition()
        raise TopicError(
            f"no event for specimen {specimen_id} within {timeout:.0f}s — is the connector "
            f"streaming? Check `make connector-status` and `make slot-status`."
        )
    finally:
        consumer.close()


def _await_key_count(
    settings: Settings, partition: int, specimen_id: int, expected: int, *, timeout: float = 60.0
) -> PartitionScan:
    """Poll until the key has ``expected`` records on the partition (Debezium caught up)."""
    scan = scan_partition(settings, SPECIMENS_TOPIC, partition, specimen_id)
    deadline = time.time() + timeout
    while scan.key_count < expected and time.time() < deadline:
        time.sleep(2.0)
        scan = scan_partition(settings, SPECIMENS_TOPIC, partition, specimen_id)
    return scan


def _nudge(conn: psycopg.Connection, batch: int) -> None:
    """Insert and immediately hard-delete ``batch`` throwaway specimens.

    Pure segment-roll pressure: the keys spread across every partition, so each partition's
    active segment gets an append and rolls once ``segment.ms`` has passed. Self-cleaning —
    every row inserted here is deleted before the function returns.
    """
    ids: list[int] = []
    tag = uuid.uuid4().hex[:8]
    for i in range(batch):
        ids.append(_insert_specimen(conn, f"NUDGE-{tag}-{i:03d}"))
    conn.execute("DELETE FROM specimens WHERE id = ANY(%s)", (ids,))


def _wait_for_compaction(
    settings: Settings,
    conn: psycopg.Connection,
    partition: int,
    specimen_id: int,
    *,
    target: int,
    deadline_s: float,
    nudge_batch: int,
    poll_interval: float,
    label: str,
) -> tuple[PartitionScan, float]:
    """Nudge and poll until the key is down to ``target`` records, or give up loudly."""
    started = time.time()
    deadline = started + deadline_s
    scan = scan_partition(settings, SPECIMENS_TOPIC, partition, specimen_id)
    while scan.key_count > target and time.time() < deadline:
        _nudge(conn, nudge_batch)
        time.sleep(poll_interval)
        scan = scan_partition(settings, SPECIMENS_TOPIC, partition, specimen_id)
        typer.echo(
            f"    [{time.time() - started:5.0f}s] {label}: {scan.key_count} record(s) for the key "
            f"(partition holds {scan.total} of {scan.high - scan.low} offsets)"
        )
    return scan, time.time() - started


# ── The demo ────────────────────────────────────────────────────────────────────

app = typer.Typer(help="Observe log compaction and the tombstone lifecycle.", no_args_is_help=True)


@app.command()
def run(
    updates: int = typer.Option(1000, help="How many times to update the one hot specimen."),
    deadline: float = typer.Option(600.0, help="Seconds to wait for each compaction stage."),
    nudge_batch: int = typer.Option(24, help="Throwaway rows per nudge round (rolls segments)."),
    poll_interval: float = typer.Option(15.0, help="Seconds between nudge/poll rounds."),
) -> None:
    """Run both demos against the live stack and print the numbers."""
    settings = _settings()
    rule = "─" * 74
    with psycopg.connect(settings.database_url, connect_timeout=10, autocommit=True) as conn:
        # ── Demo 1: 1000 updates → 1 record ─────────────────────────────────────
        typer.echo(rule)
        typer.secho("  Demo 1 — compaction is retention by key", bold=True)
        typer.echo(rule)
        barcode = f"COMPACT-{uuid.uuid4().hex[:10]}"
        specimen_id, partition = _locate_key_partition(settings, conn, barcode)
        typer.echo(f"  hot specimen id={specimen_id} ({barcode}) → partition {partition}")

        typer.echo(f"  applying {updates:,} updates …")
        t0 = time.time()
        for i in range(updates):
            conn.execute(
                "UPDATE specimens SET volume_ul = %s, updated_at = now() WHERE id = %s",
                (100 + (i % 900) + 0.25, specimen_id),
            )
        typer.echo(f"  {updates:,} updates in {time.time() - t0:.1f}s; waiting for Debezium …")

        before = _await_key_count(settings, partition, specimen_id, updates + 1, timeout=120.0)
        if not before.key_offsets:
            raise TopicError(
                f"no events for specimen {specimen_id} reached partition {partition} — is the "
                f"connector streaming? Check `make connector-status` and `make slot-status`."
            )
        typer.echo(
            f"  before compaction: {before.key_count} records for the key "
            f"(offsets {before.key_offsets[0]}…{before.key_offsets[-1]}); partition "
            f"{partition} spans offsets {before.low}–{before.high} holding {before.total} records"
        )

        typer.echo("  waiting for the cleaner (nudging so segments roll) …")
        after, elapsed = _wait_for_compaction(
            settings,
            conn,
            partition,
            specimen_id,
            target=1,
            deadline_s=deadline,
            nudge_batch=nudge_batch,
            poll_interval=poll_interval,
            label="hot key",
        )
        typer.echo("")
        typer.secho(
            f"  → {before.key_count} → {after.key_count} record(s) for key {specimen_id} "
            f"after {elapsed:.0f}s"
            + (f" (survivor at offset {after.key_offsets[-1]})" if after.key_offsets else ""),
            fg=typer.colors.GREEN if after.key_count <= 2 else typer.colors.RED,
        )
        typer.echo(
            f"  → partition {partition} now returns {after.total} records across offsets "
            f"{after.low}–{after.high}: {after.high - after.low - after.total} offsets have been "
            f"compacted away (the log is deliberately non-contiguous)."
        )

        # ── Demo 2: the tombstone lifecycle ─────────────────────────────────────
        typer.echo("")
        typer.echo(rule)
        typer.secho("  Demo 2 — the tombstone lifecycle", bold=True)
        typer.echo(rule)
        del_barcode = f"TOMBSTONE-{uuid.uuid4().hex[:10]}"
        del_id, del_partition = _locate_key_partition(settings, conn, del_barcode)
        typer.echo(f"  registered specimen id={del_id} ({del_barcode}) → partition {del_partition}")
        conn.execute("DELETE FROM specimens WHERE id = %s", (del_id,))
        typer.echo('  hard-deleted; expecting op:"d" + a null-valued tombstone on the same key')

        deleted = _await_key_count(settings, del_partition, del_id, 3, timeout=60.0)
        typer.echo(
            f"  after the delete: {deleted.key_count} records for the key "
            f"({deleted.key_tombstones} tombstone) at offsets {deleted.key_offsets}"
        )

        typer.echo("  stage 1 — waiting for the delete event to collapse into the tombstone …")
        stage1, e1 = _wait_for_compaction(
            settings,
            conn,
            del_partition,
            del_id,
            target=1,
            deadline_s=deadline,
            nudge_batch=nudge_batch,
            poll_interval=poll_interval,
            label="deleted key",
        )
        typer.secho(
            f"  → {deleted.key_count} → {stage1.key_count} record(s) after {e1:.0f}s "
            f"({stage1.key_tombstones} tombstone remaining)",
            fg=typer.colors.GREEN if stage1.key_count <= 1 else typer.colors.RED,
        )

        typer.echo("  stage 2 — waiting out delete.retention.ms for the tombstone itself …")
        stage2, e2 = _wait_for_compaction(
            settings,
            conn,
            del_partition,
            del_id,
            target=0,
            deadline_s=deadline,
            nudge_batch=nudge_batch,
            poll_interval=poll_interval,
            label="deleted key",
        )
        reaped = stage2.key_count == 0
        typer.secho(
            f"  → {stage1.key_count} → {stage2.key_count} record(s) after {e2:.0f}s — the key "
            f"has left the log entirely."
            if reaped
            else f"  → tombstone still present after {e2:.0f}s (delete.retention.ms not elapsed)",
            fg=typer.colors.GREEN if reaped else typer.colors.YELLOW,
        )

        # ── Cleanup: the hot specimen is registered, so it can be hard-deleted ──
        conn.execute("DELETE FROM specimens WHERE id = %s", (specimen_id,))
        typer.echo("")
        typer.echo(rule)
        typer.echo(
            f"  summary: key {specimen_id} {before.key_count}→{after.key_count} records; "
            f"key {del_id} {deleted.key_count}→{stage1.key_count}→{stage2.key_count}; "
            f"partition {partition} holds {after.total} of {after.high - after.low} offsets."
        )
        typer.echo(rule)


def main() -> None:
    """Console-script entrypoint, with a clean one-line error instead of a traceback."""
    try:
        app()
    except TopicError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    main()
