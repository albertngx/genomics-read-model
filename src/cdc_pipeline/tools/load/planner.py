"""The workload planner — the single source of every random decision, and the reason a run
is reproducible from ``--seed``.

**Reproducibility contract.** One planner (one coroutine) owns one ``random.Random``. Each
call to :meth:`WorkloadPlanner.next` draws a *fixed* number of values for its chosen op
kind, and — crucially — never lets a runtime condition change how many draws it makes: a
target that has since been deleted turns the op into a **skip**, decided from state *without
consuming any RNG*. So the sequence of draws (hence the op-kind stream, the key skew, and
the field values) is a pure function of the seed, identical on camera and in rehearsal.

The only irreducible non-determinism is *which* freshly-inserted specimen a hard-delete or
dispose lands on: insert ids are assigned by Postgres and fed back at execution time
(:meth:`on_inserted`). Those ids never enter the Zipf update universe, so they can perturb
only the registered pool — never the draw stream and never a Zipf update decision.

State that skip decisions read (statuses, the registered pool, the dead set) is mutated only
here, at plan time, in deterministic order; :meth:`on_inserted` only appends to the
registered pool. That separation is what keeps the guarantee above true under async
execution.
"""

from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass, field

from cdc_pipeline.tools.load.model import Operation, OpKind
from cdc_pipeline.tools.load.scenarios import OpMix, Scenario

# The status walk an "update" advances a specimen along (ARCHITECTURE.md §3). It stops at
# ``completed``; disposal is its own op, so updates never soft-delete via PATCH.
STATUS_WALK: tuple[str, ...] = ("registered", "in_prep", "on_instrument", "completed")

# Vocabulary for the fields writes mutate. Kept local (not imported from the seed script) so
# the generator has no dependency on seed internals; values only need to be plausible.
SEQUENCERS = [f"SEQ-{n:02d}" for n in range(1, 9)]
RUN_PROTOCOLS = [
    "WGS PCR-free (TruSeq DNA PCR-Free)",
    "WGS PCR-plus (Nextera DNA Flex)",
    "WES (Twist Exome 2.0)",
    "RNA-seq (TruSeq Stranded mRNA)",
    "Targeted panel (custom hybrid capture)",
    "Methylation array (Infinium EPIC)",
]
SPECIMEN_TYPES = ["blood", "saliva", "buccal swab", "FFPE tissue", "cfDNA", "total RNA"]
ASSAYS = ["WGS PCR-free", "WGS PCR-plus", "WES", "RNA-seq", "Targeted panel"]


def _successor(status: str | None) -> str | None:
    """Next status in the walk, or ``None`` at/after ``completed`` (→ benign field edit)."""
    if status in STATUS_WALK:
        idx = STATUS_WALK.index(status)
        return STATUS_WALK[idx + 1] if idx + 1 < len(STATUS_WALK) else None
    return None


@dataclass(frozen=True)
class Bootstrap:
    """The live keys the planner starts from, sampled from the API at startup.

    ``specimen_ids`` is the Zipf *update* universe; ``specimen_status`` seeds the lifecycle
    walk; ``registered_ids`` are the initial hard-delete targets; ``run_ids`` is the run
    universe for reassignment writes.
    """

    specimen_ids: list[int]
    specimen_status: dict[int, str]
    registered_ids: list[int]
    run_ids: list[int]


@dataclass
class WorkloadPlanner:
    """Turns a seeded RNG + a scenario into a deterministic stream of :class:`Operation`s."""

    mix: OpMix
    rng: random.Random
    _specimen_zipf: "object"  # ZipfSampler; typed loosely to avoid an import cycle in hints
    _run_zipf: "object | None"
    run_ids: list[int]
    insert_prefix: str

    # Mutable state (all planner-owned; see module docstring).
    _status: dict[int, str] = field(default_factory=dict)  # specimen id → last-known status
    _dead: set[int] = field(default_factory=set)  # hard-deleted ids — update skips them
    # The renewable registered pool: hard-delete AND dispose targets. Seeded from the
    # bootstrap's registered specimens and topped up by every successful insert; balanced
    # against insert rate so it neither starves nor grows without bound.
    _registered_set: set[int] = field(default_factory=set)
    _registered_queue: deque[int] = field(default_factory=deque)
    _insert_seq: int = 0

    @classmethod
    def build(
        cls, scenario: Scenario, mix: OpMix, bootstrap: Bootstrap, seed: int, *, insert_prefix: str
    ) -> WorkloadPlanner:
        from cdc_pipeline.tools.load.keys import ZipfSampler  # local: keeps module deps flat

        rng = random.Random(seed)
        # A *separate* seeded RNG shuffles the Zipf key order, so the hot set is a stable
        # pseudo-random subset without perturbing the main draw stream.
        specimen_zipf = ZipfSampler(
            bootstrap.specimen_ids,
            scenario.specimen_alpha,
            shuffle_rng=random.Random(seed ^ 0x5DEECE66D),
        )
        run_zipf = (
            ZipfSampler(
                bootstrap.run_ids,
                scenario.run_alpha,
                shuffle_rng=random.Random(seed ^ 0x123456789),
            )
            if bootstrap.run_ids
            else None
        )
        planner = cls(
            mix=mix,
            rng=rng,
            _specimen_zipf=specimen_zipf,
            _run_zipf=run_zipf,
            run_ids=list(bootstrap.run_ids),
            insert_prefix=insert_prefix,
        )
        planner._status.update(bootstrap.specimen_status)
        for rid in bootstrap.registered_ids:
            planner._registered_set.add(rid)
            planner._registered_queue.append(rid)
        return planner

    # ── the op stream ────────────────────────────────────────────────────────────

    def next(self) -> Operation | None:
        """Return the next operation, or ``None`` when the drawn op has no eligible target
        (a *skip* — e.g. a hard-delete with no registered specimen left)."""
        kind = self.mix.choose(self.rng)
        if kind is OpKind.SPECIMEN_UPDATE:
            return self._update()
        if kind is OpKind.SPECIMEN_INSERT:
            return self._insert()
        if kind is OpKind.SPECIMEN_DISPOSE:
            return self._dispose()
        if kind is OpKind.SPECIMEN_HARD_DELETE:
            return self._hard_delete()
        if kind is OpKind.RUN_UPDATE:
            return self._run_update()
        raise AssertionError(f"unhandled op kind {kind!r}")  # pragma: no cover

    def _update(self) -> Operation | None:
        key = self._specimen_zipf.sample(self.rng)  # draw 1 (always)
        volume = round(self.rng.uniform(5.0, 300.0), 2)  # draw 2 (always — used only if benign)
        if key in self._dead:
            return None  # hard-deleted since bootstrap; no RNG consumed by the skip
        nxt = _successor(self._status.get(key))
        if nxt is not None:
            self._status[key] = nxt
            if nxt != "registered":
                self._registered_set.discard(key)  # no longer a hard-delete/dispose target
            body: dict = {"status": nxt}
        else:
            # Already at/after ``completed`` (or disposed) — a harmless field edit instead.
            # PATCHing a disposed row's volume is a legal 200 (no lifecycle guard on PATCH).
            body = {"volume_ul": f"{volume:.2f}"}
        return Operation(OpKind.SPECIMEN_UPDATE, "PATCH", f"/specimens/{key}", body)

    def _insert(self) -> Operation:
        run_id = self.rng.choice(self.run_ids) if self.run_ids else None  # draw
        attach = self.rng.random() < 0.9  # draw — a few specimens start unassigned
        specimen_type = self.rng.choice(SPECIMEN_TYPES)  # draw
        assay = self.rng.choice(ASSAYS)  # draw
        volume = round(self.rng.uniform(5.0, 300.0), 2)  # draw
        participant = self.rng.randint(0, 99999)  # draw
        aliquot = self.rng.randint(1, 6)  # draw
        self._insert_seq += 1
        barcode = f"{self.insert_prefix}-{self._insert_seq:07d}"
        body = {
            "barcode": barcode,
            "label": f"Load {participant:05d} aliquot {aliquot} ({specimen_type})",
            "description": f"Synthetic load specimen — {assay}, {specimen_type}.",
            "specimen_type": specimen_type,
            "assay": assay,
            "volume_ul": f"{volume:.2f}",
            "status": "registered",
            "run_id": run_id if attach else None,
        }
        return Operation(OpKind.SPECIMEN_INSERT, "POST", "/specimens", body)

    def _dispose(self) -> Operation | None:
        # Dispose (a terminal op) draws from the same renewable registered pool as
        # hard-delete, not the Zipf universe: a registered specimen is guaranteed
        # disposable exactly once, so this never re-disposes (self-inflicted 409) or hits an
        # already-disposed seed row. Consumes no RNG — the pool is a queue, not a draw.
        key = self._pop_registered()
        if key is None:
            return None
        self._status[key] = "disposed"  # so a later update on it does a benign field edit
        return Operation(OpKind.SPECIMEN_DISPOSE, "POST", f"/specimens/{key}/dispose", None)

    def _hard_delete(self) -> Operation | None:
        key = self._pop_registered()  # consumes no RNG (a queue, not a draw)
        if key is None:
            return None
        self._dead.add(key)
        return Operation(OpKind.SPECIMEN_HARD_DELETE, "DELETE", f"/specimens/{key}", None)

    def _run_update(self) -> Operation | None:
        if self._run_zipf is None:
            return None
        key = self._run_zipf.sample(self.rng)  # draw 1 (always)
        pick_sequencer = self.rng.random() < 0.5  # draw
        sequencer = self.rng.choice(SEQUENCERS)  # draw
        protocol = self.rng.choice(RUN_PROTOCOLS)  # draw
        body = {"sequencer": sequencer} if pick_sequencer else {"assay_protocol": protocol}
        return Operation(OpKind.RUN_UPDATE, "PATCH", f"/runs/{key}", body)

    # ── execution feedback (runtime, not part of the draw stream) ──────────────────

    def on_inserted(self, specimen_id: int) -> None:
        """Register a specimen the API just created as a future hard-delete target.

        Called by a worker after a successful insert. Touches only the delete queue, so it
        cannot perturb the reproducible draw stream (see the module docstring)."""
        self._registered_set.add(specimen_id)
        self._registered_queue.append(specimen_id)

    def _pop_registered(self) -> int | None:
        """Pop the oldest still-registered id, discarding stale queue entries (specimens
        that were advanced past ``registered`` or already removed)."""
        while self._registered_queue:
            candidate = self._registered_queue.popleft()
            if candidate in self._registered_set:
                self._registered_set.discard(candidate)
                return candidate
        return None
