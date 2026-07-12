"""Idempotent seed for the source database: ~30 runs, ~5,000 specimens.

Run via ``make seed`` (which migrates first) or directly::

    uv run python -m cdc_pipeline.db.seed

Design goals from story 02.1:

- **Realistic, searchable text.** Descriptions read like a lab tracker
  ("PCR-free WGS on sequencer SEQ-07 awaiting library prep — blood gDNA, rare-disease
  trio cohort") so the Elasticsearch demos later run queries that feel real.
- **A status mix that exercises both projection-delete paths (§3/§7):** some specimens
  are ``registered`` (hard-deletable) and some ``disposed`` (soft-deleted from the index
  though the source row survives).
- **Idempotent.** Generation is deterministic (fixed RNG seed) with stable natural keys
  (``run_code``, ``barcode``); inserts use ``ON CONFLICT DO NOTHING``. Re-running changes
  nothing — no new rows, no mutated values. A fast-path skip avoids even touching the
  sequences once the target counts are present.
"""

from __future__ import annotations

import random
from decimal import Decimal

from sqlalchemy import Engine, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from cdc_pipeline.db.engine import create_source_engine
from cdc_pipeline.db.models import Run, Specimen

# Target volumes. Deterministic, so the acceptance-criteria row counts are exact.
N_RUNS = 30
N_SPECIMENS = 5_000
# A slice of specimens are control/calibration scratch: registered, no run, hard-deletable.
N_SCRATCH = 120

_SEED = 20260712  # fixed → reproducible labels/descriptions on a fresh database

# ── Domain vocabulary (kept small but plausible) ─────────────────────────────────
SEQUENCERS = [f"SEQ-{n:02d}" for n in range(1, 9)]  # SEQ-01 … SEQ-08
OPERATORS = [
    "A. Okafor",
    "M. Chen",
    "R. Patel",
    "S. Novak",
    "J. Alvarez",
    "L. Haddad",
    "T. Ibrahim",
    "K. Yamamoto",
]
COHORTS = [
    "rare-disease trio cohort",
    "population reference panel",
    "oncology solid-tumour cohort",
    "UK Biobank replication set",
    "pharmacogenomics cohort",
    "neonatal screening batch",
]

# Assay: (canonical name stored in the column, human phrasing used in the description).
ASSAYS = [
    ("WGS PCR-free", "PCR-free WGS"),
    ("WGS PCR-plus", "PCR-plus WGS"),
    ("WES", "whole-exome sequencing"),
    ("RNA-seq", "RNA-seq"),
    ("Targeted panel", "targeted gene panel"),
    ("Methylation array", "methylation profiling"),
]
# Runs run a protocol; specimens carry the assay. Protocols are the assay's lab recipe.
RUN_PROTOCOLS = [
    "WGS PCR-free (TruSeq DNA PCR-Free)",
    "WGS PCR-plus (Nextera DNA Flex)",
    "WES (Twist Exome 2.0)",
    "RNA-seq (TruSeq Stranded mRNA)",
    "Targeted panel (custom hybrid capture)",
    "Methylation array (Infinium EPIC)",
]

SPECIMEN_TYPES = [
    ("blood", "blood gDNA"),
    ("saliva", "saliva gDNA"),
    ("buccal swab", "buccal gDNA"),
    ("FFPE tissue", "FFPE-extracted DNA"),
    ("fresh-frozen tissue", "fresh-frozen tissue DNA"),
    ("cfDNA", "plasma cfDNA"),
    ("total RNA", "total RNA"),
    ("cell line", "cell-line gDNA"),
]

RUN_STATUS_MIX = ["planned"] * 5 + ["active"] * 10 + ["completed"] * 12 + ["aborted"] * 3
# Weighted specimen status draw — guarantees a healthy slice of the two delete paths.
SPECIMEN_STATUS_WEIGHTS = {
    "registered": 12,
    "in_prep": 18,
    "on_instrument": 15,
    "completed": 40,
    "disposed": 15,
}

# How each status reads in a description (appended after "…on sequencer {seq}").
STATUS_PHRASE = {
    "registered": "awaiting library prep",
    "in_prep": "in library preparation",
    "on_instrument": "sequencing in progress",
    "completed": "sequencing complete, data delivered",
    "disposed": "consumed by extraction, specimen disposed",
}


def _build_runs(rng: random.Random) -> list[dict]:
    runs: list[dict] = []
    statuses = list(RUN_STATUS_MIX)
    rng.shuffle(statuses)
    for i in range(N_RUNS):
        assay_idx = i % len(RUN_PROTOCOLS)
        runs.append(
            {
                "run_code": f"RUN-2026-{i + 1:03d}",
                "assay_protocol": RUN_PROTOCOLS[assay_idx],
                "sequencer": SEQUENCERS[i % len(SEQUENCERS)],
                "operator": rng.choice(OPERATORS),
                "status": statuses[i],
            }
        )
    return runs


def _weighted_status(rng: random.Random) -> str:
    population = list(SPECIMEN_STATUS_WEIGHTS)
    weights = list(SPECIMEN_STATUS_WEIGHTS.values())
    return rng.choices(population, weights=weights, k=1)[0]


def _build_specimens(
    rng: random.Random,
    run_lookup: dict[str, int],
    run_sequencer: dict[str, str],
) -> list[dict]:
    """Generate specimen rows.

    ``run_lookup`` maps run_code → id (for the FK); ``run_sequencer`` maps run_code →
    sequencer so a specimen's description names the instrument its run is on.
    """
    run_codes = list(run_lookup)
    specimens: list[dict] = []
    for i in range(N_SPECIMENS):
        barcode = f"SPEC-{i + 1:06d}"
        scratch = i < N_SCRATCH  # first N_SCRATCH are control/calibration, no run
        assay_canonical, assay_human = rng.choice(ASSAYS)
        type_canonical, type_human = rng.choice(SPECIMEN_TYPES)
        cohort = rng.choice(COHORTS)
        participant = f"NGP-{rng.randint(0, 99999):05d}"
        aliquot = rng.randint(1, 6)
        volume = Decimal(str(round(rng.uniform(5.0, 300.0), 2)))

        if scratch:
            run_id: int | None = None
            sequencer = rng.choice(SEQUENCERS)
            status = "registered"  # scratch specimens stay registered → hard-deletable (§3)
            label = f"Control {participant} aliquot {aliquot} ({type_canonical})"
            description = (
                f"Calibration/control {assay_human} — {type_human}, not committed to a run; "
                f"{cohort}. Scratch specimen, awaiting disposal."
            )
        else:
            run_code = run_codes[i % len(run_codes)]
            run_id = run_lookup[run_code]
            sequencer = run_sequencer[run_code]
            status = _weighted_status(rng)
            label = f"Participant {participant} aliquot {aliquot} ({type_canonical})"
            phrase = STATUS_PHRASE[status]
            description = (
                f"{assay_human} on sequencer {sequencer} {phrase} — {type_human}, {cohort} "
                f"(run {run_code})."
            )

        specimens.append(
            {
                "barcode": barcode,
                "label": label,
                "description": description,
                "specimen_type": type_canonical,
                "assay": assay_canonical,
                "volume_ul": volume,
                "status": status,
                "run_id": run_id,
            }
        )
    return specimens


def _already_seeded(session: Session) -> bool:
    runs = session.scalar(select(func.count()).select_from(Run)) or 0
    specimens = session.scalar(select(func.count()).select_from(Specimen)) or 0
    return runs >= N_RUNS and specimens >= N_SPECIMENS


def seed(engine: Engine | None = None) -> tuple[int, int]:
    """Seed the database idempotently. Returns the final (run_count, specimen_count)."""
    engine = engine or create_source_engine()
    rng = random.Random(_SEED)

    with Session(engine) as session:
        if _already_seeded(session):
            counts = _counts(session)
            print(
                f"seed: already present ({counts[0]} runs, {counts[1]} specimens) — nothing to do"
            )
            return counts

        run_rows = _build_runs(rng)
        # ON CONFLICT DO NOTHING on the natural key → re-runs insert nothing.
        session.execute(
            pg_insert(Run).on_conflict_do_nothing(index_elements=["run_code"]), run_rows
        )
        session.flush()

        run_lookup = dict(session.execute(select(Run.run_code, Run.id)).all())
        run_sequencer = {row["run_code"]: row["sequencer"] for row in run_rows}

        specimen_rows = _build_specimens(rng, run_lookup, run_sequencer)
        session.execute(
            pg_insert(Specimen).on_conflict_do_nothing(index_elements=["barcode"]),
            specimen_rows,
        )
        session.commit()

        counts = _counts(session)

    print(f"seed: done — {counts[0]} runs, {counts[1]} specimens")
    return counts


def _counts(session: Session) -> tuple[int, int]:
    runs = session.scalar(select(func.count()).select_from(Run)) or 0
    specimens = session.scalar(select(func.count()).select_from(Specimen)) or 0
    return runs, specimens


def main() -> None:  # pragma: no cover - thin CLI wrapper
    seed()


if __name__ == "__main__":  # pragma: no cover
    main()
