"""Named scenarios and the operation-mix vocabulary.

An :class:`OpMix` is a weighted distribution over :class:`OpKind`; a :class:`Scenario`
pairs a default mix with a rate-profile *factory* (rate and duration are CLI-supplied) and
the key-skew knobs. The four scenarios are the story's:

- ``steady`` — the default 70/15/10/5 mix at a constant rate;
- ``delete-wave`` — inserts feeding a torrent of registered hard-deletes, i.e. tombstone
  pressure for the epic-04 consumers;
- ``reassignment-storm`` — sequencer re-assignments / protocol amendments on a few hot runs
  under specimen load, the ARCHITECTURE.md §7 denormalization fan-out;
- ``burst`` — the steady mix but with 10× rate spikes so consumer lag becomes visible.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from cdc_pipeline.tools.load.model import OpKind
from cdc_pipeline.tools.load.rates import BurstRate, RateProfile, SteadyRate


class OpMix:
    """A weighted operation distribution. Weights need not sum to anything in particular."""

    def __init__(self, weights: dict[OpKind, float]):
        positive = {k: float(w) for k, w in weights.items() if w > 0}
        if not positive:
            raise ValueError("operation mix must have at least one positive weight")
        self._kinds = list(positive)
        self._weights = list(positive.values())
        self._total = sum(self._weights)

    @property
    def kinds(self) -> list[OpKind]:
        return list(self._kinds)

    def as_percentages(self) -> dict[OpKind, float]:
        return {k: 100.0 * w / self._total for k, w in zip(self._kinds, self._weights, strict=True)}

    def choose(self, rng) -> OpKind:  # rng: random.Random
        """Pick a kind. Consumes exactly one ``rng.random()`` (reproducibility contract)."""
        target = rng.random() * self._total
        upto = 0.0
        for kind, weight in zip(self._kinds, self._weights, strict=True):
            upto += weight
            if target < upto:
                return kind
        return self._kinds[-1]  # float fall-through guard


def parse_mix(spec: str) -> OpMix:
    """Parse ``"update=70,insert=15,dispose=10,delete=5"`` into an :class:`OpMix`.

    Keys are :class:`OpKind` values (``update``/``insert``/``dispose``/``delete``/
    ``run_update``). Raises ``ValueError`` with an actionable message on a bad token.
    """
    weights: dict[OpKind, float] = {}
    for token in spec.split(","):
        token = token.strip()
        if not token:
            continue
        key, sep, value = token.partition("=")
        if not sep:
            raise ValueError(f"malformed mix entry {token!r}; expected 'kind=weight'")
        try:
            kind = OpKind(key.strip())
        except ValueError as exc:
            allowed = ", ".join(k.value for k in OpKind)
            raise ValueError(f"unknown op kind {key.strip()!r}; allowed: {allowed}") from exc
        try:
            weights[kind] = float(value)
        except ValueError as exc:
            raise ValueError(f"weight for {key.strip()!r} is not a number: {value!r}") from exc
    return OpMix(weights)


# The story's default mix: 70% update / 15% insert / 10% dispose / 5% registered hard-delete.
DEFAULT_MIX = OpMix(
    {
        OpKind.SPECIMEN_UPDATE: 70,
        OpKind.SPECIMEN_INSERT: 15,
        OpKind.SPECIMEN_DISPOSE: 10,
        OpKind.SPECIMEN_HARD_DELETE: 5,
    }
)


@dataclass(frozen=True)
class Scenario:
    """A named workload shape. ``rate_factory`` receives the CLI rate and duration."""

    name: str
    mix: OpMix
    rate_factory: Callable[[float, float], RateProfile]
    specimen_alpha: float  # Zipf skew for specimen selection
    run_alpha: float  # Zipf skew for run selection (higher → fewer, hotter runs)
    description: str


def _steady_factory(rps: float, duration: float) -> RateProfile:
    return SteadyRate(rps)


def _burst_factory(rps: float, duration: float) -> RateProfile:
    return BurstRate(base_rps=rps, multiplier=10.0)


SCENARIOS: dict[str, Scenario] = {
    "steady": Scenario(
        name="steady",
        mix=DEFAULT_MIX,
        rate_factory=_steady_factory,
        specimen_alpha=1.1,
        run_alpha=1.1,
        description="Default 70/15/10/5 mix at a constant rate.",
    ),
    "delete-wave": Scenario(
        name="delete-wave",
        # Inserts create registered specimens; hard-deletes immediately tombstone them.
        # Insert≈delete so the wave is *sustained* (drains the seeded registered buffer,
        # then rides the insert rate) rather than exhausting the pool in the first seconds.
        mix=OpMix(
            {
                OpKind.SPECIMEN_HARD_DELETE: 48,
                OpKind.SPECIMEN_INSERT: 48,
                OpKind.SPECIMEN_UPDATE: 4,
            }
        ),
        rate_factory=_steady_factory,
        specimen_alpha=1.1,
        run_alpha=1.1,
        description="Burst of registered hard-deletes — tombstone pressure for epic 04.",
    ),
    "reassignment-storm": Scenario(
        name="reassignment-storm",
        # Run re-assignments/amendments on a few very hot runs, under specimen update load —
        # each run write fans out to a re-index of that run's specimens (§7).
        mix=OpMix(
            {
                OpKind.RUN_UPDATE: 35,
                OpKind.SPECIMEN_UPDATE: 65,
            }
        ),
        rate_factory=_steady_factory,
        specimen_alpha=1.1,
        run_alpha=1.6,  # concentrate the storm on a handful of runs
        description="Sequencer re-assignments / protocol amendments on hot runs (§7 fan-out).",
    ),
    "burst": Scenario(
        name="burst",
        mix=DEFAULT_MIX,
        rate_factory=_burst_factory,
        specimen_alpha=1.1,
        run_alpha=1.1,
        description="Steady mix with 10× rate spikes — consumer lag becomes visible.",
    ),
}


def get_scenario(name: str) -> Scenario:
    try:
        return SCENARIOS[name]
    except KeyError as exc:
        allowed = ", ".join(SCENARIOS)
        raise ValueError(f"unknown scenario {name!r}; choose one of: {allowed}") from exc
