"""Shared value types: the operation kinds, a planned :class:`Operation`, and the
:class:`Result` a worker records after executing one.

Keeping these free of any I/O or state lets the planner (which builds operations) and the
client (which executes them) depend on a single vocabulary without depending on each other.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class OpKind(str, Enum):
    """The write operations the generator issues against the API.

    The string values double as the keys used in an operation-mix spec
    (``update=70,insert=15,...``) and in the final per-kind summary.
    """

    SPECIMEN_UPDATE = "update"  # PATCH a hot specimen, walking its status lifecycle (§3)
    SPECIMEN_INSERT = "insert"  # POST a new registered specimen (also a future delete target)
    SPECIMEN_DISPOSE = "dispose"  # soft delete — POST /specimens/{id}/dispose (§3/§7)
    SPECIMEN_HARD_DELETE = "delete"  # hard delete a registered specimen — op:"d" + tombstone (§3)
    RUN_UPDATE = "run_update"  # PATCH a hot run's sequencer/protocol — the §7 fan-out trigger


@dataclass(frozen=True, slots=True)
class Operation:
    """One HTTP write the client will perform. Built by the planner, executed by the client.

    ``kind`` is carried through to the result so per-kind stats and insert-id feedback work
    without the client having to reverse-engineer the request.
    """

    kind: OpKind
    method: str  # "POST" | "PATCH" | "DELETE"
    path: str  # e.g. "/specimens/42"
    json: dict | None = None  # request body, if any


@dataclass(frozen=True, slots=True)
class Result:
    """The outcome of executing one :class:`Operation`."""

    kind: OpKind
    ok: bool  # True iff the API returned a 2xx
    status_code: int | None  # None when the request never completed (timeout/connection error)
    latency_ms: float
    created_id: int | None = None  # server-assigned id, parsed only for successful inserts
    error: str | None = None  # short reason when not ok (status text or exception class)
