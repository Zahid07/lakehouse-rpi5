"""The frozen structural interfaces every duckstream component codes against.

These are the seams between the four planes described in ``PLAN.md`` — trigger,
plan, execute, state. They are deliberately small and deliberately structural:
a user-supplied source or sink is any object with the right shape, resolved by
dotted path through the registry, with no base class to inherit.

Nothing in this module imports duckdb. The ``con`` parameters are typed ``Any``
so that importing duckstream's declarative surface stays free of a database
dependency.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable

if TYPE_CHECKING:  # pragma: no cover - typing only
    from duckstream.model import Model

__all__ = [
    "Offset",
    "BatchLimits",
    "BatchPlan",
    "BatchContext",
    "Source",
    "Sink",
    "StateStore",
]


Offset = dict[str, Any]
"""A source-defined, JSON-serialisable position.

A consumed-file set or high-water mark for files, a broker sequence for streams,
a snapshot id for a CDC source. It must survive ``json.dumps``/``json.loads``
unchanged, because it is committed to the state store inside the same
transaction as the output rows.
"""


@dataclass(frozen=True)
class BatchLimits:
    """The memory knob, expressed as a bound on rows in flight.

    ``CONTEXT.md`` section 1.1 measured that memory is governed by DuckDB's
    buffer manager materialising a batch, not by the Python layer — so bounding
    rows is the only lever that works. ``None`` means unbounded.
    """

    max_rows_per_trigger: int | None = None
    max_files_per_trigger: int | None = None


@dataclass(frozen=True)
class BatchPlan:
    """One bounded micro-batch, produced by planning and consumed by execution."""

    start: Offset | None
    """Offset this batch resumes from; ``None`` on the very first batch."""

    end: Offset
    """Offset that becomes durable once this batch commits."""

    payload: dict[str, Any] = field(default_factory=dict)
    """Source-specific and JSON-serialisable: file list, sequence range, bounds."""

    is_empty: bool = False
    """True when there was nothing to read; the engine may still commit."""

    has_more: bool = False
    """True when :class:`BatchLimits` truncated the batch, so another pass is due."""

    @classmethod
    def empty(cls, start: Offset | None, end: Offset) -> "BatchPlan":
        """Convenience constructor for "nothing available at this trigger"."""
        return cls(start=start, end=end, payload={}, is_empty=True, has_more=False)


@dataclass(frozen=True)
class BatchContext:
    """What execution knows about the batch it is running."""

    model_name: str
    batch_id: int
    plan: BatchPlan

    watermark: datetime | None = None
    """Event-time watermark **after** this batch, or ``None`` if the model
    declares no lateness horizon.

    A sink uses it to decide which windows are complete: window ``[ws, ws + G)``
    is sealed once ``ws + G <= watermark``. It is the watermark this batch is
    about to commit, not the one it inherited, so a window closed by this very
    batch seals inside the same transaction that folded the rows closing it.

    Added after the phase-1 interface was frozen. Keyword-only in practice --
    every construction in duckstream names its arguments -- and defaulted, so a
    sink written against the phase-1 shape keeps working and simply never seals
    anything, which is the correct behaviour for a sink that does not window.
    """


@runtime_checkable
class Source(Protocol):
    """A replayable origin of rows.

    Replayability is what exactly-once rests on (``PLAN.md``): given the same
    ``start`` and ``end`` offsets, ``plan`` and ``bind`` must produce the same
    rows. Brokers that cannot promise that are modelled as landing writers into
    a source that can.
    """

    type_name: ClassVar[str]

    def latest_offset(self) -> Offset:
        """The furthest position currently available at the origin."""

    def plan(
        self, start: Offset | None, end: Offset, limits: BatchLimits
    ) -> BatchPlan:
        """Carve a bounded batch out of ``(start, end]``, honouring ``limits``."""

    def bind(self, con, plan: BatchPlan) -> str:
        """Register the batch as a view on ``con`` and return the view name."""

    def to_config(self) -> dict[str, Any]:
        """Round-trippable declaration, including the ``"type"`` registry name."""


@runtime_checkable
class Sink(Protocol):
    """An idempotent destination for a batch's output rows."""

    type_name: ClassVar[str]

    def ensure(self, con, model: "Model") -> None:
        """Create whatever DDL the sink needs. Must be idempotent."""

    def write(
        self, con, batch_view: str, model: "Model", ctx: BatchContext
    ) -> int | None:
        """Write the batch. Called inside the engine's transaction, never outside.

        Returns the number of output rows written, or ``None`` if the sink
        cannot say. The engine records it as ``rows_out`` in the batch history
        and leaves the column NULL for ``None``, so a sink written against the
        phase-1 signature -- which returned nothing -- keeps working and simply
        reports no count.
        """

    def to_config(self) -> dict[str, Any]:
        """Round-trippable declaration, including the ``"type"`` registry name."""


@runtime_checkable
class StateStore(Protocol):
    """Durable offsets and watermarks, committed with the data.

    ``CONTEXT.md`` section 1.4 measured that DuckLake commits one snapshot per
    transaction, which is what makes writing output rows, watermarks and source
    offsets atomically together nearly free.
    """

    def ensure(self, con) -> None:
        """Create the state tables. Must be idempotent."""

    def begin(self, con) -> None:
        """Open the trigger's transaction."""

    def load_offset(self, con, model_name: str) -> Offset | None:
        """The committed offset for a model, or ``None`` if it has never run."""

    def commit(
        self, con, offsets: dict[str, Offset], watermarks: dict[str, Any]
    ) -> None:
        """Persist offsets and watermarks and commit the transaction."""
