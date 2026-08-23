"""Triggers — how many batches one :meth:`~duckstream.engine.Engine.run` drains.

A trigger in duckstream is deliberately not a scheduler. ``PLAN.md`` puts the
trigger loop "in the host process, never in the database", and v1 goes one step
further: the loop does not even own a clock. There is no background thread, no
timer and no supervisor here — a trigger only answers one question, *after* a
batch has committed: is there another batch to run right now?

That answer is the whole difference between the two v1 triggers:

============================  ========================================
:class:`AvailableNow`         drain everything currently available
:class:`Once`                 exactly one batch, then return
============================  ========================================

Two measurements make this the right shape rather than a simplification.
``CONTEXT.md`` 1.6 found that while one process holds a DuckDB *file* no other
process can open it, even read-only — so a process that opens, drains and exits
leaves the warehouse usable, and one that loops forever would not. ``CONTEXT.md``
1.8 found the floor under cron is ~235 ms of process start plus ~17 ms per
committing trigger, which makes seconds the sensible scheduling unit and a
sub-second in-process timer pointless. Cron, systemd or a supervisor owns the
cadence; duckstream owns the batch.

:class:`ProcessingTime` exists only to refuse. It is the trigger people reach
for first, and a clear rejection is worth more than an ``AttributeError``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Protocol, runtime_checkable

from duckstream.errors import DuckstreamError

__all__ = ["Trigger", "AvailableNow", "Once", "ProcessingTime"]


@runtime_checkable
class Trigger(Protocol):
    """Decides whether the engine runs another batch for the same model.

    Structural, like every other duckstream seam: any object with
    ``type_name``, :meth:`should_continue` and :meth:`describe` is a trigger,
    with no base class to inherit.

    :meth:`should_continue` is consulted **only after a non-empty batch has
    committed**. An empty batch always ends the loop — there is nothing left to
    read, and ``CONTEXT.md`` 1.8 measured that an idle pass which writes nothing
    costs ~1.3 ms against ~17 ms for one that writes, so spinning on emptiness
    is both pointless and not free.
    """

    type_name: ClassVar[str]

    def should_continue(self, *, batches: int, has_more: bool) -> bool:
        """``batches`` have committed and the source says ``has_more``. Again?"""

    def describe(self) -> str:
        """One short line for a log or a CLI summary."""


@dataclass(frozen=True)
class AvailableNow:
    """Drain everything available at the moment the run started, then return.

    This is the trigger phase 1 is built around and the one the CLI's ``run``
    command uses. A batch may be truncated by
    :class:`~duckstream.protocols.BatchLimits` — ``max_files_per_trigger`` and
    ``max_rows_per_trigger``, the memory lever ``CONTEXT.md`` 1.1 identified —
    and when it is, the source sets ``has_more`` and this trigger runs another
    batch. When a batch comes back with nothing, the run is over.

    Args:
        max_batches: Optional safety cap on batches per model per run. ``None``
            (the default) means drain fully. A cap is not a memory control —
            :class:`~duckstream.protocols.BatchLimits` is — it is a way to bound
            how long one cron tick may take when a large backlog is being caught
            up, so a tick cannot overrun the next one indefinitely.
    """

    type_name: ClassVar[str] = "available_now"

    max_batches: int | None = None

    def __post_init__(self) -> None:
        limit = self.max_batches
        if limit is None:
            return
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise DuckstreamError(
                f"AvailableNow(max_batches={limit!r}) must be a positive integer "
                f"or None. Zero would drain nothing while still paying the "
                f"~235 ms of process start CONTEXT.md 1.8 measured."
            )

    def should_continue(self, *, batches: int, has_more: bool) -> bool:
        if not has_more:
            return False
        return self.max_batches is None or batches < self.max_batches

    def describe(self) -> str:
        if self.max_batches is None:
            return "AvailableNow (drain everything available)"
        return f"AvailableNow (at most {self.max_batches} batches per model)"


@dataclass(frozen=True)
class Once:
    """Run exactly one batch per model, then return, however much is left.

    Useful for stepping a pipeline forward deliberately: a test that wants to
    observe the state between two batches, a backfill drained one bounded chunk
    per invocation, or a first run on an unfamiliar landing tree where draining
    everything is not yet the plan.

    ``Once`` does not mean "one batch ever". It means one batch per call, and
    the committed offset makes the next call resume exactly where this one
    stopped.
    """

    type_name: ClassVar[str] = "once"

    def should_continue(self, *, batches: int, has_more: bool) -> bool:
        del batches, has_more  # one batch, whatever is left behind
        return False

    def describe(self) -> str:
        return "Once (exactly one batch per model)"


class ProcessingTime:
    """Post-v1. Refuses construction rather than pretending to schedule.

    A fixed-interval trigger needs three things v1 deliberately does not have: a
    long-lived process, a portable lock (never ``fcntl`` — it is POSIX-only and
    breaks import on Windows, ``CONTEXT.md`` section 5), and a second writer to
    reason about, which would invalidate the single-writer assumptions in
    ``CONTEXT.md`` 2.5 and the memoised batch id in 1.10.

    None of that is needed to run duckstream on a schedule today: cron or a
    supervisor calls ``duckstream run`` and :class:`AvailableNow` drains what
    has arrived. ``CONTEXT.md`` 1.8 measured the real floor under cron at about
    0.3 s including interpreter start, so seconds is the meaningful unit anyway.
    """

    type_name: ClassVar[str] = "processing_time"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise DuckstreamError(
            "ProcessingTime is post-v1: cron owns the loop. duckstream v1 has no "
            "background thread and no timer — a process opens the catalog, "
            "drains it with AvailableNow and exits, which is also what keeps the "
            "warehouse readable (CONTEXT.md 1.6: while one process holds a "
            "DuckDB file, nothing else can open it, not even read-only). "
            "Schedule `duckstream run --config models.yaml` from cron or a "
            "supervisor instead; CONTEXT.md 1.8 measured ~235 ms of process "
            "start per tick, so seconds is the sensible interval. A daemon "
            "trigger arrives with a portable lock, post-v1."
        )
