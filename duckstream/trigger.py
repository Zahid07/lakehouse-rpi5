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

:class:`~duckstream.daemon.ProcessingTime` is the third thing people reach for,
and it is deliberately **not** in this module: it is a *schedule*, not a
trigger. The distinction is the one this file is built on — a trigger is asked
"another batch right now?" only after a non-empty batch has committed, so it can
never express "nothing is there, wait and look again". It lives in
:mod:`duckstream.daemon`, drives its loop with :class:`AvailableNow`, and
releases the catalog between cycles. A redirect at the bottom of this module
keeps the old import path working.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Protocol, runtime_checkable

from duckstream.errors import DuckstreamError

__all__ = ["Trigger", "AvailableNow", "Once"]


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


def __getattr__(name: str) -> Any:
    """``ProcessingTime`` used to live here, refusing. It now exists, elsewhere.

    Kept as a redirect rather than deleted, because ``from duckstream.trigger
    import ProcessingTime`` is what anyone who read the old docstring will
    write. It moved to :mod:`duckstream.daemon` because it turned out **not to
    be a trigger at all** — a trigger answers "another batch right now?" after
    a non-empty batch commits, and a schedule answers "the source is empty,
    when do I look again?". Expressing the second as the first would mean
    blocking inside :meth:`Trigger.should_continue`, holding the catalog across
    the wait, which is the one thing ``CONTEXT.md`` 1.6 and 1.25 say not to do.
    """
    if name == "ProcessingTime":
        from duckstream.daemon import ProcessingTime

        return ProcessingTime
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
