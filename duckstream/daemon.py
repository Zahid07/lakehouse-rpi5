"""``ProcessingTime`` — the long-lived loop, and the lock discipline it needs.

``PLAN.md`` lists this as post-v1: *"``ProcessingTime`` trigger with portable
locking"*. Two things had to be measured before it could be written, and both
were measured on a Raspberry Pi 5 (``CONTEXT.md`` 1.25):

**It is not a** :class:`~duckstream.trigger.Trigger`. A trigger in duckstream
answers one question -- *after a batch commits, is there another right now?* --
and is consulted only after a **non-empty** batch, because an empty batch always
ends the run. A fixed-interval schedule is the opposite question: *the source is
empty, when should I look again?* Expressing that as a trigger would mean
blocking inside ``should_continue``, which would hold the catalog open across
the wait -- the single thing this design exists to avoid. So the interval owns
the loop and :class:`~duckstream.trigger.AvailableNow` still owns the batch.

**Each cycle builds a fresh engine, and that is a measurement rather than a
preference.** ``CONTEXT.md`` 1.6 says one process holding a DuckDB file locks
everyone else out, and 1.25 confirmed it applies to the DuckLake *catalog*: a
second process cannot ``ATTACH`` it even ``READ_ONLY``. A daemon that attached
once and never let go would lock the operator out of their own warehouse for as
long as it ran, which is exactly the objection ``trigger.py`` used to raise
against building this at all.

Closing the engine each cycle releases the lock, and the two costs that would
make that unaffordable turn out not to exist:

* re-attaching a **warm** process costs ~11 ms, not the ~235 ms of process start
  ``CONTEXT.md`` 1.8 measured for cron;
* ``ensure`` is idempotent, so a fresh engine writes **3 snapshots on the first
  cycle and zero on every one after**. Measured; without that a two-second loop
  would add 43,200 snapshots a day.

A whole cycle -- attach, drain, commit, detach -- measured **~200 ms**. At a
two-second interval the catalog is held about 10% of the time, against cron's
~1 s in 60 with marts up to a minute stale.

**Dropping the memos is correct, not collateral.** ``CONTEXT.md`` 1.10 and 1.11
memoise the batch id and the committed watermark per model, and both say to
revisit "the moment a second writer exists". Detaching creates exactly that
window. A fresh engine starts with empty caches, so the daemon re-reads both
from the catalog each cycle and is sound under a writer that arrived while it
was detached -- which is the same exposure cron already has between ticks.
"""

from __future__ import annotations

import signal
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable, ClassVar, Iterable

from duckstream.errors import DuckstreamError
from duckstream.trigger import AvailableNow
from duckstream.watermark import parse_lateness

__all__ = ["ProcessingTime", "CycleReport", "DaemonReport"]


def parse_interval(value: Any) -> float:
    """A schedule interval in seconds.

    Accepts a number of seconds, a :class:`~datetime.timedelta`, or the same
    duration grammar a lateness horizon uses -- ``"2 seconds"``, ``"1 minute"``.
    The grammar is *shared* rather than re-implemented: two duration parsers
    that accept different strings is a usability bug waiting to be filed, and
    ``duckstream.watermark.parse_lateness`` is already the one users have met.

    Note that grammar takes ``"10 seconds"``, not ``"10s"``.
    """
    if isinstance(value, bool):
        raise DuckstreamError(
            f"ProcessingTime interval must be a duration, got {value!r}"
        )
    if isinstance(value, (int, float)):
        seconds = float(value)
    elif isinstance(value, timedelta):
        seconds = value.total_seconds()
    elif isinstance(value, str):
        try:
            seconds = parse_lateness(value).total_seconds()
        except DuckstreamError as exc:
            # Share the grammar, not the vocabulary. Reusing `parse_lateness`
            # keeps one duration language, but a user setting a schedule should
            # not be told their *lateness* is unparseable -- they never
            # mentioned lateness.
            raise DuckstreamError(
                f"ProcessingTime interval {value!r} is not a duration. Write a "
                f"whole number and a unit, for example '2 seconds', "
                f"'30 seconds' or '1 minute'. Note the unit is spelled out: "
                f"'10s' is not accepted, '10 seconds' is."
            ) from exc
    else:
        raise DuckstreamError(
            f"ProcessingTime interval must be a number of seconds, a "
            f"datetime.timedelta, or a duration string such as '2 seconds'; "
            f"got {type(value).__name__}: {value!r}"
        )
    if seconds <= 0:
        raise DuckstreamError(
            f"ProcessingTime interval must be positive, got {seconds}s. Zero "
            f"would spin the loop with no sleep at all, re-attaching the "
            f"catalog continuously and never releasing it -- which is the one "
            f"behaviour this design exists to avoid."
        )
    return seconds


@dataclass(frozen=True)
class CycleReport:
    """What one cycle did. Emitted through ``on_cycle`` as it happens."""

    cycle: int
    seconds: float
    committed: int = 0
    failed: int = 0
    error: BaseException | None = None
    overran: bool = False
    #: The engine's own ``RunReport`` for this cycle, so a caller can report on
    #: it exactly as it would report a single pass. ``None`` when the cycle
    #: raised before producing one.
    report: Any = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.failed == 0

    def describe(self) -> str:
        if self.error is not None:
            return (f"cycle {self.cycle}: {type(self.error).__name__}: "
                    f"{self.error}")
        state = "ok" if self.ok else f"{self.failed} model(s) unhealthy"
        late = "  OVERRUN" if self.overran else ""
        return (f"cycle {self.cycle}: {state}, {self.committed} batch(es), "
                f"{self.seconds * 1000:.0f} ms{late}")


@dataclass
class DaemonReport:
    """The whole run, for whoever called :meth:`ProcessingTime.run`."""

    cycles: int = 0
    committed: int = 0
    errors: int = 0
    unhealthy: int = 0
    overruns: int = 0
    stopped_by: str = "unknown"
    last_error: BaseException | None = None
    history: list[CycleReport] = field(default_factory=list)

    def describe(self) -> str:
        return (
            f"{self.cycles} cycle(s), {self.committed} batch(es) committed, "
            f"{self.errors} error(s), {self.unhealthy} unhealthy cycle(s), "
            f"{self.overruns} overrun(s); stopped by {self.stopped_by}"
        )


class _StopFlag:
    """Set by a signal, read between cycles. Never interrupts a transaction.

    A daemon killed mid-commit is safe -- the transaction rolls back and the
    offset is not advanced, which is phase 1's whole fault-injection claim. But
    *deliberately* stopping mid-cycle would throw away work that was about to
    commit for no reason, so the flag is checked between cycles only.
    """

    def __init__(self) -> None:
        self.reason: str | None = None
        self._previous: list[tuple[Any, Any]] = []

    def request(self, reason: str) -> None:
        if self.reason is None:
            self.reason = reason

    def install(self, names: Iterable[str] = ("SIGINT", "SIGTERM")) -> None:
        """Catch the usual stop signals, where this platform has them.

        Guarded twice over: ``SIGTERM`` does not exist everywhere, and
        ``signal.signal`` raises outside the main thread. A daemon embedded in
        somebody else's event loop should still start rather than refuse.
        """
        for name in names:
            number = getattr(signal, name, None)
            if number is None:
                continue
            try:
                previous = signal.signal(number, self._handler)
            except (ValueError, OSError, RuntimeError):
                continue           # not the main thread, or not supported
            self._previous.append((number, previous))

    def restore(self) -> None:
        for number, previous in reversed(self._previous):
            try:
                signal.signal(number, previous)
            except (ValueError, OSError, RuntimeError):
                pass
        self._previous.clear()

    def _handler(self, number: int, _frame: Any) -> None:
        self.request(signal.Signals(number).name)


@dataclass(frozen=True)
class ProcessingTime:
    """Drain on a fixed interval, releasing the catalog between cycles.

    ``ProcessingTime`` is a *schedule*, not a
    :class:`~duckstream.trigger.Trigger` -- see the module docstring for why the
    two cannot be the same object. Each cycle builds an engine, drains it with
    :class:`~duckstream.trigger.AvailableNow`, and closes it, which is what lets
    anything else attach the catalog in between.

    ::

        from duckstream import ProcessingTime

        ProcessingTime("2 seconds").run(config="models.yaml")

    Args:
        interval: Seconds between the **start** of one cycle and the next.
            A number, a ``timedelta``, or ``"2 seconds"``.
        max_cycles: Stop after this many. ``None`` runs until signalled, which
            is what a service manager wants.
        stop_on_error: Default ``False``. A daemon exists to keep running when
            something goes wrong -- a quarantined batch, a full disk, a broker
            blip -- and stopping at the first one would defeat the point. Set
            ``True`` for a bounded run where any failure should surface at once.
        max_consecutive_errors: Give up after this many cycles in a row raised.
            ``None`` never gives up. The default of 10 draws the line between a
            transient fault, which a daemon should ride out, and a permanent one
            it is merely logging at speed.
    """

    type_name: ClassVar[str] = "processing_time"

    interval: Any = 2.0
    max_cycles: int | None = None
    stop_on_error: bool = False
    max_consecutive_errors: int | None = 10

    def __post_init__(self) -> None:
        object.__setattr__(self, "seconds", parse_interval(self.interval))
        for name in ("max_cycles", "max_consecutive_errors"):
            value = getattr(self, name)
            if value is None:
                continue
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise DuckstreamError(
                    f"ProcessingTime({name}={value!r}) must be a positive "
                    f"integer or None."
                )

    def describe(self) -> str:
        every = f"every {self.seconds:g}s"
        bound = "" if self.max_cycles is None else f", at most {self.max_cycles} cycles"
        return f"ProcessingTime ({every}{bound}, catalog released between cycles)"

    # -- the loop ----------------------------------------------------------

    def run(
        self,
        factory: Callable[[], Any] | None = None,
        *,
        config: Any | None = None,
        model: Any | None = None,
        drain: Callable[[Any], Any] | None = None,
        on_cycle: Callable[[CycleReport], None] | None = None,
        install_signal_handlers: bool = True,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], float] = time.monotonic,
    ) -> DaemonReport:
        """Loop until signalled, until ``max_cycles``, or until it gives up.

        Args:
            factory: Returns a fresh, ready :class:`~duckstream.engine.Engine`
                per cycle. Mutually exclusive with ``config``.
            config: A YAML path, built through
                :meth:`~duckstream.engine.Engine.from_config` each cycle. The
                engine then owns its connection, so closing it really does
                release the catalog.
            model: Restrict the drain to one model or an iterable of names.
            drain: How to drain one engine, returning its ``RunReport``.
                Defaults to ``engine.run(trigger=AvailableNow(), model=model)``.
                The CLI overrides it to unwrap ``BatchFailed``, which is a
                *verdict* rather than a crash -- every model already had its
                turn -- so a quarantined batch is an unhealthy cycle, not a
                failed one, and the daemon keeps running either way.
            on_cycle: Called with each :class:`CycleReport` as it completes --
                the logging seam, so this module prints nothing itself.
            install_signal_handlers: Catch ``SIGINT``/``SIGTERM`` and stop
                cleanly **after** the current cycle. Turn off when embedding.
            sleep, now: Injected so the loop is testable without real time.
        """
        if (factory is None) == (config is None):
            raise DuckstreamError(
                "ProcessingTime.run needs exactly one of `factory` or "
                "`config`: a factory returning a fresh Engine per cycle, or a "
                "YAML path to build one from."
            )
        if factory is None:
            from duckstream.engine import Engine

            def factory() -> Any:                       # noqa: F811
                return Engine.from_config(config)

        if drain is None:
            trigger = AvailableNow()

            def drain(engine: Any) -> Any:              # noqa: F811
                return engine.run(trigger=trigger, model=model)

        report = DaemonReport()
        flag = _StopFlag()
        if install_signal_handlers:
            flag.install()

        consecutive = 0
        try:
            while True:
                if flag.reason is not None:
                    report.stopped_by = f"signal {flag.reason}"
                    break
                if self.max_cycles is not None and report.cycles >= self.max_cycles:
                    report.stopped_by = "max_cycles"
                    break

                started = now()
                cycle = self._one_cycle(
                    factory, drain, report.cycles + 1, started, now
                )

                report.cycles += 1
                report.committed += cycle.committed
                report.history.append(cycle)
                if cycle.error is not None:
                    report.errors += 1
                    report.last_error = cycle.error
                    consecutive += 1
                else:
                    consecutive = 0
                if cycle.failed:
                    report.unhealthy += 1

                # Overrun accounting rather than an arbitrary minimum interval.
                # A cycle measured ~200 ms on a Pi 5, so a sub-second interval
                # is not forbidden -- it is simply reported as not keeping up,
                # which is a fact about the deployment rather than a rule.
                remaining = self.seconds - (now() - started)
                if remaining <= 0:
                    report.overruns += 1
                    cycle = _mark_overrun(cycle)
                    report.history[-1] = cycle

                if on_cycle is not None:
                    on_cycle(cycle)

                if cycle.error is not None and self.stop_on_error:
                    report.stopped_by = "error"
                    break
                if (
                    self.max_consecutive_errors is not None
                    and consecutive >= self.max_consecutive_errors
                ):
                    report.stopped_by = (
                        f"{consecutive} consecutive errors"
                    )
                    break
                # Checked *before* sleeping, not only at the top of the loop: a
                # bounded run that has finished its last cycle should exit now,
                # not idle out one more interval first. With `--max-cycles 1`
                # that is the difference between returning at once and hanging
                # for the whole interval.
                if self.max_cycles is not None and report.cycles >= self.max_cycles:
                    report.stopped_by = "max_cycles"
                    break

                if remaining > 0:
                    # Sleep in slices so a signal is noticed promptly rather
                    # than after a long interval has elapsed.
                    self._sleep_until(remaining, flag, sleep, now)
        finally:
            flag.restore()
        if report.stopped_by == "unknown":
            report.stopped_by = "loop exit"
        return report

    @staticmethod
    def _sleep_until(
        remaining: float,
        flag: _StopFlag,
        sleep: Callable[[float], None],
        now: Callable[[], float],
    ) -> None:
        deadline = now() + remaining
        while flag.reason is None:
            left = deadline - now()
            if left <= 0:
                return
            sleep(min(0.25, left))

    def _one_cycle(
        self,
        factory: Callable[[], Any],
        drain: Callable[[Any], Any],
        number: int,
        started: float,
        now: Callable[[], float],
    ) -> CycleReport:
        """Build, drain, close. Every exit path closes the engine.

        Errors are **contained**: a cycle that raises is recorded and the loop
        continues, because the alternative is a daemon that dies on the first
        transient fault. The engine's own contract already holds inside this --
        `_run_batch` records a failed batch and returns rather than raising, and
        `run()` raises only once every model has had its turn -- so a raise here
        means the whole run failed, not one model.
        """
        engine = None
        committed = failed = 0
        error: BaseException | None = None
        result: Any = None
        try:
            engine = factory()
            result = drain(engine)
            for outcome in getattr(result, "results", ()) or ():
                if getattr(outcome, "committed", False):
                    committed += 1
                if getattr(outcome, "failed", False) or getattr(
                    outcome, "error", None
                ):
                    failed += 1
        except BaseException as exc:                     # noqa: BLE001
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            error = exc
        finally:
            if engine is not None:
                try:
                    engine.close()      # releases the catalog -- the point
                except Exception:       # noqa: BLE001
                    pass
        return CycleReport(
            cycle=number,
            seconds=now() - started,
            committed=committed,
            failed=failed,
            error=error,
            report=result,
        )


def _mark_overrun(cycle: CycleReport) -> CycleReport:
    return CycleReport(
        cycle=cycle.cycle,
        seconds=cycle.seconds,
        committed=cycle.committed,
        failed=cycle.failed,
        error=cycle.error,
        overran=True,
        report=cycle.report,
    )
