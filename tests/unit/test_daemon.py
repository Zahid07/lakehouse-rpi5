"""``ProcessingTime`` — the loop, and the catalog release that justifies it.

Every test here runs on a fake engine and an injected clock. That is deliberate
rather than lazy: the behaviour worth pinning is *when the engine is closed* and
*when the loop stops*, and neither needs a catalog to observe. The one claim
that does need real DuckLake -- that closing the engine actually frees the
catalog for another process -- is a conformance concern, and ``CONTEXT.md`` 1.25
is the measurement behind it.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from duckstream.daemon import CycleReport, DaemonReport, ProcessingTime, parse_interval
from duckstream.errors import DuckstreamError


class FakeEngine:
    """Counts constructions and closes, and can be told to misbehave."""

    made = 0
    closed = 0
    raise_on_run: BaseException | None = None

    def __init__(self) -> None:
        type(self).made += 1
        self.closed_self = False

    def run(self, **_kwargs):
        if type(self).raise_on_run is not None:
            raise type(self).raise_on_run
        return _Report([])

    def close(self) -> None:
        type(self).closed += 1
        self.closed_self = True


class _Report:
    def __init__(self, results):
        self.results = results


class _Outcome:
    def __init__(self, committed=False, failed=False):
        self.committed = committed
        self.failed = failed
        self.error = None


@pytest.fixture(autouse=True)
def _reset():
    FakeEngine.made = FakeEngine.closed = 0
    FakeEngine.raise_on_run = None
    yield
    FakeEngine.raise_on_run = None


class Clock:
    """A clock the loop cannot outrun, so tests take no real time."""

    def __init__(self) -> None:
        self.t = 0.0

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


def drive(schedule: ProcessingTime, factory=FakeEngine, **kwargs) -> DaemonReport:
    clock = Clock()
    kwargs.setdefault("install_signal_handlers", False)
    return schedule.run(factory=factory, sleep=clock.sleep, now=clock.now, **kwargs)


# ---------------------------------------------------------------------------
# the interval
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [(2, 2.0), (2.5, 2.5), ("2 seconds", 2.0), ("1 minute", 60.0),
     (timedelta(seconds=30), 30.0)],
)
def test_an_interval_is_accepted_in_every_form_a_user_might_write(value, expected):
    assert parse_interval(value) == expected


@pytest.mark.parametrize("value", [0, -1, -0.5, timedelta(0)])
def test_a_non_positive_interval_is_refused(value):
    """Zero would spin the loop, re-attaching the catalog without ever
    releasing it -- the one behaviour the design exists to prevent."""
    with pytest.raises(DuckstreamError, match="positive"):
        parse_interval(value)


def test_a_bad_interval_string_talks_about_intervals_not_lateness():
    """The duration grammar is shared with `parse_lateness`; the vocabulary is
    not. A user setting a schedule never mentioned lateness."""
    with pytest.raises(DuckstreamError) as caught:
        parse_interval("10s")
    message = str(caught.value)
    assert "interval" in message
    assert "lateness" not in message
    assert "10 seconds" in message, "the message should show the accepted form"


@pytest.mark.parametrize("value", [None, object(), True])
def test_an_interval_of_the_wrong_type_is_refused(value):
    with pytest.raises(DuckstreamError):
        parse_interval(value)


@pytest.mark.parametrize("field", ["max_cycles", "max_consecutive_errors"])
@pytest.mark.parametrize("bad", [0, -1, 1.5, True])
def test_the_counters_must_be_positive_integers(field, bad):
    with pytest.raises(DuckstreamError, match="positive integer"):
        ProcessingTime(2, **{field: bad})


# ---------------------------------------------------------------------------
# the catalog release -- the reason this class exists
# ---------------------------------------------------------------------------


def test_every_cycle_closes_its_engine():
    """The whole justification for a fresh engine per cycle.

    `CONTEXT.md` 1.25 measured that a second process cannot ATTACH the catalog
    even READ_ONLY while one holds it, so an engine left open is an operator
    locked out of their own warehouse for as long as the daemon runs.
    """
    drive(ProcessingTime(2, max_cycles=4))
    assert FakeEngine.made == 4
    assert FakeEngine.closed == 4, "an engine left open holds the catalog"


def test_the_engine_is_closed_even_when_the_drain_raises():
    """The failure path is the one that matters: a cycle that raises and leaks
    its engine locks the catalog for ever, and the next cycle cannot attach."""
    FakeEngine.raise_on_run = RuntimeError("boom mid-drain")
    report = drive(ProcessingTime(1, max_cycles=3))
    assert FakeEngine.made == 3
    assert FakeEngine.closed == 3
    assert report.errors == 3


def test_each_cycle_builds_a_new_engine_rather_than_reusing_one():
    """Reuse would keep the memoised batch id and watermark (CONTEXT.md 1.10,
    1.11) across a window in which another writer could have committed. Both
    measurements say to revisit those memos the moment a second writer exists,
    and detaching creates exactly that window."""
    # The engines are *kept*, not their ids: CPython recycles an id as soon as
    # the object is collected, so short-lived per-cycle engines can share one
    # and this assertion would fail for a reason that has nothing to do with
    # the loop. Holding a reference makes the identities genuinely distinct.
    seen: list[FakeEngine] = []

    def factory():
        engine = FakeEngine()
        seen.append(engine)
        return engine

    drive(ProcessingTime(1, max_cycles=3), factory=factory)
    assert len(seen) == 3
    assert len({id(engine) for engine in seen}) == 3, (
        "the same engine was reused across cycles"
    )
    assert all(engine.closed_self for engine in seen)


# ---------------------------------------------------------------------------
# stopping
# ---------------------------------------------------------------------------


def test_max_cycles_stops_the_loop():
    report = drive(ProcessingTime(2, max_cycles=3))
    assert report.cycles == 3
    assert report.stopped_by == "max_cycles"


def test_a_bounded_run_does_not_idle_out_a_final_interval():
    """`--max-cycles 1` should return once the cycle is done, not sleep first."""
    clock = Clock()
    ProcessingTime(60, max_cycles=1).run(
        factory=FakeEngine, install_signal_handlers=False,
        sleep=clock.sleep, now=clock.now,
    )
    assert clock.t == 0.0, "the loop slept an interval after its last cycle"


def test_the_interval_is_measured_from_the_start_of_each_cycle():
    """Three cycles at 2 s is 4 s of sleeping, not 6 -- the last one does not
    sleep, and each gap is the remainder of the interval rather than a fresh
    one stacked on top of the work."""
    clock = Clock()
    ProcessingTime(2, max_cycles=3).run(
        factory=FakeEngine, install_signal_handlers=False,
        sleep=clock.sleep, now=clock.now,
    )
    assert clock.t == pytest.approx(4.0)


# ---------------------------------------------------------------------------
# errors: contained, but not forever
# ---------------------------------------------------------------------------


def test_a_failing_cycle_does_not_stop_the_daemon():
    """A daemon exists to survive a transient fault. Stopping at the first one
    -- a full disk, a broker blip, a catalog briefly held elsewhere -- would
    defeat the point of running unattended."""
    class Boom:
        def __init__(self):
            raise RuntimeError("catalog held by someone else")

    report = drive(ProcessingTime(1, max_cycles=5), factory=Boom)
    assert report.cycles == 5
    assert report.errors == 5
    assert report.stopped_by == "max_cycles"


def test_stop_on_error_stops_at_the_first_one():
    class Boom:
        def __init__(self):
            raise RuntimeError("nope")

    report = drive(ProcessingTime(1, stop_on_error=True), factory=Boom)
    assert report.cycles == 1
    assert report.stopped_by == "error"


def test_the_daemon_gives_up_after_enough_consecutive_errors():
    """Riding out a transient fault is the point; logging a permanent one at
    full speed for ever is not."""
    class Boom:
        def __init__(self):
            raise RuntimeError("permanent")

    report = drive(ProcessingTime(1, max_consecutive_errors=3), factory=Boom)
    assert report.cycles == 3
    assert "consecutive" in report.stopped_by


def test_a_successful_cycle_resets_the_consecutive_error_count():
    """Otherwise a daemon that fails intermittently but recovers every time
    still gives up, which is the opposite of what the budget is for."""
    state = {"n": 0}

    def factory():
        state["n"] += 1
        if state["n"] % 2 == 1:      # fail, succeed, fail, succeed...
            raise RuntimeError("intermittent")
        return FakeEngine()

    report = drive(
        ProcessingTime(1, max_cycles=8, max_consecutive_errors=2), factory=factory
    )
    assert report.cycles == 8, "it gave up despite recovering between failures"
    assert report.stopped_by == "max_cycles"


def test_a_keyboard_interrupt_is_not_swallowed_as_a_cycle_error():
    """Ctrl-C must stop the process, not be logged as one more failed cycle
    and retried a second later."""
    class Interrupted:
        def __init__(self):
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        drive(ProcessingTime(1, max_cycles=3), factory=Interrupted)


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------


def test_a_cycle_carries_the_engine_report_so_a_caller_can_summarise_it():
    """The CLI reports a daemon cycle with exactly the code that reports a
    single pass; that only works if the run report survives the cycle."""
    committed = _Report([_Outcome(committed=True), _Outcome(committed=True)])

    class Engine:
        def run(self, **_k):
            return committed

        def close(self):
            pass

    seen: list[CycleReport] = []
    report = drive(
        ProcessingTime(1, max_cycles=1), factory=Engine, on_cycle=seen.append
    )
    assert seen[0].report is committed
    assert seen[0].committed == 2
    assert report.committed == 2


def test_an_unhealthy_model_is_counted_apart_from_a_crashed_cycle():
    """A quarantined batch is a verdict, not a crash. Folding the two together
    would make `stop_on_error` fire on data the pipeline handled correctly."""
    class Engine:
        def run(self, **_k):
            return _Report([_Outcome(failed=True)])

        def close(self):
            pass

    report = drive(ProcessingTime(1, max_cycles=2), factory=Engine)
    assert report.unhealthy == 2
    assert report.errors == 0, "an unhealthy model is not a failed cycle"


def test_a_cycle_longer_than_the_interval_is_reported_as_an_overrun():
    """Not forbidden -- reported. A cycle measured ~200 ms on a Pi 5, so a
    sub-second interval is achievable but leaves no headroom; whether it keeps
    up is a fact about the deployment rather than a rule to enforce."""
    clock = Clock()

    class Slow:
        def run(self, **_k):
            clock.t += 5.0          # the work outlasts the interval
            return _Report([])

        def close(self):
            pass

    report = ProcessingTime(2, max_cycles=2).run(
        factory=Slow, install_signal_handlers=False,
        sleep=clock.sleep, now=clock.now,
    )
    assert report.overruns == 2
    assert all(c.overran for c in report.history)


def test_the_drain_can_be_overridden():
    """The CLI overrides it to unwrap `BatchFailed`, which is a verdict rather
    than a crash. Without the seam the daemon would record a quarantined batch
    as a failed cycle and count it against the give-up budget."""
    calls = []

    def drain(engine):
        calls.append(engine)
        return _Report([_Outcome(committed=True)])

    report = drive(ProcessingTime(1, max_cycles=2), drain=drain)
    assert len(calls) == 2
    assert report.committed == 2


def test_run_needs_exactly_one_of_factory_or_config():
    with pytest.raises(DuckstreamError, match="exactly one"):
        ProcessingTime(1).run()
    with pytest.raises(DuckstreamError, match="exactly one"):
        ProcessingTime(1).run(factory=FakeEngine, config="models.yaml")


def test_describe_says_the_interval_and_the_release():
    text = ProcessingTime("2 seconds", max_cycles=5).describe()
    assert "2s" in text
    assert "5" in text
    assert "released" in text


# ---------------------------------------------------------------------------
# signals
# ---------------------------------------------------------------------------


def test_a_stop_signal_ends_the_loop_between_cycles_not_inside_one():
    """A daemon killed mid-commit is safe -- the transaction rolls back and the
    offset does not advance. But deliberately stopping mid-cycle would throw
    away work that was about to commit for no reason at all."""
    from duckstream.daemon import _StopFlag

    flag = _StopFlag()
    finished = []

    class Engine:
        def run(self, **_k):
            flag.request("SIGTERM")      # arrives during the drain
            return _Report([])

        def close(self):
            finished.append(True)

    schedule = ProcessingTime(1, max_cycles=5)
    clock = Clock()
    # Drive the real loop with our flag by patching the constructor it uses.
    import duckstream.daemon as daemon

    original = daemon._StopFlag
    daemon._StopFlag = lambda: flag
    try:
        report = schedule.run(
            factory=Engine, install_signal_handlers=False,
            sleep=clock.sleep, now=clock.now,
        )
    finally:
        daemon._StopFlag = original

    assert report.cycles == 1, "the cycle in flight should have completed"
    assert finished == [True], "and its engine closed"
    assert "SIGTERM" in report.stopped_by


def test_signal_handlers_are_restored_afterwards():
    """A daemon embedded in a larger process must not leave its handlers behind."""
    import signal

    before = signal.getsignal(signal.SIGINT)
    ProcessingTime(1, max_cycles=1).run(
        factory=FakeEngine, install_signal_handlers=True,
        sleep=lambda _s: None, now=lambda: 0.0,
    )
    assert signal.getsignal(signal.SIGINT) is before
