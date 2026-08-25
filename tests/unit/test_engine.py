"""Tests for :mod:`duckstream.engine`, :mod:`duckstream.trigger` and the CLI.

**Every test here runs against a real DuckLake catalog under ``tmp_path``.**
There is no in-memory shortcut, and that is not caution for its own sake:
``CONTEXT.md`` 1.5 measured a statement that passes on in-memory DuckDB and
raises ``Out of buffer`` against DuckLake — on the *second* batch, the first to
take the ``WHEN MATCHED`` branch. So the end-to-end tests run **at least two
batches**, and the aggregation is diffed against a full recompute from source
rather than against a hand-written expectation, because a hand-written
expectation is exactly what the mart bugs in ``CONTEXT.md`` section 4 agreed
with.

Three assertions carry the phase-1 claim:

* :func:`test_an_idle_run_opens_no_transaction_and_adds_no_snapshot` —
  ``CONTEXT.md`` 1.8. An idle pass costs ~1.3 ms only if it writes nothing at
  all, so "adds zero snapshots" is a correctness property, not a saving.
* :func:`test_one_batch_is_exactly_one_snapshot` — ``CONTEXT.md`` 1.4. Output
  rows, batch record and offset in one snapshot *is* the exactly-once mechanism.
* :func:`test_a_crash_between_sink_write_and_commit_loses_nothing` — the
  in-process shadow of the real fault injection. W4 does the process kill; this
  proves the transaction boundary is where the engine says it is.
"""

from __future__ import annotations

import datetime as dt
import io
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import duckdb
import pytest

from duckstream import cli
from duckstream.engine import FAULT_POINTS, Engine
from duckstream import state as state_module
from duckstream.errors import BatchFailed, DuckstreamError
from duckstream.lake import data_file_count, snapshot_count
from duckstream.model import Model
from duckstream.offsets import FileOffset, encode_offset
from duckstream.protocols import BatchLimits, BatchPlan
from duckstream.sinks.table import TableSink
from duckstream.sources.files import FileSource
from duckstream.trigger import AvailableNow, Once, ProcessingTime

REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def landing(tmp_path):
    """An empty landing tree, plus a writer that drops a marked batch into it."""
    root = tmp_path / "landing"
    root.mkdir()
    return root


def drop_batch(landing: Path, name: str, rows: int, *, first: int = 0) -> Path:
    """Write one ready directory of ``rows`` rows, marker last.

    Marker last is the contract from ``CONTEXT.md`` section 5: write the data,
    then drop the marker — never the other order, or a half-written file becomes
    eligible.
    """
    directory = landing / name
    directory.mkdir(parents=True, exist_ok=True)
    target = (directory / "part.parquet").as_posix()
    con = duckdb.connect()
    try:
        con.execute(
            "COPY (SELECT TIMESTAMP '2026-08-22 10:00:00' "
            "        + INTERVAL (i) MINUTE AS event_ts, "
            "      'sensor' || (i % 3) AS sensor_id, "
            "      i::DOUBLE AS value "
            f"FROM range({first}, {first + rows}) t(i)) "
            f"TO '{target}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    (directory / "_READY").write_text("", encoding="utf-8")
    return directory


@pytest.fixture
def no_backoff(monkeypatch):
    """Retry immediately, for tests that are about something other than backoff.

    A recorded failure holds the model back for ``backoff_delay(attempt)`` --
    one second on the first retry. That is the right behaviour and
    :func:`test_a_failed_batch_is_held_back_before_it_is_retried` covers it, but
    in a test about *recovery* it would mean either sleeping or asserting
    nothing. Zeroing the base makes the delay zero at every attempt count.
    """
    monkeypatch.setattr(state_module, "BACKOFF_BASE", dt.timedelta(0))


def counts_model(landing: Path, **overrides) -> Model:
    """The phase-1 model: a file source, an additive aggregate, an update sink."""
    settings = dict(
        name="hourly_counts",
        source=FileSource(Path(landing).as_posix(), marker="_READY"),
        time_column="event_ts",
        grain="hour",
        key=["window_ts", "sensor_id"],
        aggregates={"n": "count(*)", "total": "sum(value)"},
        sink=TableSink("marts.hourly_counts", mode="update"),
    )
    settings.update(overrides)
    return Model(**settings)


def open_engine(tmp_path, *, connection=None) -> Engine:
    """An engine on ``tmp_path``'s catalog, on a fresh connection by default."""
    con = connection if connection is not None else duckdb.connect()
    return Engine(
        con,
        catalog=tmp_path / "catalog.ducklake",
        data_path=tmp_path / "lake_data",
    )


def recompute(con, landing: Path) -> list[tuple]:
    """Ground truth: the whole landing tree aggregated in one pass."""
    pattern = (landing / "**" / "*.parquet").as_posix()
    return con.execute(
        "SELECT date_trunc('hour', event_ts) AS window_ts, sensor_id, "
        "       count(*) AS n, sum(value) AS total "
        f"FROM read_parquet('{pattern}') "
        "GROUP BY 1, 2 ORDER BY 1, 2"
    ).fetchall()


def table_exists(con, name: str) -> bool:
    schema, _, table = name.rpartition(".")
    return bool(
        con.execute(
            "SELECT count(*) FROM duckdb_tables() "
            "WHERE schema_name = ? AND table_name = ?",
            [schema, table],
        ).fetchone()[0]
    )


def sink_rows(con) -> list[tuple]:
    return con.execute(
        "SELECT window_ts, sensor_id, n, total FROM marts.hourly_counts "
        "ORDER BY window_ts, sensor_id"
    ).fetchall()


def consumed_relpaths(engine, model_name: str) -> list[str]:
    """What the catalog says this model has read, straight from the rows.

    The offset stopped carrying this in phase 4 (``CONTEXT.md`` 1.15), and the
    distinction is worth keeping in the assertions rather than hiding behind a
    helper that could read either: the offset's count is a *report*, and these
    rows are what the next ``plan`` actually consults.
    """
    return sorted(
        row[0]
        for row in engine.con.execute(
            f"SELECT relpath FROM {engine.state.consumed_files_table} "
            f"WHERE model_name = ?",
            [model_name],
        ).fetchall()
    )


def write_config(tmp_path, landing: Path, **model_overrides) -> Path:
    """A YAML document equivalent to :func:`counts_model`."""
    extra = "".join(f"    {k}: {v}\n" for k, v in model_overrides.items())
    path = tmp_path / "models.yaml"
    path.write_text(
        textwrap.dedent(
            f"""\
            catalog: "ducklake:{(tmp_path / 'catalog.ducklake').as_posix()}"
            data_path: "{(tmp_path / 'lake_data').as_posix()}"

            models:
              - name: hourly_counts
                source:
                  type: file
                  path: "{landing.as_posix()}"
                  marker: _READY
                time_column: event_ts
                grain: hour
                key: [window_ts, sensor_id]
                aggregates:
                  n: "count(*)"
                  total: "sum(value)"
                sink:
                  type: table
                  table: marts.hourly_counts
                  mode: update
            """
        )
        + extra,
        encoding="utf-8",
    )
    return path


# ---------------------------------------------------------------------------
# triggers
# ---------------------------------------------------------------------------


def test_available_now_continues_only_while_the_source_has_more():
    trigger = AvailableNow()
    assert trigger.should_continue(batches=1, has_more=True) is True
    assert trigger.should_continue(batches=99, has_more=False) is False


def test_available_now_max_batches_caps_one_run():
    trigger = AvailableNow(max_batches=2)
    assert trigger.should_continue(batches=1, has_more=True) is True
    assert trigger.should_continue(batches=2, has_more=True) is False
    with pytest.raises(DuckstreamError, match="positive integer"):
        AvailableNow(max_batches=0)


def test_once_never_continues():
    assert Once().should_continue(batches=1, has_more=True) is False


def test_processing_time_refuses_construction_with_a_useful_message():
    with pytest.raises(DuckstreamError) as caught:
        ProcessingTime(interval="10s")
    message = str(caught.value)
    assert "post-v1" in message
    assert "cron" in message


# ---------------------------------------------------------------------------
# the batch lifecycle
# ---------------------------------------------------------------------------


def test_two_batches_match_a_full_recompute(tmp_path, landing):
    """End to end, and diffed against ground truth rather than an expectation.

    ``max_files_per_trigger=1`` forces at least two batches, so the merge takes
    its ``WHEN MATCHED`` branch — the branch ``CONTEXT.md`` 1.5 found a DuckLake
    failure hiding behind.
    """
    drop_batch(landing, "b1", 12)
    drop_batch(landing, "b2", 9, first=12)

    engine = open_engine(tmp_path)
    engine.add(counts_model(landing, limits=BatchLimits(max_files_per_trigger=1)))
    report = engine.run(trigger=AvailableNow())

    assert len(report.batches) == 2
    assert [r.batch_id for r in report.batches] == [1, 2]
    assert report.rows_in == 21
    assert sink_rows(engine.con) == recompute(engine.con, landing)
    # Inlining stayed off, so the rows are in parquet and not in the catalog.
    assert data_file_count(engine.con, "lake", "marts.hourly_counts") >= 1
    engine.con.close()


def test_one_batch_is_exactly_one_snapshot(tmp_path, landing):
    """``CONTEXT.md`` 1.4 — the primitive the exactly-once claim rests on."""
    drop_batch(landing, "b1", 5)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    engine.run()  # first run also pays for the state and schema DDL

    drop_batch(landing, "b2", 5, first=5)
    before = snapshot_count(engine.con)
    engine.run()
    assert snapshot_count(engine.con) - before == 1
    engine.con.close()


def test_an_idle_run_opens_no_transaction_and_adds_no_snapshot(tmp_path, landing):
    """``CONTEXT.md`` 1.8 — an idle trigger must stay on the cheap side of 15 ms.

    A fresh engine, so the run also pays for whatever setup a cron tick pays on
    an existing catalog. Nothing may open a transaction and nothing may write.
    """
    drop_batch(landing, "b1", 4)
    first = open_engine(tmp_path)
    first.add(counts_model(landing))
    first.run()
    first.con.close()

    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    opened: list[int] = []
    real_begin = engine.state.begin
    engine.state.begin = lambda con: (opened.append(1), real_begin(con))[1]

    before = snapshot_count(engine.con)
    report = engine.run()

    assert opened == []
    assert snapshot_count(engine.con) - before == 0
    assert len(report.results) == 1
    assert report.results[0].is_empty is True
    assert report.batches == ()
    engine.con.close()


def test_offsets_advance_and_a_second_run_changes_nothing(tmp_path, landing):
    drop_batch(landing, "b1", 7)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    engine.run()

    offset = engine.state.load_offset(engine.con, "hourly_counts")
    assert offset is not None
    # The consumed set is rows now, not a map inside the offset (CONTEXT.md
    # 1.15), so the offset carries a count and the table carries the identity.
    # Both are asserted: the count is what the engine's stalled-loop guard
    # watches, and the rows are what the next plan actually consults.
    assert offset["entries"] == 1
    assert consumed_relpaths(engine, "hourly_counts") == ["b1/part.parquet"]

    rows = sink_rows(engine.con)
    engine.run()
    assert engine.state.load_offset(engine.con, "hourly_counts") == offset
    assert sink_rows(engine.con) == rows
    engine.con.close()


def test_available_now_drains_what_limits_truncate_and_once_does_not(
    tmp_path, landing
):
    for index in range(3):
        drop_batch(landing, f"b{index}", 4, first=index * 4)

    once_dir = tmp_path / "once"
    once_dir.mkdir()
    once_engine = Engine(
        duckdb.connect(),
        catalog=once_dir / "catalog.ducklake",
        data_path=once_dir / "lake_data",
    )
    once_engine.add(
        counts_model(landing, limits=BatchLimits(max_files_per_trigger=1))
    )
    once_report = once_engine.run(trigger=Once())
    assert len(once_report.batches) == 1
    assert once_report.batches[0].has_more is True
    once_engine.con.close()

    drained = open_engine(tmp_path)
    drained.add(counts_model(landing, limits=BatchLimits(max_files_per_trigger=1)))
    drained_report = drained.run(trigger=AvailableNow())
    assert len(drained_report.batches) == 3
    assert sink_rows(drained.con) == recompute(drained.con, landing)
    drained.con.close()


def test_max_batches_stops_a_run_short_without_losing_the_rest(tmp_path, landing):
    for index in range(3):
        drop_batch(landing, f"b{index}", 3, first=index * 3)

    engine = open_engine(tmp_path)
    engine.add(counts_model(landing, limits=BatchLimits(max_files_per_trigger=1)))
    assert len(engine.run(trigger=AvailableNow(max_batches=2)).batches) == 2
    assert len(engine.run(trigger=AvailableNow()).batches) == 1
    assert sink_rows(engine.con) == recompute(engine.con, landing)
    engine.con.close()


# ---------------------------------------------------------------------------
# fault injection
# ---------------------------------------------------------------------------


def test_fault_hooks_fire_in_lifecycle_order(tmp_path, landing):
    drop_batch(landing, "b1", 3)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))

    seen: list[str] = []
    for point in FAULT_POINTS:
        engine.faults.install(point, lambda event: seen.append(event.point))
    assert engine.faults.installed() == list(FAULT_POINTS)

    engine.run()
    assert seen == list(FAULT_POINTS)
    engine.con.close()


def test_an_unknown_fault_point_is_refused_at_install_time(tmp_path):
    engine = open_engine(tmp_path)
    with pytest.raises(DuckstreamError, match="unknown fault point"):
        engine.faults.install("after_the_lord_mayors_show", lambda event: None)
    assert not engine.faults
    engine.con.close()


def test_a_crash_between_sink_write_and_commit_loses_nothing(tmp_path, landing, no_backoff):
    """The headline claim, in process. W4 repeats it with a real process kill.

    A hook that raises at ``after_sink_write`` is the same interception point
    the conformance suite's ``os._exit`` uses; the difference is only whether
    the process survives to be asked what it sees. Either way the transaction
    never commits, so the offset must not move and the sink must not change.

    Where the two part company is the *record*. A hard kill writes nothing at
    all -- it cannot. An exception the engine catches is recorded as a failed
    attempt, which costs one snapshot and is what makes the retry budget
    survive a restart. The consequence is worth stating: a process that dies
    hard never spends an attempt, so a crash-looping deployment cannot
    quarantine its own data. Only failures that fail *cleanly* count.
    """
    drop_batch(landing, "b1", 6)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    engine.run()

    committed_rows = sink_rows(engine.con)
    committed_offset = engine.state.load_offset(engine.con, "hourly_counts")
    snapshots = snapshot_count(engine.con)

    drop_batch(landing, "b2", 6, first=6)

    def boom(event):
        raise RuntimeError("killed between sink write and commit")

    engine.faults.install("after_sink_write", boom)
    with pytest.raises(BatchFailed, match="killed between") as caught:
        engine.run()

    # The batch is gone, and nothing it wrote survived it.
    assert sink_rows(engine.con) == committed_rows
    assert engine.state.load_offset(engine.con, "hourly_counts") == committed_offset

    # What did survive is the knowledge that it was tried.
    position = engine.state.load_position(engine.con, "hourly_counts")
    assert position.attempt == 1
    assert "killed between" in position.error
    assert position.offset == committed_offset, (
        "a failed attempt moved the position, so the batch would not replay"
    )
    assert snapshot_count(engine.con) == snapshots + 1, (
        "recording the attempt should cost exactly one snapshot"
    )

    # The report is still available through the exception, so a caller can see
    # what did succeed rather than only what did not.
    assert caught.value.report is not None
    assert caught.value.report.failures[0].attempt == 1

    # And the connection is usable: the rollback left no half-open transaction,
    # so the very next trigger replays the batch that never committed.
    engine.faults.clear()
    engine.run()
    assert sink_rows(engine.con) == recompute(engine.con, landing)
    assert engine.state.load_position(engine.con, "hourly_counts").attempt == 0, (
        "a success should clear the attempt counter"
    )
    engine.con.close()


def test_a_crash_before_commit_replays_in_a_new_process_view(tmp_path, landing, no_backoff):
    """Recovery is from the catalog, not from anything held in memory."""
    drop_batch(landing, "b1", 5)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    engine.faults.install("before_commit", lambda event: (_ for _ in ()).throw(
        RuntimeError("gone")
    ))
    with pytest.raises(BatchFailed):
        engine.run()
    engine.con.close()  # the crashed process's connection is gone

    restarted = open_engine(tmp_path)
    restarted.add(counts_model(landing))
    restarted.run()
    assert sink_rows(restarted.con) == recompute(restarted.con, landing)
    assert restarted.state.load_offset(restarted.con, "hourly_counts") is not None
    restarted.con.close()


def test_a_crash_after_commit_is_durable(tmp_path, landing):
    drop_batch(landing, "b1", 5)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    engine.faults.install("after_commit", lambda event: (_ for _ in ()).throw(
        RuntimeError("gone after commit")
    ))
    with pytest.raises(BatchFailed):
        engine.run()
    engine.con.close()

    restarted = open_engine(tmp_path)
    restarted.add(counts_model(landing))
    assert sink_rows(restarted.con) == recompute(restarted.con, landing)
    # Nothing left to do: the batch was durable before the crash. Note the
    # attempt *was* recorded even though the batch succeeded -- the hook fires
    # after COMMIT, so the data is safe and only the bookkeeping saw an error.
    # The next run finds nothing to read, which is the correct outcome either
    # way, and the counter clears on the next successful commit.
    assert restarted.run().batches == ()
    restarted.con.close()


# ---------------------------------------------------------------------------
# failure policy
# ---------------------------------------------------------------------------


def test_a_run_takes_the_catalog_lock_and_gives_it_back(tmp_path, landing):
    """The engine must actually take the lock, not merely own one.

    Worth its own test because nothing about the output would change if it
    stopped: the catalog's own file lock would still prevent corruption, and the
    only thing lost would be the message explaining what happened -- which is
    the entire reason the advisory lock exists.
    """
    drop_batch(landing, "b1", 3)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))

    seen = {}

    def observe(event):
        seen["held"] = os.path.exists(event.engine.lock.path)

    engine.faults.install("after_bind", observe)
    engine.run()

    assert seen.get("held"), "the run did not hold its lock while working"
    assert not os.path.exists(engine.lock.path), (
        "the lock outlived the run, so the next cron tick is refused"
    )
    engine.con.close()


def test_a_second_engine_on_one_catalog_is_refused_by_name(tmp_path, landing):
    """The failure an operator actually meets, with the message they need."""
    from duckstream.lock import LockError

    drop_batch(landing, "b1", 3)
    first = open_engine(tmp_path)
    first.add(counts_model(landing))
    first.lock.acquire()
    try:
        second = open_engine(tmp_path, connection=first.con)
        second.add(counts_model(landing))
        with pytest.raises(LockError, match="already holds this catalog"):
            second.run()
    finally:
        first.lock.release()
        first.con.close()


def test_a_failed_batch_is_held_back_before_it_is_retried(tmp_path, landing):
    """Backoff, and why it exists at all.

    Under cron it rarely bites -- consecutive attempts are already a tick apart.
    It exists for the drain loop and for anyone calling ``run()`` in a tight
    loop: without it a source that fails instantly would spend its entire
    attempt budget in a few hundred milliseconds and quarantine data that a
    two-second-old transient would have let through.
    """
    drop_batch(landing, "b1", 4)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing, max_attempts=10))
    engine.faults.install("before_commit", _explode)

    with pytest.raises(BatchFailed):
        engine.run()
    assert engine.state.load_position(engine.con, "hourly_counts").attempt == 1

    # Immediately again: held back, so the attempt count does not move and the
    # source is not even planned.
    report = engine.run()
    assert [r.outcome for r in report] == ["backoff"]
    assert engine.state.load_position(engine.con, "hourly_counts").attempt == 1, (
        "a backed-off pass should not spend an attempt"
    )
    engine.con.close()


def test_attempts_run_out_and_the_batch_is_quarantined(tmp_path, landing, no_backoff):
    """The default policy, end to end: skip past it, and record that you did.

    The point of quarantine is that the *next* batch gets through. A stream
    blocked on one bad batch stops collecting everything after it as well, so
    skipping loses strictly less than halting -- but only if the loss is
    recorded, which is what makes it a policy rather than a bug.
    """
    drop_batch(landing, "b1", 4)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing, max_attempts=3))
    engine.faults.install("before_commit", _explode)

    for expected in (1, 2):
        with pytest.raises(BatchFailed):
            engine.run()
        assert engine.state.load_position(engine.con, "hourly_counts").attempt == expected

    # The third attempt exhausts the budget: quarantined, and no longer an
    # exception, because this is the outcome the model asked for.
    report = engine.run()
    assert [r.outcome for r in report] == ["quarantined"]
    assert report.quarantined and not report.failures

    records = engine.state.quarantined(engine.con, "hourly_counts")
    assert len(records) == 1
    record = records[0]
    assert record["attempts"] == 3
    assert "no commit for you" in record["error"]
    assert record["skipped_from"] is None, "nothing had been committed before it"
    assert "b1" in record["payload_json"], (
        "the quarantine record must name what was skipped"
    )

    # The offset moved past the bad batch, the attempt counter cleared, and the
    # mart is still empty -- nothing was written from the batch that failed.
    #
    # "Past the batch" is the load-bearing half and is asserted against the
    # *files*, not merely against non-None: a quarantine that recorded the loss
    # and left the position where it was would log a permanent "data was lost"
    # row and then retry the same batch for ever -- the worst of both policies.
    #
    # With the set stored as rows, advancing the offset is no longer *by itself*
    # the skip: the count moving and the file being recorded are two writes, and
    # only the second one stops the next plan re-selecting the batch. Both are
    # asserted, and the row is the one that matters.
    position = engine.state.load_position(engine.con, "hourly_counts")
    assert position.attempt == 0 and position.offset is not None
    consumed = consumed_relpaths(engine, "hourly_counts")
    assert consumed == ["b1/part.parquet"], (
        f"the quarantined batch was not skipped past: {consumed}"
    )
    assert position.offset["entries"] == 1, (
        "the offset's count and the consumed rows disagree about the skip"
    )
    quarantined = [r for r in report if r.quarantined][0]
    assert quarantined.end_offset == position.offset, (
        "the result reports an offset the state store did not commit"
    )
    assert quarantined.end_offset != quarantined.start_offset, (
        "a quarantined batch reported a position that never moved"
    )
    assert not table_exists(engine.con, "marts.hourly_counts"), (
        "the quarantined batch left output behind, so its transaction did not "
        "roll back cleanly"
    )

    # And the stream is live again: new data lands normally.
    engine.faults.clear()
    drop_batch(landing, "b2", 3, first=100)
    engine.run()
    assert sink_rows(engine.con), "the stream did not recover after quarantine"
    engine.con.close()


def test_halt_never_advances_past_data_it_could_not_process(tmp_path, landing, no_backoff):
    """The other policy: a gap is worse than a stall, so never skip.

    And once the attempts are spent it stops *re-recording* the same verdict.
    A halted model is retried on every tick -- cheaply, so that fixing the
    underlying problem is all it takes to recover -- but a model that appended a
    row and a DuckLake snapshot every minute for as long as nobody looked at it
    would be growing the catalog fastest exactly when that helps least.
    """
    drop_batch(landing, "b1", 4)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing, on_failure="halt", max_attempts=2))
    engine.faults.install("before_commit", _explode)

    for _ in range(2):
        with pytest.raises(BatchFailed):
            engine.run()

    settled = engine.state.load_position(engine.con, "hourly_counts")
    assert settled.attempt == 2
    assert settled.offset is None, "halt must never advance past the bad batch"
    snapshots = snapshot_count(engine.con)

    for _ in range(3):
        with pytest.raises(BatchFailed) as caught:
            engine.run()
        assert caught.value.report.results[-1].outcome == "halted"

    assert engine.state.quarantined(engine.con) == [], "halt must not quarantine"
    assert snapshot_count(engine.con) == snapshots, (
        "a halted model kept writing, so a stuck pipeline grows the catalog"
    )

    # Fix the underlying problem and it recovers on its own.
    engine.faults.clear()
    engine.run()
    assert sink_rows(engine.con) == recompute(engine.con, landing)
    engine.con.close()


def test_one_model_failing_does_not_stop_the_others(tmp_path, landing, no_backoff):
    """Every model gets its turn before the run reports a failure.

    Raising where the failure happened would mean a corrupt file in the first
    model silently costing the second one its trigger -- and under cron, every
    trigger after that too.
    """
    drop_batch(landing, "b1", 4)
    engine = open_engine(tmp_path)
    broken = counts_model(landing, name="broken", sink=TableSink("marts.broken"))
    healthy = counts_model(landing, name="healthy", sink=TableSink("marts.healthy"))
    engine.add(broken)
    engine.add(healthy)

    def only_broken(event):
        if event.ctx.model_name == "broken":
            raise RuntimeError("this model is broken")

    engine.faults.install("before_commit", only_broken)
    with pytest.raises(BatchFailed) as caught:
        engine.run()

    report = caught.value.report
    assert {r.model for r in report.failures} == {"broken"}
    assert any(r.model == "healthy" and r.committed for r in report), (
        "the healthy model never ran, so one failure stopped an unrelated model"
    )
    assert engine.con.execute("SELECT count(*) FROM marts.healthy").fetchone()[0] > 0
    engine.con.close()


# ---------------------------------------------------------------------------
# loop safety
# ---------------------------------------------------------------------------


class _StalledSource:
    """A source whose plan never advances — the shape that would spin forever."""

    type_name = "stalled"

    def __init__(self, inner):
        self.inner = inner

    def latest_offset(self):
        return self.inner.latest_offset()

    def plan(self, start, end, limits):
        plan = self.inner.plan(start, end, limits)
        if plan.is_empty:
            return plan
        return BatchPlan(
            start=plan.start,
            end=plan.start if plan.start is not None else FileOffset.empty(),
            payload=plan.payload,
            is_empty=False,
            has_more=True,
        )

    def bind(self, con, plan):
        return self.inner.bind(con, plan)

    def to_config(self):
        return self.inner.to_config()


def test_a_source_whose_offset_does_not_advance_fails_loudly(tmp_path, landing):
    drop_batch(landing, "b1", 3)
    engine = open_engine(tmp_path)
    engine.add(
        counts_model(
            landing, source=_StalledSource(FileSource(Path(landing).as_posix()))
        )
    )
    with pytest.raises(DuckstreamError, match="did not advance"):
        engine.run()
    engine.con.close()


# ---------------------------------------------------------------------------
# phase-1 scope
# ---------------------------------------------------------------------------


def test_an_unmergeable_update_model_is_recomputed_rather_than_folded(tmp_path, landing):
    """A median cannot be merged at all, so the engine re-derives its windows.

    This test used to assert the *refusal*, because tier three did not execute.
    It now asserts the answer, and it is deliberately the same model: what
    changed is that "no merge can express this" stopped meaning "duckstream
    cannot do this". The refusal it replaced still exists one level down --
    ``TableSink.merge_sql`` will not build a fold for this model, and a unit
    test pins that -- so nothing was traded away for the capability.

    Two batches, because a median folded batch-by-batch would look right on the
    first one. 4 rows then 4 more, all inside one hour: fold them and the second
    batch's median overwrites the first's. Recompute and the answer is the
    median of all eight, which is what a full recompute from source gives.
    """
    drop_batch(landing, "b1", 4)
    engine = open_engine(tmp_path)
    engine.add(
        counts_model(
            landing,
            name="mid_value",
            aggregates={"mid_value": "median(value)"},
            strategy="recompute_window",
            memory_profile="materialising",
            sink=TableSink("marts.mid_value", mode="update"),
        )
    )
    engine.run()
    drop_batch(landing, "b2", 4, first=4)
    engine.run()

    got = engine.con.execute(
        "SELECT window_ts, sensor_id, mid_value FROM marts.mid_value "
        "ORDER BY window_ts, sensor_id"
    ).fetchall()
    truth = engine.con.execute(
        "SELECT date_trunc('hour', event_ts) AS window_ts, sensor_id, "
        "       median(value) AS mid_value "
        f"FROM read_parquet('{(landing / '**' / '*.parquet').as_posix()}') "
        "GROUP BY 1, 2 ORDER BY 1, 2"
    ).fetchall()
    assert got == truth
    assert engine.state.load_offset(engine.con, "mid_value") is not None
    engine.con.close()


def test_a_sufficient_statistics_model_runs_end_to_end(tmp_path, landing):
    """The tier the engine used to refuse, through the whole batch lifecycle.

    ``drop_batch`` lays values 0..n-1, so the expected answers come from the
    data rather than from a constant -- and they are checked against DuckDB's
    own aggregate over the same files, which is the only comparison that means
    anything here.
    """
    drop_batch(landing, "b1", 40)
    drop_batch(landing, "b2", 40, first=40)
    engine = open_engine(tmp_path)
    engine.add(
        counts_model(
            landing,
            name="stats",
            sink=TableSink("marts.stats"),
            aggregates={
                "n": "count(*)",
                "mean_value": "avg(value)",
                "sd_value": "stddev_samp(value)",
            },
        )
    )
    engine.run()

    got = engine.con.execute(
        "SELECT n, mean_value, sd_value FROM marts.stats ORDER BY window_ts, sensor_id"
    ).fetchall()
    truth = engine.con.execute(
        f"SELECT count(*), avg(value), stddev_samp(value) "
        f"FROM read_parquet({[p.as_posix() for p in landing.rglob('*.parquet')]!r}) "
        f"GROUP BY date_trunc('hour', event_ts), sensor_id "
        f"ORDER BY date_trunc('hour', event_ts), sensor_id"
    ).fetchall()
    assert len(got) == len(truth) and got, (got, truth)
    for (gn, gm, gs), (tn, tm, ts) in zip(got, truth):
        assert gn == tn
        assert gm == pytest.approx(tm, rel=1e-12)
        assert (gs is None and ts is None) or gs == pytest.approx(ts, rel=1e-12)
    engine.con.close()


# ---------------------------------------------------------------------------
# engine surface
# ---------------------------------------------------------------------------


def test_add_validates_and_refuses_a_duplicate_name(tmp_path, landing):
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    with pytest.raises(DuckstreamError, match="already registered"):
        engine.add(counts_model(landing))
    with pytest.raises(DuckstreamError, match="Model"):
        engine.add(object())
    engine.con.close()


def test_running_an_unknown_model_names_the_registered_ones(tmp_path, landing):
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    with pytest.raises(DuckstreamError, match="hourly_counts"):
        engine.run(model="nope")
    engine.con.close()


def test_run_with_no_models_refuses(tmp_path):
    engine = open_engine(tmp_path)
    with pytest.raises(DuckstreamError, match="no models to run"):
        engine.run()
    engine.con.close()


def test_run_selects_a_single_model(tmp_path, landing):
    drop_batch(landing, "b1", 4)
    other = tmp_path / "other"
    other.mkdir()
    drop_batch(other, "c1", 4)

    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    engine.add(
        counts_model(other, name="other_counts", sink=TableSink("marts.other"))
    )
    report = engine.run(model="hourly_counts")
    assert report.model_names == ["hourly_counts"]
    assert engine.state.load_offset(engine.con, "other_counts") is None
    engine.con.close()


def test_temp_views_do_not_accumulate(tmp_path, landing):
    for index in range(3):
        drop_batch(landing, f"b{index}", 2, first=index * 2)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing, limits=BatchLimits(max_files_per_trigger=1)))
    engine.run()
    leftover = engine.con.execute(
        "SELECT count(*) FROM duckdb_views() "
        "WHERE view_name LIKE 'duckstream_file_batch_%'"
    ).fetchone()[0]
    assert leftover == 0
    engine.con.close()


# ---------------------------------------------------------------------------
# the config front door
# ---------------------------------------------------------------------------


def test_from_config_produces_an_equivalent_engine(tmp_path, landing):
    """Front-door parity: same models, same output, from either door."""
    drop_batch(landing, "b1", 8)
    drop_batch(landing, "b2", 7, first=8)

    python_dir = tmp_path / "python"
    python_dir.mkdir()
    python_engine = open_engine(python_dir)
    python_engine.add(counts_model(landing))
    python_engine.run()
    expected = sink_rows(python_engine.con)
    python_engine.con.close()

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    engine = Engine.from_config(write_config(config_dir, landing))
    try:
        assert [m.name for m in engine.models] == ["hourly_counts"]
        assert engine.models[0] == counts_model(landing)
        engine.run()
        assert sink_rows(engine.con) == expected == recompute(engine.con, landing)
        # An ordinary Engine: Python can keep modifying it.
        engine.add(counts_model(landing, name="second", sink=TableSink("marts.two")))
        assert [m.name for m in engine.models] == ["hourly_counts", "second"]
    finally:
        engine.close()


def test_from_config_closes_only_the_connection_it_opened(tmp_path, landing):
    con = duckdb.connect()
    engine = Engine.from_config(write_config(tmp_path, landing), con=con)
    engine.close()
    assert con.execute("SELECT 1").fetchone() == (1,)
    con.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def run_cli(*argv) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(list(argv), out=out, err=err)
    return code, out.getvalue(), err.getvalue()


def test_validate_exits_zero_on_a_good_config(tmp_path, landing):
    code, out, err = run_cli("validate", "--config", str(write_config(tmp_path, landing)))
    assert code == 0
    assert "ok, 1 model" in out
    assert err == ""


def test_validate_exits_non_zero_on_a_bad_config(tmp_path, landing):
    """``PLAN.md``: "validate is honest". Deploy scripts depend on this."""
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        textwrap.dedent(
            f"""\
            catalog: "ducklake:{(tmp_path / 'catalog.ducklake').as_posix()}"
            models:
              - name: broken
                source: {{type: file, path: "{landing.as_posix()}"}}
                time_column: event_ts
                grain: hour
                key: [window_ts, sensor_id]
                strategy: delta_merge
                aggregates:
                  spread: "median(value)"
                sink: {{type: table, table: marts.broken, mode: update}}
            """
        ),
        encoding="utf-8",
    )
    code, out, err = run_cli("validate", "--config", str(bad))
    assert code == 1
    assert out == ""
    assert "duckstream:" in err
    assert "delta_merge" in err


def test_validate_reports_a_missing_file_without_a_traceback(tmp_path):
    code, _out, err = run_cli("validate", "--config", str(tmp_path / "absent.yaml"))
    assert code == 1
    assert "could not read" in err


def test_validate_prints_several_problems_one_per_line(tmp_path, landing):
    bad = tmp_path / "many.yaml"
    bad.write_text(
        textwrap.dedent(
            f"""\
            catalog: "ducklake:{(tmp_path / 'catalog.ducklake').as_posix()}"
            models:
              - name: one
                source: {{type: file, path: "{landing.as_posix()}"}}
                key: [sensor_id]
                strategy: delta_merge
                aggregates: {{m: "median(value)"}}
                sink: {{type: table, table: marts.one}}
              - name: two
                source: {{type: file, path: "{landing.as_posix()}"}}
                key: [sensor_id]
                aggregates: {{}}
                sink: {{type: table, table: marts.two}}
            """
        ),
        encoding="utf-8",
    )
    code, _out, err = run_cli("validate", "--config", str(bad))
    assert code == 1
    lines = [line for line in err.splitlines() if line.startswith("  - ")]
    assert len(lines) == 2


def test_models_prints_tier_and_strategy(tmp_path, landing):
    code, out, err = run_cli("models", "--config", str(write_config(tmp_path, landing)))
    assert code == 0, err
    assert "MODEL" in out and "TIER" in out and "STRATEGY" in out
    body = out.splitlines()[1]
    assert "hourly_counts" in body
    assert "additive" in body
    assert "delta_merge" in body
    assert "table(marts.hourly_counts, update)" in body


def test_run_executes_a_batch(tmp_path, landing):
    drop_batch(landing, "b1", 6)
    config = write_config(tmp_path, landing)
    code, out, err = run_cli("run", "--config", str(config))
    assert code == 0, err
    assert "hourly_counts: 1 batch, 6 source rows" in out

    con = duckdb.connect()
    try:
        engine = Engine(
            con,
            catalog=tmp_path / "catalog.ducklake",
            data_path=tmp_path / "lake_data",
        )
        assert sink_rows(con) == recompute(con, landing)
        assert engine.state.load_offset(con, "hourly_counts") is not None
    finally:
        con.close()

    code, out, _err = run_cli("run", "--config", str(config))
    assert code == 0
    assert "nothing to do" in out


def test_run_accepts_a_model_filter_and_reports_an_unknown_one(tmp_path, landing):
    drop_batch(landing, "b1", 3)
    config = write_config(tmp_path, landing)
    code, out, _err = run_cli(
        "run", "--config", str(config), "--model", "hourly_counts", "--once"
    )
    assert code == 0
    assert "hourly_counts" in out

    code, _out, err = run_cli("run", "--config", str(config), "--model", "ghost")
    assert code == 1
    assert "ghost" in err


def test_module_entry_point_runs(tmp_path, landing):
    """``python -m duckstream`` must work: cron in a venv calls the interpreter."""
    drop_batch(landing, "b1", 4)
    config = write_config(tmp_path, landing)
    env = dict(os.environ, PYTHONPATH=str(REPO_ROOT), PYTHONIOENCODING="utf-8")
    completed = subprocess.run(
        [sys.executable, "-m", "duckstream", "run", "--config", str(config)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        cwd=str(REPO_ROOT),
    )
    assert completed.returncode == 0, completed.stderr
    assert "hourly_counts" in completed.stdout


def test_the_cli_rejects_an_unknown_command():
    with pytest.raises(SystemExit) as caught:
        cli.main(["frobnicate", "--config", "x.yaml"])
    assert caught.value.code == 2


# ---------------------------------------------------------------------------
# package exports
# ---------------------------------------------------------------------------


def test_the_documented_import_line_works():
    """``PLAN.md``'s worked example imports exactly these names."""
    from duckstream import (  # noqa: F401
        AvailableNow,
        Engine,
        FileSource,
        Model,
        TableSink,
    )
    import duckstream

    for name in (
        "Once",
        "load_config",
        "ConfigDocument",
        "register_source",
        "register_sink",
        "register_udf",
        "BatchLimits",
        "DuckstreamError",
        "ModelValidationError",
        "ConfigError",
    ):
        assert hasattr(duckstream, name), name
    with pytest.raises(AttributeError):
        duckstream.definitely_not_exported


def test_importing_duckstream_stays_cheap():
    """The registry is lazy on purpose; re-exporting must not undo that.

    ``import duckstream`` alone must not drag in yaml, duckdb, pyarrow or numpy.
    ``CONTEXT.md`` 1.8 measured ~235 ms of process start per cron tick, which is
    already the largest term in an idle trigger.
    """
    code = (
        "import sys; import duckstream; "
        "print([m for m in ('yaml','duckdb','pyarrow','numpy') if m in sys.modules])"
    )
    completed = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=dict(os.environ, PYTHONPATH=str(REPO_ROOT)),
        cwd=str(REPO_ROOT),
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "[]"


def test_the_console_script_entry_point_resolves():
    """``duckstream = duckstream.cli:main`` must name something callable.

    The package is not pip-installed in this checkout, so the script itself
    cannot be executed here — but the declaration is checked against the real
    module so a rename cannot silently break the installed entry point while
    ``python -m duckstream`` keeps working.
    """
    import importlib
    import tomllib

    with open(REPO_ROOT / "pyproject.toml", "rb") as handle:
        target = tomllib.load(handle)["project"]["scripts"]["duckstream"]
    module_name, _, attribute = target.partition(":")
    assert callable(getattr(importlib.import_module(module_name), attribute))


# ---------------------------------------------------------------------------
# UDFs
# ---------------------------------------------------------------------------


def write_udf_module(tmp_path, monkeypatch) -> str:
    """A tiny importable module holding a registrar and a bare function."""
    module = tmp_path / "ds_test_udfs.py"
    module.write_text(
        textwrap.dedent(
            """\
            def register_double(con):
                con.create_function(
                    "ds_double", lambda x: x * 2.0, ["DOUBLE"], "DOUBLE"
                )


            def bare_computation(value):
                return value * 2.0
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    return "ds_test_udfs"


def test_udfs_are_registered_before_planning(tmp_path, landing, monkeypatch):
    """An aggregate may call a UDF, so the registrar runs before the first plan.

    ``append`` mode, because a UDF makes the model non-foldable and phase 1's
    update merge refuses anything but the additive tier.
    """
    module = write_udf_module(tmp_path, monkeypatch)
    drop_batch(landing, "b1", 5)

    engine = open_engine(tmp_path)
    engine.add(
        counts_model(
            landing,
            name="doubled",
            udfs=[f"{module}:register_double"],
            memory_profile="streaming",
            aggregates={"total": "sum(ds_double(value))"},
            sink=TableSink("marts.doubled", mode="append"),
            # Unwindowed append: a per-batch row, no fold, any tier. Windowed
            # append is a different mode of operation entirely -- it folds into
            # an open-window accumulator and needs a lateness horizon and the
            # additive tier, neither of which a UDF aggregate can offer.
            grain=None,
            key=["sensor_id"],
        )
    )
    engine.run()
    total = engine.con.execute("SELECT sum(total) FROM marts.doubled").fetchone()[0]
    assert total == 2.0 * sum(range(5))
    engine.con.close()


def test_a_udf_that_is_not_a_registrar_is_refused_with_the_contract(
    tmp_path, landing, monkeypatch
):
    module = write_udf_module(tmp_path, monkeypatch)
    drop_batch(landing, "b1", 3)

    engine = open_engine(tmp_path)
    engine.add(
        counts_model(
            landing,
            name="doubled",
            udfs=[f"{module}:bare_computation"],
            memory_profile="streaming",
            aggregates={"total": "sum(ds_double(value))"},
            sink=TableSink("marts.doubled", mode="append"),
            # Unwindowed append: a per-batch row, no fold, any tier. Windowed
            # append is a different mode of operation entirely -- it folds into
            # an open-window accumulator and needs a lateness horizon and the
            # additive tier, neither of which a UDF aggregate can offer.
            grain=None,
            key=["sensor_id"],
        )
    )
    with pytest.raises(DuckstreamError) as caught:
        engine.run()
    message = str(caught.value)
    assert "register(con)" in message
    assert "doubled" in message
    engine.con.close()


# ==========================================================================
# Event time in the batch lifecycle
# ==========================================================================


def horizon_model(landing, **overrides):
    """The phase-1 model plus a lateness horizon, so the engine reads event time."""
    settings = dict(lateness="10 minutes")
    settings.update(overrides)
    return counts_model(landing, **settings)


def test_a_model_with_no_horizon_reads_and_writes_no_watermark(tmp_path, landing):
    """The phase-1 path stays free: nothing about event time runs at all.

    Asserted on the state store rather than on timings, because "costs nothing"
    is only credible if the work genuinely does not happen. A model without a
    horizon must leave the watermarks table empty -- an engine that wrote a NULL
    row per trigger would look correct and quietly pay ~5 ms a trigger for it
    (CONTEXT.md 1.10).
    """
    drop_batch(landing, "b1", 5)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    report = engine.run()

    assert all(r.watermark is None for r in report)
    assert all(r.rows_late is None and r.rows_undated is None for r in report)
    assert engine.con.execute(
        "SELECT count(*) FROM duckstream.watermarks"
    ).fetchone() == (0,)
    engine.con.close()


def test_a_horizon_commits_a_watermark_with_every_batch(tmp_path, landing):
    drop_batch(landing, "b1", 5)
    engine = open_engine(tmp_path)
    engine.add(horizon_model(landing))
    report = engine.run()

    committed = [r for r in report if r.committed]
    assert len(committed) == 1
    # drop_batch lays rows a minute apart from 10:00, so five rows top out at
    # 10:04 and the horizon puts the watermark at 09:54.
    assert committed[0].watermark == dt.datetime(2026, 8, 22, 9, 54)
    assert committed[0].rows_late == 0
    assert committed[0].rows_undated == 0
    assert engine.con.execute(
        "SELECT watermark FROM duckstream.watermarks ORDER BY batch_id DESC LIMIT 1"
    ).fetchone() == (dt.datetime(2026, 8, 22, 9, 54),)
    engine.con.close()


def test_a_rolled_back_batch_does_not_advance_the_memoised_watermark(
    tmp_path, landing, no_backoff
):
    """The in-process shadow of the fault-injection test, and it earns its place.

    The engine keeps the committed watermark in memory rather than re-reading it
    every trigger -- measured at 10.4 ms a trigger, a third of everything a
    horizon costs, and the same optimisation CONTEXT.md 1.10 already made for
    the batch id. The rule that makes it sound is that the cache is written only
    *after* a successful commit.

    A separate process cannot check that: a killed process starts with an empty
    cache, so it would pass whether the rule held or not. This is the test that
    actually pins it. If the cache were updated before the commit, the second
    run would judge the replayed batch against a horizon it never durably had.
    """
    drop_batch(landing, "b1", 5)
    engine = open_engine(tmp_path)
    engine.add(horizon_model(landing))
    engine.run()
    settled = engine.con.execute(
        "SELECT watermark FROM duckstream.watermarks ORDER BY batch_id DESC LIMIT 1"
    ).fetchone()[0]

    # A much later batch, whose watermark would be unmistakable if it leaked.
    drop_batch(landing, "b2", 5, first=600)
    engine.faults.install("before_commit", _explode)
    with pytest.raises(BatchFailed, match="no commit for you"):
        engine.run()
    engine.faults.clear()

    assert engine.con.execute(
        "SELECT watermark FROM duckstream.watermarks ORDER BY batch_id DESC LIMIT 1"
    ).fetchone()[0] == settled
    assert engine._watermarks[engine.models[0].name] == settled, (
        "the in-memory watermark advanced on a batch that never committed"
    )

    report = engine.run()
    committed = [r for r in report if r.committed]
    assert committed and committed[-1].rows_late == 0, (
        "the replayed batch was judged against a horizon that was never "
        "committed, so its rows were dropped as late"
    )
    engine.con.close()


def _explode(event) -> None:
    raise RuntimeError("no commit for you")


def test_late_rows_are_dropped_counted_and_recorded(tmp_path, landing):
    """One batch establishes the horizon, the next arrives behind it."""
    drop_batch(landing, "b1", 5, first=600)  # 20:00 onwards
    engine = open_engine(tmp_path)
    engine.add(horizon_model(landing))
    engine.run()

    drop_batch(landing, "b2", 3)  # back at 10:00, long sealed
    report = engine.run()
    committed = [r for r in report if r.committed]
    assert len(committed) == 1
    assert committed[0].rows_late == 3
    assert committed[0].rows_in == 3
    assert committed[0].rows_dropped == 3

    history = engine.con.execute(
        "SELECT rows_in, rows_late, rows_undated FROM duckstream.batches "
        "ORDER BY batch_id"
    ).fetchall()
    assert history == [(5, 0, 0), (3, 3, 0)]
    assert report.rows_late == 3
    assert report.rows_dropped == 3
    engine.con.close()


def test_the_filter_view_is_dropped_with_the_batch_view(tmp_path, landing):
    """Two temp views a batch, and neither may outlive it.

    One view per batch accumulating for the life of the connection is a slow
    leak that no assertion about output would ever notice.
    """
    drop_batch(landing, "b1", 5, first=600)
    engine = open_engine(tmp_path)
    engine.add(horizon_model(landing))
    engine.run()
    drop_batch(landing, "b2", 3)
    engine.run()

    views = engine.con.execute(
        "SELECT count(*) FROM duckdb_views() WHERE view_name LIKE 'duckstream%'"
    ).fetchone()[0]
    assert views == 0
    engine.con.close()


def _downgrade_to_v1(engine, model_name: str) -> dict:
    """Rewrite this model's catalog state into the shape duckstream v1 wrote.

    There is no other way to test the migration honestly. Every catalog this
    suite creates is written by the current code, so the migration path is a
    no-op in all of them -- which is exactly how a migration ships broken. So
    the position is put back the way an upgrading deployment will actually find
    it: the consumed set inside the offset, and no rows behind it.
    """
    relpaths = consumed_relpaths(engine, model_name)
    entries = {}
    for rel in relpaths:
        stat = os.stat(engine.model(model_name).source._absolute(rel))
        entries[rel] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
    store = engine.state
    engine.con.execute("BEGIN")
    engine.con.execute(
        f"DELETE FROM {store.consumed_files_table} WHERE model_name = ?",
        [model_name],
    )
    engine.con.execute(
        f"UPDATE {store.offsets_table} SET offset_json = ? WHERE model_name = ?",
        [encode_offset(FileOffset.build(entries)), model_name],
    )
    engine.con.execute("COMMIT")
    # The engine memoises; a fresh process is what an upgrade actually is.
    engine._next_ids.clear()
    engine.state._last_batch_id.clear()
    return entries


def test_a_v1_catalog_migrates_instead_of_replaying(tmp_path, landing):
    """The upgrade path, end to end, through the engine rather than by hand.

    An existing deployment has a consumed map inside its offset. On the first
    run of the new code that map has to move into rows -- once, atomically, and
    without re-reading a single file. Getting it wrong in the obvious direction
    replays the whole landing tree and folds every row into the mart a second
    time, which is the section 4 bug class arriving as an upgrade note.
    """
    drop_batch(landing, "b1", 7)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    engine.run()

    before = sink_rows(engine.con)
    entries = _downgrade_to_v1(engine, "hourly_counts")
    assert entries, "the downgrade must actually have something to migrate"
    assert consumed_relpaths(engine, "hourly_counts") == [], "rows were cleared"

    # Nothing new has landed, so this run has no batch to run -- and must still
    # migrate, because the migration is about the position, not about a batch.
    report = engine.run()

    assert report.adopted == (("hourly_counts", len(entries)),), (
        f"the run did not report a migration: {report.adopted}"
    )
    assert consumed_relpaths(engine, "hourly_counts") == sorted(entries), (
        "the consumed map did not reach the table"
    )
    position = engine.state.load_position(engine.con, "hourly_counts")
    assert position.offset == FileOffset.rows(len(entries))
    assert sink_rows(engine.con) == before, (
        "the migration re-read the landing tree and folded it a second time"
    )

    # And it happens once. A second run migrates nothing and changes nothing.
    again = engine.run()
    assert again.adopted == ()
    assert consumed_relpaths(engine, "hourly_counts") == sorted(entries)
    assert sink_rows(engine.con) == before
    engine.con.close()


def test_a_v1_catalog_that_still_has_work_migrates_and_then_does_it(tmp_path, landing):
    """The same upgrade, on a deployment with a backlog waiting.

    The migration and the batch are separate transactions, and this pins that
    the first does not swallow the second: the new drop is still read, exactly
    once, on the same run.
    """
    drop_batch(landing, "b1", 3)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    engine.run()
    entries = _downgrade_to_v1(engine, "hourly_counts")

    drop_batch(landing, "b2", 5, first=100)
    report = engine.run()

    assert report.adopted == (("hourly_counts", len(entries)),)
    assert sorted(consumed_relpaths(engine, "hourly_counts")) == [
        "b1/part.parquet",
        "b2/part.parquet",
    ]
    total = engine.con.execute(
        "SELECT sum(n) FROM marts.hourly_counts"
    ).fetchone()[0]
    assert total == 8, f"expected 3 + 5 rows folded exactly once, got {total}"
    engine.con.close()


def test_a_v1_catalog_keeps_its_failure_state_across_the_migration(tmp_path, landing):
    """Relocating the position must not overturn a failure decision.

    An upgrade that hands a stuck model a clean attempt budget would let a
    crash-looping deployment postpone its own quarantine indefinitely, once per
    release. The unit-level version of this is in tests/unit/test_consumed.py;
    this is the engine actually doing it.
    """
    drop_batch(landing, "b1", 2)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    engine.run()
    _downgrade_to_v1(engine, "hourly_counts")

    store = engine.state
    position = store.load_position(engine.con, "hourly_counts")
    store.record_failure(
        engine.con,
        "hourly_counts",
        store.next_batch_id(engine.con, "hourly_counts"),
        position,
        RuntimeError("something upstream"),
    )
    failing = store.load_position(engine.con, "hourly_counts")
    assert failing.attempt == 1

    engine.run()

    after = store.load_position(engine.con, "hourly_counts")
    assert after.offset == FileOffset.rows(1), "it did migrate"
    assert after.attempt == 1, "and did not refund the attempt"
    assert after.error == failing.error
    engine.con.close()


# ---------------------------------------------------------------------------
# Tier three: the recompute path
#
# `test_an_unmergeable_update_model_is_recomputed_rather_than_folded` above
# covers the answer. These cover the machinery that gets there, and each one is
# a way of being wrong that still produces a number.
# ---------------------------------------------------------------------------


def median_model(landing: Path, **overrides) -> Model:
    settings = dict(
        name="mid_value",
        aggregates={"mid_value": "median(value)", "n": "count(*)"},
        # Keyed on the window alone, so one hour is one row and a test can talk
        # about "the window" without picking a sensor out of three.
        key=["window_ts"],
        strategy="recompute_window",
        memory_profile="materialising",
        sink=TableSink("marts.mid_value", mode="update"),
    )
    settings.update(overrides)
    return counts_model(landing, **settings)


def test_a_recompute_reads_files_from_earlier_batches_not_just_its_own(tmp_path, landing):
    """The property the whole file index exists to serve.

    Batch two lands in the *same hour* as batch one. If the recompute read only
    its own files, the mart would hold the median of batch two alone -- which is
    ``CONTEXT.md`` section 4's FFT mart, and it does not fail, it just disagrees
    with a full recompute.

    ``drop_batch`` puts row *i* at 10:00 + i minutes with value *i*, so both
    batches have to stay under 60 to share the hour. Batch one is values 0..3
    and batch two is 50..53: the median of all eight is **26.5**, and the median
    of the second batch alone is **51.5**. Neither can be mistaken for the
    other, which is what makes the assertion mean something.
    """
    drop_batch(landing, "b1", 4)
    engine = open_engine(tmp_path)
    engine.add(median_model(landing))
    engine.run()
    assert engine.con.execute(
        "SELECT n FROM marts.mid_value"
    ).fetchone()[0] == 4

    drop_batch(landing, "b2", 4, first=50)
    engine.run()

    rows = engine.con.execute(
        "SELECT window_ts, n, mid_value FROM marts.mid_value ORDER BY window_ts"
    ).fetchall()
    assert len(rows) == 1, f"both batches are in one hour, got {rows}"
    _window, n, mid = rows[0]
    assert n == 8, f"the window holds {n} rows; a batch-only recompute gives 4"
    assert mid == pytest.approx(26.5), (
        f"median came out {mid}; reading only the second batch gives 51.5"
    )
    engine.con.close()


def test_the_index_records_bounds_only_for_a_model_that_recomputes(tmp_path, landing):
    """The scan costs 1.4-6.7 ms a batch (``CONTEXT.md`` 1.18) and only tier
    three reads it, so a tier-one model must not be paying for it.

    A model that is promoted to tier three later therefore has unmeasured rows
    behind it -- which is safe, not merely tolerable, because the sentinel range
    means those files are read by *every* recompute rather than by none.
    """
    drop_batch(landing, "b1", 4)

    folding = open_engine(tmp_path)
    folding.add(counts_model(landing))
    folding.run()
    stored = folding.con.execute(
        f"SELECT DISTINCT min_ts, max_ts, n_rows "
        f"FROM {folding.state.consumed_files_table} WHERE model_name = ?",
        ["hourly_counts"],
    ).fetchall()
    assert stored == [(dt.datetime.min, dt.datetime.max, None)], (
        "an additive model paid for a hint nothing will read"
    )
    folding.con.close()

    recomputing = open_engine(tmp_path)
    recomputing.add(median_model(landing))
    recomputing.run()
    measured = recomputing.con.execute(
        f"SELECT min_ts, max_ts, n_rows "
        f"FROM {recomputing.state.consumed_files_table} WHERE model_name = ?",
        ["mid_value"],
    ).fetchall()
    assert len(measured) == 1
    low, high, rows = measured[0]
    assert rows == 4
    assert low == dt.datetime(2026, 8, 22, 10, 0)
    assert high == dt.datetime(2026, 8, 22, 10, 3)
    recomputing.con.close()


def temp_views(con) -> list[str]:
    """duckstream's own temp views. Not every temp view -- DuckDB's information
    schema is a few dozen of them, and counting those would swamp the signal.
    """
    return sorted(
        row[0]
        for row in con.execute(
            "SELECT view_name FROM duckdb_views() "
            "WHERE temporary AND view_name LIKE 'duckstream%'"
        ).fetchall()
    )


def test_a_recompute_leaves_no_temp_views_behind(tmp_path, landing):
    """Two views per chunk, and none of them may outlive the batch.

    A recompute binds a view over the files a chunk selected and a second one
    narrowing it to the range, so a model with several chunks creates several
    pairs per trigger. On a long-running connection an undropped pair per chunk
    per trigger accumulates for the life of the process.
    """
    drop_batch(landing, "b1", 4)
    drop_batch(landing, "b2", 4, first=50)
    engine = open_engine(tmp_path)
    before = temp_views(engine.con)
    engine.add(median_model(landing, limits=BatchLimits(max_rows_per_trigger=2)))
    engine.run()
    assert engine.con.execute("SELECT count(*) FROM marts.mid_value").fetchone()[0]
    assert temp_views(engine.con) == before
    engine.con.close()


def land_two_hours(landing: Path, name: str) -> None:
    """One file spanning two hours, so one batch touches two windows.

    A file is never split across batches, so this arrives whole however tight
    ``max_rows_per_trigger`` is -- which is what lets the recompute be chunked
    while the batch is not.
    """
    directory = landing / name
    directory.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(
            "COPY (SELECT TIMESTAMP '2026-08-22 10:00:00' + INTERVAL (i * 40) MINUTE "
            "        AS event_ts, 'sensor0' AS sensor_id, i::DOUBLE AS value "
            "FROM range(0, 4) t(i)) "
            f"TO '{(directory / 'part.parquet').as_posix()}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    (directory / "_READY").write_text("", encoding="utf-8")


def test_a_failing_chunk_still_drops_the_views_made_before_it(tmp_path, landing):
    """The half that only fails on the unhappy path, so it needs its own test.

    Registering the view names on the way *out* of the recompute looks
    equivalent and is not: a chunk that raises would strand every view its
    predecessors created, and a model that keeps failing would strand another
    set on every retry.

    One file, two hours, a chunk budget of one row: one batch, several chunks,
    and the second one raises.
    """
    land_two_hours(landing, "wide")
    engine = open_engine(tmp_path)
    model = median_model(landing, limits=BatchLimits(max_rows_per_trigger=1))

    calls = {"n": 0}
    real_write = model.sink.write

    def explode(con, batch_view, m, ctx):
        calls["n"] += 1
        if calls["n"] >= 2:
            raise DuckstreamError("the second chunk fails")
        return real_write(con, batch_view, m, ctx)

    model.sink.write = explode
    engine.add(model)
    before = temp_views(engine.con)
    with pytest.raises(DuckstreamError):
        engine.run()
    assert calls["n"] >= 2, "the test never reached a second chunk"
    assert temp_views(engine.con) == before, "a failed chunk stranded its views"
    engine.con.close()


def test_a_source_that_cannot_resolve_its_paths_is_refused_not_guessed(tmp_path, landing):
    """A guessed path is worse than an error: it may open and hold wrong rows.

    Consumed-file paths are stored relative to the source's own root, so only
    the source can turn them back. A source keeping a consumed set but unable to
    resolve it cannot be recomputed, and the engine has to say so.
    """
    drop_batch(landing, "b1", 4)

    class Unresolvable(FileSource):
        """A file source with the resolution hook taken away."""

        absolute_paths = None

    model = median_model(landing)
    model.source = Unresolvable(Path(landing).as_posix(), marker="_READY")

    engine = open_engine(tmp_path)
    engine.add(model)
    with pytest.raises(DuckstreamError) as caught:
        engine.run()
    message = str(caught.value)
    assert "absolute_paths" in message
    assert "mid_value" in message
    # Nothing committed: refusing and writing anyway would be the worst outcome.
    assert engine.state.load_offset(engine.con, "mid_value") is None
    engine.con.close()


def land_mixed_dates(landing: Path, name: str) -> None:
    """Two dated rows in one hour, and one row with no event time at all."""
    directory = landing / name
    directory.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(
            "COPY (SELECT * FROM (VALUES "
            "  (TIMESTAMP '2026-08-22 10:00:00', 'sensor0', 1.0),"
            "  (TIMESTAMP '2026-08-22 10:01:00', 'sensor0', 3.0),"
            "  (CAST(NULL AS TIMESTAMP), 'sensor0', 99.0)"
            ") t(event_ts, sensor_id, value)) "
            f"TO '{(directory / 'part.parquet').as_posix()}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    (directory / "_READY").write_text("", encoding="utf-8")


def test_an_undated_row_touches_no_window_and_is_counted(tmp_path, landing):
    """A row with no event time belongs to no window, so it is recomputed by none.

    **No lateness horizon here, and that is the whole point.** With a horizon the
    engine filters undated rows out upstream, so the recompute planner never sees
    one and this path is not exercised at all -- which is how the first version
    of this test managed to pass while a mutation deleting the guard survived.

    Two claims, and the second only exists because the first is a silent
    divergence from a full recompute:

    * the NULL window is **not** recomputed. A recompute is scoped by a window
      range and no ``[lo, hi)`` contains NULL, so a row belonging to no window
      cannot be re-derived from one. A tier-one model folds these into a NULL
      window because it never re-reads anything; tier three cannot.
    * so they are **counted**, durably, in ``duckstream.batches``.
      ``CONTEXT.md``'s ratified rule is "dropped *and counted*, never silently
      absorbed", and before this the count was ``None`` -- the drop happened and
      nothing recorded it.
    """
    land_mixed_dates(landing, "mixed")
    engine = open_engine(tmp_path)
    engine.add(median_model(landing))
    report = engine.run()

    rows = engine.con.execute(
        "SELECT window_ts, n, mid_value FROM marts.mid_value ORDER BY window_ts"
    ).fetchall()
    assert [r[0] for r in rows] == [dt.datetime(2026, 8, 22, 10, 0)], (
        f"a NULL window reached the mart: {rows}"
    )
    assert rows[0][1] == 2, "the undated row was folded into a real window"

    assert [b.rows_undated for b in report] == [1], (
        "the row was dropped without being counted"
    )
    assert engine.state.batch_history(engine.con, "mid_value")[0]["rows_undated"] == 1, (
        "the count did not become durable, so status cannot report it later"
    )
    engine.con.close()


def test_a_horizon_still_filters_undated_rows_before_the_recompute(tmp_path, landing):
    """The other route to the same answer, and it must agree.

    With a horizon the watermark policy removes undated rows and counts them
    before the planner runs. Both paths must produce the same mart and the same
    counter, or the counter would mean two different things depending on whether
    a horizon was declared.
    """
    land_mixed_dates(landing, "mixed")
    engine = open_engine(tmp_path)
    engine.add(median_model(landing, lateness="10 minutes"))
    report = engine.run()

    rows = engine.con.execute(
        "SELECT window_ts, n FROM marts.mid_value ORDER BY window_ts"
    ).fetchall()
    assert rows == [(dt.datetime(2026, 8, 22, 10, 0), 2)]
    assert [b.rows_undated for b in report] == [1]
    engine.con.close()
