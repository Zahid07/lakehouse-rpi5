"""Lag and status: the read-only view of what a pipeline is doing.

``PLAN.md`` calls lag "the operational metric that matters". The tests below are
mostly about the fact that it is not one metric but three, and that they fail
independently -- which is the whole reason for reporting all of them:

* a pipeline whose cron entry was deleted has *perfect* event-time lag, right up
  until somebody notices the processing lag;
* a pipeline processing a backfill has terrible event-time lag and is completely
  healthy;
* a pipeline whose source has silently stopped being written has zero of both
  lags and a backlog that never grows.

A status that collapsed those into one number would be reassuring in at least
one of the three.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from duckstream.lake import DEFAULT_ALIAS, attach_lake
from duckstream.metrics import ModelStatus, collect, status_for
from duckstream.model import Model
from duckstream.protocols import BatchLimits, BatchPlan
from duckstream.state import DuckLakeStateStore, Position
from duckstream.sinks import TableSink

NOW = dt.datetime(2026, 8, 23, 12, 0, 0)


class _Source:
    """A source that reports a fixed backlog, or refuses to."""

    type_name = "stub"

    def __init__(self, pending: int = 0, *, explode: bool = False):
        self.pending = pending
        self.explode = explode

    def latest_offset(self):
        if self.explode:
            raise OSError("the landing mount is gone")
        return {"n": self.pending}

    def plan(self, start, end, limits):
        if self.explode:
            raise OSError("the landing mount is gone")
        assert limits == BatchLimits(), (
            "the backlog must be planned unbounded, or a limit would flatten "
            "the difference between ten files waiting and ten thousand"
        )
        files = [f"f{i}" for i in range(self.pending)]
        return BatchPlan(
            start=start, end=end, payload={"files": files}, is_empty=not files
        )

    def bind(self, con, plan):  # pragma: no cover - never bound here
        raise NotImplementedError

    def to_config(self):
        return {"type": self.type_name}


def make_model(name="m", *, lateness=None, pending=0, explode=False) -> Model:
    return Model(
        name=name,
        source=_Source(pending, explode=explode),
        sink=TableSink(f"marts.{name}"),
        aggregates={"n": "count(*)"},
        key=["window_ts", "sensor_id"] if lateness else ["sensor_id"],
        time_column="event_ts" if lateness else None,
        grain="hour" if lateness else None,
        lateness=lateness,
    )


@pytest.fixture
def con(tmp_path):
    connection = duckdb.connect()
    attach_lake(
        connection,
        str(tmp_path / "catalog.ducklake"),
        data_path=str(tmp_path / "lake_data"),
        settings={"threads": 2},
    )
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def store(con):
    s = DuckLakeStateStore(catalog=DEFAULT_ALIAS)
    s.ensure(con)
    return s


def commit_batch(store, con, name, *, batch_id, offset, watermark=None, **counts):
    store.record_batch_start(con, name, batch_id)
    store.begin(con)
    store.record_batch_end(con, name, batch_id, **counts)
    store.commit(
        con,
        {name: offset},
        {} if watermark is None else {name: watermark},
    )


# --------------------------------------------------------------------------
# A model that has never run
# --------------------------------------------------------------------------


def test_a_model_that_has_never_run_is_idle_not_broken(con, store):
    status = status_for(con, store, make_model(), now=NOW)
    assert status.state == "idle"
    assert status.healthy, "never having run is not the same as being unhealthy"
    assert status.processing_lag is None and status.event_lag is None
    assert status.batches == 0 and status.offset is None


# --------------------------------------------------------------------------
# The three lags
# --------------------------------------------------------------------------


def test_event_lag_is_measured_from_the_watermark(con, store):
    commit_batch(
        store, con, "m", batch_id=1, offset={"a": 1},
        watermark=NOW - dt.timedelta(minutes=7), rows_in=10,
    )
    status = status_for(con, store, make_model(lateness="10 minutes"), now=NOW)
    assert status.event_lag == dt.timedelta(minutes=7)
    assert status.lateness == dt.timedelta(minutes=10)
    assert not status.behind_horizon
    assert status.state == "ok"


def test_event_lag_past_the_horizon_is_called_out(con, store):
    """The number is only meaningful against the horizon it is judged by.

    Past it, windows are sealing before their late arrivals turn up -- so this
    is the signal that ``rows_late`` is about to start climbing, and it is worth
    a word of its own rather than a bare duration.
    """
    commit_batch(
        store, con, "m", batch_id=1, offset={"a": 1},
        watermark=NOW - dt.timedelta(hours=3),
    )
    status = status_for(con, store, make_model(lateness="10 minutes"), now=NOW)
    assert status.behind_horizon
    assert status.state == "behind"
    assert status.healthy, (
        "being behind is a tuning problem, not a failure -- a backfill is "
        "hours behind by definition and is working perfectly"
    )


def test_a_model_with_no_horizon_reports_no_event_lag(con, store):
    """No horizon, no opinion about time. Reporting 0 would be a claim."""
    commit_batch(store, con, "m", batch_id=1, offset={"a": 1})
    status = status_for(con, store, make_model(), now=NOW)
    assert status.event_lag is None and status.lateness is None
    assert not status.behind_horizon


def test_processing_lag_is_measured_from_the_last_commit(con, store):
    """The metric that catches a deleted cron entry.

    Such a pipeline has a perfectly good watermark -- frozen at whatever it was
    when it last ran -- so event-time lag climbs but says nothing about *why*.
    """
    commit_batch(
        store, con, "m", batch_id=1, offset={"a": 1},
        committed_at=NOW - dt.timedelta(minutes=90), rows_in=5, rows_out=2,
    )
    status = status_for(con, store, make_model(), now=NOW)
    assert status.processing_lag == dt.timedelta(minutes=90)
    assert status.last_batch_id == 1


def test_backlog_counts_what_the_source_is_still_holding(con, store):
    status = status_for(con, store, make_model(pending=17), now=NOW)
    assert status.backlog == 17


def test_backlog_is_none_when_the_source_cannot_answer(con, store):
    """Asking is I/O against the thing that may itself be broken.

    A source that raises must cost the caller a *number*, not the whole status
    -- the rest of which is exactly what you want when the landing mount has
    gone away.
    """
    status = status_for(con, store, make_model(explode=True), now=NOW)
    assert status.backlog is None
    assert status.state == "idle", "the rest of the status survived"


def test_backlog_can_be_skipped(con, store):
    status = status_for(
        con, store, make_model(pending=5), now=NOW, include_backlog=False
    )
    assert status.backlog is None


# --------------------------------------------------------------------------
# Throughput
# --------------------------------------------------------------------------


def test_counters_accumulate_across_batches(con, store):
    for batch_id in (1, 2, 3):
        commit_batch(
            store, con, "m", batch_id=batch_id, offset={"a": batch_id},
            rows_in=10, rows_out=3, rows_late=1, rows_undated=2,
        )
    status = status_for(con, store, make_model(), now=NOW)
    assert (status.batches, status.rows_in, status.rows_out) == (3, 30, 9)
    assert (status.rows_late, status.rows_undated) == (3, 6)


def test_null_counters_do_not_poison_the_totals(con, store):
    """A model with no horizon stores NULL for the drop counts, not zero."""
    commit_batch(store, con, "m", batch_id=1, offset={"a": 1}, rows_in=4)
    status = status_for(con, store, make_model(), now=NOW)
    assert status.rows_in == 4
    assert status.rows_late == 0 and status.rows_undated == 0


# --------------------------------------------------------------------------
# Failure and quarantine
# --------------------------------------------------------------------------


def test_a_failing_model_reports_its_attempt_error_and_retry_time(con, store):
    store.record_failure(
        con, "m", 1, Position(offset={"a": 1}, attempt=2), ValueError("bad file"),
        now=NOW - dt.timedelta(seconds=1),
    )
    status = status_for(con, store, make_model(), now=NOW)
    assert status.state == "failing"
    assert not status.healthy
    assert status.attempt == 3
    assert "bad file" in status.error
    assert status.retry_at is not None, "a failing model must say when it retries"


def test_quarantine_marks_a_model_unhealthy_permanently(con, store):
    """The record that data was lost does not expire when the stream recovers.

    A status that went green again would be quietly retiring the only evidence
    that a gap exists in the output.
    """
    store.quarantine(
        con, "m", 1, Position(offset={"a": 1}), {"a": 2},
        payload={"files": ["bad.parquet"]}, rows_in=12, attempts=5,
        error=ValueError("no magic bytes"),
    )
    commit_batch(store, con, "m", batch_id=2, offset={"a": 3}, rows_in=9, rows_out=4)

    status = status_for(con, store, make_model(), now=NOW)
    assert status.quarantined == 1
    assert status.quarantined_rows == 12
    assert status.state == "quarantined"
    assert not status.healthy
    assert status.attempt == 0, "the stream itself recovered"


def test_failing_outranks_quarantined_in_the_one_word_verdict(con, store):
    """Ordered by what to look at first: one is actionable now, one is history."""
    store.quarantine(con, "m", 1, Position(), {"a": 1}, error="old")
    store.record_failure(con, "m", 2, Position(offset={"a": 1}), "new")
    assert status_for(con, store, make_model(), now=NOW).state == "failing"


# --------------------------------------------------------------------------
# Collecting several
# --------------------------------------------------------------------------


def test_collect_reports_every_model_in_declaration_order(con, store):
    models = [make_model("alpha"), make_model("beta"), make_model("gamma")]
    commit_batch(store, con, "beta", batch_id=1, offset={"a": 1})
    snapshot = collect(con, store, models, now=NOW)

    assert [m.name for m in snapshot.models] == ["alpha", "beta", "gamma"]
    assert snapshot.by_name("beta").state == "ok"
    assert snapshot.healthy


def test_one_unhealthy_model_makes_the_snapshot_unhealthy(con, store):
    store.record_failure(con, "beta", 1, Position(), "boom")
    snapshot = collect(con, store, [make_model("alpha"), make_model("beta")], now=NOW)
    assert not snapshot.healthy
    assert snapshot.by_name("alpha").healthy


def test_status_reads_nothing_but_the_catalog(con, store):
    """It must be safe to point at a live deployment from another process.

    So it opens no transaction and writes nothing -- asserted on the snapshot
    count, which is the only thing that would notice.
    """
    from duckstream.lake import snapshot_count

    commit_batch(store, con, "m", batch_id=1, offset={"a": 1})
    before = snapshot_count(con, DEFAULT_ALIAS)
    collect(con, store, [make_model(pending=3)], now=NOW)
    assert snapshot_count(con, DEFAULT_ALIAS) == before


def test_a_large_offset_is_reported_because_it_is_rewritten_every_trigger(con, store):
    """The Pi cliff, made visible before somebody drives off it.

    The file source's offset is written **in full** on every trigger, so its
    size is a write-amplification figure rather than a storage one.
    ``CONTEXT.md`` 1.15 measured it at 45.7 MB after a year at one file a
    minute — about 65 GB of writes a day, which is an SD card's budget being
    spent re-recording file names it already knows. Nothing surfaced it, so
    nothing would have surfaced it until the card died.
    """
    big = {"kind": "file", "v": 1, "consumed": {
        f"2026/05/part-{i:06d}.parquet": {"size": i, "mtime_ns": i}
        for i in range(20_000)
    }}
    commit_batch(store, con, "m", batch_id=1, offset=big)

    status = status_for(con, store, make_model(), now=NOW, include_backlog=False)
    assert status.offset_bytes > 1_000_000
    assert status.offset_is_large
    assert status.state == "bloated"


def test_a_small_offset_says_nothing(con, store):
    """The warning has to stay quiet in the ordinary case to mean anything."""
    commit_batch(store, con, "m", batch_id=1, offset={"consumed": {"a": 1}})
    status = status_for(con, store, make_model(), now=NOW, include_backlog=False)
    assert status.offset_bytes < 1_000
    assert not status.offset_is_large
    assert status.state == "ok"


def test_model_status_defaults_are_a_coherent_empty_state():
    status = ModelStatus(name="x")
    assert status.healthy and status.state == "idle"
    assert not status.behind_horizon


# --------------------------------------------------------------------------
# status on a catalog that has not been migrated yet
# --------------------------------------------------------------------------


def test_status_does_not_report_a_migrated_model_s_files_as_backlog(tmp_path):
    """The wrong number an upgrade would otherwise put in front of an operator.

    ``status`` never runs a batch, so it never migrates. A model whose position
    is still a v1 map therefore has an *empty* consumed-file table -- and asking
    that table would answer "this model has read nothing" and report the whole
    landing tree as waiting. On the deployment ``CONTEXT.md`` 1.15 is written
    about that is a backlog of 525,600 files, shown to the person checking
    whether the upgrade went well, and emitted over ``--json`` to whatever is
    thresholding on it. It reads exactly like the upgrade having lost the
    position, which is the one thing it must not do.

    ``consumed_files`` goes to ``None`` rather than ``0`` for the same reason:
    ``None`` is the documented "cannot say", and ``0`` would be a lie.
    """
    import os

    import duckdb

    from duckstream.engine import Engine
    from duckstream.metrics import status_for
    from duckstream.model import Model
    from duckstream.offsets import FileOffset, encode_offset
    from duckstream.sinks.table import TableSink
    from duckstream.sources.files import FileSource

    landing = tmp_path / "landing"
    landing.mkdir()

    def drop(name):
        directory = landing / name
        directory.mkdir()
        writer = duckdb.connect()
        try:
            writer.execute(
                "COPY (SELECT TIMESTAMP '2026-01-01 00:00:00' AS event_ts, "
                "'s' AS sensor_id, 1.0 AS value) "
                f"TO '{(directory / 'p.parquet').as_posix()}' (FORMAT PARQUET)"
            )
        finally:
            writer.close()
        (directory / "_READY").write_text("", encoding="utf-8")
        return directory

    read_already = drop("b1")
    con = duckdb.connect()
    engine = Engine(
        con, catalog=tmp_path / "c.ducklake", data_path=tmp_path / "lake"
    )
    engine.add(
        Model(
            name="m",
            source=FileSource(landing.as_posix(), marker="_READY"),
            time_column="event_ts",
            grain="hour",
            key=["window_ts", "sensor_id"],
            aggregates={"n": "count(*)"},
            sink=TableSink("marts.m", mode="update"),
        )
    )
    engine.run()

    # Put the catalog back the way an upgrading deployment will be found:
    # the set inside the offset, and no rows behind it.
    store = engine.state
    stat = os.stat(read_already / "p.parquet")
    con.execute("BEGIN")
    con.execute(
        f"DELETE FROM {store.consumed_files_table} WHERE model_name = 'm'"
    )
    con.execute(
        f"UPDATE {store.offsets_table} SET offset_json = ? WHERE model_name = 'm'",
        [
            encode_offset(
                FileOffset.build(
                    {"b1/p.parquet": {"size": stat.st_size,
                                      "mtime_ns": stat.st_mtime_ns}}
                )
            )
        ],
    )
    con.execute("COMMIT")

    drop("b2")  # one file that genuinely is waiting

    before = status_for(con, store, engine.model("m"))
    assert before.backlog == 1, (
        f"status reported {before.backlog} file(s) waiting; only b2 is. Reading "
        f"an empty consumed-file table for a model whose position has not "
        f"migrated counts everything it has already read as unread."
    )
    assert before.consumed_files is None, (
        "0 would be a lie; None is the documented 'cannot say'"
    )

    engine.run()

    after = status_for(con, store, engine.model("m"))
    assert after.backlog == 0
    assert after.consumed_files == 2
    con.close()
