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


def test_model_status_defaults_are_a_coherent_empty_state():
    status = ModelStatus(name="x")
    assert status.healthy and status.state == "idle"
    assert not status.behind_horizon
