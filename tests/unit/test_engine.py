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
from duckstream.errors import DuckstreamError
from duckstream.lake import data_file_count, snapshot_count
from duckstream.model import Model
from duckstream.offsets import FileOffset
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


def sink_rows(con) -> list[tuple]:
    return con.execute(
        "SELECT window_ts, sensor_id, n, total FROM marts.hourly_counts "
        "ORDER BY window_ts, sensor_id"
    ).fetchall()


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
    consumed = offset["consumed"]
    assert len(consumed) == 1

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


def test_a_crash_between_sink_write_and_commit_loses_nothing(tmp_path, landing):
    """The headline claim, in process. W4 repeats it with a real process kill.

    A hook that raises at ``after_sink_write`` is the same interception point
    W4's ``os._exit`` uses; the difference is only whether the process survives
    to be asked what it sees. Either way the transaction never commits, so the
    offset must not move, the sink must not change, and no snapshot may appear.
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
    with pytest.raises(RuntimeError, match="killed between"):
        engine.run()

    assert sink_rows(engine.con) == committed_rows
    assert engine.state.load_offset(engine.con, "hourly_counts") == committed_offset
    assert snapshot_count(engine.con) == snapshots

    # And the connection is usable: the rollback left no half-open transaction,
    # so the very next trigger replays the batch that never committed.
    engine.faults.clear()
    engine.run()
    assert sink_rows(engine.con) == recompute(engine.con, landing)
    engine.con.close()


def test_a_crash_before_commit_replays_in_a_new_process_view(tmp_path, landing):
    """Recovery is from the catalog, not from anything held in memory."""
    drop_batch(landing, "b1", 5)
    engine = open_engine(tmp_path)
    engine.add(counts_model(landing))
    engine.faults.install("before_commit", lambda event: (_ for _ in ()).throw(
        RuntimeError("gone")
    ))
    with pytest.raises(RuntimeError):
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
    with pytest.raises(RuntimeError):
        engine.run()
    engine.con.close()

    restarted = open_engine(tmp_path)
    restarted.add(counts_model(landing))
    assert sink_rows(restarted.con) == recompute(restarted.con, landing)
    # Nothing left to do: the batch was durable before the crash.
    assert restarted.run().batches == ()
    restarted.con.close()


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


def test_a_non_additive_update_model_is_refused_naming_the_model(tmp_path, landing):
    """Phase 1 folds the additive tier only, and says so before running anything."""
    drop_batch(landing, "b1", 4)
    engine = open_engine(tmp_path)
    engine.add(
        counts_model(
            landing,
            name="mean_value",
            aggregates={"mean_value": "avg(value)"},
        )
    )
    with pytest.raises(DuckstreamError) as caught:
        engine.run()
    message = str(caught.value)
    assert "mean_value" in message
    assert "phase 3" in message
    # Refused before any work: no sink table, no offset.
    assert engine.state.load_offset(engine.con, "mean_value") is None
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
        )
    )
    with pytest.raises(DuckstreamError) as caught:
        engine.run()
    message = str(caught.value)
    assert "register(con)" in message
    assert "doubled" in message
    engine.con.close()
