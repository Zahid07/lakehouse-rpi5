"""Unit tests for ``duckstream.sources.files`` and ``duckstream.offsets``.

Two things shape this file.

First, **every incremental test runs at least two batches.** ``CONTEXT.md``
section 1.5 records a bug that appeared only on the *second* pass — a
single-batch test would have missed it entirely. The same reasoning applies to a
source: a first plan that returns everything proves nothing about whether the
second plan correctly returns only what is new.

Second, the truncation tests assert the **union across batches** equals the full
file set exactly once. Losing a file and reading one twice are different bugs
with the same symptom in a naive "did batch 2 get the rest?" assertion, and
exactly-once claims stand or fall on both.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pytest

from duckstream.errors import ConfigError, DuckstreamError
from duckstream.offsets import FileOffset, decode_offset, encode_offset
from duckstream.protocols import BatchLimits
from duckstream.sources.files import VIEW_PREFIX, FileSource

MARKER = "_READY"


# -- helpers ---------------------------------------------------------------


@pytest.fixture(scope="module")
def writer():
    """One in-memory connection used only to author test fixtures."""
    con = duckdb.connect()
    try:
        yield con
    finally:
        con.close()


def sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def write_parquet(con, path: Path, rows: int, start: int = 0) -> Path:
    """A parquet file with ``rows`` rows of ``i`` running from ``start``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT i FROM range({start}, {start + rows}) t(i)) "
        f"TO '{sql_path(path)}' (FORMAT PARQUET)"
    )
    return path


def write_csv(path: Path, rows: int, start: int = 0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "i\n" + "".join(f"{i}\n" for i in range(start, start + rows))
    path.write_text(body, encoding="utf-8")
    return path


def mark(directory: Path, *, age_seconds: float = 0.0, name: str = MARKER) -> Path:
    """Drop a completion marker, optionally backdated by ``age_seconds``.

    Backdating beats sleeping: the settle delay is a comparison against the
    marker's mtime, so moving the mtime tests it exactly and instantly.
    """
    directory.mkdir(parents=True, exist_ok=True)
    marker = directory / name
    marker.write_text("", encoding="utf-8")
    if age_seconds:
        when = marker.stat().st_mtime - age_seconds
        os.utime(marker, (when, when))
    return marker


def touch_mtime(path: Path, *, delta_seconds: float) -> None:
    when = path.stat().st_mtime + delta_seconds
    os.utime(path, (when, when))


def consumed(offset) -> dict:
    return FileOffset.consumed(offset)


def relpaths(plan) -> list[str]:
    return list(plan.payload["relpaths"])


class Consumption:
    """Plans batches the way the engine does, against one consumed-set shape.

    The two shapes are parametrised over rather than one being picked, because
    they differ in *when* a file becomes consumed and that is the whole thing
    this file is about. With the map, planning a batch and remembering it are
    one act -- the returned offset already carries the files. With rows, ``plan``
    only *decides*, and the recording is a separate write the engine makes
    inside the batch's transaction. A suite that only ever exercised the first
    would not notice a source that decided correctly and then declared nothing
    to record, which is a silent replay of every batch for ever.

    ``take`` is therefore plan-then-commit, in that order, exactly as
    ``Engine._attempt_batch`` does it.
    """

    def __init__(self, shape: str) -> None:
        self.shape = shape
        self.offset = None
        self.batch_id = 0
        self.con = None
        self.index = None
        if shape == "rows":
            from duckstream.state import MemoryStateStore

            self.con = duckdb.connect()
            self.store = MemoryStateStore("duckstream")
            self.store.ensure(self.con)
            self.index = self.store.consumed_files.index_for(self.con, "m")

    def close(self) -> None:
        if self.con is not None:
            self.con.close()

    def plan(self, source, limits=None):
        """Decide the next batch. Nothing is consumed until :meth:`commit`."""
        end = source.latest_offset()
        limits = BatchLimits() if limits is None else limits
        if self.index is None:
            return source.plan(self.offset, end, limits)
        return source.plan(self.offset, end, limits, consumed=self.index)

    def commit(self, plan):
        """What the engine writes inside the batch's transaction."""
        self.batch_id += 1
        if self.index is not None:
            self.con.execute("BEGIN")
            self.index.record(self.batch_id, plan.payload)
            self.con.execute("COMMIT")
        self.offset = plan.end
        return plan

    def take(self, source, limits=None):
        return self.commit(self.plan(source, limits))

    def consumed(self) -> list[str]:
        """Everything this model has read, whichever shape holds it."""
        if self.index is None:
            return sorted(FileOffset.consumed(self.offset))
        return sorted(
            row[0]
            for row in self.con.execute(
                f"SELECT relpath FROM {self.store.consumed_files_table} "
                f"WHERE model_name = 'm'"
            ).fetchall()
        )


@pytest.fixture(params=["map", "rows"])
def consumption(request):
    """One :class:`Consumption` per consumed-set shape. See its docstring."""
    state = Consumption(request.param)
    try:
        yield state
    finally:
        state.close()


def drain(source, consumption, *, limits=None, max_batches: int = 50) -> list[list[str]]:
    """Run the source to exhaustion. Returns the relpaths of each batch.

    This is the loop the ``AvailableNow`` trigger runs, reduced to the source's
    part of it, and it is where truncation bugs actually show up.
    """
    batches: list[list[str]] = []
    for _ in range(max_batches):
        plan = consumption.plan(source, limits)
        if plan.is_empty:
            break
        batches.append(relpaths(plan))
        consumption.commit(plan)
        if not plan.has_more:
            break
    else:  # pragma: no cover - a runaway loop is a test failure, not a hang
        pytest.fail("source did not drain within max_batches")
    return batches


# -- construction validation ----------------------------------------------


def test_path_is_required():
    with pytest.raises(ConfigError):
        FileSource(None)
    with pytest.raises(ConfigError):
        FileSource("   ")


def test_unknown_format_is_refused():
    with pytest.raises(ConfigError) as excinfo:
        FileSource("landing", format="avro")
    assert "avro" in str(excinfo.value)
    assert "parquet" in str(excinfo.value)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_files_per_trigger": 0},
        {"max_files_per_trigger": -1},
        {"max_rows_per_trigger": 0},
        {"max_rows_per_trigger": -5},
        {"settle_seconds": -1},
        {"marker": ""},
        {"pattern": ""},
    ],
)
def test_invalid_arguments_are_refused(kwargs):
    with pytest.raises(ConfigError):
        FileSource("landing", **kwargs)


def test_config_errors_are_duckstream_errors():
    """The whole framework must be catchable with one ``except`` clause."""
    with pytest.raises(DuckstreamError):
        FileSource("landing", format="avro")


def test_marker_must_be_a_name_not_a_path():
    with pytest.raises(ConfigError):
        FileSource("landing", marker="sub/_READY")


# -- marker gating ---------------------------------------------------------


def test_unmarked_directory_yields_nothing(tmp_path, writer):
    write_parquet(writer, tmp_path / "a.parquet", 3)
    source = FileSource(tmp_path)

    assert consumed(source.latest_offset()) == {}
    plan = source.plan(None, source.latest_offset(), BatchLimits())
    assert plan.is_empty
    assert relpaths(plan) == []


def test_marked_directory_yields_its_files(tmp_path, writer):
    write_parquet(writer, tmp_path / "a.parquet", 3)
    write_parquet(writer, tmp_path / "b.parquet", 4)
    mark(tmp_path)
    source = FileSource(tmp_path)

    assert sorted(consumed(source.latest_offset())) == ["a.parquet", "b.parquet"]


def test_marker_file_is_never_data(tmp_path, writer):
    write_parquet(writer, tmp_path / "a.parquet", 1)
    mark(tmp_path, name="_READY.parquet")
    source = FileSource(tmp_path, marker="_READY.parquet")

    assert sorted(consumed(source.latest_offset())) == ["a.parquet"]


def test_marker_younger_than_settle_yields_nothing(tmp_path, writer):
    write_parquet(writer, tmp_path / "a.parquet", 3)
    mark(tmp_path)  # marker written now
    source = FileSource(tmp_path, settle_seconds=60.0)

    assert consumed(source.latest_offset()) == {}

    # Backdate the marker past the settle delay and the same source now sees it.
    mark(tmp_path, age_seconds=120.0)
    assert sorted(consumed(source.latest_offset())) == ["a.parquet"]


def test_gating_is_per_directory(tmp_path, writer):
    """A marked parent does not make an unmarked child eligible."""
    write_parquet(writer, tmp_path / "ready" / "a.parquet", 2)
    write_parquet(writer, tmp_path / "ready" / "pending" / "b.parquet", 2)
    mark(tmp_path / "ready")
    source = FileSource(tmp_path)

    assert sorted(consumed(source.latest_offset())) == ["ready/a.parquet"]

    mark(tmp_path / "ready" / "pending")
    assert sorted(consumed(source.latest_offset())) == [
        "ready/a.parquet",
        "ready/pending/b.parquet",
    ]


def test_marker_none_disables_gating(tmp_path, writer):
    write_parquet(writer, tmp_path / "a.parquet", 3)
    source = FileSource(tmp_path, marker=None)

    assert sorted(consumed(source.latest_offset())) == ["a.parquet"]


def test_non_recursive_ignores_subdirectories(tmp_path, writer):
    write_parquet(writer, tmp_path / "a.parquet", 1)
    write_parquet(writer, tmp_path / "sub" / "b.parquet", 1)
    mark(tmp_path)
    mark(tmp_path / "sub")
    source = FileSource(tmp_path, recursive=False)

    assert sorted(consumed(source.latest_offset())) == ["a.parquet"]


def test_pattern_filters_by_name(tmp_path, writer):
    write_parquet(writer, tmp_path / "keep.parquet", 1)
    write_parquet(writer, tmp_path / "skip.parquet", 1)
    mark(tmp_path)
    source = FileSource(tmp_path, pattern="keep*.parquet")

    assert sorted(consumed(source.latest_offset())) == ["keep.parquet"]


# -- incremental planning --------------------------------------------------


def test_second_plan_returns_only_new_files(tmp_path, writer, consumption):
    """Two batches minimum: batch one proves discovery, batch two proves memory."""
    write_parquet(writer, tmp_path / "a.parquet", 2)
    write_parquet(writer, tmp_path / "b.parquet", 2)
    mark(tmp_path)
    source = FileSource(tmp_path)

    first = consumption.take(source)
    assert sorted(relpaths(first)) == ["a.parquet", "b.parquet"]
    assert not first.is_empty
    assert not first.has_more

    # Nothing new: the second plan is empty and the position does not move.
    idle = consumption.plan(source)
    assert idle.is_empty
    assert relpaths(idle) == []
    assert encode_offset(idle.end) == encode_offset(first.end)

    # One new file: only that file is planned, and the position accumulates.
    write_parquet(writer, tmp_path / "c.parquet", 2)
    mark(tmp_path)
    second = consumption.take(source)
    assert relpaths(second) == ["c.parquet"]
    assert consumption.consumed() == ["a.parquet", "b.parquet", "c.parquet"]


def test_rewritten_file_is_replanned(tmp_path, writer, consumption):
    write_parquet(writer, tmp_path / "a.parquet", 2)
    mark(tmp_path)
    source = FileSource(tmp_path)

    first = consumption.take(source)
    assert relpaths(first) == ["a.parquet"]

    # Same path, different contents. Both size and mtime move.
    write_parquet(writer, tmp_path / "a.parquet", 50)
    touch_mtime(tmp_path / "a.parquet", delta_seconds=5)
    mark(tmp_path)

    second = consumption.take(source)
    assert relpaths(second) == ["a.parquet"], "a rewritten file is unconsumed"

    # And once re-consumed at its new identity, it settles again.
    assert consumption.plan(source).is_empty


def test_rewrite_detected_by_mtime_alone(tmp_path, writer, consumption):
    """Same byte length, different mtime, still a rewrite."""
    write_parquet(writer, tmp_path / "a.parquet", 4)
    mark(tmp_path)
    source = FileSource(tmp_path)
    consumption.take(source)

    size_before = (tmp_path / "a.parquet").stat().st_size
    touch_mtime(tmp_path / "a.parquet", delta_seconds=10)
    assert (tmp_path / "a.parquet").stat().st_size == size_before

    second = consumption.plan(source)
    assert relpaths(second) == ["a.parquet"]


def test_ordering_is_deterministic_across_scans(tmp_path, writer):
    for name in ("d", "a", "c", "b"):
        write_parquet(writer, tmp_path / f"{name}.parquet", 1)
    # Force a shared mtime so the path tiebreaker is the only thing ordering them.
    stamp = (tmp_path / "a.parquet").stat().st_mtime
    for name in ("d", "a", "c", "b"):
        os.utime(tmp_path / f"{name}.parquet", (stamp, stamp))
    mark(tmp_path)
    source = FileSource(tmp_path)

    orders = [
        relpaths(source.plan(None, source.latest_offset(), BatchLimits()))
        for _ in range(5)
    ]
    assert all(order == orders[0] for order in orders)
    assert orders[0] == ["a.parquet", "b.parquet", "c.parquet", "d.parquet"]


def test_ordering_follows_mtime_before_path(tmp_path, writer):
    write_parquet(writer, tmp_path / "z.parquet", 1)
    write_parquet(writer, tmp_path / "a.parquet", 1)
    touch_mtime(tmp_path / "z.parquet", delta_seconds=-60)
    mark(tmp_path)
    source = FileSource(tmp_path)

    assert relpaths(source.plan(None, source.latest_offset(), BatchLimits())) == [
        "z.parquet",
        "a.parquet",
    ]


# -- limits ----------------------------------------------------------------


def test_max_files_truncates_and_the_position_covers_only_included(
    tmp_path, writer, consumption
):
    names = [f"f{i}.parquet" for i in range(5)]
    for i, name in enumerate(names):
        write_parquet(writer, tmp_path / name, 1)
        touch_mtime(tmp_path / name, delta_seconds=i)
    mark(tmp_path)
    source = FileSource(tmp_path, max_files_per_trigger=2)

    first = consumption.take(source)
    assert relpaths(first) == names[:2]
    assert first.has_more is True
    assert consumption.consumed() == sorted(names[:2]), (
        "a batch must check point only the files it actually included; "
        "recording the whole scan would mark unread files consumed"
    )

    second = consumption.take(source)
    assert relpaths(second) == names[2:4]
    assert second.has_more is True

    third = consumption.take(source)
    assert relpaths(third) == names[4:]
    assert third.has_more is False
    assert consumption.consumed() == sorted(names)

    assert consumption.plan(source).is_empty


def test_batches_partition_the_file_set_exactly_once(
    tmp_path, writer, consumption
):
    """No gap and no duplication — the property exactly-once rests on."""
    names = [f"f{i:02d}.parquet" for i in range(7)]
    for i, name in enumerate(names):
        write_parquet(writer, tmp_path / name, 1)
        touch_mtime(tmp_path / name, delta_seconds=i)
    mark(tmp_path)
    source = FileSource(tmp_path, max_files_per_trigger=3)

    batches = drain(source, consumption)
    seen = [rel for batch in batches for rel in batch]

    assert len(batches) > 1, "the limit must actually have split the work"
    assert sorted(seen) == sorted(names), "union across batches is the full set"
    assert len(seen) == len(set(seen)), "no file appears in two batches"


def test_caller_may_tighten_but_never_loosen_limits(tmp_path, writer):
    names = [f"f{i}.parquet" for i in range(6)]
    for i, name in enumerate(names):
        write_parquet(writer, tmp_path / name, 1)
        touch_mtime(tmp_path / name, delta_seconds=i)
    mark(tmp_path)
    source = FileSource(tmp_path, max_files_per_trigger=3)

    tightened = source.plan(
        None, source.latest_offset(), BatchLimits(max_files_per_trigger=2)
    )
    assert len(relpaths(tightened)) == 2

    loosened = source.plan(
        None, source.latest_offset(), BatchLimits(max_files_per_trigger=99)
    )
    assert len(relpaths(loosened)) == 3, "the source's own limit still binds"

    unlimited = FileSource(tmp_path)
    assert len(relpaths(unlimited.plan(None, unlimited.latest_offset(), None))) == 6


def test_max_rows_per_trigger_on_parquet(tmp_path, writer, consumption):
    # 40, 40, 40 rows; a 100-row budget takes two files, then the third.
    for i, rows in enumerate((40, 40, 40)):
        path = write_parquet(writer, tmp_path / f"f{i}.parquet", rows)
        touch_mtime(path, delta_seconds=i)
    mark(tmp_path)
    source = FileSource(tmp_path, max_rows_per_trigger=100)

    first = consumption.take(source)
    assert relpaths(first) == ["f0.parquet", "f1.parquet"]
    assert first.payload["row_count"] == 80
    assert first.has_more is True

    second = consumption.take(source)
    assert relpaths(second) == ["f2.parquet"]
    assert second.payload["row_count"] == 40
    assert second.has_more is False

    assert consumption.plan(source).is_empty


def test_single_oversized_file_still_makes_progress(
    tmp_path, writer, consumption
):
    """A file bigger than the whole budget must not wedge the pipeline."""
    big = write_parquet(writer, tmp_path / "big.parquet", 500)
    small = write_parquet(writer, tmp_path / "small.parquet", 1)
    touch_mtime(big, delta_seconds=-10)
    mark(tmp_path)
    source = FileSource(tmp_path, max_rows_per_trigger=10)

    first = consumption.plan(source)
    assert relpaths(first) == ["big.parquet"]
    assert first.payload["row_count"] == 500
    assert first.has_more is True

    batches = drain(source, consumption)
    seen = [rel for batch in batches for rel in batch]
    assert sorted(seen) == ["big.parquet", "small.parquet"]
    assert len(seen) == len(set(seen))


def test_file_and_row_limits_compose(tmp_path, writer, consumption):
    for i in range(6):
        path = write_parquet(writer, tmp_path / f"f{i}.parquet", 10)
        touch_mtime(path, delta_seconds=i)
    mark(tmp_path)
    source = FileSource(
        tmp_path, max_files_per_trigger=4, max_rows_per_trigger=25
    )

    first = consumption.plan(source)
    assert relpaths(first) == ["f0.parquet", "f1.parquet"]  # rows bind before files
    assert first.has_more is True

    batches = drain(source, consumption)
    seen = [rel for batch in batches for rel in batch]
    assert sorted(seen) == sorted(f"f{i}.parquet" for i in range(6))
    assert len(seen) == len(set(seen))


def test_max_rows_is_not_enforced_for_csv(tmp_path, consumption):
    """Documented v1 limit: row counts are parquet-only, files still bind."""
    for i in range(4):
        path = write_csv(tmp_path / f"f{i}.csv", 100)
        touch_mtime(path, delta_seconds=i)
    mark(tmp_path)
    source = FileSource(
        tmp_path, format="csv", max_rows_per_trigger=1, max_files_per_trigger=2
    )

    first = consumption.plan(source)
    assert relpaths(first) == ["f0.csv", "f1.csv"]
    assert first.payload["row_count"] is None
    assert first.has_more is True

    batches = drain(source, consumption)
    seen = [rel for batch in batches for rel in batch]
    assert sorted(seen) == ["f0.csv", "f1.csv", "f2.csv", "f3.csv"]
    assert len(seen) == len(set(seen))


# -- bind ------------------------------------------------------------------


def test_bind_returns_exactly_the_planned_rows(tmp_path, writer):
    write_parquet(writer, tmp_path / "a.parquet", 3, start=0)
    write_parquet(writer, tmp_path / "b.parquet", 2, start=100)
    write_parquet(writer, tmp_path / "c.parquet", 5, start=200)
    for i, name in enumerate(("a", "b", "c")):
        touch_mtime(tmp_path / f"{name}.parquet", delta_seconds=i)
    mark(tmp_path)
    source = FileSource(tmp_path, max_files_per_trigger=2)

    con = duckdb.connect()
    try:
        first = source.plan(None, source.latest_offset(), BatchLimits())
        view = source.bind(con, first)
        rows = con.execute(f'SELECT i FROM "{view}" ORDER BY i').fetchall()
        assert [r[0] for r in rows] == [0, 1, 2, 100, 101], (
            "the view must cover the planned files and nothing else — "
            "c.parquet was truncated out of this batch"
        )

        second = source.plan(first.end, source.latest_offset(), BatchLimits())
        view2 = source.bind(con, second)
        rows2 = con.execute(f'SELECT i FROM "{view2}" ORDER BY i').fetchall()
        assert [r[0] for r in rows2] == list(range(200, 205))
    finally:
        con.close()


def test_two_binds_in_one_connection_do_not_collide(tmp_path, writer):
    write_parquet(writer, tmp_path / "a.parquet", 3)
    mark(tmp_path)
    source = FileSource(tmp_path)

    con = duckdb.connect()
    try:
        plan = source.plan(None, source.latest_offset(), BatchLimits())
        first = source.bind(con, plan)
        second = source.bind(con, plan)

        assert first != second, (
            "a shared staging name is the defect CONTEXT.md section 5 records; "
            "each bind must get its own view"
        )
        assert first.startswith(VIEW_PREFIX) and second.startswith(VIEW_PREFIX)
        assert con.execute(f'SELECT count(*) FROM "{first}"').fetchone()[0] == 3
        assert con.execute(f'SELECT count(*) FROM "{second}"').fetchone()[0] == 3
    finally:
        con.close()


def test_bind_on_an_empty_plan_raises(tmp_path):
    mark(tmp_path)
    source = FileSource(tmp_path)
    con = duckdb.connect()
    try:
        plan = source.plan(None, source.latest_offset(), BatchLimits())
        assert plan.is_empty
        with pytest.raises(DuckstreamError):
            source.bind(con, plan)
    finally:
        con.close()


def test_bind_handles_paths_with_spaces_and_quotes(tmp_path, writer):
    """These end up inside a SQL string literal, so they are a real hazard."""
    directory = tmp_path / "land ing" / "it's here"
    names = ["a file.parquet", "o'brien.parquet", "quote''double.parquet"]
    for i, name in enumerate(names):
        write_parquet(writer, directory / name, i + 1, start=10 * i)
    mark(directory)
    source = FileSource(tmp_path)

    plan = source.plan(None, source.latest_offset(), BatchLimits())
    assert len(relpaths(plan)) == 3

    con = duckdb.connect()
    try:
        view = source.bind(con, plan)
        assert con.execute(f'SELECT count(*) FROM "{view}"').fetchone()[0] == 6
    finally:
        con.close()


def test_bind_csv_and_json(tmp_path):
    write_csv(tmp_path / "a.csv", 3)
    (tmp_path / "b.json").write_text('{"i":1}\n{"i":2}\n', encoding="utf-8")
    mark(tmp_path)

    con = duckdb.connect()
    try:
        csv_source = FileSource(tmp_path, format="csv")
        csv_plan = csv_source.plan(None, csv_source.latest_offset(), BatchLimits())
        csv_view = csv_source.bind(con, csv_plan)
        assert con.execute(f'SELECT count(*) FROM "{csv_view}"').fetchone()[0] == 3

        json_source = FileSource(tmp_path, format="json")
        json_plan = json_source.plan(None, json_source.latest_offset(), BatchLimits())
        json_view = json_source.bind(con, json_plan)
        assert con.execute(f'SELECT count(*) FROM "{json_view}"').fetchone()[0] == 2
    finally:
        con.close()


# -- degenerate trees ------------------------------------------------------


def test_empty_directory_is_an_empty_batch_not_an_error(tmp_path):
    mark(tmp_path)
    source = FileSource(tmp_path)

    offset = source.latest_offset()
    assert consumed(offset) == {}
    plan = source.plan(None, offset, BatchLimits())
    assert plan.is_empty
    assert plan.has_more is False
    assert FileOffset.is_file_offset(plan.end)


def test_missing_directory_is_an_empty_batch_not_a_stack_trace(tmp_path):
    source = FileSource(tmp_path / "does" / "not" / "exist")

    offset = source.latest_offset()
    assert consumed(offset) == {}
    assert source.plan(None, offset, BatchLimits()).is_empty

    # ...and starts working the moment the tree appears, with no reconstruction.
    target = tmp_path / "does" / "not" / "exist"
    target.mkdir(parents=True)
    (target / "a.parquet").write_bytes(b"")  # discovery is by name, not content
    mark(target)
    assert sorted(consumed(source.latest_offset())) == ["a.parquet"]


def test_deleted_file_stays_consumed_and_is_not_replayed(
    tmp_path, writer, consumption
):
    """A file that vanishes stays consumed. ``CONTEXT.md`` 1.15 is why: the
    tempting optimisation is to forget entries whose file is gone, and a network
    mount that blinks then makes every entry look deleted and replays the tree.
    """
    write_parquet(writer, tmp_path / "a.parquet", 2)
    write_parquet(writer, tmp_path / "b.parquet", 2)
    mark(tmp_path)
    source = FileSource(tmp_path)

    first = consumption.take(source)
    assert sorted(relpaths(first)) == ["a.parquet", "b.parquet"]

    (tmp_path / "a.parquet").unlink()
    second = consumption.plan(source)
    assert second.is_empty
    assert "a.parquet" in consumption.consumed()


# -- offsets ---------------------------------------------------------------


def test_offset_json_round_trips_and_is_stable():
    offset = FileOffset.build(
        {
            "b/second.parquet": {"size": 2, "mtime_ns": 20},
            "a/first.parquet": {"size": 1, "mtime_ns": 10},
        }
    )
    text = encode_offset(offset)

    assert decode_offset(text) == offset
    assert encode_offset(decode_offset(text)) == text
    assert encode_offset(decode_offset(encode_offset(offset))) == encode_offset(offset)


def test_encoding_is_key_order_independent():
    a = {"kind": "file", "v": 1, "consumed": {"y": {"size": 1, "mtime_ns": 2}}}
    b = {"consumed": {"y": {"mtime_ns": 2, "size": 1}}, "v": 1, "kind": "file"}
    assert encode_offset(a) == encode_offset(b)


def test_offset_paths_use_forward_slashes(tmp_path, writer):
    write_parquet(writer, tmp_path / "day=1" / "part" / "a.parquet", 1)
    mark(tmp_path / "day=1" / "part")
    source = FileSource(tmp_path)

    offset = source.latest_offset()
    assert list(consumed(offset)) == ["day=1/part/a.parquet"]
    assert "\\" not in encode_offset(offset)

    plan = source.plan(None, offset, BatchLimits())
    assert relpaths(plan) == ["day=1/part/a.parquet"]
    assert "\\" not in json.dumps(plan.payload)


def test_offset_shape_is_exactly_as_specified(tmp_path, writer):
    write_parquet(writer, tmp_path / "a.parquet", 1)
    mark(tmp_path)
    offset = FileSource(tmp_path).latest_offset()

    assert offset["kind"] == "file"
    assert offset["v"] == 1
    assert set(offset) == {"kind", "v", "consumed"}
    assert set(offset["consumed"]["a.parquet"]) == {"size", "mtime_ns"}
    assert FileOffset.HIGH_WATER_KEY not in offset, "reserved for future pruning"


def test_non_serialisable_offset_is_a_duckstream_error():
    with pytest.raises(DuckstreamError) as excinfo:
        encode_offset({"kind": "file", "when": object()})
    assert "JSON" in str(excinfo.value)
    assert not isinstance(excinfo.value, TypeError)


@pytest.mark.parametrize("bad", [float("nan"), float("inf")])
def test_nan_and_infinity_are_refused(bad):
    with pytest.raises(DuckstreamError):
        encode_offset({"kind": "file", "x": bad})


def test_non_mapping_offset_is_refused():
    with pytest.raises(DuckstreamError):
        encode_offset(["not", "an", "offset"])


def test_bad_offset_text_is_a_duckstream_error():
    with pytest.raises(DuckstreamError):
        decode_offset("{not json")
    with pytest.raises(DuckstreamError):
        decode_offset("[1, 2, 3]")


def test_foreign_offset_kind_fails_loudly(tmp_path):
    """Silently treating an unreadable offset as empty would replay everything."""
    with pytest.raises(DuckstreamError) as excinfo:
        FileOffset.consumed({"kind": "kafka", "v": 1, "consumed": {}})
    assert "kafka" in str(excinfo.value)


def test_newer_offset_version_fails_loudly():
    with pytest.raises(DuckstreamError):
        FileOffset.consumed({"kind": "file", "v": 99, "consumed": {}})


def test_plan_rejects_a_foreign_end_offset(tmp_path):
    source = FileSource(tmp_path)
    with pytest.raises(DuckstreamError):
        source.plan(None, {"kind": "kafka", "v": 1}, BatchLimits())


# -- config round trip -----------------------------------------------------


def test_to_config_emits_only_non_defaults():
    assert FileSource("landing/").to_config() == {"type": "file", "path": "landing/"}


def test_to_config_round_trips_every_argument():
    source = FileSource(
        "landing/",
        marker="_DONE",
        settle_seconds=2.5,
        format="csv",
        pattern="*.txt",
        recursive=False,
        max_files_per_trigger=10,
        max_rows_per_trigger=1000,
    )
    config = source.to_config()
    assert config == {
        "type": "file",
        "path": "landing/",
        "marker": "_DONE",
        "settle_seconds": 2.5,
        "format": "csv",
        "pattern": "*.txt",
        "recursive": False,
        "max_files_per_trigger": 10,
        "max_rows_per_trigger": 1000,
    }

    rebuilt = FileSource(**{k: v for k, v in config.items() if k != "type"})
    assert rebuilt.to_config() == config
    assert rebuilt == source, "Model equality compares sources by value"


def test_to_config_keeps_marker_none_explicit():
    """``None`` differs from the default, so it must survive the round trip."""
    config = FileSource("landing/", marker=None).to_config()
    assert config["marker"] is None
    assert FileSource(**{k: v for k, v in config.items() if k != "type"}).marker is None


# -- pattern semantics -----------------------------------------------------
#
# `PurePath.match` requires `**` to consume at least one directory component,
# so `**/*.parquet` silently skips every file in the root. Silently reading a
# subset of the tree is the failure class this framework exists to design out,
# so the expected semantics are pinned here.


def test_doublestar_matches_zero_directories(tmp_path, writer):
    """``**/*.parquet`` must find root-level files, not only nested ones."""
    write_parquet(writer, tmp_path / "flat.parquet", 1)
    mark(tmp_path)
    flat = FileSource(tmp_path, pattern="**/*.parquet")

    assert sorted(consumed(flat.latest_offset())) == ["flat.parquet"]


def test_doublestar_matches_many_directories(tmp_path, writer):
    write_parquet(writer, tmp_path / "top.parquet", 1)
    write_parquet(writer, tmp_path / "s" / "mid.parquet", 1)
    write_parquet(writer, tmp_path / "s" / "d" / "deep.parquet", 1)
    for directory in (tmp_path, tmp_path / "s", tmp_path / "s" / "d"):
        mark(directory)
    source = FileSource(tmp_path, pattern="**/*.parquet")

    assert sorted(consumed(source.latest_offset())) == [
        "s/d/deep.parquet",
        "s/mid.parquet",
        "top.parquet",
    ]


def test_star_does_not_cross_a_directory_boundary(tmp_path, writer):
    write_parquet(writer, tmp_path / "sub" / "here.parquet", 1)
    write_parquet(writer, tmp_path / "sub" / "deep" / "there.parquet", 1)
    mark(tmp_path / "sub")
    mark(tmp_path / "sub" / "deep")
    source = FileSource(tmp_path, pattern="sub/*.parquet")

    assert sorted(consumed(source.latest_offset())) == ["sub/here.parquet"]


def test_leading_slash_anchors_the_pattern_at_the_root(tmp_path, writer):
    write_parquet(writer, tmp_path / "sub" / "a.parquet", 1)
    write_parquet(writer, tmp_path / "other" / "sub" / "b.parquet", 1)
    mark(tmp_path / "sub")
    mark(tmp_path / "other" / "sub")

    floating = FileSource(tmp_path, pattern="sub/*.parquet")
    assert sorted(consumed(floating.latest_offset())) == [
        "other/sub/b.parquet",
        "sub/a.parquet",
    ]

    anchored = FileSource(tmp_path, pattern="/sub/*.parquet")
    assert sorted(consumed(anchored.latest_offset())) == ["sub/a.parquet"]


def test_character_classes_and_question_marks(tmp_path, writer):
    for name in ("f1.parquet", "f2.parquet", "fx.parquet"):
        write_parquet(writer, tmp_path / name, 1)
    mark(tmp_path)

    digits = FileSource(tmp_path, pattern="f[0-9].parquet")
    assert sorted(consumed(digits.latest_offset())) == ["f1.parquet", "f2.parquet"]

    negated = FileSource(tmp_path, pattern="f[!0-9].parquet")
    assert sorted(consumed(negated.latest_offset())) == ["fx.parquet"]

    single = FileSource(tmp_path, pattern="f?.parquet")
    assert len(consumed(single.latest_offset())) == 3


def test_default_pattern_still_matches_at_any_depth(tmp_path, writer):
    """The default ``*.parquet`` matches from the right, as it always has."""
    write_parquet(writer, tmp_path / "deep" / "a.parquet", 1)
    mark(tmp_path / "deep")

    assert sorted(consumed(FileSource(tmp_path).latest_offset())) == ["deep/a.parquet"]


# -- case sensitivity ------------------------------------------------------

WINDOWS_ONLY = pytest.mark.skipif(
    os.name != "nt", reason="only a case-insensitive filesystem can do this rename"
)
POSIX_ONLY = pytest.mark.skipif(
    os.name == "nt", reason="Windows cannot hold two files differing only by case"
)


@WINDOWS_ONLY
def test_case_only_rename_is_not_a_second_file(tmp_path, writer, consumption):
    """On Windows ``A.parquet`` and ``a.parquet`` are one file, so re-reading it
    would double-count its rows.

    Run against both consumed-set shapes deliberately: the map folds case in
    Python and the table folds it into ``relpath_fold`` and joins on that
    column, so this is the one behaviour where the two shapes could most easily
    diverge without any other test noticing.
    """
    write_parquet(writer, tmp_path / "A.parquet", 3)
    mark(tmp_path)
    source = FileSource(tmp_path)

    first = consumption.take(source)
    assert relpaths(first) == ["A.parquet"]

    before = (tmp_path / "A.parquet").stat()
    (tmp_path / "A.parquet").rename(tmp_path / "a.parquet")
    after = (tmp_path / "a.parquet").stat()
    assert (after.st_size, after.st_mtime_ns) == (
        before.st_size,
        before.st_mtime_ns,
    ), "the rename must not have changed size or mtime, or this proves nothing"

    second = consumption.plan(source)
    assert second.is_empty, "a case-only rename is the same bytes, already read"
    assert consumption.consumed() == ["A.parquet"], (
        "nothing was read, so nothing may have been recorded"
    )


@WINDOWS_ONLY
def test_case_only_rename_with_new_content_is_replanned(
    tmp_path, writer, consumption
):
    """When the file genuinely changed it is re-planned, under either shape.

    What the two shapes do with the *old* spelling differs, and the difference
    is recorded rather than smoothed over. The map deletes it, because two keys
    for one file is how that map leaked entries it could never reclaim. The
    table keeps it, because per-trigger deletes cost ~26 ms of tombstone
    (``CONTEXT.md`` 1.10) to save one row -- and one row is all it is, since the
    fold join finds the file under either spelling.
    """
    write_parquet(writer, tmp_path / "A.parquet", 3)
    mark(tmp_path)
    source = FileSource(tmp_path)
    consumption.take(source)

    (tmp_path / "A.parquet").unlink()
    write_parquet(writer, tmp_path / "a.parquet", 40)
    touch_mtime(tmp_path / "a.parquet", delta_seconds=5)

    second = consumption.take(source)
    assert relpaths(second) == ["a.parquet"]
    assert consumption.consumed() == (
        ["a.parquet"] if consumption.shape == "map" else ["A.parquet", "a.parquet"]
    )
    assert consumption.plan(source).is_empty, (
        "whichever spellings are on record, the file is consumed exactly once"
    )


@POSIX_ONLY
def test_case_variants_are_distinct_files_on_posix(tmp_path, writer, consumption):
    """Folding case here would turn a real second file into a silent skip."""
    write_parquet(writer, tmp_path / "A.parquet", 3)
    mark(tmp_path)
    source = FileSource(tmp_path)

    first = consumption.take(source)
    assert relpaths(first) == ["A.parquet"]

    write_parquet(writer, tmp_path / "a.parquet", 4)
    mark(tmp_path)
    second = consumption.take(source)
    assert relpaths(second) == ["a.parquet"]
    assert consumption.consumed() == ["A.parquet", "a.parquet"]


def test_fold_follows_the_platform():
    from duckstream.offsets import CASE_INSENSITIVE_PATHS

    assert CASE_INSENSITIVE_PATHS is (os.name == "nt")
    assert (FileOffset.fold("A/b.parquet") == "a/b.parquet") is CASE_INSENSITIVE_PATHS


# -- known limits, pinned --------------------------------------------------


def test_rewrite_with_identical_size_and_mtime_is_not_detected(tmp_path):
    """**Known and accepted**: file identity is (size, mtime_ns), not a hash.

    A rewrite that restores both attributes exactly - a writer that resets the
    mtime, or a change smaller than the filesystem's mtime granularity that
    also preserves length - is invisible. Content hashing would close it and
    would cost a full read of every candidate file on every scan, which is the
    very thing the offset exists to avoid.

    This test exists so the behaviour is a recorded decision rather than an
    assumption someone later relies on. If duckstream ever gains a content
    digest, this is the test that should change.
    """
    # Written as raw bytes rather than parquet: this test never reads the file,
    # and parquet's footer makes an exactly-equal-length rewrite fiddly.
    path = tmp_path / "a.parquet"
    path.write_bytes(b"first-version")
    mark(tmp_path)
    source = FileSource(tmp_path)

    first = source.plan(None, source.latest_offset(), BatchLimits())
    assert relpaths(first) == ["a.parquet"]
    before = path.stat()

    # Different content, same byte length; then the mtime is restored exactly.
    path.write_bytes(b"SECOND-versio")
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    after = path.stat()
    assert (after.st_size, after.st_mtime_ns) == (before.st_size, before.st_mtime_ns)

    second = source.plan(first.end, source.latest_offset(), BatchLimits())
    assert second.is_empty, (
        "documented limit: a rewrite indistinguishable by size and mtime is "
        "not detected"
    )


# -- root resolution -------------------------------------------------------


def test_relative_path_is_pinned_at_construction(tmp_path, writer, monkeypatch):
    """Cron runs from an arbitrary cwd; the source must not follow it."""
    landing = tmp_path / "landing"
    write_parquet(writer, landing / "a.parquet", 2)
    mark(landing)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()

    monkeypatch.chdir(tmp_path)
    source = FileSource("landing")
    assert sorted(consumed(source.latest_offset())) == ["a.parquet"]

    monkeypatch.chdir(elsewhere)
    assert sorted(consumed(source.latest_offset())) == [
        "a.parquet"
    ], "the root was resolved when the source was built, not when it scanned"


def test_base_dir_anchors_a_relative_path(tmp_path, writer, monkeypatch):
    landing = tmp_path / "conf" / "landing"
    write_parquet(writer, landing / "a.parquet", 2)
    mark(landing)
    away = tmp_path / "away"
    away.mkdir()

    monkeypatch.chdir(away)
    source = FileSource("landing", base_dir=tmp_path / "conf")
    assert sorted(consumed(source.latest_offset())) == ["a.parquet"]


def test_base_dir_is_ignored_for_an_absolute_path(tmp_path, writer):
    write_parquet(writer, tmp_path / "a.parquet", 1)
    mark(tmp_path)
    source = FileSource(tmp_path, base_dir=tmp_path / "nowhere")

    assert sorted(consumed(source.latest_offset())) == ["a.parquet"]


def test_base_dir_is_not_emitted_by_to_config():
    """``path`` as written is what makes a config document portable."""
    source = FileSource("landing/", base_dir="/etc/duckstream")
    assert source.to_config() == {"type": "file", "path": "landing/"}
    assert source == FileSource(
        "landing/"
    ), "base_dir is a property of the document, not of the declaration"


def test_invalid_base_dir_is_refused():
    with pytest.raises(ConfigError):
        FileSource("landing", base_dir="")
    with pytest.raises(ConfigError):
        FileSource("landing", base_dir=17)


def test_config_relative_path_resolves_against_the_config_directory(
    tmp_path, writer, monkeypatch
):
    """A relative path in YAML means "next to the YAML", not "next to cron"."""
    from duckstream.config import load_config

    conf_dir = tmp_path / "conf"
    landing = conf_dir / "landing"
    write_parquet(writer, landing / "a.parquet", 2)
    mark(landing)

    config = conf_dir / "models.yaml"
    config.write_text(
        "catalog: 'ducklake:catalog.ducklake'\n"
        "models:\n"
        "  - name: m\n"
        "    source: {type: file, path: landing}\n"
        "    sink: {type: table, table: marts.m, mode: update}\n"
        "    aggregates: {n: 'count(*)'}\n"
        "    key: [sensor_id]\n",
        encoding="utf-8",
    )

    decoy = tmp_path / "decoy"
    (decoy / "landing").mkdir(parents=True)
    monkeypatch.chdir(decoy)

    source = load_config(config).models[0].source
    assert source.to_config()["path"] == "landing", "the config keeps path as written"
    assert sorted(consumed(source.latest_offset())) == [
        "a.parquet"
    ], "resolved against the config file's directory, not the cwd"


# -- malformed plans -------------------------------------------------------


def test_bind_refuses_a_non_list_file_payload(tmp_path):
    """A bare string would iterate per character and fail inside DuckDB."""
    from duckstream.protocols import BatchPlan

    source = FileSource(tmp_path)
    con = duckdb.connect()
    try:
        for files in ("a.parquet", 17, {"a.parquet": 1}):
            plan = BatchPlan(
                start=None,
                end=FileOffset.empty(),
                payload={"format": "parquet", "files": files},
                is_empty=False,
            )
            with pytest.raises(DuckstreamError) as excinfo:
                source.bind(con, plan)
            assert "files" in str(excinfo.value)
    finally:
        con.close()


def test_bind_refuses_non_string_paths(tmp_path):
    from duckstream.protocols import BatchPlan

    source = FileSource(tmp_path)
    con = duckdb.connect()
    try:
        plan = BatchPlan(
            start=None,
            end=FileOffset.empty(),
            payload={"format": "parquet", "files": ["ok.parquet", 3]},
            is_empty=False,
        )
        with pytest.raises(DuckstreamError) as excinfo:
            source.bind(con, plan)
        assert "files" in str(excinfo.value)
    finally:
        con.close()


def test_bind_refuses_an_unknown_format(tmp_path):
    from duckstream.protocols import BatchPlan

    source = FileSource(tmp_path)
    con = duckdb.connect()
    try:
        plan = BatchPlan(
            start=None,
            end=FileOffset.empty(),
            payload={"format": "avro", "files": ["a.avro"]},
            is_empty=False,
        )
        with pytest.raises(DuckstreamError) as excinfo:
            source.bind(con, plan)
        assert "avro" in str(excinfo.value)
    finally:
        con.close()


def test_a_malformed_glob_is_refused_at_construction():
    with pytest.raises(ConfigError) as excinfo:
        FileSource("landing", pattern="f[z-a].parquet")
    assert "glob" in str(excinfo.value)


# ==========================================================================
# The scan's fast path
#
# `_scan` builds each relative path by joining a prefix carried down the walk,
# instead of calling `FileOffset.relative_path` per file, and reads size and
# mtime off the `DirEntry` the walk already fetched. `CONTEXT.md` 1.20 has the
# measurement: 8.5x where a directory holds several files, 3x at one file per
# directory, which is duckstream's real shape.
#
# All of that is a pure optimisation, so what these tests defend is that it
# stayed one. The risk is not that it is slow, it is that it quietly disagrees
# with the general implementation on some path shape nobody thought about.
# ==========================================================================


def test_the_scan_builds_the_same_relative_paths_as_the_general_implementation(tmp_path):
    """The prefix join must equal `FileOffset.relative_path`, path for path.

    Asserted against a nested tree rather than a flat one, because a flat tree
    is the case where a prefix cannot be wrong -- it is empty.
    """
    root = tmp_path / "landing"
    for relative in ("a", "a/b", "a/b/c", "d"):
        directory = root / relative
        directory.mkdir(parents=True)
        (directory / "part.parquet").write_bytes(b"x")
        (directory / "_READY").write_text("", encoding="utf-8")
    (root).mkdir(exist_ok=True)
    (root / "top.parquet").write_bytes(b"x")
    (root / "_READY").write_text("", encoding="utf-8")

    source = FileSource(root.as_posix(), marker="_READY")
    scanned = source._scan()

    expected = {
        FileOffset.relative_path(root, path)
        for path in root.rglob("*.parquet")
    }
    assert set(scanned) == expected
    assert "top.parquet" in scanned, "a file in the root gets no leading slash"
    assert "a/b/c/part.parquet" in scanned, "a nested file keeps its whole prefix"
    assert not any(p.startswith("/") or p.startswith("./") for p in scanned)


def test_the_scan_reports_the_same_size_and_mtime_as_a_direct_stat(tmp_path):
    """`DirEntry.stat()` must agree with `Path.stat()`.

    File identity is `(path, size, mtime)`, so a `DirEntry` whose cached stat
    disagreed with a fresh one would make duckstream re-read consumed files or
    skip new ones -- and it would do it silently.
    """
    root = tmp_path / "landing"
    directory = root / "b1"
    directory.mkdir(parents=True)
    for index in range(3):
        (directory / f"p{index}.parquet").write_bytes(b"x" * (index + 1))
    (directory / "_READY").write_text("", encoding="utf-8")

    source = FileSource(root.as_posix(), marker="_READY")
    for relpath, entry in source._scan().items():
        stat = (root / relpath).stat()
        assert entry[FileOffset.SIZE_KEY] == stat.st_size, relpath
        assert entry[FileOffset.MTIME_KEY] == stat.st_mtime_ns, relpath


def test_readiness_is_the_same_answer_with_or_without_the_walk_s_entries(tmp_path):
    """`_is_ready` has two paths now; they must not disagree.

    The entry-based one saves a stat per directory. A directory is ready, not
    ready, or has no marker at all, and both paths have to say the same thing
    about each.
    """
    root = tmp_path / "landing"
    ready = root / "ready"
    ready.mkdir(parents=True)
    (ready / "p.parquet").write_bytes(b"x")
    (ready / "_READY").write_text("", encoding="utf-8")
    unmarked = root / "unmarked"
    unmarked.mkdir()
    (unmarked / "p.parquet").write_bytes(b"x")

    source = FileSource(root.as_posix(), marker="_READY")
    now = 10**19  # far in the future, so a settle delay would never pass
    for prefix, directory, entries in source._walk(root):
        with_entries = source._is_ready(directory, now, entries)
        without = source._is_ready(directory, now)
        assert with_entries == without, directory

    assert set(source._scan()) == {"ready/p.parquet"}


def test_an_unmarked_directory_still_gates_only_its_own_files(tmp_path):
    """The walk changed; the gating rule did not.

    A file is eligible when the marker sits beside it, never because an
    ancestor is marked -- so a marked parent must not drag an unmarked child in.
    """
    root = tmp_path / "landing"
    parent = root / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (parent / "p.parquet").write_bytes(b"x")
    (parent / "_READY").write_text("", encoding="utf-8")
    (child / "c.parquet").write_bytes(b"x")   # no marker of its own

    source = FileSource(root.as_posix(), marker="_READY")
    assert set(source._scan()) == {"parent/p.parquet"}


def test_a_non_recursive_source_reads_only_the_root(tmp_path):
    root = tmp_path / "landing"
    nested = root / "nested"
    nested.mkdir(parents=True)
    (root / "top.parquet").write_bytes(b"x")
    (root / "_READY").write_text("", encoding="utf-8")
    (nested / "deep.parquet").write_bytes(b"x")
    (nested / "_READY").write_text("", encoding="utf-8")

    source = FileSource(root.as_posix(), marker="_READY", recursive=False)
    assert set(source._scan()) == {"top.parquet"}


def _can_symlink_directories(tmp: Path) -> bool:
    """Windows refuses directory symlinks without a privilege most boxes lack."""
    probe = tmp / "_symlink_probe"
    probe.mkdir()
    try:
        os.symlink(probe, tmp / "_symlink_link", target_is_directory=True)
        return True
    except (OSError, NotImplementedError, AttributeError):
        return False


def test_the_walk_does_not_descend_into_symlinked_directories(tmp_path):
    """``os.walk`` does not follow directory symlinks, and neither may ``_walk``.

    This is the one semantic the hand-rolled walk could plausibly have lost, and
    losing it is not a crash: a symlink pointing back up its own tree makes the
    scan read the same files under a second set of paths, so every one of them
    is consumed twice under two different relative names. The anti-join cannot
    save that -- the paths genuinely differ.

    Skipped where a directory symlink cannot be created, which on Windows needs
    a privilege most boxes do not grant. The mutation audit carries a matching
    entry excused for the same reason, so this gap is declared rather than
    silent.
    """
    if not _can_symlink_directories(tmp_path):
        pytest.skip("creating a directory symlink needs a privilege this box lacks")

    root = tmp_path / "landing"
    real = root / "real"
    real.mkdir(parents=True)
    (real / "part.parquet").write_bytes(b"x")
    (real / "_READY").write_text("", encoding="utf-8")

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "other.parquet").write_bytes(b"x")
    (outside / "_READY").write_text("", encoding="utf-8")
    os.symlink(outside, root / "linked", target_is_directory=True)

    source = FileSource(root.as_posix(), marker="_READY")
    assert set(source._scan()) == {"real/part.parquet"}, (
        "the walk followed a directory symlink; os.walk does not"
    )
