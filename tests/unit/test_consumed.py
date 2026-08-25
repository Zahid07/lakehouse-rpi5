"""Unit tests for ``duckstream.consumed`` and the v2 file offset.

What this file is for, over and above ``test_file_source.py``: that file drives
the consumed set through a *source*, and does it against both shapes, which is
the right test of the contract. This one tests the things only the storage layer
can be wrong about — the exactness of the anti-join, the atomicity of the
migration, and the two places where a well-meant change would silently make
duckstream read a file twice.

Every test that means anything about growth runs at least two batches, per
``CONTEXT.md`` 1.5 and the discipline this suite already keeps.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from duckstream import consumed as consumed_module
from duckstream.consumed import (
    ENTRIES_KEY,
    UNKNOWN_MAX,
    UNKNOWN_MIN,
    MapIndex,
    entries_in,
)
from duckstream.errors import DuckstreamError
from duckstream.offsets import FileOffset
from duckstream.state import MemoryStateStore


# -- helpers ---------------------------------------------------------------


@pytest.fixture
def store():
    con = duckdb.connect()
    state = MemoryStateStore("duckstream")
    state.ensure(con)
    try:
        yield con, state
    finally:
        con.close()


def entry(size: int, mtime_ns: int) -> dict:
    return {"size": size, "mtime_ns": mtime_ns}


def rows_of(con, state, model: str = "m") -> list[tuple]:
    return con.execute(
        f"SELECT relpath, relpath_fold, \"size\", mtime_ns, batch_id "
        f"FROM {state.consumed_files_table} WHERE model_name = ? "
        f"ORDER BY batch_id, relpath",
        [model],
    ).fetchall()


# -- the offset shapes -----------------------------------------------------


def test_a_v2_offset_refuses_to_pretend_it_has_a_map():
    """The single most dangerous silent answer this change could have given.

    ``consumed()`` returning ``{}`` for a v2 offset would read as "this model
    has consumed nothing", which replays the entire landing tree and folds every
    row into the mart a second time. It has to be an error.
    """
    with pytest.raises(DuckstreamError) as excinfo:
        FileOffset.consumed(FileOffset.rows(41_230))
    message = str(excinfo.value)
    assert "consumed_files" in message, "the error must say where the set went"
    assert "CONTEXT.md" in message, (
        "and cite the measurement, so nobody re-litigates it from the traceback"
    )


def test_entry_count_reads_both_shapes():
    assert FileOffset.entry_count(None) == 0
    assert FileOffset.entry_count(FileOffset.rows(7)) == 7
    assert (
        FileOffset.entry_count(
            FileOffset.build({"a": entry(1, 2), "b": entry(3, 4)})
        )
        == 2
    )


def test_a_missing_offset_is_the_current_shape_not_the_old_one():
    """A model that has never committed must not enter the migration path."""
    assert FileOffset.version_of(None) == FileOffset.ROWS_VERSION


def test_both_shapes_are_still_readable_and_a_newer_one_is_refused():
    assert FileOffset.is_file_offset(FileOffset.build({}))
    assert FileOffset.is_file_offset(FileOffset.rows(0))
    with pytest.raises(DuckstreamError, match="newer than this duckstream"):
        FileOffset.version_of({"kind": "file", "v": FileOffset.VERSION + 1})


def test_the_reserved_high_water_key_is_still_unused():
    """``CONTEXT.md`` 1.15 rejected it: a file arriving with an older mtime
    would be skipped silently. Rows replaced it; the key must not creep back."""
    assert FileOffset.HIGH_WATER_KEY not in FileOffset.rows(3)
    assert FileOffset.HIGH_WATER_KEY not in FileOffset.build({"a": entry(1, 2)})


def test_the_offset_no_longer_grows_with_what_has_been_consumed():
    """The whole point, stated as an assertion rather than a measurement.

    1.15's number was 45.7 MB at 525,600 files, rewritten every trigger. The
    encoded v2 offset is the same handful of bytes at every scale, and the only
    thing that grows is a decimal integer.
    """
    from duckstream.offsets import encode_offset

    small = len(encode_offset(FileOffset.rows(1)))
    huge = len(encode_offset(FileOffset.rows(525_600)))
    assert huge - small <= 6, (
        f"a v2 offset grew by {huge - small} bytes between 1 and 525,600 files"
    )
    assert huge < 60


# -- the anti-join ---------------------------------------------------------


def test_a_second_batch_sees_only_what_the_first_did_not_take(store):
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    scan = {"a": entry(1, 100), "b": entry(2, 200)}

    assert sorted(index.unconsumed(scan)) == ["a", "b"]
    index.append(1, {"a": scan["a"]})
    assert index.unconsumed(scan) == ["b"]
    index.append(2, {"b": scan["b"]})
    assert index.unconsumed(scan) == []


def test_identity_is_path_and_size_and_mtime_together(store):
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    index.append(1, {"a": entry(10, 100)})

    assert index.unconsumed({"a": entry(10, 100)}) == []
    assert index.unconsumed({"a": entry(11, 100)}) == ["a"], "size moved"
    assert index.unconsumed({"a": entry(10, 101)}) == ["a"], "mtime moved"


def test_an_empty_scan_asks_the_database_nothing(store):
    """``CONTEXT.md`` 1.8: an idle trigger costs ~1.3 ms and 1.11 measured a
    state read at ~10 ms. A quiet stream must not pay one to be told nothing."""
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    calls = []
    original = con.execute
    index.con = type(
        "Recording",
        (),
        {
            "execute": lambda _self, sql, *a, **k: (
                calls.append(sql) or original(sql, *a, **k)
            ),
            "register": lambda _self, *a: None,
            "unregister": lambda _self, *a: None,
        },
    )()
    assert index.unconsumed({}) == []
    assert calls == []


def test_the_mtime_window_narrows_without_ever_excluding_a_match(store):
    """The narrowing is a deduction, not a heuristic, and this pins it.

    The join matches ``mtime_ns`` by equality, so a consumed row outside the
    scan's own mtime span cannot match one — which is what makes it safe to
    restrict the probe to that span. The failure it must never have is the
    silent one: narrowing past a row that *would* have matched, so an
    already-consumed file is read a second time.
    """
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    # A long history, then a scan that touches only its oldest and newest files.
    history = {f"f{i}": entry(i, 1_000 + i) for i in range(200)}
    index.append(1, history)

    for scan in (
        {"f0": history["f0"]},
        {"f199": history["f199"]},
        {"f0": history["f0"], "f199": history["f199"]},
        {"f7": history["f7"], "f8": history["f8"]},
    ):
        assert index.unconsumed(scan) == [], (
            f"the mtime window excluded a row that matches: {sorted(scan)}"
        )

    # And a genuinely new file inside the same span is still found.
    assert index.unconsumed({"fresh": entry(1, 1_100)}) == ["fresh"]


def test_one_model_never_sees_another_model_s_files(store):
    """Two models over one landing tree are two positions, not one.

    Both readers are asserted, not just the anti-join: ``count()`` feeds the
    number ``status`` reports, and a count that quietly totalled every model
    would tell an operator a model had consumed files it has never seen.
    """
    con, state = store
    mine = state.consumed_files.index_for(con, "mine")
    theirs = state.consumed_files.index_for(con, "theirs")
    theirs.append(1, {"a": entry(1, 100), "b": entry(2, 200)})
    assert mine.unconsumed({"a": entry(1, 100)}) == ["a"]
    assert theirs.unconsumed({"a": entry(1, 100)}) == []
    assert mine.count() == 0
    assert theirs.count() == 2


def test_the_fold_column_is_written_on_every_platform(store):
    """Not conditionally, which is what makes a catalog portable both ways.

    Folding at write time only on Windows would make a Linux-written table
    re-read every uppercase path when it is opened on Windows.
    """
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    index.append(1, {"Sub/A.PARQUET": entry(1, 100)})
    (row,) = rows_of(con, state)
    assert row[0] == "Sub/A.PARQUET", "the path is stored as written"
    assert row[1] == "sub/a.parquet", "and folded alongside it, always"


def test_a_rolled_back_batch_leaves_no_row(store):
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    con.execute("BEGIN")
    index.append(1, {"a": entry(1, 100)})
    con.execute("ROLLBACK")
    assert rows_of(con, state) == []
    assert index.unconsumed({"a": entry(1, 100)}) == ["a"]


def test_the_probe_relation_does_not_leak_onto_the_connection(store):
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    before = con.execute("SELECT count(*) FROM duckdb_views()").fetchone()[0]
    for i in range(5):
        index.unconsumed({f"f{i}": entry(1, 100 + i)})
        index.append(i + 1, {f"f{i}": entry(1, 100 + i)})
    after = con.execute("SELECT count(*) FROM duckdb_views()").fetchone()[0]
    assert after == before


# -- what a batch declares -------------------------------------------------


def test_a_batch_records_exactly_what_its_payload_declares(store):
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    payload = {"relpaths": ["a"], ENTRIES_KEY: {"a": entry(1, 100)}}
    assert index.record(3, payload) == 1
    assert [r[0] for r in rows_of(con, state)] == ["a"]
    assert rows_of(con, state)[0][4] == 3, "the batch id must be recorded"


def test_a_payload_that_declares_nothing_records_nothing(store):
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    assert index.record(1, {"relpaths": []}) == 0
    assert index.record(1, None) == 0
    assert rows_of(con, state) == []


def test_a_malformed_entries_payload_is_refused_rather_than_ignored():
    with pytest.raises(DuckstreamError, match=ENTRIES_KEY):
        entries_in({ENTRIES_KEY: ["a", "b"]})


# -- the map shape, still ---------------------------------------------------


def test_the_map_index_answers_the_same_questions():
    index = MapIndex({"a": entry(1, 100)})
    assert index.unconsumed({"a": entry(1, 100), "b": entry(2, 200)}) == ["b"]
    end = index.end_offset(FileOffset.build({"a": entry(1, 100)}), {"b": entry(2, 200)})
    assert sorted(FileOffset.consumed(end)) == ["a", "b"]
    assert index.record(1, {ENTRIES_KEY: {"b": entry(2, 200)}}) == 0


def test_the_map_index_refuses_a_v2_offset():
    with pytest.raises(DuckstreamError, match="consumed_files"):
        MapIndex.from_offset(FileOffset.rows(3))


def test_the_table_index_carries_the_count_forward_without_reading_it(store):
    """``CONTEXT.md`` 1.10 and 1.11: do not re-read state you just wrote."""
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    first = index.end_offset(None, {"a": entry(1, 100)})
    assert first == FileOffset.rows(1)
    second = index.end_offset(first, {"b": entry(2, 200), "c": entry(3, 300)})
    assert second == FileOffset.rows(3)
    assert index.count() == 0, "end_offset must not have touched the table"


def test_the_count_advances_on_every_committed_batch(store):
    """What the engine's stalled-loop guard compares. A batch that committed
    without it moving would make the next batch re-read the same files."""
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    offset = None
    seen = set()
    for i in range(3):
        offset = index.end_offset(offset, {f"f{i}": entry(1, 100 + i)})
        assert offset[ENTRIES_KEY] not in seen
        seen.add(offset[ENTRIES_KEY])
    # A rewrite is a new record, not a replacement, so the count still moves.
    offset = index.end_offset(offset, {"f0": entry(2, 100)})
    assert offset[ENTRIES_KEY] == 4


# -- migration --------------------------------------------------------------


def test_a_v1_offset_is_adopted_atomically(store):
    con, state = store
    entries = {"a": entry(1, 100), "b": entry(2, 200)}
    adopted = state.adopt_consumed(con, "m", entries, FileOffset.rows(len(entries)))

    assert adopted == 2
    assert [r[0] for r in rows_of(con, state)] == ["a", "b"]
    assert state.load_offset(con, "m") == FileOffset.rows(2)
    index = state.consumed_files.index_for(con, "m")
    assert index.unconsumed(entries) == [], "adopted files must read as consumed"


def test_a_failed_adoption_leaves_the_old_shape_working(store):
    con, state = store
    old = FileOffset.build({"a": entry(1, 100)})
    state.begin(con)
    state.commit(con, {"m": old}, {})

    class Boom:
        def __getitem__(self, key):
            raise RuntimeError("no")

    with pytest.raises(Exception):
        state.adopt_consumed(con, "m", {"a": Boom()}, FileOffset.rows(1))

    assert rows_of(con, state) == [], "a half-migrated position is not a position"
    assert state.load_offset(con, "m") == old


def test_the_source_asks_for_migration_only_when_there_is_one(tmp_path):
    from duckstream.sources.files import FileSource

    source = FileSource(tmp_path)
    assert source.migrate_offset(None) is None
    assert source.migrate_offset(FileOffset.rows(9)) is None

    outcome = source.migrate_offset(FileOffset.build({"a": entry(1, 100)}))
    assert outcome is not None
    new_offset, entries = outcome
    assert new_offset == FileOffset.rows(1)
    assert entries == {"a": entry(1, 100)}


# -- what must never be pruned ----------------------------------------------


def test_prune_never_touches_the_consumed_files(store):
    """These rows are the position, not a history of positions.

    Every other table here keeps one row per batch and only the newest is read,
    which is what makes dropping the rest safe. Prune a consumed-file row and
    duckstream forgets it read that file, reads it again, and folds its rows
    into the mart a second time — the section 4 bug class, produced by the
    maintenance meant to prevent bloat.
    """
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    for i in range(5):
        state.begin(con)
        index.append(i + 1, {f"f{i}": entry(1, 100 + i)})
        state.commit(con, {"m": FileOffset.rows(i + 1)}, {})

    deleted = state.prune(con, "m", keep_last=1)

    assert "consumed_files" not in deleted
    assert len(rows_of(con, state)) == 5, (
        f"prune removed consumed-file rows: {deleted}"
    )
    assert index.unconsumed({f"f{i}": entry(1, 100 + i) for i in range(5)}) == [], (
        "a pruned catalog must not re-plan files it already read"
    )


def test_migrating_a_failing_model_does_not_refund_its_attempts(store):
    """A representation change must not overturn a failure decision.

    The migration appends an offset row, and an offset row carries the retry
    state. Written carelessly it hands a stuck deployment a clean budget the
    moment it is upgraded -- so a model three attempts into quarantining a
    corrupt file would start over, and could go on starting over on every
    release. The rows move; the verdict does not.
    """
    con, state = store
    state.begin(con)
    state.commit(con, {"m": FileOffset.build({"a": entry(1, 100)})}, {})
    failing = state.load_position(con, "m")
    for _ in range(3):
        state.record_failure(
            con, "m", state.next_batch_id(con, "m"), failing, RuntimeError("bad file")
        )
        failing = state.load_position(con, "m")
    assert failing.attempt == 3

    state.adopt_consumed(con, "m", {"a": entry(1, 100)}, FileOffset.rows(1), failing)

    after = state.load_position(con, "m")
    assert after.offset == FileOffset.rows(1), "the position moved to the new shape"
    assert after.attempt == 3, "and took its attempt count with it"
    assert after.error == failing.error
    assert after.failed_at == failing.failed_at


# -- what the mutation audit found the first pass missing --------------------


def test_identity_holds_across_a_scan_that_spans_several_mtimes(store):
    """The mtime half of file identity, tested so that it is actually tested.

    The obvious version of this assertion uses a one-file scan -- and a one-file
    scan cannot test it. The probe is narrowed to
    ``BETWEEN min(scan mtime) AND max(scan mtime)``, which for one file is
    ``BETWEEN t AND t``: exactly the equality it is supposed to be independent
    of. Deleting ``c.mtime_ns = s.mtime_ns`` from the join therefore changes
    nothing a single-file test can see, and the first version of
    ``test_identity_is_path_and_size_and_mtime_together`` above did not see it.
    The mutation audit found that; review did not.

    So: a scan spanning two mtimes, and a file rewritten to a *different* mtime
    inside that span while keeping its size. Without the equality the stale row
    matches through the window alone, and the rewritten file is silently never
    read again.
    """
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    index.append(1, {"b": entry(10, 200)})

    scan = {"a": entry(1, 100), "b": entry(10, 300)}
    assert sorted(index.unconsumed(scan)) == ["a", "b"], (
        "b was consumed at mtime 200 and rewritten at 300 keeping its size; the "
        "probe window spans 100..300 and contains 200, so only the mtime "
        "equality keeps this right"
    )


def test_a_rewrite_backwards_in_time_inside_the_window_is_still_a_rewrite(store):
    """The same hole from the other side: a file whose mtime moved earlier."""
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    index.append(1, {"b": entry(10, 300)})
    scan = {"a": entry(1, 100), "b": entry(10, 200)}
    assert sorted(index.unconsumed(scan)) == ["a", "b"]


def test_the_fold_column_does_not_depend_on_the_writing_platform(store, monkeypatch):
    """Written folded on every platform, which is what makes a catalog portable.

    A catalog is written on one machine and may be read on another -- a Windows
    dev box and the Pi it deploys to is the case this project actually has.
    Folding at *write* time only where it is *read* would leave a Linux-written
    table holding unfolded keys, and every uppercase path would then be re-read
    the first time that table was opened on Windows.

    Testable from one platform only because the flag can be moved: this table is
    written from a dict, not from the filesystem, so nothing here needs a
    case-sensitive volume to exist. Without that, the mutation naming this
    defect is inert on Windows and the suite reports coverage it does not have.
    """
    con, state = store
    monkeypatch.setattr(consumed_module, "CASE_INSENSITIVE_PATHS", False)
    index = state.consumed_files.index_for(con, "m")
    index.append(1, {"Sub/A.PARQUET": entry(1, 100)})
    (row,) = rows_of(con, state)
    assert row[0] == "Sub/A.PARQUET", "the path is stored as written"
    assert row[1] == "sub/a.parquet", (
        "the fold column must be written folded even where nothing reads it"
    )


def test_the_join_column_follows_the_reading_platform(store, monkeypatch):
    """POSIX keeps case-variant paths apart; Windows folds them together.

    Both halves are asserted whichever platform is running, for the same reason
    as above: the anti-join is handed a dict of paths, so two spellings a
    Windows *filesystem* could never hold at once are still two dict keys.
    Getting this backwards is a silent skip on POSIX -- a real second file
    treated as already read -- or a silent double-read on Windows.
    """
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    index.append(1, {"A.parquet": entry(3, 100)})
    scan = {"A.parquet": entry(3, 100), "a.parquet": entry(3, 100)}

    monkeypatch.setattr(consumed_module, "CASE_INSENSITIVE_PATHS", False)
    assert index.unconsumed(scan) == ["a.parquet"], (
        "on a case-sensitive filesystem these are two files and one is unread"
    )

    monkeypatch.setattr(consumed_module, "CASE_INSENSITIVE_PATHS", True)
    assert index.unconsumed(scan) == [], (
        "on a case-insensitive filesystem these are one file, already read"
    )


def test_a_checkpoint_that_outruns_its_rows_is_refused(store):
    """The count advancing is not evidence that anything was recorded.

    This is the hole the audit opened up. The offset carries a count and the
    table carries the identities, and only the identities stop the next trigger
    re-planning the batch. A source that plans files and then declares none
    writes no rows, advances the count anyway, and is handed the same files
    again on every trigger for ever -- re-folding them into the mart each time.
    The engine's stalled-loop guard cannot see it, because that guard watches
    the checkpoint move and the checkpoint does move.

    So the two are tied together at the point of writing rather than trusted to
    agree.
    """
    con, state = store
    index = state.consumed_files.index_for(con, "m")

    with pytest.raises(DuckstreamError, match="the checkpoint says"):
        index.record(
            1,
            {"relpaths": ["a"], ENTRIES_KEY: {}},
            start=FileOffset.rows(0),
            end=FileOffset.rows(1),
        )

    # And the other direction: rows written that the checkpoint does not count.
    with pytest.raises(DuckstreamError, match="the checkpoint says"):
        index.record(
            2,
            {"relpaths": ["a"], ENTRIES_KEY: {"a": entry(1, 100)}},
            start=FileOffset.rows(0),
            end=FileOffset.rows(0),
        )


def test_a_matching_checkpoint_passes_the_check(store):
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    written = index.record(
        1,
        {"relpaths": ["a", "b"],
         ENTRIES_KEY: {"a": entry(1, 100), "b": entry(2, 200)}},
        start=FileOffset.rows(7),
        end=FileOffset.rows(9),
    )
    assert written == 2
    assert len(rows_of(con, state)) == 2


def test_a_source_that_drops_the_index_fails_loudly_rather_than_replaying():
    """The hazard built into signature-driven injection, made loud.

    A source that *wraps* a file source but does not itself declare ``consumed``
    is called without the index, and forwards the call without it too. That
    source is then planning against a v2 checkpoint with no set inside it. The
    only acceptable outcome is an error: answering "nothing consumed" would
    replay the whole landing tree and fold every row a second time.
    """
    from duckstream.sources.files import FileSource

    source = FileSource("nowhere")
    with pytest.raises(DuckstreamError, match="consumed_files"):
        source.plan(FileOffset.rows(41_230), FileOffset.build({}), None)


def test_the_engine_refuses_a_batch_that_recorded_nothing():
    """The guard that turns an infinite loop into a loud failure.

    Reachable only from inside the batch lifecycle, so it is tested where it
    lives rather than through a contrived source. That is not a shortcut: the
    audit's own rule is that a check nothing tests is a check somebody removes
    later, and this one survived its first mutation precisely because nothing
    exercised it.
    """
    from duckstream.engine import Engine

    model = type("M", (), {"name": "m"})()
    # Nothing recorded, and an index that should have recorded something.
    with pytest.raises(DuckstreamError, match="without recording which files"):
        Engine._require_recorded(model, 7, object(), None)

    # The two ways it must stay quiet: no index at all, and a real count --
    # including zero, which an empty batch legitimately reports.
    Engine._require_recorded(model, 7, None, None)
    Engine._require_recorded(model, 7, object(), 0)
    Engine._require_recorded(model, 7, object(), 3)


# --------------------------------------------------------------------------
# The file -> time-range index
#
# A hint, never truth (`CONTEXT.md` 1.13). Everything below is about the one
# direction that is allowed to be wrong: it may select a file a recompute did
# not need, and it may never fail to select one it did. `CONTEXT.md` 1.17 is
# why "unknown" is stored as the widest range rather than as NULL, and the
# first two tests here are what stop that being tidied away.
# --------------------------------------------------------------------------


def bounds_of(con, state, model: str = "m") -> dict:
    return {
        row[0]: (row[1], row[2], row[3])
        for row in con.execute(
            f"SELECT relpath, min_ts, max_ts, n_rows "
            f"FROM {state.consumed_files_table} WHERE model_name = ?",
            [model],
        ).fetchall()
    }


def test_a_file_with_no_measured_bounds_is_stored_at_the_widest_range(store):
    """Not NULL. ``CONTEXT.md`` 1.17 measured what NULL costs.

    NULL fails ``max_ts >= lo AND min_ts < hi``, so a row stored that way would
    be silently dropped from every recompute — and expressing the fallback as
    ``IS NULL OR ...`` instead puts back the O(files-ever-consumed) scan the
    index exists to remove (117.5 ms against 2,160 files, versus 12.1 ms).
    """
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    index.append(1, {"a.parquet": entry(1, 10)})

    stored = bounds_of(con, state)["a.parquet"]
    assert stored[0] == UNKNOWN_MIN
    assert stored[1] == UNKNOWN_MAX
    assert stored[2] is None, "an unknown row count is genuinely unknown"


def test_an_unmeasured_file_is_selected_by_every_window(store):
    """The hint contract, proved through the SQL rather than in Python."""
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    index.append(1, {"mystery.parquet": entry(1, 10)})

    for lo in (dt.datetime(1999, 1, 1), dt.datetime(2026, 6, 1), dt.datetime(2200, 1, 1)):
        picked = index.overlapping(lo, lo + dt.timedelta(hours=1))
        assert [f.relpath for f in picked] == ["mystery.parquet"], lo


def test_a_measured_file_is_selected_only_by_windows_it_can_hold_a_row_in(store):
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    index.append(
        1,
        {"a.parquet": entry(1, 10), "b.parquet": entry(2, 20)},
        bounds={
            "a.parquet": (dt.datetime(2026, 6, 1, 0), dt.datetime(2026, 6, 1, 0, 59), 50),
            "b.parquet": (dt.datetime(2026, 6, 1, 5), dt.datetime(2026, 6, 1, 5, 59), 70),
        },
    )

    first = index.overlapping(dt.datetime(2026, 6, 1, 0), dt.datetime(2026, 6, 1, 1))
    assert [f.relpath for f in first] == ["a.parquet"]
    assert first[0].n_rows == 50

    sixth = index.overlapping(dt.datetime(2026, 6, 1, 5), dt.datetime(2026, 6, 1, 6))
    assert [f.relpath for f in sixth] == ["b.parquet"]

    between = index.overlapping(dt.datetime(2026, 6, 1, 3), dt.datetime(2026, 6, 1, 4))
    assert between == [], "neither file can hold a row in an hour they both miss"


def test_the_selection_is_half_open_at_the_top(store):
    """``[lo, hi)``: a file starting exactly at ``hi`` belongs to the next window.

    Two files, so this is not the degenerate single-row case that trap 16
    warns about — with one row the boundary test cannot be told apart from an
    equality.
    """
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    index.append(
        1,
        {"ends.parquet": entry(1, 10), "starts.parquet": entry(2, 20)},
        bounds={
            "ends.parquet": (dt.datetime(2026, 6, 1, 4), dt.datetime(2026, 6, 1, 5), 1),
            "starts.parquet": (dt.datetime(2026, 6, 1, 6), dt.datetime(2026, 6, 1, 7), 1),
        },
    )
    picked = index.overlapping(dt.datetime(2026, 6, 1, 5), dt.datetime(2026, 6, 1, 6))
    assert [f.relpath for f in picked] == ["ends.parquet"]


def test_a_half_measured_file_is_widened_rather_than_guessed(store):
    """A known maximum says nothing about where the file starts.

    Filling the missing half from the half that is known would be inventing a
    bound, and an invented bound can exclude.
    """
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    index.append(
        1,
        {"half.parquet": entry(1, 10)},
        bounds={"half.parquet": (None, dt.datetime(2026, 6, 1, 5), 3)},
    )
    stored = bounds_of(con, state)["half.parquet"]
    assert stored[0] == UNKNOWN_MIN
    assert stored[1] == dt.datetime(2026, 6, 1, 5)

    # ...and it is still selected by an hour long before its known maximum.
    picked = index.overlapping(dt.datetime(1999, 1, 1), dt.datetime(1999, 1, 1, 1))
    assert [f.relpath for f in picked] == ["half.parquet"]


def test_a_rewritten_file_is_selected_once_at_its_widest(store):
    """Two rows, one path: the recompute reads the file once.

    A file rewritten in place is consumed again at its new identity, so the
    table holds both. The selection returns the widest bounds and the largest
    row count among them, which keeps it an over-estimate in both directions --
    the safe one for a hint.
    """
    con, state = store
    index = state.consumed_files.index_for(con, "m")
    index.append(
        1,
        {"same.parquet": entry(1, 10)},
        bounds={"same.parquet": (dt.datetime(2026, 6, 1, 1), dt.datetime(2026, 6, 1, 2), 10)},
    )
    index.append(
        2,
        {"same.parquet": entry(2, 20)},
        bounds={"same.parquet": (dt.datetime(2026, 6, 1, 3), dt.datetime(2026, 6, 1, 4), 90)},
    )

    picked = index.overlapping(dt.datetime(2026, 6, 1, 0), dt.datetime(2026, 6, 1, 9))
    assert [f.relpath for f in picked] == ["same.parquet"], "read once, not twice"
    assert picked[0].min_ts == dt.datetime(2026, 6, 1, 1)
    assert picked[0].max_ts == dt.datetime(2026, 6, 1, 4)
    assert picked[0].n_rows == 90


def test_one_model_never_sees_another_model_s_index(store):
    con, state = store
    window = (dt.datetime(2026, 6, 1, 0), dt.datetime(2026, 6, 1, 1))
    inside = {"x.parquet": (window[0], window[0] + dt.timedelta(minutes=1), 5)}

    state.consumed_files.index_for(con, "m").append(
        1, {"x.parquet": entry(1, 10)}, bounds=inside
    )
    other = state.consumed_files.index_for(con, "other")
    assert other.overlapping(*window) == []
