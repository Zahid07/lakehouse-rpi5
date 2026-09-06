"""The tier-three planner: touched windows, chunk sizing, file selection.

:mod:`duckstream.recompute` decides *what* a recompute reads. The conformance
suite proves the numbers that come out; this file pins the decisions that get
there, because most of them fail in the direction that still produces a number.

The recurring theme is an asymmetry that ``CONTEXT.md`` 1.13 states once and
that shows up in three places here: **over-selecting is harmless and
under-selecting is silently wrong.** A chunk that reads a file it did not need
gets the right answer slowly; a chunk that misses one gets a plausible wrong
answer at full speed. Every default below leans the first way, deliberately.
"""

from __future__ import annotations

import datetime as dt

import pytest

from duckstream.consumed import UNKNOWN_MAX, UNKNOWN_MIN, ConsumedFile
from duckstream.recompute import plan_chunks

T = dt.datetime
BASE = T(2026, 6, 1, 0)


def hour(n: int) -> dt.datetime:
    return BASE + dt.timedelta(hours=n)


def measured(name: str, first: int, last: int, rows: int = 100) -> ConsumedFile:
    """A file whose range really is known: hours ``first``..``last`` inclusive."""
    return ConsumedFile(
        relpath=name,
        min_ts=hour(first),
        max_ts=hour(last) + dt.timedelta(minutes=59),
        n_rows=rows,
    )


def unmeasured(name: str, rows: int | None = None) -> ConsumedFile:
    """A file nobody has placed: the widest possible range, as stored."""
    return ConsumedFile(
        relpath=name, min_ts=UNKNOWN_MIN, max_ts=UNKNOWN_MAX, n_rows=rows
    )


def files(*candidates):
    """A ``files_for`` that ignores the span, so selection is tested here."""
    return lambda lo, hi: list(candidates)


# --------------------------------------------------------------------------
# Selection
# --------------------------------------------------------------------------


def test_only_the_files_overlapping_a_chunk_are_read():
    """The whole point of the index: untouched files are not opened.

    ``CONTEXT.md`` 1.13 measured ~0.1 ms per file *listed*, read or not, so a
    chunk that names a file it cannot possibly need has already paid for it.
    """
    chunks = plan_chunks(
        [hour(5)],
        "hour",
        files_for=files(measured("a", 0, 1), measured("b", 5, 5), measured("c", 9, 9)),
    )
    assert [c.relpaths for c in chunks] == [("b",)]


def test_a_file_touching_the_edge_of_a_window_is_included():
    """Half-open ``[lo, hi)``, and the boundary is where an off-by-one hides.

    A file ending exactly at the window's first instant *does* overlap it; one
    beginning exactly at the window's end does not.
    """
    ending_on_the_edge = ConsumedFile(
        relpath="ends", min_ts=hour(4), max_ts=hour(5), n_rows=10
    )
    starting_on_the_edge = ConsumedFile(
        relpath="starts", min_ts=hour(6), max_ts=hour(7), n_rows=10
    )
    chunks = plan_chunks(
        [hour(5)], "hour", files_for=files(ending_on_the_edge, starting_on_the_edge)
    )
    assert chunks[0].relpaths == ("ends",)


def test_an_unmeasured_file_is_selected_by_every_window():
    """The hint contract, stated as a test.

    A file stored at the sentinel range must be read by every recompute rather
    than by none. This is the assertion that fails if somebody encodes "unknown"
    as NULL -- see ``CONTEXT.md`` 1.17 for why that is tempting and what it
    costs.
    """
    for window in (hour(0), hour(5), hour(500)):
        chunks = plan_chunks(
            [window], "hour", files_for=files(unmeasured("mystery", 10))
        )
        assert chunks[0].relpaths == ("mystery",), window


def test_a_file_spanning_several_windows_is_read_by_each_of_them():
    wide = measured("wide", 0, 9, rows=900)
    for window in (hour(0), hour(4), hour(9)):
        chunks = plan_chunks([window], "hour", files_for=files(wide))
        assert chunks[0].relpaths == ("wide",), window


# --------------------------------------------------------------------------
# Chunk sizing
# --------------------------------------------------------------------------


def test_no_budget_means_one_chunk_over_everything():
    """The "unbounded" half of ``PLAN.md``'s chunked-equals-unchunked."""
    windows = [hour(n) for n in range(6)]
    chunks = plan_chunks(windows, "hour", files_for=files(measured("a", 0, 5)))
    assert len(chunks) == 1
    assert chunks[0].lo == hour(0)
    assert chunks[0].hi == hour(6)
    assert chunks[0].windows == 6


def test_a_budget_splits_the_span_by_estimated_rows():
    """Sized from rows, not from a window count.

    Four windows, one file each, 100 rows each, budget 250: three windows fit
    (300 > 250 fails, so two per chunk).
    """
    windows = [hour(n) for n in range(4)]
    candidates = [measured(f"f{n}", n, n, rows=100) for n in range(4)]
    chunks = plan_chunks(
        windows, "hour", files_for=files(*candidates), max_rows=250
    )
    assert [c.windows for c in chunks] == [2, 2]
    assert all(c.estimated_rows <= 250 for c in chunks)


def test_window_density_decides_the_split_not_the_window_count():
    """``PLAN.md``'s reason for sizing by rows: density varies enormously.

    Hour 0 is saturated and hours 1-3 are quiet. A fixed window count would cut
    them into equal pieces; a row budget gives the busy hour a chunk of its own.
    """
    windows = [hour(n) for n in range(4)]
    candidates = [
        measured("busy", 0, 0, rows=1000),
        measured("q1", 1, 1, rows=10),
        measured("q2", 2, 2, rows=10),
        measured("q3", 3, 3, rows=10),
    ]
    chunks = plan_chunks(
        windows, "hour", files_for=files(*candidates), max_rows=100
    )
    assert chunks[0].windows == 1, "the saturated hour is not packed with others"
    assert chunks[0].relpaths == ("busy",)
    assert sum(c.windows for c in chunks) == 4


def test_a_single_window_over_budget_still_becomes_its_own_chunk():
    """Never refuse: a batch that can never progress wedges the pipeline.

    The same call the file source makes about an oversized file, and for the
    same reason -- one oversized execution beats a stream that stops.
    """
    chunks = plan_chunks(
        [hour(0)], "hour", files_for=files(measured("huge", 0, 0, rows=10_000)),
        max_rows=10,
    )
    assert len(chunks) == 1
    assert chunks[0].windows == 1


def test_an_unknown_row_count_takes_the_smallest_chunk_not_the_largest():
    """Unknown must not read as zero.

    Treating an unmeasured file as contributing nothing would under-estimate,
    and under-estimating is the direction that runs out of memory -- so an
    unknown estimate packs one window at a time instead.
    """
    windows = [hour(n) for n in range(3)]
    chunks = plan_chunks(
        windows,
        "hour",
        files_for=files(unmeasured("no_count"), measured("a", 0, 2, rows=1)),
        max_rows=10_000,
    )
    assert [c.windows for c in chunks] == [1, 1, 1]
    assert all(c.estimated_rows is None for c in chunks)


def test_the_batch_s_own_files_are_counted_by_the_budget():
    """The index cannot see this batch yet, so the planner has to be told.

    Consumed rows are written in the same transaction as the output, so on a
    first batch -- and on a replay after a crash -- the index knows nothing
    about the files being read right now. Leaving them out of the estimate makes
    the row budget silently ignore *exactly* the rows guaranteed to be read: the
    planner sees no candidates, estimates zero, and hands back one unbounded
    chunk. Under-estimating is the direction that runs out of memory.
    """
    windows = [hour(n) for n in range(3)]
    own = [measured(f"new{n}", n, n, rows=100) for n in range(3)]

    # Nothing in the index at all -- the first batch a model ever runs.
    chunks = plan_chunks(
        windows, "hour", files_for=files(), own=own, max_rows=150
    )
    assert [c.windows for c in chunks] == [1, 1, 1], (
        "the budget was ignored for the batch's own rows"
    )
    assert all(c.estimated_rows == 100 for c in chunks)
    assert [c.relpaths for c in chunks] == [("new0",), ("new1",), ("new2",)]


def test_the_batch_s_own_files_are_always_read_even_when_unmeasured():
    """A file the bounds scan could not place still has to be recomputed."""
    chunks = plan_chunks(
        [hour(5)], "hour", files_for=files(), own=[unmeasured("mystery")]
    )
    assert chunks[0].relpaths == ("mystery",)
    assert chunks[0].estimated_rows is None


def test_a_file_in_both_the_index_and_the_batch_is_counted_once():
    """A replay re-plans files the index already holds.

    Counting it twice would double its contribution to the estimate and could
    list it twice for reading. The merge keeps the widest bounds and the largest
    row count, so it stays an over-estimate rather than becoming a wrong one.
    """
    indexed = measured("same", 0, 0, rows=100)
    replanned = measured("same", 0, 0, rows=120)
    chunks = plan_chunks(
        [hour(0)], "hour", files_for=files(indexed), own=[replanned]
    )
    assert chunks[0].relpaths == ("same",)
    assert chunks[0].estimated_rows == 120, "the larger of the two, counted once"


def test_windows_are_never_split_across_chunks():
    """Every row of a window is in exactly one execution.

    Two partial recomputes of one window would each clear and rewrite what the
    other wrote -- the tier-three bug in miniature.
    """
    windows = [hour(n) for n in range(5)]
    candidates = [measured(f"f{n}", n, n, rows=100) for n in range(5)]
    chunks = plan_chunks(
        windows, "hour", files_for=files(*candidates), max_rows=100
    )
    edges = [(c.lo, c.hi) for c in chunks]
    for lo, hi in edges:
        assert lo.minute == lo.second == 0
        assert (hi - lo) % dt.timedelta(hours=1) == dt.timedelta(0)
    assert sum(c.windows for c in chunks) == len(windows)


def test_a_gap_between_touched_windows_costs_nothing_to_span():
    """Adjacency is not required. There are no rows in the gap to read."""
    chunks = plan_chunks(
        [hour(0), hour(9)],
        "hour",
        files_for=files(measured("a", 0, 0, rows=1), measured("b", 9, 9, rows=1)),
    )
    assert len(chunks) == 1
    assert (chunks[0].lo, chunks[0].hi) == (hour(0), hour(10))


def test_no_touched_windows_plans_no_work():
    assert plan_chunks([], "hour", files_for=files(measured("a", 0, 0))) == []


@pytest.mark.parametrize("budget", [0, None])
def test_a_zero_or_absent_budget_is_unbounded(budget):
    windows = [hour(n) for n in range(4)]
    chunks = plan_chunks(
        windows, "hour", files_for=files(measured("a", 0, 3)), max_rows=budget
    )
    assert len(chunks) == 1


def test_windows_are_ordered_before_they_are_packed():
    """Chunk bounds must be time-ordered whatever order the windows arrive in."""
    chunks = plan_chunks(
        [hour(3), hour(1), hour(2)],
        "hour",
        files_for=files(*(measured(f"f{n}", n, n, rows=100) for n in (1, 2, 3))),
    )
    assert chunks[0].lo == hour(1)
    assert chunks[-1].hi == hour(4)
