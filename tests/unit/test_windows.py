"""Tumbling-window arithmetic, and the agreement between its Python and SQL halves.

:mod:`duckstream.windows` computes the same boundary three times over in two
languages -- ``date_trunc`` in the SQL that derives ``window_ts``, Python when
the seal cutoff is worked out, and ``date_trunc`` again in the predicate that
decides whether a row's window has already closed. If any two of those disagree
by a microsecond the result is not a crash but a wrong answer: a window that
seals one row early, or a late row folded into a window that had been declared
complete.

So the load-bearing test here is :func:`test_python_and_sql_agree_on_every_boundary`,
which asks DuckDB itself rather than asserting that two hand-written truncations
look similar.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from duckstream.errors import DuckstreamError
from duckstream.windows import (
    GRAIN_INTERVALS,
    WINDOW_COLUMN,
    floor_to_grain,
    grain_interval,
    seal_cutoff,
    sealed_predicate,
    window_end,
    window_expression,
)

GRAINS = tuple(GRAIN_INTERVALS)

#: Timestamps chosen to sit on, just before and just after a boundary, plus a
#: sub-second value -- the places two truncations are most likely to part ways.
MOMENTS = [
    dt.datetime(2026, 8, 1, 0, 0, 0),
    dt.datetime(2026, 8, 1, 0, 0, 0, 1),
    dt.datetime(2026, 8, 1, 12, 0, 0),
    dt.datetime(2026, 8, 1, 12, 59, 59, 999999),
    dt.datetime(2026, 8, 1, 13, 0, 0),
    dt.datetime(2026, 8, 1, 23, 59, 59, 999999),
    dt.datetime(2026, 2, 28, 23, 59, 59),
    dt.datetime(2024, 2, 29, 6, 30, 15, 500000),
    dt.datetime(2026, 12, 31, 23, 59, 59, 999999),
]


@pytest.fixture(scope="module")
def con():
    connection = duckdb.connect()
    try:
        yield connection
    finally:
        connection.close()


# --------------------------------------------------------------------------
# The agreement
# --------------------------------------------------------------------------


@pytest.mark.parametrize("grain", GRAINS)
@pytest.mark.parametrize("moment", MOMENTS, ids=lambda m: m.isoformat())
def test_python_and_sql_agree_on_every_boundary(con, grain, moment):
    """``floor_to_grain`` equals DuckDB's ``date_trunc``, exactly.

    Asked of DuckDB rather than asserted from a second Python expression: the
    SQL half is the one that actually assigns rows to windows, so it is the one
    the Python half has to be checked against.
    """
    expression = window_expression(grain, "ts")
    actual = con.execute(
        f"SELECT {expression} FROM (SELECT ? AS ts)", [moment]
    ).fetchone()[0]
    assert actual == floor_to_grain(moment, grain)


@pytest.mark.parametrize("grain", GRAINS)
def test_a_window_contains_its_start_and_excludes_its_end(grain):
    """Windows are half-open, which is what makes them tile without overlap."""
    start = floor_to_grain(dt.datetime(2026, 8, 1, 13, 27, 42), grain)
    end = window_end(start, grain)
    assert floor_to_grain(start, grain) == start
    assert floor_to_grain(end - dt.timedelta(microseconds=1), grain) == start
    assert floor_to_grain(end, grain) == end


@pytest.mark.parametrize("grain", GRAINS)
def test_flooring_is_idempotent(grain):
    for moment in MOMENTS:
        once = floor_to_grain(moment, grain)
        assert floor_to_grain(once, grain) == once


# --------------------------------------------------------------------------
# Sealing
# --------------------------------------------------------------------------


@pytest.mark.parametrize("grain", GRAINS)
def test_seal_cutoff_is_exactly_one_window_behind_the_watermark(grain):
    """``ws + G <= W`` rearranged to ``ws <= W - G``, which is the point.

    The rearranged form puts the whole comparison on one side, so the engine
    inlines a single literal instead of emitting per-row interval arithmetic --
    which is what keeps ``CONTEXT.md`` 1.5 satisfied and lets DuckLake prune on
    ``window_ts`` statistics.
    """
    watermark = dt.datetime(2026, 8, 1, 13, 20)
    cutoff = seal_cutoff(watermark, grain)
    assert cutoff == watermark - grain_interval(grain)
    # The rearrangement is equivalence, not approximation: a window seals under
    # one form exactly when it seals under the other.
    for offset in (-2, -1, 0, 1, 2):
        start = floor_to_grain(watermark, grain) + offset * grain_interval(grain)
        assert (start <= cutoff) == (window_end(start, grain) <= watermark)


def test_a_window_seals_the_instant_the_watermark_reaches_its_end():
    """``<=``, not ``<``. The watermark's promise is about everything before it."""
    start = dt.datetime(2026, 8, 1, 12)
    end = window_end(start, "hour")
    assert start <= seal_cutoff(end, "hour")
    assert start > seal_cutoff(end - dt.timedelta(microseconds=1), "hour")


def test_nothing_seals_before_a_watermark_exists():
    assert seal_cutoff(None, "hour") is None
    assert sealed_predicate("ts", "hour", None) is None


def test_the_sealed_predicate_tests_the_window_not_the_timestamp(con):
    """The distinction the whole lateness horizon rests on.

    With ``grain='hour'`` and a watermark of 12:50, a row at 12:05 is older than
    the watermark but its window has not ended, so it is *not* late. A predicate
    written as ``ts <= watermark`` would drop it -- and dropping it is exactly
    the "late arrival within the horizon" case ``PLAN.md`` requires to update
    its window.
    """
    predicate = sealed_predicate("ts", "hour", dt.datetime(2026, 8, 1, 12, 50))
    rows = con.execute(
        f"SELECT ts, ({predicate}) AS sealed FROM (VALUES "
        f"  (TIMESTAMP '2026-08-01 12:05:00'),"
        f"  (TIMESTAMP '2026-08-01 11:59:59'),"
        f"  (TIMESTAMP '2026-08-01 12:59:59'),"
        f"  (TIMESTAMP '2026-08-01 13:00:00')"
        f") AS v(ts)"
    ).fetchall()
    assert dict(rows) == {
        dt.datetime(2026, 8, 1, 12, 5): False,
        dt.datetime(2026, 8, 1, 11, 59, 59): True,
        dt.datetime(2026, 8, 1, 12, 59, 59): False,
        dt.datetime(2026, 8, 1, 13): False,
    }


def test_a_null_timestamp_is_neither_sealed_nor_unsealed(con):
    """NULL propagates, so an undated row is never *dropped* as late.

    It is refused for a different reason -- it belongs to no window at all --
    and the engine counts it separately. Pinned here so the predicate is not
    later "fixed" into treating NULL as sealed, which would merge two distinct
    operational problems into one number.
    """
    predicate = sealed_predicate("ts", "hour", dt.datetime(2026, 8, 1, 12, 50))
    value = con.execute(
        f"SELECT ({predicate}) FROM (SELECT NULL::TIMESTAMP AS ts)"
    ).fetchone()[0]
    assert value is None


# --------------------------------------------------------------------------
# Refusals
# --------------------------------------------------------------------------


@pytest.mark.parametrize("grain", ["month", "week", "second", "hours", "", None, 3600])
def test_an_unsupported_grain_is_refused(grain):
    with pytest.raises(DuckstreamError) as excinfo:
        grain_interval(grain)
    assert "minute" in str(excinfo.value) and "hour" in str(excinfo.value)


def test_month_is_absent_for_a_stated_reason():
    """Not an oversight: a month has no fixed length, and the seal needs one."""
    with pytest.raises(DuckstreamError) as excinfo:
        grain_interval("month")
    assert "length varies" in str(excinfo.value)


def test_windowing_without_a_time_column_is_refused():
    with pytest.raises(DuckstreamError, match="no event-time value"):
        window_expression("hour", "")


def test_the_grain_reaches_sql_as_a_literal():
    """Quoted, not interpolated: the grain arrives from a config file."""
    assert window_expression("hour", "event ts") == (
        "date_trunc('hour', \"event ts\")"
    )


def test_the_window_column_name_is_fixed():
    assert WINDOW_COLUMN == "window_ts"
    from duckstream.model import WINDOW_COLUMN as from_model

    assert from_model is WINDOW_COLUMN
