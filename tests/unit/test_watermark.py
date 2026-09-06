"""The watermark: parsing a horizon, advancing it, and what it filters out.

Three separable things live in :mod:`duckstream.watermark`, and they fail in
three different ways:

* **parsing** a declared horizon -- a user-facing surface, so it is refused
  loudly and canonicalised so that ``timedelta(minutes=10)`` and
  ``"10 minutes"`` produce equal models (which the config round-trip needs);
* **advancing** -- arithmetic, and monotone. A watermark that could regress
  would re-open windows that had been declared complete and, in append mode,
  already emitted;
* **observing** -- one scan of a bound batch for its counts and its newest
  event time. Tested against a real DuckDB view rather than a mock, because the
  thing worth checking is the SQL, and specifically that ``rows_late`` counts
  rows whose *window* has sealed rather than rows older than the watermark.

The end-to-end contract -- late-within-horizon folding, sealing, drop counts
reaching the catalog -- is ``tests/conformance/test_event_time.py``, against
DuckLake and both front doors.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from duckstream.errors import DuckstreamError
from duckstream.watermark import (
    LATENESS_UNITS,
    BatchObservation,
    WatermarkPolicy,
    format_lateness,
    parse_lateness,
    policy_for,
)

MINUTE = dt.timedelta(minutes=1)


@pytest.fixture
def con():
    connection = duckdb.connect()
    try:
        yield connection
    finally:
        connection.close()


def view(con, rows, name="batch"):
    """A temp view shaped like a bound batch: ``event_ts``, ``sensor``, ``value``."""
    if not rows:
        con.execute(
            f'CREATE OR REPLACE TEMP VIEW "{name}" AS SELECT '
            f"NULL::TIMESTAMP AS event_ts, NULL::VARCHAR AS sensor, "
            f"NULL::DOUBLE AS value WHERE false"
        )
        return name
    values = ", ".join(
        "("
        + ("NULL" if ts is None else f"TIMESTAMP '{ts.isoformat(sep=' ')}'")
        + f", '{sensor}', {value})"
        for ts, sensor, value in rows
    )
    con.execute(
        f'CREATE OR REPLACE TEMP VIEW "{name}" AS SELECT * FROM (VALUES {values}) '
        f"AS v(event_ts, sensor, value)"
    )
    return name


HOURLY = WatermarkPolicy(
    time_column="event_ts", grain="hour", lateness=dt.timedelta(minutes=10)
)


# --------------------------------------------------------------------------
# Parsing a horizon
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        ("0 seconds", dt.timedelta(0)),
        ("1 second", dt.timedelta(seconds=1)),
        ("30 seconds", dt.timedelta(seconds=30)),
        ("10 minutes", dt.timedelta(minutes=10)),
        ("1 minute", MINUTE),
        ("90 minutes", dt.timedelta(minutes=90)),
        ("1 hour", dt.timedelta(hours=1)),
        ("2 days", dt.timedelta(days=2)),
        ("  10   minutes  ", dt.timedelta(minutes=10)),
        ("10 MINUTES", dt.timedelta(minutes=10)),
        ("10 Minute", dt.timedelta(minutes=10)),
    ],
)
def test_a_horizon_parses(text, expected):
    assert parse_lateness(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "10",
        "minutes",
        "ten minutes",
        "10 weeks",
        "10 months",
        "1.5 hours",
        "-5 minutes",
        "PT10M",
        "10m",
        "",
    ],
)
def test_an_unparseable_horizon_is_refused_with_an_example(text):
    with pytest.raises(DuckstreamError) as excinfo:
        parse_lateness(text)
    message = str(excinfo.value)
    assert "'10 minutes'" in message
    for unit in LATENESS_UNITS:
        assert unit in message


def test_a_timedelta_is_accepted_so_the_python_door_reads_naturally():
    assert parse_lateness(dt.timedelta(hours=2)) == dt.timedelta(hours=2)


def test_a_negative_timedelta_is_refused():
    with pytest.raises(DuckstreamError, match="negative"):
        parse_lateness(dt.timedelta(minutes=-1))


def test_a_sub_second_horizon_is_refused_rather_than_rounded():
    """Rounding would silently change which rows are dropped."""
    with pytest.raises(DuckstreamError, match="sub-second"):
        parse_lateness(dt.timedelta(milliseconds=1500))


@pytest.mark.parametrize("value", [10, 10.0, None, ["10 minutes"], {"minutes": 10}])
def test_a_horizon_of_the_wrong_type_is_refused(value):
    with pytest.raises(DuckstreamError, match="duration string"):
        parse_lateness(value)


@pytest.mark.parametrize(
    "value,text",
    [
        (dt.timedelta(0), "0 seconds"),
        (dt.timedelta(seconds=1), "1 second"),
        (dt.timedelta(seconds=45), "45 seconds"),
        (dt.timedelta(minutes=1), "1 minute"),
        (dt.timedelta(minutes=90), "90 minutes"),
        (dt.timedelta(hours=1), "1 hour"),
        (dt.timedelta(hours=25), "25 hours"),
        (dt.timedelta(days=1), "1 day"),
        (dt.timedelta(days=3), "3 days"),
    ],
)
def test_the_canonical_form_uses_the_largest_exact_unit(value, text):
    assert format_lateness(value) == text


@pytest.mark.parametrize(
    "text", ["0 seconds", "45 seconds", "1 minute", "90 minutes", "1 hour", "3 days"]
)
def test_formatting_round_trips_through_parsing(text):
    """What makes a ``timedelta`` model equal to the same model loaded from YAML."""
    assert format_lateness(parse_lateness(text)) == text


# --------------------------------------------------------------------------
# Advancing
# --------------------------------------------------------------------------


def test_the_first_batch_establishes_the_watermark():
    assert HOURLY.advance(None, dt.datetime(2026, 5, 1, 12, 0)) == dt.datetime(
        2026, 5, 1, 11, 50
    )


def test_the_watermark_never_regresses():
    high = dt.datetime(2026, 5, 1, 11, 50)
    assert HOURLY.advance(high, dt.datetime(2026, 5, 1, 9, 0)) == high


def test_a_batch_with_no_dated_rows_leaves_the_watermark_alone():
    high = dt.datetime(2026, 5, 1, 11, 50)
    assert HOURLY.advance(high, None) == high
    assert HOURLY.advance(None, None) is None


def test_a_zero_horizon_advances_to_the_newest_event():
    policy = WatermarkPolicy("event_ts", "hour", dt.timedelta(0))
    newest = dt.datetime(2026, 5, 1, 12, 34, 56)
    assert policy.advance(None, newest) == newest


# --------------------------------------------------------------------------
# Observing a batch
# --------------------------------------------------------------------------


def test_observe_counts_rows_and_finds_the_newest_event(con):
    name = view(
        con,
        [
            (dt.datetime(2026, 5, 1, 10, 5), "s1", 1.0),
            (dt.datetime(2026, 5, 1, 10, 30), "s1", 2.0),
        ],
    )
    seen = HOURLY.observe(con, name, None)
    assert seen == BatchObservation(
        rows_in=2, rows_late=0, rows_undated=0,
        max_event_ts=dt.datetime(2026, 5, 1, 10, 30),
    )
    assert not seen.drops_anything


def test_nothing_is_late_before_a_watermark_exists(con):
    """A first batch cannot be late: nothing has been declared complete yet."""
    name = view(con, [(dt.datetime(2020, 1, 1), "s1", 1.0)])
    assert HOURLY.observe(con, name, None).rows_late == 0


def test_late_is_decided_by_the_window_not_by_the_timestamp(con):
    """The distinction the horizon exists for, at the SQL level.

    Watermark 11:20, grain hour. The 10:15 row is older than the watermark but
    its window ``[10:00, 11:00)`` has ended, so it *is* late. The 11:05 row is
    also older than the watermark, and its window has not ended, so it is not.
    """
    name = view(
        con,
        [
            (dt.datetime(2026, 5, 1, 10, 15), "s1", 1.0),
            (dt.datetime(2026, 5, 1, 11, 5), "s1", 2.0),
            (dt.datetime(2026, 5, 1, 11, 45), "s1", 4.0),
        ],
    )
    seen = HOURLY.observe(con, name, dt.datetime(2026, 5, 1, 11, 20))
    assert seen.rows_in == 3
    assert seen.rows_late == 1
    assert seen.max_event_ts == dt.datetime(2026, 5, 1, 11, 45)


def test_undated_rows_are_counted_apart_from_late_ones(con):
    name = view(
        con,
        [
            (None, "s1", 1.0),
            (dt.datetime(2026, 5, 1, 10, 15), "s1", 2.0),
            (dt.datetime(2026, 5, 1, 11, 45), "s1", 4.0),
        ],
    )
    seen = HOURLY.observe(con, name, dt.datetime(2026, 5, 1, 11, 20))
    assert (seen.rows_late, seen.rows_undated) == (1, 1)
    assert seen.rows_dropped == 2
    assert seen.drops_anything


def test_an_empty_batch_observes_cleanly(con):
    seen = HOURLY.observe(con, view(con, []), dt.datetime(2026, 5, 1, 11, 20))
    assert seen == BatchObservation(0, 0, 0, None)


# --------------------------------------------------------------------------
# Filtering
# --------------------------------------------------------------------------


def test_the_on_time_view_keeps_exactly_what_observe_did_not_drop(con):
    rows = [
        (None, "s1", 1.0),
        (dt.datetime(2026, 5, 1, 10, 15), "s1", 2.0),
        (dt.datetime(2026, 5, 1, 11, 5), "s1", 4.0),
        (dt.datetime(2026, 5, 1, 11, 45), "s1", 8.0),
    ]
    name = view(con, rows)
    previous = dt.datetime(2026, 5, 1, 11, 20)
    seen = HOURLY.observe(con, name, previous)
    filtered = HOURLY.on_time_view(con, name, previous)

    kept = con.execute(f'SELECT value FROM "{filtered}" ORDER BY value').fetchall()
    assert [v for (v,) in kept] == [4.0, 8.0]
    assert len(kept) == seen.rows_in - seen.rows_dropped


def test_the_on_time_view_still_drops_undated_rows_before_any_watermark(con):
    """No watermark yet, but event-time semantics already apply."""
    name = view(con, [(None, "s1", 1.0), (dt.datetime(2026, 5, 1, 10), "s1", 2.0)])
    filtered = HOURLY.on_time_view(con, name, None)
    assert con.execute(f'SELECT count(*) FROM "{filtered}"').fetchone()[0] == 1


def test_each_on_time_view_gets_its_own_name(con):
    name = view(con, [(dt.datetime(2026, 5, 1, 10), "s1", 1.0)])
    first = HOURLY.on_time_view(con, name, None)
    second = HOURLY.on_time_view(con, name, None)
    assert first != second


# --------------------------------------------------------------------------
# Resolving a policy from a model
# --------------------------------------------------------------------------


class _Model:
    def __init__(self, **kw):
        self.name = "m"
        self.lateness = None
        self.grain = None
        self.time_column = None
        self.__dict__.update(kw)


def test_a_model_with_no_horizon_has_no_policy():
    """The phase-1 path: nothing read, nothing written, nothing filtered."""
    assert policy_for(_Model(grain="hour", time_column="event_ts")) is None


def test_a_model_with_a_horizon_resolves_its_policy():
    policy = policy_for(
        _Model(grain="hour", time_column="event_ts", lateness="10 minutes")
    )
    assert policy == HOURLY


@pytest.mark.parametrize(
    "kwargs,missing",
    [
        ({"time_column": "event_ts"}, "grain"),
        ({"grain": "hour"}, "time_column"),
    ],
)
def test_a_horizon_without_windows_is_refused(kwargs, missing):
    """Defence in depth: ``Model.validate`` refuses this first, with a better
    message. This catches a hand-built object that never went through it."""
    with pytest.raises(DuckstreamError) as excinfo:
        policy_for(_Model(lateness="10 minutes", **kwargs))
    assert missing in str(excinfo.value)


def test_an_unknown_grain_is_refused_when_the_policy_resolves():
    with pytest.raises(DuckstreamError, match="tumbling-window grain"):
        policy_for(
            _Model(grain="month", time_column="event_ts", lateness="1 day")
        )
