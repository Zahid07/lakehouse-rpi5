"""Tumbling windows: the arithmetic, in one place.

A window is a half-open interval ``[start, start + grain)``. Every row whose
event time falls inside it belongs to it, and the column duckstream emits for
it is ``window_ts`` -- the *start* -- at every grain. That fixed name is not a
convenience: it is what makes ``PLAN.md``'s invariant "the sink merge key must
equal the window grain key" checkable mechanically rather than by convention
(``Model._check_window_key``).

Everything about window boundaries lives here rather than being spelled out
wherever it is needed, because the same boundary is computed three times over
in three different languages and the three must agree exactly:

* in **SQL**, when the sink derives ``window_ts`` from the time column;
* in **SQL**, when the engine decides which of a batch's rows fall in a window
  that has already sealed;
* in **Python**, when the sealing cutoff is computed for a watermark.

The Python and SQL forms are proved equal against DuckDB itself in
``tests/unit/test_windows.py`` rather than assumed, because a disagreement of
one microsecond between ``date_trunc`` and :func:`floor_to_grain` would show up
as a window that seals a batch early -- a wrong answer, not a crash.

Why only fixed-duration grains
------------------------------

``minute``, ``hour`` and ``day`` are all exact multiples of a second, so
``window_end = window_start + grain`` is ordinary arithmetic and the seal
cutoff ``watermark - grain`` is exact. A ``month`` grain would not be: months
have different lengths, so the cutoff would have to be computed per window
rather than once per batch, and the single inlined literal that
``CONTEXT.md`` 1.5 requires (no scalar subquery near a MERGE) would become a
correlated expression. Sliding and session windows are post-v1 for their own
reasons; ``month`` is absent for this one.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from duckstream.errors import DuckstreamError
from duckstream.sql import quote_ident, quote_literal

__all__ = [
    "GRAIN_INTERVALS",
    "WINDOW_COLUMN",
    "grain_interval",
    "floor_to_grain",
    "window_end",
    "window_expression",
    "seal_cutoff",
    "sealed_predicate",
]


#: The window column duckstream emits, whatever the grain. Defined here and
#: re-exported by :mod:`duckstream.model`, which is where users meet it.
WINDOW_COLUMN = "window_ts"

#: Every supported grain and its exact duration. Ordered coarsest-last so error
#: messages read the way a user thinks about them.
GRAIN_INTERVALS: dict[str, timedelta] = {
    "minute": timedelta(minutes=1),
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
}


def grain_interval(grain: Any) -> timedelta:
    """The exact duration of one window at ``grain``.

    Raises:
        DuckstreamError: for anything not in :data:`GRAIN_INTERVALS`, including
            ``None``. A caller reaching here with ``None`` has skipped the
            check that a windowed operation needs a grain at all, and inventing
            a default would silently window the data at whatever this module
            happened to prefer.
    """
    try:
        return GRAIN_INTERVALS[grain]
    except (KeyError, TypeError):
        raise DuckstreamError(
            f"grain {grain!r} is not a tumbling-window grain; expected one of "
            f"{', '.join(repr(g) for g in GRAIN_INTERVALS)}. Sliding and "
            f"session windows are post-v1, and 'month' is absent because its "
            f"length varies, which the seal arithmetic here relies on being "
            f"fixed."
        ) from None


def floor_to_grain(moment: datetime, grain: str) -> datetime:
    """The start of the window ``moment`` falls in. The Python half of ``date_trunc``.

    Must agree with :func:`window_expression` to the microsecond; a unit test
    asserts it against DuckDB across grains, boundaries and sub-second values
    rather than trusting that two implementations of "truncate" mean the same
    thing.
    """
    interval = grain_interval(grain)
    if interval == GRAIN_INTERVALS["day"]:
        return moment.replace(hour=0, minute=0, second=0, microsecond=0)
    if interval == GRAIN_INTERVALS["hour"]:
        return moment.replace(minute=0, second=0, microsecond=0)
    return moment.replace(second=0, microsecond=0)


def window_end(start: datetime, grain: str) -> datetime:
    """The exclusive end of the window beginning at ``start``."""
    return start + grain_interval(grain)


def window_expression(grain: str, time_column: str) -> str:
    """SQL deriving ``window_ts`` from a row's event time.

    The grain reaches SQL as a quoted *literal* rather than being interpolated
    raw. It arrives from a config file, and :func:`duckstream.sql.quote_literal`
    is the only thing standing between that file and the statement text --
    even though :func:`grain_interval` has already refused anything but the
    three known names, defence in depth costs nothing here.
    """
    grain_interval(grain)  # refuse an unknown grain before building any SQL
    if not time_column:
        raise DuckstreamError(
            f"cannot window at grain {grain!r} without a time column: there is "
            f"no event-time value to truncate into {WINDOW_COLUMN!r}"
        )
    return f"date_trunc({quote_literal(grain)}, {quote_ident(time_column)})"


def seal_cutoff(watermark: datetime | None, grain: str) -> datetime | None:
    """The largest ``window_ts`` that is sealed at ``watermark``. ``None`` if none is.

    A window ``[ws, ws + G)`` is sealed once the watermark has reached its end,
    because the watermark's promise is that nothing older than it is still to
    come::

        sealed  <=>  ws + G <= watermark  <=>  ws <= watermark - G

    The right-hand form is the one worth having. It puts the whole comparison
    on one side, so the engine computes a single ``TIMESTAMP`` in Python and
    inlines it as one literal instead of emitting ``window_ts + INTERVAL``
    arithmetic per row -- which keeps ``CONTEXT.md`` 1.5 satisfied (no scalar
    subquery anywhere near a MERGE) and lets DuckLake prune data files on the
    ``window_ts`` statistics it already keeps.

    ``None`` in means no watermark has been established yet, so nothing can be
    proved complete and nothing seals.
    """
    if watermark is None:
        return None
    return watermark - grain_interval(grain)


def sealed_predicate(
    time_column: str, grain: str, watermark: datetime | None
) -> str | None:
    """SQL true for a row whose window has **already** sealed. ``None`` if none has.

    This is the late-past-the-horizon test. It is expressed on the row's
    *window*, not on the row's timestamp, and the difference is the whole point
    of a lateness horizon: with ``grain='hour'`` and ``lateness='10 minutes'``,
    a watermark of 12:50 leaves the 12:00 window open, so a row arriving late
    at 12:05 still belongs in it and must be folded. Testing ``event_ts <
    watermark`` instead would drop that row -- exactly the "late arrival within
    the horizon" case ``PLAN.md`` requires to update its window.
    """
    cutoff = seal_cutoff(watermark, grain)
    if cutoff is None:
        return None
    return (
        f"{window_expression(grain, time_column)} <= "
        f"{quote_literal(cutoff)}"
    )
