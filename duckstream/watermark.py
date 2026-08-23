"""Event time: the watermark, the lateness horizon, and what falls outside it.

A watermark is a claim, and it is worth stating precisely because everything
else here is derived from it:

    **at watermark W, no row with an event time before W is still expected.**

duckstream derives it the way every micro-batch engine does -- from the data
itself, as ``max(event time seen so far) - lateness`` -- and it never goes
backwards. The ``lateness`` a model declares is therefore not a filter on rows;
it is how far behind the newest observed event the engine is willing to keep
windows open. A model that declares no lateness has no watermark, keeps every
window open forever, and behaves exactly as phase 1 did.

What the horizon buys, and what it costs
----------------------------------------

It buys **sealing**. Once the watermark passes a window's end, that window is
complete: nothing more can change it, so it can be emitted once and for all
(``append`` output mode) or trusted as final (``update``). Without a watermark
no window is ever knowably complete, which is why ``append`` over a windowed
aggregation requires one -- see :meth:`duckstream.model.Model.validate`.

It costs the rows that arrive after their window sealed. Those are **dropped
and counted**, never silently absorbed: ``PLAN.md`` asks for exactly that, and
the counting is the half that matters. A stream that quietly discards 4% of its
input looks identical to one that discards none, which is the failure mode this
framework exists to remove. The count is per batch, durable in
``duckstream.batches``, and surfaced on :class:`~duckstream.engine.BatchResult`.

Two kinds of row fall outside the horizon, and they are counted separately
because they mean different things:

``rows_late``
    The row's window had already sealed. Someone's data is arriving later than
    the declared lateness allows for -- widen the horizon, or accept the loss.

``rows_undated``
    The row's event-time column is NULL, so it belongs to no window at all and
    could never be sealed or emitted. Under event-time semantics there is
    nowhere to put it. This applies only to a model that opted into a lateness
    horizon: without one, a NULL event time still produces a NULL ``window_ts``
    exactly as it did in phase 1.

The order of operations, and why it is that order
-------------------------------------------------

Within one batch the engine filters with the **committed** watermark and then
advances to a new one::

    W_in  = state.load_watermark(...)          # what previous batches proved
    stats = policy.observe(con, view, W_in)    # one scan: counts and max
    W_out = policy.advance(W_in, stats.max_event_ts)

Filtering with ``W_out`` instead would be wrong in a way that is easy to miss:
a single batch may legitimately span a wide range of event times, and using its
own maximum to judge its own rows would drop the older half of it. The
committed watermark is the only value that reflects what was actually complete
*before* this batch arrived.

The consequence at the other end is deliberate and correct: a row whose window
is not sealed by ``W_in`` but is sealed by ``W_out`` is folded first and its
window seals afterwards, in the same transaction. It arrived in time; the batch
that carried it is what closed the window.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from duckstream.errors import DuckstreamError
from duckstream.sql import quote_ident
from duckstream.windows import grain_interval, seal_cutoff, sealed_predicate

__all__ = [
    "LATENESS_UNITS",
    "BatchObservation",
    "WatermarkPolicy",
    "format_lateness",
    "parse_lateness",
    "policy_for",
]


#: Accepted duration units and their length. Deliberately short: a duration
#: language with weeks, months and ISO-8601 forms invites a value whose meaning
#: depends on when it is evaluated, and a lateness horizon has to be a fixed
#: number of seconds for the seal arithmetic in :mod:`duckstream.windows` to be
#: exact.
LATENESS_UNITS: dict[str, timedelta] = {
    "second": timedelta(seconds=1),
    "minute": timedelta(minutes=1),
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
}

#: ``"<whole number> <unit>"``, with an optional trailing ``s``. Nothing else.
_LATENESS_TEXT = re.compile(
    r"^\s*(\d+)\s*(second|minute|hour|day)s?\s*$", re.IGNORECASE
)

#: Prefix of the temp view holding a batch with its out-of-horizon rows removed.
_FILTERED_PREFIX = "duckstream_ontime_"


def parse_lateness(value: Any) -> timedelta:
    """A declared lateness horizon as a :class:`~datetime.timedelta`.

    Accepts ``"10 minutes"``, ``"1 hour"``, ``"0 seconds"`` and the equivalent
    :class:`~datetime.timedelta`. Both front doors reach this: the Python API
    because a user may write either, the YAML loader because a config file can
    only carry the string.

    Raises:
        DuckstreamError: for an unparseable string, a negative horizon, or a
            :class:`~datetime.timedelta` carrying a sub-second component.
            Sub-second is refused rather than rounded because rounding a
            horizon silently changes which rows survive, and a stream whose
            lateness is measured in microseconds is not the shape of problem
            this engine is for.
    """
    if isinstance(value, timedelta):
        return _validated_timedelta(value)
    if not isinstance(value, str):
        raise DuckstreamError(
            f"lateness must be a duration string such as '10 minutes', or a "
            f"datetime.timedelta; got {type(value).__name__}: {value!r}"
        )
    match = _LATENESS_TEXT.match(value)
    if match is None:
        raise DuckstreamError(
            f"lateness {value!r} is not a duration. Write a whole number and a "
            f"unit, for example '10 minutes', '1 hour', '30 seconds' or "
            f"'0 seconds'. Units: "
            f"{', '.join(sorted(LATENESS_UNITS))} (plural or singular)."
        )
    count = int(match.group(1))
    unit = match.group(2).lower()
    return _validated_timedelta(count * LATENESS_UNITS[unit])


def _validated_timedelta(value: timedelta) -> timedelta:
    if value < timedelta(0):
        raise DuckstreamError(
            f"lateness {value!r} is negative. A lateness horizon is how far "
            f"behind the newest event windows are kept open; a negative one "
            f"would seal windows before their own data could arrive."
        )
    if value.microseconds:
        raise DuckstreamError(
            f"lateness {value!r} has a sub-second component. Declare whole "
            f"seconds or more -- rounding a horizon would silently change "
            f"which rows are dropped as late."
        )
    return value


def format_lateness(value: timedelta) -> str:
    """The canonical text for a horizon: the largest unit that divides it exactly.

    ``timedelta(hours=1)`` becomes ``'1 hour'`` and ``timedelta(minutes=90)``
    becomes ``'90 minutes'``. This is what :meth:`Model.__post_init__` stores,
    so a model built in Python from a ``timedelta`` and the same model loaded
    from YAML compare equal -- which is what the config round-trip test checks.
    """
    value = _validated_timedelta(value)
    seconds = int(value.total_seconds())
    for name in ("day", "hour", "minute", "second"):
        unit = int(LATENESS_UNITS[name].total_seconds())
        if seconds and seconds % unit == 0:
            count = seconds // unit
            return f"{count} {name}" if count == 1 else f"{count} {name}s"
    return "0 seconds"


# --------------------------------------------------------------------------
# What one batch looked like in event time
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class BatchObservation:
    """Everything one scan of a bound batch says about its event time.

    One scan, four numbers. The alternative -- a ``count(*)`` for the batch, a
    second for the late rows and a third query for the maximum -- would read
    the same parquet three times for values that are all aggregates of the same
    pass.
    """

    rows_in: int
    """Every row the batch bound, including the ones about to be dropped."""

    rows_late: int
    """Rows whose window had already sealed at the committed watermark."""

    rows_undated: int
    """Rows whose event-time column is NULL, so they belong to no window."""

    max_event_ts: datetime | None
    """The newest event time in the batch; ``None`` if every row was undated."""

    @property
    def rows_dropped(self) -> int:
        """Rows this batch will not contribute to any window."""
        return self.rows_late + self.rows_undated

    @property
    def drops_anything(self) -> bool:
        return self.rows_dropped > 0


# --------------------------------------------------------------------------
# The policy
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class WatermarkPolicy:
    """One model's event-time rules, resolved once and applied per batch.

    Built by :func:`policy_for`, which returns ``None`` for a model that
    declares no lateness. That ``None`` is the phase-1 path: no watermark is
    read, none is written, no row is filtered and no extra scan happens, so a
    model that has not opted into event time pays nothing for its existence.
    """

    time_column: str
    grain: str
    lateness: timedelta

    # -- observation ------------------------------------------------------

    def observe(
        self, con: Any, view: str, previous: datetime | None
    ) -> BatchObservation:
        """Scan the bound batch once for its counts and its newest event time.

        ``previous`` is the **committed** watermark, and it is what decides
        which rows count as late -- see the module docstring on ordering. It
        reaches SQL as an inlined literal, never as a scalar subquery
        (``CONTEXT.md`` 1.5).
        """
        sealed = sealed_predicate(self.time_column, self.grain, previous)
        late_count = "0" if sealed is None else f"count(*) FILTER (WHERE {sealed})"
        column = quote_ident(self.time_column)
        row = con.execute(
            f"SELECT count(*), "
            f"{late_count}, "
            f"count(*) FILTER (WHERE {column} IS NULL), "
            f"max({column}) "
            f"FROM {quote_ident(view)}"
        ).fetchone()
        if row is None:  # pragma: no cover - an aggregate always returns a row
            return BatchObservation(0, 0, 0, None)
        return BatchObservation(
            rows_in=int(row[0] or 0),
            rows_late=int(row[1] or 0),
            rows_undated=int(row[2] or 0),
            max_event_ts=row[3],
        )

    # -- advancing --------------------------------------------------------

    def advance(
        self, previous: datetime | None, max_event_ts: datetime | None
    ) -> datetime | None:
        """The watermark after this batch: monotone, never regressing.

        ``max(previous, max_event_ts - lateness)``. The monotonicity is not
        decoration -- a batch of genuinely old data would otherwise pull the
        watermark backwards and re-open windows that had already been sealed
        and, in ``append`` mode, already emitted. Sealing has to be a one-way
        door or the mode means nothing.
        """
        if max_event_ts is None:
            return previous
        candidate = max_event_ts - self.lateness
        if previous is None:
            return candidate
        return max(previous, candidate)

    def seal_cutoff(self, watermark: datetime | None) -> datetime | None:
        """The largest ``window_ts`` that is complete at ``watermark``."""
        return seal_cutoff(watermark, self.grain)

    # -- filtering --------------------------------------------------------

    def on_time_view(self, con: Any, view: str, previous: datetime | None) -> str:
        """A temp view over ``view`` with the out-of-horizon rows removed.

        The caller decides whether to ask for one: the engine only does so when
        :meth:`observe` said something would actually be dropped, so a healthy
        stream never creates a second view and never scans a row twice.
        """
        sealed = sealed_predicate(self.time_column, self.grain, previous)
        column = quote_ident(self.time_column)
        conditions = [f"{column} IS NOT NULL"]
        if sealed is not None:
            conditions.append(f"NOT ({sealed})")
        name = f"{_FILTERED_PREFIX}{uuid.uuid4().hex}"
        con.execute(
            f"CREATE TEMP VIEW {quote_ident(name)} AS "
            f"SELECT * FROM {quote_ident(view)} "
            f"WHERE {' AND '.join(conditions)}"
        )
        return name


def policy_for(model: Any) -> WatermarkPolicy | None:
    """The model's watermark policy, or ``None`` if it declared no lateness.

    A model reaching here has been validated, so a declared ``lateness``
    implies a ``grain`` and a ``time_column``; the checks below catch a
    hand-built object that skipped :meth:`Model.validate`, not a user error.
    """
    lateness = getattr(model, "lateness", None)
    if lateness is None:
        return None
    grain = getattr(model, "grain", None)
    time_column = getattr(model, "time_column", None)
    if not grain or not time_column:
        missing = "grain" if not grain else "time_column"
        raise DuckstreamError(
            f"model {getattr(model, 'name', '?')!r} declares a lateness horizon "
            f"but no {missing}. A watermark is only meaningful against windows, "
            f"and windows need both."
        )
    grain_interval(grain)  # refuse an unknown grain before anything runs
    return WatermarkPolicy(
        time_column=time_column,
        grain=grain,
        lateness=parse_lateness(lateness),
    )
