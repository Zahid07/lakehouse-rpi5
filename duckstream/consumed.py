"""The consumed-file set, stored as rows rather than as one JSON cell.

``CONTEXT.md`` 1.15 and 1.16 measured the cost of the shape this module
replaces. The file source's offset carried its whole consumed map inside itself,
and the offset is rewritten in full on **every** trigger. At 525,600 files --
one a minute for a year -- that is:

=========================  ===========  ============
                           the offset   as rows
=========================  ===========  ============
bytes written per trigger  **7.97 MB**  **4.9 KB**
per day, at one a minute   11.2 GB      6.8 MB
commit                     1,078 ms     13.8 ms
planning                   545 ms       3.6 ms
=========================  ===========  ============

The latency is bad; the number that decides whether duckstream runs unattended
on a Raspberry Pi is the first row -- an SD card's write budget spent
re-recording file names it already knows. The whole year of names, stored once
as rows, is 8.8 MB: about what the old shape wrote *every single trigger*.

Two shortcuts were measured and rejected, and neither should be revisited
without new evidence. Collapsing old entries behind ``high_water_mtime_ns``
bounds the map but silently skips a file that arrives with an older mtime, which
is the failure class this project exists to refuse. Compressing the stored form
gives 9.2x on the JSON but only **2.38x on the disk**, because parquet has
already taken most of what there is to take -- for **+185 ms a trigger**, which
is a mitigation dressed as a fix.

The fix is 1.12's rule in its most extreme form -- *anything that reads a state
table and then loops over the result in Python is a defect waiting for the table
to grow*, and here the whole table was a single cell. Consuming a file becomes
an **insert of one row**; "has this been consumed?" becomes an **anti-join the
database answers**.

Two representations, one interface
----------------------------------

:class:`MapIndex` is the old shape and :class:`TableIndex` is the new one, and
they answer the same two questions -- *which of these scanned files are new?*
and *what offset does this batch check point?* That pairing is deliberate: the
consumed-set representation and the offset shape are one decision, so they are
expressed in one place rather than kept in step by hand.

``FileSource.plan`` therefore holds no opinion about where the set lives. It is
handed an index and asks it. The engine supplies the table-backed one; the map
one survives to read a v1 offset during migration, and to keep the source's own
unit tests free of a catalog.

Why the join is exact
---------------------

A row is ``(model_name, relpath, relpath_fold, size, mtime_ns)``. The join
matches on **path and identity together**, which is what preserves the
rewritten-in-place detection the map shape had: a file rewritten under the same
name has a new size or mtime, matches no row, and is re-planned.

``relpath_fold`` is ``relpath.casefold()`` and is written on **every** platform,
while which column the join uses is decided by the platform reading it --
``relpath`` on POSIX, where two paths differing only in case are two files, and
``relpath_fold`` on Windows, where they are one. Storing both and choosing at
read time is what keeps a catalog portable in *both* directions: folding at
write time would make a Linux-written table re-read every uppercase path when
opened on Windows, and folding at read time in SQL would put DuckDB's ``lower``
against Python's ``casefold``, which disagree on non-ASCII.

The table is append-only, per ``CONTEXT.md`` 1.10 -- a matching DuckLake
``DELETE`` costs ~26 ms and writes a tombstone, and this is per-trigger state.
A case-only rename therefore leaves both spellings behind, one extra row rather
than the map shape's permanent leak, and it is not a double-read: a rename
preserves size and mtime, so the folded join still finds the file consumed.

**Nothing prunes this table**, for the same reason nothing prunes
``quarantine``: it is not a history of what happened, it is the position
itself. Dropping a row makes duckstream read that file again and fold its rows
into the mart a second time. Bounding it is a *retention* question -- fewer
files -- not a pruning one.

The file -> time-range index
----------------------------

``min_ts``, ``max_ts`` and ``n_rows`` sit beside ``relpath`` and are what makes
a tier-three ``recompute_window`` affordable. ``CONTEXT.md`` 1.13 measured the
problem: statistics pruning skips data pages but never the **file open**, at a
flat ~0.1 ms per file listed whether it is read or not -- so handing DuckDB a
year of consumed files and a time predicate costs ~52 seconds a recompute on a
dev box, and more on a Pi, where the cost is small random I/O. Asking the table
which files can possibly contain a window turns that back into a lookup.

It is a **hint and never truth**: it only ever narrows, and a file it cannot
place is a file it must return. Over-selecting reads extra files and still
gives the right answer; under-selecting is silently wrong, which is the one
outcome this framework refuses. Correctness therefore never depends on these
three columns and only cost does -- which is what makes it safe for them to be
absent on a catalog written before they existed, on a source that cannot
supply them, and on every model that is not tier three.

Unknown bounds are the widest bounds
------------------------------------

A file whose range is not known is stored as ``[-infinity, +infinity]``, not as
NULL, and ``CONTEXT.md`` 1.17 is why. Both encodings express the same rule --
*if you cannot place it, always select it* -- but only one of them is a plain
conjunctive range, and only a plain conjunctive range can be pruned. Written
the obvious way::

    WHERE min_ts IS NULL OR max_ts IS NULL OR (max_ts >= lo AND min_ts < hi)

the disjunction defeats DuckLake's data-file pruning outright. Inlining is off
(1.7), so this table is one small parquet file per trigger, and selection then
goes **O(files ever consumed)** -- measured at 118 ms against 2,160 consumed
files and climbing, which is exactly the cost class 1.13 says the index exists
to remove. With the sentinel the same rule is::

    WHERE max_ts >= lo AND min_ts < hi

which prunes, and measures flat. The sentinel is not a trick standing in for
NULL: *this file may contain a row at any time* is a true statement about a
file nobody has measured, and stating it that way makes the widest answer fall
out of the ordinary comparison instead of needing a special case that a reader
of the SQL can forget to write.

One consequence worth naming: a genuine event timestamp of exactly
``datetime.max`` is indistinguishable from "unknown", so its file is selected by
every range. That is over-selection, which the contract above already calls
harmless.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, ClassVar, Iterator, Mapping, Sequence

from dataclasses import dataclass

from duckstream.errors import DuckstreamError
from duckstream.offsets import CASE_INSENSITIVE_PATHS, FileEntry, FileOffset
from duckstream.protocols import Offset
from duckstream.sql import quote_literal

__all__ = [
    "CONSUMED_TABLE",
    "ENTRIES_KEY",
    "UNKNOWN_MIN",
    "UNKNOWN_MAX",
    "ConsumedFiles",
    "FileBounds",
    "entries_in",
    "MapIndex",
    "TableIndex",
]


#: Unqualified name of the table. Qualified by the state store, which owns the
#: schema and the catalog -- ``CONTEXT.md`` 1.9 requires it to share both with
#: the sink, because the rows and the output they check point become durable in
#: one transaction or neither does.
CONSUMED_TABLE = "consumed_files"

#: The ``BatchPlan.payload`` key carrying the files this batch consumes, as
#: ``{relpath: {"size": int, "mtime_ns": int}}``. The plan names them and the
#: index writes them, so the two halves of "consumed" -- deciding and recording
#: -- stay one contract rather than two that have to agree.
ENTRIES_KEY = "entries"

#: Prefix of the temporary relations :class:`TableIndex` registers. The uuid4
#: suffix is not decoration: two indexes may share one connection.
_REL_PREFIX = "duckstream_consumed_"

#: The time range recorded for a file whose bounds are not known. **Not NULL**,
#: and that is a measured decision rather than a stylistic one -- see the
#: "Unknown bounds are the widest bounds" section of the module docstring.
#: ``datetime.min`` and ``datetime.max`` are what DuckDB's ``TIMESTAMP
#: '-infinity'`` and ``TIMESTAMP 'infinity'`` read back as, so a row written by
#: SQL, by a bound parameter or through pyarrow lands on the same two values.
UNKNOWN_MIN = datetime.min
UNKNOWN_MAX = datetime.max

#: ``{relpath: (min_ts, max_ts, n_rows)}``. ``None`` in any position means "not
#: known", and every writer here turns that into the widest possible answer
#: rather than into a NULL.
FileBounds = Mapping[str, "tuple[datetime | None, datetime | None, int | None]"]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass(frozen=True)
class ConsumedFile:
    """One consumed file as the time-range index knows it.

    ``min_ts``/``max_ts`` are never ``None`` when they come out of the table --
    an unmeasured file is stored at the sentinel bounds, not at NULL. They are
    typed optional anyway because a catalog written before these columns existed
    has NULL in them until the row is replaced, and a reader that assumed
    otherwise would fail on exactly the upgrade this design is meant to survive.
    """

    relpath: str
    min_ts: datetime | None = None
    max_ts: datetime | None = None
    n_rows: int | None = None

    @property
    def bounded(self) -> bool:
        """Is this file's range actually known, or is it the widest answer?"""
        return (
            self.min_ts is not None
            and self.max_ts is not None
            and (self.min_ts, self.max_ts) != (UNKNOWN_MIN, UNKNOWN_MAX)
        )


def entries_in(payload: Any) -> dict[str, FileEntry]:
    """The consumption records a batch payload declares, or an empty map."""
    if not isinstance(payload, Mapping):
        return {}
    raw = payload.get(ENTRIES_KEY)
    if not raw:
        return {}
    if not isinstance(raw, Mapping):
        raise DuckstreamError(
            f"batch plan payload {ENTRIES_KEY!r} must be a mapping of relative "
            f"path to size and mtime, got {type(raw).__name__}"
        )
    return {
        path: FileOffset.entry(
            entry[FileOffset.SIZE_KEY], entry[FileOffset.MTIME_KEY]
        )
        for path, entry in raw.items()
    }


class MapIndex:
    """The consumed set carried inside the offset, as duckstream v1 stored it.

    Retained for two jobs and no others: reading a v1 offset so it can be
    migrated, and letting :class:`~duckstream.sources.files.FileSource`'s own
    unit tests exercise planning without a catalog. Nothing on the trigger path
    builds one -- see the module docstring for the measurement that decided it.
    """

    #: What :meth:`end_offset` writes. Distinguishes a checkpoint whose set is
    #: inside it from one whose set is in the table.
    version: ClassVar[int] = FileOffset.MAP_VERSION

    def __init__(
        self, consumed: Mapping[str, Mapping[str, Any]] | None = None
    ) -> None:
        self._consumed: dict[str, FileEntry] = {
            path: FileOffset.entry(
                entry[FileOffset.SIZE_KEY], entry[FileOffset.MTIME_KEY]
            )
            for path, entry in (consumed or {}).items()
        }
        self._fold = FileOffset.fold_index(self._consumed)

    @classmethod
    def from_offset(cls, offset: Offset | None) -> "MapIndex":
        """Build from a stored offset. Refuses one whose set is not inside it."""
        return cls(FileOffset.consumed(offset))

    def unconsumed(self, scan: Mapping[str, FileEntry]) -> list[str]:
        return [
            path
            for path, entry in scan.items()
            if not FileOffset.is_consumed(
                self._consumed,
                path,
                entry[FileOffset.SIZE_KEY],
                entry[FileOffset.MTIME_KEY],
                index=self._fold,
            )
        ]

    def end_offset(
        self, start: Offset | None, included: Mapping[str, FileEntry]
    ) -> Offset:
        return FileOffset.merge(start, included)

    def record(
        self,
        batch_id: int,
        payload: Any,
        *,
        start: Offset | None = None,
        end: Offset | None = None,
        bounds: "FileBounds | None" = None,
    ) -> int:
        """Nothing to write: this shape records the set in the offset itself.

        And nothing to verify either — the count and the set cannot disagree
        when they are the same object.

        ``bounds`` is accepted and dropped. The v1 offset has nowhere to put a
        time-range index, and giving it one was measured and rejected: it took
        the encoded offset from 45.1 MB to 71.2 MB, 1.58x worse on the project's
        worst number. That is the reason the index lives in the table, and the
        reason this shape simply does without it -- a model still reading a v1
        offset recomputes from the whole file list, which is slower and right.
        """
        return 0

    def overlapping(self, lo: datetime, hi: datetime) -> list["ConsumedFile"]:
        """Every consumed file, unbounded. The honest answer for this shape.

        There is no time-range index in a v1 offset, so nothing here can narrow
        anything -- and *narrowing is the only thing the index is allowed to
        do*. Returning every consumed file at the sentinel range is therefore
        not a degraded answer, it is the correct one: a recompute reads more
        files than it needs and produces exactly the same numbers.
        """
        return [ConsumedFile(relpath=path) for path in self._consumed]


class TableIndex:
    """The consumed set as rows, answered by the database.

    Bound to one connection and one model, because both are fixed for the life
    of a trigger and threading them through every call would only make the SQL
    harder to read.
    """

    version: ClassVar[int] = FileOffset.ROWS_VERSION

    def __init__(self, con: Any, table: str, model_name: str) -> None:
        self.con = con
        self.table = table
        self.model_name = model_name

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"TableIndex(table={self.table!r}, model={self.model_name!r})"

    # -- reading ----------------------------------------------------------

    def unconsumed(self, scan: Mapping[str, FileEntry]) -> list[str]:
        """Which of ``scan``'s paths were never consumed at that exact identity.

        An empty scan answers itself, and that matters more than it looks:
        ``CONTEXT.md`` 1.8 measured an idle trigger at ~1.3 ms and 1.11 measured
        a state read at ~10 ms, so a quiet stream must not pay a query to be
        told nothing happened.
        """
        if not scan:
            return []
        paths = list(scan)
        mtimes = [scan[path][FileOffset.MTIME_KEY] for path in paths]
        probe = self._arrow(
            paths,
            [scan[path][FileOffset.SIZE_KEY] for path in paths],
            mtimes,
        )
        join_on = "relpath_fold" if CASE_INSENSITIVE_PATHS else "relpath"
        # The join matches mtime_ns exactly, so no consumed row outside the
        # scan's own mtime span can match one. Narrowing to that span is a
        # deduction rather than a heuristic -- it cannot change the answer --
        # and it is what lets DuckLake skip data files on their statistics.
        # Inlined as literals, never as a subquery: CONTEXT.md 1.5.
        window = f"AND c.mtime_ns BETWEEN {min(mtimes)} AND {max(mtimes)}"
        with self._registered(probe) as name:
            rows = self.con.execute(
                f'SELECT s.relpath FROM "{name}" s '
                f"LEFT JOIN {self.table} c "
                f"ON c.model_name = ? "
                f"AND c.{join_on} = s.{join_on} "
                f'AND c."size" = s."size" '
                f"AND c.mtime_ns = s.mtime_ns "
                f"{window} "
                f"WHERE c.relpath IS NULL",
                [self.model_name],
            ).fetchall()
        return [row[0] for row in rows]

    def overlapping(self, lo: datetime, hi: datetime) -> list["ConsumedFile"]:
        """Every consumed file that can possibly hold a row in ``[lo, hi)``.

        The hint, doing its one job. A row survives when ``max_ts >= lo AND
        min_ts < hi``, which is the ordinary half-open overlap test -- and a
        file whose range was never measured is stored as the widest possible
        range, so it satisfies that test for every window and is returned. The
        module docstring has the measurement behind writing it this way rather
        than as an ``IS NULL OR ...`` disjunction; the short version is that the
        disjunction cannot be pruned and turns this back into the O(n) scan the
        index exists to remove.

        The bounds reach SQL as inlined literals, never as a subquery
        (``CONTEXT.md`` 1.5), which is also what lets DuckLake skip this table's
        own data files on their ``min_ts``/``max_ts`` statistics.

        ``DISTINCT`` on the path because one file can hold several rows: a file
        rewritten in place is consumed again at its new identity, and the
        recompute wants to read it once. The widest bounds and the largest row
        count among those rows are the ones that survive, which keeps this an
        over-estimate in both directions -- the safe one for a hint.
        """
        rows = self.con.execute(
            f"SELECT relpath, min(min_ts), max(max_ts), max(n_rows) "
            f"FROM {self.table} "
            f"WHERE model_name = ? "
            f"  AND max_ts >= {quote_literal(lo)} "
            f"  AND min_ts < {quote_literal(hi)} "
            f"GROUP BY relpath "
            f"ORDER BY relpath",
            [self.model_name],
        ).fetchall()
        return [
            ConsumedFile(
                relpath=row[0],
                min_ts=row[1],
                max_ts=row[2],
                n_rows=None if row[3] is None else int(row[3]),
            )
            for row in rows
        ]

    def count(self) -> int:
        """How many consumption records this model has.

        For ``status``, not for the trigger path: it is a full aggregate over
        the table, which is exactly what ``CONTEXT.md`` 1.12 says to keep off
        every tick.
        """
        row = self.con.execute(
            f"SELECT count(*) FROM {self.table} WHERE model_name = ?",
            [self.model_name],
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    # -- writing ----------------------------------------------------------

    def end_offset(
        self, start: Offset | None, included: Mapping[str, FileEntry]
    ) -> Offset:
        """The checkpoint this batch commits: a marker, not a set.

        The count is carried forward from ``start`` rather than read back, for
        the reason ``CONTEXT.md`` 1.10 and 1.11 both landed on -- do not
        re-read state you just wrote. It exists so the engine's stalled-loop
        guard has something that moves and so ``status`` can say how far along a
        model is without an aggregate. The authority on *which* files are
        consumed is the table, always.
        """
        return FileOffset.rows(FileOffset.entry_count(start) + len(included))

    def record(
        self,
        batch_id: int,
        payload: Any,
        *,
        start: Offset | None = None,
        end: Offset | None = None,
        bounds: "FileBounds | None" = None,
    ) -> int:
        """Append the consumption records a batch payload declares. How many.

        Takes the payload rather than the plan because the two callers hold
        different things: the engine has the plan, and ``quarantine`` -- which
        must record the skip in the same transaction that advances past it, or
        the batch is retried for ever -- has only the payload it stored.

        Called **inside a transaction** either way, so the rows become durable
        in the same DuckLake snapshot as whatever they check point. A rolled
        back batch leaves no row behind, which is the property every other
        append-only table here has.

        Given ``start`` and ``end``, it also checks that what was written is
        what the checkpoint claims. See :meth:`_verify` -- that check is not
        defensive tidiness, it closes a real hole.

        ``bounds`` carries the file -> time-range index for the files being
        recorded, and is optional in the strict sense: a caller that has not
        measured them, or cannot, passes nothing and every row lands at the
        sentinel range. Correctness does not depend on it -- see the module
        docstring -- so it is never an error to omit, and a file it does not
        mention is simply one no recompute can rule out.
        """
        written = self.append(batch_id, entries_in(payload), bounds=bounds)
        if start is not None or end is not None:
            self._verify(written, start, end)
        return written

    def _verify(self, written: int, start: Offset | None, end: Offset | None) -> None:
        """Refuse a checkpoint whose count does not match the rows behind it.

        The offset carries a count and the table carries the identities, and
        **only the identities stop the next trigger re-planning the batch**.
        Nothing previously tied the two together: a source that planned files
        and then declared no entries would write no rows, advance the count
        anyway, and be handed the same files again on the next pass -- for ever,
        re-folding them into the mart each time. That is not a stall the
        engine's stalled-loop guard can see, because the guard watches the
        checkpoint move and the checkpoint *does* move.

        Found by mutation audit rather than by review, which is the reason it is
        checked here rather than trusted: the two writes are far enough apart in
        the code that "they obviously agree" is not something a reader can see.
        """
        claimed = FileOffset.entry_count(end) - FileOffset.entry_count(start)
        if written == claimed:
            return
        raise DuckstreamError(
            f"model {self.model_name!r}: the checkpoint says this batch consumed "
            f"{claimed} file(s) but {written} consumption record(s) were written. "
            f"The count is only a report; these rows are the position, and the "
            f"next trigger plans against them — so committing this would either "
            f"re-read {claimed - written} file(s) on every trigger from now on, "
            f"or skip past {written - claimed} that were never read. Refusing "
            f"the batch instead. A source keeping its consumed set as rows must "
            f"declare every file it plans under the {ENTRIES_KEY!r} payload key."
        )

    def append(
        self,
        batch_id: int,
        entries: Mapping[str, FileEntry],
        *,
        bounds: "FileBounds | None" = None,
    ) -> int:
        """Insert one row per entry. The write half of :meth:`unconsumed`.

        A file ``bounds`` says nothing about is written at the sentinel range,
        so it is selected by every recompute rather than by none. That default
        is the whole reason the index can be a hint: forgetting to measure a
        file costs a read, never an answer.
        """
        if not entries:
            return 0
        paths = list(entries)
        probe = self._arrow(
            paths,
            [entries[path][FileOffset.SIZE_KEY] for path in paths],
            [entries[path][FileOffset.MTIME_KEY] for path in paths],
            bounds=bounds,
        )
        with self._registered(probe) as name:
            self.con.execute(
                f"INSERT INTO {self.table} "
                f'(model_name, relpath, relpath_fold, "size", mtime_ns, '
                f"batch_id, consumed_at, min_ts, max_ts, n_rows) "
                f'SELECT ?, s.relpath, s.relpath_fold, s."size", s.mtime_ns, ?, ?, '
                f"       s.min_ts, s.max_ts, s.n_rows "
                f'FROM "{name}" s',
                [self.model_name, int(batch_id), _utcnow()],
            )
        return len(paths)

    # -- internals --------------------------------------------------------

    @staticmethod
    def _arrow(
        paths: Sequence[str],
        sizes: Sequence[int],
        mtimes: Sequence[int],
        *,
        bounds: "FileBounds | None" = None,
    ) -> Any:
        import pyarrow as pa

        bounds = bounds or {}
        lows: list[datetime] = []
        highs: list[datetime] = []
        counts: list[int | None] = []
        for path in paths:
            low, high, rows = bounds.get(path, (None, None, None))
            # A half-known range is treated as unknown in the direction that is
            # missing, never guessed from the other half: a file whose maximum
            # is known and whose minimum is not could still begin anywhere.
            lows.append(UNKNOWN_MIN if low is None else low)
            highs.append(UNKNOWN_MAX if high is None else high)
            counts.append(None if rows is None else int(rows))

        return pa.table(
            {
                "relpath": pa.array(list(paths), type=pa.string()),
                "relpath_fold": pa.array(
                    [path.casefold() for path in paths], type=pa.string()
                ),
                "size": pa.array([int(v) for v in sizes], type=pa.int64()),
                "mtime_ns": pa.array([int(v) for v in mtimes], type=pa.int64()),
                "min_ts": pa.array(lows, type=pa.timestamp("us")),
                "max_ts": pa.array(highs, type=pa.timestamp("us")),
                "n_rows": pa.array(counts, type=pa.int64()),
            }
        )

    @contextmanager
    def _registered(self, probe: Any) -> Iterator[str]:
        name = f"{_REL_PREFIX}{uuid.uuid4().hex}"
        self.con.register(name, probe)
        try:
            yield name
        finally:
            try:
                self.con.unregister(name)
            except Exception:  # pragma: no cover - unregister is total
                pass


class ConsumedFiles:
    """The table, and the indexes over it. Owned by the state store.

    A small object rather than three loose functions, because the qualified
    table name is the one thing every caller needs and the one thing none of
    them should compose for itself.
    """

    def __init__(self, table: str) -> None:
        self.table = table

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"ConsumedFiles({self.table!r})"

    #: Columns carrying the file -> time-range index, and their types. Named
    #: here rather than only in the ``CREATE`` because the state store's
    #: migration adds them to a catalog written before they existed, and two
    #: lists that have to agree should be one list.
    INDEX_COLUMNS: ClassVar[dict[str, str]] = {
        "min_ts": "TIMESTAMP",
        "max_ts": "TIMESTAMP",
        "n_rows": "BIGINT",
    }

    def ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table} (\n"
            f"    model_name VARCHAR,\n"
            f"    relpath VARCHAR,\n"
            f"    relpath_fold VARCHAR,\n"
            f'    "size" BIGINT,\n'
            f"    mtime_ns BIGINT,\n"
            f"    batch_id BIGINT,\n"
            f"    consumed_at TIMESTAMP,\n"
            f"    min_ts TIMESTAMP,\n"
            f"    max_ts TIMESTAMP,\n"
            f"    n_rows BIGINT\n"
            f")"
        )

    def index_for(self, con: Any, model_name: str) -> TableIndex:
        return TableIndex(con, self.table, model_name)
