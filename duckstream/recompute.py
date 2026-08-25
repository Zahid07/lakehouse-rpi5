"""Tier three: re-deriving a whole window from source, in bounded chunks.

``PLAN.md`` gives tier three one sentence -- *"recompute the affected window
from source; no shortcut exists"* -- and that sentence is the whole design.
An ``additive`` model folds a delta into the stored value and a
``sufficient_statistics`` model folds a mergeable state, but a median, an exact
``COUNT DISTINCT`` or an FFT has **no decomposition at all**: there is no pair
of partial answers that combine into the true one. ``CONTEXT.md`` section 4
records what happens when somebody tries anyway -- an FFT mart in this
repository transformed only each batch's own rows and held a spectrum over half
a window, reporting 51 bins where the truth was 201.

So the affected windows are read again, in full, from the files they came from.
This module answers the two questions that makes practical:

* **which windows did this batch touch?** -- so untouched history is never
  re-read;
* **how much can be recomputed at once?** -- so a window range that turns out
  to be enormous does not take the process down with it.

Where the rows come from
------------------------

They are in files consumed long ago, still on disk and still identifiable,
because the position records **every file consumed** rather than a high-water
mark -- rows in ``duckstream.consumed_files`` since phase 4. That is what makes
tier three possible at all, and it is why the consumed-set shape was settled
before this was built rather than after.

What it is *not* is a licence to hand DuckDB the whole list. ``CONTEXT.md``
1.13 measured statistics pruning skipping data pages but never the file open,
at a flat ~0.1 ms per file listed whether it is read or not: 411 ms unfiltered
against 217 ms filtered at 2,160 files, where reading only the matching files
is 1.7 ms at every corpus size. At one file a trigger on a one-minute schedule
that list is 525,000 files a year, and on a Pi the constant multiplies, because
the cost is small random I/O rather than CPU.

Hence the file -> time-range index on ``consumed_files``, and hence its
contract: it is a **hint, never truth**. It only narrows, it never removes, and
a file it cannot place is a file it returns. Over-selecting reads extra files
and gets the right answer; under-selecting is silently wrong. Correctness never
depends on it and only cost does. :mod:`duckstream.consumed` holds the index
itself and the measurement that decided how "unknown" is encoded.

Sizing a chunk from rows, not from windows
------------------------------------------

``CONTEXT.md`` 1.1 is unambiguous about the lever: the memory ceiling is
DuckDB's buffer manager materialising ``LIST(...)``, not the Python layer --
256 MB with a UDF and 256 MB without one, against 64 MB for a plain
``GROUP BY``. **A faster UDF buys no headroom.** Memory is bounded by rows in
flight per execution and by nothing else, which was re-confirmed here: the same
recompute over 2.4 M rows needs 128 MB in one execution and fits in 64 MB at
400,000 rows a chunk.

That is why chunks are sized from an **estimated row count** and not from a
fixed number of windows, which ``PLAN.md`` calls out directly: window density
varies enormously between a quiet interval and a saturated one, so "five
windows" is not a memory bound at all.

The estimate is deliberately an **upper** bound -- the total rows in every file
a range selects, rather than an attempt to apportion each file across the
windows it spans. The asymmetry is the same one the index has: over-estimating
costs an extra chunk, and under-estimating costs the out-of-memory failure the
budget was set to prevent. When the row count of a selected file is not known,
the estimate is unknown too, and an unknown estimate takes the smallest chunk
rather than the largest.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Sequence

from duckstream.consumed import ConsumedFile
from duckstream.sql import quote_ident
from duckstream.windows import grain_interval, window_end

__all__ = ["WindowChunk", "touched_windows", "plan_chunks"]


@dataclass(frozen=True)
class WindowChunk:
    """One bounded unit of recompute work: a half-open range and its files.

    ``lo`` and ``hi`` are window boundaries, so a row belongs to exactly one
    chunk and no window is ever split across two. Splitting a window would be
    the tier-three bug in miniature -- two partial recomputes of one window,
    each overwriting the other.
    """

    lo: datetime
    hi: datetime
    windows: int
    relpaths: tuple[str, ...]
    estimated_rows: int | None

    def __len__(self) -> int:
        return self.windows


def touched_windows(
    con: Any, view: str, time_column: str, grain: str
) -> list[datetime]:
    """The distinct windows this batch has rows in, oldest first.

    The planning step for a tier-three model, and the reason untouched history
    is never re-read: a batch that landed in one hour causes one hour to be
    recomputed, whatever else the stream holds.

    Undated rows are excluded rather than grouped into a NULL window. A row
    with no event time belongs to no window, so it cannot have touched one --
    the engine has already counted it as ``rows_undated`` and dropped it, and
    the same rule has to hold here or a NULL window would be "recomputed" into
    the mart.

    Read from the view the sink is about to be given, which for a model with a
    lateness horizon is the **on-time** view: rows whose window had already
    sealed have been filtered out upstream, so a sealed window is not reopened
    and rewritten by a late arrival. Sealing stays a one-way door.
    """
    column = quote_ident(time_column)
    rows = con.execute(
        f"SELECT DISTINCT date_trunc('{grain}', {column}) AS w "
        f"FROM {quote_ident(view)} WHERE {column} IS NOT NULL ORDER BY w"
    ).fetchall()
    grain_interval(grain)  # refuse an unknown grain rather than trusting the SQL
    return [row[0] for row in rows if row[0] is not None]


def plan_chunks(
    windows: Sequence[datetime],
    grain: str,
    *,
    files_for: Any,
    own: Sequence[ConsumedFile] = (),
    max_rows: int | None = None,
) -> list[WindowChunk]:
    """Group ``windows`` into the largest ranges that stay inside ``max_rows``.

    ``files_for`` is called once with the whole touched span and returns the
    :class:`~duckstream.consumed.ConsumedFile` rows overlapping it -- one query
    rather than one per window, because ``CONTEXT.md`` 1.12's rule applies to
    this loop as much as to any other: do not read a state table row by row.

    ``own`` is the batch's **own** files, and passing them here rather than
    merging them into the result afterwards is load-bearing rather than tidy.
    They are recorded in the same transaction as the write, so the index cannot
    be relied on to know about them yet -- on a first batch, and on a replay
    after a crash, it does not. Left out, their rows are missing from every
    estimate, so a batch's own data is precisely the data the row budget fails
    to bound: the chunk planner would hand back one unbounded chunk for exactly
    the rows guaranteed to be read. Under-estimating is the direction that runs
    out of memory.

    With no budget there is one chunk covering everything, which is the
    "unbounded" half of ``PLAN.md``'s *chunked equals unchunked* property. With
    a budget, windows are packed greedily in time order, and a single window
    that exceeds the budget on its own still becomes its own chunk rather than
    being refused -- the same call the file source makes about an oversized
    file, and for the same reason: a batch that can never make progress wedges
    the pipeline, which is worse than one oversized execution.

    Adjacency is not required and not assumed. Two windows an hour apart with
    nothing between them can share a chunk, and the empty stretch between them
    costs nothing to include -- there are no rows there to read, and the files
    are selected by the range either way.
    """
    if not windows:
        return []
    ordered = sorted(windows)
    span_lo = ordered[0]
    span_hi = window_end(ordered[-1], grain)
    candidates = _merge(files_for(span_lo, span_hi), own)

    if max_rows is None or max_rows <= 0:
        return [_chunk(ordered, span_lo, span_hi, candidates)]

    chunks: list[WindowChunk] = []
    pending: list[datetime] = []
    for window in ordered:
        trial = pending + [window]
        estimate = _estimate(trial, grain, candidates)
        if pending and (estimate is None or estimate > max_rows):
            chunks.append(_chunk(pending, pending[0], window_end(pending[-1], grain),
                                 candidates))
            pending = [window]
        else:
            pending = trial
    if pending:
        chunks.append(
            _chunk(pending, pending[0], window_end(pending[-1], grain), candidates)
        )
    return chunks


def _merge(
    indexed: Iterable[ConsumedFile], own: Iterable[ConsumedFile]
) -> list[ConsumedFile]:
    """One entry per path, at the widest bounds and largest row count seen.

    A file can appear in both lists -- a replay re-plans files the index already
    holds -- and reading it twice would double its estimate and could double its
    rows if the two entries disagreed about the range. Merging to the widest and
    largest keeps the result an over-estimate in both directions, which is the
    safe one for a hint.
    """
    merged: dict[str, ConsumedFile] = {}
    for candidate in list(indexed) + list(own):
        seen = merged.get(candidate.relpath)
        if seen is None:
            merged[candidate.relpath] = candidate
            continue
        merged[candidate.relpath] = ConsumedFile(
            relpath=candidate.relpath,
            min_ts=_least(seen.min_ts, candidate.min_ts),
            max_ts=_most(seen.max_ts, candidate.max_ts),
            n_rows=(
                None
                if seen.n_rows is None or candidate.n_rows is None
                else max(seen.n_rows, candidate.n_rows)
            ),
        )
    return [merged[path] for path in sorted(merged)]


def _least(a: datetime | None, b: datetime | None) -> datetime | None:
    if a is None or b is None:
        return None
    return min(a, b)


def _most(a: datetime | None, b: datetime | None) -> datetime | None:
    if a is None or b is None:
        return None
    return max(a, b)


def _selected(
    lo: datetime, hi: datetime, candidates: Iterable[ConsumedFile]
) -> list[ConsumedFile]:
    """The candidates whose range can overlap ``[lo, hi)``.

    The same half-open overlap test the index applies in SQL, applied again in
    Python so a chunk narrower than the whole touched span reads fewer files
    without a second query. A candidate with an unknown bound is kept, because
    an unknown bound is the widest one.
    """
    keep = []
    for candidate in candidates:
        if candidate.max_ts is not None and candidate.max_ts < lo:
            continue
        if candidate.min_ts is not None and candidate.min_ts >= hi:
            continue
        keep.append(candidate)
    return keep


def _estimate(
    windows: Sequence[datetime], grain: str, candidates: Iterable[ConsumedFile]
) -> int | None:
    """Upper bound on rows a chunk over ``windows`` would read. ``None`` if unknown.

    Every row of every selected file, with no attempt to apportion a file
    across the windows it spans. That over-counts a file wider than the chunk,
    and over-counting is the direction that fails safe -- see the module
    docstring.
    """
    lo, hi = windows[0], window_end(windows[-1], grain)
    return _sum_rows(_selected(lo, hi, candidates))


def _chunk(
    windows: Sequence[datetime],
    lo: datetime,
    hi: datetime,
    candidates: Sequence[ConsumedFile],
) -> WindowChunk:
    selected = _selected(lo, hi, candidates)
    return WindowChunk(
        lo=lo,
        hi=hi,
        windows=len(windows),
        relpaths=tuple(sorted({candidate.relpath for candidate in selected})),
        estimated_rows=_sum_rows(selected),
    )


def _sum_rows(selected: Sequence[ConsumedFile]) -> int | None:
    """Total rows in ``selected``, or ``None`` if any of them is unmeasured."""
    total = 0
    for candidate in selected:
        if candidate.n_rows is None:
            return None
        total += candidate.n_rows
    return total
