"""Landing writers: turning an unreplayable stream into a replayable one.

``CONTEXT.md`` section 4 settles what MQTT is and is not. It is **not a source**,
and it cannot be made into one: once a message is acked it is gone from the
broker, so there is no offset to resume from and nothing to replay. Exactly-once
is unformable directly. What is formable is the two-step duckstream actually
ships — land the messages **durably** first, at least once, and then let the
replayable :class:`~duckstream.sources.files.FileSource` read that landing tree
exactly once.

This module is the first step. It has no opinion about MQTT: it takes records,
buffers them, and lands them the one way that is safe to read concurrently.

Why "at-least-once" has to be built rather than asserted
--------------------------------------------------------

The reference implementation in this repository (``subscriber.py``, and
``CONTEXT.md`` section 5 points at it for the *write* pattern, which is right)
buffers messages in memory and lets the client acknowledge them on arrival.
That is at-**most**-once for anything still in the buffer: the broker has been
told the message was handled, the process dies, and the message is gone. Nothing
reports it, because from the broker's side nothing went wrong.

So the buffer here hands back an **acknowledgement token** per record and
releases them only from :meth:`LandingWriter.flush`, after the completion marker
is on disk. A caller that acks when told to cannot lose an acked message. A
caller that acks earlier has chosen at-most-once and should say so out loud.

The write order, which is the whole of the durability
-----------------------------------------------------

Temp path, ``os.replace``, **then** the marker. Never the other order, and never
a marker beside a file still being written::

    landing/20260601T120000_123456_a1b2c3/data.parquet.tmp   <- written
    landing/20260601T120000_123456_a1b2c3/data.parquet       <- renamed, atomic
    landing/20260601T120000_123456_a1b2c3/_READY             <- marker, last

``os.replace`` is atomic on POSIX and on Windows, so a reader either sees the
whole file or does not see it at all. The marker is what makes the directory
eligible, so a crash at any point leaves an unmarked directory that the file
source ignores for ever — visible litter rather than a half-read batch. That is
the right way round: ``PLAN.md``'s trap 7 records that a fixture which appends
to an already-marked directory produces a genuine double-count that looks
exactly like an engine bug.

**One directory per flush, always.** A marker means "this directory is complete",
so nothing may be added to it afterwards — the file source's scan is entitled to
rely on that, and phase 4's scan work does. Appending a second file to a marked
directory is the trap, not a shortcut.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from duckstream.errors import ConfigError, DuckstreamError

__all__ = ["LandedBatch", "LandingWriter", "MARKER"]

#: The completion marker duckstream's file source looks for by default. The
#: writer and the reader agree on it here rather than by convention.
MARKER = "_READY"

#: Suffix of the file being written. It deliberately does **not** match the file
#: source's default `*.parquet` pattern, so a temp file is not eligible even for
#: the instant it exists beside a marker that should not be there yet.
_TEMP_SUFFIX = ".tmp"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class LandedBatch:
    """What one :meth:`LandingWriter.flush` put on disk.

    ``tokens`` is the point of the whole class. They are whatever the caller
    passed alongside each record — for MQTT, the message ids it must acknowledge
    — and they come back only once the marker exists. Acking them is then safe
    by construction; acking anything earlier is a choice to lose data on a crash.
    """

    directory: Path
    path: Path
    rows: int
    tokens: tuple[Any, ...] = ()

    def __len__(self) -> int:
        return self.rows


class LandingWriter:
    """Buffer records and land them as one complete, marked directory.

    Args:
        path: Root of the landing tree. The same directory a
            :class:`~duckstream.sources.files.FileSource` is pointed at.
        marker: Completion-marker name. Must match the source's.
        flush_rows: Land once this many records are buffered. ``None`` disables
            the size trigger.
        flush_seconds: Land once the oldest buffered record is this old.
            ``None`` disables the time trigger.
        filename: Name of the data file inside each landed directory.
        base_dir: What a relative ``path`` is resolved against, defaulting to
            the working directory at construction. Same rule, and the same
            reason, as :class:`~duckstream.sources.files.FileSource`.

    At least one of ``flush_rows`` and ``flush_seconds`` must be set. A writer
    with neither never lands anything on its own, and would look like it was
    working right up until the buffer exhausted memory.

    Not thread-safe on purpose. A subscriber typically has one network thread
    and one writer thread, and the queue between them is the synchronisation --
    putting a lock in here as well would suggest it is safe to share, which is a
    harder promise than it sounds and one nothing here needs.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        marker: str | None = MARKER,
        flush_rows: int | None = 10_000,
        flush_seconds: float | None = 60.0,
        filename: str = "data.parquet",
        base_dir: str | os.PathLike[str] | None = None,
    ) -> None:
        if flush_rows is None and flush_seconds is None:
            raise ConfigError(
                "a landing writer needs flush_rows, flush_seconds, or both. "
                "With neither it never lands anything by itself, which looks "
                "exactly like working until the buffer exhausts memory."
            )
        if flush_rows is not None and flush_rows <= 0:
            raise ConfigError(f"flush_rows must be positive, got {flush_rows!r}")
        if flush_seconds is not None and flush_seconds <= 0:
            raise ConfigError(
                f"flush_seconds must be positive, got {flush_seconds!r}"
            )
        if not filename or "/" in filename or "\\" in filename:
            raise ConfigError(
                f"filename must be a plain file name, got {filename!r}"
            )
        if marker is not None and filename == marker:
            raise ConfigError(
                f"filename and marker are both {marker!r}; the marker would be "
                f"overwritten by the data file and the directory would announce "
                f"itself complete while being written"
            )

        self.path = os.fspath(path)
        self.marker = marker
        self.flush_rows = flush_rows
        self.flush_seconds = flush_seconds
        self.filename = filename
        self.base_dir = None if base_dir is None else os.fspath(base_dir)
        self._root = Path(
            os.path.abspath(
                self.path
                if self.base_dir is None
                else os.path.join(self.base_dir, self.path)
            )
        )
        self._records: list[Mapping[str, Any]] = []
        self._tokens: list[Any] = []
        self._opened_at: datetime | None = None

    # -- buffering ---------------------------------------------------------

    def add(self, record: Mapping[str, Any], token: Any = None) -> None:
        """Buffer one record, and the token that releases it.

        ``token`` is returned by :meth:`flush` once the record is durable, and
        never before. For MQTT it is the message id to acknowledge. ``None`` is
        fine for a caller with nothing to release.
        """
        if not isinstance(record, Mapping):
            raise DuckstreamError(
                f"a landed record must be a mapping of column name to value, "
                f"got {type(record).__name__}. A landing writer builds columns "
                f"from keys, so a scalar or a list has no shape it can write."
            )
        if self._opened_at is None:
            self._opened_at = _utcnow()
        self._records.append(record)
        self._tokens.append(token)

    def extend(
        self, records: Iterable[Mapping[str, Any]], tokens: Iterable[Any] | None = None
    ) -> None:
        """:meth:`add` for many. ``tokens`` must match ``records`` in length."""
        records = list(records)
        if tokens is None:
            tokens = [None] * len(records)
        else:
            tokens = list(tokens)
            if len(tokens) != len(records):
                raise DuckstreamError(
                    f"got {len(records)} record(s) and {len(tokens)} token(s); "
                    f"they must correspond, or an acknowledgement releases the "
                    f"wrong message"
                )
        for record, token in zip(records, tokens):
            self.add(record, token)

    # -- the flush decision ------------------------------------------------

    @property
    def pending(self) -> int:
        """Records buffered and not yet durable."""
        return len(self._records)

    def due(self, now: datetime | None = None) -> bool:
        """Has a flush trigger fired?

        Checked by the caller rather than by a timer inside this object. A
        writer that flushed on its own schedule would need a thread, and a
        thread here would be a second place the process can die with data in it.
        """
        if not self._records:
            return False
        if self.flush_rows is not None and len(self._records) >= self.flush_rows:
            return True
        if self.flush_seconds is not None and self._opened_at is not None:
            now = now or _utcnow()
            if (now - self._opened_at).total_seconds() >= self.flush_seconds:
                return True
        return False

    # -- landing -----------------------------------------------------------

    def flush(self) -> LandedBatch | None:
        """Land everything buffered. ``None`` when there was nothing.

        Returns only after the marker is on disk, so the tokens it hands back
        are safe to acknowledge. An empty buffer writes **nothing** -- not an
        empty directory and not a marker -- because an empty marked directory is
        a batch the file source would plan, bind and find empty, and because it
        would be one more directory on the scan's critical path for ever
        (``CONTEXT.md`` 1.20).

        The buffer is cleared only on success. A failed write leaves the records
        buffered, so the next flush retries them and nothing is acknowledged in
        between -- which is what makes a full disk a delay rather than a loss.
        """
        if not self._records:
            return None

        directory = self._root / self._directory_name()
        directory.mkdir(parents=True, exist_ok=False)
        target = directory / self.filename
        temp = directory / f"{self.filename}{_TEMP_SUFFIX}"

        rows = len(self._records)
        self._write(temp, self._records)
        # Atomic on POSIX and on Windows: a reader sees the whole file or no
        # file. This is the line the marker below is allowed to depend on.
        os.replace(temp, target)
        if self.marker is not None:
            (directory / self.marker).write_bytes(b"")

        tokens = tuple(self._tokens)
        self._records = []
        self._tokens = []
        self._opened_at = None
        return LandedBatch(
            directory=directory, path=target, rows=rows, tokens=tokens
        )

    def close(self) -> LandedBatch | None:
        """Land whatever is buffered. Call it on shutdown, or lose the buffer."""
        return self.flush()

    def __enter__(self) -> "LandingWriter":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        # Only on a clean exit. Landing a partial buffer while unwinding from an
        # error would turn "this batch failed" into "this batch was fine and
        # shorter than it should have been", which is the quieter mistake.
        if exc_info[0] is None:
            self.close()

    # -- internals ---------------------------------------------------------

    def _directory_name(self) -> str:
        """A unique, sortable directory name: UTC instant plus a random tail.

        Sortable because a landing tree read by a human is read in time order.
        Random-tailed because two writers, or one writer restarted inside the
        same microsecond, must not collide -- and ``mkdir(exist_ok=False)``
        turns a collision into an error rather than into two batches quietly
        sharing a directory and one marker.
        """
        stamp = _utcnow().strftime("%Y%m%dT%H%M%S_%f")
        return f"{stamp}_{uuid.uuid4().hex[:8]}"

    def _write(self, temp: Path, records: Sequence[Mapping[str, Any]]) -> None:
        """Write ``records`` to ``temp`` as parquet, keeping **every** key.

        ``pyarrow`` rather than pandas: it is already a dependency.

        The union of keys is computed here rather than left to
        ``pa.Table.from_pylist``, and that is not tidiness. ``from_pylist``
        infers its schema from the **first record only**, so a field that
        appears later in the batch is *silently dropped* -- the write succeeds,
        the file looks fine, and the column is simply not there. A sensor that
        starts reporting a battery level half way through a flush would lose it
        with nothing to say so, which is section 4's failure class arriving in
        the writer. A test pins it.

        Keys keep first-seen order, so a stable stream produces stable column
        order and the parquet files stay comparable by eye. A record missing a
        key lands NULL there, which is what SQL means by "this message did not
        say" -- and a stream whose shape drifts is a fact about the data, not an
        error to raise at 03:00.

        Schema drift **between** landed files is a different matter and is the
        reader's problem, not the writer's. Landing them is still the right
        thing: the data exists and refusing to write it loses it.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        columns: dict[str, None] = {}
        for record in records:
            for key in record:
                columns.setdefault(key, None)
        rows = [
            {key: record.get(key) for key in columns} for record in records
        ]
        pq.write_table(pa.Table.from_pylist(rows), temp)

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        return (
            f"LandingWriter({self.path!r}, flush_rows={self.flush_rows!r}, "
            f"flush_seconds={self.flush_seconds!r}, pending={self.pending})"
        )
