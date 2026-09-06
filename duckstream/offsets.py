"""Offset encoding, and the shape of the file source's offset.

An :data:`~duckstream.protocols.Offset` is a plain ``dict[str, Any]``: JSON
serialisable, source-defined, and opaque to the engine. The engine's only
requirement is that it survives a round trip through the state store's text
column unchanged, because the offset is committed inside the same transaction
as the output rows (``PLAN.md``, "Exactly-once"). That is what
:func:`encode_offset` and :func:`decode_offset` guarantee.

Encoding is **deterministic** — ``sort_keys=True`` and compact separators — so
the same logical offset always produces the same string. Two things depend on
that: comparing a stored offset to a freshly computed one is a string compare,
and a checkpoint row does not churn when nothing has changed.

The file offset shape
---------------------

There are two, and which one a stored offset uses says where its consumed set
lives::

    v2, what duckstream writes now:
    {"kind": "file", "v": 2, "entries": 41230}

    v1, still readable so it can be migrated:
    {"kind": "file", "v": 1,
     "consumed": {"<relative/path>": {"size": 123, "mtime_ns": 456}}}

**v2 carries no set.** The consumed files are rows in
``duckstream.consumed_files`` (see :mod:`duckstream.consumed`), because
``CONTEXT.md`` 1.15 and 1.16 measured v1's map at **45.7 MB encoded after a year
at one file a minute, rewritten in full on every trigger** -- 7.97 MB reaching
the disk each time, ~11.2 GB of writes a day, and the largest single obstacle to
running duckstream unattended on a Pi. As rows it is 4.9 KB a trigger. ``entries``
is a count carried forward from the previous checkpoint: it moves so the
engine's stalled-loop guard has something to compare, and it is a report, never
the authority. The table is the authority.

:meth:`FileOffset.consumed` therefore **refuses** a v2 offset rather than
returning an empty map. Answering "nothing has been consumed" would replay the
whole landing tree and fold every row a second time, which is precisely the
silent wrong answer this framework exists to remove.

Both shapes describe a **consumed-file set, not a high-water mark**, and that
difference is load bearing:

* Replay is exact. Re-planning from a stored offset yields precisely the files
  that were never consumed, so a crash mid-batch cannot skip a file that landed
  out of mtime order while the batch was running.
* ``size`` and ``mtime_ns`` together detect a file **rewritten in place**. A
  path-only set would treat a rewritten file as already consumed and silently
  lose its new contents. This mirrors the pattern in this repository's
  ``realtime_queue_worker.upsert_queue_jobs``, which re-queues a job when either
  attribute changes.

Paths are stored **relative to the source root, with forward slashes**. Two
consequences, both deliberate: an offset stays valid when the landing tree is
moved or the deployment root differs, and an offset written on Windows means the
same thing on Linux.

Case
----

Paths are stored **as written**, but compared the way the local filesystem
compares them: case-insensitively on Windows (:data:`CASE_INSENSITIVE_PATHS`),
case-sensitively elsewhere. The asymmetry is not sloppiness — it is the only
correct reading of each platform.

On Windows, ``A.parquet`` and ``a.parquet`` are one file. Comparing keys exactly
would make a case-only rename look like a brand new file: the same bytes get
read a second time (double-counted output rows) and the old key is stranded in
the map forever. On Linux those are genuinely two files, and folding them would
turn a real second file into a *skip* — losing data, which is strictly worse
than reading twice. So the fold follows the filesystem rather than picking one
answer for both.

The v1 limit, and why the obvious fix was not taken
---------------------------------------------------

v1's map grew with the number of files ever consumed and was rewritten whole on
every checkpoint. :data:`FileOffset.HIGH_WATER_KEY` (``"high_water_mtime_ns"``)
was reserved for collapsing old entries behind a mark, and that is the fix that
was **not** taken: it bounds the map, and it also makes a file arriving with an
mtime older than the mark disappear without a word. The key stays reserved and
unused, and remains a key a reader must tolerate the absence of.

What replaced it is :mod:`duckstream.consumed` — the set as rows, so the
checkpoint stops carrying it at all. The one thing that had to survive the
change is stated at the top: a v2 offset is not an empty v1 offset, and asking
it for a consumed map is an error rather than a silence.
"""

from __future__ import annotations

import json
import os
from pathlib import Path, PurePath
from typing import Any, Mapping

from duckstream.errors import DuckstreamError
from duckstream.protocols import Offset

__all__ = [
    "encode_offset",
    "decode_offset",
    "FileOffset",
    "FileEntry",
    "CASE_INSENSITIVE_PATHS",
]


#: Whether the local filesystem treats paths differing only by case as the same
#: file. Windows does; POSIX does not. See "Case" in the module docstring for
#: why this is read from the platform rather than fixed one way.
CASE_INSENSITIVE_PATHS = os.name == "nt"


#: One file's identity within a consumed map: its byte size and mtime in
#: nanoseconds. Both are needed — a rewrite that preserves length still moves
#: the mtime, and a rewrite within the filesystem's mtime granularity still
#: usually changes the length.
FileEntry = dict[str, int]


def encode_offset(offset: Offset) -> str:
    """Serialise an offset to deterministic JSON text.

    ``sort_keys=True`` makes the output a pure function of the offset's content,
    so ``encode_offset(a) == encode_offset(b)`` is a sound equality test and a
    checkpoint write is a no-op when nothing changed.

    ``allow_nan=False`` is set on purpose: ``NaN`` and ``Infinity`` are not
    valid JSON, and a state store round trip through a strict parser would fail
    later, far from the source that produced them. Fail here instead.

    Raises:
        DuckstreamError: if ``offset`` is not a mapping, or contains anything
            that is not JSON serialisable. Deliberately not the bare
            ``TypeError`` ``json`` raises — that message names the offending
            type but not the fact that an offset was being written.
    """
    if not isinstance(offset, Mapping):
        raise DuckstreamError(
            f"an offset must be a JSON object (dict), got {type(offset).__name__}: "
            f"{offset!r}"
        )
    try:
        return json.dumps(
            offset,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise DuckstreamError(
            f"offset is not JSON-serialisable ({exc}); offsets are persisted as "
            f"JSON text in the state store, so every value must be a string, "
            f"number, bool, None, list or dict. Offset was: {offset!r}"
        ) from exc


def decode_offset(text: str) -> Offset:
    """Parse offset text produced by :func:`encode_offset`.

    Raises:
        DuckstreamError: if the text is not valid JSON or does not decode to a
            JSON object.
    """
    if not isinstance(text, str):
        raise DuckstreamError(
            f"offset text must be a string, got {type(text).__name__}: {text!r}"
        )
    try:
        value = json.loads(text)
    except ValueError as exc:
        raise DuckstreamError(
            f"stored offset is not valid JSON ({exc}); the state store holds "
            f"offsets as JSON text. Value was: {text!r}"
        ) from exc
    if not isinstance(value, dict):
        raise DuckstreamError(
            f"stored offset decoded to {type(value).__name__}, expected a JSON "
            f"object. Value was: {text!r}"
        )
    return value


class FileOffset:
    """Constructors and accessors for the file source's offset shape.

    A namespace of static methods rather than a type: the offset stays a plain
    ``dict`` at every boundary — the state store persists JSON text and the
    engine treats offsets as opaque — so nothing outside this module and
    :mod:`duckstream.sources.files` should need to know the layout.
    """

    KIND = "file"

    #: The shape that carries its consumed set inside itself. Still read, so a
    #: catalog written by an earlier duckstream migrates instead of replaying.
    MAP_VERSION = 1

    #: The shape that keeps its set in ``duckstream.consumed_files`` and carries
    #: only a count. What :meth:`rows` writes and what every commit now stores.
    ROWS_VERSION = 2

    #: Highest version this duckstream can read. A stored offset above it is
    #: refused rather than guessed at.
    VERSION = ROWS_VERSION

    KIND_KEY = "kind"
    VERSION_KEY = "v"
    CONSUMED_KEY = "consumed"

    #: v2's payload: how many consumption records exist for this model. A
    #: report and an advance marker, never the authority -- see the module
    #: docstring.
    ENTRIES_KEY = "entries"

    #: Reserved for the pruning described in the module docstring. Unused in v1;
    #: a writer that starts setting it must bump :attr:`VERSION`.
    HIGH_WATER_KEY = "high_water_mtime_ns"

    SIZE_KEY = "size"
    MTIME_KEY = "mtime_ns"

    # -- construction -----------------------------------------------------

    @staticmethod
    def entry(size: int, mtime_ns: int) -> FileEntry:
        """One consumed-map value."""
        return {FileOffset.SIZE_KEY: int(size), FileOffset.MTIME_KEY: int(mtime_ns)}

    @staticmethod
    def empty() -> Offset:
        """An offset that has consumed nothing — what a model starts from."""
        return FileOffset.build({})

    @staticmethod
    def rows(entries: int) -> Offset:
        """A v2 checkpoint: the set is in the table, this is the count of it.

        ``entries`` counts consumption *records*, not distinct paths, so it
        rises by one for a file rewritten in place as well as for a new one.
        That is deliberate — it is what makes it strictly increase on every
        committed batch, which the engine's stalled-loop guard relies on.
        """
        count = int(entries)
        if count < 0:
            raise DuckstreamError(
                f"a file offset cannot have consumed {count} entries; the count "
                f"only ever advances, so a negative value means it was computed "
                f"from something other than the previous checkpoint"
            )
        return {
            FileOffset.KIND_KEY: FileOffset.KIND,
            FileOffset.VERSION_KEY: FileOffset.ROWS_VERSION,
            FileOffset.ENTRIES_KEY: count,
        }

    @staticmethod
    def build(consumed: Mapping[str, Mapping[str, Any]]) -> Offset:
        """Assemble a **v1** file offset from a consumed map.

        Still the shape :meth:`~duckstream.sources.files.FileSource.latest_offset`
        returns, and that is not a leftover: what is *on disk right now* really
        is a map, it is never checkpointed, and it never leaves the pair of
        calls that plan a batch. What gets stored is :meth:`rows`.

        Values are normalised to exactly ``{"size": int, "mtime_ns": int}`` so
        that an offset built here and one decoded from the state store encode
        identically.
        """
        normalised: dict[str, FileEntry] = {}
        for relpath, entry in consumed.items():
            if not isinstance(relpath, str) or not relpath:
                raise DuckstreamError(
                    f"file offset paths must be non-empty strings, got {relpath!r}"
                )
            normalised[relpath] = FileOffset._normalise_entry(relpath, entry)
        return {
            FileOffset.KIND_KEY: FileOffset.KIND,
            FileOffset.VERSION_KEY: FileOffset.MAP_VERSION,
            FileOffset.CONSUMED_KEY: normalised,
        }

    @staticmethod
    def merge(
        base: Offset | None, entries: Mapping[str, Mapping[str, Any]]
    ) -> Offset:
        """``base`` with ``entries`` applied on top.

        This is how a batch's ``end`` offset is built: the offset the batch
        resumed from, plus **only** the files the batch actually included. A
        rewritten file's entry is overwritten, which is what makes the next scan
        see it as consumed at its new size and mtime.
        """
        merged = FileOffset.consumed(base)
        index = FileOffset.fold_index(merged)
        for relpath, entry in entries.items():
            stored = index.get(FileOffset.fold(relpath))
            if stored is not None and stored != relpath:
                # A case-only rename on a case-insensitive filesystem. Drop the
                # old spelling rather than keeping both: two keys for one file
                # is how the consumed map leaks entries that can never be
                # reclaimed. The newest spelling wins, because it is what a
                # subsequent scan will report.
                del merged[stored]
            merged[relpath] = FileOffset._normalise_entry(relpath, entry)
            index[FileOffset.fold(relpath)] = relpath
        return FileOffset.build(merged)

    # -- inspection -------------------------------------------------------

    @staticmethod
    def is_file_offset(offset: Any) -> bool:
        """True when ``offset`` looks like a file offset of a readable version."""
        return (
            isinstance(offset, Mapping)
            and offset.get(FileOffset.KIND_KEY) == FileOffset.KIND
            and isinstance(offset.get(FileOffset.VERSION_KEY), int)
            and offset[FileOffset.VERSION_KEY] <= FileOffset.VERSION
        )

    @staticmethod
    def consumed(offset: Offset | None) -> dict[str, FileEntry]:
        """The consumed map, as a fresh mutable dict. ``None`` means empty.

        Only a **v1** offset has one. A v2 offset keeps its set in
        ``duckstream.consumed_files``, so this refuses rather than answering
        ``{}`` — see :meth:`version_of`.

        Raises:
            DuckstreamError: if ``offset`` is not a file offset, was written by
                a newer duckstream, or is a v2 offset whose set is elsewhere.
                Failing loudly on an unreadable offset is deliberate — silently
                treating it as empty would replay the entire landing tree.
        """
        if offset is None:
            return {}
        version = FileOffset.version_of(offset)
        if version >= FileOffset.ROWS_VERSION:
            raise DuckstreamError(
                f"this file offset (v{version}) does not carry a consumed map: "
                f"its consumed files are rows in the state store's "
                f"{__name__.rsplit('.', 1)[0]}.consumed_files table, because "
                f"carrying them here cost 7.97 MB of writes per trigger after a "
                f"year (CONTEXT.md 1.16). Ask the consumed-file index instead of "
                f"the offset. Returning an empty map here would look like a "
                f"model that has consumed nothing and replay the whole landing "
                f"tree."
            )
        raw = offset.get(FileOffset.CONSUMED_KEY, {})
        if not isinstance(raw, Mapping):
            raise DuckstreamError(
                f"file offset {FileOffset.CONSUMED_KEY!r} must be an object, got "
                f"{type(raw).__name__}"
            )
        return {
            relpath: FileOffset._normalise_entry(relpath, entry)
            for relpath, entry in raw.items()
        }

    @staticmethod
    def version_of(offset: Offset | None) -> int:
        """Which file-offset shape ``offset`` is, validated. ``None`` is v2.

        A missing offset reports the **current** version rather than v1: a model
        that has never committed starts on the shape duckstream writes today,
        and reporting v1 would send a fresh model down the migration path.

        Raises:
            DuckstreamError: if it is not a file offset at all, or names a
                version this duckstream cannot read. Both are refusals rather
                than guesses, because every wrong guess here re-reads data.
        """
        if offset is None:
            return FileOffset.ROWS_VERSION
        if not isinstance(offset, Mapping):
            raise DuckstreamError(
                f"expected a file offset object, got {type(offset).__name__}: "
                f"{offset!r}"
            )
        kind = offset.get(FileOffset.KIND_KEY)
        if kind != FileOffset.KIND:
            raise DuckstreamError(
                f"offset kind is {kind!r}, expected {FileOffset.KIND!r}. The "
                f"stored offset was written by a different source type; point "
                f"the model at the source that wrote it, or reset the checkpoint."
            )
        version = offset.get(FileOffset.VERSION_KEY)
        if not isinstance(version, int) or version > FileOffset.VERSION:
            raise DuckstreamError(
                f"file offset version {version!r} is newer than this duckstream "
                f"understands (v{FileOffset.VERSION}). Upgrade duckstream rather "
                f"than resetting the checkpoint, which would replay everything."
            )
        return version

    @staticmethod
    def entry_count(offset: Offset | None) -> int:
        """How many consumption records ``offset`` says exist behind it.

        Defined for both shapes so a v1 offset migrates to a v2 one whose count
        is right from the first batch rather than restarting at zero. For v1
        that means the size of its map; for v2 the number it carries.
        """
        version = FileOffset.version_of(offset)
        if offset is None:
            return 0
        if version >= FileOffset.ROWS_VERSION:
            raw = offset.get(FileOffset.ENTRIES_KEY, 0)
            if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
                raise DuckstreamError(
                    f"file offset {FileOffset.ENTRIES_KEY!r} must be a "
                    f"non-negative integer, got {raw!r}"
                )
            return raw
        return len(FileOffset.consumed(offset))

    @staticmethod
    def fold(relpath: str) -> str:
        """The comparison form of ``relpath`` for the local filesystem.

        Identity on POSIX, case-folded on Windows. Keys are still *stored* as
        written — only comparison folds.
        """
        return relpath.casefold() if CASE_INSENSITIVE_PATHS else relpath

    @staticmethod
    def fold_index(consumed: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
        """Map each key's comparison form back to the key as stored.

        Built once per plan rather than folded per lookup, so matching a scan
        against the consumed map stays linear instead of quadratic.
        """
        if not CASE_INSENSITIVE_PATHS:
            return {relpath: relpath for relpath in consumed}
        return {relpath.casefold(): relpath for relpath in consumed}

    @staticmethod
    def is_consumed(
        consumed: Mapping[str, Mapping[str, Any]],
        relpath: str,
        size: int,
        mtime_ns: int,
        index: Mapping[str, str] | None = None,
    ) -> bool:
        """True when ``relpath`` was consumed **at this exact size and mtime**.

        A file whose size or mtime moved is *not* consumed: it was rewritten in
        place and its new contents have never been read.

        ``index`` is an optional prebuilt :meth:`fold_index` over ``consumed``;
        pass it when checking many paths against the same map. Without it the
        lookup is still correct, just built per call.
        """
        entry = consumed.get(relpath)
        if entry is None and CASE_INSENSITIVE_PATHS:
            if index is None:
                index = FileOffset.fold_index(consumed)
            stored = index.get(FileOffset.fold(relpath))
            if stored is not None:
                entry = consumed.get(stored)
        if entry is None:
            return False
        return (
            entry.get(FileOffset.SIZE_KEY) == int(size)
            and entry.get(FileOffset.MTIME_KEY) == int(mtime_ns)
        )

    # -- paths ------------------------------------------------------------

    @staticmethod
    def relative_path(root: str | os.PathLike[str], path: str | os.PathLike[str]) -> str:
        """``path`` relative to ``root``, with forward slashes.

        Forward slashes are not cosmetic: they are what makes an offset written
        by a Windows dev box and one written by the Raspberry Pi it deploys to
        compare equal.

        Case is preserved as written. Comparison folds it or not according to
        the platform -- see :meth:`fold` and "Case" in the module docstring.
        """
        rel = os.path.relpath(os.fspath(path), os.fspath(root))
        return PurePath(rel).as_posix()

    @staticmethod
    def resolve_path(root: str | os.PathLike[str], relpath: str) -> str:
        """The absolute filesystem path of ``relpath`` under ``root``."""
        return os.path.abspath(os.path.join(os.fspath(root), Path(relpath)))

    # -- internals --------------------------------------------------------

    @staticmethod
    def _normalise_entry(relpath: str, entry: Mapping[str, Any]) -> FileEntry:
        if not isinstance(entry, Mapping):
            raise DuckstreamError(
                f"file offset entry for {relpath!r} must be an object with "
                f"{FileOffset.SIZE_KEY!r} and {FileOffset.MTIME_KEY!r}, got "
                f"{entry!r}"
            )
        try:
            size = int(entry[FileOffset.SIZE_KEY])
            mtime_ns = int(entry[FileOffset.MTIME_KEY])
        except KeyError as exc:
            raise DuckstreamError(
                f"file offset entry for {relpath!r} is missing key {exc.args[0]!r}; "
                f"expected {{'{FileOffset.SIZE_KEY}': int, "
                f"'{FileOffset.MTIME_KEY}': int}}, got {entry!r}"
            ) from exc
        except (TypeError, ValueError) as exc:
            raise DuckstreamError(
                f"file offset entry for {relpath!r} has non-integer size or "
                f"mtime: {entry!r}"
            ) from exc
        return {FileOffset.SIZE_KEY: size, FileOffset.MTIME_KEY: mtime_ns}
