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

::

    {"kind": "file", "v": 1,
     "consumed": {"<relative/path>": {"size": 123, "mtime_ns": 456}}}

This is a **consumed-file map, not a high-water mark**, and the difference is
load bearing:

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

Known v1 limit
--------------

The consumed map **grows with the number of files ever consumed**. For a landing
directory that is drained and pruned this is bounded by the retention window,
but for an append-only tree it grows without limit, and the whole map is
rewritten on every checkpoint.

The fix is pruning: once every file older than some mtime is known to have been
consumed, those entries collapse into a single high-water mark and only files
newer than it need individual tracking. The key :data:`FileOffset.HIGH_WATER_KEY`
(``"high_water_mtime_ns"``) is **reserved for that** and is deliberately unused
in v1 — readers must tolerate its absence, and a future writer that sets it must
bump ``v``.
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
    VERSION = 1

    KIND_KEY = "kind"
    VERSION_KEY = "v"
    CONSUMED_KEY = "consumed"

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
    def build(consumed: Mapping[str, Mapping[str, Any]]) -> Offset:
        """Assemble a file offset from a consumed map.

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
            FileOffset.VERSION_KEY: FileOffset.VERSION,
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

        Raises:
            DuckstreamError: if ``offset`` is not a file offset, or was written
                by a newer duckstream. Failing loudly on an unreadable offset is
                deliberate — silently treating it as empty would replay the
                entire landing tree.
        """
        if offset is None:
            return {}
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
