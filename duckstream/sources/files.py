"""``FileSource`` — directory tailing with completion markers.

The file source is the foundation of duckstream rather than a convenience.
Exactly-once requires a **replayable** source (``PLAN.md``, "Exactly-once"), and
a directory of immutable files is the simplest thing that is genuinely
replayable: given the same ``start`` and ``end`` offsets, :meth:`FileSource.plan`
selects the same files and :meth:`FileSource.bind` reads the same rows. Brokers
that cannot promise that — MQTT above all — are modelled as landing writers
*into* this source.

Three behaviours carry most of the correctness:

**Completion-marker gating.** A directory's files become eligible only once its
marker file exists, optionally after a settle delay. This is the pattern from
this repository's ``realtime_queue_worker.get_ready_folders``, and it is what
stops a half-written parquet file being read as if it were complete. The writer
side of the contract is ``subscriber.py``: write to a temporary path, atomic
rename, **then** drop the marker — never the other order.

**Offsets are a consumed-file map.** See :mod:`duckstream.offsets`. Size and
mtime are tracked per file so a file rewritten in place is re-planned rather
than silently skipped.

**The plan is the contract.** :meth:`bind` registers a view over the explicit
list of files the plan chose, never a glob. A glob would re-resolve at bind time
and could pick up a file that landed between planning and binding, which would
be read but never checkpointed.

Not carried over from the reference implementation
--------------------------------------------------

``CONTEXT.md`` section 5 records two defects in this repository's existing
drivers that must not reappear here:

* No ``fcntl``. This module is pure ``os``/``pathlib`` and imports on Windows.
  v1 is single-writer under ``AvailableNow``, so no lock is needed at all.
* No shared staging name. Every :meth:`bind` creates a **uniquely named** temp
  view; the existing drivers ``CREATE OR REPLACE`` one shared staging table and
  clobber each other under concurrency.
"""

from __future__ import annotations

import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any, ClassVar, Iterator, Mapping, Sequence

from duckstream.errors import ConfigError, DuckstreamError
from duckstream.offsets import FileEntry, FileOffset
from duckstream.protocols import BatchLimits, BatchPlan, Offset

__all__ = ["FileSource", "FORMATS"]


#: Readable formats. Each maps to the DuckDB table function used to bind it and
#: the filename pattern used to discover it.
FORMATS: dict[str, dict[str, str]] = {
    "parquet": {"reader": "read_parquet", "pattern": "*.parquet"},
    "csv": {"reader": "read_csv", "pattern": "*.csv"},
    "json": {"reader": "read_json", "pattern": "*.json"},
}

#: Prefix of the temp views :meth:`FileSource.bind` creates. The uuid4 suffix is
#: the point — see the module docstring.
VIEW_PREFIX = "duckstream_file_batch_"


# -- glob matching --------------------------------------------------------
#
# Patterns are matched against a file's path *relative to the source root*,
# always with POSIX (case-sensitive) semantics, so discovery is identical on
# Windows and on the Pi it deploys to.
#
# `PurePath.match` is deliberately not used. It requires `**` to consume at
# least one directory component, so `pattern: "**/*.parquet"` -- the obvious
# spelling for "recursively" -- silently matches nested files and skips every
# file in the root. A pattern that quietly reads a subset of the tree with no
# error is exactly the failure mode this framework exists to design out.
# `PurePath.full_match` fixes it but is 3.13+, and pyproject declares >=3.11.

def _segment_regex(segment: str) -> str:
    """Translate one path segment of a glob. ``*`` never crosses a ``/``."""
    out: list[str] = []
    i, n = 0, len(segment)
    while i < n:
        char = segment[i]
        i += 1
        if char == "*":
            while i < n and segment[i] == "*":
                i += 1
            out.append("[^/]*")
        elif char == "?":
            out.append("[^/]")
        elif char == "[":
            close = i
            if close < n and segment[close] in "!^":
                close += 1
            if close < n and segment[close] == "]":
                close += 1
            while close < n and segment[close] != "]":
                close += 1
            if close >= n:  # unterminated class: a literal bracket
                out.append(re.escape("["))
            else:
                body = segment[i:close]
                i = close + 1
                if body[:1] in ("!", "^"):
                    body = "^" + body[1:]
                out.append("[" + body.replace("\\", "\\\\") + "]")
        else:
            out.append(re.escape(char))
    return "".join(out)


def _compile_pattern(pattern: str) -> re.Pattern[str]:
    """Compile a glob into a regex over forward-slashed relative paths.

    Semantics, chosen to be the ones a user actually expects:

    * ``**`` matches **zero or more** path segments, so ``**/*.parquet`` covers
      both ``a.parquet`` and ``day=1/a.parquet``.
    * ``*`` and ``?`` never cross a ``/``, so ``sub/*.parquet`` does not reach
      into ``sub/deep/``.
    * A pattern without a leading ``/`` matches from the right, the way
      ``PurePath.match`` does — which is what lets the default ``*.parquet``
      find files at any depth. Lead with ``/`` to anchor at the source root.
    """
    body = pattern[1:] if pattern.startswith("/") else pattern
    anchored = pattern.startswith("/")
    while body.startswith("./"):
        body = body[2:]

    parts = body.split("/")
    out: list[str] = []
    for index, part in enumerate(parts):
        last = index == len(parts) - 1
        if part == "**":
            # Trailing `**` matches the rest of the path, including nothing.
            out.append(".*" if last else "(?:[^/]+/)*")
            continue
        out.append(_segment_regex(part))
        if not last:
            out.append("/")
    prefix = "" if anchored else "(?:.*/)?"
    return re.compile(prefix + "".join(out) + r"\Z")


# -- parquet footer reader ------------------------------------------------
#
# Row limiting needs row counts before the batch is read, and a parquet footer
# carries `num_rows` without touching a single data page. This connection exists
# only for that: in-memory, created on first use, and holding no file open — so
# it cannot take the exclusive database lock that CONTEXT.md section 1.6
# measured, and cannot interfere with the engine's own connection.

_METADATA_LOCK = threading.Lock()
_METADATA_CON: Any = None


def _metadata_connection() -> Any:
    """The lazily created, module-level in-memory connection for footer reads."""
    global _METADATA_CON
    if _METADATA_CON is None:
        import duckdb  # imported lazily: importing this module stays duckdb-free

        _METADATA_CON = duckdb.connect()
    return _METADATA_CON


def _sql_string_literal(value: str) -> str:
    """A single-quoted DuckDB string literal.

    DuckDB follows standard-conforming strings — a backslash is an ordinary
    character, so doubling the single quote is the whole escape. Verified on
    1.5.5 against Windows paths containing both ``\\`` and ``'``.
    """
    return "'" + value.replace("'", "''") + "'"


def _tighter(own: int | None, requested: int | None) -> int | None:
    """The stricter of two limits, where ``None`` means unbounded.

    The direction matters: the caller may tighten the source's own limit but
    must never be able to loosen it, or a model that declared a memory bound
    would quietly exceed it.
    """
    if own is None:
        return requested
    if requested is None:
        return own
    return min(own, requested)


class FileSource:
    """Tails a directory tree, one bounded micro-batch at a time.

    Args:
        path: Root of the landing tree. Files are discovered under it and offset
            paths are recorded relative to it.
        marker: Name of the completion marker file that makes a directory's
            files eligible, e.g. ``"_READY"``. The marker itself is never read
            as data.

            ``marker=None`` **disables gating entirely**. Every matching file is
            eligible the instant it appears on disk, which means duckstream can
            and eventually will read a file that is still being written — a
            truncated parquet footer, a half-flushed CSV row. Only pass ``None``
            when the files are known to appear atomically (for example written
            elsewhere and moved in with a same-filesystem rename).
        settle_seconds: A marker younger than this is treated as not yet ready.
            Guards against a writer that drops the marker before its last write
            is visible, and against clock or network-filesystem skew. ``0.0``
            (the default) means the marker alone gates.
        format: ``"parquet"``, ``"csv"`` or ``"json"``.
        pattern: Glob matched against each file's path relative to ``path``,
            using POSIX (case-sensitive) semantics so discovery does not differ
            between Windows and Linux. Defaults to the format's pattern —
            ``"*.parquet"``, ``"*.csv"``, ``"*.json"``.

            ``**`` matches **zero or more** directories, so ``**/*.parquet``
            finds files in the root as well as nested ones; ``*`` and ``?``
            never cross a ``/``. A pattern matches from the right unless it
            starts with ``/``, which anchors it at ``path``.
        recursive: Walk subdirectories. When ``False``, only files directly in
            ``path`` are considered, gated by ``path``'s own marker.
        max_files_per_trigger: Cap on files per batch.
        max_rows_per_trigger: Cap on rows per batch. **Parquet only in v1** —
            CSV and JSON row counts are not available without reading the files,
            which is exactly what the limit exists to avoid, so for those formats
            this is accepted, recorded for round-tripping, and not enforced. Use
            ``max_files_per_trigger`` there instead.

            A single file larger than the limit is always included on its own,
            rather than being refused forever: a batch that can never make
            progress wedges the pipeline, which is worse than one oversized
            batch.
        base_dir: What a relative ``path`` is resolved against. Defaults to the
            working directory **at construction time**. The YAML loader passes
            the directory holding the config file, so ``path: landing/`` in
            ``/etc/duckstream/models.yaml`` means ``/etc/duckstream/landing``
            whatever directory cron happened to start the process in. It is not
            emitted by :meth:`to_config`: the config records ``path`` as
            written, and where that is anchored is a property of the document,
            not of the declaration.

    Both limits are the memory lever ``CONTEXT.md`` section 1.1 identified.
    Memory is governed by DuckDB's buffer manager materialising the batch, so
    bounding rows in flight is the only control that works — a faster UDF buys
    no headroom.
    """

    type_name: ClassVar[str] = "file"

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        marker: str | None = "_READY",
        settle_seconds: float = 0.0,
        format: str = "parquet",
        pattern: str | None = None,
        recursive: bool = True,
        max_files_per_trigger: int | None = None,
        max_rows_per_trigger: int | None = None,
        base_dir: str | os.PathLike[str] | None = None,
    ) -> None:
        self.path = self._check_path(path)
        self.marker = self._check_marker(marker)
        self.settle_seconds = self._check_settle(settle_seconds)
        self.format = self._check_format(format)
        self._pattern_arg = self._check_pattern(pattern)
        self.recursive = self._check_flag("recursive", recursive)
        self.max_files_per_trigger = self._check_limit(
            "max_files_per_trigger", max_files_per_trigger
        )
        self.max_rows_per_trigger = self._check_limit(
            "max_rows_per_trigger", max_rows_per_trigger
        )
        self.base_dir = self._check_base_dir(base_dir)

        self.pattern = self._pattern_arg or FORMATS[self.format]["pattern"]
        try:
            self._pattern_re = _compile_pattern(self.pattern)
        except re.error as exc:
            # A malformed character class, e.g. '[z-a]'. Refusing it here beats
            # a source that matches nothing and reports an empty batch.
            raise ConfigError(
                f"file source 'pattern' {self.pattern!r} is not a valid glob: {exc}"
            ) from exc
        self._root = self._resolve_root(self.path, self.base_dir)
        self._settle_ns = int(self.settle_seconds * 1_000_000_000)

    @staticmethod
    def _resolve_root(path: str, base_dir: str | None) -> Path:
        """Pin the root to an absolute path, once, at construction time.

        A relative ``path`` left unresolved is silently cwd-dependent: the same
        source object reads a different tree depending on where the process
        started, and when the tree is simply absent it returns an empty batch
        rather than failing. Under cron -- ``duckstream run -c /etc/ds.yaml``
        from an arbitrary working directory -- that is a realistic way to have
        a pipeline that reports success and ingests nothing.

        Resolution happens here rather than in :meth:`_scan` so behaviour is
        fixed when the object is built, not re-decided on every trigger by
        whatever the cwd happens to be by then.
        """
        root = Path(path).expanduser()
        if not root.is_absolute():
            anchor = Path(base_dir).expanduser() if base_dir else Path.cwd()
            root = anchor / root
        return Path(os.path.abspath(root))

    # -- construction-time validation -------------------------------------
    #
    # Eager, because a landing path typo or an unknown format should fail at
    # `duckstream validate` on the deploy box, not at 03:00 in a cron log.

    @staticmethod
    def _check_path(path: str | os.PathLike[str]) -> str:
        if path is None:
            raise ConfigError("file source requires a 'path'; none was given")
        try:
            text = os.fspath(path)
        except TypeError as exc:
            raise ConfigError(
                f"file source 'path' must be a string or path-like, got "
                f"{type(path).__name__}: {path!r}"
            ) from exc
        if isinstance(text, bytes):
            text = text.decode()
        if not text.strip():
            raise ConfigError("file source 'path' must not be empty")
        return text

    @staticmethod
    def _check_marker(marker: str | None) -> str | None:
        if marker is None:
            return None
        if not isinstance(marker, str) or not marker.strip():
            raise ConfigError(
                f"file source 'marker' must be a non-empty file name, or None to "
                f"disable completion gating; got {marker!r}"
            )
        if os.sep in marker or (os.altsep and os.altsep in marker):
            raise ConfigError(
                f"file source 'marker' is a file name looked for inside each "
                f"directory, not a path; got {marker!r}"
            )
        return marker

    @staticmethod
    def _check_settle(settle_seconds: float) -> float:
        if isinstance(settle_seconds, bool) or not isinstance(
            settle_seconds, (int, float)
        ):
            raise ConfigError(
                f"file source 'settle_seconds' must be a number, got "
                f"{type(settle_seconds).__name__}: {settle_seconds!r}"
            )
        if settle_seconds < 0:
            raise ConfigError(
                f"file source 'settle_seconds' must not be negative, got "
                f"{settle_seconds!r}"
            )
        return float(settle_seconds)

    @staticmethod
    def _check_format(fmt: str) -> str:
        if fmt not in FORMATS:
            raise ConfigError(
                f"file source 'format' must be one of "
                f"{', '.join(sorted(FORMATS))}; got {fmt!r}"
            )
        return fmt

    @staticmethod
    def _check_pattern(pattern: str | None) -> str | None:
        if pattern is None:
            return None
        if not isinstance(pattern, str) or not pattern.strip():
            raise ConfigError(
                f"file source 'pattern' must be a non-empty glob, or None to use "
                f"the format default; got {pattern!r}"
            )
        return pattern

    @staticmethod
    def _check_flag(name: str, value: bool) -> bool:
        if not isinstance(value, bool):
            raise ConfigError(
                f"file source {name!r} must be true or false, got "
                f"{type(value).__name__}: {value!r}"
            )
        return value

    @staticmethod
    def _check_base_dir(base_dir: str | os.PathLike[str] | None) -> str | None:
        if base_dir is None:
            return None
        try:
            text = os.fspath(base_dir)
        except TypeError as exc:
            raise ConfigError(
                f"file source 'base_dir' must be a string or path-like, got "
                f"{type(base_dir).__name__}: {base_dir!r}"
            ) from exc
        if isinstance(text, bytes):
            text = text.decode()
        if not text.strip():
            raise ConfigError(
                "file source 'base_dir' must not be empty; pass None to resolve "
                "a relative 'path' against the current working directory"
            )
        return text

    @staticmethod
    def _check_limit(name: str, value: int | None) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigError(
                f"file source {name!r} must be a positive integer or None "
                f"(unbounded), got {type(value).__name__}: {value!r}"
            )
        if value < 1:
            raise ConfigError(
                f"file source {name!r} must be at least 1, got {value!r}. Use "
                f"None for unbounded; 0 would mean no batch can ever make "
                f"progress."
            )
        return value

    # -- Source protocol ---------------------------------------------------

    def latest_offset(self) -> Offset:
        """An offset covering every file that is currently ready to read.

        A directory that does not exist yet is not an error: a landing tree is
        often created by the first writer, and a source that raised here would
        make an empty pipeline fail rather than idle.
        """
        return FileOffset.build(self._scan())

    def plan(
        self,
        start: Offset | None,
        end: Offset,
        limits: BatchLimits | None = None,
    ) -> BatchPlan:
        """Carve a bounded batch out of the files in ``end`` but not ``start``.

        A file counts as unconsumed when it is absent from ``start`` **or** its
        size or mtime differs from what ``start`` recorded — that second clause
        is what catches a file rewritten in place.

        Files are ordered by ``(mtime_ns, relative path)``. Deterministic
        ordering is what makes a replayed batch identical to the original one;
        the path tiebreaker matters because a filesystem can stamp several files
        with the same mtime.

        The returned ``end`` offset covers the batch's starting offset plus
        **only the files actually included**. Never the whole scan: a truncated
        batch that checkpointed the full scan would mark files consumed that it
        never read, and they would be lost.
        """
        limits = limits or BatchLimits()
        start_consumed = FileOffset.consumed(start)
        end_consumed = FileOffset.consumed(end)

        # Built once: on Windows a lookup has to fold case, and folding per
        # file against the whole map would make planning quadratic.
        start_index = FileOffset.fold_index(start_consumed)

        candidates: list[tuple[int, str]] = [
            (entry[FileOffset.MTIME_KEY], relpath)
            for relpath, entry in end_consumed.items()
            if not FileOffset.is_consumed(
                start_consumed,
                relpath,
                entry[FileOffset.SIZE_KEY],
                entry[FileOffset.MTIME_KEY],
                index=start_index,
            )
        ]
        candidates.sort()
        ordered = [relpath for _mtime, relpath in candidates]

        max_files = _tighter(self.max_files_per_trigger, limits.max_files_per_trigger)
        max_rows = _tighter(self.max_rows_per_trigger, limits.max_rows_per_trigger)

        truncated = False
        if max_files is not None and len(ordered) > max_files:
            ordered = ordered[:max_files]
            truncated = True

        row_count: int | None = None
        if ordered and max_rows is not None and self.format == "parquet":
            ordered, row_count, rows_truncated = self._limit_rows(ordered, max_rows)
            truncated = truncated or rows_truncated

        included: dict[str, FileEntry] = {rel: end_consumed[rel] for rel in ordered}
        batch_end = FileOffset.merge(start, included)

        payload: dict[str, Any] = {
            "format": self.format,
            "root": Path(os.path.abspath(os.fspath(self._root))).as_posix(),
            "files": [self._absolute(rel) for rel in ordered],
            "relpaths": list(ordered),
            "row_count": row_count,
        }
        return BatchPlan(
            start=start,
            end=batch_end,
            payload=payload,
            is_empty=not ordered,
            has_more=truncated,
        )

    def bind(self, con: Any, plan: BatchPlan) -> str:
        """Register a temp view over exactly the planned files; return its name.

        The view name carries a ``uuid4`` suffix so two binds on one connection
        cannot collide. ``CONTEXT.md`` section 5 records that this repository's
        existing drivers ``CREATE OR REPLACE`` a single shared staging name and
        clobber each other; that defect is not reproduced here.

        The file list is emitted as an explicit, escaped SQL list literal rather
        than a glob or a concatenated string. A bound parameter would be safer
        still, but DuckDB refuses prepared parameters inside ``CREATE VIEW``
        (``Binder Error: Unexpected prepared parameter``), so the escaping is
        done here and unit-tested against paths containing spaces and quotes.

        Raises:
            DuckstreamError: if the plan is empty. The engine will not call
                ``bind`` for an empty batch, so reaching here means a caller bug
                — and silently binding zero files would hide it behind a view
                that returns nothing.
        """
        files = self._plan_files(plan)
        fmt = plan.payload.get("format", self.format)
        reader = FORMATS.get(fmt, {}).get("reader")
        if reader is None:
            raise DuckstreamError(
                f"batch plan declares format {fmt!r}, which {type(self).__name__} "
                f"cannot bind; expected one of {', '.join(sorted(FORMATS))}"
            )

        file_list = "[" + ", ".join(_sql_string_literal(f) for f in files) + "]"
        view = f"{VIEW_PREFIX}{uuid.uuid4().hex}"
        con.execute(f'CREATE TEMP VIEW "{view}" AS SELECT * FROM {reader}({file_list})')
        return view

    def to_config(self) -> dict[str, Any]:
        """Round-trippable declaration: ``type``, ``path``, and every non-default.

        Only differences from the defaults are emitted, so a config file stays
        readable and a default that changes later is picked up rather than
        frozen into every document ever written. ``marker: None`` *is* a
        difference from the default and is emitted explicitly.
        """
        config: dict[str, Any] = {"type": self.type_name, "path": self.path}
        if self.marker != "_READY":
            config["marker"] = self.marker
        if self.settle_seconds != 0.0:
            config["settle_seconds"] = self.settle_seconds
        if self.format != "parquet":
            config["format"] = self.format
        if self._pattern_arg is not None:
            config["pattern"] = self._pattern_arg
        if self.recursive is not True:
            config["recursive"] = self.recursive
        if self.max_files_per_trigger is not None:
            config["max_files_per_trigger"] = self.max_files_per_trigger
        if self.max_rows_per_trigger is not None:
            config["max_rows_per_trigger"] = self.max_rows_per_trigger
        return config

    # -- equality ----------------------------------------------------------
    #
    # `Model` is a dataclass, so `Model.__eq__` compares sources by value. The
    # config round-trip test in PLAN.md ("Model -> dict -> YAML -> Model must
    # reconstruct an identical object") therefore needs this.

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        return self.to_config() == other.to_config()

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.to_config().items(), key=lambda kv: kv[0])))

    def __repr__(self) -> str:
        config = self.to_config()
        config.pop("type")
        args = ", ".join(f"{k}={v!r}" for k, v in config.items())
        return f"{type(self).__name__}({args})"

    # -- scanning ----------------------------------------------------------

    def _absolute(self, relpath: str) -> str:
        """Absolute path of ``relpath``, forward-slashed.

        DuckDB accepts either separator on Windows, and forward slashes keep the
        JSON payload free of escaped backslashes.
        """
        return Path(FileOffset.resolve_path(self._root, relpath)).as_posix()

    def _scan(self) -> dict[str, FileEntry]:
        """Every ready, matching file under the root, as a consumed map."""
        root = self._root
        if not root.is_dir():
            return {}

        now_ns = time.time_ns()
        found: dict[str, FileEntry] = {}
        for directory in self._directories(root):
            if not self._is_ready(directory, now_ns):
                continue
            for name in self._filenames(directory):
                if self.marker is not None and name == self.marker:
                    continue
                full = directory / name
                relpath = FileOffset.relative_path(root, full)
                if not self._pattern_re.match(relpath):
                    continue
                try:
                    stat = full.stat()
                except OSError:
                    # Vanished between listing and stat. It is simply not part
                    # of this scan; if it comes back it is picked up next time.
                    continue
                found[relpath] = FileOffset.entry(stat.st_size, stat.st_mtime_ns)
        return found

    def _directories(self, root: Path) -> Iterator[Path]:
        """Directories whose marker gates their own files.

        Gating is per containing directory: a file is eligible when the marker
        sits beside it, not when some ancestor is marked. The reference
        implementation in ``realtime_queue_worker`` globs ``**`` from each ready
        folder, which reads files out of *unmarked* subdirectories and visits
        nested ready folders twice.
        """
        if not self.recursive:
            yield root
            return
        for dirpath, dirnames, _filenames in os.walk(root):
            dirnames.sort()  # deterministic traversal; ordering is re-imposed later
            yield Path(dirpath)

    @staticmethod
    def _filenames(directory: Path) -> list[str]:
        try:
            with os.scandir(directory) as entries:
                return sorted(e.name for e in entries if e.is_file())
        except OSError:
            return []

    def _is_ready(self, directory: Path, now_ns: int) -> bool:
        """Whether ``directory``'s completion marker exists and has settled."""
        if self.marker is None:
            return True
        try:
            stat = (directory / self.marker).stat()
        except OSError:
            return False
        if self._settle_ns <= 0:
            return True
        return (now_ns - stat.st_mtime_ns) >= self._settle_ns

    # -- row limiting ------------------------------------------------------

    def _limit_rows(
        self, ordered: Sequence[str], max_rows: int
    ) -> tuple[list[str], int, bool]:
        """Trim ``ordered`` to the row budget. Returns (files, rows, truncated)."""
        counts = self._row_counts(ordered)
        selected: list[str] = []
        total = 0
        for relpath in ordered:
            rows = counts[relpath]
            if selected and total + rows > max_rows:
                return selected, total, True
            selected.append(relpath)
            total += rows
        return selected, total, False

    def _row_counts(self, relpaths: Sequence[str]) -> dict[str, int]:
        """Row count per file, read from parquet footers only.

        ``parquet_file_metadata`` returns ``num_rows`` per file without touching
        a data page, so the cost is one seek per file. (``parquet_metadata``
        gives the same thing per row group, which is more detail than a row
        budget needs.) The ``file_name`` column echoes back exactly the string
        passed in, which is what the mapping below relies on.
        """
        absolute = {relpath: self._absolute(relpath) for relpath in relpaths}
        con = _metadata_connection()
        try:
            with _METADATA_LOCK:
                rows = con.execute(
                    "SELECT file_name, num_rows FROM parquet_file_metadata(?)",
                    [list(absolute.values())],
                ).fetchall()
        except Exception as exc:  # duckdb's exception types are not imported here
            raise DuckstreamError(
                f"could not read parquet footers to enforce "
                f"max_rows_per_trigger on {self.path!r}: {exc}. Every planned "
                f"file must be a readable parquet file; drop "
                f"max_rows_per_trigger to bound the batch by file count instead."
            ) from exc

        by_name = {str(name): int(count) for name, count in rows}
        counts: dict[str, int] = {}
        for relpath, path in absolute.items():
            if path not in by_name:
                raise DuckstreamError(
                    f"parquet footer for {relpath!r} was not returned by "
                    f"parquet_file_metadata; the file may have been removed "
                    f"between planning and reading."
                )
            counts[relpath] = by_name[path]
        return counts

    # -- plan helpers ------------------------------------------------------

    @staticmethod
    def _plan_files(plan: BatchPlan) -> list[str]:
        payload: Mapping[str, Any] = plan.payload or {}
        files = payload.get("files") or []
        if plan.is_empty or not files:
            raise DuckstreamError(
                "FileSource.bind was called with an empty batch plan. The engine "
                "skips bind when BatchPlan.is_empty, so this is a caller bug; "
                "binding zero files would produce a view that silently returns "
                "nothing."
            )
        # A bare string here would otherwise iterate per character and surface
        # as `IO Error: No files found that match the pattern "a"` from deep
        # inside DuckDB. Same reasoning as the format check above: say what is
        # actually wrong with the plan.
        if isinstance(files, (str, bytes)) or not isinstance(files, Sequence):
            raise DuckstreamError(
                f"batch plan payload['files'] must be a list of paths, got "
                f"{type(files).__name__}: {files!r}"
            )
        bad = [f for f in files if not isinstance(f, str)]
        if bad:
            raise DuckstreamError(
                f"batch plan payload['files'] must contain only path strings; "
                f"got {bad[0]!r} ({type(bad[0]).__name__})"
            )
        return list(files)
