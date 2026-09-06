"""One writer at a time, and a clear message when there is not.

``CONTEXT.md`` 2.5 says contention is "structurally impossible" under an
``AvailableNow`` trigger. That is very nearly true and it is not quite true, and
the gap is worth stating precisely because it is the shape of a real 3am
incident.

``AvailableNow`` drains until the source is empty. Given a backlog it can take
longer than the cron interval that started it, and then the next tick begins
while the first is still running. What stops the two corrupting each other is
not the trigger: it is the DuckDB file lock on the DuckLake catalog
(``CONTEXT.md`` 1.6 -- while one process holds a DuckDB file no other can open
it, even read-only). Measured on 1.5.5, the second process gets::

    Binder Error: Failed to attach DuckLake MetaData "__ducklake_metadata_lake"
    at path "…/catalog.ducklake" Unique file handle conflict: Cannot attach …

No corruption, which is the important half. But that message reaches an
operator through a cron log, at 3am, describing a metadata handle rather than
the thing that actually happened, which is that two copies of their pipeline are
running at once.

So this module takes the lock **first**, before the catalog is touched, and says
so in words. It is advisory -- the catalog's own lock is still what guarantees
safety -- and that division is deliberate: an advisory lock that is trusted for
safety is a lock that fails open on any file-system it does not fully
understand.

Portability
-----------

No ``fcntl``. ``CONTEXT.md`` section 5 records that the reference implementation
in this repository cannot even be *imported* on Windows because of it, and the
package's hard rules forbid repeating that. The primitive here is
``os.open(..., O_CREAT | O_EXCL)``, which is atomic on both Windows and POSIX
and needs nothing beyond the standard library.

Stale locks
-----------

A process killed hard -- which the fault-injection tests do on purpose, and
which an OOM killer does by accident -- leaves its lock file behind. A lock that
needs manual clearing after every crash is worse than no lock, so the file
records the pid and the host that took it, and a lock whose pid is no longer
alive on this host is broken automatically and the fact reported. A lock held by
a *live* process is never broken, and a lock from another host is never broken
either, because liveness cannot be checked from here.
"""

from __future__ import annotations

import errno
import json
import os
import socket
import time
from dataclasses import dataclass
from typing import Any

from duckstream.errors import DuckstreamError

__all__ = ["LockError", "RunLock", "lock_path_for"]

#: Suffix appended to the catalog path to name its lock file.
LOCK_SUFFIX = ".lock"


class LockError(DuckstreamError):
    """Another run holds this catalog."""

    def __init__(self, message: str, *, holder: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.holder = holder or {}


def lock_path_for(catalog: Any) -> str:
    """The lock file that guards ``catalog``.

    Beside the catalog, named after it. A lock in a temp directory would be
    invisible to an operator looking at the deployment, and one keyed by a hash
    of the path would not survive the path being spelled two different ways.
    """
    target = str(catalog)
    for prefix in ("ducklake:", "DUCKLAKE:"):
        if target.startswith(prefix):
            target = target[len(prefix) :]
            break
    # A DSN (postgres://…, sqlite:…) is not a path and cannot carry a sibling
    # file; the catalog server is then the thing arbitrating access anyway.
    if "://" in target:
        return ""
    return os.path.abspath(target) + LOCK_SUFFIX


def _alive(pid: int) -> bool:
    """Is ``pid`` a live process on this machine?

    ``os.kill(pid, 0)`` is the POSIX idiom and works on Windows too for the
    "does it exist" question, raising ``OSError`` for a pid that is gone and
    ``PermissionError`` for one owned by somebody else -- which still means
    alive, so it counts as held.
    """
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except OSError:
        return False
    except Exception:  # pragma: no cover - defensive
        return True
    return True


@dataclass
class RunLock:
    """An advisory, cooperative lock on one catalog.

    Used as a context manager by :class:`~duckstream.engine.Engine`::

        with RunLock(catalog) as lock:
            ...                       # drain

    Taking a lock that is already held raises :class:`LockError` naming the pid,
    the host and how long it has been running, which is the message the DuckDB
    handle conflict could not give.
    """

    catalog: Any
    enabled: bool = True

    def __post_init__(self) -> None:
        self.path = lock_path_for(self.catalog) if self.enabled else ""
        self._held = False

    # -- taking and releasing ------------------------------------------

    def acquire(self) -> "RunLock":
        if not self.path:
            return self
        payload = json.dumps(
            {
                "pid": os.getpid(),
                "host": socket.gethostname(),
                "started_at": time.time(),
            }
        ).encode("utf-8")
        try:
            self._create(payload)
        except FileExistsError:
            holder = self._read_holder()
            if not self._break_if_stale(holder):
                raise self._conflict(holder) from None
            try:
                self._create(payload)
            except FileExistsError:
                # Someone else won the race to replace the stale lock. That is
                # a genuine conflict with a live process, not a stale file.
                raise self._conflict(self._read_holder()) from None
        self._held = True
        return self

    def _create(self, payload: bytes) -> None:
        directory = os.path.dirname(self.path)
        if directory and not os.path.isdir(directory):
            os.makedirs(directory, exist_ok=True)
        handle = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(handle, payload)
        finally:
            os.close(handle)

    def release(self) -> None:
        """Drop the lock. Never raises -- releasing is cleanup, not work."""
        if not self._held or not self.path:
            return
        self._held = False
        try:
            os.unlink(self.path)
        except OSError as exc:  # pragma: no cover - best effort
            if exc.errno != errno.ENOENT:
                pass

    # -- diagnosis ------------------------------------------------------

    def _read_holder(self) -> dict[str, Any]:
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                loaded = json.load(handle)
        except (OSError, ValueError):
            # An unreadable or half-written lock file is still a lock. It is
            # not broken automatically, because "I cannot tell who holds this"
            # is the worst possible reason to start a second writer.
            return {}
        return loaded if isinstance(loaded, dict) else {}

    def _break_if_stale(self, holder: dict[str, Any]) -> bool:
        """Remove a lock whose owner is provably gone. Returns whether it did.

        Provably is doing real work here. The pid is only meaningful on the host
        that wrote it, so a lock from another machine is left alone however old
        it looks -- a shared filesystem is exactly where guessing would start a
        second writer against a live one.
        """
        pid = holder.get("pid")
        host = holder.get("host")
        if not isinstance(pid, int) or host != socket.gethostname():
            return False
        if _alive(pid):
            return False
        try:
            os.unlink(self.path)
        except OSError:  # pragma: no cover - lost the race, treat as held
            return False
        return True

    def _conflict(self, holder: dict[str, Any]) -> LockError:
        pid = holder.get("pid", "unknown")
        host = holder.get("host", "unknown host")
        started = holder.get("started_at")
        age = ""
        if isinstance(started, (int, float)):
            seconds = max(0, int(time.time() - started))
            age = f", running for {seconds}s"
        return LockError(
            f"another duckstream run already holds this catalog: pid {pid} on "
            f"{host}{age} (lock file {self.path!r}).\n"
            f"An AvailableNow run drains until the source is empty, so a backlog "
            f"can make one tick outlast the interval that started it and the "
            f"next tick then overlaps. Two writers cannot share a DuckLake "
            f"catalog -- DuckDB's own file lock would refuse the second one "
            f"anyway, with a message about a metadata handle rather than about "
            f"this.\n"
            f"Either let the running pass finish, or bound how long a tick may "
            f"take with AvailableNow(max_batches=N) so it cannot outrun your "
            f"schedule. If pid {pid} is genuinely gone, delete the lock file.",
            holder=holder,
        )

    # -- context manager ------------------------------------------------

    def __enter__(self) -> "RunLock":
        return self.acquire()

    def __exit__(self, *exc_info: Any) -> None:
        self.release()
