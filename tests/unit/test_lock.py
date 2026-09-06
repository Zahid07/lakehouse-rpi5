"""The run lock: one writer per catalog, and a message that says so.

The property under test is not really mutual exclusion -- DuckDB's own file lock
on the catalog already provides that, and ``CONTEXT.md`` 1.6 measured it. It is
*diagnosis*. Two overlapping cron ticks used to surface as::

    Binder Error: Failed to attach DuckLake MetaData "__ducklake_metadata_lake"
    at path "…" Unique file handle conflict: Cannot attach …

which describes a metadata handle rather than the thing that happened. So the
tests below care as much about what the error says as about when it is raised,
and about the two ways a lock can be wrong in the other direction: refusing
forever after a crash, or breaking a lock that is genuinely held.
"""

from __future__ import annotations

import json
import os
import socket
import time

import pytest

from duckstream.lock import LockError, RunLock, lock_path_for


@pytest.fixture
def catalog(tmp_path):
    return str(tmp_path / "catalog.ducklake")


def write_holder(lock: RunLock, **fields) -> None:
    payload = {"pid": os.getpid(), "host": socket.gethostname(), "started_at": time.time()}
    payload.update(fields)
    with open(lock.path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)


# --------------------------------------------------------------------------
# Naming
# --------------------------------------------------------------------------


def test_the_lock_sits_beside_the_catalog(catalog):
    """Discoverable. A lock in a temp directory is invisible to an operator."""
    assert lock_path_for(catalog) == os.path.abspath(catalog) + ".lock"


def test_the_ducklake_prefix_is_not_part_of_the_path(tmp_path):
    """``ducklake:x`` and ``x`` name one catalog, so they must share one lock."""
    plain = str(tmp_path / "c.ducklake")
    assert lock_path_for(f"ducklake:{plain}") == lock_path_for(plain)


def test_a_dsn_catalog_takes_no_file_lock():
    """A postgres or sqlite DSN is not a path and has no sibling to write.

    The catalog server is the thing arbitrating access in that case, so a local
    file would be describing a guarantee it does not provide.
    """
    assert lock_path_for("ducklake:postgres://host/db") == ""
    assert RunLock("ducklake:postgres://host/db").acquire().path == ""


# --------------------------------------------------------------------------
# Holding
# --------------------------------------------------------------------------


def test_a_second_run_is_refused_while_the_first_holds_it(catalog):
    first = RunLock(catalog).acquire()
    try:
        with pytest.raises(LockError) as excinfo:
            RunLock(catalog).acquire()
    finally:
        first.release()

    message = str(excinfo.value)
    assert str(os.getpid()) in message, "the message must name the holder"
    assert socket.gethostname() in message
    assert "max_batches" in message, (
        "the message must say what to do about it, not merely that it happened"
    )
    assert excinfo.value.holder["pid"] == os.getpid()


def test_releasing_lets_the_next_run_in(catalog):
    RunLock(catalog).acquire().release()
    second = RunLock(catalog).acquire()
    assert second._held
    second.release()


def test_the_context_manager_releases_on_the_way_out(catalog):
    with RunLock(catalog) as lock:
        assert os.path.exists(lock.path)
    assert not os.path.exists(lock.path)


def test_the_lock_is_released_even_when_the_body_raises(catalog):
    lock = RunLock(catalog)
    with pytest.raises(RuntimeError):
        with lock:
            raise RuntimeError("boom")
    assert not os.path.exists(lock.path), (
        "a failed run left its lock behind, so every later run is refused"
    )


def test_releasing_twice_is_not_an_error(catalog):
    lock = RunLock(catalog).acquire()
    lock.release()
    lock.release()


def test_a_disabled_lock_takes_nothing(catalog):
    """``lock=False`` is for a second engine sharing a catalog inside one process."""
    lock = RunLock(catalog, enabled=False).acquire()
    assert lock.path == ""
    assert not os.path.exists(str(catalog) + ".lock")
    RunLock(catalog).acquire().release()  # and it did not block a real one


# --------------------------------------------------------------------------
# Stale locks
# --------------------------------------------------------------------------


def test_a_lock_whose_owner_is_gone_is_broken_automatically(catalog):
    """A lock needing manual clearing after every crash is worse than none.

    The fault-injection tests kill processes on purpose and an OOM killer does
    it by accident, so a leftover lock file is an ordinary event rather than an
    exceptional one.
    """
    lock = RunLock(catalog)
    os.makedirs(os.path.dirname(lock.path), exist_ok=True)
    write_holder(lock, pid=999_999)  # a pid that is not running

    lock.acquire()
    assert lock._held
    assert json.load(open(lock.path, encoding="utf-8"))["pid"] == os.getpid()
    lock.release()


def test_a_lock_held_by_a_live_process_is_never_broken(catalog):
    lock = RunLock(catalog)
    os.makedirs(os.path.dirname(lock.path), exist_ok=True)
    write_holder(lock, pid=os.getpid())  # this very process is alive
    with pytest.raises(LockError):
        lock.acquire()


def test_a_lock_from_another_host_is_never_broken(catalog):
    """A pid is only meaningful on the machine that wrote it.

    On a shared filesystem, guessing that a foreign pid is dead is how a second
    writer gets started against a live one.
    """
    lock = RunLock(catalog)
    os.makedirs(os.path.dirname(lock.path), exist_ok=True)
    write_holder(lock, pid=999_999, host="some-other-box")
    with pytest.raises(LockError) as excinfo:
        lock.acquire()
    assert "some-other-box" in str(excinfo.value)


def test_an_unreadable_lock_is_treated_as_held(catalog):
    """"I cannot tell who holds this" is the worst reason to start a writer."""
    lock = RunLock(catalog)
    os.makedirs(os.path.dirname(lock.path), exist_ok=True)
    with open(lock.path, "w", encoding="utf-8") as handle:
        handle.write("{ not json")
    with pytest.raises(LockError):
        lock.acquire()


def test_the_lock_directory_is_created_if_it_does_not_exist(tmp_path):
    """The catalog's directory may not exist yet on a first ever run."""
    lock = RunLock(str(tmp_path / "nested" / "deep" / "c.ducklake"))
    lock.acquire()
    assert os.path.exists(lock.path)
    lock.release()
