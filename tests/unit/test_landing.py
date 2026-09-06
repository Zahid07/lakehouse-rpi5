"""The landing writer: the half of phase 5 that carries the guarantee.

MQTT cannot be a source — `CONTEXT.md` section 4 — so duckstream lands messages
durably and reads the landing tree with a replayable file source. Everything
that makes that safe is here rather than in the MQTT adapter, so all of it is
tested without a broker.

Two properties, and neither is decoration:

**The write order is the durability.** Temp path, `os.replace`, *then* the
marker. A crash at any point must leave a directory the file source ignores,
never a marked directory holding a half-written file. Getting this backwards
does not fail — it produces a batch that reads short, which is `PLAN.md`'s trap
7 arriving from the writer's side instead of the fixture's.

**Tokens are released only once the marker exists.** That is the difference
between at-least-once and at-most-once, and it is a difference nothing observes
until a process dies with a full buffer. The reference implementation in this
repository acks on arrival and would lose exactly that buffer.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import duckdb
import pytest

from duckstream.errors import ConfigError, DuckstreamError
from duckstream.landing import MARKER, LandingWriter
from duckstream.sources.files import FileSource


def reading(index: int, sensor: str = "s1") -> dict:
    return {
        "event_ts": f"2026-06-01T10:00:{index:02d}",
        "sensor_id": sensor,
        "value": float(index),
    }


def landed_directories(root: Path) -> list[Path]:
    return sorted(p for p in root.iterdir() if p.is_dir())


# ==========================================================================
# The write order
# ==========================================================================


def test_a_landed_directory_holds_the_data_file_and_the_marker(tmp_path):
    writer = LandingWriter(tmp_path / "landing", flush_rows=2)
    writer.extend([reading(0), reading(1)])
    batch = writer.flush()

    assert batch is not None
    assert batch.rows == 2
    assert batch.path.is_file()
    assert (batch.directory / MARKER).is_file()
    assert not list(batch.directory.glob("*.tmp")), "a temp file survived"


def test_the_marker_is_written_after_the_data_file(tmp_path, monkeypatch):
    """Ordering is the guarantee, so it is asserted rather than assumed.

    Observed at the **rename**, which is the only moment that distinguishes the
    two orders. An earlier version of this test hooked `_write` instead and
    passed either way -- both the rename and the marker happen after `_write`
    returns, so it was asserting something true of the defect as well. The
    mutation audit found that; reading the test did not.
    """
    import duckstream.landing as landing

    root = tmp_path / "landing"
    writer = LandingWriter(root, flush_rows=1)
    marker_at_rename: list[bool] = []
    real_replace = os.replace

    def observing(src, dst):
        marker_at_rename.append((Path(dst).parent / MARKER).exists())
        return real_replace(src, dst)

    monkeypatch.setattr(landing.os, "replace", observing)
    writer.add(reading(0))
    batch = writer.flush()

    assert marker_at_rename == [False], (
        "the marker existed before the data file was in place, so a reader "
        "could have planned a directory whose data file was not there yet"
    )
    assert (batch.directory / MARKER).is_file(), "the marker was never written"


def test_a_failed_write_leaves_no_marker_and_keeps_the_records(tmp_path):
    """A full disk must be a delay, not a loss.

    Nothing is acknowledged, because nothing is returned; and the records stay
    buffered so the next flush retries them. The directory left behind is
    unmarked, so the file source ignores it for ever -- litter, not data loss
    and not a short batch.
    """
    root = tmp_path / "landing"
    writer = LandingWriter(root, flush_rows=1)
    writer.extend([reading(0), reading(1)], tokens=["a", "b"])

    def explode(temp, records):
        raise OSError("no space left on device")

    writer._write = explode
    with pytest.raises(OSError):
        writer.flush()

    assert writer.pending == 2, "records were dropped by a failed write"
    for directory in landed_directories(root):
        assert not (directory / MARKER).exists(), "a failed write marked ready"

    # And the retry succeeds, landing everything that was buffered.
    writer._write = LandingWriter._write.__get__(writer)
    batch = writer.flush()
    assert batch is not None and batch.rows == 2
    assert batch.tokens == ("a", "b")


def test_each_flush_gets_its_own_directory(tmp_path):
    """A marker means "complete", so a landed directory never gains a file.

    The file source's scan is entitled to rely on that, and phase 4's scan work
    does. Appending a second file to a marked directory is trap 7.
    """
    root = tmp_path / "landing"
    writer = LandingWriter(root, flush_rows=1)
    for index in range(3):
        writer.add(reading(index))
        writer.flush()

    directories = landed_directories(root)
    assert len(directories) == 3
    for directory in directories:
        assert sorted(p.name for p in directory.iterdir()) == [
            MARKER, "data.parquet",
        ]


def test_directory_names_sort_in_time_order(tmp_path):
    root = tmp_path / "landing"
    writer = LandingWriter(root, flush_rows=1)
    names = []
    for index in range(4):
        writer.add(reading(index))
        names.append(writer.flush().directory.name)
    assert names == sorted(names), "landed directories do not sort by time"
    assert len(set(names)) == 4, "two flushes shared a directory name"


# ==========================================================================
# Acknowledgement: the at-least-once half
# ==========================================================================


def test_tokens_come_back_only_after_the_marker_exists(tmp_path):
    """The whole of at-least-once, in one assertion.

    A caller that acks what `flush` returns cannot lose an acked message,
    because the marker was already on disk when it was told.
    """
    root = tmp_path / "landing"
    writer = LandingWriter(root, flush_rows=3)
    marker_present_when_released: list[bool] = []

    real_flush = writer.flush

    def observed():
        batch = real_flush()
        if batch is not None:
            marker_present_when_released.append(
                (batch.directory / MARKER).is_file()
            )
        return batch

    writer.extend([reading(i) for i in range(3)], tokens=[1, 2, 3])
    batch = observed()

    assert batch.tokens == (1, 2, 3)
    assert marker_present_when_released == [True]


def test_nothing_is_released_until_a_flush_happens(tmp_path):
    """Buffered is not durable, and the writer never pretends otherwise."""
    writer = LandingWriter(tmp_path / "landing", flush_rows=100)
    writer.extend([reading(i) for i in range(5)], tokens=list(range(5)))
    assert writer.pending == 5
    assert not (tmp_path / "landing").exists(), "a buffer created a directory"


def test_records_and_tokens_must_correspond(tmp_path):
    """A mismatch would acknowledge the wrong message, which is silent."""
    writer = LandingWriter(tmp_path / "landing", flush_rows=10)
    with pytest.raises(DuckstreamError, match="correspond"):
        writer.extend([reading(0), reading(1)], tokens=["only-one"])


# ==========================================================================
# The flush decision
# ==========================================================================


def test_an_empty_flush_writes_nothing_at_all(tmp_path):
    """Not an empty directory, and not a marker.

    An empty marked directory is a batch the engine would plan and bind and find
    empty, and it is one more directory on the scan's critical path for ever
    (`CONTEXT.md` 1.20).
    """
    root = tmp_path / "landing"
    writer = LandingWriter(root, flush_rows=1)
    assert writer.flush() is None
    assert not root.exists() or landed_directories(root) == []


def test_the_row_trigger_fires_at_the_threshold(tmp_path):
    writer = LandingWriter(tmp_path / "landing", flush_rows=3, flush_seconds=None)
    for index in range(2):
        writer.add(reading(index))
        assert not writer.due()
    writer.add(reading(2))
    assert writer.due()


def test_the_time_trigger_fires_on_the_oldest_record(tmp_path):
    """Age is measured from the oldest buffered record, not the newest.

    From the newest, a topic publishing steadily just under the threshold would
    never flush at all -- its buffer would grow for ever while every individual
    record stayed young.

    **Two records, at two different ages, and that is the whole point.** With a
    single buffered record the oldest and the newest are the same record, so a
    one-record test cannot tell the two rules apart -- the same shape as trap
    16. The first version of this test used one record and a mutation resetting
    the clock on every `add` survived it.
    """
    writer = LandingWriter(
        tmp_path / "landing", flush_rows=None, flush_seconds=60.0
    )
    writer.add(reading(0))
    opened = writer._opened_at
    assert not writer.due(now=opened + dt.timedelta(seconds=59))

    # A second record arrives 59 seconds later. The buffer is still 59 seconds
    # old, so nothing has changed yet...
    writer.add(reading(1))
    assert writer._opened_at == opened, (
        "a later record reset the clock; the buffer's age is its oldest record"
    )
    assert not writer.due(now=opened + dt.timedelta(seconds=59))

    # ...and one second after that the *buffer* is due, even though the newest
    # record in it is one second old.
    assert writer.due(now=opened + dt.timedelta(seconds=60))


def test_a_writer_with_no_trigger_at_all_is_refused(tmp_path):
    """It would look like it was working until the buffer exhausted memory."""
    with pytest.raises(ConfigError, match="flush_rows"):
        LandingWriter(tmp_path / "landing", flush_rows=None, flush_seconds=None)


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"flush_rows": 0}, "positive"),
        ({"flush_seconds": -1}, "positive"),
        ({"filename": "a/b.parquet"}, "plain file name"),
        ({"filename": MARKER}, "marker"),
    ],
)
def test_incoherent_settings_are_refused_at_construction(tmp_path, kwargs, match):
    with pytest.raises(ConfigError, match=match):
        LandingWriter(tmp_path / "landing", **kwargs)


def test_close_lands_the_buffer(tmp_path):
    """A clean shutdown must not be a data loss."""
    root = tmp_path / "landing"
    with LandingWriter(root, flush_rows=1000) as writer:
        writer.extend([reading(i) for i in range(4)], tokens=list(range(4)))
    assert len(landed_directories(root)) == 1


def test_an_error_inside_the_context_does_not_land_a_partial_buffer(tmp_path):
    """"This batch failed" must not become "this batch was fine and short"."""
    root = tmp_path / "landing"
    with pytest.raises(RuntimeError):
        with LandingWriter(root, flush_rows=1000) as writer:
            writer.add(reading(0))
            raise RuntimeError("the subscriber fell over")
    assert not root.exists() or landed_directories(root) == []


def test_a_non_mapping_record_is_refused(tmp_path):
    writer = LandingWriter(tmp_path / "landing", flush_rows=10)
    with pytest.raises(DuckstreamError, match="mapping"):
        writer.add([1, 2, 3])


# ==========================================================================
# The seam: what is landed is what a FileSource reads
#
# The two halves are written against each other -- the writer's marker is the
# source's marker, the writer's directory is the source's batch. A test that
# only checked the writer would let them drift apart, and the failure would be
# a pipeline that lands data nothing ever reads.
# ==========================================================================


def test_a_file_source_reads_exactly_what_was_landed(tmp_path):
    root = tmp_path / "landing"
    writer = LandingWriter(root, flush_rows=2)
    writer.extend([reading(0), reading(1)])
    writer.flush()
    writer.extend([reading(2, "s2"), reading(3, "s2")])
    writer.flush()

    source = FileSource(root.as_posix(), marker=MARKER)
    scanned = source._scan()
    assert len(scanned) == 2, f"expected one file per flush, got {scanned}"

    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT sensor_id, count(*), sum(value) "
            f"FROM read_parquet('{(root / '**' / '*.parquet').as_posix()}') "
            "GROUP BY 1 ORDER BY 1"
        ).fetchall()
    finally:
        con.close()
    assert rows == [("s1", 2, 1.0), ("s2", 2, 5.0)]


def test_an_unmarked_directory_from_a_crash_is_never_read(tmp_path):
    """A crash before the marker leaves litter, not a short batch."""
    root = tmp_path / "landing"
    writer = LandingWriter(root, flush_rows=1)
    writer.add(reading(0))
    writer.flush()

    # A directory the writer got half-way through: data file present, no marker.
    crashed = root / "20260601T000000_000000_deadbeef"
    crashed.mkdir()
    (crashed / "data.parquet").write_bytes(b"not even parquet")

    source = FileSource(root.as_posix(), marker=MARKER)
    scanned = source._scan()
    assert len(scanned) == 1
    assert not any("deadbeef" in path for path in scanned)


def test_records_with_different_keys_land_as_nulls_rather_than_failing(tmp_path):
    """A stream whose shape drifts is a fact about the data, not an error.

    Refusing the batch would lose the messages that *are* well formed, and they
    are the majority. The absent field lands NULL, which is what SQL means by
    "this message did not say".
    """
    root = tmp_path / "landing"
    writer = LandingWriter(root, flush_rows=2)
    writer.add({"sensor_id": "s1", "value": 1.0})
    writer.add({"sensor_id": "s1", "value": 2.0, "battery": 90})
    batch = writer.flush()

    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT battery FROM read_parquet('{batch.path.as_posix()}') "
            "ORDER BY value"
        ).fetchall()
    finally:
        con.close()
    assert rows == [(None,), (90,)]
