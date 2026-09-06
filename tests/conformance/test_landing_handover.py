"""The handover: at-least-once in, exactly-once out.

Phase 5's claim is a *composition*, and neither half proves it alone. The
landing writer is at-least-once and its unit tests say so. The engine is
exactly-once and its conformance suite says so. What nobody has checked until
here is the seam — that what the writer puts on disk is what the source reads,
that the engine folds it exactly once, and that the two halves agree about the
completion marker without anyone keeping them in step by hand.

`CONTEXT.md` section 4 is the shape being tested::

    broker --> LandingWriter --> landing/ --> FileSource --> engine
              at least once                    exactly once

The MQTT client is absent on purpose. `paho-mqtt` is optional and a broker is
not available in CI, so the durability seam is driven through
`LandingWriter` directly — which is the same object the MQTT adapter drives, and
the only part of the path that touches disk. A test that needed a broker would
run nowhere.

The interesting case is the last one. At-least-once means duplicates are
*expected* after a crash, and the engine cannot dedupe them — they are genuinely
different files holding genuinely different rows. That is a real property of the
design and it is asserted rather than left to be discovered.
"""

from __future__ import annotations

import datetime as dt

import pytest

from harness import Scenario

from duckstream.landing import MARKER, LandingWriter

BASE = dt.datetime(2026, 6, 1, 10)

#: An additive model over what a landing writer produces. Deliberately the
#: simplest tier: what is under test is the handover, not the aggregation.
LANDED = Scenario(
    name="landed_counts",
    aggregates={"n": "count(*)", "total": "sum(value)"},
    key=("window_ts", "sensor_id"),
    recompute_sql=(
        "SELECT date_trunc('hour', event_ts) AS window_ts,\n"
        "       sensor_id,\n"
        "       count(*) AS n,\n"
        "       sum(value) AS total\n"
        "  FROM {source}\n"
        " GROUP BY 1, 2"
    ),
    table="marts.landed_counts",
)


def land(writer: LandingWriter, count: int, *, start: int = 0, sensor: str = "s1"):
    """Buffer `count` readings and land them as one complete directory."""
    for index in range(start, start + count):
        writer.add(
            {
                "event_ts": BASE + dt.timedelta(seconds=index),
                "sensor_id": sensor,
                "value": float(index),
            }
        )
    return writer.flush()


def test_landed_messages_reach_the_mart_exactly_once(make_parity, tmp_path):
    """The whole composition, through both front doors.

    Three flushes, three drains, and the mart is compared against an independent
    recompute of the landing tree after each one. The writer's marker and the
    source's marker are never configured to match by the test -- they match
    because both default to the same constant, which is the coupling under test.
    """
    parity = make_parity(LANDED, name="handover")
    root = parity.landing.root

    for index in range(3):
        writer = LandingWriter(root, flush_rows=1000)
        land(writer, 4, start=index * 10)
        writer.flush()
        parity.run()
        parity.assert_matches_ground_truth()

    parity.assert_reached_matched_branch()
    parity.assert_snapshot_history_consistent()

    rows = parity.worlds["python"].rows()
    assert sum(row[2] for row in rows) == 12, f"expected 12 landed rows, got {rows}"


def test_a_crash_before_the_marker_is_never_read(make_parity, tmp_path):
    """A half-written batch must be invisible, not short.

    This is the property the write order buys, and it is the one that decides
    whether a crash costs nothing or costs a silently incomplete mart. The
    unmarked directory stays on disk for ever as visible litter, which is the
    right trade: a human can see it, and the engine cannot mistake it for data.
    """
    parity = make_parity(LANDED, name="crash")
    root = parity.landing.root

    writer = LandingWriter(root, flush_rows=1000)
    land(writer, 3)
    parity.run()
    parity.assert_matches_ground_truth()
    before = parity.worlds["python"].rows()

    # A flush that died between the rename and the marker.
    crashed = root / "20260601T090000_000000_deadbeef"
    crashed.mkdir()
    (crashed / "data.parquet").write_bytes(b"not even parquet")

    parity.run()
    assert parity.worlds["python"].rows() == before, (
        "an unmarked directory reached the mart"
    )
    assert crashed.is_dir(), "duckstream deleted a directory it does not own"


def test_a_redelivered_message_lands_twice_and_is_counted_twice(
    make_parity, tmp_path
):
    """At-least-once means duplicates, and duckstream does not hide them.

    After a crash the broker re-delivers whatever was never acknowledged, so the
    same reading can land in two different files. The engine consumes both files
    exactly once each -- which is exactly-once over *files*, and it is the only
    guarantee available: the two files are genuinely different files holding
    genuinely different rows, and nothing in the landing tree marks one as a
    repeat of the other.

    Asserted rather than left to be found, because the alternative reading --
    "exactly-once means my rows cannot be duplicated" -- is the one a user will
    arrive with. A model that cares needs a merge key and `mode='update'`; this
    scenario has one, which is why the *keyed* row count stays right while the
    summed count doubles.
    """
    parity = make_parity(LANDED, name="redelivery")
    root = parity.landing.root

    writer = LandingWriter(root, flush_rows=1000)
    land(writer, 3)
    parity.run()

    # The same three readings again, in a new file: a redelivery.
    again = LandingWriter(root, flush_rows=1000)
    land(again, 3)
    parity.run()

    # Ground truth is a recompute of the landing tree, which *also* sees both
    # copies -- so the mart agreeing with it is the real assertion here.
    parity.assert_matches_ground_truth()

    rows = parity.worlds["python"].rows()
    assert len(rows) == 1, "one key, one row: the merge key did its job"
    assert rows[0][2] == 6, (
        "a redelivered message must be counted twice, visibly -- duckstream "
        "does not silently de-duplicate what the broker sent twice"
    )


def test_the_writer_and_the_source_agree_on_the_marker_by_construction(
    make_parity, tmp_path
):
    """Both default to the same constant, so they cannot drift apart.

    If the two ever disagreed, the pipeline would land data that nothing read --
    a silence that looks exactly like a broker publishing nothing.
    """
    from duckstream.sources.files import FileSource

    assert FileSource(str(tmp_path)).marker == MARKER
    assert LandingWriter(tmp_path / "x", flush_rows=1).marker == MARKER

    parity = make_parity(LANDED, name="marker")
    writer = LandingWriter(parity.landing.root, flush_rows=1000)
    batch = land(writer, 2)
    assert (batch.directory / MARKER).is_file()

    parity.run()
    parity.assert_matches_ground_truth()
    assert parity.worlds["python"].rows(), "nothing was read from the landed batch"
