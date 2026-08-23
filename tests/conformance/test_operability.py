"""Running this thing unattended: what happens when a batch will not process.

Exactly-once says what happens when a process *dies*. It says nothing about what
happens when the data itself is unprocessable, and the honest phase-1 answer was
"the offset never advances, so every trigger from now until a human intervenes
retries the same corrupt file". That is not a crash and nothing raises an alarm
about it; the pipeline simply stops, quietly, forever.

So these scenarios use a genuinely corrupt file -- bytes that are not parquet,
carrying a completion marker, exactly as a truncated upload would arrive -- and
check the whole path an operator would walk:

1. the failure is **recorded**, not just raised, so the attempt budget survives
   the process exiting between ticks;
2. the retry is **held back**, so a drain loop cannot burn the budget in
   milliseconds;
3. the batch is eventually **skipped and the loss recorded permanently**, and
   the stream is live again afterwards;
4. every step of that is visible from the catalog through ``status``, and makes
   ``duckstream run`` exit non-zero.

Through both front doors, as everything here is: a config path that reported a
different exit code, or skipped a batch the Python path halted on, would be the
same defect wearing a different hat.
"""

from __future__ import annotations

import datetime as dt

import pytest

from harness import DOORS, Landing, Scenario, same_rows

from duckstream import state as state_module

T = dt.datetime

#: `max_attempts=2` keeps the scenarios short. The budget itself is tested in
#: tests/unit/test_engine.py; what matters here is the whole path end to end.
CORRUPTIBLE = Scenario(
    name="counts",
    aggregates={"n": "count(*)", "total": "sum(value)"},
    key=("sensor_id",),
    recompute_sql=(
        "SELECT sensor_id, count(*) AS n, sum(value) AS total\n"
        "  FROM {source} GROUP BY 1"
    ),
    time_column=None,
    grain=None,
    max_files_per_trigger=1,
    max_attempts=2,
    table="marts.counts",
)


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch):
    """Retry immediately. Backoff itself is covered in tests/unit/test_engine.py.

    Without this every scenario below would have to sleep a real second between
    attempts, which buys nothing here -- the property under test is what happens
    *after* the attempts run out.
    """
    monkeypatch.setattr(state_module, "BACKOFF_BASE", dt.timedelta(0))


def corrupt(landing: Landing, name: str) -> None:
    """A marked drop whose parquet file is not parquet.

    Written the way a truncated upload arrives: marker last, exactly as the
    contract requires, so the *source* is behaving correctly and the file is
    genuinely eligible. Nothing here is a fixture shortcut -- the engine has no
    way to know until DuckDB tries to read it.
    """
    directory = landing.root / name
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "part.parquet").write_bytes(b"PAR1 and then nothing valid at all")
    (directory / "_READY").write_text("", encoding="utf-8")


def good(index: int) -> list[tuple]:
    return [(T(2026, 4, 1, 9, index), f"s{index % 2}", float(index))]


# --------------------------------------------------------------------------
# The whole path
# --------------------------------------------------------------------------


def test_a_corrupt_file_is_retried_then_skipped_and_the_stream_continues(
    make_parity, landing
):
    """The headline: one bad file costs one bad file, not the whole stream.

    The assertion that matters most is the last one. A halted pipeline does not
    preserve the data it choked on -- it stops collecting everything that
    arrives *after* it too -- so the test of quarantine is not that it skipped
    something, it is that the next drop got through.
    """
    parity = make_parity(CORRUPTIBLE, name="quarantine")
    parity.land("a1", good(1))
    parity.run()

    corrupt(landing, "a2")

    # Attempt one: recorded, retried, and loudly non-zero through the CLI.
    parity.run(expect_failure=True)
    for door, world in parity.worlds.items():
        status = world.status()
        assert status.state == "failing", door
        assert status.attempt == 1, door
        assert "magic bytes" in status.error.lower() or "parquet" in status.error.lower()
        assert world.quarantined() == [], f"{door}: skipped on the first failure"

    # Attempt two exhausts the budget: skipped, and recorded as skipped.
    parity.run(expect_failure=True)
    for door, world in parity.worlds.items():
        records = world.quarantined()
        assert len(records) == 1, door
        assert records[0]["attempts"] == 2, door
        assert "a2" in (records[0]["payload_json"] or ""), (
            f"{door}: the record does not say what was skipped"
        )
        assert world.status().attempt == 0, f"{door}: still marked as failing"

    # And the stream is live: the next drop lands normally.
    parity.land("a3", good(3))
    parity.run()
    for door, world in parity.worlds.items():
        rows = {row[0] for row in world.rows()}
        assert rows == {"s1"}, f"{door}: {world.rows()}"
        assert world.status().rows_in == 2, (
            f"{door}: both good drops should have been read"
        )


def test_the_offset_and_the_quarantine_record_land_in_one_snapshot(
    make_parity, landing
):
    """Skipping data and recording that you skipped it cannot come apart.

    If they could, a crash between them would leave a catalog whose offset has
    moved past data with nothing to say why -- silent loss, which is the exact
    failure this framework exists to make impossible. They are one transaction,
    so ``CONTEXT.md`` 1.4 makes them one snapshot, and that is checkable by
    reading both sides at the same version.
    """
    parity = make_parity(CORRUPTIBLE, name="atomic")
    parity.land("a1", good(1))
    parity.run()
    corrupt(landing, "a2")
    parity.run(expect_failure=True)
    parity.run(expect_failure=True)

    world = parity.worlds["python"]
    with world.connect() as con:
        versions = [
            row[0]
            for row in con.execute(
                "SELECT snapshot_id FROM lake.snapshots() ORDER BY snapshot_id"
            ).fetchall()
        ]
        seen = 0
        for version in versions:
            try:
                rows = con.execute(
                    "SELECT count(*) FROM duckstream.quarantine "
                    f"AT (VERSION => {version})"
                ).fetchone()[0]
                offsets = con.execute(
                    "SELECT offset_json FROM duckstream.offsets "
                    f"AT (VERSION => {version}) WHERE model_name = 'counts' "
                    "ORDER BY batch_id DESC LIMIT 1"
                ).fetchone()
            except Exception:
                continue
            if rows:
                seen += 1
                assert offsets and offsets[0] and "a2" in offsets[0], (
                    f"snapshot {version} records a quarantine but its offset has "
                    f"not moved past the batch, so the two did not commit together"
                )
        assert seen, "no snapshot carried the quarantine record"


def test_halt_stops_rather_than_skipping(make_parity, landing):
    """The other policy, and the reason it is not the default.

    ``halt`` never produces a gap. What it does instead is stop: the offset
    stays put and nothing that arrived after the bad file is processed either.
    Both halves are asserted, because choosing between the policies means
    understanding that halting is not the safe option, only the different one.
    """
    parity = make_parity(
        Scenario(**{**CORRUPTIBLE.__dict__, "on_failure": "halt", "name": "halted",
                    "table": "marts.halted"}),
        name="halt",
    )
    parity.land("a1", good(1))
    parity.run()
    settled = parity.worlds["python"].offset_files()

    corrupt(landing, "a2")
    parity.land("a3", good(3))
    for _ in range(3):
        parity.run(expect_failure=True)

    for door, world in parity.worlds.items():
        assert world.quarantined() == [], f"{door}: halt skipped a batch"
        assert world.offset_files() == settled, f"{door}: halt advanced the offset"
        assert world.status().rows_in == 1, (
            f"{door}: a3 was processed, but halt should be stuck behind a2"
        )

    # Remove the cause and it recovers on its own, without intervention.
    (landing.root / "a2" / "_READY").unlink()
    parity.run()
    for door, world in parity.worlds.items():
        assert world.status().rows_in == 2, f"{door}: did not recover"
        assert world.status().healthy, f"{door}: still unhealthy after recovering"


# --------------------------------------------------------------------------
# What the operator sees
# --------------------------------------------------------------------------


@pytest.mark.parametrize("door", DOORS)
def test_a_failing_run_exits_non_zero_through_both_doors(door, make_world, landing):
    """Cron notices exit codes, not log lines.

    A run that lost data or is stuck must not exit 0. This is the whole reason
    the failure is a *verdict* on the run rather than an exception thrown where
    it happened -- every model still gets its turn, and the exit code still
    reports the truth afterwards.
    """
    world = make_world(door, CORRUPTIBLE)
    landing.drop("a1", good(1))
    world.run()

    corrupt(landing, "a2")
    failing = world.run(expect_failure=True)
    if door == "yaml":
        assert failing.returncode != 0
        assert "attempt 1 failed" in failing.stdout

    quarantining = world.run(expect_failure=True)
    if door == "yaml":
        assert quarantining.returncode != 0
        assert "QUARANTINED" in quarantining.stdout
        assert "Data was lost" in quarantining.stdout


def test_status_answers_the_question_the_run_provokes(make_parity, landing):
    """"Why is my mart short?" has to be answerable after the fact.

    The run that quarantined said so once, in a log that has since rotated.
    ``status`` is read from the catalog, so it still knows -- and keeps knowing,
    because a status that went green again once the stream recovered would be
    retiring the only evidence that a gap exists.
    """
    parity = make_parity(CORRUPTIBLE, name="status")
    parity.land("a1", good(1))
    parity.run()
    corrupt(landing, "a2")
    parity.run(expect_failure=True)
    parity.run(expect_failure=True)
    parity.land("a3", good(3))
    parity.run()

    for door, world in parity.worlds.items():
        status = world.status()
        assert status.state == "quarantined", door
        assert not status.healthy, (
            f"{door}: the stream recovered and the loss stopped being reported"
        )
        assert status.quarantined == 1
        assert status.attempt == 0, f"{door}: reported as failing after recovery"
        assert status.rows_in == 2
        assert status.batches == 2
        assert status.processing_lag is not None
        assert status.backlog == 0, f"{door}: nothing should still be pending"


def test_rows_out_is_recorded_for_every_committed_batch(make_parity, landing):
    """Phase 1 left ``rows_out`` NULL believing it cost a second aggregation.

    Measured on 1.5.5: ``con.execute`` on the ``INSERT`` or ``MERGE`` the sink
    already issues returns the affected count, and the sink was discarding it.
    So this costs nothing and closes the last always-NULL column in the batch
    history.
    """
    parity = make_parity(CORRUPTIBLE, name="rowsout")
    parity.land("a1", good(1))
    parity.run()
    parity.land("a2", good(2))
    parity.run()

    for door, world in parity.worlds.items():
        history = world.batch_history()
        assert history, door
        assert all(row["rows_out"] is not None for row in history), (
            f"{door}: rows_out is still NULL"
        )
        assert world.status().rows_out == sum(r["rows_out"] for r in history)
    parity.assert_matches_ground_truth()
