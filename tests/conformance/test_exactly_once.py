"""The headline claim: exactly-once under real fault injection.

``PLAN.md``'s definition of done for phase 1 is not a feature list. It is "a
fault-injection test that kills the process between sink write and commit and
proves on restart that rows are neither lost nor duplicated", and it says in as
many words that this "needs real fault injection, not a unit test".

So every kill here is a real child process that really dies. ``os._exit(9)``
inside a fault hook takes the interpreter down with the DuckLake transaction
open: no ``finally``, no ``ROLLBACK``, no ``con.close()``, no flush. Whether the
next process then sees a lakehouse that lost nothing and duplicated nothing is a
property of the transaction boundary, not of duckstream's exception handling --
which is precisely the property being claimed.

Four cases, each parametrised over both front doors:

======================================  =================================
kill at ``after_sink_write``            rows written, offset not committed
kill at ``before_commit``               state appended, ``COMMIT`` not issued
kill at ``after_commit``                durable; must not be reprocessed
repeated kills across a drain           luck cannot survive a sequence
======================================  =================================

Every one of them is checked against **snapshot history**, not only against the
final state. One trigger is one snapshot (``CONTEXT.md`` 1.4) and the sink rows
and the source offset land in the same snapshot (``CONTEXT.md`` 1.9 makes that
mandatory rather than merely convenient), so time travel can ask, at every point
in the catalog's history, "is the mart exactly a full recompute of the files the
offset says had been consumed by then?". ``PLAN.md`` calls that out specifically:
it is what makes the verification inspectable rather than inferred.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest

from harness import ADDITIVE, DOORS, Landing, World, kill_run, same_rows

T = dt.datetime

FAULT_EXIT = 9

# Values are integers and binary-exact halves, so a sum is bit-identical
# whatever order the batches folded in. That keeps "neither lost nor duplicated"
# an exact claim rather than one hedged by a float tolerance.
DROPS: dict[str, list[tuple]] = {
    "d1": [(T(2026, 1, 1, 0, 5), "a", 1.0), (T(2026, 1, 1, 0, 20), "a", 2.0)],
    "d2": [(T(2026, 1, 1, 0, 40), "a", 4.0), (T(2026, 1, 1, 1, 5), "b", 8.0)],
    "d3": [(T(2026, 1, 1, 0, 55), None, 0.5), (T(2026, 1, 1, 2, 0), "c", 16.0)],
    "d4": [(T(2026, 1, 1, 1, 30), "b", 32.0)],
    "d5": [(T(2026, 1, 1, 0, 10), None, 0.25), (T(2026, 1, 1, 3, 0), "a", 64.0)],
    "d6": [(T(2026, 1, 1, 2, 15), "c", 128.0)],
}


def _assert_died(process, point: str) -> None:
    assert process.returncode == FAULT_EXIT, (
        f"the child was expected to die at {point!r} with exit {FAULT_EXIT}, "
        f"got {process.returncode}.\nstdout: {process.stdout}\n"
        f"stderr: {process.stderr}"
    )
    assert f"FAULT {point}" in process.stderr, (
        f"the fault hook at {point!r} never fired, so this run proved nothing.\n"
        f"stderr: {process.stderr}"
    )


def _assert_clean(process) -> dict:
    assert process.returncode == 0, (
        f"restart failed ({process.returncode}).\nstdout: {process.stdout}\n"
        f"stderr: {process.stderr}"
    )
    return json.loads(process.stdout.strip().splitlines()[-1])


def _prime(world: World, landing: Landing) -> None:
    """One clean committed batch, so the kill lands on the *second* one.

    Deliberate: ``CONTEXT.md`` 1.5's ``Out of buffer`` appeared only on the
    second MERGE, the first to take the ``WHEN MATCHED`` branch, so a fault test
    whose only batch was the first would be testing the easier half of the sink.
    """
    landing.drop("d1", DROPS["d1"])
    _assert_clean(kill_run(world))
    assert world.offset_files() == ["d1/part.parquet"]


@pytest.fixture(params=DOORS)
def world(request, make_world) -> World:
    return make_world(request.param)


# --------------------------------------------------------------------------
# A kill before the commit loses the batch, and only the batch
# --------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.parametrize("point", ["after_sink_write", "before_commit"])
def test_kill_before_commit_neither_loses_nor_duplicates(world, landing, point):
    """Kill inside the transaction: nothing durable, nothing lost on restart.

    ``after_sink_write`` is the exact moment ``PLAN.md`` names -- the MERGE has
    run and folded the batch into the mart, but the offset has not been
    appended and ``COMMIT`` has not been issued. ``before_commit`` goes one step
    further: the batch history row is written too, and the very next statement
    would have been the ``COMMIT``. Both must leave the catalog exactly as the
    previous batch left it.
    """
    _prime(world, landing)
    landing.drop("d2", DROPS["d2"])

    before_rows = world.rows()
    before_offsets = world.offset_files()
    before_snapshots = world.snapshot_ids()
    before_batches = len(world.batch_history())

    _assert_died(kill_run(world, fault=point), point)

    # Nothing became durable. Not the sink rows, not the offset, not even a
    # snapshot: the transaction that would have produced one never committed.
    assert world.snapshot_ids() == before_snapshots, (
        "a killed batch left a snapshot behind, so the write was not atomic"
    )
    assert world.rows() == before_rows, "rows from an uncommitted batch are visible"
    assert world.offset_files() == before_offsets, "the offset advanced without a commit"
    assert "d2/part.parquet" not in world.offset_files()
    assert len(world.batch_history()) == before_batches

    # And the history reads the same way: the mart as of the last snapshot
    # before the kill is the mart as it stands now.
    assert world.rows(at=before_snapshots[-1]) == before_rows

    # Restart. The uncommitted batch replays from the stored offset.
    _assert_clean(kill_run(world))

    assert world.offset_files() == ["d1/part.parquet", "d2/part.parquet"]
    assert same_rows(world.rows(), world.recompute()), (
        "after replaying a killed batch the mart is not a full recompute: rows "
        "were lost or double-counted"
    )
    assert world.snapshot_count() == len(before_snapshots) + 1, (
        "the replayed batch should cost exactly one snapshot"
    )
    # The pre-kill snapshot is still readable and still says what it said, so
    # the replay appended history rather than rewriting it.
    assert world.rows(at=before_snapshots[-1]) == before_rows


@pytest.mark.slow
def test_kill_after_commit_is_durable_and_not_reprocessed(world, landing):
    """Kill immediately after ``COMMIT``: the batch stands, and is not redone.

    This is the other half of the guarantee and the one a naive implementation
    fails. The rows are in the mart; if the offset were checkpointed separately
    -- a second transaction, a second database -- the restart would fold the
    same batch a second time and every count would be doubled. ``CONTEXT.md``
    1.9 is why that cannot happen here: sink and state share one catalog and
    therefore one snapshot.
    """
    _prime(world, landing)
    landing.drop("d2", DROPS["d2"])

    before_snapshots = world.snapshot_ids()

    _assert_died(kill_run(world, fault="after_commit"), "after_commit")

    after_snapshots = world.snapshot_ids()
    assert len(after_snapshots) == len(before_snapshots) + 1, (
        "a committed batch should have produced exactly one snapshot before the "
        "process was killed"
    )
    committed_rows = world.rows()
    assert world.offset_files() == ["d1/part.parquet", "d2/part.parquet"]
    assert same_rows(committed_rows, world.recompute())

    # Time travel makes the atomicity visible rather than inferred: the
    # snapshot before the batch has the pre-batch mart, the snapshot after it
    # has the post-batch mart, and there is nothing in between.
    assert world.rows(at=before_snapshots[-1]) != committed_rows
    assert world.rows(at=after_snapshots[-1]) == committed_rows

    # Restart: there is nothing left to do.
    result = _assert_clean(kill_run(world))
    assert result["committed"] == [], (
        f"the restart re-ran a batch that had already committed: {result}"
    )
    assert result["empty_passes"] == 1
    assert world.snapshot_ids() == after_snapshots, (
        "an idle restart after a committed batch added a snapshot, so the batch "
        "was reprocessed"
    )
    assert world.rows() == committed_rows, "the committed batch was folded twice"


# --------------------------------------------------------------------------
# A sequence of kills, which luck cannot survive
# --------------------------------------------------------------------------


#: Where to kill, and on which firing within that child. A single kill can pass
#: by chance -- the batch might have been empty, the fold might have been a
#: no-op. A scripted sequence that kills before the commit, after the commit,
#: mid-drain and at plan time cannot.
KILL_SCRIPT: list[tuple[str | None, int]] = [
    ("after_sink_write", 1),  # first batch dies inside the transaction
    ("before_commit", 1),     # and again, one statement later
    ("after_commit", 1),      # commits one batch, then dies
    ("after_bind", 2),        # commits one, dies binding the next
    ("after_commit", 2),      # commits two, dies
    ("after_plan", 2),        # commits one, dies before touching the second
    ("after_sink_write", 3),  # commits two, dies inside the third
    (None, 0),                # drain to completion
]


@pytest.mark.slow
@pytest.mark.parametrize("door", DOORS)
def test_repeated_kills_across_a_drain_equal_a_full_recompute(
    make_world, landing, door
):
    """Kill repeatedly through a multi-batch drain; converge on the truth.

    Six drops and ``max_files_per_trigger=1``, so a full drain is six batches
    and there are five interior moments a kill can land on. The engine is
    restarted after every death and the sequence continues from whatever the
    catalog committed.

    Two assertions carry the weight. The final mart must equal a full recompute
    from source -- nothing lost, nothing double-counted, after seven kills. And
    the *entire snapshot history* must be consistent: at every snapshot, the
    mart is exactly a recompute of the files the offset recorded as consumed at
    that same snapshot. The second is what rules out a lucky final state.
    """
    world = make_world(door, ADDITIVE.chunked(1))
    for name, payload in DROPS.items():
        landing.drop(name, payload)

    kills = 0
    for point, nth in KILL_SCRIPT:
        process = kill_run(world, fault=point, nth=nth) if point else kill_run(world)
        if point is None:
            _assert_clean(process)
            continue
        if process.returncode == FAULT_EXIT:
            kills += 1
            assert f"FAULT {point}" in process.stderr
        else:
            # The drain finished before this kill point could fire: legitimate,
            # because earlier kills commit different amounts of work. It must
            # then have exited cleanly.
            _assert_clean(process)

    assert kills >= 4, (
        f"only {kills} of {len(KILL_SCRIPT) - 1} scripted kills actually fired; "
        f"the sequence is not exercising the lifecycle it claims to"
    )

    # Drain whatever is left, then assert the pipeline is genuinely idle.
    _assert_clean(kill_run(world))
    idle_snapshots = world.snapshot_ids()
    _assert_clean(kill_run(world))
    assert world.snapshot_ids() == idle_snapshots, "an idle pass wrote a snapshot"

    assert world.offset_files() == sorted(f"{name}/part.parquet" for name in DROPS)

    expected = world.recompute()
    assert same_rows(world.rows(), expected), (
        f"after {kills} kills the mart is not a full recompute:\n"
        f"  sink:      {world.rows()}\n"
        f"  recompute: {expected}"
    )

    total_rows = sum(len(payload) for payload in DROPS.values())
    counted = sum(row[2] for row in world.rows())
    assert counted == total_rows, (
        f"count(*) across the mart is {counted}, but {total_rows} source rows "
        f"were landed: rows were lost or duplicated"
    )

    # The strongest statement available: every point in history is itself a
    # full recompute of what had been consumed by then.
    walk = world.snapshot_walk()
    assert len(walk) >= 5, f"only {len(walk)} snapshots contain the mart"
    for step in walk:
        assert same_rows(step["mart"], step["expected"]), (
            f"snapshot {step['snapshot_id']} is not a full recompute of the "
            f"{len(step['consumed'])} file(s) its offset had consumed:\n"
            f"  consumed:  {step['consumed']}\n"
            f"  mart:      {step['mart']}\n"
            f"  recompute: {step['expected']}"
        )

    # Monotone: a snapshot never un-consumes a file. Together with the check
    # above this says the history is a prefix-ordered sequence of correct
    # states, which is exactly what exactly-once means over time.
    seen: list[str] = []
    for step in walk:
        assert set(seen).issubset(set(step["consumed"])), (
            f"snapshot {step['snapshot_id']} dropped files the previous snapshot "
            f"had already consumed"
        )
        seen = step["consumed"]


@pytest.mark.slow
@pytest.mark.parametrize("door", DOORS)
def test_snapshot_count_equals_committed_batches(make_world, landing, door):
    """Every committed batch is one snapshot, across a kill-interrupted drain.

    The accounting that the exactly-once claim rests on, checked end to end
    rather than on a single trigger: however many times the process died, the
    number of snapshots carrying the mart equals the number of batches the
    catalog's own history records as committed.
    """
    world = make_world(door, ADDITIVE.chunked(1))
    for name in ("d1", "d2", "d3", "d4"):
        landing.drop(name, DROPS[name])

    _assert_died(kill_run(world, fault="before_commit", nth=2), "before_commit")
    _assert_clean(kill_run(world))

    history = world.batch_history()
    committed = [row for row in history if row["committed_at"] is not None]
    walk = world.snapshot_walk()
    assert len(walk) == len(committed), (
        f"{len(committed)} batches are recorded as committed but {len(walk)} "
        f"snapshots contain the mart"
    )
    # Batch ids are contiguous from 1: a killed batch reuses its id rather than
    # burning one, so a gap would mean an uncommitted batch left a trace.
    assert [row["batch_id"] for row in committed] == list(
        range(1, len(committed) + 1)
    )
