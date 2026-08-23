"""What the exactly-once guarantee rests on, asserted directly against DuckLake.

Four properties, all of them measured rather than assumed elsewhere in this
project, and all of them cheap to regress silently:

* **one trigger, one snapshot** (``CONTEXT.md`` 1.4). This is the primitive.
  Sink rows, batch history and source offset become durable together or not at
  all *because* they share a snapshot.
* **an idle trigger costs nothing** (``CONTEXT.md`` 1.8). Not "costs little":
  zero snapshots, and no transaction opened for the batch. A DuckLake
  transaction that writes nothing costs ~1.3 ms, one that writes anything pays
  ~15 ms, and under cron most ticks are idle.
* **inlining stayed off** (``CONTEXT.md`` 1.7). With the default limit of 10
  rows, a small batch is written into the catalog instead of a parquet file and
  takes the code path holding DuckLake's open correctness bugs. Every batch here
  is deliberately smaller than 10 rows, so a non-empty ``ducklake_list_files``
  is the proof.
* **no scalar subquery in the merge ``ON`` clause** (``CONTEXT.md`` 1.5). One
  there fails with ``Out of buffer`` against DuckLake -- and only on the second
  merge, the first to take ``WHEN MATCHED``. A string scan of the statement the
  engine actually executed is cheap insurance on a property whose regression
  would otherwise need a two-batch DuckLake run to find.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from harness import (
    ADDITIVE,
    ALIAS,
    DOORS,
    SETTINGS,
    RecordingConnection,
    build_model,
)

from duckstream import AvailableNow, Engine, Once
from duckstream import lake as lakemod
from duckstream.state import DuckLakeStateStore

T = dt.datetime

SMALL = [(T(2026, 9, 1, 0, 5), "s1", 1.0), (T(2026, 9, 1, 0, 30), "s2", 2.0)]
SMALL_2 = [(T(2026, 9, 1, 0, 45), "s1", 4.0), (T(2026, 9, 1, 1, 5), "s2", 8.0)]
SMALL_3 = [(T(2026, 9, 1, 1, 30), None, 16.0)]


# --------------------------------------------------------------------------
# Snapshot accounting
# --------------------------------------------------------------------------


@pytest.mark.parametrize("door", DOORS)
def test_one_trigger_is_exactly_one_snapshot(make_world, landing, door):
    """The delta per non-empty trigger is 1 -- after setup, which costs 2.

    Setup is accounted for rather than waved at, because the build graph warns
    that the *first* ``ensure()`` costs a snapshot for its ``CREATE SCHEMA``. On
    a virgin catalog there are two such calls, and they are the only two:

    ==========================================  =========
    ``StateStore.ensure``: schema + 3 tables    1 snapshot
    ``TableSink.ensure``: ``CREATE SCHEMA``     1 snapshot
    the batch itself                            1 snapshot
    ==========================================  =========

    Both ``ensure`` calls are idempotent and DuckLake produces no snapshot for a
    transaction that changes nothing, so from the second trigger on -- including
    from an entirely new process -- the delta is exactly 1.

    Creating the catalog is itself snapshot 0, which is why the baseline is
    taken *after* attaching an empty one rather than from zero.
    """
    world = make_world(door)
    landing.drop("s1", SMALL)

    assert not world.exists
    virgin = world.snapshot_count()  # attaches, creating the catalog: snapshot 0
    assert virgin == 1, f"a freshly created catalog should hold 1 snapshot, not {virgin}"
    world.run()
    first = world.snapshot_count()
    assert first - virgin == 3, (
        f"a first run on a virgin catalog should cost 3 snapshots (state ensure, "
        f"sink schema, the batch); it cost {first - virgin}"
    )

    for name, payload in (("s2", SMALL_2), ("s3", SMALL_3)):
        landing.drop(name, payload)
        before = world.snapshot_count()
        world.run()
        after = world.snapshot_count()
        assert after - before == 1, (
            f"trigger over {name!r} produced {after - before} snapshots, not 1. "
            f"The exactly-once guarantee is that the sink rows, the batch record "
            f"and the offset share one snapshot; more than one means they do not."
        )

    # A brand-new process over the existing catalog re-runs both ensures and
    # still costs exactly one snapshot for its batch.
    landing.drop("s4", [(T(2026, 9, 1, 2, 0), "s3", 32.0)])
    before = world.snapshot_count()
    world.run()
    assert world.snapshot_count() - before == 1


@pytest.mark.parametrize("door", DOORS)
def test_an_idle_trigger_adds_zero_snapshots(make_world, landing, door):
    """Nothing to read means nothing written -- and nothing recorded either.

    Checked three ways, because "zero snapshots" alone could be produced by a
    transaction that opened, wrote nothing and committed: no snapshot, no offset
    row, and no batch-history row.
    """
    world = make_world(door)
    landing.drop("s1", SMALL)
    world.run()

    settled_snapshots = world.snapshot_count()
    settled_offsets = world.offset_files()
    settled_batches = world.batch_history()

    for _ in range(3):
        world.run()
        assert world.snapshot_count() == settled_snapshots, (
            "an idle trigger wrote a snapshot, so the catalog's history is a "
            "record of cron ticks rather than of work done"
        )
        assert world.offset_files() == settled_offsets
        assert world.batch_history() == settled_batches


def test_an_idle_trigger_opens_no_transaction(make_world, landing):
    """The batch lifecycle does not reach ``BEGIN`` when the plan is empty.

    ``CONTEXT.md`` 1.8 measured the difference this makes: an empty DuckLake
    transaction costs ~1.3 ms, one that writes anything pays ~15 ms of commit.
    The saving is only real if the engine genuinely returns before opening a
    transaction, so that is asserted directly -- with a state store that records
    every call, and with a connection that records every statement.

    ``ensure()`` does open a transaction of its own, once per engine, to wrap
    its DDL into a single snapshot. That is setup, not a trigger, so the second
    run of an already-prepared engine is where the claim is checked.
    """
    world = make_world("python")
    landing.drop("s1", SMALL)

    class SpyStore(DuckLakeStateStore):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self.begins = 0
            self.commits = 0

        def begin(self, con):
            self.begins += 1
            return super().begin(con)

        def commit(self, con, offsets, watermarks):
            self.commits += 1
            return super().commit(con, offsets, watermarks)

    raw = duckdb.connect()
    recorder = RecordingConnection(raw)
    try:
        store = SpyStore("duckstream", catalog=ALIAS)
        engine = Engine(
            recorder,
            catalog=str(world.catalog),
            data_path=str(world.data_path),
            settings=dict(SETTINGS),
            state=store,
        )
        engine.add(build_model(ADDITIVE, world.landing))

        engine.run(trigger=AvailableNow())
        assert store.begins == 1 and store.commits == 1, (
            "a non-empty batch must open exactly one transaction and commit it"
        )

        recorder.statements.clear()
        report = engine.run(trigger=AvailableNow())

        assert all(r.is_empty for r in report)
        assert store.begins == 1, (
            f"the idle trigger opened {store.begins - 1} transaction(s); "
            f"CONTEXT.md 1.8 requires it to return before begin()"
        )
        assert store.commits == 1
        begins = [
            s
            for s in recorder.statements
            if s.lstrip().upper().startswith(("BEGIN", "COMMIT", "ROLLBACK"))
        ]
        assert begins == [], (
            f"the idle trigger issued transaction control statements: {begins}"
        )
    finally:
        raw.close()


@pytest.mark.parametrize("door", DOORS)
def test_setup_costs_two_snapshots_even_when_there_is_nothing_to_do(
    make_world, landing, door
):
    """"An idle trigger adds zero snapshots" holds *after* setup, not before.

    Worth pinning because it is the one place the claim has an asterisk, and an
    unstated asterisk is how a snapshot-accounting assertion starts failing on
    a fresh deployment only. On a virgin catalog the first pass runs
    ``StateStore.ensure`` and ``TableSink.ensure``, each of which creates a
    schema and therefore costs a snapshot, and it does so whether or not there
    is any data to read. From the second pass on, an idle trigger is free.
    """
    world = make_world(door)
    assert world.snapshot_count() == 1  # catalog creation

    world.run()  # nothing has ever been landed
    assert world.snapshot_count() == 3, (
        "the first pass over a virgin catalog should cost exactly the two "
        "ensure() snapshots: the state schema and the sink schema"
    )
    assert world.offset_files() == []
    assert world.batch_history() == []

    for _ in range(3):
        world.run()
        assert world.snapshot_count() == 3, (
            "once the schemas exist, an idle pass must be free"
        )


@pytest.mark.parametrize("door", DOORS)
def test_a_zero_row_file_commits_and_advances_the_offset(make_world, landing, door):
    """An empty parquet file is consumed, not skipped and not re-read forever.

    A landing writer that flushes on a timer produces these routinely. The
    behaviour that matters is that the file is checkpointed: a batch whose
    aggregation yields no rows still commits its offset, so the next trigger
    does not plan the same empty file again and again. It is also the one case
    where a trigger costs a snapshot while adding no rows to the mart, which is
    worth knowing before reading a snapshot count as a row count.
    """
    world = make_world(door)
    landing.drop("empty", [])

    world.run()
    assert world.rows() == []
    assert world.offset_files() == ["empty/part.parquet"], (
        "an empty file must be recorded as consumed, or every trigger replans it"
    )
    settled = world.snapshot_count()

    world.run()
    assert world.snapshot_count() == settled, "the empty file was planned twice"

    landing.drop("real", SMALL)
    world.run()
    assert world.rows() == world.recompute()
    history = world.batch_history()
    assert [row["rows_in"] for row in history] == [0, 2]


# --------------------------------------------------------------------------
# Inlining
# --------------------------------------------------------------------------


@pytest.mark.parametrize("door", DOORS)
def test_small_batches_still_write_parquet_files(make_world, landing, door):
    """Fewer than 10 rows per batch, and still one data file per batch.

    The number 10 is the whole reason this test exists: it is DuckLake's default
    ``ducklake_default_data_inlining_row_limit``, and ``CONTEXT.md`` 1.7
    measured a 3-row insert leaving ``ducklake_list_files`` **empty** at that
    default -- the rows went into the catalog, down the path carrying DuckLake's
    open correctness bugs. Every batch here is 1 or 2 rows, so if inlining were
    ever re-enabled this assertion would fail immediately.
    """
    world = make_world(door)
    for name, payload in (("s1", SMALL), ("s2", SMALL_2), ("s3", SMALL_3)):
        landing.drop(name, payload)
        world.run()
        assert len(payload) < 10

    with world.connect() as con:
        files = lakemod.list_files(con, ALIAS, ADDITIVE.table)
        limit = con.execute(
            "SELECT current_setting('ducklake_default_data_inlining_row_limit')"
        ).fetchone()[0]

    assert int(limit) == 0, (
        f"data inlining is enabled ({limit}); every batch under 10 rows is then "
        f"written into the catalog instead of a parquet file"
    )
    assert files, (
        "ducklake_list_files is empty after three sub-10-row batches, which is "
        "exactly what CONTEXT.md 1.7 measured when inlining was on"
    )
    assert world.data_file_count() >= 3, (
        "inlining off means one parquet file per trigger per table; three "
        "triggers produced fewer than three data files"
    )


# --------------------------------------------------------------------------
# The merge statement itself
# --------------------------------------------------------------------------


def test_merge_on_clause_contains_no_scalar_subquery(make_world, landing):
    """Scan the ``MERGE`` the engine actually executed, on the second batch too.

    ``CONTEXT.md`` 1.5 bisected an ``Out of buffer`` failure down to a scalar
    subquery in a join condition against DuckLake, and recorded that it appeared
    only on the **second** merge -- the first to take ``WHEN MATCHED``. So this
    runs two batches, checks both statements, and checks the whole statement as
    well as the ``ON`` clause: the ``USING`` source is a table subquery and is
    fine, but nothing may reach the join condition.
    """
    world = make_world("python")
    landing.drop("s1", SMALL)
    landing.drop("s2", SMALL_2)

    raw = duckdb.connect()
    recorder = RecordingConnection(raw)
    try:
        engine = Engine(
            recorder,
            catalog=str(world.catalog),
            data_path=str(world.data_path),
            settings=dict(SETTINGS),
        )
        engine.add(build_model(ADDITIVE.chunked(1), world.landing))
        engine.run(trigger=AvailableNow())
    finally:
        raw.close()

    merges = recorder.of_kind("MERGE")
    assert len(merges) == 2, (
        f"expected one MERGE per batch and two batches, saw {len(merges)}; "
        f"without a second merge the WHEN MATCHED branch is never reached"
    )
    for statement in merges:
        clause = RecordingConnection.on_clause(statement)
        assert "(SELECT" not in clause.upper().replace(" ", ""), (
            f"the merge ON clause contains a scalar subquery, which CONTEXT.md "
            f"1.5 measured as 'Out of buffer' against DuckLake:\n{clause}"
        )
        assert "SELECT" not in clause.upper(), (
            f"unexpected SELECT in the merge ON clause:\n{clause}"
        )
        # The property that makes a NULL grouping key fold rather than duplicate.
        assert "IS NOT DISTINCT FROM" in clause
        assert "WHEN MATCHED" in statement and "WHEN NOT MATCHED" in statement


def test_second_batch_takes_the_matched_branch(parity):
    """Behavioural proof that ``WHEN MATCHED`` ran, not just that it was written.

    A structural scan says the branch exists. This says it executed: two batches
    contributing to the same ``(window_ts, sensor_id)`` leave one row whose
    count is the sum. Under ``WHEN NOT MATCHED`` alone there would be two rows.

    Through both doors, because nothing here needs a single-door world -- the
    statement-recording test above is the one that does.
    """
    parity.land("m1", [(T(2026, 10, 1, 0, 5), "s1", 1.0)])
    parity.run()
    parity.land("m2", [(T(2026, 10, 1, 0, 45), "s1", 2.0)])
    parity.run()

    rows = parity.worlds["python"].rows()
    assert len(rows) == 1, f"the second batch inserted instead of folding: {rows}"
    window_ts, sensor_id, n, total, lo, hi = rows[0]
    assert (n, total, lo, hi) == (2, 3.0, 1.0, 2.0)
    parity.assert_matches_ground_truth()
    parity.assert_reached_matched_branch()


# --------------------------------------------------------------------------
# Time travel
# --------------------------------------------------------------------------


@pytest.mark.parametrize("door", DOORS)
def test_every_snapshot_is_a_full_recompute_of_its_own_offset(
    make_world, landing, door
):
    """Walk the whole history: mart at snapshot N == recompute of offset at N.

    This is the assertion ``PLAN.md`` is pointing at when it calls time travel
    "a genuine asset for this framework, not just a feature to inherit". Because
    a transaction cannot span two attached databases (``CONTEXT.md`` 1.9), the
    offset is necessarily in the same snapshot as the rows it checkpoints -- so
    the two halves of this comparison are read at the same instant of history
    and the answer is a fact about the catalog rather than an inference from it.
    """
    world = make_world(door, ADDITIVE.chunked(1))
    for name, payload in (
        ("t1", SMALL),
        ("t2", SMALL_2),
        ("t3", SMALL_3),
        ("t4", [(T(2026, 9, 1, 0, 6), "s1", 64.0)]),
    ):
        landing.drop(name, payload)
    world.run()

    walk = world.snapshot_walk()
    assert len(walk) == 4, f"expected one snapshot per batch, got {len(walk)}"
    for index, step in enumerate(walk, start=1):
        assert len(step["consumed"]) == index, (
            f"snapshot {step['snapshot_id']} had consumed "
            f"{len(step['consumed'])} files; batch {index} should have consumed "
            f"{index}"
        )
        assert step["mart"] == step["expected"], (
            f"snapshot {step['snapshot_id']}: mart is not a full recompute of "
            f"{step['consumed']}\n  mart:      {step['mart']}\n"
            f"  recompute: {step['expected']}"
        )
