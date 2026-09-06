"""Event time: watermarks, the lateness horizon, sealing, and both output modes.

This module is phase 2's definition of done, and it replaces
``test_watermarks_are_not_implemented_in_phase_one`` -- the tripwire phase 1
left behind to make sure the missing coverage was noticed the day a ``lateness``
field appeared rather than quietly shipped. It has now fired and been answered.

``PLAN.md`` asks for four things, and each has its own section below:

**Watermark semantics.** *"Late data inside the horizon updates its window; data
past the horizon is dropped and counted, never silently absorbed."* Both halves
matter. The first is what a lateness horizon is *for* -- a row arriving after
its window's nominal end is exactly the case a horizon exists to accommodate,
and dropping it would make the horizon pointless. The second is the framework's
standing position on silence: a stream that quietly discards 4% of its input
looks identical to one that discards none, so the count is durable, in the
catalog, and checked here from the catalog rather than from a return value.

**Sealing past the horizon.** Once the watermark passes a window's end the
window is complete. In ``update`` mode that makes it immutable; in ``append``
mode it is the moment the window is written, once and for all.

**Ground truth is a second implementation, not the sink's own SQL.** The plain
``recompute_sql`` other conformance modules use cannot serve here: what the sink
*should* hold is a function of the watermark trajectory -- which rows were
dropped, which windows had sealed -- and that depends on batch boundaries rather
than on file contents. So :func:`harness.replay` writes the contract out again
in plain Python, from ``PLAN.md``'s description, down to flooring timestamps by
epoch arithmetic where duckstream floors them by replacing fields. One test
below pins the two ground truths against each other in the case where both are
valid, so the reference is itself checked rather than merely trusted.

**Both front doors, at least two batches.** Through :class:`harness.Parity` as
everywhere else, which also compares the committed watermark and the drop counts
across doors -- and every scenario commits at least two batches, because
``CONTEXT.md`` 1.5's DuckLake failure appeared only on the second merge.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest
import yaml

from harness import DOORS, Landing, Scenario, normalise, replay, same_rows

from duckstream import Engine, Model
from duckstream.errors import ModelValidationError
from duckstream.sinks.table import TableSink
from duckstream.sources.files import FileSource

T = dt.datetime

#: One horizon, used by every scenario here so the arithmetic stays checkable by
#: hand while reading a test: with ``grain='hour'``, a window ``[h, h+1h)``
#: seals once some row has been seen at ``h + 1h + 10min``.
LATENESS = "10 minutes"
HORIZON = dt.timedelta(minutes=10)

AGGREGATES = {
    "n": "count(*)",
    "total": "sum(value)",
    "lo": "min(value)",
    "hi": "max(value)",
}

#: Valid only when nothing was dropped and nothing is being withheld -- i.e. for
#: the update scenario with no late rows. Carried so one test can pin the Python
#: reference against independent SQL; the rest use :func:`harness.replay`.
RECOMPUTE = (
    "SELECT date_trunc('hour', event_ts) AS window_ts,\n"
    "       sensor_id,\n"
    "       count(*) AS n,\n"
    "       sum(value) AS total,\n"
    "       min(value) AS lo,\n"
    "       max(value) AS hi\n"
    "  FROM {source}\n"
    " GROUP BY 1, 2"
)

UPDATE_SCENARIO = Scenario(
    name="hourly_horizon",
    aggregates=dict(AGGREGATES),
    key=("window_ts", "sensor_id"),
    recompute_sql=RECOMPUTE,
    grain="hour",
    lateness=LATENESS,
    mode="update",
    table="marts.hourly_horizon",
)

APPEND_SCENARIO = Scenario(
    name="hourly_sealed",
    aggregates=dict(AGGREGATES),
    key=("window_ts", "sensor_id"),
    recompute_sql=RECOMPUTE,
    grain="hour",
    lateness=LATENESS,
    mode="append",
    table="marts.hourly_sealed",
)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def drive(parity, batches) -> None:
    """Land each batch as its own drop and drain it, one batch at a time.

    One drop, one file, one trigger -- so ``harness.Parity.batches`` records
    exactly the boundaries the engine saw, which is what the reference needs to
    reproduce the watermark trajectory. Anything that let two drops share a
    trigger would make the reference and the engine disagree about batching
    rather than about event time.
    """
    for index, payload in enumerate(batches):
        parity.land(f"b{index}", payload)
        parity.run()


def expect(parity, mode: str):
    """Replay the batches this parity has actually driven."""
    return replay(parity.batches, grain="hour", lateness=HORIZON, mode=mode)


def assert_mart_matches(parity, expected) -> None:
    for door, world in parity.worlds.items():
        actual = world.rows()
        assert actual is not None, f"{door} door never created the mart"
        assert same_rows(actual, expected.mart), (
            f"{door} door disagrees with the event-time reference:\n"
            f"  sink:      {actual}\n"
            f"  reference: {expected.mart}"
        )


def assert_history_matches(parity, mode: str) -> None:
    """Every snapshot's mart equals the reference over what it had consumed.

    The event-time analogue of ``Parity.assert_snapshot_history_consistent``,
    and stronger than checking the end state for the same reason: a final state
    can be right by luck, a history in which every intermediate snapshot is also
    exactly right cannot. The mapping from a snapshot to a batch prefix is the
    committed offset's own file list, read at that same snapshot -- which is
    only meaningful because the offset shares its snapshot with the rows it
    checkpoints (``CONTEXT.md`` 1.9).
    """
    for door, world in parity.worlds.items():
        walk = world.snapshot_walk()
        assert walk, f"{door}: no snapshot contains the mart"
        for step in walk:
            consumed = len(step["consumed"])
            assert consumed <= len(parity.batches), (
                f"{door}: snapshot {step['snapshot_id']} consumed {consumed} "
                f"files but only {len(parity.batches)} batches were driven; "
                f"this helper assumes one drop of one file per batch"
            )
            expected = replay(
                parity.batches[:consumed],
                grain="hour",
                lateness=HORIZON,
                mode=mode,
            )
            assert same_rows(step["mart"], expected.mart), (
                f"{door} door, snapshot {step['snapshot_id']}: the mart is not "
                f"what the event-time contract says it should be after "
                f"{consumed} batch(es).\n"
                f"  mart:      {step['mart']}\n"
                f"  reference: {expected.mart}"
            )


# --------------------------------------------------------------------------
# Watermark semantics: the horizon accommodates, and the horizon ends
# --------------------------------------------------------------------------


def test_late_arrival_within_the_horizon_updates_its_window(make_parity):
    """The headline requirement, and the reason the drop test is on windows.

    ``grain='hour'``, ``lateness='10 minutes'``. Batch 1 tops out at 10:30, so
    the watermark is 10:20 -- *later than the 10:05 row it just folded*. Batch 2
    then delivers a row at 10:15, older than the watermark, and it must still be
    folded, because its window ``[10:00, 11:00)`` has not ended.

    That is the case a naive implementation gets wrong. Testing lateness against
    the row's timestamp rather than its window's end would drop this row and
    call it late, and the mart would silently under-count exactly the data the
    horizon was declared to accommodate.
    """
    parity = make_parity(UPDATE_SCENARIO, name="within")
    drive(
        parity,
        [
            [(T(2026, 5, 1, 10, 5), "s1", 1.0), (T(2026, 5, 1, 10, 30), "s1", 2.0)],
            [(T(2026, 5, 1, 10, 15), "s1", 4.0), (T(2026, 5, 1, 11, 30), "s1", 8.0)],
        ],
    )

    expected = expect(parity, "update")
    assert expected.late == [0, 0], "the reference thinks something was dropped"
    assert_mart_matches(parity, expected)
    parity.assert_reached_matched_branch()

    # Concretely: three rows in the 10:00 window, not two.
    window = [row for row in parity.worlds["python"].rows() if row[0] == T(2026, 5, 1, 10)]
    assert window == [(T(2026, 5, 1, 10), "s1", 3, 7.0, 1.0, 4.0)]

    # Nothing was dropped, so the plain SQL recompute is valid here too -- which
    # makes this the one place the Python reference is checked against an
    # independent SQL statement rather than merely trusted.
    assert same_rows(expected.mart, parity.worlds["python"].recompute())


def test_data_past_the_horizon_is_dropped_and_counted(make_parity):
    """Past the horizon: dropped, and the count is durable in the catalog.

    Batch 2 pushes the watermark to 11:20, which seals ``[10:00, 11:00)``.
    Batch 3 then carries a row at 10:45 -- inside a sealed window -- and it is
    refused: the 10:00 row keeps the count it had.

    The assertion deliberately reads ``duckstream.batches`` out of the catalog
    rather than the engine's return value. ``PLAN.md`` requires late data to be
    *counted*, and a number that only ever existed in a Python object has not
    been counted in any sense an operator can use at 03:00.
    """
    parity = make_parity(UPDATE_SCENARIO, name="past")
    drive(
        parity,
        [
            [(T(2026, 5, 1, 10, 5), "s1", 1.0), (T(2026, 5, 1, 10, 30), "s1", 2.0)],
            [(T(2026, 5, 1, 11, 30), "s1", 8.0)],
            [(T(2026, 5, 1, 10, 45), "s1", 99.0), (T(2026, 5, 1, 11, 40), "s1", 16.0)],
        ],
    )

    expected = expect(parity, "update")
    assert expected.late == [0, 0, 1], expected.late
    assert_mart_matches(parity, expected)

    for door, world in parity.worlds.items():
        counts = world.drop_counts()
        assert [late for _, late, _ in counts] == [0, 0, 1], f"{door}: {counts}"
        assert [undated for _, _, undated in counts] == [0, 0, 0], f"{door}: {counts}"

    # The dropped row carried 99.0, so if it had leaked in it would be the
    # window's `hi` and impossible to miss.
    window = [row for row in parity.worlds["python"].rows() if row[0] == T(2026, 5, 1, 10)]
    assert window == [(T(2026, 5, 1, 10), "s1", 2, 3.0, 1.0, 2.0)]


def test_a_sealed_window_is_never_modified_again(make_parity):
    """Sealing is a one-way door, checked by comparing before against after.

    Weaker than it looks if asserted only on the final state, so the 10:00
    window's row is captured the moment it seals and compared with itself after
    two further batches have tried to reach it.
    """
    parity = make_parity(UPDATE_SCENARIO, name="frozen")
    drive(
        parity,
        [
            [(T(2026, 5, 1, 9, 5), "s1", 1.0)],
            [(T(2026, 5, 1, 10, 30), "s1", 2.0)],
        ],
    )
    sealed = [row for row in parity.worlds["python"].rows() if row[0] == T(2026, 5, 1, 9)]
    assert sealed, "the 09:00 window should exist by now"

    drive(
        parity,
        [
            [(T(2026, 5, 1, 9, 30), "s1", 100.0)],
            [(T(2026, 5, 1, 9, 45), "s1", 200.0)],
        ],
    )
    after = [row for row in parity.worlds["python"].rows() if row[0] == T(2026, 5, 1, 9)]
    assert after == sealed, "a sealed window was modified by a later batch"

    expected = expect(parity, "update")
    assert expected.late == [0, 0, 1, 1], expected.late
    assert_mart_matches(parity, expected)


def test_the_watermark_never_regresses(make_parity):
    """A batch of genuinely old data must not re-open sealed windows.

    Without monotonicity the watermark would follow the last batch's maximum
    rather than the stream's, and a backfill drop would pull it backwards --
    un-sealing windows that had already been declared complete and, in append
    mode, already emitted. Sealing would then mean nothing at all.
    """
    parity = make_parity(UPDATE_SCENARIO, name="monotone")
    drive(parity, [[(T(2026, 5, 1, 14, 0), "s1", 1.0)]])
    high = parity.worlds["python"].watermark()
    assert high == T(2026, 5, 1, 13, 50)

    drive(parity, [[(T(2026, 5, 1, 9, 0), "s1", 2.0)]])
    assert parity.worlds["python"].watermark() == high

    expected = expect(parity, "update")
    assert expected.watermark == high
    assert expected.late == [0, 1], "the old batch belongs to a long-sealed window"
    assert_mart_matches(parity, expected)


def test_a_row_with_no_event_time_is_dropped_and_counted(make_parity):
    """Under event-time semantics a row with no event time belongs nowhere.

    It cannot be placed in a window, so it can never seal and never be emitted.
    Absorbing it into a NULL window would leave it permanently invisible in
    append mode, which is the silent failure this framework exists to remove --
    so it is dropped, and counted separately from late data, because "arrived
    too late" and "carries no timestamp" are different operational problems.

    Note the scope: this applies only to a model that declared a horizon. A
    model without one still produces a NULL ``window_ts``, exactly as phase 1
    did, and ``test_ground_truth.py`` still covers that.
    """
    parity = make_parity(UPDATE_SCENARIO, name="undated")
    drive(
        parity,
        [
            [(T(2026, 5, 1, 10, 5), "s1", 1.0)],
            [(None, "s1", 50.0), (T(2026, 5, 1, 10, 20), "s1", 2.0)],
        ],
    )

    expected = expect(parity, "update")
    assert expected.undated == [0, 1]
    assert_mart_matches(parity, expected)

    for door, world in parity.worlds.items():
        counts = world.drop_counts()
        assert [undated for _, _, undated in counts] == [0, 1], f"{door}: {counts}"
        assert all(row[0] is not None for row in world.rows()), (
            f"{door}: an undated row created a NULL window"
        )


def test_the_watermark_is_recovered_from_the_catalog_by_the_next_process(make_parity):
    """The horizon survives a restart, because it is committed, not remembered.

    Every ``World.run`` opens a *fresh* connection and closes it -- that is what
    cron does, and it means the watermark under test was necessarily read back
    out of ``duckstream.watermarks`` rather than carried in memory. If it were
    not durable, the third batch would be judged against no horizon at all and
    its late row would be folded instead of dropped.
    """
    parity = make_parity(UPDATE_SCENARIO, name="restart")
    drive(
        parity,
        [
            [(T(2026, 5, 1, 10, 5), "s1", 1.0)],
            [(T(2026, 5, 1, 11, 30), "s1", 2.0)],
            [(T(2026, 5, 1, 10, 10), "s1", 400.0)],
        ],
    )

    expected = expect(parity, "update")
    assert expected.late == [0, 0, 1]
    for door, world in parity.worlds.items():
        assert world.watermark() == expected.watermark, door
        assert [late for _, late, _ in world.drop_counts()] == [0, 0, 1], door
    assert_mart_matches(parity, expected)


# --------------------------------------------------------------------------
# Append: one row per window, written once, when it seals
# --------------------------------------------------------------------------


def test_a_window_fed_by_three_batches_reaches_the_sink_once_and_complete(make_parity):
    """The bug phase 2 exists to fix, asserted directly.

    Three batches all land in ``[10:00, 11:00)``. Phase 1's append would have
    written three partial rows for that window and left the mart over-counting
    by construction. With a horizon there is a right answer: the window
    accumulates while it is open and is written **once**, complete, when the
    watermark passes its end.
    """
    parity = make_parity(APPEND_SCENARIO, name="sealed")
    drive(
        parity,
        [
            [(T(2026, 5, 1, 10, 5), "s1", 1.0)],
            [(T(2026, 5, 1, 10, 25), "s1", 2.0)],
            [(T(2026, 5, 1, 10, 45), "s1", 4.0)],
            [(T(2026, 5, 1, 11, 30), "s1", 8.0)],
        ],
    )

    expected = expect(parity, "append")
    assert_mart_matches(parity, expected)
    parity.assert_reached_matched_branch()

    rows = parity.worlds["python"].rows()
    assert rows == [(T(2026, 5, 1, 10), "s1", 3, 7.0, 1.0, 4.0)], rows
    # And the still-open 11:00 window is withheld rather than half-written.
    assert parity.worlds["python"].open_windows() == normalise(
        [(T(2026, 5, 1, 11), "s1", 1, 8.0, 8.0, 8.0)]
    )


def test_an_open_window_is_visible_in_the_accumulator_not_in_the_mart(make_parity):
    """Where an incomplete window lives, and that it is one place only.

    "Why is this hour missing from my mart" is the first question sealing
    provokes, so the answer has to be inspectable: the accumulator sits beside
    the target, in the target's own schema, and holds exactly the windows the
    watermark has not reached. A window is in one of the two tables, never in
    both and never in neither.
    """
    parity = make_parity(APPEND_SCENARIO, name="open")
    drive(
        parity,
        [
            [(T(2026, 5, 1, 10, 5), "s1", 1.0)],
            [(T(2026, 5, 1, 10, 40), "s2", 2.0)],
        ],
    )

    expected = expect(parity, "append")
    assert expected.mart == [], "nothing should have sealed yet"
    for door, world in parity.worlds.items():
        assert world.rows() == [], f"{door}: an open window reached the mart"
        assert world.open_windows() == expected.open_windows, door

    drive(parity, [[(T(2026, 5, 1, 11, 30), "s1", 4.0)]])
    expected = expect(parity, "append")
    assert len(expected.mart) == 2, "both sensors' 10:00 windows should seal"
    for door, world in parity.worlds.items():
        assert same_rows(world.rows(), expected.mart), door
        assert world.open_windows() == expected.open_windows, door
        overlap = {row[:2] for row in world.rows()} & {
            row[:2] for row in world.open_windows()
        }
        assert not overlap, f"{door}: window in both the mart and the accumulator"


def test_a_late_row_cannot_re_emit_a_window_already_appended(make_parity):
    """The property that makes append-mode output final.

    A row for a sealed window is dropped before the fold, so it can neither
    change an emitted row nor -- worse -- recreate its window in the
    accumulator and have it emitted a second time. Append would otherwise be
    append-mostly, which is not a guarantee anyone can build on.
    """
    parity = make_parity(APPEND_SCENARIO, name="reemit")
    drive(
        parity,
        [
            [(T(2026, 5, 1, 10, 5), "s1", 1.0)],
            [(T(2026, 5, 1, 11, 30), "s1", 2.0)],
            [(T(2026, 5, 1, 10, 55), "s1", 512.0)],
            [(T(2026, 5, 1, 12, 30), "s1", 4.0)],
        ],
    )

    expected = expect(parity, "append")
    assert expected.late == [0, 0, 1, 0]
    assert_mart_matches(parity, expected)

    windows = [row[0] for row in parity.worlds["python"].rows()]
    assert len(windows) == len(set(windows)), f"a window was emitted twice: {windows}"
    assert (T(2026, 5, 1, 10), "s1", 1, 1.0, 1.0, 1.0) in parity.worlds["python"].rows()


def test_a_null_grouping_key_seals_like_any_other(make_parity):
    """The two hard cases at once: a NULL merge key inside a sealed window.

    ``PLAN.md`` asks the ground-truth diff to cover NULL grouping keys, and
    phase 1 covers them without a horizon. The combination is what is new, and
    it has two ways to go wrong that the parts do not: the merge into the
    accumulator matches keys with ``IS NOT DISTINCT FROM`` (with plain ``=`` a
    NULL key never matches itself, so each batch would add another row and the
    window would seal as several partial ones), and the seal predicate reads
    ``window_ts`` alone, so it must not care that another key column is NULL.
    """
    parity = make_parity(APPEND_SCENARIO, name="nullkey")
    drive(
        parity,
        [
            [(T(2026, 5, 1, 10, 5), None, 1.0), (T(2026, 5, 1, 10, 10), "s1", 2.0)],
            [(T(2026, 5, 1, 10, 40), None, 4.0)],
            [(T(2026, 5, 1, 11, 30), None, 8.0)],
        ],
    )

    expected = expect(parity, "append")
    assert_mart_matches(parity, expected)
    assert_history_matches(parity, "append")

    rows = parity.worlds["python"].rows()
    null_rows = [row for row in rows if row[1] is None]
    assert null_rows == [(T(2026, 5, 1, 10), None, 2, 5.0, 1.0, 4.0)], (
        f"the NULL-key window should have sealed as one folded row: {rows}"
    )


# --------------------------------------------------------------------------
# What event time deliberately gives up
# --------------------------------------------------------------------------


def test_batch_boundaries_change_which_rows_are_late(make_parity, tmp_path):
    """Chunking is **not** neutral once a horizon exists, and that is correct.

    Phase 1 can assert "chunked equals unchunked" because a fold over the same
    rows gives the same answer whatever order they arrive in. Event time breaks
    that on purpose: the watermark is a function of what has been *observed*, so
    a batch boundary between two rows can make the second one late when reading
    both at once would not have. This is inherent to event-time semantics rather
    than a duckstream quirk -- every micro-batch engine behaves this way -- but
    it is the kind of property that gets "fixed" by someone who has only read
    the phase-1 invariant, so it is pinned.

    Both worlds see the same rows; only the trigger boundaries differ.
    """
    rows = [
        (T(2026, 5, 1, 12, 30), "s1", 1.0),   # sets the watermark high
        (T(2026, 5, 1, 10, 5), "s1", 2.0),    # a long-sealed window
        (T(2026, 5, 1, 13, 30), "s1", 4.0),
    ]
    # A landing tree each: a file source scans the whole tree, so two parities
    # sharing one would consume each other's drops.
    together = make_parity(
        UPDATE_SCENARIO, name="whole", landing=Landing(tmp_path / "landing_whole")
    )
    drive(together, [rows[:2], rows[2:]])

    split = make_parity(
        UPDATE_SCENARIO, name="split", landing=Landing(tmp_path / "landing_split")
    )
    drive(split, [[rows[0]], [rows[1]], [rows[2]]])

    # One trigger: nothing is late, because no watermark was committed before it.
    assert expect(together, "update").late == [0, 0]
    # Split: the second trigger is judged against the first one's watermark.
    assert expect(split, "update").late == [0, 1, 0]

    for parity, mode in ((together, "update"), (split, "update")):
        assert_mart_matches(parity, expect(parity, mode))

    assert together.worlds["python"].rows() != split.worlds["python"].rows(), (
        "if these agreed, the watermark would not depend on batch boundaries "
        "and this test would be asserting nothing"
    )
    # What does still hold is the invariant that matters: each is exactly what
    # the contract says for the batches it actually saw.
    assert_history_matches(together, "update")
    assert_history_matches(split, "update")


# --------------------------------------------------------------------------
# The refusal: append over windows with no horizon
# --------------------------------------------------------------------------


def _unhorizoned_append() -> Model:
    return Model(
        name="rejected",
        source=FileSource("landing", marker="_READY"),
        sink=TableSink("marts.rejected", mode="append"),
        aggregates={"n": "count(*)"},
        key=["window_ts", "sensor_id"],
        time_column="event_ts",
        grain="hour",
    )


def test_windowed_append_without_a_horizon_is_refused_at_load():
    """The phase-2 reversal, and the message an operator has to act on.

    Phase 1 accepted this shape and wrote one partial row per window per batch,
    which equals the truth only when no two batches ever touch the same window
    -- a condition the user cannot enforce and the engine never checked. It is
    the ``CONTEXT.md`` section 4 bug class in the one place the framework had
    left it, so it is now refused at load, where a deploy fails, rather than at
    03:00 where a mart merely disagrees.

    The message must name all three ways forward, because each corresponds to a
    different thing the user might actually have meant.
    """
    with pytest.raises(ModelValidationError) as excinfo:
        _unhorizoned_append().validate()

    message = str(excinfo.value)
    assert "append" in message and "lateness" in message
    assert "over-count" in message
    assert "mode='update'" in message
    assert "drop `grain`" in message


@pytest.mark.parametrize("door", DOORS)
def test_the_refusal_is_identical_through_both_front_doors(door, tmp_path):
    """A config path with a weaker check is the same defect in a different hat."""
    model = _unhorizoned_append()
    if door == "python":
        with pytest.raises(ModelValidationError) as excinfo:
            model.validate()
        assert "lateness" in str(excinfo.value)
        return

    catalog = tmp_path / "catalog.ducklake"
    config = model.to_config()
    config["source"]["path"] = "landing/"
    path = tmp_path / "models.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "catalog": f"ducklake:{catalog.as_posix()}",
                "data_path": "lake_data",
                "models": [config],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ModelValidationError) as excinfo:
        Engine.from_config(str(path))
    assert "lateness" in str(excinfo.value)
    assert not catalog.exists(), (
        "a catalog was created before the model was refused, so the refusal is "
        "not at load time"
    )


def test_adding_a_horizon_makes_the_same_model_acceptable():
    """So the rule is a rule, not a blanket refusal of windowed append."""
    model = _unhorizoned_append()
    model.lateness = "10 minutes"
    model.validate()
    assert model.to_config()["lateness"] == "10 minutes"


# --------------------------------------------------------------------------
# Accounting, and history
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "scenario,mode",
    [(UPDATE_SCENARIO, "update"), (APPEND_SCENARIO, "append")],
    ids=["update", "append"],
)
def test_every_snapshot_matches_the_event_time_contract(make_parity, scenario, mode):
    """Not just the end state: every intermediate snapshot, for both modes.

    The append case is the demanding one. Sealing moves rows out of the
    accumulator and into the target inside the same transaction that commits the
    offset and the watermark, so a snapshot in which a window had been evicted
    but not emitted -- or emitted but not evicted -- would be observable here.
    """
    parity = make_parity(scenario, name=f"history_{mode}")
    drive(
        parity,
        [
            [(T(2026, 5, 1, 10, 5), "s1", 1.0), (T(2026, 5, 1, 10, 50), "s2", 2.0)],
            [(T(2026, 5, 1, 11, 15), "s1", 4.0)],
            [(T(2026, 5, 1, 10, 30), "s1", 256.0), (T(2026, 5, 1, 12, 5), "s2", 8.0)],
            [(T(2026, 5, 1, 13, 30), "s1", 16.0)],
        ],
    )

    expected = expect(parity, mode)
    assert expected.late == [0, 0, 1, 0], expected.late
    assert_mart_matches(parity, expected)
    assert_history_matches(parity, mode)
    parity.assert_reached_matched_branch()


@pytest.mark.parametrize(
    "scenario,mode",
    [(UPDATE_SCENARIO, "update"), (APPEND_SCENARIO, "append")],
    ids=["update", "append"],
)
def test_one_trigger_is_still_one_snapshot_with_event_time(
    make_parity, scenario, mode
):
    """Event time adds statements to the transaction, not transactions.

    ``update`` gains a filter; ``append`` gains a merge into the accumulator, an
    insert into the target and a delete from the accumulator. All of it belongs
    to the trigger's single transaction, so the snapshot accounting the
    exactly-once claim rests on is unchanged -- one trigger, one snapshot.
    """
    parity = make_parity(scenario, name=f"snapshots_{mode}")
    drive(parity, [[(T(2026, 5, 1, 10, 5), "s1", 1.0)]])
    baseline = parity.worlds["python"].snapshot_count()

    drive(
        parity,
        [
            [(T(2026, 5, 1, 11, 30), "s1", 2.0)],
            [(T(2026, 5, 1, 12, 30), "s1", 4.0)],
        ],
    )
    assert parity.worlds["python"].snapshot_count() - baseline == 2

    # An idle pass still writes nothing at all: no watermark row either, since
    # an empty batch opens no transaction (CONTEXT.md 1.8).
    before = parity.worlds["python"].snapshot_count()
    parity.run()
    assert parity.worlds["python"].snapshot_count() == before


def test_the_watermark_lands_in_the_same_snapshot_as_the_offset(make_parity):
    """Read both sides ``AT (VERSION => n)``; they must agree at every version.

    This is the event-time half of the exactly-once claim. A watermark that
    committed separately from the offset would let a restart resume reading from
    one point in the stream while judging lateness from another, and the window
    of rows silently dropped between them would be invisible.
    """
    parity = make_parity(UPDATE_SCENARIO, name="atomic")
    drive(
        parity,
        [
            [(T(2026, 5, 1, 10, 5), "s1", 1.0)],
            [(T(2026, 5, 1, 11, 30), "s1", 2.0)],
            [(T(2026, 5, 1, 12, 30), "s1", 4.0)],
        ],
    )

    world = parity.worlds["python"]
    seen = 0
    with world.connect() as con:
        for snapshot in con.execute(
            "SELECT snapshot_id FROM lake.snapshots() ORDER BY snapshot_id"
        ).fetchall():
            version = snapshot[0]
            try:
                offset = con.execute(
                    "SELECT offset_json FROM duckstream.offsets "
                    f"AT (VERSION => {version}) WHERE model_name = ? "
                    "ORDER BY batch_id DESC LIMIT 1",
                    [UPDATE_SCENARIO.name],
                ).fetchone()
                watermark = con.execute(
                    "SELECT watermark FROM duckstream.watermarks "
                    f"AT (VERSION => {version}) WHERE model_name = ? "
                    "ORDER BY batch_id DESC LIMIT 1",
                    [UPDATE_SCENARIO.name],
                ).fetchone()
                files = con.execute(
                    "SELECT count(DISTINCT relpath) "
                    f"FROM duckstream.consumed_files AT (VERSION => {version}) "
                    "WHERE model_name = ?",
                    [UPDATE_SCENARIO.name],
                ).fetchone()
            except Exception:
                continue  # snapshots from before the state tables existed
            if offset is None:
                assert watermark is None, (
                    f"snapshot {version} has a watermark but no offset, so the "
                    f"two did not commit together"
                )
                continue
            assert watermark is not None and watermark[0] is not None, (
                f"snapshot {version} committed an offset without a watermark"
            )
            # Three things now have to land in one snapshot rather than two:
            # the offset, the watermark, and the rows saying which files were
            # read. The offset carries a *count* of those rows and the table
            # carries their identity (CONTEXT.md 1.15), and asserting the two
            # agree at every point in history is what keeps the count honest --
            # it is documented as a report rather than the authority, and a
            # report nobody checks is how the two drift apart.
            consumed = files[0]
            assert json.loads(offset[0])["entries"] == consumed, (
                f"snapshot {version}: the offset counts "
                f"{json.loads(offset[0])['entries']} consumed record(s) but the "
                f"table holds {consumed} — they did not commit together"
            )
            expected = replay(
                parity.batches[:consumed], grain="hour", lateness=HORIZON
            )
            assert watermark[0] == expected.watermark, (
                f"snapshot {version}: watermark {watermark[0]} does not match "
                f"the {consumed} batch(es) the offset says were consumed "
                f"(expected {expected.watermark})"
            )
            seen += 1

    assert seen >= 3, f"only {seen} snapshots carried a checkpoint to compare"


def test_a_healthy_batch_creates_no_extra_view(landing, tmp_path):
    """The filter is paid for only when it filters.

    Every event-time trigger scans the batch once for its counts and its newest
    event time -- unavoidable, and the same single scan the phase-1 ``count(*)``
    already cost. The *second* temp view that removes out-of-horizon rows is
    created only when the scan says there is something to remove, so the steady
    state adds no view and reads no row twice. Asserted structurally, because
    nothing about the output would reveal a regression here.
    """
    import duckdb

    from harness import RecordingConnection, World

    world = World("python", tmp_path / "novel", landing, UPDATE_SCENARIO)
    landing.drop("v1", [(T(2026, 5, 1, 10, 5), "s1", 1.0)])
    con = RecordingConnection(duckdb.connect())
    try:
        world.run(con=con)
        landing.drop("v2", [(T(2026, 5, 1, 11, 30), "s1", 2.0)])
        world.run(con=con)
        clean = [s for s in con.of_kind("CREATE TEMP VIEW") if "ontime" in s]
        assert clean == [], f"a filter view was created for a clean batch: {clean}"

        landing.drop("v3", [(T(2026, 5, 1, 10, 30), "s1", 4.0)])
        world.run(con=con)
        filtered = [s for s in con.of_kind("CREATE TEMP VIEW") if "ontime" in s]
        assert len(filtered) == 1, (
            f"expected exactly one filter view once a row was late, got "
            f"{len(filtered)}"
        )
    finally:
        con.close()
