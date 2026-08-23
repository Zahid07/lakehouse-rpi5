"""Ground-truth diff: the sink against a full recompute from source.

``PLAN.md``: "For every model, compare the sink against a full recompute from
source, under interleaved batches, out-of-order arrival, re-runs, late arrival
within the horizon, and NULL grouping keys."

Four of those five are here. **Late arrival within a lateness horizon is out of
scope for phase 1** and is not tested but is pinned instead -- see
:func:`test_watermarks_are_not_implemented_in_phase_one` at the bottom. Phase 1
has no watermarks, no lateness horizon and no window sealing; ``PLAN.md`` puts
all three in phase 2. A test that pretended to cover late arrival would be
asserting something the framework does not claim, and a scenario quietly missing
from the list is exactly how a phase gets marked done that is not.

The recompute is hand-written SQL carried on each :class:`~harness.Scenario`.
Generating it from ``TableSink.aggregation_sql`` would compare duckstream
against itself and would pass even if the generator were wrong.

Every test here runs through :class:`~harness.Parity`, so every one of them is
executed twice -- once through the Python API, once through the YAML/CLI path --
and the two are compared to each other as well as to the recompute.
"""

from __future__ import annotations

import datetime as dt

import pytest

from harness import ADDITIVE, Scenario, same_rows

T = dt.datetime


# --------------------------------------------------------------------------
# The four in-scope cases
# --------------------------------------------------------------------------


def test_interleaved_batches_equal_a_full_recompute(parity):
    """Land, drain, land, drain -- interleaved, and correct at every step.

    The mart is checked against a full recompute *after each drain*, not only at
    the end, because an incremental fold that is wrong in one direction and
    wrong again in the other can still finish in the right place.
    """
    schedule = [
        [("A", [(T(2026, 3, 1, 0, 5), "s1", 1.0), (T(2026, 3, 1, 0, 40), "s1", 2.0)])],
        [("B", [(T(2026, 3, 1, 0, 50), "s1", 4.0), (T(2026, 3, 1, 1, 5), "s2", 8.0)])],
        [
            ("C", [(T(2026, 3, 1, 1, 30), "s2", 16.0)]),
            ("D", [(T(2026, 3, 1, 0, 15), "s1", 32.0), (T(2026, 3, 1, 2, 0), "s3", 0.5)]),
        ],
    ]
    for group in schedule:
        for name, payload in group:
            parity.land(name, payload)
        parity.run()
        parity.assert_matches_ground_truth()

    parity.assert_reached_matched_branch()
    parity.assert_snapshot_history_consistent()


def test_out_of_order_arrival_equals_a_full_recompute(parity):
    """Files arriving with event time running backwards.

    An additive fold over a windowed key is order-independent by construction --
    that is what the tier *means* -- so the correct outcome here is that arrival
    order makes no difference at all. The test earns its place by proving the
    windowing does not smuggle in an ordering assumption, for instance by
    sealing a window once a later one has been seen. Phase 1 does not seal
    anything; this is what says so.
    """
    parity.land("late_hour", [(T(2026, 4, 1, 5, 30), "s1", 1.0)])
    parity.run()
    parity.land("early_hour", [(T(2026, 4, 1, 0, 30), "s1", 2.0)])
    parity.run()
    parity.land("middle_hour", [(T(2026, 4, 1, 2, 30), "s1", 4.0)])
    parity.land("early_hour_again", [(T(2026, 4, 1, 0, 45), "s1", 8.0)])
    parity.run()

    rows = parity.assert_matches_ground_truth()
    # The oldest window was reopened by a batch that arrived last, and folded.
    hour_zero = [r for r in rows if r[0] == T(2026, 4, 1, 0, 0)]
    assert len(hour_zero) == 1
    assert hour_zero[0][2] == 2 and hour_zero[0][3] == 10.0

    parity.assert_reached_matched_branch()
    parity.assert_snapshot_history_consistent()


def test_null_grouping_keys_fold_into_one_row(parity):
    """A NULL merge key must match itself, or every batch inserts a new row.

    ``IS NOT DISTINCT FROM`` in the merge ``ON`` clause is what makes this work;
    plain ``=`` would send every NULL-keyed batch down ``WHEN NOT MATCHED`` and
    grow one duplicate row per batch, with no error anywhere. Silence is the
    failure mode this framework exists to remove, so it gets an explicit test
    across three batches rather than two.
    """
    parity.land("n1", [(T(2026, 5, 1, 0, 10), None, 1.0), (T(2026, 5, 1, 0, 20), "s1", 2.0)])
    parity.run()
    parity.land("n2", [(T(2026, 5, 1, 0, 30), None, 4.0)])
    parity.run()
    parity.land("n3", [(T(2026, 5, 1, 0, 40), None, 8.0), (T(2026, 5, 1, 1, 0), None, 16.0)])
    parity.run()

    rows = parity.assert_matches_ground_truth()
    null_hour_zero = [r for r in rows if r[0] == T(2026, 5, 1, 0, 0) and r[1] is None]
    assert len(null_hour_zero) == 1, (
        f"the NULL grouping key produced {len(null_hour_zero)} rows in one "
        f"window; it must fold into exactly one: {rows}"
    )
    assert null_hour_zero[0][2] == 3
    assert null_hour_zero[0][3] == 13.0

    parity.assert_reached_matched_branch()
    parity.assert_snapshot_history_consistent()


def test_reruns_over_unchanged_input_change_nothing(parity):
    """Draining again with no new files is a no-op, three times over.

    Re-running is the ordinary case under cron: most ticks find nothing. It must
    not fold anything again, must not write a snapshot, and must not move the
    offset (``CONTEXT.md`` 1.8 -- an idle trigger that writes nothing stays at
    ~1.3 ms instead of paying ~17 ms of commit, and that saving is only real if
    it genuinely writes nothing).
    """
    parity.land("r1", [(T(2026, 6, 1, 0, 5), "s1", 1.0)])
    parity.run()
    parity.land("r2", [(T(2026, 6, 1, 0, 15), "s1", 2.0)])
    parity.run()

    settled_rows = parity.worlds["python"].rows()
    settled_snapshots = {d: w.snapshot_count() for d, w in parity.worlds.items()}
    settled_offsets = parity.worlds["python"].offset_files()

    for _ in range(3):
        summaries = parity.run()
        assert summaries["python"].committed == 0
        for door, world in parity.worlds.items():
            assert world.rows() == settled_rows, f"{door}: a re-run changed the mart"
            assert world.snapshot_count() == settled_snapshots[door], (
                f"{door}: an idle re-run wrote a snapshot"
            )
            assert world.offset_files() == settled_offsets

    parity.assert_matches_ground_truth()
    parity.assert_reached_matched_branch()


# --------------------------------------------------------------------------
# Chunked equals unchunked
# --------------------------------------------------------------------------


def test_chunked_equals_unchunked(make_parity, landing):
    """``max_files_per_trigger=1`` and unbounded produce identical output.

    ``PLAN.md`` asks for this "byte-identical ... for every ``non_foldable``
    model". Phase 1 has no non-foldable execution path -- the sink refuses the
    tier outright -- so the property is asserted where it *is* reachable: the
    additive delta merge, which is the tier whose whole claim is that batch
    boundaries do not matter.

    Equality is exact, not approximate. The fixture's values are integers and
    binary-exact halves, so six one-file batches and one six-file batch produce
    bit-identical sums, and any difference would be a real one.
    """
    drops = {
        "c1": [(T(2026, 7, 1, 0, 5), "s1", 1.0), (T(2026, 7, 1, 0, 20), "s2", 2.0)],
        "c2": [(T(2026, 7, 1, 0, 35), "s1", 4.0)],
        "c3": [(T(2026, 7, 1, 1, 5), None, 8.0)],
        "c4": [(T(2026, 7, 1, 1, 20), "s1", 16.0), (T(2026, 7, 1, 0, 45), "s2", 0.25)],
        "c5": [(T(2026, 7, 1, 2, 0), None, 32.0)],
        "c6": [(T(2026, 7, 1, 0, 55), "s1", 0.5)],
    }
    for name, payload in drops.items():
        landing.drop(name, payload)

    chunked = make_parity(ADDITIVE.chunked(1), name="chunked")
    unbounded = make_parity(ADDITIVE.chunked(None), name="unbounded")

    chunked_summaries = chunked.run()
    unbounded_summaries = unbounded.run()

    assert chunked_summaries["python"].committed == len(drops), (
        "max_files_per_trigger=1 should give one batch per file"
    )
    assert unbounded_summaries["python"].committed == 1, (
        "an unbounded trigger should take every file in one batch"
    )

    chunked_rows = chunked.worlds["python"].rows()
    unbounded_rows = unbounded.worlds["python"].rows()
    assert chunked_rows == unbounded_rows, (
        f"chunking changed the output:\n  chunked:   {chunked_rows}\n"
        f"  unbounded: {unbounded_rows}"
    )

    chunked.assert_matches_ground_truth()
    unbounded.assert_matches_ground_truth()
    chunked.assert_reached_matched_branch()
    chunked.assert_snapshot_history_consistent()

    # And the accounting differs exactly as it should: six triggers, six
    # snapshots against one trigger, one snapshot, over the same input.
    assert chunked.worlds["python"].snapshot_count() - unbounded.worlds[
        "python"
    ].snapshot_count() == len(drops) - 1


# --------------------------------------------------------------------------
# A second model shape, so "for every model" is not one model
# --------------------------------------------------------------------------


APPEND_PER_BATCH = Scenario(
    name="minute_append",
    aggregates={"n": "count(*)", "total": "sum(value)"},
    key=("window_ts", "sensor_id"),
    recompute_sql=(
        "SELECT date_trunc('minute', event_ts) AS window_ts, sensor_id,\n"
        "       count(*) AS n, sum(value) AS total\n"
        "  FROM {source} GROUP BY 1, 2"
    ),
    grain="minute",
    mode="append",
    table="marts.minute_append",
)


def test_append_mode_matches_recompute_when_batches_do_not_overlap(
    make_parity, landing
):
    """Append mode: no merge key, no fold, so batches must not share a window.

    Worth a conformance test because append is the tier-agnostic escape hatch
    the sink offers, and its contract is narrower than update's in a way that is
    easy to miss: it deduplicates nothing. With disjoint windows per batch the
    concatenation of per-batch aggregates *is* the full recompute, and that is
    the only condition under which append is equivalent. The engine's offset
    transaction is what stops a replayed batch appending twice -- the sink
    contributes nothing to that.
    """
    parity = make_parity(APPEND_PER_BATCH, name="append")
    parity.land("a1", [(T(2026, 8, 1, 0, 0, 5), "s1", 1.0), (T(2026, 8, 1, 0, 0, 30), "s1", 2.0)])
    parity.run()
    parity.land("a2", [(T(2026, 8, 1, 0, 1, 5), "s1", 4.0)])
    parity.run()
    parity.land("a3", [(T(2026, 8, 1, 0, 2, 5), None, 8.0)])
    parity.run()

    parity.assert_matches_ground_truth()
    parity.assert_reached_matched_branch()
    parity.assert_snapshot_history_consistent()


# --------------------------------------------------------------------------
# What phase 1 does not do
# --------------------------------------------------------------------------


def test_watermarks_are_not_implemented_in_phase_one():
    """Late-arrival semantics are phase 2. Pinned rather than quietly skipped.

    ``PLAN.md`` lists "late arrival within the horizon" under the ground-truth
    diff and "watermark semantics" under Verification, and it puts watermarks,
    window sealing and the lateness horizon in **phase 2**. There is therefore
    nothing to test yet, and the honest way to record that is an assertion that
    the capability is genuinely absent -- so that the day someone adds a
    lateness field, this test fails and the missing coverage is noticed at the
    moment it becomes possible to write.
    """
    import importlib

    from duckstream import Model

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("duckstream.watermark")

    fields = set(Model.__dataclass_fields__)
    for absent in ("lateness", "watermark", "horizon", "allowed_lateness"):
        assert absent not in fields, (
            f"Model has grown a {absent!r} field, so watermark semantics are no "
            f"longer out of scope and this suite needs the late-arrival "
            f"scenarios PLAN.md asks for"
        )

    # The state store carries a watermarks table and the engine commits an empty
    # watermark dict every trigger: the seam exists, the semantics do not.
    from duckstream.state import DuckLakeStateStore

    assert hasattr(DuckLakeStateStore, "load_watermark")


# --------------------------------------------------------------------------
# NULL measure values: the fold's identity element
# --------------------------------------------------------------------------


def _by_key(rows):
    """Index mart rows of the ADDITIVE scenario by ``(window_ts, sensor_id)``."""
    return {(row[0], row[1]): row[2:] for row in rows}


def test_a_null_measure_delta_does_not_erase_a_stored_value(parity):
    """An all-NULL batch must be the fold's identity, not an eraser.

    ``PLAN.md``'s ground-truth list names NULL *grouping keys*, which
    :func:`test_null_grouping_keys_fold_into_one_row` covers. This is the other
    NULL: a NULL *measure*, which is not in the letter of the list and is asked
    for anyway, because it is the exact bug class the framework exists to
    remove. It fails silently, it is reachable from ordinary data -- one landing
    file where every ``value`` is NULL for a key -- and once it happens it is
    permanent, because every later ``WHEN MATCHED`` propagates the NULL forward.

    The arithmetic is why. ``sum`` over a batch of NULLs is NULL, and ``NULL +
    5`` is NULL, so a bare ``t.total + s.total`` fold replaces a correct stored
    total with NULL and keeps doing so. What makes the additive tier a monoid is
    an *identity*: combining with nothing leaves the value alone. The fold has
    to encode that explicitly for ``sum``/``count`` -- ``least``/``greatest``
    already ignore NULL operands, which this test also pins, since a fold
    rewritten as a ``CASE`` would lose it.

    Both directions are exercised, because the identity has two sides:

    =========  ==============================  ==============================
    key        batch 1                         batch 2
    =========  ==============================  ==============================
    ``K1``     real values, total 5.0          all NULL -- total must survive
    ``N1``     all NULL, total NULL            real values -- must become 10.0
    ``Z``      all NULL                        all NULL -- stays NULL, not 0
    =========  ==============================  ==============================

    Values are integers and binary-exact halves, so every assertion below is an
    exact equality rather than a tolerance.
    """
    hour = T(2027, 5, 1, 0, 0)

    parity.land(
        "nm1",
        [
            (T(2027, 5, 1, 0, 5), "K1", 1.0),
            (T(2027, 5, 1, 0, 20), "K1", 4.0),
            (T(2027, 5, 1, 0, 22), "N1", None),
            (T(2027, 5, 1, 0, 25), "Z", None),
        ],
    )
    parity.run()

    stored = _by_key(parity.worlds["python"].rows())
    assert stored[(hour, "K1")] == (2, 5.0, 1.0, 4.0)
    assert stored[(hour, "N1")] == (1, None, None, None), (
        "a batch whose measures are all NULL must aggregate to NULL, not to 0"
    )
    parity.assert_matches_ground_truth()

    # Batch 2: a NULL delta over a real stored value, and the reverse.
    parity.land(
        "nm2",
        [
            (T(2027, 5, 1, 0, 40), "K1", None),
            (T(2027, 5, 1, 0, 45), "N1", 2.0),
            (T(2027, 5, 1, 0, 50), "N1", 8.0),
            (T(2027, 5, 1, 0, 55), "Z", None),
        ],
    )
    parity.run()

    folded = _by_key(parity.worlds["python"].rows())

    # The headline: the stored total survived a NULL delta untouched.
    n, total, lo, hi = folded[(hour, "K1")]
    assert total == 5.0, (
        f"an all-NULL batch erased a correct stored total: 5.0 became {total!r}. "
        f"The additive fold is not a monoid with an identity -- 'sum(value) + "
        f"NULL' is NULL, so every later matched batch will propagate it forever."
    )
    assert (lo, hi) == (1.0, 4.0), (
        f"min/max did not survive a NULL delta: got {(lo, hi)!r}, expected "
        f"(1.0, 4.0). least()/greatest() ignore NULL operands and the fold must "
        f"keep relying on that."
    )
    assert n == 3, "count(*) is never NULL and must still have folded"

    # The other side of the identity: a NULL stored value plus a real delta.
    assert folded[(hour, "N1")] == (3, 10.0, 2.0, 8.0), (
        f"a real delta folded into a NULL stored value gave "
        f"{folded[(hour, 'N1')]!r}; NULL is the identity, so the result must be "
        f"the delta itself"
    )

    # Identity, not zero. A fold "fixed" as coalesce(t,0) + coalesce(s,0) would
    # turn an all-NULL key into 0.0 and disagree with SQL's own sum().
    assert folded[(hour, "Z")] == (2, None, None, None), (
        f"a key whose measures are NULL in every batch became "
        f"{folded[(hour, 'Z')]!r}; it must stay NULL, because sum() over NULLs "
        f"is NULL and 0 would be a value nobody measured"
    )

    parity.assert_matches_ground_truth()

    # A third batch, so the NULL-carrying rows take the matched branch twice.
    parity.land(
        "nm3",
        [(T(2027, 5, 1, 0, 58), "K1", 0.5), (T(2027, 5, 1, 0, 59), "Z", None)],
    )
    parity.run()

    final = _by_key(parity.worlds["python"].rows())
    assert final[(hour, "K1")] == (4, 5.5, 0.5, 4.0)
    assert final[(hour, "Z")] == (3, None, None, None)

    # And the whole point: the hand-written recompute agrees, because SQL's own
    # sum() skips NULLs. The fold is only correct if it reproduces that.
    parity.assert_matches_ground_truth()
    parity.assert_reached_matched_branch()
    parity.assert_snapshot_history_consistent()
