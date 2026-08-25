"""Tier three, against a full recompute, through both doors.

This is the tier ``PLAN.md`` calls the reason to build duckstream rather than
adopt Arroyo, and ``CONTEXT.md`` section 4 records the production bug it exists
to make impossible:

    An FFT mart transformed only the current batch's rows, so a one-minute
    window fed by 30-second batches held a spectrum over half a window:
    **sample_count 100 instead of 400, and 51 spectrum bins instead of 201.**

Nothing failed. The mart looked current, the numbers looked plausible, and it
was found by a reconciliation query months later.

So every scenario here is built the way that bug was built -- a window fed by
**several batches**, none of which contains the whole window -- and diffed
against a full recompute from source. A model that folded batch-by-batch would
pass a single-batch test perfectly, which is why there isn't one.

What makes this tier different from the two below it is that there is no state
to carry: the affected windows are read again, in full, out of the files that
were consumed to build them. Three things therefore have to hold at once, and
each has its own test:

* **the answer is the recompute** -- the mart equals a full recompute of the
  whole landing tree, after every drain, not merely at the end;
* **a window already written is corrected, not appended to** -- a later batch
  landing in an earlier window replaces that window's row;
* **chunking changes nothing but memory** -- ``PLAN.md``'s "chunked equals
  unchunked", which for this tier is the property that says the file selection
  and the range predicate agree.
"""

from __future__ import annotations

import datetime as dt

import pytest

from harness import Scenario, same_rows

T = dt.datetime

#: Aggregates with no decomposition at all, each wrong in a different way if
#: folded. ``n`` rides along additive on purpose: a model's tier is its worst
#: column, so this whole model is tier three while ``count(*)`` beside it is
#: still additive -- and in a *recompute* it is simply recomputed like the rest,
#: which is the one case where the mixed-tier rule costs nothing.
RECOMPUTED = Scenario(
    name="hourly_shape",
    aggregates={
        "n": "count(*)",
        "mid": "median(value)",
        "spread": "ds_spread(list(value ORDER BY event_ts))",
        "gap": "ds_gap(list(value ORDER BY event_ts))",
        "sensors": "count(DISTINCT sensor_id)",
    },
    key=("window_ts",),
    recompute_sql=(
        "SELECT date_trunc('hour', event_ts) AS window_ts,\n"
        "       count(*) AS n,\n"
        "       median(value) AS mid,\n"
        "       ds_spread(list(value ORDER BY event_ts)) AS spread,\n"
        "       ds_gap(list(value ORDER BY event_ts)) AS gap,\n"
        "       count(DISTINCT sensor_id) AS sensors\n"
        "  FROM {source}\n"
        " GROUP BY 1"
    ),
    grain="hour",
    strategy="recompute_window",
    memory_profile="materialising",
    udfs=("_udfs:spread", "_udfs:first_last_gap"),
    table="marts.hourly_shape",
)

BASE = T(2026, 6, 1, 10)


def rows(values, *, start: int = 0, sensor: str = "s1", hour: int = 0):
    """One row per value, a second apart, inside ``BASE + hour``."""
    return [
        (BASE + dt.timedelta(hours=hour, seconds=start + i), sensor, float(v))
        for i, v in enumerate(values)
    ]


# --------------------------------------------------------------------------
# The regression this tier exists for
# --------------------------------------------------------------------------


def test_a_window_fed_by_several_batches_is_recomputed_not_folded(make_parity):
    """``CONTEXT.md`` section 4's FFT mart, reduced to numbers a test can assert.

    One hour, four batches, no batch holding the whole window. Every aggregate
    here is wrong in a *different* way if the last batch is allowed to overwrite
    the window:

    * ``n`` would be 4 rather than 16 (section 4's "sample_count 100 instead
      of 400");
    * ``spread`` would be the range of the last four values, not of all
      sixteen;
    * ``gap`` would be last-minus-first *within a batch*, which is the
      order-dependence that cannot survive batching at all.

    The values are chosen so a fold and a recompute cannot coincide: the last
    batch is the *narrowest*, so an overwrite lands strictly inside the true
    range rather than on it.
    """
    parity = make_parity(RECOMPUTED, name="section4")
    batches = [
        [0.0, 100.0, 5.0, 5.0],
        [50.0, 51.0, 52.0, 53.0],
        [20.0, 21.0, 22.0, 23.0],
        [40.0, 41.0, 42.0, 43.0],
    ]
    for index, values in enumerate(batches):
        parity.land(f"b{index}", rows(values, start=index * 10))
        parity.run()

    parity.assert_matches_ground_truth()
    parity.assert_reached_matched_branch()

    got = parity.worlds["python"].rows()
    assert len(got) == 1, "one hour, one row"
    _window, n, mid, spread, gap, sensors = got[0]
    assert n == 16, f"folded to {n}; section 4's mart held a quarter of its window"
    assert spread == pytest.approx(100.0), (
        f"spread came out {spread}; the last batch alone would give 3.0"
    )
    assert gap == pytest.approx(43.0 - 0.0), (
        f"gap came out {gap}; a per-batch fold would give 3.0"
    )
    assert sensors == 1


def test_the_mart_equals_a_full_recompute_after_every_drain(make_parity):
    """The ordinary diff: several windows, several keys, interleaved batches.

    Checked after every drain rather than only at the end. A recompute that
    selected the wrong files could still finish in the right place if the last
    batch happened to touch everything.
    """
    parity = make_parity(RECOMPUTED, name="diff")
    schedule = [
        rows([1.0, 2.0, 3.0], hour=0) + rows([9.0, 8.0], hour=1),
        rows([4.0], hour=0, start=50) + rows([7.0, 6.0], hour=2),
        rows([5.5, 0.5], hour=1, start=50),
        rows([2.5], hour=2, start=50) + rows([11.0], hour=0, start=90),
    ]
    for index, payload in enumerate(schedule):
        parity.land(f"d{index}", payload)
        parity.run()
        parity.assert_matches_ground_truth()

    parity.assert_reached_matched_branch()
    parity.assert_snapshot_history_consistent()


def test_a_late_batch_corrects_the_window_it_lands_in(make_parity):
    """An hour written two drains ago is *replaced*, not added to.

    This is what "recompute the affected window" buys that no fold does. The
    first drain writes hour 0 and hour 2; the second lands more rows in hour 0,
    which must be re-derived from every row it now has -- while hour 2, which
    nothing touched, must be left exactly as it was rather than recomputed or
    dropped.
    """
    parity = make_parity(RECOMPUTED, name="late")
    parity.land("first", rows([1.0, 2.0], hour=0) + rows([30.0, 31.0], hour=2))
    parity.run()
    before = {row[0]: row for row in parity.worlds["python"].rows()}

    parity.land("second", rows([100.0], hour=0, start=50))
    parity.run()
    parity.assert_matches_ground_truth()

    after = {row[0]: row for row in parity.worlds["python"].rows()}
    assert set(after) == set(before), "no window appeared or vanished"

    touched = BASE.replace(minute=0, second=0, microsecond=0)
    untouched = touched + dt.timedelta(hours=2)
    assert after[touched] != before[touched], "the touched window was not corrected"
    assert after[touched][1] == 3, "hour 0 should hold all three of its rows"
    assert after[untouched] == before[untouched], (
        "an untouched window was rewritten; only affected windows are recomputed"
    )


# --------------------------------------------------------------------------
# Chunked equals unchunked
# --------------------------------------------------------------------------


@pytest.mark.parametrize("max_rows", [1, 3, None])
def test_chunking_changes_nothing_but_memory(make_parity, max_rows):
    """``PLAN.md``'s "chunked equals unchunked", for the tier it was written for.

    ``max_rows_per_trigger`` is the recompute's chunk budget as well as the
    source's batch limit (``CONTEXT.md`` 1.1 names one lever for both), so this
    drives the window-range chunking from one window per chunk up to unbounded
    and asserts the answer does not move.

    It is the property that catches a file-selection bug specifically: a chunk
    narrower than the touched span selects a *different* set of files, so a
    range predicate and a file filter that disagree show up here and nowhere
    else.

    Phase 2's deliberate exception does not apply -- that one is about a
    lateness horizon making a batch boundary decide what is late, and this
    scenario declares no horizon.
    """
    scenario = RECOMPUTED
    if max_rows is not None:
        from dataclasses import replace as _replace

        scenario = _replace(scenario, max_rows_per_trigger=max_rows)

    parity = make_parity(scenario, name=f"chunk{max_rows}")
    for index in range(3):
        parity.land(
            f"c{index}",
            rows([1.0, 5.0, 3.0], hour=index % 2, start=index * 20),
        )
        parity.run()

    parity.assert_matches_ground_truth()
    parity.assert_reached_matched_branch()


def test_every_chunk_size_gives_byte_identical_output(make_parity, tmp_path):
    """The same claim, stated as a direct comparison rather than per-run.

    Three parities, three chunk budgets, one landing schedule. Comparing the
    *results to each other* is stronger than comparing each to the ground truth:
    it would catch a recompute that was wrong in a way the hand-written
    recompute SQL was also wrong in.

    Each parity gets its **own** landing tree. The two doors of one parity share
    one deliberately; two parities must not, because a file source scans the
    whole tree and the second would consume the first's drops as well as its own
    -- which presents as the engine folding a row twice.
    """
    from dataclasses import replace as _replace

    from harness import Landing

    schedule = [
        rows([1.0, 5.0, 3.0], hour=0),
        rows([2.0], hour=0, start=40) + rows([8.0, 9.0], hour=1),
        rows([4.0, 0.0], hour=1, start=40),
    ]

    results = []
    for max_rows in (1, 2, None):
        scenario = RECOMPUTED
        if max_rows is not None:
            scenario = _replace(scenario, max_rows_per_trigger=max_rows)
        parity = make_parity(
            scenario,
            name=f"identical{max_rows}",
            landing=Landing(tmp_path / f"tree{max_rows}"),
        )
        for index, payload in enumerate(schedule):
            parity.land(f"e{index}", payload)
            parity.run()
        parity.assert_matches_ground_truth()
        results.append((max_rows, parity.worlds["python"].rows()))

    reference = results[0][1]
    for max_rows, got in results[1:]:
        assert same_rows(got, reference), (
            f"chunk budget {max_rows} disagreed with 1 row per chunk:\n"
            f"  {got}\n  {reference}"
        )
