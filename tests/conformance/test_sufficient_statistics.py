"""Tier two, against a full recompute, through both doors.

`CONTEXT.md` section 4 records why this tier is the reason the project exists.
A mart in this repository folded averages as ``(target.avg + source.avg) / 2``:
with 300 samples of 1.0 followed by 100 of 5.0 the correct average is **2.0**
and it held **3.0**, and the standard deviation should have been **1.7342** and
had been overwritten to **0.0**. Nothing failed. Nobody noticed until it was
diffed against a full recompute.

So that is the diff these scenarios run, and those are the numbers the first one
asserts by name. The batching is deliberate throughout: the populations never
share a batch, because a fold that is wrong across batches is exactly right
within one, and a scenario that put all the data in a single trigger would pass
against any implementation at all.

The other thing being defended here is numerical, and it is subtler. The
textbook state for this tier is ``(n, sum, sum_sq)``, which `CONTEXT.md` 1.14
measured returning **524** for a true variance of 0.25 at Unix-timestamp
magnitudes, and exactly **0.0** at 1e8 with a small spread -- the same number
the section-4 mart produced, from an entirely different cause. duckstream stores
``(n, mean, M2)`` instead, and
:func:`test_the_statistics_survive_magnitudes_that_break_the_textbook_form`
is what keeps that decision from being quietly undone.
"""

from __future__ import annotations

import datetime as dt

import pytest

from harness import Scenario, same_rows

T = dt.datetime

#: count(*) rides along on purpose: a model's tier is its worst column, so this
#: whole model is `sufficient_statistics` while `n` is still additive and must
#: still fold by addition rather than acquire a state it has no use for.
STATS = Scenario(
    name="hourly_stats",
    aggregates={
        "n": "count(*)",
        "mean_v": "avg(value)",
        "sd_v": "stddev_samp(value)",
        "var_v": "var_samp(value)",
        "sdp_v": "stddev_pop(value)",
    },
    key=("window_ts", "sensor_id"),
    recompute_sql=(
        "SELECT date_trunc('hour', event_ts) AS window_ts,\n"
        "       sensor_id,\n"
        "       count(*) AS n,\n"
        "       avg(value) AS mean_v,\n"
        "       stddev_samp(value) AS sd_v,\n"
        "       var_samp(value) AS var_v,\n"
        "       stddev_pop(value) AS sdp_v\n"
        "  FROM {source}\n"
        " GROUP BY 1, 2"
    ),
    grain="hour",
    table="marts.hourly_stats",
)

BASE = T(2026, 6, 1, 10)


def rows(count: int, value: float, *, start: int = 0, sensor: str = "s1"):
    return [
        (BASE + dt.timedelta(seconds=start + i), sensor, value) for i in range(count)
    ]


# --------------------------------------------------------------------------
# The regression this tier exists for
# --------------------------------------------------------------------------


def test_the_production_bug_from_context_section_four_cannot_happen(make_parity):
    """300 samples of 1.0 then 100 of 5.0: the average is 2.0, not 3.0.

    Split across four batches so the two populations never share one, which is
    the shape that made the original wrong. An unweighted average of averages
    gives 3.0 here; overwriting the standard deviation with the last batch's
    gives 0.0. Both are asserted by value rather than only against the
    recompute, because those two numbers are the whole point.
    """
    parity = make_parity(STATS, name="section4")
    for index, (count, value) in enumerate(
        [(150, 1.0), (150, 1.0), (50, 5.0), (50, 5.0)]
    ):
        parity.land(f"b{index}", rows(count, value, start=index * 200))
        parity.run()

    parity.assert_matches_ground_truth()
    parity.assert_reached_matched_branch()

    row = parity.worlds["python"].rows()[0]
    _window, _sensor, n, mean_v, sd_v, *_ = row
    assert n == 400
    assert mean_v == pytest.approx(2.0, abs=1e-12), (
        f"the average folded to {mean_v}; section 4's mart held 3.0"
    )
    assert sd_v == pytest.approx(1.7342199390482398, abs=1e-12), (
        f"the standard deviation folded to {sd_v}; section 4's mart held 0.0"
    )


def test_every_statistic_equals_a_full_recompute_across_many_batches(make_parity):
    """The ordinary diff: interleaved batches, several keys, several windows.

    Checked after *every* drain rather than only at the end -- a fold that is
    wrong in one direction and wrong again in the other can still finish in the
    right place.
    """
    parity = make_parity(STATS, name="diff")
    schedule = [
        rows(20, 1.0) + rows(20, 3.0, start=100, sensor="s2"),
        rows(15, 7.0, start=200) + rows(5, 2.0, start=300, sensor="s2"),
        rows(30, 4.5, start=400),
        rows(10, 9.0, start=500, sensor="s2") + rows(10, 0.5, start=600),
    ]
    for index, payload in enumerate(schedule):
        parity.land(f"d{index}", payload)
        parity.run()
        parity.assert_matches_ground_truth()

    parity.assert_reached_matched_branch()
    parity.assert_snapshot_history_consistent()


def test_a_single_observation_gives_a_null_sample_statistic(make_parity):
    """``stddev_samp`` of one row is NULL, and the derived column must agree.

    DuckDB returns NULL there, so anything else would mean the mart and a full
    recompute disagree on the *definition* rather than on the arithmetic -- and
    the ground-truth diff would be comparing two different questions.
    """
    parity = make_parity(STATS, name="single")
    parity.land("one", rows(1, 42.0))
    parity.run()
    parity.land("two", rows(1, 43.0, start=5000, sensor="s2"))
    parity.run()

    parity.assert_matches_ground_truth()
    for door, world in parity.worlds.items():
        for row in world.rows():
            _w, _s, n, mean_v, sd_v, var_v, sdp_v = row
            assert n == 1, door
            assert mean_v == pytest.approx(42.0) or mean_v == pytest.approx(43.0)
            assert sd_v is None, f"{door}: stddev_samp of one row should be NULL"
            assert var_v is None, door
            assert sdp_v == 0.0, f"{door}: stddev_pop of one row is 0, not NULL"


# --------------------------------------------------------------------------
# The numerical decision
# --------------------------------------------------------------------------


MAGNITUDE = Scenario(
    name="magnitude",
    aggregates={"sd_v": "stddev_samp(value)", "var_v": "var_samp(value)"},
    key=("sensor_id",),
    recompute_sql=(
        "SELECT sensor_id, stddev_samp(value) AS sd_v, var_samp(value) AS var_v\n"
        "  FROM {source} GROUP BY 1"
    ),
    time_column=None,
    grain=None,
    table="marts.magnitude",
)


def test_the_statistics_survive_magnitudes_that_break_the_textbook_form(make_parity):
    """Values around 1e8 with a spread of 1.0 -- where ``sum_sq`` returns 0.0.

    `CONTEXT.md` 1.14 measured the textbook ``(n, sum, sum_sq)`` state giving
    exactly **0.0** for this data, which is both catastrophically wrong and
    indistinguishable from a sensor that never moves. duckstream stores
    ``(n, mean, M2)`` instead, and this is the test that stops that being
    quietly reverted to the simpler-looking form.

    The tolerance is loose on purpose and stated rather than hidden: at these
    magnitudes DuckDB's own single-pass aggregate is itself a float64
    approximation, so the two agree to about nine significant figures rather
    than to the suite's usual 1e-12. Against the textbook form's factor of
    2,096 on comparable data, nine figures is not a close call.
    """
    parity = make_parity(MAGNITUDE, name="magnitude")
    for index in range(8):
        payload = [
            (BASE + dt.timedelta(seconds=index * 500 + i), "s1",
             1e8 + (0.5 if i % 2 else -0.5))
            for i in range(500)
        ]
        parity.land(f"m{index}", payload)
        parity.run()

    parity.assert_reached_matched_branch()
    expected = parity.worlds["python"].recompute()
    for door, world in parity.worlds.items():
        actual = world.rows()
        assert len(actual) == 1, door
        (_sensor, sd_v, var_v) = actual[0]
        (_es, e_sd, e_var) = expected[0]
        assert sd_v not in (None, 0.0), (
            f"{door}: the standard deviation collapsed to {sd_v} -- this is the "
            f"exact failure the (n, mean, M2) state exists to prevent"
        )
        assert sd_v == pytest.approx(e_sd, rel=1e-6), door
        assert var_v == pytest.approx(e_var, rel=1e-6), door


# --------------------------------------------------------------------------
# What is still refused
# --------------------------------------------------------------------------


def test_a_two_argument_statistic_is_still_refused_at_load(landing):
    """corr and covar need cross terms and are not built.

    Refused rather than approximated, and the message says which of the two
    problems it is -- "not implemented" reads very differently from "cannot be
    done", and only one of them is true here.
    """
    from duckstream.errors import DuckstreamError
    from duckstream.sinks.table import TableSink
    from duckstream.sources.files import FileSource

    from duckstream import Model

    model = Model(
        name="bivariate",
        source=FileSource(str(landing.root), marker="_READY"),
        sink=TableSink("marts.bivariate", mode="update"),
        aggregates={"c": "corr(value, value)"},
        key=["sensor_id"],
    )
    model.validate()
    assert model.resolved_strategy == "sufficient_statistics"

    with pytest.raises(DuckstreamError) as excinfo:
        model.sink.output_columns(model)
    message = str(excinfo.value)
    assert "corr()" in message
    assert "cross terms" in message
    assert "recompute_window" in message
