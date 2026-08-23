"""Load-time validation of ``Model``.

This is the specification for the framework's value proposition. ``PLAN.md``:
"the framework refusing an additive strategy over a non-foldable aggregate --
at load time, not runtime -- is its reason to exist." So the rejection tests
below assert on *message content*, not just on exception type: an error an
operator cannot act on is barely better than no error at all.

Sources and sinks are faked in this module. Nothing here needs a real one, and
the point of the structural protocols in ``duckstream.protocols`` is that a
plain object of the right shape is a source.
"""

from __future__ import annotations

from dataclasses import fields
from typing import Any, ClassVar

import pytest

from duckstream.aggregates import Tier
from duckstream.errors import DuckstreamError, ModelValidationError
from duckstream.model import GRAINS, MEMORY_PROFILES, WINDOW_COLUMN, Model
from duckstream.protocols import BatchLimits, BatchPlan, Offset, Sink, Source

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeSource:
    """A structurally valid source. No real one exists yet; none is needed."""

    type_name: ClassVar[str] = "fake"

    def __init__(self, path: str = "landing/") -> None:
        self.path = path

    def latest_offset(self) -> Offset:
        return {"seq": 0}

    def plan(
        self, start: Offset | None, end: Offset, limits: BatchLimits
    ) -> BatchPlan:
        return BatchPlan.empty(start, end)

    def bind(self, con, plan: BatchPlan) -> str:
        return "fake_batch"

    def to_config(self) -> dict[str, Any]:
        return {"type": self.type_name, "path": self.path}

    def __eq__(self, other: object) -> bool:
        return isinstance(other, FakeSource) and other.path == self.path


class FakeSink:
    type_name: ClassVar[str] = "fake"

    def __init__(self, table: str = "marts.t", mode: str = "update") -> None:
        self.table = table
        self.mode = mode

    def ensure(self, con, model: Model) -> None:
        return None

    def write(self, con, batch_view: str, model: Model, ctx) -> None:
        return None

    def to_config(self) -> dict[str, Any]:
        return {"type": self.type_name, "table": self.table, "mode": self.mode}

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, FakeSink)
            and other.table == self.table
            and other.mode == self.mode
        )


def make_model(**overrides: Any) -> Model:
    """An otherwise valid additive model, so each test perturbs exactly one thing."""
    kwargs: dict[str, Any] = {
        "name": "hourly_counts",
        "source": FakeSource(),
        "sink": FakeSink(),
        "aggregates": {"n": "count(*)", "total": "sum(value)"},
        "key": ["sensor_id"],
    }
    kwargs.update(overrides)
    return Model(**kwargs)


def test_the_baseline_model_is_valid() -> None:
    """If this ever fails, every other test in the file is testing the wrong thing."""
    make_model().validate()


# ---------------------------------------------------------------------------
# THE HEADLINE REJECTION
#
# An additive strategy declared over an aggregate that does not fold. This is
# the bug class from CONTEXT.md section 4 -- a mart that folded averages and
# held 3.0 where the truth was 2.0 -- refused before a single row is read.
# ---------------------------------------------------------------------------

REJECTIONS: list[tuple[str, str, str, str]] = [
    # id, aggregate expression, aggregate named in the message, tier named
    ("median", "median(value)", "median", "non_foldable"),
    ("count_distinct", "count(DISTINCT sensor_id)", "count(DISTINCT ...)", "non_foldable"),
    ("udf_over_list", "arrow_fft(list(value ORDER BY event_ts))", "arrow_fft", "non_foldable"),
    ("avg", "avg(value)", "avg", "sufficient_statistics"),
    ("stddev", "stddev(value)", "stddev", "sufficient_statistics"),
]


@pytest.mark.parametrize(
    "expr,aggregate,tier",
    [(e, a, t) for _, e, a, t in REJECTIONS],
    ids=[i for i, _, _, _ in REJECTIONS],
)
def test_delta_merge_over_a_non_additive_aggregate_is_refused_at_load_time(
    expr: str, aggregate: str, tier: str
) -> None:
    model = make_model(aggregates={"result": expr}, strategy="delta_merge")

    with pytest.raises(ModelValidationError) as excinfo:
        model.validate()

    message = str(excinfo.value)
    # the message has to be actionable on its own, so it must carry:
    assert "hourly_counts" in message, "the model that failed"
    assert "'result'" in message, "the column that failed"
    assert repr(expr) in message, "the expression that failed"
    assert aggregate in message, "the aggregate responsible"
    assert tier in message, "the tier it was classified into"
    assert "delta_merge" in message, "the strategy that was declared"
    # and it must say what to do instead
    assert excinfo.value.remedy
    assert "strategy" in excinfo.value.remedy


def test_the_rejection_happens_before_anything_executes() -> None:
    """Validation must not touch the source or the sink.

    A load-time rejection that first opened a connection would not be a
    load-time rejection.
    """

    class ExplodingSource(FakeSource):
        def latest_offset(self) -> Offset:  # pragma: no cover - must not run
            raise AssertionError("validation executed the source")

        def plan(self, start, end, limits):  # pragma: no cover - must not run
            raise AssertionError("validation executed the source")

        def bind(self, con, plan):  # pragma: no cover - must not run
            raise AssertionError("validation executed the source")

    model = make_model(
        source=ExplodingSource(),
        aggregates={"p50": "median(value)"},
        strategy="delta_merge",
    )
    with pytest.raises(ModelValidationError):
        model.validate()


def test_sufficient_statistics_strategy_over_a_non_foldable_model_is_refused() -> None:
    model = make_model(
        aggregates={"p50": "median(value)"},
        strategy="sufficient_statistics",
        time_column="event_ts",
        memory_profile="streaming",
    )
    with pytest.raises(ModelValidationError) as excinfo:
        model.validate()
    assert "non_foldable" in str(excinfo.value)
    assert "recompute_window" in excinfo.value.remedy


def test_a_stronger_strategy_than_needed_is_accepted_silently() -> None:
    """recompute_window over an additive model is correct, just slower."""
    make_model(
        strategy="recompute_window", time_column="event_ts"
    ).validate()
    make_model(strategy="sufficient_statistics").validate()


def test_the_strategy_conflict_wins_over_the_missing_memory_profile() -> None:
    """Check order is part of the contract.

    A user who wrote ``strategy: delta_merge`` over a median made one mistake.
    Telling them about a missing memory_profile first would send them to fix the
    wrong thing.
    """
    model = make_model(aggregates={"p50": "median(value)"}, strategy="delta_merge")
    with pytest.raises(ModelValidationError) as excinfo:
        model.validate()
    assert excinfo.value.field == "strategy"


# ---------------------------------------------------------------------------
# Rule 1 -- name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["hourly_counts", "_x", "marts.hourly_counts", "a1"])
def test_valid_names_are_accepted(name: str) -> None:
    make_model(name=name).validate()


@pytest.mark.parametrize(
    "name", ["", "   ", "1abc", "a-b", "a.b.c", "drop table t", "a b", "a."]
)
def test_invalid_names_are_rejected(name: str) -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(name=name).validate()
    assert excinfo.value.field == "name"


# ---------------------------------------------------------------------------
# Rule 2 -- aggregates
# ---------------------------------------------------------------------------


def test_empty_aggregates_are_rejected() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(aggregates={}).validate()
    assert excinfo.value.field == "aggregates"


def test_an_invalid_output_column_name_is_rejected() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(aggregates={"total rows": "count(*)"}).validate()
    assert "'total rows'" in str(excinfo.value)


def test_an_unclassifiable_expression_is_rejected() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(aggregates={"n": "count("}).validate()
    assert "'count('" in str(excinfo.value)
    assert "hourly_counts" in str(excinfo.value)


def test_a_non_aggregate_expression_is_rejected() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(aggregates={"v": "value"}).validate()
    assert "no aggregate function" in str(excinfo.value)


def test_a_column_cannot_be_both_a_key_and_an_aggregate() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(
            key=["sensor_id"], aggregates={"sensor_id": "count(*)"}
        ).validate()
    assert "'sensor_id'" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Rule 3 -- key
# ---------------------------------------------------------------------------


def test_an_empty_key_is_rejected() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(key=[]).validate()
    assert excinfo.value.field == "key"
    assert "idempotent" in str(excinfo.value)


def test_a_duplicated_key_column_is_rejected() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(key=["sensor_id", "sensor_id"]).validate()
    assert "more than once" in str(excinfo.value)


def test_a_malformed_key_column_is_rejected() -> None:
    with pytest.raises(ModelValidationError):
        make_model(key=["sensor id"]).validate()


# ---------------------------------------------------------------------------
# Rule 4 -- strategy vocabulary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "strategy", ["delta_merge", "sufficient_statistics", "recompute_window"]
)
def test_every_declared_strategy_name_is_accepted_over_a_compatible_model(
    strategy: str,
) -> None:
    make_model(strategy=strategy, time_column="event_ts").validate()


@pytest.mark.parametrize("strategy", ["merge", "DELTA_MERGE", "fold", ""])
def test_an_unknown_strategy_name_is_rejected(strategy: str) -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(strategy=strategy).validate()
    assert excinfo.value.field == "strategy"


# ---------------------------------------------------------------------------
# Rule 5 -- non_foldable requirements
# ---------------------------------------------------------------------------


def test_a_non_foldable_model_missing_both_requirements_names_both() -> None:
    model = make_model(aggregates={"p50": "median(value)"})
    with pytest.raises(ModelValidationError) as excinfo:
        model.validate()
    message = str(excinfo.value)
    assert "time_column" in message
    assert "memory_profile" in message
    assert "non_foldable" in message
    assert "median" in message


def test_a_non_foldable_model_missing_only_the_memory_profile() -> None:
    model = make_model(aggregates={"p50": "median(value)"}, time_column="event_ts")
    with pytest.raises(ModelValidationError) as excinfo:
        model.validate()
    assert "memory_profile" in str(excinfo.value)
    assert excinfo.value.field == "memory_profile"


def test_a_non_foldable_model_missing_only_the_time_column() -> None:
    model = make_model(
        aggregates={"p50": "median(value)"}, memory_profile="materialising"
    )
    with pytest.raises(ModelValidationError) as excinfo:
        model.validate()
    assert excinfo.value.field == "time_column"


def test_a_fully_declared_non_foldable_model_is_valid() -> None:
    make_model(
        aggregates={"spectrum": "arrow_fft(list(value ORDER BY event_ts))"},
        time_column="event_ts",
        grain="minute",
        key=[WINDOW_COLUMN, "sensor_id"],
        strategy="recompute_window",
        memory_profile="materialising",
        udfs=["my_pkg.signal:arrow_fft"],
    ).validate()


def test_a_wrapped_aggregate_is_non_foldable_and_inherits_its_requirements() -> None:
    """The tier-two narrowing, seen from the model.

    `sum(a)/count(*)` used to resolve to strategy `sufficient_statistics`, which
    named a maintenance plan -- store count/sum/sum_sq -- that cannot represent
    a ratio. It is now non_foldable, so the model must say how to recompute it.
    """
    bare = make_model(aggregates={"ratio": "sum(a)/count(*)"})
    assert bare.tier is Tier.NON_FOLDABLE
    with pytest.raises(ModelValidationError) as excinfo:
        bare.validate()
    assert "non_foldable" in str(excinfo.value)

    make_model(
        aggregates={"ratio": "sum(a)/count(*)"},
        time_column="event_ts",
        memory_profile="streaming",
    ).validate()


def test_declaring_delta_merge_over_a_wrapped_additive_expression_is_refused() -> None:
    model = make_model(
        aggregates={"ratio": "sum(a)/count(*)"},
        strategy="delta_merge",
        time_column="event_ts",
        memory_profile="streaming",
    )
    with pytest.raises(ModelValidationError) as excinfo:
        model.validate()
    message = str(excinfo.value)
    assert "non_foldable" in message
    assert "a scalar expression wrapping sum(...)" in message


def test_sufficient_statistics_models_do_not_need_a_memory_profile() -> None:
    """Only tier three has to materialise; tier two folds components."""
    make_model(aggregates={"mean_v": "avg(value)"}).validate()


# ---------------------------------------------------------------------------
# Rule 6 -- grain
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("grain", GRAINS)
def test_supported_grains_are_accepted(grain: str) -> None:
    make_model(
        grain=grain, time_column="event_ts", key=[WINDOW_COLUMN, "sensor_id"]
    ).validate()


@pytest.mark.parametrize("grain", ["second", "week", "month", "HOUR", "5 minutes"])
def test_unsupported_grains_are_rejected(grain: str) -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(
            grain=grain, time_column="event_ts", key=[WINDOW_COLUMN]
        ).validate()
    assert excinfo.value.field == "grain"


def test_a_grain_without_a_time_column_is_rejected() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(grain="hour", key=[WINDOW_COLUMN, "sensor_id"]).validate()
    assert excinfo.value.field == "time_column"


# ---------------------------------------------------------------------------
# Rule 7 -- the sink merge key must equal the window grain key
# ---------------------------------------------------------------------------


def test_a_windowed_model_whose_key_omits_the_window_column_is_rejected() -> None:
    """Otherwise idempotency silently breaks -- PLAN.md calls this an invariant.

    A re-run would merge a window's rows onto whatever row shares the remaining
    key, overwriting a different window instead of replacing itself.
    """
    model = make_model(grain="hour", time_column="event_ts", key=["sensor_id"])
    with pytest.raises(ModelValidationError) as excinfo:
        model.validate()
    message = str(excinfo.value)
    assert WINDOW_COLUMN in message
    assert "idempotent" in message
    assert excinfo.value.field == "key"


def test_the_window_column_name_does_not_vary_with_the_grain() -> None:
    for grain in GRAINS:
        make_model(
            grain=grain, time_column="event_ts", key=[WINDOW_COLUMN, "sensor_id"]
        ).validate()
    # a plausible-looking alternative is still refused
    with pytest.raises(ModelValidationError):
        make_model(
            grain="hour", time_column="event_ts", key=["hour_ts", "sensor_id"]
        ).validate()


def test_an_unwindowed_model_needs_no_window_column() -> None:
    make_model(key=["sensor_id"]).validate()


# ---------------------------------------------------------------------------
# Rule 8 -- memory profile
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("profile", MEMORY_PROFILES)
def test_supported_memory_profiles_are_accepted(profile: str) -> None:
    make_model(memory_profile=profile).validate()


@pytest.mark.parametrize("profile", ["stream", "materializing", "low", ""])
def test_unsupported_memory_profiles_are_rejected(profile: str) -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(memory_profile=profile).validate()
    assert excinfo.value.field == "memory_profile"


# ---------------------------------------------------------------------------
# Rule 9 -- udfs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    ["my_pkg.signal:arrow_fft", "mod:obj", "a.b.c.d:e", "pkg.mod:Cls.method"],
)
def test_well_shaped_udf_paths_are_accepted(path: str) -> None:
    make_model(udfs=[path]).validate()


@pytest.mark.parametrize(
    "path",
    ["my_pkg.signal", "my_pkg:", ":arrow_fft", "my pkg:fft", "a:b:c", "", 42],
)
def test_malformed_udf_paths_are_rejected(path: Any) -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(udfs=[path]).validate()
    assert excinfo.value.field == "udfs"


def test_udfs_are_not_imported_at_validation_time() -> None:
    """`duckstream validate` runs at deploy time and must not need the runtime
    environment to be importable."""
    make_model(udfs=["definitely_not_installed.module:thing"]).validate()


# ---------------------------------------------------------------------------
# udfs must cover the unknown function names an expression uses
# ---------------------------------------------------------------------------


def test_an_undeclared_udf_is_refused_at_load_time() -> None:
    """Otherwise the model validates and dies on its first trigger.

    An unknown function name is already enough to make the expression
    non_foldable, but nothing was registering it, so `duckstream validate`
    passed at deploy time and the Catalog Error arrived at 03:00 in a cron log
    -- exactly the sequence `validate` exists to prevent.
    """
    model = make_model(
        aggregates={"spectrum": "arrow_fft(list(value ORDER BY event_ts))"},
        time_column="event_ts",
        memory_profile="materialising",
        udfs=[],
    )
    with pytest.raises(ModelValidationError) as excinfo:
        model.validate()

    message = str(excinfo.value)
    assert excinfo.value.field == "udfs"
    assert "arrow_fft" in message, "the unknown function"
    assert "spectrum" in message, "the column that uses it"
    assert "Catalog Error" in message, "what would happen at runtime"
    assert "udfs" in excinfo.value.remedy


def test_declaring_any_udf_accepts_the_model() -> None:
    """The check is coarse on purpose.

    A dotted path and the SQL name a UDF registers itself under are genuinely
    independent, so matching them would reject correct models. The engine
    registers the udfs before planning and can re-check against the live
    catalog once they exist.
    """
    make_model(
        aggregates={"spectrum": "arrow_fft(list(value ORDER BY event_ts))"},
        time_column="event_ts",
        memory_profile="materialising",
        udfs=["my_pkg.signal:arrow_fft"],
    ).validate()

    # even a path that plainly does not name this function is accepted
    make_model(
        aggregates={"spectrum": "arrow_fft(list(value ORDER BY event_ts))"},
        time_column="event_ts",
        memory_profile="materialising",
        udfs=["my_pkg.something:entirely_else"],
    ).validate()


def test_a_misspelled_builtin_is_caught_by_the_same_rule() -> None:
    """The common case is a typo, not a UDF."""
    model = make_model(aggregates={"total": "sumn(value)"}, time_column="event_ts",
                       memory_profile="streaming")
    with pytest.raises(ModelValidationError) as excinfo:
        model.validate()
    assert "sumn" in str(excinfo.value)
    assert "typo" in excinfo.value.remedy


def test_every_unknown_function_is_named_not_just_the_first() -> None:
    model = make_model(
        aggregates={
            "a": "my_first(list(value))",
            "b": "my_second(list(value))",
        },
        time_column="event_ts",
        memory_profile="streaming",
    )
    with pytest.raises(ModelValidationError) as excinfo:
        model.validate()
    message = str(excinfo.value)
    assert "my_first" in message
    assert "my_second" in message


def test_a_model_using_only_known_functions_needs_no_udfs() -> None:
    make_model(aggregates={"n": "count(*)", "total": "sum(abs(value))"}).validate()


# ---------------------------------------------------------------------------
# Aggregate expressions cannot smuggle SQL past validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "expr",
    [
        "count(*) FROM other_table",
        "count(*) AS n",
        "sum((SELECT max(v) FROM other_table))",
        "sum(x) OVER ()",
    ],
)
def test_a_model_cannot_declare_an_expression_that_reaches_outside_the_batch(
    expr: str,
) -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(aggregates={"n": expr}).validate()
    assert excinfo.value.field == "aggregates"
    assert "hourly_counts" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Non-string expressions (the shape YAML produces)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("expr", [["count(*)"], {"expr": "count(*)"}, 42, None])
def test_a_non_string_aggregate_expression_is_a_validation_error(
    expr: object,
) -> None:
    """W2d's loader hits this first: `n: [count(*)]` in YAML is a list.

    It used to escape as `TypeError: unhashable type: 'list'` from inside the
    classifier's cache, which is neither catchable as a DuckstreamError nor
    informative about which column is wrong.
    """
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(aggregates={"n": expr}).validate()  # type: ignore[dict-item]
    assert isinstance(excinfo.value, DuckstreamError)
    assert "'n'" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Limits
# ---------------------------------------------------------------------------


def test_limits_must_be_positive() -> None:
    for bad in (BatchLimits(max_rows_per_trigger=0), BatchLimits(max_files_per_trigger=-1)):
        with pytest.raises(ModelValidationError) as excinfo:
            make_model(limits=bad).validate()
        assert excinfo.value.field == "limits"


def test_unbounded_limits_are_the_default() -> None:
    model = make_model()
    assert model.limits == BatchLimits(None, None)
    model.validate()


# ---------------------------------------------------------------------------
# Source and sink shape
# ---------------------------------------------------------------------------


def test_an_object_that_is_not_a_source_is_rejected() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(source=object()).validate()
    assert excinfo.value.field == "source"
    assert "latest_offset" in str(excinfo.value)


def test_an_object_that_is_not_a_sink_is_rejected() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        make_model(sink=object()).validate()
    assert excinfo.value.field == "sink"


def test_the_fakes_satisfy_the_runtime_checkable_protocols() -> None:
    assert isinstance(FakeSource(), Source)
    assert isinstance(FakeSink(), Sink)


# ---------------------------------------------------------------------------
# Tier and strategy properties
# ---------------------------------------------------------------------------


def test_tier_and_resolved_strategy_are_inferred_when_not_declared() -> None:
    assert make_model().tier is Tier.ADDITIVE
    assert make_model().resolved_strategy == "delta_merge"

    stats = make_model(aggregates={"mean_v": "avg(value)"})
    assert stats.tier is Tier.SUFFICIENT_STATISTICS
    assert stats.resolved_strategy == "sufficient_statistics"

    heavy = make_model(
        aggregates={"p50": "median(value)"},
        time_column="event_ts",
        memory_profile="streaming",
    )
    assert heavy.tier is Tier.NON_FOLDABLE
    assert heavy.resolved_strategy == "recompute_window"


def test_a_declared_strategy_overrides_the_inferred_one() -> None:
    model = make_model(strategy="recompute_window", time_column="event_ts")
    assert model.tier is Tier.ADDITIVE
    assert model.resolved_strategy == "recompute_window"


def test_column_tiers_expose_the_per_column_detail() -> None:
    model = make_model(aggregates={"n": "count(*)", "mean_v": "avg(value)"})
    assert model.column_tiers == {
        "n": Tier.ADDITIVE,
        "mean_v": Tier.SUFFICIENT_STATISTICS,
    }
    assert model.tier is Tier.SUFFICIENT_STATISTICS


# ---------------------------------------------------------------------------
# validate() is idempotent
# ---------------------------------------------------------------------------


def test_validate_is_idempotent_on_a_good_model() -> None:
    model = make_model()
    for _ in range(3):
        model.validate()


def test_validate_is_idempotent_on_a_bad_model() -> None:
    """The loader, the CLI and the engine all validate; each must see the same
    failure, not a different one on the second pass."""
    model = make_model(aggregates={"p50": "median(value)"}, strategy="delta_merge")
    messages = []
    for _ in range(3):
        with pytest.raises(ModelValidationError) as excinfo:
            model.validate()
        messages.append(str(excinfo.value))
    assert len(set(messages)) == 1


def test_validation_does_not_mutate_the_model() -> None:
    model = make_model(key=["sensor_id"], udfs=["a.b:c"])
    before = (dict(model.aggregates), list(model.key), list(model.udfs), model.strategy)
    model.validate()
    assert (
        dict(model.aggregates),
        list(model.key),
        list(model.udfs),
        model.strategy,
    ) == before


# ---------------------------------------------------------------------------
# to_config()
# ---------------------------------------------------------------------------


def test_to_config_delegates_source_and_sink() -> None:
    config = make_model().to_config()
    assert config["source"] == {"type": "fake", "path": "landing/"}
    assert config["sink"] == {"type": "fake", "table": "marts.t", "mode": "update"}


def test_to_config_expresses_every_field_of_a_fully_populated_model() -> None:
    """The drift guard.

    W2d builds the loader against this dict. A field addable in Python but not
    expressible here would break the round-trip test later, so catch it now:
    every dataclass field must appear when it is set to a non-default value.
    """
    model = Model(
        name="marts.minute_spectrum",
        source=FakeSource("landing/"),
        sink=FakeSink("marts.minute_spectrum", "update"),
        aggregates={"spectrum": "arrow_fft(list(value ORDER BY event_ts))"},
        key=[WINDOW_COLUMN, "sensor_id"],
        time_column="event_ts",
        grain="minute",
        strategy="recompute_window",
        memory_profile="materialising",
        udfs=["my_pkg.signal:arrow_fft"],
        limits=BatchLimits(max_rows_per_trigger=100_000, max_files_per_trigger=10),
    )
    model.validate()

    config = model.to_config()
    declared = {f.name for f in fields(Model)}
    assert declared <= set(config), f"not expressible in config: {declared - set(config)}"

    assert config["limits"] == {
        "max_rows_per_trigger": 100_000,
        "max_files_per_trigger": 10,
    }
    assert config["udfs"] == ["my_pkg.signal:arrow_fft"]


def test_to_config_omits_fields_left_at_their_default() -> None:
    config = make_model().to_config()
    for absent in ("time_column", "grain", "strategy", "memory_profile", "udfs", "limits"):
        assert absent not in config
    assert set(config) == {"name", "source", "sink", "aggregates", "key"}


def test_to_config_is_yaml_safe() -> None:
    import yaml

    model = make_model(
        grain="hour",
        time_column="event_ts",
        key=[WINDOW_COLUMN, "sensor_id"],
        limits=BatchLimits(max_rows_per_trigger=1000),
    )
    text = yaml.safe_dump(model.to_config(), sort_keys=False)
    assert yaml.safe_load(text) == model.to_config()


def test_to_config_returns_copies_not_live_references() -> None:
    model = make_model(udfs=["a.b:c"])
    config = model.to_config()
    config["aggregates"]["injected"] = "count(*)"
    config["key"].append("oops")
    config["udfs"].append("x.y:z")
    assert "injected" not in model.aggregates
    assert model.key == ["sensor_id"]
    assert model.udfs == ["a.b:c"]


# ---------------------------------------------------------------------------
# Error hierarchy and import hygiene
# ---------------------------------------------------------------------------


def test_model_validation_error_is_a_duckstream_error() -> None:
    assert issubclass(ModelValidationError, DuckstreamError)


def test_model_module_does_not_require_duckdb_at_import_time() -> None:
    """The declarative surface stays cheap to import; only classification needs
    DuckDB's parser, and it connects lazily."""
    import subprocess
    import sys
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; "
            "sys.modules['duckdb'] = None; "  # any `import duckdb` now fails
            "import duckstream.model; "
            "print('ok')",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout
