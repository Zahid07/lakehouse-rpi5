"""The YAML front door, and the mechanism that stops it drifting from Python.

``PLAN.md`` names two failure modes for a config layer and one mechanical guard
against each:

* **Drift** -- "a field addable in Python but not expressible in config fails
  this test". The round-trip property test below is that guard, and it is driven
  off the ``Model`` dataclass *programmatically*: :func:`unexercised_fields`
  fails if a field exists that no corpus model exercises, and
  :func:`fields_missing_from_config` fails if an exercised field does not
  survive ``to_config()``. Together they mean a new ``Model`` field cannot be
  added silently -- one of the two breaks. Both helpers are themselves tested
  against a ``Model`` subclass carrying an extra field, so the guard is proven
  to fail rather than merely asserted to work.

* **A parallel validation path.** The loader must not have opinions of its own.
  So the tests here split cleanly: *shape* problems (unknown key, wrong type,
  unset variable) are the loader's and raise ``ConfigError``; *meaning* problems
  (a bad strategy, a merge key missing ``window_ts``) are ``Model.validate``'s
  and must reach the caller as the identical ``ModelValidationError`` the Python
  front door raises.

The other theme is error quality. ``duckstream validate`` runs at deploy time
and its message is all an operator gets, so the assertions below are on message
*content* -- the file, the line, the model, the field, the offending key -- not
merely on exception type.
"""

from __future__ import annotations

import sys
import textwrap
from dataclasses import MISSING, dataclass, fields
from typing import Any, ClassVar

import pytest
import yaml

from duckstream import registry as reg
from duckstream.config import (
    DOCUMENT_KEYS,
    LIMIT_KEYS,
    MODEL_KEYS,
    ConfigDocument,
    load_config,
    parse_config,
    parse_yaml,
    substitute_env,
)
from duckstream.errors import ConfigError, DuckstreamError, ModelValidationError
from duckstream.model import Model
from duckstream.protocols import BatchLimits
from duckstream.sources.files import FileSource

try:  # W2c's sink; the tests that need the real one skip if it has not landed.
    from duckstream.sinks.table import TableSink
except Exception:  # pragma: no cover - only while W2c is in flight
    TableSink = None

requires_table_sink = pytest.mark.skipif(
    TableSink is None, reason="duckstream.sinks.table (W2c) is not available"
)

CATALOG = "ducklake:catalog.ducklake"


# ---------------------------------------------------------------------------
# A user-registered sink
#
# The corpus deliberately uses this rather than TableSink, for two reasons: the
# headline round-trip test then depends on nothing outside W1/W2a, and a sink
# registered from Python is exactly the extension path the registry exists for,
# so round-tripping one proves the mechanism rather than a built-in special case.
# ---------------------------------------------------------------------------


class FakeSink:
    type_name: ClassVar[str] = "fake_sink"

    def __init__(self, table: str, *, mode: str = "update") -> None:
        self.table = table
        self.mode = mode

    def ensure(self, con, model) -> None:  # pragma: no cover - never executed
        raise AssertionError("the config loader must not execute anything")

    def write(self, con, batch_view, model, ctx) -> None:  # pragma: no cover
        raise AssertionError("the config loader must not execute anything")

    def to_config(self) -> dict[str, Any]:
        return {"type": self.type_name, "table": self.table, "mode": self.mode}

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        return self.to_config() == other.to_config()

    def __hash__(self) -> int:
        return hash((self.table, self.mode))

    def __repr__(self) -> str:
        return f"FakeSink({self.table!r}, mode={self.mode!r})"


@pytest.fixture(autouse=True)
def registered_fake_sink():
    saved = reg.SINKS.snapshot()
    reg.register_sink("fake_sink", FakeSink)
    yield
    reg.SINKS.restore(saved)


# ---------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------


def build(name: str, **overrides: Any) -> Model:
    declaration: dict[str, Any] = {
        "name": name,
        "source": FileSource("landing/"),
        "sink": FakeSink(f"marts.{name}"),
        "aggregates": {"n": "count(*)"},
        "key": ["sensor_id"],
    }
    declaration.update(overrides)
    model = Model(**declaration)
    model.validate()
    return model


def corpus() -> list[Model]:
    """Models covering every ``Model`` field, checked by the coverage test."""
    return [
        # Required fields only: no grain, no time column, no limits.
        build("plain_counts"),
        # time_column and grain, plus a source with every option off default.
        build(
            "hourly_counts",
            source=FileSource(
                "landing/hourly",
                marker=None,
                settle_seconds=2.5,
                format="csv",
                pattern="*.csv",
                recursive=False,
                max_files_per_trigger=10,
                max_rows_per_trigger=50_000,
            ),
            sink=FakeSink("marts.hourly_counts", mode="append"),
            aggregates={"n": "count(*)", "total": "sum(value)"},
            key=["window_ts", "sensor_id"],
            time_column="event_ts",
            grain="hour",
            # Append over windows requires a horizon, so this entry covers
            # `lateness` as well -- see Model._check_output_mode.
            lateness="10 minutes",
        ),
        # Tier two, with strategy and memory_profile declared explicitly.
        build(
            "sensor_stats",
            aggregates={"mean_v": "avg(value)", "sd_v": "stddev_samp(value)"},
            key=["window_ts", "sensor_id"],
            time_column="event_ts",
            grain="day",
            strategy="sufficient_statistics",
            memory_profile="streaming",
        ),
        # Tier three: a whole-window UDF, so udfs must round-trip too.
        build(
            "minute_spectrum",
            aggregates={"spectrum": "arrow_fft(list(value ORDER BY event_ts))"},
            key=["window_ts", "sensor_id"],
            time_column="event_ts",
            grain="minute",
            strategy="recompute_window",
            memory_profile="materialising",
            udfs=["my_pkg.signal:arrow_fft"],
        ),
        # Model-level batch limits.
        build(
            "bounded_counts",
            limits=BatchLimits(max_rows_per_trigger=50_000, max_files_per_trigger=4),
        ),
        # The failure policy: 'halt' rather than the default, so both it and
        # max_attempts are exercised off their defaults and therefore covered
        # by the round trip.
        build("strict_counts", on_failure="halt", max_attempts=2),
    ]


CORPUS = corpus()
CORPUS_IDS = [m.name for m in CORPUS]


# ---------------------------------------------------------------------------
# Field-driven helpers -- the drift guard itself
# ---------------------------------------------------------------------------

_NO_DEFAULT = object()


def default_of(spec) -> Any:
    if spec.default is not MISSING:
        return spec.default
    if spec.default_factory is not MISSING:  # type: ignore[misc]
        return spec.default_factory()  # type: ignore[misc]
    return _NO_DEFAULT


def is_exercised(model: Model, spec) -> bool:
    """True when this model says something about ``spec`` beyond the default."""
    default = default_of(spec)
    if default is _NO_DEFAULT:
        return True  # required: every model necessarily carries a value
    return getattr(model, spec.name) != default


def unexercised_fields(model_cls: type, models: list[Model]) -> set[str]:
    """Fields of ``model_cls`` that no model in ``models`` sets off its default."""
    return {
        spec.name
        for spec in fields(model_cls)
        if not any(is_exercised(model, spec) for model in models)
    }


def fields_missing_from_config(model: Model) -> set[str]:
    """Fields this model sets that ``to_config()`` does not emit."""
    emitted = set(model.to_config())
    return {
        spec.name
        for spec in fields(type(model))
        if is_exercised(model, spec) and spec.name not in emitted
    }


def document_yaml(models: list[Model], **document: Any) -> str:
    body: dict[str, Any] = {"catalog": CATALOG}
    body.update(document)
    body["models"] = [m.to_config() for m in models]
    return yaml.safe_dump(body, sort_keys=False)


# ---------------------------------------------------------------------------
# The headline: round-trip
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("model", CORPUS, ids=CORPUS_IDS)
def test_model_survives_a_round_trip_through_yaml(model: Model):
    """``Model`` -> ``to_config()`` -> YAML -> load -> an *identical* ``Model``."""
    document = parse_yaml(document_yaml([model]), source="round_trip.yaml", env={})

    assert len(document.models) == 1
    reloaded = document.models[0]
    assert reloaded == model
    assert reloaded.to_config() == model.to_config()
    # Not just equal-by-value: the same resolved behaviour.
    assert reloaded.tier is model.tier
    assert reloaded.resolved_strategy == model.resolved_strategy


def test_the_whole_document_survives_a_round_trip():
    original = ConfigDocument(
        catalog=CATALOG,
        data_path="lake_data",
        settings={"memory_limit": "2GB", "threads": 2},
        models=CORPUS,
    )
    reloaded = parse_yaml(
        yaml.safe_dump(original.to_config(), sort_keys=False),
        source="round_trip.yaml",
        env={},
    )
    assert reloaded == original
    assert reloaded.to_config() == original.to_config()


def test_every_model_field_is_exercised_by_the_corpus():
    """The programmatic half of the drift guard.

    A field added to ``Model`` that no corpus model sets fails here, which is
    what forces the round-trip test to actually cover it rather than quietly
    testing a subset.
    """
    missing = unexercised_fields(Model, CORPUS)
    assert not missing, (
        f"Model fields {sorted(missing)} are never set away from their default "
        f"by any model in the corpus, so the round-trip test does not cover "
        f"them. Add a model to corpus() that exercises them."
    )


@pytest.mark.parametrize("model", CORPUS, ids=CORPUS_IDS)
def test_every_field_a_model_sets_is_expressible_in_config(model: Model):
    """The other half: a field Python can set but ``to_config()`` cannot emit."""
    missing = fields_missing_from_config(model)
    assert not missing, (
        f"model {model.name!r} sets {sorted(missing)} but Model.to_config() does "
        f"not emit them, so the Python front door can express something the "
        f"config front door cannot."
    )


def test_the_loader_accepts_exactly_the_model_dataclass_fields():
    assert set(MODEL_KEYS) == {spec.name for spec in fields(Model)}
    assert set(LIMIT_KEYS) == {spec.name for spec in fields(BatchLimits)}


# -- proof that the drift guard can fail ------------------------------------


@dataclass(kw_only=True)
class ModelWithNewField(Model):
    """A ``Model`` as a future commit might leave it: one more knob, no config."""

    lateness_horizon: str | None = None


def test_the_coverage_guard_catches_a_field_nothing_exercises():
    unexercised = build("probe")
    promoted = ModelWithNewField(**{f.name: getattr(unexercised, f.name) for f in fields(Model)})
    assert "lateness_horizon" in unexercised_fields(ModelWithNewField, [promoted])


def test_the_expressibility_guard_catches_a_field_config_cannot_carry():
    exercised = ModelWithNewField(
        **{f.name: getattr(build("probe"), f.name) for f in fields(Model)},
        lateness_horizon="10 minutes",
    )
    # Model.to_config() knows nothing about the new field, so it is lost.
    assert "lateness_horizon" not in exercised.to_config()
    assert fields_missing_from_config(exercised) == {"lateness_horizon"}


# ---------------------------------------------------------------------------
# ${VAR} substitution
# ---------------------------------------------------------------------------


def test_a_set_variable_is_substituted():
    assert substitute_env("${LANDING}", env={"LANDING": "/srv/landing"}) == "/srv/landing"


def test_an_unset_variable_falls_back_to_its_default():
    assert substitute_env("${LANDING:-landing/}", env={}) == "landing/"


def test_an_empty_variable_falls_back_to_its_default():
    """POSIX ``:-`` semantics: an empty value is as good as unset."""
    assert substitute_env("${LANDING:-landing/}", env={"LANDING": ""}) == "landing/"


def test_a_set_variable_wins_over_its_default():
    assert substitute_env("${L:-landing/}", env={"L": "/srv"}) == "/srv"


def test_an_unset_variable_without_a_default_is_an_error():
    with pytest.raises(ConfigError) as caught:
        substitute_env("${DUCKSTREAM_LANDING}", env={})
    message = str(caught.value)
    assert "DUCKSTREAM_LANDING" in message
    assert "not set" in message
    # It must say why it will not silently produce "".
    assert "empty string" in message


def test_an_empty_variable_without_a_default_is_allowed():
    assert substitute_env("${L}", env={"L": ""}) == ""


def test_a_variable_inside_a_longer_string_is_substituted():
    resolved = substitute_env(
        "s3://${BUCKET}/landing/${STAGE:-dev}/in", env={"BUCKET": "sensors"}
    )
    assert resolved == "s3://sensors/landing/dev/in"


def test_a_literal_dollar_is_left_alone():
    for text in ("$", "cost is $5", "$HOME", "regexp_matches(x, '^\\$')", "a$$b"):
        assert substitute_env(text, env={}) == text


def test_a_malformed_reference_is_refused():
    with pytest.raises(ConfigError, match="malformed variable reference"):
        substitute_env("${NOT CLOSED", env={})
    with pytest.raises(ConfigError, match="malformed variable reference"):
        substitute_env("${1BAD}", env={})


def test_substitution_reaches_nested_values_but_never_keys():
    resolved = substitute_env(
        {"a": ["${X}", {"b": "${X}"}], "${X}": "${X}"}, env={"X": "v"}
    )
    assert resolved == {"a": ["v", {"b": "v"}], "${X}": "v"}


def test_a_variable_in_a_key_is_not_substituted_and_is_then_rejected():
    text = textwrap.dedent(
        f"""
        catalog: {CATALOG}
        ${{KEYNAME}}: nope
        models:
          - name: m
            source: {{type: file, path: landing/}}
            sink: {{type: fake_sink, table: marts.m}}
            aggregates: {{n: "count(*)"}}
            key: [sensor_id]
        """
    )
    with pytest.raises(ConfigError) as caught:
        parse_yaml(text, source="models.yaml", env={"KEYNAME": "catalog"})
    assert "unknown key '${KEYNAME}'" in str(caught.value)


def test_substitution_happens_end_to_end_in_the_document(monkeypatch):
    monkeypatch.setenv("DUCKSTREAM_LANDING", "/srv/landing")
    text = textwrap.dedent(
        """
        catalog: "ducklake:${CAT:-catalog}.ducklake"
        models:
          - name: m
            source: {type: file, path: "${DUCKSTREAM_LANDING}"}
            sink: {type: fake_sink, table: marts.m}
            aggregates: {n: "count(*)"}
            key: [sensor_id]
        """
    )
    document = parse_yaml(text, source="models.yaml")
    assert document.catalog == "ducklake:catalog.ducklake"
    assert document.models[0].source.path == "/srv/landing"


def test_an_unset_variable_names_the_file_and_the_line():
    text = textwrap.dedent(
        f"""
        catalog: {CATALOG}
        models:
          - name: m
            source:
              type: file
              path: "${{DUCKSTREAM_LANDING}}"
            sink: {{type: fake_sink, table: marts.m}}
            aggregates: {{n: "count(*)"}}
            key: [sensor_id]
        """
    )
    with pytest.raises(ConfigError) as caught:
        parse_yaml(text, source="models.yaml", env={})
    assert "models.yaml:7" in str(caught.value)


# ---------------------------------------------------------------------------
# Unknown keys, at every level
# ---------------------------------------------------------------------------


def minimal(**model: Any) -> str:
    block: dict[str, Any] = {
        "name": "m",
        "source": {"type": "file", "path": "landing/"},
        "sink": {"type": "fake_sink", "table": "marts.m"},
        "aggregates": {"n": "count(*)"},
        "key": ["sensor_id"],
    }
    block.update(model)
    return yaml.safe_dump({"catalog": CATALOG, "models": [block]}, sort_keys=False)


def test_an_unknown_document_key_is_rejected():
    text = yaml.safe_dump(
        {
            "catalog": CATALOG,
            "datapath": "lake_data",
            "models": [yaml.safe_load(minimal())["models"][0]],
        },
        sort_keys=False,
    )
    with pytest.raises(ConfigError) as caught:
        parse_yaml(text, source="models.yaml", env={})
    message = str(caught.value)
    assert "unknown key 'datapath'" in message
    assert "Did you mean 'data_path'?" in message
    for key in DOCUMENT_KEYS:
        assert repr(key) in message


def test_an_unknown_model_key_is_rejected_with_the_model_named():
    with pytest.raises(ConfigError) as caught:
        parse_yaml(minimal(time_colum="event_ts"), source="models.yaml", env={})
    message = str(caught.value)
    assert "unknown key 'time_colum'" in message
    assert "Did you mean 'time_column'?" in message
    assert "model 'm'" in message
    assert "models.yaml:" in message


def test_an_unknown_source_key_is_rejected():
    with pytest.raises(ConfigError) as caught:
        parse_yaml(
            minimal(source={"type": "file", "path": "l/", "max_files_per_triger": 3}),
            source="models.yaml",
            env={},
        )
    message = str(caught.value)
    assert "unknown key 'max_files_per_triger'" in message
    assert "Did you mean 'max_files_per_trigger'?" in message
    assert "'file' source block" in message
    assert "model 'm'" in message


def test_an_unknown_sink_key_is_rejected():
    with pytest.raises(ConfigError) as caught:
        parse_yaml(
            minimal(sink={"type": "fake_sink", "table": "marts.m", "moed": "update"}),
            source="models.yaml",
            env={},
        )
    message = str(caught.value)
    assert "unknown key 'moed'" in message
    assert "Did you mean 'mode'?" in message
    assert "'fake_sink' sink block" in message


def test_an_unknown_limits_key_is_rejected():
    with pytest.raises(ConfigError) as caught:
        parse_yaml(
            minimal(limits={"max_rows_per_triger": 10}), source="models.yaml", env={}
        )
    message = str(caught.value)
    assert "unknown key 'max_rows_per_triger'" in message
    assert "Did you mean 'max_rows_per_trigger'?" in message


def test_a_typo_is_never_silently_ignored():
    """The whole point: the near-miss must fail, not be dropped on the floor."""
    with pytest.raises(ConfigError):
        parse_yaml(minimal(grian="hour"), source="models.yaml", env={})


def test_a_duplicate_key_is_rejected_rather_than_last_one_wins():
    text = textwrap.dedent(
        f"""
        catalog: {CATALOG}
        models:
          - name: m
            source:
              type: file
              path: landing/a
              path: landing/b
            sink: {{type: fake_sink, table: marts.m}}
            aggregates: {{n: "count(*)"}}
            key: [sensor_id]
        """
    )
    with pytest.raises(ConfigError) as caught:
        parse_yaml(text, source="models.yaml", env={})
    message = str(caught.value)
    assert "'path' appears twice" in message
    assert "models.yaml:8" in message  # the duplicate
    assert "first at line 7" in message


# ---------------------------------------------------------------------------
# Validation reaches the config path unweakened
# ---------------------------------------------------------------------------


def test_a_bad_strategy_is_refused_with_the_aggregate_and_tier_named():
    text = minimal(
        aggregates={"p50": "median(value)"},
        strategy="delta_merge",
    )
    with pytest.raises(ModelValidationError) as caught:
        parse_yaml(text, source="models.yaml", env={})
    message = str(caught.value)
    assert "delta_merge" in message
    assert "non_foldable" in message
    assert "median" in message
    assert "model 'm'" in message
    assert "field 'strategy'" in message
    assert "models.yaml:" in message
    assert "recompute_window" in message  # the remedy survives


def test_the_config_path_gives_the_same_rejection_as_the_python_path():
    """Same declaration, both front doors, same error type and same reason."""
    source = FileSource("landing/")
    sink = FakeSink("marts.m")
    declaration = dict(
        name="m",
        source=source,
        sink=sink,
        aggregates={"p50": "median(value)"},
        key=["sensor_id"],
        strategy="delta_merge",
    )

    with pytest.raises(ModelValidationError) as via_python:
        Model(**declaration).validate()
    with pytest.raises(ModelValidationError) as via_config:
        parse_config(
            {"catalog": CATALOG, "models": [Model(**declaration).to_config()]},
            env={},
        )

    assert via_config.value.field == via_python.value.field
    assert via_config.value.model == via_python.value.model
    assert via_config.value.remedy == via_python.value.remedy
    # The config path adds a location and changes nothing else.
    assert via_python.value.reason in via_config.value.reason


def test_the_window_column_invariant_is_enforced_through_config():
    """``PLAN.md``'s worked example says ``hour_ts``; that is stale and refused."""
    text = minimal(
        time_column="event_ts", grain="hour", key=["hour_ts", "sensor_id"]
    )
    with pytest.raises(ModelValidationError) as caught:
        parse_yaml(text, source="models.yaml", env={})
    message = str(caught.value)
    assert "window_ts" in message
    assert "hour_ts" in message


def test_a_non_foldable_model_must_declare_a_memory_profile():
    text = minimal(
        aggregates={"spectrum": "arrow_fft(list(value ORDER BY event_ts))"},
        udfs=["my_pkg.signal:arrow_fft"],
        time_column="event_ts",
    )
    with pytest.raises(ModelValidationError, match="memory_profile"):
        parse_yaml(text, source="models.yaml", env={})


def test_several_bad_models_are_all_reported():
    blocks = []
    for name, override in (
        ("bad_strategy", {"aggregates": {"p50": "median(v)"}, "strategy": "delta_merge"}),
        ("bad_grain", {"grain": "fortnight", "time_column": "event_ts", "key": ["window_ts"]}),
        ("bad_key", {"key": []}),
    ):
        block = yaml.safe_load(minimal(**override))["models"][0]
        block["name"] = name
        blocks.append(block)
    text = yaml.safe_dump({"catalog": CATALOG, "models": blocks}, sort_keys=False)

    with pytest.raises(ConfigError) as caught:
        parse_yaml(text, source="models.yaml", env={})

    message = str(caught.value)
    assert "3 problems in models.yaml" in message
    for name in ("bad_strategy", "bad_grain", "bad_key"):
        assert name in message
    assert len(caught.value.errors) == 3


def test_a_single_problem_is_not_wrapped_in_an_aggregate():
    with pytest.raises(ModelValidationError):
        parse_yaml(minimal(grain="fortnight", time_column="t"), source="m.yaml", env={})


def test_two_models_may_not_share_a_name():
    first = yaml.safe_load(minimal())["models"][0]
    text = yaml.safe_dump({"catalog": CATALOG, "models": [first, dict(first)]}, sort_keys=False)
    with pytest.raises(ConfigError) as caught:
        parse_yaml(text, source="models.yaml", env={})
    message = str(caught.value)
    assert "two models are called 'm'" in message
    assert "checkpoints" in message


# ---------------------------------------------------------------------------
# Registry names through the config path
# ---------------------------------------------------------------------------


def test_an_unknown_source_type_lists_the_available_names():
    with pytest.raises(ConfigError) as caught:
        parse_yaml(
            minimal(source={"type": "kafka", "path": "l/"}),
            source="models.yaml",
            env={},
        )
    message = str(caught.value)
    assert "unknown source type 'kafka'" in message
    assert "'file'" in message
    assert "models.yaml:" in message


def test_a_source_with_no_type_says_what_types_exist():
    with pytest.raises(ConfigError) as caught:
        parse_yaml(minimal(source={"path": "l/"}), source="models.yaml", env={})
    message = str(caught.value)
    assert "no 'type'" in message
    assert "'file'" in message


def test_a_source_that_is_not_a_mapping_is_refused():
    with pytest.raises(ConfigError, match="must be a mapping with a 'type' key"):
        parse_yaml(minimal(source="file"), source="models.yaml", env={})


def test_a_user_source_is_reached_by_dotted_path(tmp_path, monkeypatch):
    (tmp_path / "duckstream_cfg_plugin.py").write_text(
        textwrap.dedent(
            '''
            class MySource:
                type_name = "duckstream_cfg_plugin:MySource"

                def __init__(self, path):
                    self.path = path

                def latest_offset(self):
                    return {}

                def plan(self, start, end, limits):
                    return {}

                def bind(self, con, plan):
                    return "v"

                def to_config(self):
                    return {"type": self.type_name, "path": self.path}
            '''
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    try:
        document = parse_yaml(
            minimal(source={"type": "duckstream_cfg_plugin:MySource", "path": "l/"}),
            source="models.yaml",
            env={},
        )
        assert type(document.models[0].source).__name__ == "MySource"
    finally:
        sys.modules.pop("duckstream_cfg_plugin", None)


def test_udfs_are_recorded_but_never_imported():
    """The engine registers them before planning; the loader must not touch them.

    Proven with a path that could not possibly import: if the loader resolved
    ``udfs`` it would fail here, and ``duckstream validate`` would then need the
    whole runtime environment installed to run at all.
    """
    text = minimal(
        aggregates={"spectrum": "arrow_fft(list(v ORDER BY t))"},
        udfs=["definitely_not_installed_pkg.signal:arrow_fft"],
        time_column="event_ts",
        memory_profile="materialising",
    )
    document = parse_yaml(text, source="models.yaml", env={})
    assert document.models[0].udfs == ["definitely_not_installed_pkg.signal:arrow_fft"]
    assert "definitely_not_installed_pkg" not in sys.modules


# ---------------------------------------------------------------------------
# Document-level shape
# ---------------------------------------------------------------------------


def test_an_empty_document_is_refused():
    with pytest.raises(ConfigError, match="empty"):
        parse_yaml("", source="models.yaml", env={})


def test_a_document_that_is_not_a_mapping_is_refused():
    with pytest.raises(ConfigError, match="must be a mapping"):
        parse_yaml("- a\n- b\n", source="models.yaml", env={})


def test_a_missing_catalog_is_refused():
    text = yaml.safe_dump({"models": [yaml.safe_load(minimal())["models"][0]]})
    with pytest.raises(ConfigError, match="no 'catalog' declared"):
        parse_yaml(text, source="models.yaml", env={})


def test_missing_or_empty_models_are_refused():
    with pytest.raises(ConfigError, match="no 'models' declared"):
        parse_yaml(f"catalog: {CATALOG}\n", source="models.yaml", env={})
    with pytest.raises(ConfigError, match="'models' is empty"):
        parse_yaml(f"catalog: {CATALOG}\nmodels: []\n", source="models.yaml", env={})


def test_settings_must_hold_scalars():
    text = yaml.safe_dump(
        {
            "catalog": CATALOG,
            "settings": {"memory_limit": {"nested": True}},
            "models": [yaml.safe_load(minimal())["models"][0]],
        },
        sort_keys=False,
    )
    with pytest.raises(ConfigError) as caught:
        parse_yaml(text, source="models.yaml", env={})
    assert "memory_limit" in str(caught.value)
    assert "interpolated into a SQL `SET`" in str(caught.value)


def test_settings_are_carried_through_untouched():
    text = yaml.safe_dump(
        {
            "catalog": CATALOG,
            "settings": {
                "ducklake_default_data_inlining_row_limit": 0,
                "memory_limit": "2GB",
                "threads": 2,
            },
            "models": [yaml.safe_load(minimal())["models"][0]],
        },
        sort_keys=False,
    )
    document = parse_yaml(text, source="models.yaml", env={})
    assert document.settings == {
        "ducklake_default_data_inlining_row_limit": 0,
        "memory_limit": "2GB",
        "threads": 2,
    }


@pytest.mark.parametrize(
    "override, fragment",
    [
        ({"key": "sensor_id"}, "'key' must be a list"),
        ({"udfs": "my_pkg:fn"}, "'udfs' must be a list"),
        ({"aggregates": "count(*)"}, "'aggregates' must be a mapping"),
        ({"grain": 3}, "'grain' must be a string"),
        ({"limits": 10}, "'limits' must be a mapping"),
    ],
)
def test_wrong_shapes_are_named_precisely(override, fragment):
    with pytest.raises(ConfigError, match=fragment):
        parse_yaml(minimal(**override), source="models.yaml", env={})


def test_a_missing_required_model_key_is_named():
    block = yaml.safe_load(minimal())["models"][0]
    block.pop("sink")
    text = yaml.safe_dump({"catalog": CATALOG, "models": [block]}, sort_keys=False)
    with pytest.raises(ConfigError, match="no 'sink'"):
        parse_yaml(text, source="models.yaml", env={})


def test_invalid_yaml_reports_the_file_and_line():
    with pytest.raises(ConfigError) as caught:
        parse_yaml("catalog: [1,\nmodels: 2\n", source="models.yaml", env={})
    message = str(caught.value)
    assert "could not parse YAML" in message
    assert "models.yaml:" in message


# ---------------------------------------------------------------------------
# The three entry points
# ---------------------------------------------------------------------------


def test_load_config_reads_a_file_and_names_it_in_errors(tmp_path):
    path = tmp_path / "models.yaml"
    path.write_text(minimal(grian="hour"), encoding="utf-8")
    with pytest.raises(ConfigError) as caught:
        load_config(path, env={})
    assert str(path) in str(caught.value)


def test_load_config_reports_a_missing_file():
    with pytest.raises(ConfigError, match="could not read the configuration file"):
        load_config("no_such_directory/models.yaml", env={})


def test_load_config_returns_a_usable_document(tmp_path):
    path = tmp_path / "models.yaml"
    path.write_text(document_yaml(CORPUS, data_path="lake_data"), encoding="utf-8")
    document = load_config(path, env={})
    assert document.path == str(path)
    assert document.names == CORPUS_IDS
    assert document.models == CORPUS
    assert document.model("sensor_stats").grain == "day"
    with pytest.raises(ConfigError, match="no model named 'nope'"):
        document.model("nope")


def test_parse_config_takes_an_already_loaded_mapping():
    document = parse_config(
        {"catalog": CATALOG, "models": [m.to_config() for m in CORPUS]}, env={}
    )
    assert document.models == CORPUS


def test_the_document_is_frozen():
    document = parse_config({"catalog": CATALOG, "models": [CORPUS[0].to_config()]}, env={})
    with pytest.raises(Exception):
        document.catalog = "somewhere else"  # type: ignore[misc]


def test_every_error_is_a_duckstream_error():
    """One ``except`` clause has to be enough for a host application."""
    for text in ("", "- a", f"catalog: {CATALOG}\n"):
        with pytest.raises(DuckstreamError):
            parse_yaml(text, source="models.yaml", env={})


# ---------------------------------------------------------------------------
# The worked example from PLAN.md
# ---------------------------------------------------------------------------

PLAN_EXAMPLE = textwrap.dedent(
    """
    catalog: "ducklake:catalog.ducklake"
    data_path: "lake_data"

    settings:
      ducklake_default_data_inlining_row_limit: 0
      memory_limit: "2GB"
      threads: 2

    models:
      - name: hourly_counts
        source:
          type: file
          path: "${DUCKSTREAM_LANDING:-landing/}"
          marker: _READY
          max_files_per_trigger: 10
        time_column: event_ts
        grain: hour
        key: [WINDOW, sensor_id]
        aggregates:
          n: "count(*)"
          total: "sum(value)"
        sink:
          type: SINK
          table: marts.hourly_counts
          mode: update
    """
)


def plan_example(*, window: str, sink: str) -> str:
    return PLAN_EXAMPLE.replace("WINDOW", window).replace("SINK", sink)


@pytest.mark.parametrize(
    "sink",
    [
        "fake_sink",
        pytest.param("table", marks=requires_table_sink),
    ],
)
def test_the_plan_example_loads_once_corrected_to_window_ts(sink):
    document = parse_yaml(
        plan_example(window="window_ts", sink=sink), source="models.yaml", env={}
    )
    assert document.catalog == "ducklake:catalog.ducklake"
    assert document.data_path == "lake_data"
    assert document.settings["threads"] == 2

    model = document.models[0]
    assert model.name == "hourly_counts"
    assert model.key == ["window_ts", "sensor_id"]
    assert model.grain == "hour"
    assert model.source.path == "landing/"  # the ${VAR:-default} took its default
    assert model.source.max_files_per_trigger == 10
    assert str(model.tier) == "additive"
    assert model.resolved_strategy == "delta_merge"


@pytest.mark.parametrize(
    "sink",
    [
        "fake_sink",
        pytest.param("table", marks=requires_table_sink),
    ],
)
def test_the_plan_example_as_written_is_refused(sink):
    """Pins the invariant: a fixture copied from ``PLAN.md`` must not load.

    ``PLAN.md``'s worked examples say ``key: [hour_ts, sensor_id]``. The window
    column is ``window_ts`` at every grain, which is what makes "the merge key
    must equal the window grain key" mechanically checkable instead of a
    convention nobody enforces.
    """
    with pytest.raises(ModelValidationError) as caught:
        parse_yaml(
            plan_example(window="hour_ts", sink=sink), source="models.yaml", env={}
        )
    message = str(caught.value)
    assert "window_ts" in message
    assert "silently overwrites" in message


# ---------------------------------------------------------------------------
# Typed coercion of substituted values
#
# ``${VAR}`` is the only mechanism PLAN.md offers for environment differences,
# and there are no CLI overrides -- so if a substituted value could never become
# an int, no numeric knob could vary between environments. Coercion is therefore
# required, but narrow: only a value that is *entirely* one reference, and only
# to the type its target declares.
# ---------------------------------------------------------------------------


def env_model(env: dict[str, str], **overrides: Any) -> Model:
    return parse_yaml(minimal(**overrides), source="models.yaml", env=env).models[0]


def test_a_substituted_integer_becomes_an_int():
    source = env_model(
        {"MAX_FILES": "7"},
        source={"type": "file", "path": "l/", "max_files_per_trigger": "${MAX_FILES}"},
    ).source
    assert source.max_files_per_trigger == 7
    assert type(source.max_files_per_trigger) is int


def test_a_substituted_float_becomes_a_float():
    source = env_model(
        {"SETTLE": "2.5"},
        source={"type": "file", "path": "l/", "settle_seconds": "${SETTLE}"},
    ).source
    assert source.settle_seconds == 2.5
    assert type(source.settle_seconds) is float


@pytest.mark.parametrize(
    "text, expected",
    [("true", True), ("TRUE", True), ("yes", True), ("1", True),
     ("false", False), ("No", False), ("0", False)],
)
def test_a_substituted_boolean_becomes_a_bool(text, expected):
    source = env_model(
        {"RECURSIVE": text},
        source={"type": "file", "path": "l/", "recursive": "${RECURSIVE}"},
    ).source
    assert source.recursive is expected


def test_a_default_valued_reference_is_coerced_too():
    source = env_model(
        {}, source={"type": "file", "path": "l/", "max_files_per_trigger": "${N:-10}"}
    ).source
    assert source.max_files_per_trigger == 10


def test_a_model_level_limit_is_coerced():
    model = env_model({"ROWS": "50000"}, limits={"max_rows_per_trigger": "${ROWS}"})
    assert model.limits == BatchLimits(max_rows_per_trigger=50_000)


def test_the_declared_type_decides_not_the_text():
    """``${MARKER}`` of ``"2024"`` targets a ``str`` field, so it stays a string.

    Guessing from the shape of the substituted text is the tempting shortcut and
    the wrong one: a marker file, a table suffix or a sensor id that happens to
    look numeric must not silently become an int.
    """
    source = env_model(
        {"MARKER": "2024"}, source={"type": "file", "path": "l/", "marker": "${MARKER}"}
    ).source
    assert source.marker == "2024"
    assert type(source.marker) is str


def test_a_reference_inside_a_longer_string_stays_a_string():
    source = env_model(
        {"STAGE": "2024"}, source={"type": "file", "path": "landing/${STAGE}"}
    ).source
    assert source.path == "landing/2024"
    assert type(source.path) is str


def test_an_unparseable_value_names_the_variable_field_and_type():
    with pytest.raises(ConfigError) as caught:
        env_model(
            {"MAX_FILES": "ten"},
            source={
                "type": "file",
                "path": "l/",
                "max_files_per_trigger": "${MAX_FILES}",
            },
        )
    message = str(caught.value)
    assert "environment variable 'MAX_FILES'" in message
    assert "resolved to 'ten'" in message
    assert "'max_files_per_trigger' expects an integer" in message
    assert "models.yaml:" in message
    assert "model 'm'" in message


def test_an_unparseable_default_says_it_came_from_the_default():
    with pytest.raises(ConfigError, match=r"the default of \$\{N:-\.\.\.\}"):
        env_model(
            {}, source={"type": "file", "path": "l/", "max_files_per_trigger": "${N:-x}"}
        )


def test_an_unparseable_boolean_is_refused():
    with pytest.raises(ConfigError, match="expects a boolean"):
        env_model(
            {"R": "maybe"}, source={"type": "file", "path": "l/", "recursive": "${R}"}
        )


def test_a_float_valued_variable_is_refused_where_an_int_is_expected():
    with pytest.raises(ConfigError, match="expects an integer"):
        env_model(
            {"N": "1.5"},
            source={"type": "file", "path": "l/", "max_files_per_trigger": "${N}"},
        )


def test_a_literal_quoted_number_is_still_refused():
    """Coercion is for substitution only; a number the user quoted is a mistake."""
    with pytest.raises(ConfigError) as caught:
        env_model(
            {}, source={"type": "file", "path": "l/", "max_files_per_trigger": "10"}
        )
    message = str(caught.value)
    assert "must be a positive integer or None" in message
    assert "got str: '10'" in message

    with pytest.raises(ModelValidationError, match="limits.max_rows_per_trigger"):
        env_model({}, limits={"max_rows_per_trigger": "50000"})


def test_settings_keep_a_non_numeric_substituted_value_as_a_string():
    text = yaml.safe_dump(
        {
            "catalog": CATALOG,
            "settings": {
                "memory_limit": "${MEM}",
                "threads": "${THREADS}",
                "preserve_insertion_order": "${ORDER}",
                "temp_directory": "${TMP:-/var/tmp}",
            },
            "models": [yaml.safe_load(minimal())["models"][0]],
        },
        sort_keys=False,
    )
    document = parse_yaml(
        text,
        source="models.yaml",
        env={"MEM": "2GB", "THREADS": "2", "ORDER": "false"},
    )
    assert document.settings == {
        "memory_limit": "2GB",  # not a number, so still a string
        "threads": 2,
        "preserve_insertion_order": False,
        "temp_directory": "/var/tmp",
    }
    assert type(document.settings["threads"]) is int
    assert document.settings["preserve_insertion_order"] is False


def test_a_coerced_document_still_round_trips_through_yaml():
    """Nothing of the internal marker type may survive: safe_dump refuses subclasses."""
    document = parse_yaml(
        minimal(
            source={
                "type": "file",
                "path": "${LANDING}",
                "max_files_per_trigger": "${MAX_FILES}",
            }
        ),
        source="models.yaml",
        env={"LANDING": "landing/", "MAX_FILES": "7"},
    )
    dumped = yaml.safe_dump(document.to_config(), sort_keys=False)
    assert parse_yaml(dumped, source="again.yaml", env={}) == document


def test_substitute_env_returns_plain_strings():
    """It has no target field, so it cannot coerce -- and must not leak the marker."""
    value = substitute_env({"a": ["${N}"]}, env={"N": "10"})
    assert value == {"a": ["10"]}
    assert type(value["a"][0]) is str
    yaml.safe_dump(value)  # would raise on a str subclass
