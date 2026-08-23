"""The rejection path: an additive strategy over a non-foldable aggregate.

``PLAN.md`` calls foldability classification "the framework's reason to exist"
and is explicit about where the refusal must happen: *"an additive strategy over
a non-foldable aggregate must be refused at load, not at runtime"*. That word is
the whole test. A refusal at runtime is a cron log at 03:00; a refusal at load
is a failed deploy. And the alternative to refusing at all is not an error but a
plausible wrong number -- a mart that folds an average as if it were additive
does not fail, it just quietly disagrees with a full recompute.

So these tests assert three things, in order of how easy they are to lose:

1. the *message*, not merely the exception type. A rejection that does not name
   the offending column, the tier it classified as, the aggregate that decided
   it, and what to do instead is a rejection an operator has to reverse-engineer.
2. that it happens at **load**: before a catalog is created, before a connection
   is opened, before any data is touched.
3. that it happens identically through **both front doors**, since a config path
   with a weaker check is the same defect wearing a different hat.
"""

from __future__ import annotations

import pytest
import yaml

from harness import ADDITIVE, build_model

from duckstream import Engine, Model
from duckstream.errors import ConfigError, ModelValidationError
from duckstream.sinks.table import TableSink
from duckstream.sources.files import FileSource

#: Aggregate expressions that are *not* additive, with the substring the message
#: must name so the operator can see which construct decided the tier.
NON_ADDITIVE = {
    "median": ("median(value)", "non_foldable", "median"),
    "quantile": ("quantile_cont(value, 0.5)", "non_foldable", "quantile"),
    "count_distinct": ("count(DISTINCT sensor_id)", "non_foldable", "DISTINCT"),
    "avg": ("avg(value)", "sufficient_statistics", "avg"),
    "stddev": ("stddev(value)", "sufficient_statistics", "stddev"),
    "derived": ("sum(value) / count(*)", "non_foldable", ""),
}


def _model(expression: str, *, strategy: str | None = "delta_merge") -> Model:
    return Model(
        name="rejected",
        source=FileSource("landing", marker="_READY"),
        sink=TableSink("marts.rejected", mode="update"),
        aggregates={"bad": expression, "n": "count(*)"},
        key=["window_ts", "sensor_id"],
        time_column="event_ts",
        grain="hour",
        strategy=strategy,
    )


def _document(expression: str, catalog: str, *, strategy: str = "delta_merge") -> str:
    model = _model(expression, strategy=strategy)
    config = model.to_config()
    config["source"]["path"] = "landing/"
    return yaml.safe_dump(
        {"catalog": catalog, "data_path": "lake_data", "models": [config]},
        sort_keys=False,
    )


# --------------------------------------------------------------------------
# The refusal, through the Python door
# --------------------------------------------------------------------------


@pytest.mark.parametrize("label", sorted(NON_ADDITIVE))
def test_additive_strategy_over_a_non_foldable_aggregate_is_refused(label):
    """``Model.validate`` refuses it, and says enough to act on."""
    expression, tier, culprit = NON_ADDITIVE[label]

    with pytest.raises(ModelValidationError) as excinfo:
        _model(expression).validate()

    message = str(excinfo.value)
    assert "delta_merge" in message, "the message must name the declared strategy"
    assert tier in message, f"the message must name the tier it classified as ({tier})"
    assert "'bad'" in message, "the message must name the offending output column"
    assert expression in message, "the message must quote the aggregate expression"
    if culprit:
        assert culprit in message, (
            f"the message must name what decided the tier ({culprit!r}), or the "
            f"operator cannot tell which part of the expression is the problem"
        )
    # And it must say what to do instead, not only what is wrong.
    assert "strategy=" in message or "strategy '" in message
    assert "rejected" in message, "the message must name the model"


@pytest.mark.parametrize("label", sorted(NON_ADDITIVE))
def test_engine_add_refuses_the_same_declaration(tmp_path, label):
    """``Engine.add`` validates, so the Python door cannot skip the check.

    ``Model(...)`` on its own does not validate -- validation is a method, and
    a half-built model in a notebook is not an error. The claim is that nothing
    can *run* an invalid model, and ``Engine.add`` is the only way in.
    """
    expression, _tier, _culprit = NON_ADDITIVE[label]
    import duckdb

    con = duckdb.connect()
    try:
        engine = Engine(
            con,
            catalog=str(tmp_path / "catalog.ducklake"),
            data_path=str(tmp_path / "lake_data"),
        )
        with pytest.raises(ModelValidationError):
            engine.add(_model(expression))
        assert engine.models == []
    finally:
        con.close()


def test_an_additive_model_asking_for_recompute_window_is_also_refused():
    """The check is keyed off the resolved strategy, not only the tier.

    A ``count(*)`` model that explicitly declares ``recompute_window`` is not
    silently folded as additive: phase 1 implements one strategy and says so.
    Without this the "declared strategy is honoured" property would hold in one
    direction only.
    """
    model = _model("count(*)", strategy="recompute_window")
    model.validate()  # the declaration itself is coherent
    assert model.resolved_strategy == "recompute_window"

    sink = TableSink("marts.rejected", mode="update")
    with pytest.raises(Exception) as excinfo:
        sink.ensure(_FailingConnection(), model)
    message = str(excinfo.value)
    assert "recompute_window" in message
    assert "cannot be expressed as a merge" in message


class _FailingConnection:
    """Refuses every statement. The refusal must come before any SQL runs."""

    def execute(self, sql, *args, **kwargs):  # pragma: no cover - must not run
        raise AssertionError(f"the sink issued SQL before refusing the model:\n{sql}")


# --------------------------------------------------------------------------
# The refusal, through the config door
# --------------------------------------------------------------------------


@pytest.mark.parametrize("label", sorted(NON_ADDITIVE))
def test_the_config_door_refuses_it_identically(tmp_path, label):
    """Same error, same class, plus the file and line. Never weaker.

    ``PLAN.md`` rule 2: the loader is a deserialiser and the same validation
    runs. A ``ConfigError`` here instead of a ``ModelValidationError`` would
    already be a divergence -- a deploy script catching one would miss the
    other.
    """
    expression, tier, _culprit = NON_ADDITIVE[label]
    path = tmp_path / "models.yaml"
    catalog = f"ducklake:{(tmp_path / 'catalog.ducklake').as_posix()}"
    path.write_text(_document(expression, catalog), encoding="utf-8")

    from duckstream.config import load_config

    with pytest.raises(ModelValidationError) as excinfo:
        load_config(str(path))

    message = str(excinfo.value)
    assert "delta_merge" in message and tier in message
    assert "models.yaml" in message, "the config path must locate the failure"

    python_message = str(
        pytest.raises(ModelValidationError, _model(expression).validate).value
    )
    # The config message is the Python one plus a location, never a shorter one.
    for fragment in ("delta_merge", tier, "'bad'", expression):
        assert fragment in python_message and fragment in message


def test_the_refusal_happens_at_load_and_touches_nothing(tmp_path):
    """No catalog, no data directory, no connection. Refused before any of it.

    "At load, not at runtime" is only meaningful if load genuinely precedes the
    storage layer. This asserts the filesystem afterwards: a catalog file or a
    data directory left behind would mean the model was refused *after* the
    engine had begun setting itself up, which is a different and much weaker
    guarantee.
    """
    path = tmp_path / "models.yaml"
    catalog_path = tmp_path / "catalog.ducklake"
    data_path = tmp_path / "lake_data"
    path.write_text(
        _document("median(value)", f"ducklake:{catalog_path.as_posix()}"),
        encoding="utf-8",
    )

    with pytest.raises(ModelValidationError):
        Engine.from_config(str(path))

    assert not catalog_path.exists(), (
        "a catalog was created before the model was refused, so the refusal is "
        "not at load time"
    )
    assert not data_path.exists()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["models.yaml"]


def test_several_bad_models_are_reported_together(tmp_path):
    """A deploy-time check that reports one problem per run is a bad one.

    ``ConfigError`` aggregates, and the CLI prints the list. This is about the
    ergonomics of the rejection path rather than its correctness, but the
    rejection path is the framework's product and a check nobody runs twice is
    a check nobody runs.
    """
    catalog = f"ducklake:{(tmp_path / 'c.ducklake').as_posix()}"
    models = []
    for index, expression in enumerate(("median(value)", "avg(value)")):
        config = _model(expression).to_config()
        config["name"] = f"bad{index}"
        config["sink"]["table"] = f"marts.bad{index}"
        models.append(config)
    path = tmp_path / "models.yaml"
    path.write_text(
        yaml.safe_dump({"catalog": catalog, "models": models}, sort_keys=False),
        encoding="utf-8",
    )

    from duckstream.config import load_config

    with pytest.raises(ConfigError) as excinfo:
        load_config(str(path))
    problems = [str(p) for p in getattr(excinfo.value, "errors", [])]
    assert len(problems) == 2, f"expected both models reported, got {problems}"
    assert any("bad0" in p for p in problems) and any("bad1" in p for p in problems)
    assert any("non_foldable" in p for p in problems)
    assert any("sufficient_statistics" in p for p in problems)


# --------------------------------------------------------------------------
# The accepted case, so the refusal is not simply "refuse everything"
# --------------------------------------------------------------------------


def test_the_additive_tier_is_accepted_and_runs(parity):
    """count / sum / min / max classify as additive and drain end to end.

    A rejection test in isolation proves nothing: a validator that refused every
    model would pass all of the above. This is the other half.
    """
    model = build_model(ADDITIVE, parity.landing)
    model.validate()
    assert str(model.tier) == "additive"
    assert model.resolved_strategy == "delta_merge"

    import datetime as dt

    parity.land("f1", [(dt.datetime(2026, 12, 1, 0, 5), "s1", 1.0)])
    parity.run()
    parity.land("f2", [(dt.datetime(2026, 12, 1, 0, 45), "s1", 2.0)])
    parity.run()
    parity.assert_matches_ground_truth()
    parity.assert_reached_matched_branch()
