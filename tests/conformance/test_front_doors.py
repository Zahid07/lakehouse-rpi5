"""Two front doors, one canonical model -- and the machinery that keeps it so.

``PLAN.md`` names drift as the failure mode to design against: the config loader
supporting a subset, the Python API growing what config cannot express, the two
paths diverging. It also says discipline is not sufficient and the check must be
mechanical. Two mechanisms are specified, and both are here.

**Config round-trip.** ``Model`` -> dict -> YAML -> ``Model`` must reconstruct
an identical object. A field addable in Python but not expressible in config
fails here.

**Front-door parity.** Every conformance scenario runs through both doors and
must produce identical output. That is enforced in :class:`~harness.Parity`
rather than in this module, deliberately: a guarantee that each new test has to
remember to ask for is a guarantee that decays. What this module adds is the
end-to-end statement of it over a multi-batch drain, plus the checks on the
*seam* between the doors that a scenario cannot reach -- that
``Engine.from_config`` returns an ordinary engine, that the document a scenario
runs from is byte-for-byte the model's own ``to_config``, and that the CLI has
no way to arm a fault hook.
"""

from __future__ import annotations

import datetime as dt
from textwrap import dedent

import pytest
import yaml

from harness import ADDITIVE, DOORS, Scenario, build_model, document_for

from duckstream import FAULT_POINTS, Model
from duckstream.config import parse_config, parse_yaml
from duckstream.protocols import BatchLimits
from duckstream.sinks.table import TableSink
from duckstream.sources.files import FileSource

T = dt.datetime


# --------------------------------------------------------------------------
# Round-trip
# --------------------------------------------------------------------------


def _round_trip(model: Model, *, catalog: str = "ducklake:c.ducklake") -> Model:
    """``Model`` -> dict -> YAML text -> parsed -> ``Model``."""
    text = yaml.safe_dump(
        {"catalog": catalog, "models": [model.to_config()]}, sort_keys=False
    )
    document = parse_yaml(text, source="round-trip.yaml")
    assert len(document.models) == 1
    return document.models[0]


ROUND_TRIP_MODELS = {
    "minimal": Model(
        name="minimal",
        source=FileSource("landing"),
        sink=TableSink("marts.minimal"),
        aggregates={"n": "count(*)"},
        key=["sensor_id"],
    ),
    "phase_one": build_model(ADDITIVE, "landing"),
    "every_field": Model(
        name="every_field",
        source=FileSource(
            "landing/tree",
            marker="_DONE",
            settle_seconds=1.5,
            format="csv",
            pattern="**/*.csv",
            recursive=False,
            max_files_per_trigger=7,
            max_rows_per_trigger=1000,
        ),
        sink=TableSink("marts.every_field", mode="append"),
        aggregates={"n": "count(*)", "total": "sum(value)"},
        key=["window_ts", "sensor_id"],
        time_column="event_ts",
        grain="day",
        # Windowed append needs a horizon -- Model._check_output_mode -- which
        # is convenient here, since this model exists to set every field.
        lateness="90 minutes",
        strategy="delta_merge",
        memory_profile="streaming",
        udfs=["my_pkg.signal:arrow_fft"],
        limits=BatchLimits(max_rows_per_trigger=500, max_files_per_trigger=3),
        # Both off their defaults, so the round trip covers them. 'halt' is the
        # non-default policy; see Model._check_failure_policy.
        on_failure="halt",
        max_attempts=9,
    ),
    "no_marker": Model(
        name="no_marker",
        source=FileSource("landing", marker=None),
        sink=TableSink("marts.no_marker", mode="update"),
        aggregates={"hi": "max(value)"},
        key=["window_ts"],
        time_column="event_ts",
        grain="minute",
    ),
}


@pytest.mark.parametrize("label", sorted(ROUND_TRIP_MODELS))
def test_model_round_trips_through_yaml_unchanged(label):
    """The object that comes back is equal to the one that went out.

    Equality, not "the fields I remembered to compare": ``Model`` is a
    dataclass, and ``FileSource``/``TableSink`` define ``__eq__`` over their own
    ``to_config``, so this compares the whole declaration. That is what makes a
    Python-only field a test failure rather than a slow drift.
    """
    original = ROUND_TRIP_MODELS[label]
    original.validate()
    restored = _round_trip(original)

    assert restored == original
    assert restored.to_config() == original.to_config()
    assert restored.tier == original.tier
    assert restored.resolved_strategy == original.resolved_strategy


@pytest.mark.parametrize("label", sorted(ROUND_TRIP_MODELS))
def test_round_trip_is_a_fixed_point(label):
    """A second pass changes nothing, so the config is canonical, not lossy.

    A round trip that merely *survives* could still be normalising something on
    every pass -- growing a ``main.`` prefix, expanding a default. Comparing the
    second pass to the first is what rules that out.
    """
    once = _round_trip(ROUND_TRIP_MODELS[label])
    twice = _round_trip(once)
    assert twice.to_config() == once.to_config()


def test_every_model_field_is_expressible_in_config():
    """No ``Model`` field exists that ``to_config`` cannot emit.

    The round-trip tests above catch a field that is dropped *and* whose loss
    changes equality. This catches the other shape of the same drift: a field
    added to the dataclass and never taught to ``to_config``, which the
    round-trip would miss for as long as its default happened to be restored.
    """
    fields = set(Model.__dataclass_fields__)
    emitted = set(ROUND_TRIP_MODELS["every_field"].to_config())
    missing = fields - emitted
    assert not missing, (
        f"Model fields {sorted(missing)} are not emitted by to_config(), so they "
        f"cannot be expressed in YAML and the two front doors have drifted. Add "
        f"them to to_config() and to the 'every_field' fixture here."
    )


def test_parse_config_accepts_the_dict_to_config_produced():
    """``to_config`` output is valid input, without a YAML text round trip."""
    document = parse_config(
        {
            "catalog": "ducklake:c.ducklake",
            "models": [m.to_config() for m in ROUND_TRIP_MODELS.values()],
        }
    )
    assert document.names == [m.name for m in ROUND_TRIP_MODELS.values()]


# --------------------------------------------------------------------------
# Parity, end to end
# --------------------------------------------------------------------------


def test_both_doors_produce_identical_output_over_a_drain(parity):
    """The parity guarantee, stated once explicitly as well as enforced always.

    Interleaved drops, a chunked drain, NULL keys and an idle pass, through both
    doors over the same landing tree. :meth:`~harness.Parity.run` compares the
    mart, the committed offset and the snapshot count after every step; this
    test additionally compares the full snapshot *history*, so the two doors are
    shown to have taken the same path and not merely arrived at the same place.
    """
    parity.land("p1", [(T(2026, 11, 1, 0, 5), "s1", 1.0), (T(2026, 11, 1, 0, 9), None, 2.0)])
    parity.run()
    parity.land("p2", [(T(2026, 11, 1, 0, 45), "s1", 4.0)])
    parity.land("p3", [(T(2026, 11, 1, 1, 5), None, 8.0)])
    parity.run()
    parity.run()  # idle through both doors

    parity.assert_agree()
    parity.assert_matches_ground_truth()
    parity.assert_reached_matched_branch()

    walks = {entry["door"]: entry["walk"] for entry in
             parity.assert_snapshot_history_consistent()}
    python_history = [(s["snapshot_id"], s["mart"], s["consumed"]) for s in walks["python"]]
    yaml_history = [(s["snapshot_id"], s["mart"], s["consumed"]) for s in walks["yaml"]]
    assert python_history == yaml_history, (
        "the two front doors reached the same final state by different "
        "snapshot histories"
    )


def test_the_yaml_document_is_the_model_s_own_to_config(tmp_path, landing):
    """The YAML door is fed ``Model.to_config()``, not a hand-written document.

    This is why the parity check is meaningful. If the harness wrote its own
    YAML, a parity failure could be the fixture describing the model
    differently, and a parity *pass* could be two documents that happen to agree
    while the loader quietly drops a field.
    """
    document = document_for(
        ADDITIVE, landing, catalog=tmp_path / "c.ducklake", data_path=tmp_path / "d"
    )
    assert document["models"] == [build_model(ADDITIVE, landing).to_config()]

    reparsed = parse_config(document)
    assert reparsed.models[0] == build_model(ADDITIVE, landing)
    assert reparsed.settings == document["settings"]


def test_from_config_returns_an_ordinary_engine(tmp_path, landing):
    """``Engine.from_config`` is a constructor, not a second execution path.

    ``PLAN.md`` rule 2: the loader is a deserialiser only, and the engine it
    returns is one Python can keep modifying. If it were a distinct kind of
    engine, "both doors run the same code" would be false however well the
    outputs matched.
    """
    from duckstream import Engine

    world_root = tmp_path / "fc"
    world_root.mkdir()
    catalog = world_root / "catalog.ducklake"
    path = world_root / "models.yaml"
    path.write_text(
        yaml.safe_dump(
            document_for(
                ADDITIVE, landing, catalog=catalog, data_path=world_root / "lake_data"
            ),
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    engine = Engine.from_config(str(path))
    try:
        assert type(engine) is Engine
        assert [m.name for m in engine.models] == [ADDITIVE.name]
        assert engine.models[0] == build_model(ADDITIVE, landing)
        assert engine.document is not None and engine.document.path == str(path)

        # Still an ordinary engine: more models can be added in Python.
        extra = Scenario(
            name="second",
            aggregates={"n": "count(*)"},
            key=("window_ts",),
            recompute_sql="SELECT 1",
            table="marts.second",
        )
        engine.add(build_model(extra, landing))
        assert [m.name for m in engine.models] == [ADDITIVE.name, "second"]

        # And fault hooks are installable on it, which is the only way they ever
        # get installed -- see the test below.
        assert engine.faults.installed() == []
        engine.faults.install("after_commit", lambda event: None)
        assert engine.faults.installed() == ["after_commit"]
    finally:
        engine.close()


# --------------------------------------------------------------------------
# The cron entry point cannot arm a fault
# --------------------------------------------------------------------------


def test_no_config_key_or_cli_flag_can_arm_a_fault_hook(tmp_path, landing):
    """Fault injection is a test facility, and structurally unreachable in prod.

    The conformance suite kills real processes at named points inside the batch
    lifecycle, which means the mechanism to do so ships in the package. It is
    only safe because there is exactly one way to reach it -- an explicit call
    on a live ``Engine`` -- and no config key, environment variable or CLI flag
    that does. That is asserted rather than trusted.
    """
    from duckstream.cli import build_parser

    parser = build_parser()
    options = {
        action.dest
        for action in parser._actions  # noqa: SLF001 - the surface being audited
    }
    for subparser in (
        action
        for action in parser._actions  # noqa: SLF001
        if hasattr(action, "choices") and isinstance(action.choices, dict)
    ):
        for command in subparser.choices.values():
            options |= {a.dest for a in command._actions}  # noqa: SLF001
    forbidden = {o for o in options if "fault" in o or "kill" in o or "crash" in o}
    assert not forbidden, f"the CLI exposes fault injection: {forbidden}"

    # A document naming a fault point is rejected as an unknown key rather than
    # quietly honoured.
    document = document_for(
        ADDITIVE, landing, catalog=tmp_path / "c.ducklake", data_path=tmp_path / "d"
    )
    document["faults"] = {"after_sink_write": "os._exit"}
    with pytest.raises(Exception) as excinfo:
        parse_config(document)
    assert "faults" in str(excinfo.value)

    document.pop("faults")
    document["models"][0]["faults"] = {"after_commit": "os._exit"}
    with pytest.raises(Exception) as excinfo:
        parse_config(document)
    assert "faults" in str(excinfo.value)

    assert FAULT_POINTS == (
        "after_plan",
        "after_bind",
        "after_sink_write",
        "before_commit",
        "after_commit",
    )


# --------------------------------------------------------------------------
# The guard that keeps parity unforgettable
# --------------------------------------------------------------------------


def test_the_single_door_exemption_list_has_no_stale_entries(
    single_door_exemptions, stale_exemptions
):
    """Every exemption still applies, and every one gives a reason.

    The guard in ``conftest.py`` stops a new scenario silently covering one
    door. This stops the *exemption list* rotting in the other direction -- an
    entry left behind after a test was deleted, renamed, or moved onto
    ``Parity`` is a hole nobody would notice, because a stale allowlist entry
    never fails anything.

    ``stale_exemptions`` is ``None`` when the run was filtered (``-k``, a single
    file, a marker expression), because then every unused entry is an artefact
    of the selection rather than a fact about the suite. Skipping is the honest
    answer; the full-suite run is where this test earns its place.
    """
    for name, reason in single_door_exemptions.items():
        assert reason and len(reason) > 40, (
            f"the exemption for {name!r} needs a reason someone else can "
            f"evaluate, not a placeholder"
        )

    if stale_exemptions is None:
        pytest.skip(
            "the exemption audit needs a complete, unfiltered collection of "
            "tests/conformance; run the whole directory to exercise it"
        )
    assert stale_exemptions == [], (
        f"these entries in SINGLE_DOOR_EXEMPTIONS no longer describe a test "
        f"that drives one front door -- the test was renamed, deleted, or now "
        f"covers both doors, so remove them: {stale_exemptions}"
    )


_NESTED_CONFTEST = """
import sys
from importlib import util

sys.path[:0] = [{repo!r}, {conf!r}]

# Load the real conformance conftest by path and re-export the guard plus the
# fixtures it guards. Importing it as a module named `conftest` would collide
# with this file, so it is loaded under its own name.
_spec = util.spec_from_file_location("duckstream_conformance_conftest", {conftest!r})
_guard = util.module_from_spec(_spec)
_spec.loader.exec_module(_guard)

_door_choice_guard = _guard._door_choice_guard
landing = _guard.landing
make_world = _guard.make_world
make_parity = _guard.make_parity
parity = _guard.parity
"""


def _nested(tmp_path, body: str, filename: str = "test_nested.py"):
    """Run a miniature conformance session under the real guard, in its own process.

    A real nested pytest run rather than a synthesised ``Item``, because the
    thing being verified is not only the guard's decision but that it is wired
    in autouse and therefore cannot be bypassed by not asking for it. The
    ``pytester`` fixture would be the idiomatic tool; it needs a plugin
    registration this repository's pyproject does not carry, and a subprocess
    proves the same thing with nothing to install.
    """
    from harness import CONFORMANCE_DIR, REPO_ROOT, spawn

    directory = tmp_path / "nested"
    directory.mkdir()
    (directory / "conftest.py").write_text(
        _NESTED_CONFTEST.format(
            repo=str(REPO_ROOT),
            conf=str(CONFORMANCE_DIR),
            conftest=str(CONFORMANCE_DIR / "conftest.py"),
        ),
        encoding="utf-8",
    )
    (directory / filename).write_text(dedent(body), encoding="utf-8")
    return spawn(["-m", "pytest", str(directory), "-q", "-p", "no:cacheprovider"])


def test_the_door_choice_guard_rejects_a_forgetful_scenario(tmp_path):
    """The guard itself, verified the way the auditor verified the suite.

    A guard nobody has seen fail is a guard nobody knows works. This runs one
    miniature conformance-style test that drives a ``World`` through a single
    door without parametrising and without an exemption. It must fail, and the
    message must name all three ways out -- a guard that says "no" without
    saying "instead" just gets worked around.
    """
    result = _nested(
        tmp_path,
        """
        def test_forgets_the_second_door(make_world, landing):
            world = make_world("python")
            assert world.door == "python"
        """,
    )
    assert result.returncode != 0, result.stdout
    combined = result.stdout + result.stderr
    # Reported as an *error*, not a failure: the guard is an autouse fixture, so
    # the refusal happens in setup. That is the accurate semantics -- the test
    # could not be run as configured rather than having run and disagreed -- and
    # it is equally red, with a non-zero exit code either way.
    assert "1 error" in combined, combined
    assert "covers only one front door" in combined
    assert "make_parity" in combined and "harness.DOORS" in combined
    assert "SINGLE_DOOR_EXEMPTIONS" in combined


def test_the_door_choice_guard_accepts_both_ways_of_covering_both_doors(tmp_path):
    """And it does not fire on a test that made either legitimate choice.

    A guard that rejected everything would pass the test above while making the
    suite unwritable, so both accepted shapes are exercised -- parametrisation
    over ``DOORS``, and the ``Parity`` fixture -- along with a test that touches
    no catalog at all and is therefore none of the guard's business.
    """
    result = _nested(
        tmp_path,
        """
        import pytest
        from harness import DOORS

        @pytest.mark.parametrize("door", DOORS)
        def test_parametrised(make_world, landing, door):
            assert make_world(door).door == door

        def test_through_parity(parity):
            assert sorted(parity.worlds) == sorted(DOORS)

        def test_touches_no_catalog():
            assert DOORS == ("python", "yaml")
        """,
    )
    combined = result.stdout + result.stderr
    assert result.returncode == 0, combined
    assert "4 passed" in combined, combined
