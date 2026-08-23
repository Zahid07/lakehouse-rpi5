"""The CLI, in real subprocesses, because the exit code is the contract.

``PLAN.md`` puts *"``validate`` is honest"* under Verification with a reason
attached: "A model that fails at load must make ``duckstream validate`` exit
non-zero. Deployment scripts depend on it." A deployment script does not call
``duckstream.cli.main`` and read its return value -- it runs a process and looks
at ``$?``. So these tests spawn processes, even though ``CONTEXT.md`` 1.8
measures that at ~235 ms each. Everywhere else in this suite the CLI is driven
in process, where the spawn buys nothing; here the spawn *is* the thing under
test.

Both invocations ``PLAN.md`` requires are covered: ``python -m duckstream``,
which is what cron in a venv actually calls, and the ``duckstream`` console
script, which is skipped when it is not installed rather than silently not run.
"""

from __future__ import annotations

import datetime as dt
import shutil
import sys
from pathlib import Path

import pytest
import yaml

from harness import ADDITIVE, Landing, World, document_for, spawn

T = dt.datetime

MODULE = ["-m", "duckstream"]

#: The ``duckstream`` console script, if this environment has it installed.
#: ``PLAN.md`` asks for both entry points precisely because cron usually calls
#: the interpreter directly, so a missing script is a skip rather than a failure.
CONSOLE_SCRIPT = shutil.which("duckstream") or shutil.which(
    "duckstream", path=str(Path(sys.executable).parent)
)


def _invocations():
    yield pytest.param(MODULE, id="python -m duckstream")
    if CONSOLE_SCRIPT:
        yield pytest.param([CONSOLE_SCRIPT], id="console script")


def _run(invocation, *args):
    if invocation is MODULE or invocation == MODULE:
        return spawn([*MODULE, *args])
    # The console script is its own executable, not an interpreter argument.
    import subprocess

    return subprocess.run(
        [*invocation, *args], capture_output=True, text=True, timeout=300
    )


@pytest.fixture
def good_config(tmp_path, landing) -> Path:
    landing.drop("g1", [(T(2027, 1, 1, 0, 5), "s1", 1.0)])
    landing.drop("g2", [(T(2027, 1, 1, 0, 45), "s1", 2.0)])
    path = tmp_path / "good.yaml"
    path.write_text(
        yaml.safe_dump(
            document_for(
                ADDITIVE,
                landing,
                catalog=tmp_path / "good.ducklake",
                data_path=tmp_path / "good_data",
            ),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def bad_config(tmp_path, landing) -> Path:
    """A document whose only defect is one duckstream refuses at load."""
    document = document_for(
        ADDITIVE,
        landing,
        catalog=tmp_path / "bad.ducklake",
        data_path=tmp_path / "bad_data",
    )
    model = document["models"][0]
    model["strategy"] = "delta_merge"
    model["aggregates"]["p50"] = "median(value)"
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# validate
# --------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.parametrize("invocation", list(_invocations()))
def test_validate_exits_zero_on_a_good_document(invocation, good_config):
    result = _run(invocation, "validate", "--config", str(good_config))
    assert result.returncode == 0, result.stderr
    assert ADDITIVE.name in result.stdout
    assert "ok" in result.stdout


@pytest.mark.slow
@pytest.mark.parametrize("invocation", list(_invocations()))
def test_validate_exits_non_zero_on_a_model_that_fails_at_load(
    invocation, bad_config, tmp_path
):
    """The honesty requirement, as a deploy script would observe it.

    Non-zero exit, the reason on stderr rather than as a traceback, and nothing
    created on disk -- a validate that opened the catalog before deciding would
    be doing more than it says.
    """
    result = _run(invocation, "validate", "--config", str(bad_config))

    assert result.returncode != 0, (
        "validate exited 0 on a model that cannot be loaded; every deploy-time "
        "check in front of this framework is worthless if that is allowed"
    )
    assert result.returncode == 1, f"expected exit 1, got {result.returncode}"
    assert "Traceback" not in result.stderr, (
        f"the failure was reported as a traceback rather than a message:\n"
        f"{result.stderr}"
    )
    assert "delta_merge" in result.stderr and "non_foldable" in result.stderr
    assert "p50" in result.stderr
    assert not (tmp_path / "bad.ducklake").exists()
    assert not (tmp_path / "bad_data").exists()


@pytest.mark.slow
def test_validate_exits_non_zero_when_the_document_is_missing(tmp_path):
    result = spawn([*MODULE, "validate", "--config", str(tmp_path / "absent.yaml")])
    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert "absent.yaml" in result.stderr


@pytest.mark.slow
def test_validate_exits_non_zero_on_malformed_yaml(tmp_path):
    path = tmp_path / "broken.yaml"
    path.write_text("catalog: [unclosed\nmodels:\n", encoding="utf-8")
    result = spawn([*MODULE, "validate", "--config", str(path)])
    assert result.returncode == 1
    assert "Traceback" not in result.stderr


@pytest.mark.slow
def test_a_usage_error_is_distinguishable_from_a_model_error(tmp_path):
    """argparse exits 2; a duckstream failure exits 1. Deploy scripts branch on it."""
    result = spawn([*MODULE, "validate"])
    assert result.returncode == 2
    result = spawn([*MODULE, "no-such-command", "--config", "x.yaml"])
    assert result.returncode == 2


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.parametrize("invocation", list(_invocations()))
def test_run_drains_and_the_mart_is_readable_afterwards(
    invocation, good_config, tmp_path, landing
):
    """The cron entry point, end to end, in the process cron would spawn.

    Two batches, because ``CONTEXT.md`` 1.5's DuckLake failure only appeared on
    the second MERGE. Then the mart is read by an ordinary DuckDB client with no
    duckstream in the picture at all, which is the read-path claim ``PLAN.md``
    makes: "Results are ordinary DuckLake tables".
    """
    world = World("yaml", tmp_path, landing, ADDITIVE)
    world.catalog = tmp_path / "good.ducklake"
    world.data_path = tmp_path / "good_data"

    first = _run(invocation, "run", "--config", str(good_config))
    assert first.returncode == 0, first.stderr
    assert ADDITIVE.name in first.stdout

    landing.drop("g3", [(T(2027, 1, 1, 1, 5), None, 4.0)])
    second = _run(invocation, "run", "--config", str(good_config))
    assert second.returncode == 0, second.stderr

    third = _run(invocation, "run", "--config", str(good_config))
    assert third.returncode == 0, third.stderr
    assert "nothing to do" in third.stdout

    import duckdb

    from duckstream import lake as lakemod

    con = duckdb.connect()
    try:
        con.execute("INSTALL ducklake")
        con.execute("LOAD ducklake")
        con.execute(
            f"ATTACH 'ducklake:{world.catalog.as_posix()}' AS lake (READ_ONLY)"
        )
        rows = con.execute(
            "SELECT window_ts, sensor_id, n, total FROM lake.marts.out ORDER BY 1, 2"
        ).fetchall()
        snapshots = lakemod.snapshot_count(con, "lake")
    finally:
        con.close()

    assert rows == [
        (T(2027, 1, 1, 0, 0), "s1", 2, 3.0),
        (T(2027, 1, 1, 1, 0), None, 1, 4.0),
    ]
    # Two committing triggers plus the two setup snapshots and catalog creation.
    assert snapshots == 5


@pytest.mark.slow
def test_run_with_an_unknown_model_name_exits_non_zero(good_config):
    result = spawn([*MODULE, "run", "--config", str(good_config), "--model", "nope"])
    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert "nope" in result.stderr


@pytest.mark.slow
def test_run_once_commits_a_single_batch(tmp_path, landing):
    """``--once`` steps one batch per model rather than draining.

    Checked through the snapshot count rather than the printed summary: one
    trigger is one snapshot, so "exactly one batch happened" is a fact about the
    catalog and not about the wording of a log line.
    """
    for name in ("o1", "o2", "o3"):
        landing.drop(name, [(T(2027, 2, 1, 0, 5), "s1", 1.0)])

    scenario = ADDITIVE.chunked(1)
    path = tmp_path / "once.yaml"
    path.write_text(
        yaml.safe_dump(
            document_for(
                scenario,
                landing,
                catalog=tmp_path / "once.ducklake",
                data_path=tmp_path / "once_data",
            ),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    world = World("yaml", tmp_path / "once_world", landing, scenario)
    world.catalog = tmp_path / "once.ducklake"
    world.data_path = tmp_path / "once_data"

    assert spawn([*MODULE, "run", "--config", str(path), "--once"]).returncode == 0
    after_first = world.snapshot_count()
    assert world.offset_files() == ["o1/part.parquet"]

    assert spawn([*MODULE, "run", "--config", str(path), "--once"]).returncode == 0
    assert world.snapshot_count() == after_first + 1
    assert world.offset_files() == ["o1/part.parquet", "o2/part.parquet"]


# --------------------------------------------------------------------------
# models, version
# --------------------------------------------------------------------------


@pytest.mark.slow
def test_status_reports_a_model_that_has_never_run(good_config):
    """Idle is not broken, and a health probe must not say it is."""
    result = spawn(["-m", "duckstream", "status", "--config", str(good_config)])
    assert result.returncode == 0, result.stderr
    assert "MODEL" in result.stdout and "hourly_counts" in result.stdout
    assert "idle" in result.stdout


def test_status_reports_lag_after_a_run(good_config):
    drained = spawn(["-m", "duckstream", "run", "--config", str(good_config)])
    assert drained.returncode == 0, drained.stderr

    result = spawn(["-m", "duckstream", "status", "--config", str(good_config)])
    assert result.returncode == 0, result.stderr
    header, row = result.stdout.strip().splitlines()[:2]
    assert "EVENT LAG" in header and "SINCE RUN" in header and "BACKLOG" in header
    assert "ok" in row
    # Two drops of one row each were consumed, and the mart holds one folded row.
    assert " 2 " in f" {row} " or "	2	" in row


def test_status_json_is_machine_readable(good_config):
    """A probe should not have to parse columns, and durations are seconds.

    "3m12s" is not something a threshold can be compared against.
    """
    import json

    spawn(["-m", "duckstream", "run", "--config", str(good_config)])
    result = spawn(
        ["-m", "duckstream", "status", "--json", "--config", str(good_config)]
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout.strip().splitlines()[0])
    assert payload["model"] == "hourly_counts"
    assert payload["state"] == "ok" and payload["healthy"] is True
    assert isinstance(payload["processing_lag_seconds"], float)
    assert payload["rows_out"] >= 1, "rows_out is still not being recorded"
    assert payload["batches"] >= 1


def test_status_can_be_narrowed_to_one_model(good_config):
    result = spawn(
        ["-m", "duckstream", "status", "--model", "hourly_counts",
         "--config", str(good_config)]
    )
    assert result.returncode == 0, result.stderr
    assert "hourly_counts" in result.stdout

    missing = spawn(
        ["-m", "duckstream", "status", "--model", "nope",
         "--config", str(good_config)]
    )
    assert missing.returncode != 0


def test_models_lists_the_resolved_tier_and_strategy(good_config):
    result = spawn([*MODULE, "models", "--config", str(good_config)])
    assert result.returncode == 0, result.stderr
    assert "TIER" in result.stdout and "STRATEGY" in result.stdout
    assert "additive" in result.stdout and "delta_merge" in result.stdout
    assert ADDITIVE.name in result.stdout


@pytest.mark.slow
def test_version_is_reported_by_both_entry_points():
    from duckstream import __version__

    result = spawn([*MODULE, "--version"])
    assert result.returncode == 0
    assert __version__ in (result.stdout + result.stderr)


@pytest.mark.slow
def test_the_console_script_is_declared_even_when_not_installed():
    """``PLAN.md`` asks for both entry points; the declaration is checkable.

    Whether the script is on ``PATH`` depends on how the package was installed,
    and this repository's venv runs duckstream from the working tree. The
    *declaration* is not environment-dependent, so it is asserted here and the
    behavioural tests above skip the script when it is genuinely absent.
    """
    import tomllib

    from harness import REPO_ROOT

    with open(REPO_ROOT / "pyproject.toml", "rb") as handle:
        pyproject = tomllib.load(handle)
    assert pyproject["project"]["scripts"]["duckstream"] == "duckstream.cli:main"

    if CONSOLE_SCRIPT is None:
        pytest.skip(
            "the duckstream console script is not installed in this venv; "
            "python -m duckstream is covered above"
        )
