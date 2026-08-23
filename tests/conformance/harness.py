"""The conformance harness: one scenario, two front doors, one landing tree.

Everything in ``tests/conformance`` is built on three objects here.

:class:`Landing`
    The source of truth for input. It writes parquet **atomically** -- temp
    path, ``os.replace``, *then* the completion marker, never the other order.
    That ordering is the writer's half of the file source's contract, and the
    build graph records what happens when a fixture breaks it: a file appended
    to between ``plan()`` and ``bind()`` is planned as one row, bound as three,
    and counted twice. That is a genuine double-count produced by the fixture,
    and it looks exactly like an engine bug. So the fixture never appends.

:class:`World`
    One catalog, one front door. ``door="python"`` builds an
    :class:`~duckstream.engine.Engine` and adds a :class:`~duckstream.model.Model`;
    ``door="yaml"`` writes ``models.yaml`` from ``Model.to_config()`` and calls
    :func:`duckstream.cli.main`. Both read the *same* landing tree, so the only
    difference between them is the front door.

:class:`Parity`
    Two worlds moved in lockstep. Every ``run()`` runs both doors and refuses
    to return until their mart contents, snapshot counts and committed offsets
    agree. ``PLAN.md`` asks that front-door parity live in the suite rather than
    in review; putting it in the harness rather than in a test is the stronger
    version of that -- a new scenario written against :class:`Parity` gets the
    parity guarantee whether or not its author remembered to ask for one.

Two deliberate choices worth knowing before writing a new scenario:

**Ground truth is hand-written SQL, not the sink's own generator.** Each
:class:`Scenario` carries a ``recompute_sql`` template. Reusing
``TableSink.aggregation_sql`` would compare duckstream against itself and pass
even if the generator were wrong.

**Every world opens a fresh connection per operation.** That is what cron does
(``CONTEXT.md`` 1.8: ~235 ms of process start per tick), it exercises offset
recovery from the state store on every run rather than only on the first, and
it sidesteps ``CONTEXT.md`` 1.6 -- a DuckDB catalog file held open by one
connection is not reliably openable by another.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
import math
import os
import subprocess
import sys
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

import duckdb

from duckstream import Model
from duckstream import lake as lakemod
from duckstream.sinks.table import TableSink
from duckstream.sources.files import FileSource
from duckstream.state import DuckLakeStateStore

__all__ = [
    "DOORS",
    "MARKER",
    "Landing",
    "Parity",
    "RecordingConnection",
    "RunSummary",
    "Scenario",
    "World",
    "ADDITIVE",
    "build_model",
    "normalise",
    "replay",
    "same_rows",
    "spawn",
]

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFORMANCE_DIR = Path(__file__).resolve().parent
FAULT_CHILD = CONFORMANCE_DIR / "_faultchild.py"

#: Both front doors. Test modules parametrise over this, so a scenario that
#: only works through one of them cannot be added without the failure showing.
DOORS: tuple[str, ...] = ("python", "yaml")

MARKER = "_READY"

#: Applied to every connection, through both doors, so the settings path is
#: itself covered by the parity check. Inlining is *not* listed: the engine
#: disables it unconditionally and refuses a caller who tries to set it.
SETTINGS: dict[str, Any] = {"threads": 2}

#: Catalog alias the engine attaches under. Fixed in ``Engine.__init__``.
ALIAS = "lake"

STATE_SCHEMA = "duckstream"


# --------------------------------------------------------------------------
# Comparing results
# --------------------------------------------------------------------------


def _sort_key(row: Sequence[Any]) -> tuple:
    """A total order over result rows that tolerates NULLs.

    ``ORDER BY`` in SQL would do, but a NULL grouping key is one of the cases
    ``PLAN.md`` asks the ground-truth diff to cover, and sorting NULLs in
    Python keeps the comparison independent of DuckDB's NULL ordering default.
    """
    return tuple((value is None, "" if value is None else str(value)) for value in row)


def normalise(rows: Iterable[Sequence[Any]]) -> list[tuple]:
    """Rows as sorted tuples, so two result sets compare by content only."""
    return sorted((tuple(row) for row in rows), key=_sort_key)


def same_rows(left: Iterable[Sequence[Any]], right: Iterable[Sequence[Any]]) -> bool:
    """Content equality, with floats compared to within 1e-12 relative.

    Exact equality would be the stronger claim and it does hold for every
    fixture here -- values are integers and binary-exact fractions, so a sum is
    the same whatever order the batches folded in. The tolerance exists so that
    a scenario added later with, say, 0.1 does not fail on float addition being
    non-associative, which is a property of arithmetic rather than a defect in
    the engine.
    """
    a, b = normalise(left), normalise(right)
    if len(a) != len(b):
        return False
    for row_a, row_b in zip(a, b):
        if len(row_a) != len(row_b):
            return False
        for x, y in zip(row_a, row_b):
            if isinstance(x, float) and isinstance(y, float):
                if not math.isclose(x, y, rel_tol=1e-12, abs_tol=1e-12):
                    return False
            elif x != y:
                return False
    return True


# --------------------------------------------------------------------------
# The landing tree
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Row:
    """One source row. ``sensor_id=None`` is the NULL-grouping-key case."""

    event_ts: dt.datetime
    sensor_id: str | None
    value: float


class Landing:
    """A directory tree of completion-marked parquet, written atomically.

    One subdirectory per drop, so a drop's marker gates exactly its own files
    and a later drop cannot make an earlier one's half-written file eligible.
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._drops: list[str] = []

    # -- writing ---------------------------------------------------------

    def drop(
        self,
        name: str,
        payload: Sequence[Row] | Sequence[tuple],
        *,
        filename: str = "part.parquet",
        marker: str | None = MARKER,
    ) -> Path:
        """Land one file, then its marker. Returns the parquet path.

        The order is the whole point and is never negotiable: write to a temp
        name, ``os.replace`` it into place (atomic on one filesystem, on
        Windows as well as POSIX), and only then create the marker. A marker
        that appears first would make a partial file eligible, and a file
        appended to after planning would be double-counted -- both would look
        like engine defects.
        """
        directory = self.root / name
        directory.mkdir(parents=True, exist_ok=True)
        table = _arrow_table(payload)

        temp = directory / f".{filename}.{uuid.uuid4().hex}.tmp"
        pq.write_table(table, temp)
        final = directory / filename
        os.replace(temp, final)  # atomic; the file is complete or absent

        if marker is not None:
            marker_path = directory / marker
            marker_temp = directory / f".{marker}.{uuid.uuid4().hex}.tmp"
            marker_temp.write_text("", encoding="utf-8")
            os.replace(marker_temp, marker_path)

        if name not in self._drops:
            self._drops.append(name)
        return final

    # -- reading ---------------------------------------------------------

    def parquet_files(self, *, marker: str | None = MARKER) -> list[str]:
        """Every readable parquet file, as absolute posix paths, sorted.

        "Readable" means the same thing the source means by it: inside a
        directory whose marker exists. An unmarked drop is invisible to the
        engine and must be invisible to the ground-truth recompute too, or the
        diff would report a difference the engine is right about.
        """
        found: list[str] = []
        for directory, _dirs, files in os.walk(self.root):
            path = Path(directory)
            if marker is not None and not (path / marker).exists():
                continue
            for name in files:
                if name.endswith(".parquet"):
                    found.append(Path(path / name).resolve().as_posix())
        return sorted(found)

    def relpaths(self, *, marker: str | None = MARKER) -> list[str]:
        base = self.root.resolve()
        return sorted(
            Path(f).relative_to(base).as_posix()
            for f in self.parquet_files(marker=marker)
        )

    def absolute(self, relpath: str) -> str:
        return (self.root.resolve() / relpath).as_posix()

    def source_expression(self, files: Sequence[str] | None = None) -> str:
        """``read_parquet([...])`` over ``files``, for the ground-truth SQL."""
        chosen = list(files) if files is not None else self.parquet_files()
        if not chosen:
            # An empty list is a binder error; an empty scan is what is meant.
            return "(SELECT NULL::TIMESTAMP AS event_ts, NULL::VARCHAR AS " \
                   "sensor_id, NULL::DOUBLE AS value WHERE false)"
        rendered = ", ".join("'" + f.replace("'", "''") + "'" for f in chosen)
        return f"read_parquet([{rendered}])"


def _arrow_table(payload: Sequence[Row] | Sequence[tuple]) -> pa.Table:
    items = [r if isinstance(r, Row) else Row(*r) for r in payload]
    return pa.table(
        {
            "event_ts": pa.array([r.event_ts for r in items], pa.timestamp("us")),
            "sensor_id": pa.array([r.sensor_id for r in items], pa.string()),
            "value": pa.array([r.value for r in items], pa.float64()),
        }
    )


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Scenario:
    """A declaration plus the independent SQL that says what it should produce.

    ``recompute_sql`` is written by hand and carries a ``{source}`` placeholder
    that the harness fills with a ``read_parquet`` over the eligible files. It
    is the ground truth ``PLAN.md``'s "compare the sink against a full recompute
    from source" asks for, and it is deliberately not generated by duckstream.
    """

    name: str
    aggregates: dict[str, str]
    key: tuple[str, ...]
    recompute_sql: str
    time_column: str | None = "event_ts"
    grain: str | None = "hour"
    lateness: str | None = None
    mode: str = "update"
    table: str = "marts.out"
    strategy: str | None = None
    memory_profile: str | None = None
    max_files_per_trigger: int | None = None
    max_rows_per_trigger: int | None = None
    on_failure: str = "quarantine"
    max_attempts: int = 5

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(self.key) + tuple(self.aggregates)

    def chunked(self, max_files: int | None) -> "Scenario":
        return replace(self, max_files_per_trigger=max_files)


#: The phase-1 model: one file source, one additive model, update-by-merge.
#: ``window_ts`` rather than ``PLAN.md``'s stale ``hour_ts`` -- the window
#: column is ``window_ts`` at every grain so the "merge key equals the window
#: grain key" invariant is checkable mechanically (build graph, "Resolved, W1").
ADDITIVE = Scenario(
    name="hourly_counts",
    aggregates={
        "n": "count(*)",
        "total": "sum(value)",
        "lo": "min(value)",
        "hi": "max(value)",
    },
    key=("window_ts", "sensor_id"),
    recompute_sql=(
        "SELECT date_trunc('hour', event_ts) AS window_ts,\n"
        "       sensor_id,\n"
        "       count(*) AS n,\n"
        "       sum(value) AS total,\n"
        "       min(value) AS lo,\n"
        "       max(value) AS hi\n"
        "  FROM {source}\n"
        " GROUP BY 1, 2"
    ),
)


def build_model(scenario: Scenario, landing: Landing | Path) -> Model:
    """The canonical ``Model``. Both doors are built from this one function.

    The YAML door does not get a hand-written document: it gets
    ``Model.to_config()`` of exactly this object, dumped to YAML. So the two
    doors cannot drift by a document being written differently from the object
    -- if they differ at all, the difference is in the loader or the engine,
    which is what the parity check is for.
    """
    root = landing.root if isinstance(landing, Landing) else Path(landing)
    return Model(
        name=scenario.name,
        source=FileSource(
            str(root),
            marker=MARKER,
            max_files_per_trigger=scenario.max_files_per_trigger,
            max_rows_per_trigger=scenario.max_rows_per_trigger,
        ),
        sink=TableSink(scenario.table, mode=scenario.mode),
        aggregates=dict(scenario.aggregates),
        key=list(scenario.key),
        time_column=scenario.time_column,
        grain=scenario.grain,
        lateness=scenario.lateness,
        strategy=scenario.strategy,
        memory_profile=scenario.memory_profile,
        on_failure=scenario.on_failure,
        max_attempts=scenario.max_attempts,
    )


def document_for(
    scenario: Scenario, landing: Landing | Path, *, catalog: Path, data_path: Path
) -> dict[str, Any]:
    """The YAML document, derived from the model rather than written by hand."""
    model = build_model(scenario, landing)
    return {
        "catalog": f"ducklake:{Path(catalog).as_posix()}",
        "data_path": Path(data_path).as_posix(),
        "settings": dict(SETTINGS),
        "models": [model.to_config()],
    }


# --------------------------------------------------------------------------
# A recording connection
# --------------------------------------------------------------------------


class RecordingConnection:
    """Delegates to a real DuckDB connection and keeps every SQL string.

    Used for exactly one thing the build graph asks for and that no behavioural
    assertion can reach: proving that the ``ON`` clause of the ``MERGE`` the
    engine *actually executed* contains no scalar subquery. ``CONTEXT.md`` 1.5
    measured that a ``(SELECT ...)`` there fails with ``Out of buffer`` against
    DuckLake, and only on the second merge, so the property is load-bearing and
    a string scan is cheap insurance against a regression.
    """

    def __init__(self, con: Any) -> None:
        self._con = con
        self.statements: list[str] = []

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        self.statements.append(sql)
        return self._con.execute(sql, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._con, name)

    # -- queries over what was recorded ---------------------------------

    def of_kind(self, keyword: str) -> list[str]:
        upper = keyword.upper()
        return [s for s in self.statements if s.lstrip().upper().startswith(upper)]

    @staticmethod
    def on_clause(merge_sql: str) -> str:
        """The text between ``ON`` and the first ``WHEN`` of a MERGE."""
        after = merge_sql.split("\n   ON ", 1)
        assert len(after) == 2, f"unexpected MERGE shape:\n{merge_sql}"
        return after[1].split("\n WHEN ", 1)[0]


# --------------------------------------------------------------------------
# One catalog behind one front door
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class RunSummary:
    """What one ``run`` did, in the terms both doors can report."""

    door: str
    returncode: int
    stdout: str
    committed: int | None = None
    batch_ids: tuple[int, ...] = ()
    rows_in: int | None = None
    empty_passes: int = 0


class World:
    """One DuckLake catalog, driven through one front door."""

    def __init__(
        self,
        door: str,
        root: Path,
        landing: Landing,
        scenario: Scenario,
    ) -> None:
        assert door in DOORS, door
        self.door = door
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.landing = landing
        self.scenario = scenario
        self.catalog = self.root / "catalog.ducklake"
        self.data_path = self.root / "lake_data"
        self.yaml_path = self.root / "models.yaml"
        self._wrote_yaml = False

    # -- the document ----------------------------------------------------

    @property
    def catalog_dsn(self) -> str:
        return f"ducklake:{self.catalog.as_posix()}"

    def write_yaml(self, scenario: Scenario | None = None) -> Path:
        document = document_for(
            scenario or self.scenario,
            self.landing,
            catalog=self.catalog,
            data_path=self.data_path,
        )
        self.yaml_path.write_text(
            yaml.safe_dump(document, sort_keys=False), encoding="utf-8"
        )
        self._wrote_yaml = True
        return self.yaml_path

    def ensure_yaml(self) -> Path:
        return self.yaml_path if self._wrote_yaml else self.write_yaml()

    def model(self) -> Model:
        return build_model(self.scenario, self.landing)

    # -- running ---------------------------------------------------------

    def run(
        self,
        *,
        once: bool = False,
        model: str | None = None,
        con: Any | None = None,
        expect_failure: bool = False,
    ) -> RunSummary:
        """One pass through this world's front door.

        ``con`` is for the rare test that needs to watch the statements the
        engine issues; it is only honoured on the Python door, because the CLI
        owns its own connection and that is exactly the property being tested.
        """
        if self.door == "python":
            return self._run_python(
                once=once, model=model, con=con, expect_failure=expect_failure
            )
        assert con is None, "the yaml/CLI door owns its own connection"
        return self._run_cli(once=once, model=model, expect_failure=expect_failure)

    def _run_python(
        self,
        *,
        once: bool,
        model: str | None,
        con: Any | None,
        expect_failure: bool = False,
    ) -> RunSummary:
        from duckstream import AvailableNow, Engine, Once
        from duckstream.errors import BatchFailed

        owned = con is None
        connection = duckdb.connect() if owned else con
        try:
            engine = Engine(
                connection,
                catalog=str(self.catalog),
                data_path=str(self.data_path),
                settings=dict(SETTINGS),
            )
            engine.add(self.model())
            try:
                report = engine.run(
                    trigger=Once() if once else AvailableNow(), model=model
                )
            except BatchFailed as failure:
                # A scenario about failure needs the report, not the traceback.
                # Anything that did *not* ask for a failure still gets one.
                if not expect_failure:
                    raise
                report = failure.report
        finally:
            if owned:
                connection.close()
        committed = [r for r in report if r.committed]
        return RunSummary(
            door=self.door,
            returncode=0,
            stdout="",
            committed=len(committed),
            batch_ids=tuple(r.batch_id for r in committed if r.batch_id is not None),
            rows_in=sum(r.rows_in or 0 for r in committed),
            empty_passes=sum(1 for r in report if r.is_empty),
        )

    def _run_cli(
        self, *, once: bool, model: str | None, expect_failure: bool = False
    ) -> RunSummary:
        """The real CLI, in process.

        ``duckstream.cli.main`` is the exact code path ``python -m duckstream
        run`` takes -- argument parsing, ``load_config``, ``Engine.from_document``,
        ``engine.run`` -- minus the ~235 ms of interpreter start ``CONTEXT.md``
        1.8 measured. ``tests/conformance/test_cli_contract.py`` pays that cost
        in a real subprocess where the process boundary is the thing under test;
        paying it on every scenario would buy nothing and cost minutes.
        """
        from duckstream.cli import main

        argv = ["run", "--config", str(self.ensure_yaml())]
        if once:
            argv.append("--once")
        if model:
            argv += ["--model", model]
        out, err = io.StringIO(), io.StringIO()
        code = main(argv, out=out, err=err)
        if expect_failure:
            assert code != 0, (
                "the CLI exited 0 for a run expected to fail, so a broken "
                "pipeline would look healthy to cron"
            )
            return RunSummary(
                door=self.door, returncode=code,
                stdout=out.getvalue() + err.getvalue(),
            )
        assert code == 0, f"cli run failed ({code}):\n{err.getvalue()}"
        return RunSummary(
            door=self.door, returncode=code, stdout=out.getvalue() + err.getvalue()
        )

    # -- inspecting ------------------------------------------------------

    @contextlib.contextmanager
    def connect(self) -> Iterator[Any]:
        """A fresh, attached, read-side connection. Always closed."""
        con = duckdb.connect()
        try:
            lakemod.attach_lake(
                con,
                str(self.catalog),
                data_path=str(self.data_path),
                settings=dict(SETTINGS),
            )
            yield con
        finally:
            con.close()

    @property
    def exists(self) -> bool:
        return self.catalog.exists()

    def rows(self, *, at: int | None = None) -> list[tuple] | None:
        """The mart, normalised. ``at`` time-travels to a snapshot id.

        ``None`` -- distinct from ``[]`` -- when the mart does not exist yet.
        A table that has not been created and a table with no rows are different
        states, and a parity failure between the two should say so rather than
        surface as a catalog error from whichever side happened to be read
        first.
        """
        with self.connect() as con:
            try:
                return normalise(
                    _fetch_mart(
                        con,
                        self.scenario.table,
                        at=at,
                        columns=self.scenario.columns,
                    )
                )
            except duckdb.Error:
                return None

    def snapshot_count(self) -> int:
        with self.connect() as con:
            return lakemod.snapshot_count(con, ALIAS)

    def snapshot_ids(self) -> list[int]:
        with self.connect() as con:
            return [s["snapshot_id"] for s in lakemod.snapshots(con, ALIAS)]

    def data_file_count(self) -> int:
        with self.connect() as con:
            return lakemod.data_file_count(con, ALIAS, self.scenario.table)

    def offset_files(self) -> list[str]:
        """Relative paths the committed offset records as consumed."""
        with self.connect() as con:
            store = DuckLakeStateStore(STATE_SCHEMA, catalog=ALIAS)
            offset = store.load_offset(con, self.scenario.name)
        return sorted((offset or {}).get("consumed", {}))

    def quarantined(self) -> list[dict[str, Any]]:
        """Batches this world gave up on, straight from the catalog."""
        with self.connect() as con:
            store = DuckLakeStateStore(STATE_SCHEMA, catalog=ALIAS)
            return store.quarantined(con, self.scenario.name)

    def status(self) -> Any:
        """What ``duckstream status`` would report for this world."""
        from duckstream.metrics import status_for

        with self.connect() as con:
            store = DuckLakeStateStore(STATE_SCHEMA, catalog=ALIAS)
            return status_for(con, store, self.model())

    def batch_history(self) -> list[dict[str, Any]]:
        with self.connect() as con:
            store = DuckLakeStateStore(STATE_SCHEMA, catalog=ALIAS)
            return store.batch_history(con, self.scenario.name)

    def watermark(self) -> dt.datetime | None:
        """The committed watermark, read back the way a restart would read it."""
        with self.connect() as con:
            store = DuckLakeStateStore(STATE_SCHEMA, catalog=ALIAS)
            return store.load_watermark(con, self.scenario.name)

    def drop_counts(self) -> list[tuple[int, int | None, int | None]]:
        """``(batch_id, rows_late, rows_undated)`` per committed batch.

        Durable, not in-process: read out of ``duckstream.batches`` in the
        catalog, because ``PLAN.md`` asks for late data to be counted and a
        count that only ever existed in a return value has not been.
        """
        return [
            (row["batch_id"], row["rows_late"], row["rows_undated"])
            for row in self.batch_history()
        ]

    def open_windows(self) -> list[tuple] | None:
        """The sealed-append accumulator, or ``None`` if this model has none."""
        with self.connect() as con:
            try:
                return normalise(
                    con.execute(
                        f"SELECT * FROM {self.scenario.table}__open_windows"
                    ).fetchall()
                )
            except duckdb.Error:
                return None

    def recompute(self, files: Sequence[str] | None = None) -> list[tuple]:
        """Ground truth: the scenario's own SQL over the eligible source files."""
        with self.connect() as con:
            return normalise(_recompute(con, self.scenario, self.landing, files))

    def snapshot_walk(self) -> list[dict[str, Any]]:
        """Every snapshot in which the mart exists, with its own ground truth.

        This is what makes exactly-once *inspectable* rather than inferred, and
        it is the reason ``PLAN.md`` calls time travel "a genuine asset for this
        framework". One trigger is one snapshot (``CONTEXT.md`` 1.4), and the
        sink rows and the source offset land in the *same* snapshot, so at every
        point in history the question "does the mart equal a full recompute of
        exactly the files this catalog had consumed by then?" has an answer that
        can be read straight out of the catalog.
        """
        walk: list[dict[str, Any]] = []
        with self.connect() as con:
            store = DuckLakeStateStore(STATE_SCHEMA, catalog=ALIAS)
            for snapshot in lakemod.snapshots(con, ALIAS):
                sid = snapshot["snapshot_id"]
                try:
                    mart = normalise(
                        _fetch_mart(
                            con,
                            self.scenario.table,
                            at=sid,
                            columns=self.scenario.columns,
                        )
                    )
                except duckdb.Error:
                    # The mart does not exist yet at this snapshot: catalog
                    # creation, the state schema, the marts schema. Not a gap in
                    # the history, just history from before there was a mart.
                    continue
                consumed = _offset_at(con, store, self.scenario.name, sid)
                expected = normalise(
                    _recompute(
                        con,
                        self.scenario,
                        self.landing,
                        [self.landing.absolute(rel) for rel in consumed],
                    )
                )
                walk.append(
                    {
                        "snapshot_id": sid,
                        "mart": mart,
                        "consumed": consumed,
                        "expected": expected,
                    }
                )
        return walk


def _fetch_mart(
    con: Any,
    table: str,
    *,
    at: int | None = None,
    columns: Sequence[str] | None = None,
) -> list[tuple]:
    """The mart's **declared** columns, not ``SELECT *``.

    A ``sufficient_statistics`` model carries its ``(n, mean, M2)`` state in
    real columns of the same table, so ``SELECT *`` returns those too and a
    ground-truth diff against a recompute would be comparing different shapes.
    The declared columns are what the model promises and what the recompute
    produces, so they are what the diff is about. That the state columns exist
    and fold correctly is asserted separately, where it is the actual subject.
    """
    reference = table if at is None else f"{table} AT (VERSION => {int(at)})"
    projection = (
        "*" if not columns else ", ".join(f'"{c}"' for c in columns)
    )
    return con.execute(f"SELECT {projection} FROM {reference}").fetchall()


def _offset_at(con: Any, store: DuckLakeStateStore, model: str, sid: int) -> list[str]:
    """Files the committed offset covered as of snapshot ``sid``.

    The offsets table lives in the same catalog as the mart, which is not an
    implementation detail: ``CONTEXT.md`` 1.9 measured that one transaction
    cannot write two attached databases, so the offset *has* to share the
    snapshot with the rows it checkpoints. That is what makes this query
    meaningful -- both sides of the comparison are read at the same instant of
    catalog history.
    """
    sql = (
        f"SELECT offset_json FROM {store.offsets_table} AT (VERSION => {int(sid)}) "
        f"WHERE model_name = ? ORDER BY batch_id DESC LIMIT 1"
    )
    try:
        row = con.execute(sql, [model]).fetchone()
    except duckdb.Error:
        return []
    if not row or not row[0]:
        return []
    return sorted(json.loads(row[0]).get("consumed", {}))


def _recompute(
    con: Any,
    scenario: Scenario,
    landing: Landing,
    files: Sequence[str] | None = None,
) -> list[tuple]:
    source = landing.source_expression(files)
    return con.execute(scenario.recompute_sql.format(source=source)).fetchall()


# --------------------------------------------------------------------------
# Both doors, in lockstep
# --------------------------------------------------------------------------


class Parity:
    """Two :class:`World` objects over one landing tree, kept in agreement.

    Every method that advances the pipeline advances *both* doors and then
    asserts they still agree. A scenario written against this object therefore
    cannot silently lose its second door: forgetting to exercise the YAML path
    is not possible, because the harness does it whether asked to or not.
    """

    def __init__(self, root: Path, landing: Landing, scenario: Scenario) -> None:
        self.root = Path(root)
        self.landing = landing
        self.scenario = scenario
        self.worlds: dict[str, World] = {
            door: World(door, self.root / door, landing, scenario) for door in DOORS
        }
        self.runs = 0
        self.committed_batches = 0
        #: Rows landed since the last :meth:`run`, then one entry per run. An
        #: event-time scenario needs the batch boundaries to reproduce the
        #: watermark trajectory, and inferring them afterwards would be
        #: guessing at exactly the thing under test.
        self.batches: list[list[Row]] = []
        self._pending: list[Row] = []

    # -- input -----------------------------------------------------------

    def land(self, name: str, payload: Sequence[Row] | Sequence[tuple], **kw) -> Path:
        """One drop, visible to both doors -- they share the landing tree."""
        self._pending.extend(
            r if isinstance(r, Row) else Row(*r) for r in payload
        )
        return self.landing.drop(name, payload, **kw)

    # -- advancing -------------------------------------------------------

    def run(
        self, *, once: bool = False, expect_failure: bool = False
    ) -> dict[str, RunSummary]:
        self.batches.append(list(self._pending))
        self._pending = []
        summaries = {
            door: w.run(once=once, expect_failure=expect_failure)
            for door, w in self.worlds.items()
        }
        self.runs += 1
        python = summaries["python"]
        if python.committed:
            self.committed_batches += python.committed
        self.assert_agree()
        return summaries

    # -- assertions ------------------------------------------------------

    def assert_agree(self) -> None:
        """The parity guarantee: identical output through both front doors."""
        reference_door, *others = DOORS
        reference = self.worlds[reference_door]
        expected_rows = reference.rows()
        expected_offsets = reference.offset_files()
        expected_snapshots = reference.snapshot_count()
        for door in others:
            world = self.worlds[door]
            assert world.rows() == expected_rows, (
                f"front doors disagree on {self.scenario.table!r}:\n"
                f"  {reference_door}: {expected_rows}\n"
                f"  {door}: {world.rows()}"
            )
            assert world.offset_files() == expected_offsets, (
                f"front doors disagree on the committed offset:\n"
                f"  {reference_door}: {expected_offsets}\n"
                f"  {door}: {world.offset_files()}"
            )
            assert world.snapshot_count() == expected_snapshots, (
                f"front doors disagree on snapshot count: "
                f"{reference_door}={expected_snapshots}, "
                f"{door}={world.snapshot_count()}"
            )
            assert world.watermark() == reference.watermark(), (
                f"front doors disagree on the committed watermark:\n"
                f"  {reference_door}: {reference.watermark()}\n"
                f"  {door}: {world.watermark()}"
            )
            assert world.drop_counts() == reference.drop_counts(), (
                f"front doors disagree on what they dropped:\n"
                f"  {reference_door}: {reference.drop_counts()}\n"
                f"  {door}: {world.drop_counts()}"
            )

    def assert_matches_ground_truth(self) -> list[tuple]:
        """Both doors equal an independent full recompute from source."""
        expected = self.worlds["python"].recompute()
        for door, world in self.worlds.items():
            actual = world.rows()
            assert actual is not None, f"{door} door never created {self.scenario.table}"
            assert same_rows(actual, expected), (
                f"{door} door differs from a full recompute:\n"
                f"  sink:      {actual}\n"
                f"  recompute: {expected}"
            )
        return expected

    def assert_snapshot_history_consistent(self) -> list[dict[str, Any]]:
        """At *every* snapshot, the mart equals a recompute of what it had read.

        Stronger than checking the final state, and the difference is the point:
        a final state can be right by luck after a sequence of kills, but a
        history in which every intermediate snapshot is also exactly a full
        recompute of its own committed offset cannot be.
        """
        walks: list[dict[str, Any]] = []
        for door, world in self.worlds.items():
            walk = world.snapshot_walk()
            assert walk, f"{door}: no snapshot contains the mart"
            for step in walk:
                assert same_rows(step["mart"], step["expected"]), (
                    f"{door} door, snapshot {step['snapshot_id']}: the mart is not "
                    f"a full recompute of the {len(step['consumed'])} file(s) the "
                    f"offset had consumed at that snapshot.\n"
                    f"  consumed:  {step['consumed']}\n"
                    f"  mart:      {step['mart']}\n"
                    f"  recompute: {step['expected']}"
                )
            walks.append({"door": door, "walk": walk})
        return walks

    def assert_reached_matched_branch(self) -> None:
        """At least two batches committed, so ``WHEN MATCHED`` was reached.

        ``CONTEXT.md`` 1.5's ``Out of buffer`` appeared only on the *second*
        merge -- the first one to take the matched branch -- so a single-batch
        conformance run would have missed it entirely. Every scenario asserts
        this before it is allowed to claim it verified anything.
        """
        assert self.committed_batches >= 2, (
            f"scenario committed only {self.committed_batches} batch(es); the "
            f"MERGE never took its WHEN MATCHED branch, which is the branch "
            f"CONTEXT.md 1.5's DuckLake failure hid behind"
        )


# --------------------------------------------------------------------------
# An independent event-time reference
# --------------------------------------------------------------------------


#: What :func:`replay` can compute. Deliberately not read off the scenario --
#: a reference that derived its arithmetic from the model's SQL would be
#: comparing duckstream to itself, which is the failure ``harness``'s header
#: warns about for ``recompute_sql``.
REFERENCE_AGGREGATES = ("n", "total", "lo", "hi")

_GRAIN_SECONDS = {"minute": 60, "hour": 3600, "day": 86400}


def _floor(moment: dt.datetime, grain: str) -> dt.datetime:
    """Window start, by epoch arithmetic rather than by field truncation.

    Deliberately a different method from both ``date_trunc`` and
    :func:`duckstream.windows.floor_to_grain`, which agree with each other by
    replacing fields. Flooring the epoch second modulo the grain gets there a
    different way, so the two implementations failing identically would take a
    coincidence rather than a shared assumption.
    """
    epoch = int(moment.replace(tzinfo=dt.timezone.utc).timestamp())
    seconds = _GRAIN_SECONDS[grain]
    start = epoch - epoch % seconds
    return dt.datetime.fromtimestamp(start, dt.timezone.utc).replace(tzinfo=None)


@dataclass(frozen=True)
class Replay:
    """What :func:`replay` says should be true after the last batch."""

    mart: list[tuple]
    """Expected sink contents: every window for ``update``, sealed only for
    ``append``."""

    open_windows: list[tuple]
    """Windows still accumulating. Empty for ``update``, which never evicts."""

    watermark: dt.datetime | None
    late: list[int]
    undated: list[int]


def replay(
    batches: Sequence[Sequence[Row]],
    *,
    grain: str,
    lateness: dt.timedelta,
    mode: str = "update",
) -> Replay:
    """The event-time contract, implemented again, in plain Python.

    This is ground truth for a scenario with a lateness horizon. The plain
    ``recompute_sql`` cannot serve: what the sink should hold depends on the
    *watermark trajectory* -- which rows were dropped as late, and which
    windows had sealed -- and that is a function of the batch boundaries, not
    of the file contents. So the contract is written out a second time here,
    from ``PLAN.md``'s description rather than from duckstream's code:

    * the watermark is ``max(event time seen) - lateness`` and never regresses;
    * a batch is judged against the watermark **committed before it**, so a row
      is late when its window ``[ws, ws + grain)`` had already ended by then;
    * a row with no event time belongs to no window;
    * a window seals when the watermark reaches its end, and in ``append`` mode
      it moves to the sink exactly once at that moment.

    Aggregates are fixed to :data:`REFERENCE_AGGREGATES` -- count, sum, min,
    max of ``value`` grouped by window and sensor -- because a reference that
    interpreted the model's own SQL would not be independent of it.
    """
    assert mode in ("update", "append"), mode
    watermark: dt.datetime | None = None
    accumulated: dict[tuple, dict] = {}
    emitted: list[tuple] = []
    late: list[int] = []
    undated: list[int] = []

    for batch in batches:
        rows = [r if isinstance(r, Row) else Row(*r) for r in batch]
        cutoff = None if watermark is None else watermark - _interval(grain)
        dated = [r for r in rows if r.event_ts is not None]
        undated.append(len(rows) - len(dated))
        if cutoff is None:
            kept, dropped = dated, []
        else:
            kept = [r for r in dated if _floor(r.event_ts, grain) > cutoff]
            dropped = [r for r in dated if _floor(r.event_ts, grain) <= cutoff]
        late.append(len(dropped))

        for row in kept:
            key = (_floor(row.event_ts, grain), row.sensor_id)
            cell = accumulated.setdefault(
                key, {"n": 0, "total": 0.0, "lo": None, "hi": None}
            )
            cell["n"] += 1
            cell["total"] += row.value
            cell["lo"] = row.value if cell["lo"] is None else min(cell["lo"], row.value)
            cell["hi"] = row.value if cell["hi"] is None else max(cell["hi"], row.value)

        # The maximum is taken over the whole batch, dropped rows included: the
        # watermark tracks what has been *observed*, and a late row was still
        # observed. (It cannot raise the watermark anyway -- it is older than
        # what already moved it -- but saying so is not the same as relying on
        # it.)
        newest = max((r.event_ts for r in dated), default=None)
        if newest is not None:
            candidate = newest - lateness
            watermark = candidate if watermark is None else max(watermark, candidate)

        if mode == "append" and watermark is not None:
            seal = watermark - _interval(grain)
            for key in sorted(
                (k for k in accumulated if k[0] <= seal),
                key=lambda k: (k[0], k[1] is None, str(k[1])),
            ):
                cell = accumulated.pop(key)
                emitted.append((key[0], key[1], cell["n"], cell["total"], cell["lo"], cell["hi"]))

    remaining = [
        (key[0], key[1], cell["n"], cell["total"], cell["lo"], cell["hi"])
        for key, cell in accumulated.items()
    ]
    return Replay(
        mart=normalise(emitted if mode == "append" else remaining),
        open_windows=normalise(remaining if mode == "append" else []),
        watermark=watermark,
        late=late,
        undated=undated,
    )


def _interval(grain: str) -> dt.timedelta:
    return dt.timedelta(seconds=_GRAIN_SECONDS[grain])


# --------------------------------------------------------------------------
# Subprocesses
# --------------------------------------------------------------------------


def spawn(args: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess:
    """Run the venv interpreter with ``args``, from the repository root."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(REPO_ROOT), env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    env.pop("PYTHONWARNINGS", None)
    return subprocess.run(
        [sys.executable, *args],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        env=env,
        timeout=300,
        **kwargs,
    )


def kill_run(
    world: World,
    *,
    fault: str | None = None,
    nth: int = 1,
    once: bool = False,
) -> subprocess.CompletedProcess:
    """Run one engine pass in a **real child process** that may really die.

    ``fault`` arms one of :data:`duckstream.FAULT_POINTS` with a hook that calls
    ``os._exit(9)`` on its ``nth`` firing. ``os._exit`` skips every ``finally``,
    every atexit hook and every destructor, so DuckDB gets no chance to close
    the catalog cleanly -- which is the point. A mocked exception would test the
    engine's ``except`` block; this tests the guarantee.
    """
    spec = {
        "door": world.door,
        "catalog": str(world.catalog),
        "data_path": str(world.data_path),
        "landing": str(world.landing.root),
        "yaml": str(world.ensure_yaml()) if world.door == "yaml" else None,
        "scenario": _scenario_payload(world.scenario),
        "fault": fault,
        "nth": nth,
        "once": once,
    }
    return spawn([str(FAULT_CHILD), json.dumps(spec)])


def _scenario_payload(scenario: Scenario) -> dict[str, Any]:
    return {
        "name": scenario.name,
        "aggregates": dict(scenario.aggregates),
        "key": list(scenario.key),
        "recompute_sql": scenario.recompute_sql,
        "time_column": scenario.time_column,
        "grain": scenario.grain,
        "lateness": scenario.lateness,
        "mode": scenario.mode,
        "table": scenario.table,
        "strategy": scenario.strategy,
        "memory_profile": scenario.memory_profile,
        "max_files_per_trigger": scenario.max_files_per_trigger,
        "max_rows_per_trigger": scenario.max_rows_per_trigger,
        "on_failure": scenario.on_failure,
        "max_attempts": scenario.max_attempts,
    }


def scenario_from_payload(payload: Mapping[str, Any]) -> Scenario:
    data = dict(payload)
    data["key"] = tuple(data["key"])
    return Scenario(**data)
