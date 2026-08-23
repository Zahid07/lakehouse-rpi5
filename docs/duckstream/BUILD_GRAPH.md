# duckstream — build graph (manager working file)

Not a design document. `PLAN.md` says what to build and `CONTEXT.md` says why.
This file says **who builds which files, in what order, and what "done" means** so
that parallel work does not collide and interfaces do not drift.

The manager (main session) owns this file. Subagents read it and never edit it.

## Where the build stands (2026-08-23)

**787 unit tests passing**, one skip that is correct on Windows (it cannot hold two
files differing only by case, so the POSIX half of the case-sensitivity pair
skips). ~17,600 lines across 19 package modules and the suites.

W1 through W3 are **done and reviewed**. W4 — the conformance suite — is the only
task still running, and it is the one that decides whether phase 1 is finished:
`PLAN.md`'s definition of done is a fault-injection test, not a feature list.
Until it is green, exactly-once is a well-evidenced design intent rather than a
demonstrated property.

**Nothing is committed yet.**

What the review process actually caught, since its value is the reason it costs
what it does:

- **W1** shipped green and was **failed on two blockers**: an aggregate expression
  could smuggle `sum((SELECT max(v) FROM other_table))` past validation and then
  execute against a table outside the batch, and a YAML-shaped
  `aggregates: {n: [count(*)]}` crashed with a raw `TypeError` rather than a
  validation error.
- **W2a** shipped with two *silent* wrong answers: `pattern: "**/*.parquet"`
  skipped every root-level file, and a Windows case-only rename re-read a file and
  double-counted its rows.
- **W2b** discovered §1.9 and §1.10 — the transaction/attached-database limit, and
  a state layer running six times slower than the floor, which it then made 4.1x
  faster.
- **W2c** retracted its own idempotency claim: update mode double-counts a
  replayed batch, so exactly-once comes only from the offset transaction. It also
  shipped a type-mismatch hole that aborted the engine's transaction mid-merge.

Three constraints in `CONTEXT.md` (§1.8, §1.9, §1.10) were measured during this
build and are not derivable from the original document.

## Environment (verified on this box, 2026-08-22)

- `.venv/` at repo root, Python 3.14.3. Interpreter: `.venv/Scripts/python.exe`.
- `duckdb==1.5.5` (pinned — constraints 1.5 and 1.7 are version-sensitive),
  `pyarrow 25.0.1`, `numpy 2.5.2`, `pyyaml 6.0.3`, `pytest 9.1.1`.
- `INSTALL ducklake` / `LOAD ducklake` works; the extension is present locally.
- Re-verified from `CONTEXT.md`: with `ducklake_default_data_inlining_row_limit = 0`
  a 3-row insert writes **1** parquet file; `BEGIN; INSERT; INSERT; COMMIT` advances
  `ducklake_snapshots()` by exactly **1**.
- `json_serialize_sql('SELECT <expr>')` returns a JSON AST carrying
  `class: FUNCTION`, `function_name` and `distinct` — this is how aggregates get
  classified.

## Hard rules for every task

1. **`duckstream/` imports nothing from this repo** (`run_pipeline.py`, `utils/`,
   `subscriber.py`, `realtime_queue_worker.py`). It gets extracted to its own repo later.
2. **No `fcntl`, no POSIX-only imports.** The package must import on Windows.
3. **Own only your files**, listed per task below. Touching a file owned by another
   task is a failure, even a trivial edit. `duckstream/__init__.py` is owned by W3.
4. **Tests run against DuckLake**, with at least two batches so the `WHEN MATCHED`
   merge branch is reached. An in-memory DuckDB path may exist for speed but is
   never the only gate.
5. **Never put a scalar subquery in a MERGE or JOIN condition against DuckLake**
   (constraint 1.5 — `Out of buffer`, and only on the second batch). Compute bounds
   in Python and inline them as literals.
6. Always `SET ducklake_default_data_inlining_row_limit = 0`.
7. Run tests with `.venv/Scripts/python.exe -m pytest`. Do not `pip install`
   anything new without asking the manager.
8. No git commits, no `git add`. The manager commits.

## Frozen interfaces

Every task codes against exactly these. Do not rename, do not add positional
parameters, do not "improve" a signature — raise it with the manager instead.

```python
# duckstream/protocols.py  (owned by W1)
Offset = dict[str, Any]                    # JSON-serialisable, source-defined

@dataclass(frozen=True)
class BatchLimits:
    max_rows_per_trigger: int | None = None
    max_files_per_trigger: int | None = None

@dataclass(frozen=True)
class BatchPlan:
    start: Offset | None                   # offset this batch resumes from
    end: Offset                            # offset once this batch commits
    payload: dict[str, Any]                # source-specific, JSON-serialisable
    is_empty: bool
    has_more: bool                         # limits truncated the batch

@dataclass(frozen=True)
class BatchContext:
    model_name: str
    batch_id: int
    plan: BatchPlan

class Source(Protocol):
    type_name: ClassVar[str]                       # registry name, e.g. "file"
    def latest_offset(self) -> Offset: ...
    def plan(self, start: Offset | None, end: Offset,
             limits: BatchLimits) -> BatchPlan: ...
    def bind(self, con, plan: BatchPlan) -> str: ...   # registers a view, returns its name
    def to_config(self) -> dict[str, Any]: ...          # round-trip; includes "type"

class Sink(Protocol):
    type_name: ClassVar[str]
    def ensure(self, con, model: "Model") -> None: ...  # DDL, idempotent
    def write(self, con, batch_view: str, model: "Model",
              ctx: BatchContext) -> None: ...
    def to_config(self) -> dict[str, Any]: ...

class StateStore(Protocol):
    def ensure(self, con) -> None: ...
    def begin(self, con) -> None: ...
    def load_offset(self, con, model_name: str) -> Offset | None: ...
    def commit(self, con, offsets: dict[str, Offset],
               watermarks: dict[str, Any]) -> None: ...
```

`Model` (owned by W1), keyword-only construction:

```python
@dataclass
class Model:
    name: str
    source: Source
    sink: Sink
    aggregates: dict[str, str]        # output column -> SQL aggregate expression
    key: list[str]                    # sink merge key
    time_column: str | None = None
    grain: str | None = None          # "minute" | "hour" | "day" | None
    strategy: str | None = None       # None = infer from tier
    memory_profile: str | None = None # "streaming" | "materialising"
    udfs: list[str] = field(default_factory=list)   # dotted paths
    limits: BatchLimits = BatchLimits()
    def validate(self) -> None        # raises ModelValidationError
    @property
    def tier(self) -> Tier
    @property
    def resolved_strategy(self) -> str
    def to_config(self) -> dict[str, Any]
```

Vocabulary, fixed:

| Concept | Values |
|---|---|
| `Tier` | `additive`, `sufficient_statistics`, `non_foldable` |
| strategy | `delta_merge`, `sufficient_statistics`, `recompute_window` |
| grain | `minute`, `hour`, `day` |
| window column | `window_ts` — always, whatever the grain |
| sink modes | `append`, `update` |

**Resolved, W1:** `PLAN.md`'s worked examples write `key: [hour_ts, sensor_id]`.
That is stale. The window column is `window_ts` at every grain, so the
"merge key must equal the window grain key" invariant is checkable mechanically
instead of by convention. Fixtures copied from `PLAN.md` must be corrected to
`window_ts` or they will be refused at load.

**Resolved, W1 review:** tier two (`sufficient_statistics`) is exactly a **bare
single call** to `avg`/`mean`/`stddev*`/`var*`/`variance`/`corr`/`covar*` — the
list `PLAN.md` gives. Anything wrapped in a scalar expression (`sum(a)/count(*)`,
`cast(sum(a) AS INTEGER)`, `max(a)+max(b)`) is `non_foldable`, because
"store sum, sum_sq, count and derive on read" has no meaning for most of them.
Non-foldable means recompute-the-window: always correct, merely slower.

**Resolved, manager measurement (now `CONTEXT.md` §1.8):** a DuckLake trigger that
writes nothing costs ~1.3 ms; one that writes anything pays ~15 ms of commit. So
**the engine must not open a transaction or write a checkpoint for an empty
batch** — it returns early. W3 implements it, W4 asserts it: an idle pass adds
zero snapshots. Budget ~20 ms per idle trigger and ~40 ms per small one, plus
~235 ms of process start under cron.

**Resolved, W2b (now `CONTEXT.md` §1.9 and §1.10):** sink and state must live in
the **same** catalog, because a transaction cannot span attached databases — so
both are in DuckLake, and the plain-DuckDB store is usable only when the sink is
also plain DuckDB. Per-trigger state is **append-only**, read back with
`ORDER BY batch_id DESC LIMIT 1`; a matching DuckLake `DELETE` costs ~26 ms
because it writes a tombstone, and guarding it with an existence probe measured
as a net regression. `ducklake_snapshots()` must not be `SELECT *`-ed — its
`TIMESTAMP WITH TIME ZONE` column needs `pytz`, which is not installed; use
`lake.snapshots()` / `lake.snapshot_count()`.

**Resolved, W2c — all six ratified:** the phase-3 refusal fires in `ensure` as
well as `write`, keyed off `resolved_strategy` rather than `tier`, so an additive
model that explicitly asks for `recompute_window` is refused instead of silently
folded; `mode="append"` accepts any tier because appending a per-batch value
involves no fold; existence is checked via `duckdb_columns()` rather than a probe
`SELECT`, since a failing statement would abort the engine's transaction;
`to_config` always emits `mode`. **And the honest one: update mode is not
idempotent by itself** — the merge key keeps the row set stable, but an additive
fold double-counts a replayed batch. Exactly-once comes from the engine's offset
transaction, nowhere else. A test asserts the doubling rather than hiding it.

Errors: `DuckstreamError` base; `ModelValidationError` and `ConfigError` subclass
it. All in `duckstream/errors.py` (W1).

## Tasks

Status: `todo` / `running` / `review` / `done` / `blocked`.
Every task ran a feedback agent after delivery; `done` means it also
cleared that review, including any fix round the review forced.

| id | task | depends on | owns | status |
|---|---|---|---|---|
| W1 | packaging, errors, protocols, aggregates classifier, `Model` + load-time validation | — | `pyproject.toml`, `duckstream/errors.py`, `duckstream/protocols.py`, `duckstream/aggregates.py`, `duckstream/model.py`, `tests/unit/test_aggregates.py`, `tests/unit/test_model_validation.py` | **done** — 254 tests; review returned FAIL on 2 blockers, fixed |
| W2a | file source + offsets | W1 | `duckstream/offsets.py`, `duckstream/sources/__init__.py`, `duckstream/sources/files.py`, `tests/unit/test_file_source.py` | **done** — 74 tests; review PASS + 4 defects, fixed |
| W2b | DuckLake session/settings + state store | W1 | `duckstream/lake.py`, `duckstream/state.py`, `tests/unit/test_state.py`, `tests/unit/test_lake.py` | **done** — 131 tests; found §1.9 and §1.10, 4.1x faster |
| W2c | table sink (append / update-by-merge) | W1 | `duckstream/sinks/__init__.py`, `duckstream/sinks/table.py`, `duckstream/sql.py`, `tests/unit/test_table_sink.py` | **done** — 157 tests; review PASS + 1 defect, fixed |
| W2d | registry + YAML config loader | W1 | `duckstream/registry.py`, `duckstream/config.py`, `tests/unit/test_config.py`, `tests/unit/test_registry.py` | **done** — 112 tests; review PASS, no defects |
| W3 | engine (trigger loop, one txn per trigger) + CLI + package exports | W2a–d | `duckstream/engine.py`, `duckstream/trigger.py`, `duckstream/cli.py`, `duckstream/__main__.py`, `duckstream/__init__.py`, `tests/unit/test_engine.py` | **done** — 38 tests |
| W4 | conformance suite: fault injection, exactly-once, snapshot accounting, front-door parity | W3 | `tests/conformance/**`, `tests/conftest.py` | **running** |
| W5 | package README + status doc, final sweep | W4 | `duckstream/README.md`, `docs/duckstream/STATUS.md` | todo |

Each task is followed by a **feedback agent** that re-reads `PLAN.md`, `CONTEXT.md`
and this file, then verifies the delivered work against that task's definition of
done. It reports PASS or FAIL with specific defects; it does not fix them.

**Resolved, W3 — all six ratified.** `udfs` dotted paths resolve to a **registrar**,
not the computation: either `obj.register(con)` or a callable whose first parameter
is `con`/`conn`/`connection`. A bare computation function is refused, because a
dotted path cannot carry the SQL name, argument types and return type that
`create_function` needs (and Arrow mode for `LIST`, §1.2). Phase 3's `udf.py`
ships ready-made registrars against this contract. `rows_out` stays NULL until
`metrics.py` — obtaining it would mean running the aggregation twice or coupling
to a sink internal. The state store is constructed catalog-qualified;
`sink.ensure`/`state.ensure` run once at prepare time outside any trigger
transaction (idempotent DDL adds zero snapshots, measured); batch ids are memoised
per model per process, advanced only on successful commit.

Fault points, for W4: `after_plan`, `after_bind`, `after_sink_write`,
`before_commit`, `after_commit`, via `engine.faults.install(point, hook)`.
Nothing in config or the CLI can arm one.

**Resolved, W2c fix round — ratified.** A target-table type mismatch is caught in
`write()` before any statement runs, not in `ensure()`, which structurally cannot
know the aggregation's result types. Compatibility is decided by **DuckDB's own
lattice** — `can_cast_implicitly` in *either* direction — rather than a matrix
maintained here. **Narrowing is deliberately allowed**: `count(*)` is `BIGINT` and
`sum(BIGINT)` widens to `HUGEINT`, so a sensible hand-made `total BIGINT, n
INTEGER` table would otherwise be refused despite working, and an overflow
announces itself where the §4 bug class is silent. The check is scoped to
`mode="update"` **because it was measured**: `INSERT INTO t(n VARCHAR) SELECT
count(*)` succeeds today and stores `'7'`, since an INSERT applies an assignment
cast a fold cannot — so refusing append would have broken a working pipeline to
prevent a failure that does not occur. A target column the model does not write is
accepted and left NULL; the caveat, documented, is that a *renamed* aggregate
leaves its predecessor behind and wants a migration rather than a redeploy.

**Resolved, W2a fix round — all three ratified.** `base_dir` injection is
**signature-driven**, not name-driven: any registry component declaring a
`base_dir` parameter receives the config file's directory, so a user component
opts in by declaring it and an explicit YAML value still wins. `FileSource.__eq__`
compares `to_config()` and therefore **ignores `base_dir`** — two sources reading
different trees can compare equal. That is the price of `to_config()` not emitting
`base_dir`, which the config round-trip requires; equality means "same declared
configuration", not "same resolved location". Conformance parity must compare
**output**, never source equality. Pattern matching stays **case-sensitive on both
platforms** even though offset keys fold on Windows: discovery must not differ
between the dev box and the Pi, while identity must follow the local filesystem.
The asymmetry is deliberate.

## Notes W4 must not rediscover

Measured by the W2a/W2c/W2d review, all confirmed by execution:

1. **`bind` inside the transaction is safe.** §1.9 made this doubtful, so it was
   measured: `CREATE TEMP VIEW` followed by `MERGE` into DuckLake in the same
   `BEGIN … COMMIT` works — three batches, one snapshot each, correct result.
   Binding before the transaction gives an identical result. Either shape is fine.
2. **`ensure()` is snapshot-free after the first call** (1 → 2 → 2 → 2), and an
   empty `BEGIN; COMMIT` adds zero. So "an idle trigger adds zero snapshots" holds
   even if the engine calls `ensure()` every trigger. But the **first** `ensure()`
   does cost a snapshot for the `CREATE SCHEMA` — account for it, or fold `ensure`
   into the first trigger's transaction.
3. **The landing-tree fixture must write atomically.** A file appended to between
   `plan()` and `bind()` is read in full and then re-planned: measured 1 planned
   row, 3 rows bound, rows counted twice. Out of contract — the writer owes
   rename-then-marker — but a fault-injection fixture that appends will trip it and
   look like an engine bug.
4. **Assert structurally that the merge `ON` clause contains no `(SELECT`.** The
   property holds today and §1.5 makes it load-bearing, but only a two-batch
   DuckLake run would catch a regression. A string scan of the generated statement
   is cheap insurance.
5. **`test_file_source.py` never touches DuckLake** — every bind there is against
   in-memory DuckDB. Conformance is the first place a file view is bound on a
   DuckLake connection.
6. **Three silent behaviours worth conformance coverage**, because silence is what
   this framework exists to remove: a file rewritten with identical size *and*
   mtime is missed (inherent to the identity, now pinned by a test), and D1/D2
   below were both silent until fixed.

## Phase 1 definition of done (from PLAN.md, not negotiable)

- one file source, one `additive` model, `AvailableNow` trigger
- DuckLake state store and sink, inlining disabled
- a fault-injection test that kills the process between sink write and commit and
  proves on restart that rows are neither lost nor duplicated
- both front doors: Python API and YAML/CLI, over one canonical `Model`
- config round-trip test, front-door parity test, foldability rejection test,
  one-snapshot-per-trigger assertion, `ducklake_list_files` non-empty assertion

Phases 2–6 follow only after this passes.
