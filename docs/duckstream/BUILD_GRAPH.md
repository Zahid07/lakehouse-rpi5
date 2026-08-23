# duckstream — build graph (manager working file)

Not a design document. `PLAN.md` says what to build and `CONTEXT.md` says why.
This file says **who builds which files, in what order, and what "done" means** so
that parallel work does not collide and interfaces do not drift.

The manager (main session) owns this file. Subagents read it and never edit it.

## Where the build stands (2026-08-23)

**Phase 1 is committed** at `693e691` on `feat/duckstream`. **Phase 2 — event
time — is complete**: watermarks, tumbling windows, sealing past the lateness
horizon, and both output modes. See the phase-2 section below for what was
decided and why.

Phase 1 finished at 873 tests. Phase 2 adds `duckstream/windows.py` and
`duckstream/watermark.py`, a new conformance module, and unit suites for both
new modules.

**Phase 2b — operability and failure handling — is also complete.** It was not
in `PLAN.md` when phases 1 and 2 were built; it was added after, because the gap
it closes was a gap in the plan rather than in the code. See "Phase 2b" below.

One shipped phase-1 behaviour was **deliberately reversed** in phase 2 —
`mode="append"` together with a `grain` — and the reasoning is recorded under
"Resolved, phase 2" below. Read it before reversing the reversal.

### What phase 1's review process caught

Kept because its value is the reason it cost what it did:

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
    watermark: datetime | None = None   # phase 2; keyword, defaulted

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
    lateness: str | None = None       # phase 2; "10 minutes", or a timedelta
    strategy: str | None = None       # None = infer from tier
    memory_profile: str | None = None # "streaming" | "materialising"
    udfs: list[str] = field(default_factory=list)   # dotted paths
    limits: BatchLimits = BatchLimits()
    on_failure: str = "quarantine"    # phase 2b; "quarantine" | "halt"
    max_attempts: int = 5             # phase 2b
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
| failure policy | `quarantine` (default), `halt` |
| batch outcome | `committed`, `empty`, `failed`, `halted`, `quarantined`, `backoff` |
| strategy | `delta_merge`, `sufficient_statistics`, `recompute_window` |
| grain | `minute`, `hour`, `day` |
| window column | `window_ts` — always, whatever the grain |
| sink modes | `append`, `update` |
| lateness units | `second`, `minute`, `hour`, `day` (plural or singular) |
| open-window table | `<target>__open_windows`, in the target's own schema |

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
| W5 | package README + status doc, final sweep | W4 | `duckstream/README.md`, `docs/duckstream/STATUS.md` | **done** |
| P2 | event time: watermarks, windows, sealing, output modes | W5 | `duckstream/windows.py`, `duckstream/watermark.py`, and the phase-2 edits listed below | **done** |
| P2b | operability: metrics/status/lag, retry + quarantine, run lock, `rows_out` | P2 | `duckstream/metrics.py`, `duckstream/lock.py`, and the phase-2b edits listed below | **done** |

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

## Phase 2 — event time

One work unit, built and verified in one session rather than fanned out: the
pieces are too tightly coupled to parallelise usefully (the watermark decides
what the sink sees, and the sink's output mode decides what the watermark is
for), and phase 1's frozen interfaces meant there was no interface to negotiate.

| File | Status |
|---|---|
| `duckstream/windows.py` | **new** — tumbling-window arithmetic, one owner for the boundary |
| `duckstream/watermark.py` | **new** — horizon parsing, advancing, observing, filtering |
| `duckstream/model.py` | `lateness` field, three new rules, `WINDOW_COLUMN` now re-exported from `windows.py` |
| `duckstream/engine.py` | event-time step in the batch lifecycle; memoised watermark |
| `duckstream/sinks/table.py` | sealed-append path; `merge_sql(into=)`; `seal_sql`/`evict_sql` |
| `duckstream/state.py` | `rows_late`/`rows_undated` on `batches`, plus a migration |
| `duckstream/protocols.py` | `BatchContext.watermark` |
| `duckstream/cli.py` | `WINDOW` column in `models`; drops reported by `run` |
| `tests/conformance/test_event_time.py` | **new** — phase 2's definition of done |
| `tests/unit/test_windows.py`, `tests/unit/test_watermark.py` | **new** |

### Resolved, phase 2 — ratified

**Lateness is opt-in, and it is the switch.** A model with no `lateness` reads
no watermark, writes none, filters nothing and behaves exactly as it did in
phase 1 — verified by asserting the `watermarks` table stays empty, not by
timing. Measured cost of the horizon once declared: **~5 ms a trigger**
(`CONTEXT.md` §1.11), which is one state append and is irreducible.

**A row is late when its *window* has sealed, never when its timestamp is older
than the watermark.** This is the single most reversible-looking decision here
and the one that matters most: with `grain='hour'` and `lateness='10 minutes'`,
a watermark of 12:50 leaves `[12:00, 13:00)` open, so a row arriving at 12:05
must still be folded. `PLAN.md` calls that "late arrival within the horizon" and
requires it to update its window. Testing the timestamp instead passes every
casual test and silently under-counts.

**The batch is filtered against the *committed* watermark, then the watermark
advances.** Using the batch's own maximum to judge its own rows would drop the
older half of any batch spanning a wide time range.

**The watermark is memoised in the engine, written only after a successful
commit.** `CONTEXT.md` §1.11: the read costs 10.4 ms, a third of everything a
horizon adds. Identical in shape and in soundness argument to §1.10's memoised
batch id — single-writer only, and the two expire together.

**Sealing is `window_ts <= watermark - grain`, computed in Python.** The
rearrangement of `ws + G <= W` puts the whole comparison on one side, so one
literal is inlined rather than per-row interval arithmetic — which is what keeps
§1.5 satisfied and lets DuckLake prune on `window_ts` statistics. `month` is
absent from the grains for this reason: its length varies, so the cutoff could
not be a single literal.

**Windowed `append` requires a horizon — a deliberate reversal.** Phase 1
accepted `mode="append"` with a `grain` and wrote one *partial* row per window
per batch. Its own conformance test documented the catch: that equals the truth
only "when batches do not overlap" a window. That is a condition the user cannot
enforce, the engine never checked, and the output never reveals — the
`CONTEXT.md` §4 bug class, in the one place the framework had left it. Phase 2
refuses the shape at load and provides the correct mechanism instead: fold into
an open-window accumulator, emit each window **once** when the watermark passes
its end. The refusal names all three ways forward (declare a horizon, drop the
grain, or use `update`) because each is a different thing the user might have
meant. `append` **without** a grain is untouched and still accepts any tier.

**The accumulator lives beside the target**, as `<target>__open_windows` in the
target's own schema. It must share the catalog (§1.9 — sealing moves rows
between the two inside one transaction), and putting it next to the target
rather than hiding it in the state schema makes "why is this hour missing from
my mart" answerable with a `SELECT`.

**Sealing deletes, and that is not a violation of §1.10.** The tombstone rule
there is about *per-trigger* state. This delete fires per window sealed, and it
is what bounds the accumulator by the lateness horizon instead of by the age of
the stream — which is exactly the eviction §1.3's caveat asks for. Measured
whole-trigger cost of the sealed-append path: **+20 ms** over phase 1.

**Windowed append is additive-only in phase 2**, because it folds across
batches. Unwindowed append still accepts any tier, because it never folds.

**Rows with no event time are dropped and counted separately** (`rows_undated`),
for models that declared a horizon. Under event-time semantics such a row
belongs to no window, so in `append` mode it could never seal and would be
permanently invisible. Models with no horizon keep phase-1 behaviour: a NULL
`window_ts`.

**Both counters are durable**, on `duckstream.batches`. A pre-phase-2 catalog is
migrated with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` — verified working on
DuckLake 1.5.5, leaving existing rows NULL, and NULL is the right value for a
batch that ran before the counters existed. NULL and 0 stay distinguishable
throughout: NULL means "no horizon", 0 means "a horizon, and nothing was late".

### Notes phase 3 must not rediscover

1. **`observe()` is one scan and it is free.** Counting rows, both drop classes
   and `max(event_ts)` in a single pass costs 0.26 ms more than the `count(*)`
   it replaced (§1.11). Do not split it back into separate queries.
2. **The filter view is created only when something is dropped.** A healthy
   stream builds no second view and reads no row twice; a conformance test
   asserts the negative structurally, because nothing in the output would show
   a regression.
3. **The sink does not filter late rows and must not start.** The engine removes
   them before `write` is called. Duplicating the check would put the decision
   in two places and the sink's copy would be the one without the committed
   watermark to check against. Pinned by a unit test that asserts the sink
   *does* re-emit a window if handed a row for it.
4. **`harness.replay` is a second implementation, not a helper.** It reproduces
   the event-time contract in plain Python — flooring by epoch arithmetic where
   duckstream floors by replacing fields — because the SQL recompute cannot be
   ground truth once rows are dropped or windows withheld. Do not "simplify" it
   by importing from `duckstream.windows`; that would make it agree by
   construction.
5. **Every event-time scenario drives one drop per trigger.** The reference
   needs the engine's actual batch boundaries; a scenario that let two drops
   share a trigger would make the two disagree about batching rather than about
   event time.
6. **Two `Parity` objects in one test must not share a landing tree.** The two
   *doors* of one parity share one on purpose — if they read different trees a
   parity failure could be a fixture difference rather than a duckstream one.
   But a file source scans the whole tree, so a *second* parity over the same
   tree consumes the first's drops as well as its own. This was found the hard
   way while writing the batch-boundary test, and it presented as the engine
   folding a row twice — the same class of trap as note 3 in the phase-1 list,
   where a fixture that appended to a planned file produced a genuine
   double-count that looked like an engine bug. `make_parity` now takes
   `landing=` and its docstring says when to pass it.
7. **Chunking is not neutral once a horizon exists.** Phase 1 asserts "chunked
   equals unchunked" because a fold gives the same answer whatever order the
   rows arrive in. Event time breaks that deliberately: the watermark depends on
   what has been observed, so a batch boundary between two rows can make the
   second late when reading both at once would not have. `max_files_per_trigger`
   therefore stops being purely a memory knob. Pinned by
   `test_batch_boundaries_change_which_rows_are_late`, which also asserts the
   two results *differ* — so the test cannot quietly stop testing anything.

---

## Phase 2b — operability and failure handling

Added to `PLAN.md` after the fact. The framework had a complete, demonstrated
answer for what happens when the *process* dies and no answer at all for what
happens when the *data* will not process: the offset stopped advancing and every
later trigger retried the same corrupt file, silently, forever.

| File | Status |
|---|---|
| `duckstream/metrics.py` | **new** — three lags, per-model status, health verdict |
| `duckstream/lock.py` | **new** — advisory single-writer lock, portable, self-healing |
| `duckstream/state.py` | retry state on `offsets`, a `quarantine` table, `record_failure`, `quarantine`, `Position` |
| `duckstream/engine.py` | retry budget, backoff, the failure decision, `rows_out`, the lock |
| `duckstream/model.py` | `on_failure`, `max_attempts` |
| `duckstream/sinks/table.py` | `write` returns the affected-row count |
| `duckstream/protocols.py` | `Sink.write -> int | None` |
| `duckstream/cli.py` | `status`; `run` reports outcomes and exits non-zero |
| `tests/unit/test_metrics.py`, `tests/unit/test_lock.py` | **new** |
| `tests/conformance/test_operability.py` | **new** — the whole path, both doors |

### Resolved, phase 2b — ratified

**Quarantine is the default, `halt` is the option.** The argument is not that
losing a batch is acceptable; it is that halting does not preserve the batch
either. A stream blocked on one bad file stops collecting everything behind it
too, so continuing loses strictly less. `halt` is right when a *gap* is worse
than a *stall*, which is a real case and why it exists.

**Quarantine is never silent, and that is what makes it a policy.** The skip and
the record of the skip are one transaction, so the offset cannot move past data
without the row saying why (`CONTEXT.md` 1.4 makes them one snapshot, and a
conformance test reads both sides `AT (VERSION => n)` to prove it). The
`quarantine` table is excluded from `prune` for the same reason.

**Retry state lives in the `offsets` row, not a second table.** `CONTEXT.md`
1.11 measured a scalar read of a DuckLake state table at ~10 ms, and the engine
already pays one per trigger to learn its offset. A second table would have
doubled that on **every** trigger to carry information that only matters when
something is broken. A failure appends a row with the *same* offset, so "newest
row wins" is unchanged.

**A failure spends a batch id.** Two rows sharing an id would make
`ORDER BY batch_id DESC LIMIT 1` arbitrary. This turned up a latent bug while
being implemented: `next_batch_id` consulted only `batches`, which records
*committed* batches, so a fresh process would hand back an id a failure row had
already used. Fixed to span both tables; pinned by
`test_a_batch_id_is_never_reused_after_a_recorded_failure`.

**Only clean failures spend an attempt.** A hard kill records nothing, so a
crash-looping deployment cannot quarantine its own data. Deliberate:
infrastructure trouble must not be mistaken for bad data.

**A halted model stops re-recording its verdict.** Otherwise a stuck pipeline
appends a row and a DuckLake snapshot every tick for as long as nobody looks —
growing the catalog fastest exactly when that helps least. It still *retries*
each tick, cheaply, so fixing the cause is all it takes to recover.

**One quarantine per model per run.** A source where every batch is
unprocessable would otherwise skip the whole backlog in one run, before anyone
saw the first record.

**`run()` raises `BatchFailed` after every model has had its turn**, never where
the failure happened. Raising in place meant one corrupt file in one model cost
every later model its trigger. The exception carries the `RunReport`, so a
caller can still see what succeeded. Quarantine does **not** raise — it is the
outcome the model asked for — but the CLI exits non-zero for it, because losing
data should page somebody exactly once.

**`rows_out` was free all along.** Phase 1 recorded it as NULL believing it
meant "running the aggregation twice or coupling to a sink internal". Measured
on 1.5.5: `con.execute` on the `INSERT`, `MERGE` or `DELETE` the sink already
issues returns a one-row result carrying the affected count, and the sink was
discarding it. For sealed `append` the number reported is rows that reached the
*target* — an open window has not been output yet — which makes a `0` on a
sealing model informative rather than alarming.

**The run lock is advisory and the catalog lock is authoritative.**
`CONTEXT.md` 2.5's claim that contention is "structurally impossible" is very
nearly true and not quite: `AvailableNow` drains until empty, so a backlog can
make a tick outlast its own interval. What actually prevented corruption was
DuckDB's file lock on the catalog (1.6), reporting `Unique file handle
conflict`. The advisory lock exists to say what happened in words; it is never
trusted for safety, because a lock trusted for safety is one that fails open on
a filesystem it does not understand. A lock whose owner is provably dead **on
this host** is broken automatically; one from another host never is.

**`status` reads and never writes.** It can be pointed at a live deployment from
another process, which is the DuckLake property from 1.6 paying off. Lag is
three numbers because they fail independently — a pipeline whose cron entry was
deleted has perfect event-time lag until you look at the processing lag.

### Notes phase 3 must not rediscover

1. **`Sink.write` now returns `int | None`.** A sink that returns nothing still
   works and simply reports no count; phase 3's tier-two and tier-three write
   paths should return theirs.
2. **A failed batch is not an exception at the call site.** `_run_batch` catches
   and records; `_drain` breaks on any non-committed outcome. Anything phase 3
   adds inside the batch lifecycle inherits that, so it must leave the
   transaction rolled back before it propagates.
3. **`no_backoff` is a fixture, not a hack.** Any test about recovery has to
   neutralise the retry delay or sleep a real second. It lives in
   `tests/unit/test_engine.py` and `tests/conformance/test_operability.py`.
4. **Two `Parity` objects still need a landing tree each** (phase-2 note 6), and
   `World.run(expect_failure=True)` is how a scenario asserts a non-zero exit
   through the CLI door.

---

## Phase 1 definition of done (from PLAN.md, not negotiable)

- one file source, one `additive` model, `AvailableNow` trigger
- DuckLake state store and sink, inlining disabled
- a fault-injection test that kills the process between sink write and commit and
  proves on restart that rows are neither lost nor duplicated
- both front doors: Python API and YAML/CLI, over one canonical `Model`
- config round-trip test, front-door parity test, foldability rejection test,
  one-snapshot-per-trigger assertion, `ducklake_list_files` non-empty assertion

Phases 2–6 follow only after this passes.
