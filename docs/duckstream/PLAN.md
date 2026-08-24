# duckstream — a micro-batch streaming framework for DuckDB

> **New session? Read `CONTEXT.md` first.** It holds measured constraints that
> will otherwise lead you to wrong conclusions, and records decisions already
> settled so you neither re-research nor re-litigate them.
>
> **PLAN.md says what to do. CONTEXT.md says why, and what has already been proven.**

## Start here

You are implementing `duckstream`, a micro-batch streaming framework for DuckDB.

**Confirm your starting state:**

```bash
git branch --show-current      # expect: feat/duckstream
ls docs/duckstream/            # expect: CONTEXT.md PLAN.md
python -c "import duckdb; print(duckdb.__version__)"
```

**Environment.** There is no dependency manifest in this repo yet — creating one
is your first commit. `duckstream` needs `duckdb`, `pyarrow`, `numpy` and
`pyyaml`; tests need `pytest`. Declare a `duckstream` console script alongside
`python -m duckstream`. **Pin `duckdb` exactly**: constraints 7 and 8 in `CONTEXT.md` are
version-sensitive.

**Then begin Phase 1.** Do not start with source connectors or SQL — start with
`duckstream/model.py` and the protocols, because the `Model` declaration is the
public API and everything else is shaped by it. Write the load-time validation and
its rejection test *before* any execution code: the framework's whole value
proposition is refusing incorrect models, so that path is the specification.

**Definition of done for Phase 1:** one file source, one `additive` model, one
`AvailableNow` trigger, **DuckLake state store and sink with inlining disabled**,
and a fault-injection test that kills the process between sink write and commit
and proves on restart that rows are neither lost nor duplicated. Nothing else.
Resist adding windows or extra sources until that test passes — it is the
load-bearing claim of the whole framework.

**Working agreements:**

- Every phase ends with tests passing, including the fault-injection test.
- DuckLake is the storage layer and the conformance target. Never assume it
  behaves like plain DuckDB — it demonstrably does not (constraint 7), and
  in-memory tests hide real failures.
- Always set `ducklake_default_data_inlining_row_limit = 0`. It defaults to 10,
  which silently routes small batches through DuckLake's buggiest code path.
- When a measured number in `CONTEXT.md` conflicts with intuition, trust the
  number or re-measure it — do not reason around it.
- Keep `duckstream/` free of any import from this repo's existing pipeline. The
  package is meant to be extracted to its own repository once the API stabilises.

## Context

DuckDB has no continuous-ingestion story. Concretely:

- **No materialized views.** Views are re-executed on every reference.
  Materialized views sit in the *"future work / seeking funding"* bucket of the
  DuckDB roadmap, with no date. DuckLake's roadmap lists *"materialized views and
  incremental maintenance"* under future work as well.
- **"Streaming" in DuckDB means something else.** Lazy/chunked result streaming
  (`fetch_arrow_reader`) is not continuous ingestion. Expect this terminology
  collision in every discussion.
- Existing extensions each cover one slice and stop short of the full loop. See
  the prior-art survey in `CONTEXT.md`.

The goal is a **generic micro-batch streaming framework**, modelled on Spark
Structured Streaming: sources with durable offsets, a trigger loop, event-time
windows with watermarks, correct incremental aggregation, and idempotent sinks —
delivered as a Python library over **DuckDB and DuckLake**, so the output is a
real lakehouse: parquet data files, a SQL catalog, snapshots, time travel and
schema evolution.

## The differentiator: foldability classification

Every streaming engine makes you hand-roll incremental aggregation and will
happily let you get it wrong. This is the framework's reason to exist.

An incremental merge is only correct when the aggregate forms a monoid over
batches — partial result combined with partial result gives the true combined
result. The framework classifies every aggregate and picks its strategy:

| Tier | Aggregates | Strategy |
|---|---|---|
| `additive` | `COUNT`, `SUM`, `MIN`, `MAX` | Fold the delta into the stored value. No rescan. The only tier a naive `WHERE ts > last_seen` delta merge is safe for. |
| `sufficient_statistics` | `AVG`, `STDDEV`, `VAR`, `CORR`, `COVAR` | Persist a mergeable state per statistic argument and derive the result from it. Exact, still no rescan. **The state is `(n, mean, M2)`, not `(sum, sum_sq, count)`** — constraint 14 measured the latter returning 524 for a true variance of 0.25 at Unix-timestamp magnitudes, and exactly 0.0 at 1e8 with a small spread, which is the same symptom section 4's production bug produced. |
| `non_foldable` | median / exact quantiles, exact `COUNT DISTINCT`, order-dependent aggregates, any UDF over a whole window (FFT, DTW, entropy) | Recompute the affected window from source. No shortcut exists. |

The framework **rejects at model-load time** a declaration of an additive strategy
over a non-foldable aggregate, and requires a memory profile on `non_foldable`
models. Two further invariants: the sink merge key must equal the window grain key
(or idempotency silently breaks), and a `non_foldable` model must declare its
source time column so affected windows can be identified.

This taxonomy is the intellectual core and is entirely domain-agnostic.

## Non-goals

Stating these prevents scope collapse:

- **Not IVM for arbitrary SQL.** That is OpenIVM / Feldera-DBSP territory and a
  research-grade problem. The framework handles declared aggregate models.
- **Not distributed.** Single process, vertical scale.
- **Not a broker.** It reads from brokers and files, it does not replace them.
- **Not a DuckDB extension.** See `CONTEXT.md` for why, in detail. It is a
  Python process that drives an embedded DuckDB — see *Running it*.

## Architecture

Four planes. The trigger loop stays in the host process, never in the database.

```
Trigger   AvailableNow / Once / ProcessingTime   - cron or supervisor owns it
Plan      offsets in, bounded micro-batch out    - enforces memory limits
Execute   SQL over (micro-batch x state)         - DuckDB
State     offsets, watermarks, windows, metrics  - DuckLake tables
```

### Core protocols

```python
class Source(Protocol):
    def latest_offset(self) -> Offset: ...
    def plan(self, start: Offset | None, end: Offset,
             limits: BatchLimits) -> BatchPlan: ...   # honours max_rows/max_files
    def bind(self, con, plan: BatchPlan) -> str: ...  # registers a view, returns name

class Sink(Protocol):
    def write(self, con, batch_view: str, model: Model, ctx: BatchContext) -> None: ...

class StateStore(Protocol):                            # DuckDB table | DuckLake table
    def begin(self, con): ...
    def commit(self, con, offsets: dict[str, Offset], watermarks: dict): ...
```

`Offset` is JSON-serialisable — a consumed-file set or high-water mark for files,
a broker sequence for streams, a snapshot id for CDC sources.

### Storage: DuckLake is the foundation

**DuckLake is the storage layer, from phase 1, not an optional backend.** The
framework is a streaming engine *for a lakehouse* — parquet data files, a SQL
catalog, snapshots, time travel and schema evolution are the point, not an
add-on.

The `StateStore` abstraction is retained for one reason only: a plain in-memory
DuckDB implementation makes unit tests fast. It is **not** a supported production
backend, and it is never the sole test gate — conformance runs against DuckLake,
because in-memory DuckDB demonstrably hides DuckLake failures (constraint 7).

Time travel is a genuine asset for this framework, not just a feature to inherit.
Because every trigger is one snapshot (constraint 6), you can query a mart exactly
as it stood before and after any batch. That makes the exactly-once verification
directly inspectable rather than inferred — use it in the fault-injection tests.

### Required DuckLake setup

```sql
INSTALL ducklake;                      -- ATTACH autoloads, but be explicit:
LOAD ducklake;                         -- autoload still needs it pre-installed,
                                       -- which an air-gapped device will not do
ATTACH 'ducklake:catalog.ducklake' AS lake (DATA_PATH 'lake_data');
USE lake;
```

Attaching an existing catalog omits `DATA_PATH` — it is already recorded. Deploy
note: run `INSTALL ducklake` once on the target device while it has network, or
the first run of a disconnected deployment fails.

**Settings the engine must set explicitly**, not inherit:

| Setting | Value | Why |
|---|---|---|
| `ducklake_default_data_inlining_row_limit` | **`0`** | Inlining defaults to **10 rows**, so any trigger writing fewer than 10 rows silently takes the inlining path — which is where the open correctness bugs are (constraint 8). Verified: a 3-row insert produces zero data files by default, one parquet file with inlining off. Leaving it on makes behaviour depend on batch size, i.e. non-deterministic across triggers. Disable it for uniformity, and revisit only when those bugs close. |
| `ducklake_max_retry_count` | `10` (default) | Snapshot-id conflicts retry without rewriting staged data files. |
| `memory_limit`, `threads` | per device | The buffer manager is the memory ceiling (constraint 1). |

**The cost of disabling inlining is small files** — one parquet file per trigger
per table. That makes compaction a first-class, framework-owned concern rather
than an operator chore; see phase 4.

### Exactly-once

One transaction per trigger:

```
BEGIN
  write output rows                 -- sink
  update watermark and window state -- state
  upsert source offset              -- checkpoint
COMMIT
```

Crash before `COMMIT` replays from the stored offset; crash after is durable. On
DuckLake this is a single snapshot (constraint 6). On plain DuckDB it is one
transaction.

State the limit honestly in the docs: **exactly-once requires a replayable
source.** Brokers without durable offsets (MQTT, and Tributary's stateless reader)
cannot provide it. Those are modelled as *landing writers* — at-least-once into
durable storage, which then becomes a replayable source. This is why the file
source is the foundation rather than a convenience.

### Memory control

From constraint 1: bound rows, not UDF cost.

- `max_rows_per_trigger` / `max_files_per_trigger` at the source.
- `non_foldable` models split execution by window range, sized from an estimated
  row count rather than a fixed window count — window density varies enormously
  between a quiet interval and a saturated one.
- Arrow-mode UDFs by default (constraint 2), while keeping UDFs off the hot path
  wherever a foldable tier applies (constraint 3).

## Package layout

| Path | Contents |
|---|---|
| `duckstream/model.py` | `Model` declaration, load-time validation, invariants — **the canonical representation** |
| `duckstream/config.py` | YAML deserialiser into `Model`, `${VAR}` substitution. Parsing isolated here so the format can be swapped |
| `duckstream/registry.py` | built-in names (`file`, `mqtt`, `table`) plus dotted-path resolution for user sources, sinks and UDFs |
| `duckstream/cli.py` | `run` / `validate` / `status` / `models`. Thin — arg parsing only, no logic of its own |
| `duckstream/engine.py` | trigger loop, transaction boundary, batch lifecycle |
| `duckstream/offsets.py` | `Offset` types, checkpoint tables, commit and recovery |
| `duckstream/watermark.py` | event-time tracking, window sealing, lateness policy |
| `duckstream/aggregates.py` | foldability tiers, strategy validation, SQL generation |
| `duckstream/windows.py` | tumbling windows (sliding and session are post-v1) |
| `duckstream/sources/files.py` | directory tailing with completion markers |
| `duckstream/sources/mqtt.py` | landing writer (at-least-once into durable storage) |
| `duckstream/sinks/table.py` | append / update-by-merge, DuckDB and DuckLake |
| `duckstream/state.py` | pluggable state store, DDL |
| `duckstream/udf.py` | Arrow-mode UDF helpers and registration |
| `duckstream/metrics.py` | per-batch timings, rows, lag |
| `tests/conformance/` | **runs against DuckLake, and through both front doors** |

## Phases

1. **Core loop on DuckLake.** `Model`, `Source`, `Sink`, `StateStore` protocols.
   File source with completion markers. `AvailableNow` trigger only — no
   background thread; cron or a supervisor drives it. **DuckLake state store and
   sink from the start**, with inlining disabled and one snapshot per trigger.
   Prove one exactly-once batch end to end, including a fault-injected replay
   verified against the snapshot history.

   **Both front doors ship in this phase**, because retrofitting config onto a
   Python-shaped API is far more expensive than building them together: the
   `Model` object, the registry, the YAML loader, and `run` + `validate` in the
   CLI. `status` waits for `metrics.py`. The round-trip and parity tests below
   are part of this phase's definition of done, not a later cleanup.
2. **Event time.** Watermarks, tumbling windows, sealing past the lateness
   horizon, `append` and `update` output modes via merge-by-key.
2b. **Operability and failure handling.** Added to this plan after phases 1
   and 2 shipped, because the gap it closes was not in it: the framework had a
   complete answer for what happens when the *process* dies and no answer at
   all for what happens when the *data* will not process. A corrupt file made
   the offset stop advancing, so every trigger from then on retried it and the
   pipeline stopped, quietly, forever.

   - **A retry budget with capped backoff**, and a declared policy for what
     happens when it runs out. `on_failure: quarantine` (the default) skips the
     batch and records the loss permanently in `duckstream.quarantine`;
     `on_failure: halt` never advances past data it could not process. The
     default is `quarantine` because halting does not preserve the unprocessable
     data either — it stops collecting everything that arrives after it too — so
     continuing loses strictly less. Both are loud: `duckstream run` exits
     non-zero on the run that quarantines.
   - **Failures are recorded, not merely raised**, in the same row of `offsets`
     the engine already reads once a trigger, so the budget survives the process
     exiting between cron ticks and costs no extra read (constraint 11).
   - **Every model gets its turn before a run reports failure**, so one corrupt
     file in one model cannot cost an unrelated model its trigger.
   - **`metrics.py` and `status`**, which `PLAN.md` deferred from phase 1
     pending exactly this module. Lag is reported three ways because they fail
     independently: event-time lag (`now - watermark`), processing lag
     (`now - last commit`) and source backlog. `status` exits non-zero when any
     model is unhealthy, so it doubles as a health probe.
   - **`rows_out`**, which phase 1 recorded as NULL believing it cost a second
     aggregation pass. Measured: `con.execute` on the `INSERT`/`MERGE` the sink
     already issues returns the affected count, and the sink was discarding it.
   - **An explicit run lock.** `AvailableNow` drains until empty, so a backlog
     can make one tick outlast the interval that started it. Two overlapping
     ticks were previously stopped by DuckDB's own catalog file lock, reporting
     `Unique file handle conflict` — no corruption, but a message about a
     metadata handle rather than about two copies of the pipeline running. The
     lock is advisory, portable (no `fcntl`), and breaks a lock whose owner is
     provably dead on this host.

3. **Foldability.** All three tiers with load-time validation and the rejection
   path. Arrow-mode UDF helpers for tier three. Window-range chunking sized from
   estimated rows.

   **Where a tier-three recompute reads from is settled by measurement, not
   preference.** It re-derives a whole window from source, and those rows are in
   files consumed long ago — still on disk and still identifiable, because the
   offset keeps a consumed-file *map* rather than a high-water mark. But
   constraint 13 measured that handing DuckDB the whole list with a time
   predicate costs **~0.1 ms per file listed whether it is read or not**: the
   footer still has to be opened to decide. At one file per trigger on a
   one-minute schedule that is 525,000 files a year, and on a Pi the cost is
   small random I/O, which SD and USB storage are worst at.

   So tier three needs a **file → time-range index**, built as a *hint* rather
   than as truth: it only ever narrows, it never removes entries, and anything
   it cannot answer confidently falls back to the whole list. Correctness then
   never depends on it and only cost does — over-selecting reads extra files and
   still gets the right answer, while under-selecting would be silently wrong,
   which is the one outcome this framework refuses. It is close to free to
   build: the engine already scans each bound batch once for the watermark, so
   grouping that pass by `read_parquet`'s `filename` pseudo-column yields
   per-file bounds with no extra I/O.
4. **Maintenance and small files.** Because inlining is off, every trigger writes
   a parquet file — so compaction is part of the framework, not an operator
   chore. A maintenance model running `ducklake_merge_adjacent_files`,
   `ducklake_expire_snapshots` and `ducklake_cleanup_old_files` on its own
   cadence, with the retention window set from the lateness horizon. Do **not**
   rely on `CHECKPOINT` to flush inlined data (ducklake#1368) — moot here, but do
   not reintroduce the dependency. Also add partitioning of sink tables by time
   grain, which is what makes window-range scans prune to a few files.

   **The first item was not compaction, and it is DONE.** Constraint 15 measured
   the file source's consumed-file map at **45.7 MB after a year at one file a
   minute**, rewritten in full on every trigger. On a Raspberry Pi that was an SD
   card's write budget being spent re-recording file names it already knew, and
   it was the single largest obstacle to running duckstream unattended on one.

   The fix shipped: the consumed set is **rows** in `duckstream.consumed_files`,
   consuming a file is an insert, and "has this been consumed?" is an anti-join
   the database answers. Constraint 12's rule applied to its most extreme case —
   a state table read back and looped over in Python, where the whole table was a
   single cell. Measured in constraint **16**: **7.97 MB of writes per trigger
   becomes 4.9 KB** (1,665x), the commit goes from 1,078 ms to 13.8 ms, and both
   are now flat in the number of files consumed. Constraint 16 also corrects two
   figures in 15 that were derived rather than measured — the daily write volume
   was 11.2 GB, not 65 GB, because the offset reaches the disk as parquet.

   Two shortcuts were tempting and both were wrong, and neither should come
   back. `offsets.py` reserves `high_water_mtime_ns` for collapsing old entries,
   which bounds the map but silently skips a file that arrives with an older
   mtime; the key is still reserved and still unused. And dropping entries for
   files that no longer exist sounds exact, but the source learns what exists by
   scanning the tree — so a network mount that blinks returns an empty scan,
   every entry looks deleted, and the next good scan re-reads the whole landing
   directory.

   **What is still open is the number of files.** `latest_offset()` walks the
   whole landing tree every trigger and constraint 13's ~0.1 ms per file listed
   applies to that walk. Retention at the source is the lever. It is now a speed
   problem rather than a write-endurance one, which is a different priority.

   Three further items belong here, deferred from the phase-2b sweep because
   they are data-lifecycle concerns rather than failure-handling ones:

   - **Schedule `DuckLakeStateStore.prune()`.** It exists and nothing calls it.
     State grows by two rows per committed trigger, three with a horizon. The
     `quarantine` table is explicitly excluded from pruning — it is the record
     that data was lost, and discarding it would leave a mart quietly short of
     rows with nothing to say why.
   - **A migration path for a renamed aggregate.** Today a rename leaves its
     predecessor column behind, silently NULL, and the type check refuses a
     *changed* type with a good error but offers no way forward. `ensure` should
     be able to report the diff, and there should be a documented procedure —
     not an automatic `ALTER`, which would be a silent coercion of exactly the
     kind section 4 is about.
   - **Compaction of the open-window accumulator.** Sealed `append` writes to
     `<target>__open_windows` every trigger and deletes from it on every seal,
     so it accumulates both small files and tombstones faster than the target
     does.
5. **MQTT landing writer.** Fills a genuinely empty slot — no MQTT extension for
   DuckDB exists. At-least-once into durable storage, replayable downstream.
6. **Validation on a real workload, and a release.** Point it at a real sensor
   pipeline and diff against a full recompute. This repo's accelerometer marts
   are a convenient reference case, exercising all three tiers (counts,
   averages/stddev, and a windowed FFT). Purely a test consumer — the framework
   must not acquire any dependency on it.

   Everything measured so far was measured on a Windows dev box with
   `threads=2` as a Pi proxy, against synthetic fixtures. This phase is where
   that stops being an assumption:

   - **A soak run.** Days, not minutes, on the actual hardware. The state store
     was only ever measured to 6,000 rows (constraint 3) and the open-window
     accumulator has never been measured at all.
   - **The two unmeasured numbers** from section 6 of `CONTEXT.md`: the memory
     ratio per tier, and the UDF parallelism penalty.
   - **Release discipline**, which a library other people depend on needs and
     `0.1.0` does not have: semantic versioning, a deprecation policy, and an
     upgrade note for the state-store migrations that have already accumulated
     (`rows_late`/`rows_undated` on `batches`, `attempt`/`failed_at`/`error` on
     `offsets`, and the `quarantine` table).

Post-v1: `ProcessingTime` trigger with portable locking, sliding and session
windows, CDC source (evaluate adopting `ducklake_cdc` rather than building),
Kafka (evaluate Tributary plus external offset tracking), stream-stream joins,
native extension against the DuckDB v2.0 C ABI.

## Running it

Worth being explicit, because it is the first thing a user asks and it shapes the
entry point. duckstream is **not** `INSTALL duckstream; LOAD duckstream;` — that
is the extension model, which is out of scope. It is a Python process that drives
an embedded DuckDB, so "running it" means running Python on a schedule.

A user writes a script declaring models, and cron drives it:

```python
# pipeline.py
import duckdb
from duckstream import Engine, Model, FileSource, TableSink, AvailableNow

con = duckdb.connect()                       # in-memory session, DuckLake holds the data
engine = Engine(con, catalog="ducklake:catalog.ducklake", data_path="lake_data")

engine.add(Model(
    name="hourly_counts",
    source=FileSource("landing/", marker="_READY", max_files_per_trigger=10),
    time_column="event_ts",
    grain="hour",
    key=["window_ts", "sensor_id"],             # the window column is always `window_ts`
    aggregates={"n": "count(*)", "total": "sum(value)"},   # additive tier
    sink=TableSink("marts.hourly_counts", mode="update"),
))

engine.run(trigger=AvailableNow())           # drain what is available, then exit
```

```cron
* * * * * cd /opt/pipeline && ./venv/bin/python pipeline.py >> logs/duckstream.log 2>&1
```

Results are ordinary DuckLake tables, so the read path is plain SQL from any
DuckDB client — duckstream is not in it:

```sql
ATTACH 'ducklake:catalog.ducklake' AS lake;
SELECT * FROM lake.marts.hourly_counts ORDER BY window_ts DESC LIMIT 5;
```

### Two front doors, one canonical model

**Both entry points are supported**: the Python API above, and a config-driven
CLI. The failure mode to design against is drift — the config loader supporting a
subset, the Python API growing what config cannot express, and the two paths
diverging. Three rules prevent that:

1. **`Model` is the single source of truth.** It is a plain Python object and all
   validation lives on it.
2. **The config loader is only a deserialiser.** It constructs the same `Model`
   objects and then the same validation runs. There is no parallel validation and
   no parallel execution path. `Engine.from_config(path)` returns an ordinary
   `Engine` that Python can keep modifying.
3. **No merge or precedence semantics.** Config is a constructor, not an override
   layer. If you need environment differences, use `${VAR}` substitution in config
   values, or load the config and adjust in Python. Anything more becomes a
   configuration language.

The equivalent of the script above:

```yaml
# models.yaml
catalog: "ducklake:catalog.ducklake"
data_path: "lake_data"

settings:                                  # applied to every connection
  ducklake_default_data_inlining_row_limit: 0
  memory_limit: "2GB"
  threads: 2

models:
  - name: hourly_counts
    source:
      type: file                           # registry name
      path: "${DUCKSTREAM_LANDING:-landing/}"
      marker: _READY
      max_files_per_trigger: 10
    time_column: event_ts
    grain: hour
    key: [window_ts, sensor_id]        # the window column is always `window_ts`
    aggregates:
      n: "count(*)"
      total: "sum(value)"
    sink:
      type: table
      table: marts.hourly_counts
      mode: update
```

**The capability boundary is callables.** Config can express declarative
structure but not functions. Resolve that with a **registry**: built-in names
(`file`, `mqtt`, `table`) plus dotted-path resolution for user code, so config
stays fully capable without becoming a programming language:

```yaml
  - name: minute_spectrum
    source: {type: file, path: landing/, marker: _READY}
    time_column: event_ts
    grain: minute
    key: [window_ts, sensor_id]
    strategy: recompute_window             # tier 3 must be declared explicitly
    memory_profile: materialising
    udfs: ["my_pkg.signal:arrow_fft"]      # imported and registered before planning
    aggregates:
      spectrum: "arrow_fft(list(value ORDER BY event_ts))"
    sink: {type: table, table: marts.minute_spectrum, mode: update}
```

A custom source is the same mechanism: `type: "my_pkg.sources:MySource"`.

### CLI surface

Exposed both as `python -m duckstream` and as a `duckstream` console script — cron
in a venv usually calls the interpreter directly, so do not rely on the script
being on `PATH`.

| Command | Purpose |
|---|---|
| `run --config models.yaml [--model NAME]` | One `AvailableNow` pass. The cron entry point. |
| `validate --config models.yaml` | Load and validate, non-zero exit on failure. **Run this at deploy time** so a bad model is caught then, not at 03:00 in a cron log. |
| `status --config models.yaml` | Per model: offset, watermark, last batch, and **lag** — the operational metric that matters. Arrives with `metrics.py`. |
| `models --config models.yaml` | List declared models with their resolved tier and strategy. |

Cron then becomes:

```cron
* * * * * cd /opt/pipeline && ./venv/bin/python -m duckstream run --config models.yaml >> logs/duckstream.log 2>&1
```

### How drift is prevented

Discipline is not sufficient; make it mechanical:

- **Round-trip property test.** For every model, `Model` → dict → YAML → `Model`
  must reconstruct an identical object. A field addable in Python but not
  expressible in config fails this test.
- **Every conformance scenario runs through both front doors** and must produce
  identical output. That is the parity guarantee, enforced by the suite rather
  than by review.

Format note: YAML is chosen because nested model declarations read far better than
the alternatives and it is the ecosystem norm for data tooling, at the cost of a
`pyyaml` dependency. TOML via stdlib `tomllib` is the zero-dependency alternative
if that dependency is ever unwelcome on a constrained device — the loader is small
enough that swapping it is cheap, so keep parsing isolated in `config.py`.

**Why DuckLake matters operationally here**, beyond the lakehouse features: a
DuckDB *file* can be held by only one process at a time — while one holds it
read-write, no other process can open it **even read-only** (measured; see
`CONTEXT.md`). A long-running engine on a single DuckDB file would lock you out of
your own warehouse. DuckLake avoids this entirely: the catalog is a separate
database and the data is parquet, so readers attach freely while the engine
writes. This is what makes a `ProcessingTime` daemon viable later, and it is
another reason DuckLake is the foundation rather than an option.

## Verification

Correctness:

- **Exactly-once under fault injection.** Kill the process between sink write and
  commit; assert on restart that rows are neither lost nor duplicated. Repeat for
  a crash after commit. This is the headline claim and needs real fault injection,
  not a unit test.
- **Ground-truth diff.** For every model, compare the sink against a full
  recompute from source, under interleaved batches, out-of-order arrival, re-runs,
  late arrival within the horizon, and NULL grouping keys.
- **Chunked equals unchunked.** Byte-identical output at chunk size 1 versus
  unbounded, for every `non_foldable` model.
- **Foldability rejection.** Assert the *failure* path — an additive strategy over
  a non-foldable aggregate must be refused at load, not at runtime.
- **Front-door parity.** Every conformance scenario runs through both the Python
  API and the YAML/CLI path and must produce identical output. This is what stops
  the two entry points drifting, and it belongs in the suite rather than in review.
- **Config round-trip.** For every model, `Model` -> dict -> YAML -> `Model` must
  reconstruct an identical object. A field addable in Python but not expressible
  in config fails here, which is precisely the drift this catches early.
- **`validate` is honest.** A model that fails at load must make
  `duckstream validate` exit non-zero. Deployment scripts depend on it.
- **Against DuckLake, always.** DuckLake is the conformance target. In-memory
  DuckDB demonstrably hides DuckLake failures (constraint 7), so a fast
  in-memory path may exist for unit tests but must never be the sole gate. Every
  suite must also run at least two batches, so the `WHEN MATCHED` merge branch is
  reached — the constraint-7 bug appeared only on the second batch.
- **Snapshot accounting.** One trigger produces exactly one snapshot. Assert it,
  because it is what the exactly-once guarantee rests on.
- **No inlined data.** Assert `ducklake_list_files` is non-empty after a small
  batch, proving inlining stayed off and the batch did not take the buggy path.
- **Watermark semantics.** Late data inside the horizon updates its window; data
  past the horizon is dropped **and counted**, never silently absorbed.
- **Unprocessable data does not stop the stream, and is not lost silently.** A
  corrupt source file must be retried a bounded number of times, then skipped
  with a permanent record of what was skipped and why — or halt the model, if
  that is what it declared. Either way the run exits non-zero, and `status`
  keeps reporting it after the log has rotated.
- **One model's failure does not cost another model its trigger.**
- **`status` is honest about lag.** Event-time lag, processing lag and source
  backlog fail independently; a pipeline whose cron entry was deleted has
  perfect event-time lag until you look at the second one.

Performance, to publish the operating envelope:

- **Micro-batch latency floor** — trigger overhead with an empty batch, per
  backend. State merge is ~3.2 ms (constraint 5), so this is dominated by commit
  cost and sets the minimum usable trigger interval. **Not yet measured.**
- **Memory ratio** — bisect `memory_limit` against `max_rows_per_trigger` per
  tier, and publish the ratio so users can size the knob.
- **UDF parallelism impact** — quantify the single-thread penalty (constraint 3)
  so the docs can state when to split across processes.

## Open questions, and the safe v1 answer

| Question | v1 resolution |
|---|---|
| DuckLake inlining for low-latency small writes | **Explicitly disable** (`ducklake_default_data_inlining_row_limit = 0`). It defaults to 10 rows, so small triggers silently take the path holding ~12 open correctness bugs, making behaviour batch-size dependent. Verified: a 3-row insert writes zero data files by default, one parquet file with it off. Accept small files and compact instead (phase 4). Revisit when those bugs close — inlining is the right long-term answer to small writes. Note the docs contradict themselves on the default (`0` on the ATTACH page, `10` in source — source wins). |
| Storage backend | **DuckLake, from phase 1.** Not optional. The plain-DuckDB `StateStore` exists only to make unit tests fast and is not a supported production backend. |
| Entry point | **Both, built together in phase 1.** Python API and a config-driven CLI, over one canonical `Model`. The config loader is a deserialiser only — no parallel validation, no parallel execution path, no override semantics. Drift is prevented by the round-trip and front-door-parity tests, not by discipline. |
| Config format | **YAML** (`pyyaml`), for readable nested model declarations and ecosystem familiarity. Keep parsing isolated in `config.py` so stdlib `tomllib` can replace it if the dependency ever becomes unwelcome on a constrained device. |
| Change feed as a source | **Post-v1**, and evaluate adopting `ducklake_cdc` first. It dies with snapshot expiry, so any consumer must **fail loudly** on a missing snapshot rather than skip silently. |
| DuckLake maintenance | Do not rely on `CHECKPOINT` to flush inlined data (ducklake#1368). Since v1 avoids inlining, the ordinary maintenance chain suffices. |
| Concurrency and locking | v1 is single-writer under `AvailableNow`, so contention is structurally impossible and **no lock is needed**. DuckLake additionally retries snapshot-id conflicts without rewriting data files. A portable lock arrives with `ProcessingTime`. Never `fcntl` — it is POSIX-only and breaks import on Windows. |
| `ducklake_add_data_files` as cheap ingestion | **Do not use.** Registration transfers file ownership to DuckLake, so maintenance may delete the landing file, and pruning is silently lost if the writer omits footer statistics. |
| `ducklake_commit` / staged commit | **Do not use.** In-tree but undocumented transaction internals. Attractive later — it would let an external writer inherit DuckLake's conflict handling. |
| Kafka | Out of v1 scope. Tributary is a bulk reader with no offset commits, so adopting it means building the offset layer anyway. |
| Custom aggregates / native extension | Out of v1 scope. Revisit against DuckDB v2.0's stable C ABI. |
