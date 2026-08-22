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
is your first commit. `duckstream` needs `duckdb`, `pyarrow`, `numpy`; tests need
`pytest`. **Pin `duckdb` exactly**: constraints 7 and 8 in `CONTEXT.md` are
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
| `sufficient_statistics` | `AVG`, `STDDEV`, `VAR`, `CORR`, `COVAR` | Persist `sum`, `sum_sq`, `count`; derive the result on read. Exact, still no rescan. |
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
| `duckstream/model.py` | `Model` declaration, load-time validation, invariants |
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
| `tests/conformance/` | **runs against both DuckDB and DuckLake backends** |

## Phases

1. **Core loop on DuckLake.** `Model`, `Source`, `Sink`, `StateStore` protocols.
   File source with completion markers. `AvailableNow` trigger only — no
   background thread; cron or a supervisor drives it. **DuckLake state store and
   sink from the start**, with inlining disabled and one snapshot per trigger.
   Prove one exactly-once batch end to end, including a fault-injected replay
   verified against the snapshot history.
2. **Event time.** Watermarks, tumbling windows, sealing past the lateness
   horizon, `append` and `update` output modes via merge-by-key.
3. **Foldability.** All three tiers with load-time validation and the rejection
   path. Arrow-mode UDF helpers for tier three. Window-range chunking sized from
   estimated rows.
4. **Maintenance and small files.** Because inlining is off, every trigger writes
   a parquet file — so compaction is part of the framework, not an operator
   chore. A maintenance model running `ducklake_merge_adjacent_files`,
   `ducklake_expire_snapshots` and `ducklake_cleanup_old_files` on its own
   cadence, with the retention window set from the lateness horizon. Do **not**
   rely on `CHECKPOINT` to flush inlined data (ducklake#1368) — moot here, but do
   not reintroduce the dependency. Also add partitioning of sink tables by time
   grain, which is what makes window-range scans prune to a few files.
5. **MQTT landing writer.** Fills a genuinely empty slot — no MQTT extension for
   DuckDB exists. At-least-once into durable storage, replayable downstream.
6. **Validation on a real workload.** Point it at a real sensor pipeline and diff
   against a full recompute. This repo's accelerometer marts are a convenient
   reference case, exercising all three tiers (counts, averages/stddev, and a
   windowed FFT). Purely a test consumer — the framework must not acquire any
   dependency on it.

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
    key=["hour_ts", "sensor_id"],
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
SELECT * FROM lake.marts.hourly_counts ORDER BY hour_ts DESC LIMIT 5;
```

**An entry-point decision is still open** and should be settled in phase 1, since
it shapes `model.py`: script-only as above, or also a
`python -m duckstream run --config models.yaml` CLI. A cron-driven framework
arguably wants the CLI more than the importable API, and the choice determines
whether models are declared in Python or in config.

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
| Entry point | **Open, settle in phase 1.** Script-only versus a `python -m duckstream run --config ...` CLI. Determines whether models are declared in Python or config, so it shapes `model.py`. |
| Change feed as a source | **Post-v1**, and evaluate adopting `ducklake_cdc` first. It dies with snapshot expiry, so any consumer must **fail loudly** on a missing snapshot rather than skip silently. |
| DuckLake maintenance | Do not rely on `CHECKPOINT` to flush inlined data (ducklake#1368). Since v1 avoids inlining, the ordinary maintenance chain suffices. |
| Concurrency and locking | v1 is single-writer under `AvailableNow`, so contention is structurally impossible and **no lock is needed**. DuckLake additionally retries snapshot-id conflicts without rewriting data files. A portable lock arrives with `ProcessingTime`. Never `fcntl` — it is POSIX-only and breaks import on Windows. |
| `ducklake_add_data_files` as cheap ingestion | **Do not use.** Registration transfers file ownership to DuckLake, so maintenance may delete the landing file, and pruning is silently lost if the writer omits footer statistics. |
| `ducklake_commit` / staged commit | **Do not use.** In-tree but undocumented transaction internals. Attractive later — it would let an external writer inherit DuckLake's conflict handling. |
| Kafka | Out of v1 scope. Tributary is a bulk reader with no offset commits, so adopting it means building the offset layer anyway. |
| Custom aggregates / native extension | Out of v1 scope. Revisit against DuckDB v2.0's stable C ABI. |
