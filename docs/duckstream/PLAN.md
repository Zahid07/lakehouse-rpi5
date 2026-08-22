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
`AvailableNow` trigger, plain DuckDB backend, and a fault-injection test that
kills the process between sink write and commit and proves on restart that rows
are neither lost nor duplicated. Nothing else. Resist adding windows, DuckLake, or
extra sources until that test passes — it is the load-bearing claim of the whole
framework.

**Working agreements:**

- Every phase ends with tests passing, including the fault-injection test.
- Never assume DuckLake behaves like DuckDB; run conformance on both (constraint 7).
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
delivered as a Python library over DuckDB, with pluggable storage.

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
- **Not a DuckDB extension.** See `CONTEXT.md` for why, in detail.

## Architecture

Four planes. The trigger loop stays in the host process, never in the database.

```
Trigger   AvailableNow / Once / ProcessingTime   - cron or supervisor owns it
Plan      offsets in, bounded micro-batch out    - enforces memory limits
Execute   SQL over (micro-batch x state)         - DuckDB
State     offsets, watermarks, windows, metrics  - pluggable backend
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

**Storage is pluggable and DuckLake is optional.** A plain DuckDB file must work
as both state store and sink. This keeps the framework generic and avoids betting
v1 on DuckLake's newest surfaces (constraint 8).

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

1. **Core loop.** `Model`, `Source`, `Sink`, `StateStore` protocols. File source
   with completion markers. `AvailableNow` trigger only — no background thread;
   cron or a supervisor drives it. Plain DuckDB backend. Prove one exactly-once
   batch end to end, including a fault-injected replay.
2. **Event time.** Watermarks, tumbling windows, sealing past the lateness
   horizon, `append` and `update` output modes via merge-by-key.
3. **Foldability.** All three tiers with load-time validation and the rejection
   path. Arrow-mode UDF helpers for tier three. Window-range chunking sized from
   estimated rows.
4. **DuckLake backend.** State store and sink on DuckLake, one snapshot per
   trigger. Run the full conformance suite on both backends and expect divergence
   (constraint 7). Depends on neither inlining nor the change feed.
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
- **Both backends.** Whole suite against DuckDB and DuckLake (constraint 7).
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
| DuckLake inlining for low-latency small writes | **Do not use.** ~12 open correctness bugs filed in six weeks. Revisit only after they close, with a pinned version and a reconciliation check. Note the docs contradict themselves on the default (`0` on the ATTACH page, `10` in source — source wins). |
| Change feed as a source | **Post-v1**, and evaluate adopting `ducklake_cdc` first. It dies with snapshot expiry, so any consumer must **fail loudly** on a missing snapshot rather than skip silently. |
| DuckLake maintenance | Do not rely on `CHECKPOINT` to flush inlined data (ducklake#1368). Since v1 avoids inlining, the ordinary maintenance chain suffices. |
| Concurrency and locking | v1 is single-writer under `AvailableNow`, so contention is structurally impossible and **no lock is needed**. DuckLake additionally retries snapshot-id conflicts without rewriting data files. A portable lock arrives with `ProcessingTime`. Never `fcntl` — it is POSIX-only and breaks import on Windows. |
| `ducklake_add_data_files` as cheap ingestion | **Do not use.** Registration transfers file ownership to DuckLake, so maintenance may delete the landing file, and pruning is silently lost if the writer omits footer statistics. |
| `ducklake_commit` / staged commit | **Do not use.** In-tree but undocumented transaction internals. Attractive later — it would let an external writer inherit DuckLake's conflict handling. |
| Kafka | Out of v1 scope. Tributary is a bulk reader with no offset commits, so adopting it means building the offset layer anyway. |
| Custom aggregates / native extension | Out of v1 scope. Revisit against DuckDB v2.0's stable C ABI. |
