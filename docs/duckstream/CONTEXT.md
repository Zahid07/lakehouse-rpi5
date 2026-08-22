# duckstream — context, evidence, and settled decisions

Read this before `PLAN.md`. It exists so a new session does not re-research the
ecosystem or re-litigate decisions, and — more importantly — does not reason its
way to conclusions that measurement already contradicts.

Everything below was either **measured locally** or **verified against primary
sources**. Where something is unverified it says so explicitly. Nothing here is
recalled from memory.

Environment used for all measurements: **duckdb 1.5.5**, ducklake extension
`d8a1881e` (the `v1.5-variegata` line), Windows dev box, `SET threads=2` where
noted to approximate a Raspberry Pi 5.

---

## 1. Measured constraints

These are the load-bearing numbers. **If one conflicts with your intuition, trust
the number or re-measure it — do not reason around it.**

### 1.1 The memory ceiling is DuckDB's buffer manager, not Python

**Method.** Built a table of 2,400,000 rows across 400 groups (6,000 rows each).
For each query variant, created a fresh connection at a candidate `memory_limit`
and bisected upward over `64, 128, 256, 512, 1024, 2048, 4096 MB` to find the
minimum limit at which the query completed.

| Query | Minimum `memory_limit` |
|---|---|
| plain `GROUP BY` (avg, stddev — no `LIST`) | 64 MB |
| `LIST(x ORDER BY ts)` only, no UDF | **256 MB** |
| `LIST(...)` + native Python UDF | **256 MB** |
| `LIST(...)` + Arrow-mode Python UDF | **256 MB** |

**Conclusion.** The three `LIST` variants are identical. What consumes memory is
DuckDB materialising the list aggregate inside its buffer manager; the Python
layer is not the constraint. A faster or native UDF buys **no** memory headroom.

**Consequence for the design.** Memory is controlled by bounding **rows in flight
per execution** (`max_rows_per_trigger`, window-range chunking), never by
optimising the UDF. This is the reasoning behind the memory-control section of
`PLAN.md`.

**Historical note.** This finding corrected an earlier wrong conclusion in this
project: a real out-of-memory failure (`could not allocate block of size 256.0
KiB (3.1 GiB/3.1 GiB used)`) was initially attributed to Python UDF marshalling.
The `3.1 GiB` figure is DuckDB's buffer-manager accounting, so the attribution was
wrong. Do not repeat it.

### 1.2 Arrow-mode UDFs give ~2x, and `LIST -> LIST` works in Arrow mode

**Method.** 720,000 rows in 120 groups of 6,000. Same numpy `rfft` computation
registered twice: once as a native Python UDF taking a `list`, once in Arrow mode.
The Arrow version takes one zero-copy numpy view over the flattened child buffer
and slices per group using the offsets buffer, so no Python float object is ever
created. Best of 3 runs each, `threads=2`.

| Mode | Time |
|---|---|
| native (`type` omitted) | 219.6 ms |
| Arrow (`PythonUDFType.ARROW`) | 106.5 ms |

**2.06x speedup, byte-identical output** (verified with `numpy.allclose`).

**`LIST(DOUBLE) -> LIST(DOUBLE)` in Arrow mode is undocumented** — the DuckDB docs
do not confirm list return types for Arrow-mode UDFs. It is **verified working on
1.5.5**. Return a `pyarrow.array(..., type=pa.list_(pa.float64()))` of exactly the
input length.

Working shape:

```python
def fft_arrow(arr):
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    flat = arr.flatten().to_numpy(zero_copy_only=False)
    offs = arr.offsets.to_numpy()
    return pa.array(
        [np.abs(np.fft.rfft(flat[offs[i]:offs[i + 1]])) for i in range(len(arr))],
        type=pa.list_(pa.float64()),
    )

con.create_function("fft_arrow", fft_arrow, [LIST_DOUBLE], LIST_DOUBLE,
                    type=duckdb.functional.PythonUDFType.ARROW)
```

Gotchas to design around: inputs may arrive as `ChunkedArray`, not `Array`; use
`null_handling='special'` if the function must see or return NULLs; STRUCT returns
historically required alphabetically ordered fields (duckdb#10808, closed) so
verify before relying on them.

### 1.3 A DuckDB table is a viable state store

**Method.** State table keyed `(win, key)` with a primary key. Each of 60
successive triggers built a 30,000-row batch spread over 4 windows x 50 keys
(200 state rows touched) and timed a single `MERGE` folding count/sum/sum_sq.
`threads=2`.

| | Result |
|---|---|
| first 5 triggers, mean | 3.3 ms |
| last 5 triggers, mean | 3.2 ms |
| growth factor | 0.98x (flat) |
| final state size | 6,000 rows |

**Conclusion.** No degradation as state grows at this scale. A plain DuckDB table
is adequate as the state store — there is no need for a RocksDB equivalent, which
is what Spark Structured Streaming requires. State access will not be the latency
floor.

**Caveat.** Only tested to 6,000 state rows. If state reaches millions of open
windows, re-measure and add eviction of sealed windows.

### 1.4 DuckLake commits one snapshot per transaction, not per statement

**Method.** Counted rows from `ducklake_snapshots()` before and after.

- 5 sequential `INSERT` statements (autocommit) produced **5 snapshots**.
- 2 `INSERT` statements inside `BEGIN ... COMMIT` produced **1 snapshot**.

**Consequence.** This is the primitive that makes exactly-once cheap: sink rows,
watermark, window state and source offset can all be written in one transaction
and become durable atomically in a single snapshot. On a plain DuckDB backend the
same guarantee comes from an ordinary transaction.

### 1.5 DuckLake SQL is not DuckDB SQL

**Method.** A window-scoped `MERGE` whose source subquery referenced bounds via a
scalar subquery in the join condition — `ON f.timestamp >= (SELECT lo FROM bounds)`.
Ran the identical statement against in-memory DuckDB and against DuckLake-backed
tables, then bisected the constructs.

| Variant (DuckLake-backed) | Result |
|---|---|
| scalar subquery in join + `IS NOT DISTINCT FROM` | `Error: Out of buffer` |
| no scalar subquery, `IS NOT DISTINCT FROM` kept | passes |
| scalar subquery kept, plain `=` everywhere | `Error: Out of buffer` |
| no scalar subquery, plain `=` | passes |
| the original failing variant on **plain in-memory DuckDB** | passes |

**Conclusion.** The scalar subquery in the join is the trigger; `IS NOT DISTINCT
FROM` is not implicated. The failure appeared only on the **second** MERGE — the
first one to take the `WHEN MATCHED` branch — so a single-batch test would have
missed it entirely.

**Consequence.** The conformance suite must run against **DuckLake tables, not
in-memory DuckDB**, and must exercise at least two batches so the matched branch
is reached. Bounds are computed in the host language and inlined as literals,
which also lets DuckLake prune files on timestamp statistics.

---

## 2. Researched constraints

Verified against primary sources. Issue numbers are given so they can be
re-checked — several are open and may have changed.

### 2.1 Python UDFs force single-threaded execution

A query containing a Python UDF is forced onto a single thread
(duckdb#14817); "Parallel Python UDFs" remains an open item on the DuckDB roadmap.

**Consequence.** On a 4-core device, one pipeline containing a UDF will not use
all cores. Push aggregation into native SQL wherever a foldable tier permits, keep
UDFs off the hot path, and parallelise across **processes** rather than expecting
intra-query threads. This is a first-class architectural constraint for a
Python-based framework, not a footnote.

### 2.2 No custom aggregates from Python

`create_function` is scalar-only. There is no `create_aggregate_function` in the
Python API — confirmed by inspecting the client:

```
PythonUDFType members: ['ARROW', 'NATIVE']
has create_aggregate_function: False
```

Vectorized user-defined aggregates have been aspirational since 2023
(duckdb#5116, closed without shipping). Custom aggregates are C++-only.

**Consequence.** `non_foldable` models use the `LIST(x ORDER BY t)` -> scalar
Arrow UDF shape. The community extension `python_udf` does expose `py_agg`, but it
is pre-1.0 with a single maintainer — prefer the built-in route.

### 2.3 DuckLake inlining and the change feed are its buggiest surfaces

Roughly a dozen correctness issues were filed against `duckdb/ducklake` in a
six-week window, concentrated in exactly these two features:

| Issue | Problem |
|---|---|
| 1368 | `CHECKPOINT` does not flush inlined data to disk |
| 1387 | `table_changes` reports a single update **four times** when two MERGEs share a transaction |
| 1385 | Flush of a sorted inlined table computes delete positions with a text sort, tombstoning the **wrong rows** |
| 1364 | Inlined delete-marks retained after flush on partitioned tables, double-applied deletes, negative `count(*)` |
| 1329 | `count(*)` undercounts when Parquet and inlined tombstones overlap |
| 1335 | Inlining stamps a stale `next_file_id`, causing persistent primary-key collisions |
| 1390/1391 | Dead inlined rows never reclaimed |
| 1305 | Stale membership, concurrent reader sees a missing inlined table after flush |

**Consequence.** v1 depends on **neither** inlining nor the change feed. This
reverses an earlier recommendation in this project to raise
`ducklake_default_data_inlining_row_limit` and to use `CHECKPOINT` for
maintenance — both land squarely in the buggy area. Revisit only after these
close, with a pinned version and a reconciliation check.

Note also a documentation contradiction: the ATTACH page gives the inlining
default as `0`, the source gives `10`. The source wins.

### 2.4 The change feed does not survive snapshot expiry

Documented behaviour: compaction and snapshot expiry limit what the change feed
can return, and expired history is gone. **Retention is the CDC horizon** — an
architectural constraint on consumer lag, not a tuning knob. A consumer that falls
behind past expiry loses changes.

**Consequence.** Any future CDC source must **fail loudly** when its stored
snapshot is gone, never silently resume from a later point.

### 2.5 DuckLake concurrency

Optimistic, enforced by the primary key on `ducklake_snapshot.snapshot_id`. On
conflict DuckLake inspects the intervening snapshot's changes and, if there is no
logical conflict, **retries without rewriting staged data files**. Tunables:
`ducklake_max_retry_count` (10), `ducklake_retry_wait_ms` (100),
`ducklake_retry_backoff` (1.5).

Catalog guidance: DuckDB file for a single client, **SQLite for multiple local
processes**, PostgreSQL for multi-user, MySQL not recommended. Real-world reports
(ducklake#233) show many small concurrent committers exhaust retries — **fewer,
fatter commits beat many thin ones.**

**Consequence.** v1 is single-writer under an `AvailableNow` trigger, so
contention is structurally impossible and no lock is required.

### 2.6 Why not a DuckDB extension

Four independent reasons, any one sufficient to defer:

1. **The operator you would want is not viable.** Table in-out functions are the
   only stateful rows-in/rows-out hook. C++-only; **cannot be async** — no
   `InterruptState`, so you cannot return `BLOCKED` (duckdb#18856); lost
   parallelism in v1.5.1 (duckdb#21617); cannot detect end of input
   (duckdb#18222); no progress reporting (duckdb#21707). Open defects, not
   design choices to route around.
2. **No supported API for background work.** No scheduler, timer or idle hook is
   exposed; the docs neither permit nor forbid it. Practice is a raw
   `std::thread` owning a `DatabaseInstance` — the `cronjob` extension does this
   and its own description says "do not use it in production". Nothing runs when
   no query is executing.
3. **DuckDB v2.0 is announced but unreleased** ("this fall", 2026) and brings a
   stable, frozen C ABI plus a C++ wrapper that talks only to it, ending
   per-release rebuilds. Today's C++ API also churns at source level —
   `ExtensionUtil` was deleted outright (duckdb#17772), so pre-2025 examples do
   not compile. Extension work should target v2.0, not the API it replaces.
4. **A native aggregate would not fix the memory ceiling.** `Combine` must be
   associative, and partial spectra cannot be combined into the spectrum of a
   concatenation, so a windowed transform must buffer raw samples and stays
   O(rows per window). `Operation` also sees rows unordered under parallelism,
   which a time-ordered transform cannot accept. Consistent with measurement 1.1:
   expect a constant factor, not an asymptotic win.

If an extension is later warranted, the slice is a **scalar function over `LIST`
against the v2.0 C API** — the most stable hook, present in both C++ and C APIs.
The C API already supports scalar, aggregate, table, cast and replacement-scan
registration; it does **not** expose table in-out, optimizer, parser or storage
extensions, and there is no public statement that it ever will.

Distribution notes: community extensions build `linux_arm64` as a first-class
platform (glibc required — verify with `PRAGMA platform;` on the target device,
expect `linux_arm64`). But there is **no user-side version pinning**: rebuilds
replace artifacts at the same path, which is a real risk for appliance
deployments.

---

## 3. Prior art

The premise "nothing exists for streaming on DuckDB" is mostly right but not
entirely, and the exceptions matter.

DuckDB core has **no materialized views** — that sits in the "future work /
seeking funding" bucket of the roadmap. DuckLake's roadmap likewise lists
"materialized views and incremental maintenance" as future work. So the gap is
acknowledged by the projects themselves.

| Project | What it does | Where it stops |
|---|---|---|
| **Tributary** (MIT, maintained, Query.Farm) | Kafka topic reader, Confluent Schema Registry surface, `linux_arm64` builds, daily CI | **Stateless.** Uses librdkafka's legacy simple consumer with explicit per-partition `start()`, reads partition-begin to high-watermark every scan, never commits offsets. Open: #6 commit reads, #12 track offsets, #7 stateful processing, #19 bounded reads, #14 SIGSEGV, #15 returns no rows. No producer. A bulk reader, not a source. |
| **ducklake_cdc** (Apache-2.0) | Durable consumer cursors over DuckLake's change feed, single-reader leases, `cdc_commit(snapshot_id)`, tick listeners, typed DDL events, Python client | **Self-declared pre-alpha**; API names and row shapes may change. Explicitly not for very high throughput or sub-50ms latency. **This is genuinely the same idea for the CDC slice, and it ships.** Evaluate adopting or contributing before rebuilding. |
| **radio** (MIT, Query.Farm) | WebSocket + Redis pub/sub, per-subscription background threads, SQL-queryable receive queue | In-memory bounded queue drops old messages: at-most-once, lost on restart. MQTT is listed as planned, not implemented. |
| **nats_js** | NATS JetStream with **bounded reads** by sequence range and time range | No durable consumer state. Better design reference than Tributary for the bounded-read primitive. |
| **OpenIVM** (research) | SQL-to-SQL incremental view maintenance compiler built on DuckDB, SIGMOD 2024 | Research prototype, not in the community registry. The right prior art to *read*, not to depend on. |
| **BoilStream** | Rust server, Kafka / Arrow-Flight ingestion into DuckLake with ~1s commits | Single-vendor, commercial posture, separate service. |
| **cronjob** | In-process scheduled SQL | Its own description says "experimental and potentially unstable. Do not use it in production." |
| **airport** | Arrow Flight client for DuckDB, plus a Python Flight server framework | A credible ingestion transport, not a streaming engine. |

**There is no MQTT extension for DuckDB at all.** Every published MQTT-to-DuckDB
story is broker-side (e.g. EMQX writing Parquet) or glue via Node-RED/Telegraf.
Genuinely empty slot.

**The actual gap:** no maintained library does the whole loop — durable source
offsets *plus* event-time windowing *plus* correct incremental aggregation *plus*
idempotent sinks, in-process. Each project above covers one slice.

### Build versus adopt — state this honestly

Two alternatives genuinely run on constrained ARM64 hardware:

- **Arroyo** (Apache-2.0, Rust) — real arm64 images, vendor claims tens of MBs of
  RAM, SQL with windows, joins and exactly-once.
- **Timeplus Proton** (Apache-2.0, C++) — single binary, documented running on a
  0.5 GiB instance, no JVM or ZooKeeper.

If the requirement is only "SQL streaming on small hardware", those already solve
it and are a smaller lift than building. The defensible reason to build is to
**stay inside DuckDB** — where the data, catalog and analytics stack already live,
with no extra service — and to provide the correctness guarantee in section 4 that
none of them offer. If that is not the reason, adopt Arroyo.

Ruled out for constrained hardware: **RisingWave** (component minimums exceed a
Pi before object storage), **Materialize** (wants Kubernetes plus PostgreSQL plus
blob storage; BSL), **ksqlDB** (amd64-only image, JVM, needs a Kafka cluster;
de-emphasised by its vendor), **Estuary** (BSL self-host, cloud-first).
**Feldera/DBSP** is the most interesting for true IVM semantics but defaults to
about 4 GiB across 8 workers and needs PostgreSQL.

### Terminology trap

DuckDB does have "streaming" in an unrelated sense — lazy/chunked result streaming
(`fetch_arrow_reader`). If a source claims "DuckDB supports streaming", check
which meaning is intended. It is not continuous ingestion.

---

## 4. Settled decisions

Do not re-litigate these without new evidence.

| Decision | Choice | Why |
|---|---|---|
| Delivery form | **Python library** over `duckdb`. No C++ extension in v1. | Section 2.6: the needed operator is defective, background work is unsupported, and v2.0 replaces the API within months. |
| Semantics | **Micro-batch plus event time**: offsets, triggers, tumbling windows, watermarks, append/update output. | The core that makes it recognizably structured streaming. Stateful joins and arbitrary keyed state are explicitly post-v1. |
| First sources | **File/directory tailing**, then **MQTT landing writer**. | The file source is replayable, which is what exactly-once requires. MQTT fills an empty slot but cannot itself be replayable. |
| Guarantee | **Exactly-once via atomic commit** — offsets and data in one transaction. | Measurement 1.4 makes this nearly free, and it is a structural advantage over bolting a sink onto Spark, where offset and output stores are separate systems. |
| Storage | **Pluggable; plain DuckDB must work.** DuckLake is one backend, added in phase 4. | Keeps the framework generic and avoids betting v1 on DuckLake's buggiest surfaces (2.3). |
| MQTT modelling | **Landing writer, not a source.** | MQTT has no replayable offset; once a message is acked it is gone. Exactly-once is impossible directly, so land durably first and treat that as the source. |
| Differentiator | **Foldability classification** with load-time rejection. | See below. |

### Why foldability is the thing worth building

An incremental merge is only correct if the aggregate forms a monoid over
batches. Three tiers: `additive` (`COUNT`/`SUM`/`MIN`/`MAX`),
`sufficient_statistics` (`AVG`/`STDDEV`/`VAR` — store `sum`, `sum_sq`, `count`),
and `non_foldable` (median, exact `COUNT DISTINCT`, order-dependent aggregates,
any whole-window UDF).

This is not theoretical. In this repository, two production marts were computing
wrong numbers for exactly this reason, and both were caught by comparing against a
full recompute:

- An hourly summary folded averages as `(target.avg + source.avg) / 2` — an
  unweighted average of averages. With 300 samples of 1.0 followed by 100 samples
  of 5.0, the correct average is **2.0**; the mart held **3.0**. Standard
  deviation was simply overwritten by the last batch: correct **1.7342**, stored
  **0.0**.
- An FFT mart transformed only the current batch's rows, so a one-minute window
  fed by 30-second batches held a spectrum over half a window: **sample_count
  100** instead of 400, and **51 spectrum bins** instead of 201.

No mainstream tool distinguishes tier one from tier three, which is why this bug
class is common. The framework refusing an additive strategy over a non-foldable
aggregate — at load time, not runtime — is its reason to exist. The fix pattern
that worked here was recompute-by-window scoped to the windows a batch actually
touched, which is also idempotent and therefore safe to retry.

---

## 5. Reference implementation to learn from

This repository contains a working, narrower version of the same idea. Read it for
patterns, but **do not import from it** — `duckstream/` must stay standalone.

| Location | Pattern worth reusing |
|---|---|
| `realtime_queue_worker.py` `get_ready_folders` | Completion-marker gating plus a settle delay, so partially written directories are never read. |
| `realtime_queue_worker.py` `upsert_queue_jobs` | Offset/queue state keyed by path, tracking size and mtime to detect changed files. |
| `realtime_queue_worker.py` `mark_batch_failed` | Exponential backoff with a capped exponent. |
| `run_pipeline.py` `get_touched_windows` | Deriving affected windows from a batch — the planning step for a `non_foldable` model. |
| `run_pipeline.py` `run_window_chunked_step` | Window-range chunking with literal `[lo, hi)` bounds passed into SQL. |
| `run_pipeline.py` `ensure_benchmark_table` / `write_benchmark` | Per-step metrics table. Extend with lag. |
| `subscriber.py` writer thread | The correct durable-landing pattern: write to a temp path, atomic rename, **then** drop the completion marker. Never the other order. |
| `utils/udf_registry.py` | The FFT UDFs, in native mode. Port to Arrow mode (1.2). |

Two known defects in that code that must **not** be carried into `duckstream/`:

- `fcntl` is POSIX-only, so `realtime_queue_worker.py` cannot even be imported on
  Windows. v1 needs no lock; when `ProcessingTime` arrives, use a portable one.
- Both drivers `CREATE OR REPLACE` the same staging table, so concurrent runs
  clobber each other. Staging names must be run- or batch-scoped.

Its `duckdb_pipeline_notes.md` records the original out-of-memory incident and the
mart correctness fixes in more detail.

---

## 6. Still unverified

Do not treat these as known.

- **Micro-batch latency floor.** Trigger overhead (plan, execute, commit) with an
  empty batch, per backend. Needs writing a catalog, so it was deferred out of
  planning. State merge is ~3.2 ms, so this number is dominated by commit cost and
  determines the minimum usable trigger interval. **Measure early — it bounds the
  product.**
- **Change-feed cost** on a large table. No published guidance.
- **Whether the change feed survives `expire_snapshots`** in practice, beyond the
  documented statement that expired history is unavailable.
- **`PRAGMA platform;` on the target Pi 5.** Must print `linux_arm64` for prebuilt
  community extensions to be an option at all. Affects only future extension work.
- **Whether community-extensions CI accepts a C-API extension today.** The C
  template is marked experimental with community support "coming soon".
- **DuckDB v2.0's final scope.** Announced, unreleased, and the announcement says
  details may shift.
