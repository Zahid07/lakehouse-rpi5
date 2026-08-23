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
the number or re-measure it — do not reason around it.** §1.8, §1.9 and §1.10
were measured during the phase-1 build; §1.11 during phase 2.

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

### 1.6 A DuckDB file cannot be shared across processes

**Method.** Held `lock_test.duckdb` open read-write in one process, then tried to
open it from a second process.

| Second process | Result |
|---|---|
| `read_only=True` | **FAILED** — lock error naming the holding PID |
| read-write | **FAILED** — same |
| read-write, after the first released it | OK |

**Conclusion.** While one process holds a DuckDB *file*, no other process can open
it **even read-only**. A long-running engine on a single DuckDB file would lock
the user out of their own warehouse.

**Consequence.** Two things follow. `AvailableNow` under cron is safe because the
process opens, drains and closes, leaving the file free between runs. And it is an
independent argument for DuckLake as the storage layer: the catalog is a separate
database and the data is parquet, so readers attach freely while the engine
writes. This is what makes a `ProcessingTime` daemon viable at all.

### 1.7 DuckLake inlining silently captures small batches

**Method.** Fresh DuckLake catalog, `CREATE TABLE t (i INTEGER)`, then
`INSERT INTO t SELECT * FROM range(0,3)` — 3 rows, below the default limit of 10.
Compared `ducklake_default_data_inlining_row_limit` at its default versus `0`,
checking both `ducklake_list_files` and parquet files on disk.

| Setting | `ducklake_list_files` | parquet on disk | Where the rows went |
|---|---|---|---|
| default (`10`) | 0 | 0 | **inlined into the catalog** |
| `0` | 1 | 1 | wrote a parquet data file |

**Conclusion.** Inlining is on by default and captures any write smaller than 10
rows. For a streaming engine this means **batch size decides which code path you
are on** — and the small-batch path is the one carrying the open correctness bugs
in 2.3. Behaviour therefore varies between a busy trigger and a quiet one, which
is the worst kind of surface to debug.

**Consequence.** The engine sets `ducklake_default_data_inlining_row_limit = 0`
explicitly. The cost is one parquet file per trigger, which makes compaction a
framework-owned concern rather than an operator chore. Revisit when the inlining
bugs close — inlining is genuinely the right long-term answer to small writes, and
DuckLake's own benchmarks show large gains from it.

Also verified: **`ATTACH 'ducklake:...'` autoloads the extension** — no explicit
`INSTALL`/`LOAD` required (`loaded=True, installed=True` afterwards). But autoload
still needs the extension present or downloadable, so run `INSTALL ducklake` once
on a target device while it has network. Being explicit costs nothing and avoids a
first-run failure on a disconnected deployment.

### 1.8 The micro-batch latency floor is the DuckLake commit, ~17 ms

**Method.** Measured during implementation, on the same box, `threads=2`, 30
repetitions each, reporting the median. A "trigger" is one `BEGIN` … `COMMIT`
containing a scan plus a state write, which is the shape the engine actually uses.

| Trigger shape | Median | Snapshot? |
|---|---|---|
| plain DuckDB file, empty batch | **1.10 ms** | n/a |
| DuckLake, read-only, nothing written | **1.28 ms** | no |
| DuckLake, empty batch, one state write | **16.76 ms** | yes |
| DuckLake, 100-row batch plus state write | **36.53 ms** | yes |

**Conclusion.** The floor is the snapshot, not the query. A DuckLake transaction
that writes nothing costs the same as plain DuckDB; the moment it writes, it pays
roughly **15 ms of commit**. This is consistent with 1.3 — state merge at ~3.2 ms
was never going to be the latency floor — and it confirms the commit was the thing
to measure.

**Cold start**, which every cron tick pays before doing any work:

| Stage | Median |
|---|---|
| `LOAD` + `ATTACH` + settings, in-process | 77.9 ms |
| first query after attach | 5.6 ms |
| whole process, wall clock | **234.9 ms** |

**Consequence for the design.** Per model per trigger, budget ~20 ms of unavoidable
overhead on an idle pass and ~40 ms on a small one. Under cron the real floor is
about **0.3 s** including interpreter start, so a sub-second cron trigger is
meaningless and seconds is the sensible unit. A future `ProcessingTime` daemon
skips the 235 ms and lands near the ~17 ms commit floor. Note also that an idle
trigger which writes no state stays at ~1.3 ms — so the engine should **not**
write a checkpoint when a batch is empty, and that is worth an explicit test.

Also observed: 30 small batches produced **30 parquet files**. Inlining is off by
design (1.7), so compaction is a framework concern, not an operator chore — this
is the measured justification for phase 4.

### 1.9 One transaction can write to only one attached database

**Method.** Inside a single transaction, wrote to `memory.main.sink` and then to
`lake.duckstream.offsets`.

| | Result |
|---|---|
| write two attached databases in one transaction | **`TransactionContext Error`** |
| write one attached database in one transaction | OK |

**Conclusion.** DuckDB will not span a transaction across attached databases.

**Consequence — this is load-bearing for the whole exactly-once claim.** The sink
and the state store must live in **the same** catalog, which means both inside
DuckLake. Sinking to DuckLake while checkpointing offsets to a local DuckDB file
is not merely inadvisable, it is impossible: the one-transaction/one-snapshot
guarantee in 1.4 cannot be formed across two databases. This is an independent
confirmation of the "DuckLake from phase 1, not an optional backend" decision in
§4, and it fixes the status of the plain-DuckDB `StateStore` for good — it is
usable only when the sink is also plain DuckDB, i.e. in unit tests, exactly as
`PLAN.md` says.

### 1.10 A DuckLake DELETE that matches a row costs ~26 ms

**Method.** Isolated timings of single-row statements against a DuckLake table,
20 repetitions, median.

| Statement | Cost |
|---|---|
| `INSERT` one row | ~8 ms |
| `DELETE` matching nothing | free |
| `DELETE` matching one row | **~26 ms** |
| `UPDATE` one row | ~30 ms |

A matching `DELETE` writes a tombstone file, and that file write is the cost.

**Consequence.** Any per-trigger state kept as *one mutable row per model* pays
~26 ms per state table per trigger — with offsets, watermarks and a batch record
that reached **106 ms per trigger**, six times the 1.8 floor. Guarding the delete
behind an existence probe was measured and is a **net regression** (114 ms vs
102 ms): in steady state the row always exists, so the probe is pure added cost.

The fix is structural, not a tuning knob: keep per-trigger state **append-only**
and read the newest row back with `ORDER BY batch_id DESC LIMIT 1`. Appends cost
~8 ms and write no tombstone. This is also strictly safer for crash recovery,
since an uncommitted append is simply invisible. The cost is unbounded growth,
which a `prune` helper bounds and phase 4 maintenance schedules.

**Implemented and re-measured**, 40 reps, median, no drift between the first ten
triggers and the last ten:

| Trigger shape | Mutable-row | Append-only |
|---|---|---|
| idle: begin + empty commit | 0.09 ms | 0.11 ms |
| sink insert only, no state | 8.58 ms | 9.41 ms |
| sink + offset | 48.57 ms | **14.91 ms** |
| sink + offset + watermark | 75.19 ms | 20.29 ms |
| full, incl. batch record | **106.01 ms** | **25.73 ms** |

**4.1x.** Each state append now costs ~5.4 ms flat.

One trap worth knowing, because it inverts the obvious reading: once the writes
were cheap, `SELECT max(batch_id)` became the dominant term, and the path doing
*strictly more work* measured **faster** (27 ms) than the cheaper one (39 ms),
because recording a batch happened to populate the id in memory and skip that
read. Memoising the last committed batch id per model — populated only on
successful commit, so a rollback leaves it untouched — is what took `sink +
offset` from 39 ms to 14.9 ms. Sound only because v1 is single-writer under
`AvailableNow` (§2.5); revisit it the moment a second writer exists.

### 1.11 A watermark read per trigger costs 10.4 ms; the same fix applies

**Method.** Measured while building phase 2, on the same box, `threads=2`, 40
repetitions, median. Four variants of an otherwise identical model were run
**interleaved** — one trigger of each, in turn — because a first attempt that
ran them one after another gave a baseline of 100.8 ms on one run and 67.8 ms
on the next. Machine drift over a two-minute run swamped the effect. Interleave
anything measured on this box that takes minutes.

The per-trigger breakdown, against a DuckLake state table holding 40 rows:

| Operation | Median |
|---|---|
| `load_offset` — phase 1 already pays this | 11.90 ms |
| **`load_watermark` — the new per-trigger read** | **10.36 ms** |
| `count(*)` over the bound batch — phase 1 | 1.84 ms |
| one scan for counts + `max(event_ts)` — the new one | 2.10 ms |
| appending one offset row in a transaction | 9.41 ms |
| appending one watermark row in a transaction | 9.62 ms |

**Conclusion, and it is §1.10 again.** The extra *scan* of the batch is free:
reading the newest event time and both drop counts alongside the row count
costs **0.26 ms** more than the `count(*)` it replaces, because it is the same
single pass. What is not free is re-reading a value the process wrote itself.
`load_watermark` is `ORDER BY batch_id DESC LIMIT 1` against a DuckLake table,
and it costs the same ~10 ms as the `max(batch_id)` scan §1.10 already removed.

**Consequence.** The engine memoises the committed watermark per model, exactly
as it memoises the batch id, and writes the cache **only after a successful
commit** so a rolled-back batch leaves it at the last durable value. Same
soundness argument, same expiry condition: it holds because v1 is single-writer
(§2.5), and both memos need revisiting together the day a second writer exists.

Whole-trigger cost, interleaved, 40 reps, median, 100-row batches:

| Trigger shape | Before the memo | After |
|---|---|---|
| `update`, no horizon (phase 1) | baseline | baseline |
| `update`, horizon, nothing dropped | +30.6 ms | **+5.1 ms** |
| `update`, horizon, a late row every 4th batch | +36.6 ms | **+6.4 ms** |
| `append`, horizon, sealing every window | +49.2 ms | **+20.4 ms** |

**So a lateness horizon costs about one extra state append — ~5 ms — and that
is irreducible**: the watermark has to become durable in the same transaction
as the offset, or a restart resumes reading from one point in the stream and
judging lateness from another. Filtering adds ~1 ms more, and only when
something is actually dropped: the engine creates the filter view only if the
scan reported rows to remove, so a healthy stream never builds a second view.

Sealed `append` costs ~20 ms more than phase 1, which buys three extra
statements in the same transaction — the merge into the open-window
accumulator, the insert of sealed windows into the target, and the delete that
evicts them. The delete is the one §1.10 warns about, and it is deliberate
here: it fires per *window sealed*, not per trigger, and it is what bounds the
accumulator by the lateness horizon instead of by the age of the stream. §1.3's
own caveat asks for exactly this.

### 1.12 Reduce state where it lives; do not read it back to reduce it

**Method.** ``status`` summed the batch history in Python: it called
``batch_history``, which returns every recorded batch, and added the columns up.
Timed against a DuckLake ``batches`` table at three sizes, five repetitions,
median, `threads=2`.

| Batch history | Python sum | SQL aggregate |
|---|---|---|
| 1,000 | 61.0 ms | 46.8 ms |
| 10,000 | 71.7 ms | 45.4 ms |
| 100,000 | **213.5 ms** | **48.1 ms** |

**Conclusion.** O(n) against O(1), and *faster in absolute terms at every size*
— the aggregate never materialises a row. One trigger a minute is 525,000
batches a year and nothing prunes them until phase-4 maintenance schedules it,
so the Python version had no ceiling.

**Consequence, and it is §1.10 for the third time.** That constraint has now
been paid in three different disguises: `max(batch_id)` per trigger, then
`load_watermark` per trigger (§1.11), then this. Each looked like a different
problem and each had the same answer — do not move state to the arithmetic when
the arithmetic can go to the state. The general rule is worth stating on its own
because it will appear again: **anything that reads a state table and then loops
over the result in Python is a defect waiting for the table to grow.**

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

**Consequence.** These are *peripheral* features, not DuckLake's core table
storage, snapshots or transactions — so this is **not** an argument against
building on DuckLake. It is an argument for depending on neither inlining nor the
change feed. v1 uses DuckLake as its storage layer from phase 1, with inlining
explicitly disabled (see 1.7) and no change-feed source.

This also reverses an earlier recommendation in this project to *raise*
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

**Consequence.** v1 is single-writer under an `AvailableNow` trigger.

**Corrected during phase 2b.** This section previously said contention was
"structurally impossible and no lock is required". That is very nearly true and
not quite, and the gap is a real 3am incident. `AvailableNow` drains until the
source is empty, so a backlog can make one cron tick outlast the interval that
started it, and the next tick then begins while the first is still running.

What prevents corruption is not the trigger — it is §1.6, the DuckDB file lock
on the catalog. Verified by running it: the second process fails to attach with

```
Binder Error: Failed to attach DuckLake MetaData "__ducklake_metadata_lake"
at path "…/catalog.ducklake" Unique file handle conflict: Cannot attach …
```

So the *safety* claim stands and only the *reasoning* was wrong. duckstream now
takes an advisory lock (`duckstream/lock.py`) before touching the catalog, so
the failure reads as "another duckstream run already holds this catalog: pid N
on host, running for Ns" instead. The advisory lock is never trusted for safety
— a lock trusted for safety is one that fails open on a filesystem it does not
understand — and `AvailableNow(max_batches=N)` is the knob that stops a tick
outrunning its own schedule in the first place.

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
| Storage | **DuckLake, from phase 1.** Not an optional backend. The plain-DuckDB `StateStore` exists only to keep unit tests fast, and is never the sole test gate. | The deliverable is a streaming engine *for a lakehouse* — parquet data, a SQL catalog, snapshots, time travel, schema evolution. The bugs in 2.3 are in inlining and the change feed, not in core storage, so they are avoided by disabling those two features (1.7) rather than by avoiding DuckLake. Measurement 1.6 independently rules out a shared single-file DuckDB. |
| Inlining | **Explicitly disabled** (`= 0`). | It defaults to 10 rows and silently captures small batches into the buggiest path (1.7). Accept small files, compact instead. |
| MQTT modelling | **Landing writer, not a source.** | MQTT has no replayable offset; once a message is acked it is gone. Exactly-once is impossible directly, so land durably first and treat that as the source. |
| Entry points | **Both**: a Python API and a config-driven CLI, built together in phase 1 over one canonical `Model`. | A cron-driven framework wants the CLI; library users want the API. Retrofitting config onto a Python-shaped API costs far more than building both at once. The loader is a deserialiser only — no parallel validation, no parallel execution path, no override or precedence semantics, which is where config layers usually rot. Drift is prevented mechanically by a config round-trip test and by running every conformance scenario through both doors. |
| Config format | **YAML** via `pyyaml`, parsing isolated in `config.py`. | Nested model declarations read far better than the alternatives, and YAML is the norm for data tooling. Isolating the parser keeps stdlib `tomllib` a cheap swap if the dependency becomes unwelcome on a constrained device. |
| Callables in config | **Registry with dotted-path resolution.** | Config expresses declarative structure but cannot express functions. Built-in names (`file`, `mqtt`, `table`) plus `my_pkg.mod:obj` for user sources, sinks and UDFs keeps config fully capable without turning it into a programming language. |
| Differentiator | **Foldability classification** with load-time rejection. | See below. |
| Unprocessable data | **Quarantine by default** (`on_failure`), after a bounded retry budget: skip the batch, record the loss permanently in `duckstream.quarantine`, exit non-zero. `halt` never advances past it. | Halting does not preserve the unprocessable data — a stream blocked on one bad file stops collecting everything behind it too — so continuing loses strictly less. It is a *policy* rather than a defect only because it is never silent: skip and record are one transaction, the table is never pruned, and `status` keeps reporting it after the log has rotated. |
| Retry state | **In the `offsets` row**, not a second table. | §1.11: a scalar read of a DuckLake state table costs ~10 ms and the engine already pays one per trigger. A second table would double that on every trigger to carry information that only matters when something is broken. |
| Attempt accounting | **Only failures that fail cleanly spend an attempt.** A hard kill records nothing. | A crash-looping deployment must not be able to quarantine its own data; infrastructure trouble is not bad data. |
| Lateness horizon | **Opt-in per model** (`lateness`), and it is what turns a model on to event time. Without it there is no watermark, every window stays open forever, and nothing is ever dropped — which is exactly phase-1 behaviour. | A horizon is a claim about the data, not a default anyone can pick correctly on a user's behalf. Making it opt-in also means the phase-1 path keeps costing what it cost (§1.11), so event time is paid for only by models that asked for it. |
| Windowed `append` | **Requires a horizon**, refused at load without one. Each window is folded in an open-window accumulator beside the target and written to the target **once**, when the watermark passes its end. | Phase 1 accepted `append` with a `grain` and wrote one *partial* row per window per batch. That equals the truth only when no two batches ever touch the same window — a condition the user cannot enforce and the engine never checked. It is the §4 bug class in the one place the framework had left it, so phase 2 refuses it and offers the correct mechanism instead. |
| Rows outside the horizon | **Dropped and counted**, durably, in `duckstream.batches`. Late rows (`rows_late`) and rows with no event time (`rows_undated`) are counted apart. | `PLAN.md` requires "dropped **and counted**, never silently absorbed", and a count that lives only in a return value or a rotated log has not been counted. Late and undated are separated because "arriving later than declared" and "carrying no timestamp" are different operational problems with different fixes. |

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

- ~~**Micro-batch latency floor.**~~ **Measured — see 1.8.** ~17 ms per committing
  trigger, ~1.3 ms if the trigger writes nothing, ~235 ms of process cold start
  under cron.
- **Change-feed cost** on a large table. No published guidance.
- **Whether the change feed survives `expire_snapshots`** in practice, beyond the
  documented statement that expired history is unavailable.
- **`PRAGMA platform;` on the target Pi 5.** Must print `linux_arm64` for prebuilt
  community extensions to be an option at all. Affects only future extension work.
- **Whether community-extensions CI accepts a C-API extension today.** The C
  template is marked experimental with community support "coming soon".
- **DuckDB v2.0's final scope.** Announced, unreleased, and the announcement says
  details may shift.
