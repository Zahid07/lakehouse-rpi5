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
were measured during the phase-1 build; §1.11 during phase 2; §1.16 during phase
4, where it corrected two figures in §1.15 that had been *derived* rather than
measured; §1.17, §1.18 and §1.19 during phase 3's tier three. "Trust the number" means
the measured one — a number computed from another number is an argument, and
§1.16 is what happens when one is checked.

**§1.17 is the one to read if you are short of time.** It is §1.10's rule in a
fifth disguise, and it caught a design that was about to be written the obvious
way: the obvious encoding of "unknown" is NULL, and NULL turns the index that
removes an O(n) scan back into an O(n) scan.

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

**Consequence for the design.** Memory is controlled by bounding what is **in
flight per execution**, never by optimising the UDF. This is the reasoning
behind the memory-control section of `PLAN.md`.

**Read that with 1.21, which measured it and narrowed it.** "Rows in flight" is
the wrong unit: at a fixed 1.92 M rows, a `LIST`-based UDF needs 64 MB over one
group and **more than 2 GB over 4,000**. It is *groups* that cost, and a group
is `key x window` — so `max_rows_per_trigger` and window-range chunking each
bound part of the problem and neither bounds key cardinality at all.

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

from duckdb.func import PythonUDFType          # NOT duckdb.functional

con.create_function("fft_arrow", fft_arrow, [LIST_DOUBLE], LIST_DOUBLE,
                    type=PythonUDFType.ARROW)
```

**Correction, found when `duckstream/udf.py` first executed this.** The enum
lives in **`duckdb.func`**, not `duckdb.functional` — the latter does not exist
on 1.5.5. The timing above is sound; the import line beside it was written from
the docs and never run until phase 3. `null_handling` likewise takes
`FunctionNullHandling.SPECIAL`, not the string `'special'`.

Gotchas to design around: inputs may arrive as `ChunkedArray`, not `Array`; use
`FunctionNullHandling.SPECIAL` if the function must see or return NULLs; STRUCT returns
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

**Conclusion.** DuckDB will not span a transaction across attached databases —
**for data writes**, which is what was measured here and all that was measured.

**Re-measured during tier three, because the codebase had quietly widened it.**
`engine.py` extended this constraint to `CREATE TEMP VIEW` and stated that
binding a view inside the transaction would raise `TransactionContext Error`.
That sentence had never been executed. It is false: inside one DuckLake
transaction, a temp view, a temp table, and even a `DROP VIEW` all succeed
alongside inserts and deletes, in either order.

| Inside one DuckLake transaction | Result |
|---|---|
| temp view, then `INSERT` into the lake | OK |
| `INSERT` into the lake, then temp view | OK |
| temp view over `read_parquet`, then `INSERT ... SELECT` from it | OK |
| several temp views + `DELETE` + `INSERT` (a multi-chunk recompute) | OK |
| `DROP VIEW` inside the transaction | OK |
| `CREATE TEMP TABLE`, then `INSERT` into the lake | OK |

The distinction is *rows*, not *catalogs in the statement*: `temp` is not a
second write target in the sense this section measured.

**Consequence.** Tier three depends on the true version — a recompute cannot
bind its views before the transaction opens, because which files it reads is
decided by data read inside it. Binding the ordinary *batch* view early remains
right, but as a preference (keep the transaction short) rather than a
requirement. And this is the fourth time a claim in this project turned out to
be an argument wearing a measurement's clothes; §1.15's derived GB/day and
§1.2's never-run import line were the others.

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

### 1.13 Statistics pruning does not save the file *open*: ~0.1 ms per file

**Method.** One parquet file per hour, 2,000 rows each, footer statistics
intact. Recomputed a single one-hour window three ways at four corpus sizes,
five repetitions, median, `threads=2`: no filter at all, the whole file list
plus a `WHERE` on the window, and only the files that actually match.

| Files | all, no filter | all + `WHERE` | only matching |
|---|---|---|---|
| 24 | 6.6 ms | 4.0 ms | 1.7 ms |
| 168 | 34.0 ms | 19.8 ms | 1.7 ms |
| 720 | 138.5 ms | 77.8 ms | 1.6 ms |
| 2,160 | 410.9 ms | **216.7 ms** | **1.7 ms** |

**Conclusion.** The `WHERE` does prune — 411 ms drops to 217 ms, so data pages
really are skipped. What it cannot skip is **opening the file to read the footer
and decide**, and that costs a flat **~0.1 ms per file listed** whether the file
is read or not. Reading only the matching files is flat at 1.7 ms at every
corpus size; filtering the full list is linear in the *total*.

**Consequence for phase 3.** A `recompute_window` cannot simply hand DuckDB the
consumed-file list and a time predicate. At one file per trigger and a
one-minute schedule that list is 525,000 files a year, which is ~52 seconds per
recompute **on this box** — and a Raspberry Pi on USB SSD or SD multiplies that
constant rather than dividing it, because the cost is small random I/O rather
than CPU. duckstream therefore needs a file → time-range index.

**Design it as a hint, not as truth.** Over-selecting is harmless: extra files
are read and the answer is still right. Under-selecting is silently wrong, which
is the failure class this framework exists to refuse. So the index only ever
narrows, it never removes entries, and anything it cannot answer confidently
falls back to the whole list — which makes correctness independent of it and
leaves only cost depending on it. Building it is close to free: the engine
already scans each bound batch once for the watermark (§1.11), so grouping that
same pass by `read_parquet`'s `filename` pseudo-column yields per-file bounds
without extra I/O.

**Consequence beyond phase 3.** The same per-file constant applies to ordinary
planning, and the consumed-file map already grows without bound and is decoded
on every trigger. Bounding the *number of files* — compaction and retention,
phase 4 — is the lever that matters, and this measurement makes it more urgent
rather than less.

### 1.14 `sum_sq` is not a safe sufficient statistic — carry `mean` and `M2`

**Method.** Folded the same values batch by batch two ways and compared both
against DuckDB's own `var_samp` over the whole set: the textbook triple
`(n, sum, sum_sq)` with `var = (sum_sq - sum^2/n)/(n-1)`, and Chan's parallel
merge carrying `(n, mean, M2)`. Batches of 100.

| Data | DuckDB `var_samp` | naive `sum_sq` | Chan `mean`/`M2` |
|---|---|---|---|
| 300x1.0 then 100x5.0 | 3.00751879699248 | 3.00751879699248 | 3.00751879699248 |
| epoch-like, 1.7e9 + noise | 0.250093631269326 | **524.419** (2,096x wrong) | 0.250093633 |
| large mean, tiny variance (1e8 +/- 0.5) | 0.250062515628907 | **0.0** | 0.250062515628907 |

**Conclusion.** `sum_sq - sum^2/n` subtracts two nearly-equal large numbers, so
its error grows with the *mean* rather than with the variance. At Unix-timestamp
magnitudes it is wrong by three orders of magnitude; at 1e8 with a small spread
it returns exactly **0.0**. Chan's merge is equally associative — it folds
partial states in any order, which is the only property the tier needs — and is
correct to ~1e-9 relative on the same data.

**This overrides `PLAN.md`.** That document says "persist `sum`, `sum_sq`,
`count`; derive the result on read", and the intent is right while the triple is
not. Tier two stores `(n, mean, M2)` per statistic argument:

```
n    = n_a + n_b
mean = mean_a + (mean_b - mean_a) * n_b / n
M2   = M2_a + M2_b + (mean_b - mean_a)^2 * n_a * n_b / n
```

**Why this matters more here than in most codebases.** Section 4 records a
production mart in this repository that stored a standard deviation of **0.0**
where the truth was 1.7342, because a batch overwrote it instead of folding it.
The naive triple reproduces that exact symptom — 0.0 for a real variance — from
a completely different cause, and it would do so *inside the framework built to
prevent it*. A number that is merely wrong gets noticed; 0.0 looks like a
constant sensor.

**Confirmed end to end, in SQL, against DuckLake.** The table above is Python
arithmetic; the same states were then folded through the real ``MERGE`` batch by
batch and the derived columns diffed against DuckDB's own aggregate over every
row at once:

| Data | batches | `avg` | `stddev_samp` | worst rel. error |
|---|---|---|---|---|
| 300x1.0 then 100x5.0 | 4 | **2.0** | **1.73421993904824** | 1.5e-16 |
| large mean, tiny variance | 16 | exact | exact | **0.0** |
| one row per batch | 20 | exact | exact | 0.0 |
| epoch-like magnitudes | 16 | 1.4e-16 | 1.9e-09 | 3.7e-09 |

The first row is `CONTEXT.md` section 4's mart, computed correctly: **2.0 and
1.7342**, where the hand-written incremental merge held 3.0 and 0.0.

It also settles an assumption that would have been quietly fatal: **every
``UPDATE SET`` right-hand side reads the pre-update row**, so ``n``, ``mean`` and
``M2`` can be assigned in one statement even though ``mean`` needs the old ``n``
and ``M2`` needs the old ``mean``. Had DuckDB applied them in order, every
answer would have been subtly wrong.

**On that 3.7e-09.** It is not an error in the fold, and calling DuckDB's
single-pass result "the truth" overstates it — both are float64 approximations,
and they agree to about nine significant figures on a variance of 0.25 derived
from values of 1.7e9. Set against the naive form's factor of 2,096 on the same
data, the difference is not close. But it does mean a tier-two ground-truth diff
at those magnitudes cannot use the suite's usual ``rel_tol=1e-12``. The answer is
to keep the ordinary diff on ordinary values, where agreement is 1e-16, and give
numerical stability its own test with pathological data and a stated tolerance —
rather than loosening the tolerance everywhere and weakening every other check.

**Also measured, since it decides where the statistics live: a DuckLake view can
be time-travelled.** `CREATE VIEW` succeeds, and `view AT (VERSION => n)` returns
the state as of that snapshot, so deriving a mart from a private statistics
table would not break the snapshot-walk verification the conformance suite is
built on. Recorded because it is not obvious and would otherwise be re-derived;
v1 still stores statistics and result in one table (see `BUILD_GRAPH.md`).

### 1.15 The consumed-file map is rewritten every trigger: 45 MB at one year

**Method.** Built the file source's offset at four sizes and timed the three
things every trigger does with it -- decode it to plan, encode it to checkpoint,
and commit it as a new append-only row against a real DuckLake catalog. 20
repetitions, median, `threads=2`.

| Files consumed | Offset size | encode | decode | commit |
|---|---|---|---|---|
| 1,000 | 0.09 MB | 0.5 ms | 0.4 ms | 50.0 ms |
| 10,000 | 0.87 MB | 6.4 ms | 4.4 ms | 59.3 ms |
| 100,000 | 8.70 MB | 74.4 ms | 77.3 ms | 153.0 ms |
| **525,600** (one file/minute for a year) | **45.73 MB** | 438 ms | 545 ms | **651 ms** |

**Conclusion.** ``duckstream/offsets.py`` already names this as a known v1
limit, and the wording is exact: "the whole map is rewritten on every
checkpoint". The trigger cost is bad enough — 651 ms against the ~26 ms floor of
1.10 — but the number that matters on a Raspberry Pi is the one that does not
appear in the table: **45.7 MB written per trigger is ~65 GB per day at a
one-minute cadence.** That is not a latency problem, it is an SD card with a
finite write budget being spent on re-recording the same file names.

Even a week's worth (10,000 files) is ~1.25 GB a day of pure re-serialisation.

**Consequence.** This is the largest single obstacle to running duckstream
unattended on a Pi, larger than 1.13's per-file open cost and larger than
anything in 1.10–1.12. It is a phase-4 item and it is the *first* one.

**The fix is not the reserved high-water mark.** ``offsets.py`` reserves
``high_water_mtime_ns`` for collapsing old entries, and that would bound the
map — but it also changes the guarantee: a file arriving with an mtime older
than the mark would be skipped, silently, which is the failure class this
project refuses. The exact fix is to stop storing the set as one JSON value and
store it as **rows**: consuming a file becomes an insert of one row, and "has
this been consumed?" becomes an anti-join the database answers, rather than a
45 MB blob decoded into Python and re-encoded on every tick.

That is also 1.12's rule arriving in its most extreme form — *anything that
reads a state table and then loops over the result in Python is a defect waiting
for the table to grow* — and here the table is a single cell.

**Also tempting, also measured, also rejected: compressing the stored form.**
File paths repeat, so they compress well — zlib level 6 gives a flat **7.4x**
at every size, taking 45.7 MB to 6.2 MB. It is a two-function change, contained
entirely to ``encode_offset``/``decode_offset``. And it is still not worth doing:
it *adds* **185 ms per trigger** to compress, and 65 GB a day becomes 8.8 GB a
day, which is a card that dies in a year instead of two months. Paying CPU on
the trigger path to make a number that is three orders of magnitude too large
one order smaller is a mitigation dressed as a fix, and shipping it would make
the real fix easier to keep postponing.

**Tempting and wrong:** dropping entries for files that no longer exist. It
sounds exact, and it bounds the map by the retention window rather than by all
time. But the source learns which files exist by scanning the tree, so a network
mount that blinks returns an empty scan, every entry looks deleted, and the next
successful scan re-reads the entire landing directory. The failure is silent,
total, and arrives on the day the NAS reboots.

> **Two of the numbers above are wrong, and 1.16 has the measured ones.** The
> "~65 GB a day" was the encoded JSON size multiplied by the cadence, not bytes
> measured on the disk; the offset is a `VARCHAR` in a DuckLake table and reaches
> the disk as parquet. Measured: **7.97 MB per trigger, ~11.2 GB a day.** The
> zlib figure is off in the same direction. Everything else here stands, and the
> conclusion stands more firmly than when it was written — see 1.16.

### 1.16 The consumed-file set as rows: 1,665x fewer bytes, and 1.15 re-measured

**Method.** Built the file source's offset at 1,000 / 10,000 / 100,000 / 525,600
files — the last being one file a minute for a year, as in 1.15 — and ran 20
triggers of each shape against a real DuckLake catalog, `threads=2`, median.
Bytes were counted by measuring the catalog directory before and after, which is
a volume rather than a timing and so is immune to the drift 1.11 warns about.

| Files | shape | plan | commit | on disk per trigger |
|---|---|---|---|---|
| 525,600 | the map inside the offset (1.15's shape) | — | **1,078 ms** | **7.97 MB** |
| 525,600 | consumed files as rows | 10.7 ms | 14.0 ms | 4.9 KB |
| 525,600 | rows, probe narrowed to the scan's mtime span | **3.6 ms** | **13.8 ms** | **4.9 KB** |

Both row variants are **flat in the number of files consumed**: planning stays at
~3.4 ms and the commit at ~14 ms whether 1,000 files have been consumed or
525,600. That is 1.12's property arriving where 1.15 said it would.

**Two corrections to 1.15. Its conclusion was right and its arithmetic was not.**

*First, ~65 GB a day is ~11 GB a day.* 1.15 measured the encoded offset at
45.7 MB and multiplied by the cadence. But the offset is a `VARCHAR` in a
DuckLake table, so what reaches the disk is parquet, and parquet compresses it:

```
encoded offset JSON       :  45.11 MB
bytes on disk per trigger :   7.97 MB      (5.66x)
derived from the JSON     :  63.4 GB/day
measured on the disk      :  11.2 GB/day
```

The recorded figure was derived rather than measured, and it was 5.7x too large.
It did not change the decision — 11.2 GB a day spent re-recording file names it
already knows still kills an SD card, and it was still the largest single
obstacle to running unattended on a Pi — but a number this project quotes has to
be a number this project measured.

*Second, compression was rejected for the right reason and the wrong number.*
1.15 recorded zlib at 7.4x, taking "65 GB a day to 8.8 GB a day". Re-measured on
the same data: zlib level 6 gives **9.2x** on the JSON but only **2.38x** on the
disk — 7.97 MB to 3.36 MB — because parquet had already taken most of what there
was to take. The honest comparison is therefore 2.38x for **+185 ms a trigger**,
against the rows fix's 1,665x for **−1,064 ms**. The rejection stands and is now
much better supported than when it was written.

**The result.**

| | the map in the offset | rows |
|---|---|---|
| bytes written per trigger | 7.97 MB | **4.9 KB** (1,665x) |
| per day at one file a minute | 11.2 GB | **6.8 MB** |
| commit | 1,078 ms | **13.8 ms** (78x) |
| planning | 545 ms to decode | **3.6 ms**, flat |
| the whole year of file names | rewritten every trigger | **8.8 MB, written once** |

That last row is the change in one line: the entire consumed history, stored
once, is about the size of what the old shape wrote **every single trigger**.

**The probe window is a deduction, not a heuristic.** The anti-join matches
`mtime_ns` by equality, so no consumed row outside the scan's own mtime span can
match one, and restricting the probe to that span cannot change the answer. It
is worth 3x on planning and it lets DuckLake skip data files on their statistics.

It also carries a trap that the mutation audit found and review did not: for a
**single-file** scan, `BETWEEN t AND t` is exactly `= t`, so the window silently
stands in for the mtime equality and a one-file test does not test file identity
at all. Any test of identity must use a scan spanning more than one mtime.

**What this does not fix.** The number of *files* is still unbounded, and
`latest_offset()` still walks the whole landing tree every trigger, where 1.13's
~0.1 ms per file listed applies. Retention at the source is the lever and it is
still phase 4's business — but it is now a cost problem rather than a
write-endurance one.

### 1.17 A NULL "unknown" defeats file pruning: encode it as infinity

**Method.** The tier-three file → time-range index (1.13) has to answer *"which
consumed files can hold a row in `[lo, hi)`?"*, and its contract is that a file
it cannot place must be **selected**, never skipped. Two encodings of that same
rule were timed against a real DuckLake `consumed_files` table at three sizes,
five repetitions, median, `threads=2`, with the rows written **one insert per
trigger** — which is duckstream's actual layout, because inlining is off (1.7)
and every trigger writes its own small parquet file:

```
A   WHERE min_ts IS NULL OR max_ts IS NULL OR (max_ts >= lo AND min_ts < hi)
B   WHERE max_ts >= lo AND min_ts < hi                    -- skips unknowns: wrong
C   WHERE max_ts >= lo AND min_ts < hi, unknown stored as [-inf, +inf]
```

| Consumed files | A — disjunction | B — bare range | C — sentinel |
|---|---|---|---|
| 168 | 14.4 ms | 9.4 ms | 7.3 ms |
| 720 | 42.9 ms | 7.9 ms | 10.4 ms |
| 2,160 | **117.5 ms** | **8.9 ms** | **12.1 ms** |

**Conclusion.** The disjunction is **O(files ever consumed)** and the plain
conjunctive range is flat. `IS NULL OR ...` is not a predicate DuckLake can
prune data files on, so every one of the table's per-trigger parquet files is
opened — which is 1.13's ~0.1 ms per file arriving a second time, in the very
index built to remove it. Written as one bulk insert instead, A measures 4.2 ms
flat, which confirms the cause is the disjunction meeting the many-small-files
layout rather than the row count.

C's mild growth is the design working: the unknown rows planted in it grow
linearly (4 → 44) and are genuinely selected every time.

**Consequence.** An unmeasured file is stored at `[-infinity, +infinity]` — in
Python, `datetime.min` and `datetime.max`. This is not a trick standing in for
NULL. *"This file may contain a row at any time"* is a true statement about a
file nobody has measured, and stating it that way makes the widest answer fall
out of the ordinary comparison instead of needing a special case that a reader
of the SQL can forget to write. Same reasoning as 1.16's probe window: a
deduction, not a heuristic.

**Also verified**, because the sentinel has to survive three different writers
and a round trip: a SQL `TIMESTAMP '-infinity'` literal, a Python
`datetime.min` bound parameter and a `pyarrow` `timestamp('us')` column all
store and read back as exactly `datetime.min` / `datetime.max`, unchanged after
a `CHECKPOINT`. And `ALTER TABLE ... ADD COLUMN ... DEFAULT TIMESTAMP
'-infinity'` is **refused** by DuckLake — *"we cannot add a column with a
non-literal default value"* — so an upgraded catalog is backfilled with an
explicit `UPDATE`, measured at 44 ms over 1,000 rows and 109 ms over 100,000,
paid once per catalog. Leaving those rows NULL would silently narrow every
recompute after an upgrade, which is section 4's bug class arriving as an
upgrade note.

### 1.18 Per-file bounds cost 1.4–6.7 ms a batch, and `filename` is free

**Method.** The index needs `(min, max, rows)` per consumed file. Two ways to
get them were timed over one parquet corpus, best of nine, median, `threads=2`:
a grouped scan using `read_parquet`'s `filename` pseudo-column, and the footer
statistics from `parquet_metadata`.

| Batch | `observe()` scan (1.11) | bounds scan | plain scan, `filename=false` | …`filename=true` |
|---|---|---|---|---|
| 1 file, 2 k rows | 0.61 ms | **1.41 ms** | 0.48 ms | **0.51 ms** |
| 10 files, 20 k rows | 1.84 ms | **3.14 ms** | 1.49 ms | **1.50 ms** |
| 40 files, 80 k rows | 5.64 ms | **6.69 ms** | 4.36 ms | **4.42 ms** |

At a 240-file corpus the footer route is 21.6 ms against the grouped scan's
34.6 ms — 1.6x cheaper.

**Conclusion, and the footer route is rejected despite winning.** It returns
`stats_min_value` as **VARCHAR**, so every bound would be re-parsed from
DuckDB's rendering of the column's logical type. A timestamp reconstructed from
a string is precisely the "plausible wrong number" this framework exists to
remove, and it would be wrong in the direction that *narrows* — a file excluded
from a recompute that should have been read. It is also parquet-only, while the
file source reads CSV and JSON too. 1.6x is not worth buying that with.

`filename=true` is **free** (0.51 ms against 0.48 ms, inside the noise) and
verified working on `read_parquet`, `read_csv` and `read_json`, echoing back the
exact path string it was handed — which is what the mapping to relative paths
depends on.

**Consequence.** One grouped scan per batch, `SELECT filename, min(ts), max(ts),
count(ts) ... GROUP BY filename`, and the row count rides along for nothing.
Charged **only to models that will be recomputed**: 1.4 ms on a one-file batch
is not a rounding error against 1.8's ~15 ms committing trigger, and a tier-one
model would be paying it to build an index nothing reads. A model that becomes
tier three later therefore has unmeasured rows behind it, and 1.17's sentinel is
what makes that safe rather than merely tolerable — those files are read by
every recompute instead of by none.

### 1.19 A recompute costs ~17 ms plus ~0.14 ms per file in the window

**Method.** The recompute *step* in isolation — select the window's files, read
them, aggregate, clear the range, insert — inside one transaction against a real
DuckLake catalog, at six window sizes, median of nine, `threads=2`. Isolated
rather than measured through a whole trigger on purpose: a first attempt timed
`engine.run()` end to end and every variant landed between 130 and 165 ms with
tier three sometimes *below* tier one, because the trigger's own fixed costs
swamped the difference. That number is not reported, because it does not measure
what it appears to.

| Files in the window | Rows | Clear + re-insert |
|---|---|---|
| 1 | 200 | 17.5 ms |
| 10 | 2,000 | 19.2 ms |
| 50 | 10,000 | 24.3 ms |
| 100 | 20,000 | **31.3 ms** |

**Conclusion.** An intercept of ~17.5 ms and a slope of **~0.14 ms per file**.
The intercept is 1.8's commit floor, which a recompute pays like any other
write. The slope is 1.13's ~0.1 ms per file open plus the cost of actually
reading 200 rows out of each — so the two measurements agree, taken three phases
apart by different methods.

**Consequence, and it is the sentence to quote when somebody asks why their
recompute got slower.** A tier-three trigger's cost is a function of the
**window** it recomputes, not of the batch that touched it. A one-row batch
landing in an hour fed by a hundred files pays for a hundred files. The lever is
a finer `grain`, or fewer files per window — which is retention and compaction,
phase 4 — and emphatically *not* a smaller `max_files_per_trigger`, which makes
it worse by recomputing the same window more often.

### 1.20 The landing-tree scan was 81% Python, not filesystem — now 3–9x faster

**Method.** `FileSource.latest_offset()` against real landing trees, profiled
with `cProfile`, then old and new implementations timed **interleaved** — one of
each in turn — and asserted to return byte-identical results before either was
timed.

**The first attempt at this section reported per-file constants across tree
sizes and concluded "minutes per trigger". Those numbers are withdrawn.** They
were taken one configuration after another rather than interleaved, and a later
run disagreed with them by **7x on the same tree and the same shape**. §1.11
already says to interleave anything on this box that takes minutes; the first
pass did not, and the conclusion it reached was about this machine's mood.
What follows is what survives that correction.

**What the profile says, and a profile is a ratio within one run, so drift
cannot flatter it.** At 2,000 files:

```
bare os.stat() loop        4.7 us per file
latest_offset()           24.3 us per file
                          ---------------
overhead above the syscall  81% of total
```

The top entries were `ntpath.normcase` — **160,000 calls for 2,000 files** — and
`ntpath.relpath`, reached from `FileOffset.relative_path` once per file. At one
file per directory, `nt.scandir` was called **twice per directory**: once inside
`os.walk` and once again to list the files it had just read.

So the scan was not I/O-bound at all. It was rebuilding path strings.

**The fix, and it is a pure optimisation.** One `scandir` per directory; the
relative prefix carried down the walk instead of recomputed per file; size and
mtime taken from the `DirEntry` the walk already fetched; the completion
marker's stat taken from the same listing. Interleaved, nine repetitions,
median, with the two implementations asserted equal first:

| Files | Per directory | Before | After | |
|---|---|---|---|---|
| 2,000 | 100 | 59.5 ms | 7.0 ms | **8.5x** |
| 2,000 | 1 | 220.2 ms | 72.1 ms | **3.0x** |
| 5,000 | 50 | 147.7 ms | 16.3 ms | **9.0x** |

**Tree shape matters, which the withdrawn version denied.** At one file per
directory — duckstream's real shape, one drop per trigger — the per-*directory*
work dominates and the win is 3x; where a directory holds several files it is
8–9x. Both are worth having and neither is the whole answer.

**Consequence.** This is CPU, not I/O, so it transfers to a Pi *better* than a
DuckDB measurement would: a Pi's cores are slower than this box's, so Python
path manipulation costs it more, while `stat` on local storage is comparable.
That is the opposite of the usual caveat and worth stating plainly.

**What is still not measured.** The absolute cost at a year of files (525,600),
on either machine. The scan remains **linear in ready files** and is still paid
on every trigger including idle ones, so bounding the number of files — the
retention half of phase 4 — is still the structural fix and this is a constant
factor in front of it. Quoting a projected per-trigger figure for a year is
exactly what the withdrawn version got wrong; measure it on the target, in
phase 6's soak, or not at all.

---|---|---|---|
| 100 | 100 | 15.4 ms | 154 µs |
| 1,000 | 1,000 | 139.6 ms | 140 µs |
| 5,000 | 5,000 | 2,455.9 ms | 491 µs |
| 20,000 | 20,000 | 8,581.0 ms | 429 µs |
| 50,000 | 50,000 | **23,460 ms** | 469 µs |

Same 2,000 files, three shapes: **322 ms** at one file per directory, **251 ms**
at ten, **353 ms** at a hundred. And doubling the directories doubles the time
(1.96x, 2.02x, 2.52x).

**Conclusion.** The walk is **linear in files and near-indifferent to tree
shape** — it is the per-file `stat` that costs, not the directory traversal. The
constant is **~0.15–0.5 ms per ready file**, which is **1.4x to 5x** what
`PLAN.md` assumed when it carried 1.13's ~0.1 ms across to this walk. 1.13
measured *DuckDB opening a parquet footer*; this is `os.walk` plus a `stat` plus
a glob match, and the two constants are not the same number. Carrying one across
to the other is the derived-figure mistake 1.15 made, in a new place.

**Consequence, and it is worse than phase 4 was scoped for.** At one file a
minute for a year — 525,600 ready files — this is **minutes per trigger**, and
it is paid on *every* trigger including idle ones, because the scan is how the
source learns there is nothing to do. It dwarfs every other number in this
document: 1.16 took the commit from 1,078 ms to 13.8 ms, and this would sit at
four minutes in front of it.

**It is also not fixed by retention alone.** The cost is per *ready* file, and
every already-consumed file is re-`stat`ed on every trigger purely so the
anti-join can discard it. Retention bounds the tree; making the scan skip what
it has already consumed would bound the *work*, and the two are different
levers. Note that 1.15's forbidden shortcut is specifically a high-water **mtime
mark**, because a file may arrive with an older mtime — a directory-level rule
gated on the completion marker is a different proposition and has not been
measured or ruled out.

### 1.21 Memory follows **groups**, not rows — and no knob bounds groups

**Method.** `PLAN.md`'s outstanding performance item: *"bisect `memory_limit`
against `max_rows_per_trigger` per tier, and publish the ratio so users can size
the knob."* Measured through the **engine**, not a bare query, because what a
user sets is a model's limit and what they have is a memory budget. Binary
search over `16…2048 MB` for the smallest limit at which one trigger completes.

First, per tier, with every row in one window — one group:

| Tier | 200,000 rows | 800,000 | 3,200,000 |
|---|---|---|---|
| `additive` | 32 MB | 32 MB | 32 MB |
| `sufficient_statistics` | 32 MB | 32 MB | 32 MB |
| `non_foldable` (median) | 32 MB | 32 MB | 32 MB |
| `non_foldable` (UDF over `LIST`) | 32 MB | 32 MB | 64 MB |

Which reads as "the tier barely matters and rows barely matter" — and would have
been published as exactly that, except it contradicts 1.1, which measured 256 MB
for `LIST` at 2.4 M rows. The difference is that 1.1 had **400 groups** and the
table above has **one**. So the group count was varied at a *fixed* 1,920,000
rows:

| Groups | `additive` | `non_foldable` (UDF over `LIST`) |
|---|---|---|
| 1 | 32 MB | 64 MB |
| 40 | 48 MB | 256 MB |
| 400 | 48 MB | 384 MB |
| 4,000 | 96 MB | **>2048 MB** |

**Conclusion.** Rows are close to free; **groups are what cost**. Same row count,
64 MB to over 2 GB, from key cardinality alone. It reconciles with 1.1 — 400
groups needing 256 MB there and 384 MB here is the same measurement twice — and
it means the obvious reading of 1.1 was incomplete.

**Consequence, and this is a gap rather than a tuning note.** 1.1 says memory is
controlled by "bounding rows in flight per execution (`max_rows_per_trigger`,
window-range chunking)". Neither of those bounds **groups**. A group is
`key x window`: chunking bounds the *window* half, and nothing bounds the *key*
half at all. A model with 4,000 sensors materialises 4,000 lists in every chunk
however small the chunk is, and `max_rows_per_trigger` cannot help — a batch of
few rows spread thinly across many keys is strictly worse than a batch of many
rows in one.

So the honest sizing rule for tier three is **per group, not per row**: budget
roughly 0.1 MB per concurrently materialised group for a `LIST`-based UDF on
this box, and treat key cardinality as the number to watch. Bounding it would
need a knob duckstream does not have — chunking by key range as well as by
window range — and that is a design question, not a setting.

### 1.22 A Python UDF costs ~3x the available parallelism, and 4.2x native

**Method.** `PLAN.md`'s other outstanding item: *"quantify the single-thread
penalty (2.1) so the docs can state when to split across processes."* 2.1 is
*researched* (duckdb#14817), not measured. 1,920,000 rows in 480 groups, the
same query shape run at 1, 2 and 4 threads, interleaved, median of five.

| Threads | Native SQL | Speedup | Arrow UDF | Speedup |
|---|---|---|---|---|
| 1 | 30 ms | 1.00x | 52 ms | 1.00x |
| 2 | 16 ms | 1.90x | 45 ms | 1.15x |
| 4 | 10 ms | **3.18x** | 40 ms | **1.31x** |

**Conclusion.** 2.1 is confirmed and now has a number. Native SQL scales
essentially linearly — 3.18x on four threads. The same query with a Python UDF
in it reaches **1.31x**, which by Amdahl puts roughly **two thirds of it on one
thread**. It is not *fully* serial: the scan and the `GROUP BY` still
parallelise, and that residual is the whole 1.31x. At four threads the UDF query
costs **4.2x** the native one.

**Consequence.** On a four-core Pi, one pipeline containing a UDF leaves about
three quarters of the machine idle, and adding cores buys almost nothing. The
lever is **processes, not threads**: run independent models in separate
processes, each with its own catalog connection. And it is a direct argument for
keeping tier three off the hot path wherever a foldable tier will do — the
4.2x is on top of the recompute's own re-reading of the window (1.19).

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
| MQTT modelling | **Landing writer, not a source.** Built in phase 5; `type: mqtt` on a model is still refused, permanently. | MQTT has no replayable offset; once a message is acked it is gone. Exactly-once is impossible directly, so land durably first and treat that as the source. **The acknowledgement discipline is the guarantee and it inverts the client's default**: `paho` acks a QoS-1 message on arrival, which loses the buffer on a crash while the broker believes it was delivered. `LandingWriter` releases a token per record only after the completion marker is on disk. The price is duplicates — exactly-once is over *files*, not over readings — and a model that cares needs a merge key. `paho-mqtt` is an optional extra so a deployment with no MQTT carries none. |
| Entry points | **Both**: a Python API and a config-driven CLI, built together in phase 1 over one canonical `Model`. | A cron-driven framework wants the CLI; library users want the API. Retrofitting config onto a Python-shaped API costs far more than building both at once. The loader is a deserialiser only — no parallel validation, no parallel execution path, no override or precedence semantics, which is where config layers usually rot. Drift is prevented mechanically by a config round-trip test and by running every conformance scenario through both doors. |
| Config format | **YAML** via `pyyaml`, parsing isolated in `config.py`. | Nested model declarations read far better than the alternatives, and YAML is the norm for data tooling. Isolating the parser keeps stdlib `tomllib` a cheap swap if the dependency becomes unwelcome on a constrained device. |
| Callables in config | **Registry with dotted-path resolution.** | Config expresses declarative structure but cannot express functions. Built-in names (`file`, `mqtt`, `table`) plus `my_pkg.mod:obj` for user sources, sinks and UDFs keeps config fully capable without turning it into a programming language. |
| Differentiator | **Foldability classification** with load-time rejection. | See below. |
| Unprocessable data | **Quarantine by default** (`on_failure`), after a bounded retry budget: skip the batch, record the loss permanently in `duckstream.quarantine`, exit non-zero. `halt` never advances past it. | Halting does not preserve the unprocessable data — a stream blocked on one bad file stops collecting everything behind it too — so continuing loses strictly less. It is a *policy* rather than a defect only because it is never silent: skip and record are one transaction, the table is never pruned, and `status` keeps reporting it after the log has rotated. |
| Retry state | **In the `offsets` row**, not a second table. | §1.11: a scalar read of a DuckLake state table costs ~10 ms and the engine already pays one per trigger. A second table would double that on every trigger to carry information that only matters when something is broken. |
| Attempt accounting | **Only failures that fail cleanly spend an attempt.** A hard kill records nothing. | A crash-looping deployment must not be able to quarantine its own data; infrastructure trouble is not bad data. |
| Lateness horizon | **Opt-in per model** (`lateness`), and it is what turns a model on to event time. Without it there is no watermark, every window stays open forever, and nothing is ever dropped — which is exactly phase-1 behaviour. | A horizon is a claim about the data, not a default anyone can pick correctly on a user's behalf. Making it opt-in also means the phase-1 path keeps costing what it cost (§1.11), so event time is paid for only by models that asked for it. |
| Windowed `append` | **Requires a horizon**, refused at load without one. Each window is folded in an open-window accumulator beside the target and written to the target **once**, when the watermark passes its end. | Phase 1 accepted `append` with a `grain` and wrote one *partial* row per window per batch. That equals the truth only when no two batches ever touch the same window — a condition the user cannot enforce and the engine never checked. It is the §4 bug class in the one place the framework had left it, so phase 2 refuses it and offers the correct mechanism instead. |
| Rows outside the horizon | **Dropped and counted**, durably, in `duckstream.batches`. Late rows (`rows_late`) and rows with no event time (`rows_undated`) are counted apart. A **tier-three** model counts `rows_undated` even with no horizon: a recompute is scoped by a window range and no `[lo, hi)` contains NULL, so a row belonging to no window is dropped where a tier-one model would fold it into a NULL window — a real divergence from a full recompute, and therefore one that must be reported rather than absorbed. | `PLAN.md` requires "dropped **and counted**, never silently absorbed", and a count that lives only in a return value or a rotated log has not been counted. Late and undated are separated because "arriving later than declared" and "carrying no timestamp" are different operational problems with different fixes. |

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
