# duckstream

A micro-batch streaming framework for **DuckDB** and **DuckLake**, in one Python
process.

You declare a model — a source, an aggregation, a sink — and duckstream supplies
the parts a streaming engine has to provide: durable source offsets, a bounded
micro-batch, correct incremental aggregation, and one atomic commit per trigger.
The output is an ordinary lakehouse: parquet data files, a SQL catalog,
snapshots and time travel, readable from any DuckDB client with plain SQL.

Each run opens the catalog, drains what has arrived, and exits. **Cron or a
supervisor owns the cadence**, not a daemon inside the database.

## What it is not

- **Not a DuckDB extension.** `INSTALL duckstream` is not a thing. duckstream is
  a Python library that drives an embedded DuckDB, so "running it" means running
  Python on a schedule.
- **Not distributed.** Single process, vertical scale, single writer.
- **Not a broker.** It reads from files (and later brokers); it does not replace
  one.
- **Not multi-tenant.** A config document resolves dotted paths to your own
  code, by design — that is how sources, sinks and UDFs stay expressible without
  the config becoming a programming language. It means a `models.yaml` is
  trusted input, exactly like a Python file: do not load one you would not run.
- **Not incremental view maintenance for arbitrary SQL.** That is a
  research-grade problem. duckstream maintains *declared aggregate models*, and
  it is deliberate about which ones it can maintain correctly — see below.

## Why it exists: foldability classification

Every streaming engine makes you hand-roll incremental aggregation, and every
one of them will happily let you get it wrong. That is the gap duckstream exists
to close.

An incremental merge is only correct when the aggregate forms a **monoid over
batches**: a partial result combined with a partial result gives the true
combined result. Many aggregates do not. duckstream classifies every aggregate
expression in a model, derives the tier, and picks the strategy from it:

| Tier | Aggregates | Strategy | What it does |
|---|---|---|---|
| `additive` | `count`, `sum`, `min`, `max` | `delta_merge` | Fold the batch's value into the stored value. No rescan. The only tier for which a naive delta merge is safe. |
| `sufficient_statistics` | a bare `avg`, `stddev*`, `var*`, `variance`, `corr`, `covar*` | `sufficient_statistics` | Persist `sum`, `sum_sq`, `count`; derive the result on read. Exact, still no rescan. |
| `non_foldable` | median and exact quantiles, exact `count(distinct …)`, order-dependent aggregates, any UDF over a whole window (FFT, DTW, entropy) — and anything wrapped in a scalar expression, such as `sum(a)/count(*)` | `recompute_window` | Recompute the affected windows from source. No shortcut exists. |

### This is not theoretical

Two production marts were computing wrong numbers for exactly this reason, and
both looked healthy until they were diffed against a full recompute:

- An hourly mart folded averages as `(target.avg + source.avg) / 2` — an
  unweighted average of averages. With 300 samples of 1.0 followed by 100
  samples of 5.0, the correct average is **2.0**; the mart held **3.0**.
  Standard deviation was simply overwritten by the last batch: correct
  **1.7342**, stored **0.0**.
- An FFT mart transformed only the current batch's rows, so a one-minute window
  fed by 30-second batches held a spectrum over half a window: **100 samples**
  instead of 400, and **51 spectrum bins** instead of 201.

Neither produced an error. Both produced plausible numbers. That is the failure
mode, and it is why the classification is a load-time gate rather than a
paragraph of documentation.

### The refusal

duckstream **refuses an additive strategy over a non-foldable aggregate at load
time** — when the model is declared, not at 03:00 in a cron log, and not after
it has stored a wrong number:

```python
Model(
    name="minute_spectrum",
    source=FileSource("landing/", marker="_READY"),
    sink=TableSink("marts.minute_spectrum", mode="update"),
    time_column="event_ts",
    grain="minute",
    key=["window_ts", "sensor_id"],
    aggregates={"p50": "median(value)"},
    strategy="delta_merge",              # a lie about median
).validate()
```

```
duckstream.errors.ModelValidationError: model 'minute_spectrum', field 'strategy':
strategy 'delta_merge' was declared, but column 'p50' computes 'median(value)',
which classifies as tier 'non_foldable' because of median. Folding a
'non_foldable' aggregate as if it were additive produces silently wrong numbers,
not an error at runtime. Declare strategy='recompute_window', or omit strategy
and let duckstream infer it from the tier.
```

The config door refuses it identically, adds the line number of the offending
declaration, and exits non-zero:

```
$ duckstream validate --config models.yaml
duckstream: model 'minute_spectrum', field 'strategy': strategy 'delta_merge' was
declared, but column 'p50' computes 'median(value)', which classifies as tier
'non_foldable' because of median. Folding a 'non_foldable' aggregate as if it were
additive produces silently wrong numbers, not an error at runtime. (declared at
models.yaml:9). Declare strategy='recompute_window', or omit strategy and let
duckstream infer it from the tier.

exit status 1
```

Run `validate` at deploy time. That is the whole point of it.

Two further invariants are enforced the same way: the sink merge key must
contain the window column, or a re-run silently overwrites a different window's
row; and a `non_foldable` model must declare its source time column and a memory
profile, because recomputing a window needs to know which windows a batch
touched and how much memory that is allowed to cost.

## Quickstart: the Python door

```python
# pipeline.py
import duckdb
from duckstream import Engine, Model, FileSource, TableSink, AvailableNow

con = duckdb.connect()                    # in-memory session; DuckLake holds the data
engine = Engine(con, catalog="ducklake:catalog.ducklake", data_path="lake_data")

engine.add(Model(
    name="hourly_counts",
    source=FileSource("landing/", marker="_READY", max_files_per_trigger=10),
    time_column="event_ts",
    grain="hour",
    key=["window_ts", "sensor_id"],
    aggregates={"n": "count(*)", "total": "sum(value)"},
    sink=TableSink("marts.hourly_counts", mode="update"),
))

report = engine.run(trigger=AvailableNow())     # drain what is available, then exit
print(report.rows_in, "rows in", len(report), "batches")
```

```
100 rows in 1 batches
```

`FileSource` tails a directory tree and reads only directories carrying the
completion marker, so a half-written drop is never picked up. `window_ts` is
derived by duckstream from `time_column` and `grain` — you never write it into
the data, and it is called `window_ts` at every grain.

Cron drives it:

```cron
* * * * * cd /opt/pipeline && ./venv/bin/python pipeline.py >> logs/duckstream.log 2>&1
```

### Reading the output

The result is an ordinary DuckLake table. duckstream is not in the read path:

```python
con = duckdb.connect()
con.execute("ATTACH 'ducklake:catalog.ducklake' AS lake")
con.execute("SELECT * FROM lake.marts.hourly_counts ORDER BY window_ts").fetchall()
# [(datetime.datetime(2026, 8, 23, 9, 0), 's1', 100, 200.0)]
```

## Quickstart: the config door

The same model as a document. Both front doors build the same canonical `Model`
object and run the same validation; the loader is a deserialiser and nothing
more — no parallel execution path, no override or precedence semantics.

```yaml
# models.yaml
catalog: "ducklake:catalog.ducklake"
data_path: "lake_data"

settings:                                  # applied to the connection
  memory_limit: "2GB"
  threads: 2

models:
  - name: hourly_counts
    source:
      type: file                           # registry name
      path: landing/
      marker: _READY
      max_files_per_trigger: 10
    time_column: event_ts
    grain: hour
    lateness: 10 minutes                   # optional; see "Event time"
    key: [window_ts, sensor_id]
    aggregates:
      n: "count(*)"
      total: "sum(value)"
    sink:
      type: table
      table: marts.hourly_counts
      mode: update
```

Relative paths resolve against the document's own directory, so it does not
matter which directory cron starts the process in.

```
$ duckstream validate --config models.yaml
models.yaml: ok, 1 model (hourly_counts)

$ duckstream models --config models.yaml
MODEL          TIER      STRATEGY     WINDOW            SOURCE          SINK
hourly_counts  additive  delta_merge  hour +10 minutes  file(landing/)  table(marts.hourly_counts, update)

$ duckstream run --config models.yaml
hourly_counts: 1 batch, 100 source rows, through batch 1

$ duckstream run --config models.yaml
hourly_counts: 1 batch, 7 source rows, through batch 3 -- dropped 6 late, watermark 2026-08-23 12:50:00

$ duckstream run --config models.yaml
hourly_counts: nothing to do
```

The CLI is exposed both as a `duckstream` console script and as
`python -m duckstream`, because cron in a venv usually calls the interpreter
directly rather than relying on `PATH`:

```cron
* * * * * cd /opt/pipeline && ./venv/bin/python -m duckstream run --config models.yaml >> logs/duckstream.log 2>&1
```

`Engine.from_config(path)` returns an ordinary `Engine` that Python can keep
modifying, so the two doors are one API rather than two.

Config cannot express a function, so sources, sinks and UDFs resolve through a
registry: built-in names (`file`, `table`) plus dotted paths for your own code
(`type: my_pkg.sources:MySource`). Nothing on a dotted path is imported at load
time, which is what lets `duckstream validate` check a document on a deploy box
where your UDF package is not installed.

## Event time: watermarks, lateness and sealing

Everything above works on the timestamps in your data with no notion of *now*.
Declare a `lateness` horizon and the model gains one:

```python
engine.add(Model(
    name="hourly_counts",
    source=FileSource("landing/", marker="_READY"),
    time_column="event_ts",
    grain="hour",
    lateness="10 minutes",              # or datetime.timedelta(minutes=10)
    key=["window_ts", "sensor_id"],
    aggregates={"n": "count(*)", "total": "sum(value)"},
    sink=TableSink("marts.hourly_counts", mode="update"),
))
```

The **watermark** is `max(event time seen so far) - lateness`, it never goes
backwards, and it is committed in the same transaction as the source offset, so
a restart resumes reading and judging lateness from the same point.

A window `[start, start + grain)` is **sealed** once the watermark reaches its
end. Sealing is what the horizon buys, and it means two different things
depending on the output mode.

### What counts as late

**A row is late when its *window* has already sealed — not when its timestamp is
older than the watermark.** The distinction is the whole point of a horizon, and
it is easy to get backwards:

> `grain="hour"`, `lateness="10 minutes"`. A row at 10:30 puts the watermark at
> 10:20. A row for 10:05 now arrives — *older than the watermark*. Its window
> `[10:00, 11:00)` has not ended, so it is folded normally. Only once some row
> at 11:10 or later pushes the watermark past 11:00 does that window seal, and
> only then is a 10:05 row refused.

Rows that are refused are **dropped and counted**, never silently absorbed:

| Counter | Meaning |
|---|---|
| `rows_late` | the row's window had already sealed |
| `rows_undated` | the row's `time_column` was NULL, so it belongs to no window |

Each is on the per-batch `BatchResult`, stored durably in `duckstream.batches`,
and totalled on the `RunReport`; `duckstream run` prints them when they are
non-zero. On a `BatchResult` and in the catalog they are `NULL` — not `0` — for
a model with no horizon, because that model drops nothing for want of a horizon
rather than for want of late data. The `RunReport` totals are plain sums and so
are always integers.

```python
report = engine.run()
report.rows_late, report.rows_undated, report.rows_dropped
```

**Batch boundaries matter, and that is inherent.** The watermark is a function
of what has been *observed*, so whether a given row is late depends on which
trigger carried it: two rows read in one batch are both folded, while the same
two split across triggers may see the second arrive after the first has already
sealed its window. Every micro-batch engine works this way. The practical
consequence is that `max_files_per_trigger` and `max_rows_per_trigger` are not
purely a memory knob once a horizon exists — chunking more finely lets the
watermark advance more often, and drops more.

### Output modes

| | `mode="update"` | `mode="append"` |
|---|---|---|
| when a row is written | every batch that touches the window | once, when the window seals |
| what a reader sees | a row that keeps changing until its window seals | a row that is final the moment it appears |
| where an open window lives | the target itself | `<target>__open_windows`, beside the target |
| needs a horizon? | no | **yes, with a `grain`** |

`update` merges each batch into the target on the model key, exactly as it did
without a horizon. Adding a horizon changes one thing: once a window seals, no
later batch can modify it.

`append` folds each window in an open-window accumulator next to the target and
moves it into the target **once**, complete, when the watermark passes its end.
The accumulator is an ordinary table you can query — it is the answer to "why is
this hour missing from my mart":

```sql
SELECT * FROM lake.marts.hourly_counts__open_windows ORDER BY window_ts;
```

The emit and the evict happen in the same transaction as the offset, so a window
is never both emitted and still open, and never evicted without being emitted.

**`append` with a `grain` requires a `lateness`, and is refused at load without
one.** This is a deliberate change from the first release, which accepted the
combination and wrote one *partial* row per window per batch — correct only if
no two batches ever touched the same window, which nothing enforced or checked.
The error names all three ways forward, because they mean different things:

```
sink mode 'append' is declared together with grain 'hour', but no lateness
horizon is. [...] Either declare how late data may be, e.g.
lateness='10 minutes', and each window is written once when it seals; or drop
`grain` if per-batch rows are genuinely what you want; or use mode='update',
which folds each batch into the stored row and needs no horizon.
```

`append` *without* a grain is unchanged: one row per key per batch, no fold, any
tier. Windowed `append` does fold across batches, so in this release it needs
the additive tier, like `update`.

### Choosing a horizon

It is a claim about your data, so duckstream has no default. Make it as large as
the worst arrival delay you are willing to absorb: too small and real data is
dropped as late; too large and windows stay open longer, so `append` output
lags and the accumulator holds more. Watch `rows_late` and widen it if it is
not zero. `lateness="0 seconds"` is legal and means a window seals the instant
the watermark reaches its end.

Units are `second`, `minute`, `hour` and `day`, singular or plural
(`"90 minutes"`, `"1 hour"`). A `datetime.timedelta` works through the Python
door and is stored in the canonical string form, so the same model built either
way — or loaded from YAML — compares equal.

## When the data will not process

Exactly-once says what happens when the *process* dies. It says nothing about
what happens when the *data* is unprocessable — a truncated upload, a file that
is not parquet, a UDF that raises on one row — and the answer has to be
something, because the naive one is that the offset stops advancing and every
trigger from then on retries the same file. That is not a crash. Nothing raises
an alarm. The pipeline just stops, quietly, until somebody notices.

So a batch gets a bounded number of attempts, spaced by a capped exponential
backoff, and then the model's declared policy applies:

```python
Model(
    ...,
    on_failure="quarantine",   # the default
    max_attempts=5,
)
```

| | `quarantine` (default) | `halt` |
|---|---|---|
| when the attempts run out | skip the batch, record the loss | never advance past it |
| the stream afterwards | live, processing new data | stopped until a human intervenes |
| what you lose | that batch | that batch **and everything after it** |
| `duckstream run` exit code | non-zero, on the run that skipped | non-zero, every run |

**Quarantine is the default, and the reason is that halting does not actually
preserve anything.** A stream blocked on one bad file stops collecting
everything that arrives behind it too, so continuing loses strictly less. Choose
`halt` when a *gap* is worse than a *stall* — billing, or anything reconciled
downstream.

What makes quarantine a policy rather than a bug is that it is never silent. The
skip and the record of the skip are one transaction, so the offset cannot move
past data without the row explaining why:

```sql
SELECT batch_id, skipped_from, skipped_to, rows_in, attempts, error, quarantined_at
FROM lake.duckstream.quarantine WHERE model_name = 'hourly_counts';
```

That table is never pruned, and `status` keeps reporting it long after the log
line has rotated away.

Two consequences worth knowing:

- **Only clean failures spend an attempt.** A process that dies hard — SIGKILL,
  the OOM killer, a power cut — records nothing, so a crash-looping deployment
  can never quarantine its own data. That is deliberate: infrastructure trouble
  should not be mistaken for bad data.
- **One quarantine per model per run.** A source where *every* batch is
  unprocessable would otherwise burn through the whole backlog in a single run,
  skipping it batch by batch before anyone saw the first record.

## Seeing what it is doing

```
$ duckstream status --config models.yaml
MODEL          STATE   EVENT LAG  SINCE RUN  BACKLOG  BATCHES  ROWS IN  ROWS OUT  LATE  QUARANTINED
hourly_counts  ok      3m12s      41s        0        184      412000   2208      6     0
```

`status` reads the catalog and nothing else, so you can point it at a live
deployment from another process — that is what a DuckLake catalog buys you, and
it is why a long-running engine on a plain DuckDB file was never an option. It
exits non-zero when any model is unhealthy, so it works as a health probe
without its output being parsed.

**Lag is three numbers because they fail independently**, and any one of them
alone is reassuring at the wrong moment:

| | what it means | what it catches |
|---|---|---|
| `EVENT LAG` | `now - watermark` | data arriving late, or a backfill |
| `SINCE RUN` | `now - last commit` | a deleted cron entry — event lag looks perfect |
| `BACKLOG` | what the source still holds | a source nobody is writing to any more |

A model whose event-time lag exceeds its own lateness horizon is called out
separately, because that is the point at which windows start sealing before
their late data arrives and `LATE` begins to climb.

`STATE` is one word, ordered by what to look at first: `failing` (actionable
now), `quarantined` (actionable, historical), `behind` (a tuning problem),
`idle` (never run), `ok`.

## One writer at a time

`AvailableNow` drains until the source is empty, so a backlog can make one cron
tick outlast the interval that started it — and then the next tick begins while
the first is still going. duckstream takes an advisory lock beside the catalog
and refuses the second one by name:

```
duckstream: another duckstream run already holds this catalog: pid 4123 on pi5,
running for 92s (lock file '/opt/pipeline/catalog.ducklake.lock').
...
Either let the running pass finish, or bound how long a tick may take with
AvailableNow(max_batches=N) so it cannot outrun your schedule.
```

Without it you would still be safe — DuckDB's own file lock on the catalog
refuses the second process — but the message would be about a metadata handle
rather than about two copies of your pipeline running. A lock whose owner is
provably dead **on this host** is broken automatically, so a hard kill does not
need manual clearing; a lock from another machine never is, because liveness
cannot be checked from here.

## The guarantee, stated honestly

**Exactly-once, and only with a replayable source.**

One transaction per trigger:

```
BEGIN
  write output rows        -- sink
  record the batch         -- state
  append the source offset -- checkpoint
COMMIT
```

DuckLake commits one snapshot per transaction, not per statement, so the output
rows and the offset saying they were consumed become durable **together**, in
one snapshot. A crash before the commit replays from the stored offset, because
nothing was written; a crash after it is durable, because everything was. There
is no third outcome.

Three things follow, and all three are worth knowing before relying on it:

- **The sink is not idempotent on its own.** `mode="update"` merges on the model
  key, which keeps the *row set* stable — a replay updates the existing row
  rather than adding one. But an additive fold is not idempotent in its
  *values*: a replayed batch adds its counts and sums a second time.
  `mode="append"` is not idempotent in either sense. Exactly-once comes from the
  offset being committed in the same transaction as the rows, and from nowhere
  else. Drive the sink yourself, outside the engine, and you do not have it.
- **The source must be replayable.** A file source is, which is why it is the
  foundation rather than a convenience. Brokers without durable offsets (MQTT)
  cannot provide exactly-once directly; they are modelled as *landing writers* —
  at-least-once into durable storage, which then becomes a replayable source.
- **The sink and the state store must live in the same catalog.** A DuckDB
  transaction cannot write to two attached databases, so sinking to DuckLake
  while checkpointing offsets to a separate DuckDB file is not merely
  inadvisable, it is impossible. duckstream puts both in DuckLake.

An **empty trigger opens no transaction and writes no checkpoint**, so the
snapshot history stays a record of work done rather than of cron ticks that
happened.

## Operating envelope

Measured on a development box with `threads=2`, against `duckdb==1.5.5`:

| | Cost |
|---|---|
| DuckLake commit, per trigger that writes anything | **~15 ms** |
| Idle trigger — reads, writes nothing, no snapshot | **~1.3 ms** |
| Full trigger — sink insert, offset, watermark, batch record | **~25.7 ms** |
| Process cold start under cron — interpreter, `LOAD`, `ATTACH`, settings | **~235 ms** |
| A `lateness` horizon, on top of the same trigger without one | **~+5 ms** |
| …when it actually drops rows, so a filter view is built | **~+6 ms** |
| Sealed `append`: accumulator merge, emit and evict | **~+20 ms** |
| Recording a failed attempt (own transaction, so its own snapshot) | **~+15 ms** |

The floor is the snapshot, not the query. A DuckLake transaction that writes
nothing costs about what plain DuckDB costs; the moment it writes anything, it
pays for the commit.

Event time is priced the same way. Reading the newest event time and both drop
counts is folded into the row count the trigger already did, so it costs about
0.3 ms; the horizon's ~5 ms is the extra **state append** that makes the
watermark durable, and it is irreducible for the same reason the offset append
is. The filter that removes out-of-horizon rows is only built when the scan says
there is something to remove, so a healthy stream never pays for it.

**The practical conclusion: seconds, not sub-second.** Under cron the real floor
is roughly 0.3 s before any work happens, so a sub-second trigger interval is
meaningless. Choose a cadence in seconds or minutes, and size
`max_rows_per_trigger` / `max_files_per_trigger` so a batch finishes well inside
it. Memory is governed by DuckDB's buffer manager materialising the batch, so
bounding rows in flight is the control that works — a faster UDF buys no
headroom.

## Limits, and what is not in v1

- **Tumbling windows only.** `minute`, `hour` and `day`. Sliding and session
  windows are post-v1, and `month` is absent on purpose: its length varies, and
  the seal boundary is computed as a single fixed offset from the watermark.
- **One horizon per model, and no per-source horizons.** A model with several
  sources would need a watermark per source and a rule for combining them; v1
  has one source per model, so it has one watermark.
- **Only the additive tier executes.** Tiers two and three are classified,
  reported by `duckstream models`, and then **refused rather than executed**
  wherever a fold is involved — `mode="update"`, and windowed `append`, which
  folds into its accumulator. The `sufficient_statistics` and
  `recompute_window` strategies arrive in phase 3. Until then a tier-two or
  tier-three model either drops its `grain` and uses `mode="append"` (which
  never folds, so any tier is fine) or is reduced to
  `count`/`sum`/`min`/`max`.
- **Single writer.** One process, models run sequentially, no locking — under a
  drain-and-exit trigger contention is structurally impossible rather than
  merely unlikely. A portable lock arrives with a long-running trigger.
- **One parquet file per trigger.** Data inlining is disabled unconditionally,
  because it defaults to capturing any write under 10 rows into the code path
  carrying DuckLake's open correctness bugs — which would make behaviour depend
  on batch size. The cost is small files; compaction is phase 4.
- **No MQTT source yet.** Phase 5.

And the small honest ones:

- **A file rewritten with identical size *and* mtime is not detected.** File
  identity is `(path, size, mtime)`, so a rewrite that changes neither is
  indistinguishable from no rewrite. Write new files, or touch them.
- **The open-window accumulator is bounded by the horizon, not by the stream.**
  Sealing evicts, so it holds roughly the windows still inside `lateness`. A
  window whose key never appears again stays open until some *other* row pushes
  the watermark past its end — which for a sensor that stops reporting entirely
  means its last window seals only when a later-timestamped row arrives from
  anywhere in the same model.
- **State grows by two rows per committed trigger** — the offset and the batch
  record — plus a third, the watermark, for a model with a lateness horizon.
  Nothing reclaims it automatically. That is deliberate: append-only state
  measured about 4x faster than mutating one row per model, because a matching
  DuckLake `DELETE` writes a tombstone file and costs ~26 ms, and it is strictly
  safer under a crash, since an uncommitted append is simply invisible.
  `DuckLakeStateStore.prune()` bounds the growth; schedule it with your other
  maintenance.
- **A halted model retries on every tick.** Cheaply, and writing nothing after
  the first verdict — so fixing the underlying problem is all it takes to
  recover — but it does re-read and re-plan its batch each time.
- **Quarantine is whole-batch.** The unit skipped is the batch, not the row or
  the file, so `max_files_per_trigger: 1` is what makes it precise. Narrowing a
  failing batch to isolate the offending file is not implemented.
- **Compaction is still phase 4**, and the open-window accumulator makes it more
  pressing rather than less: sealed `append` writes to it every trigger and
  deletes from it on every seal.

## Requirements

- **Python ≥ 3.11**
- **`duckdb==1.5.5`, pinned exactly.** Not a nicety: DuckLake's SQL differs from
  DuckDB's in version-sensitive ways, the inlining default is version-sensitive,
  and the aggregate classifier reads the shape of `json_serialize_sql` output.
  Re-verify against a new DuckDB before moving the pin.
- `pyarrow`, `numpy`, `pyyaml`

```bash
pip install -e .
```

**Deployment note.** `ATTACH 'ducklake:…'` autoloads the DuckLake extension, but
autoload still needs the extension present or downloadable. Run

```sql
INSTALL ducklake;
```

**once on the target device while it has network**, or the first run of a
disconnected deployment fails.
