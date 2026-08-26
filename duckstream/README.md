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
| `sufficient_statistics` | a bare `avg`, `stddev*`, `var*`, `variance`, `corr`, `covar*` | `sufficient_statistics` | Persist a mergeable `(n, mean, M2)` per statistic argument; derive the result from it. Exact, still no rescan. **Not `(sum, sum_sq, count)`** — that form returns 524 for a true variance of 0.25 at Unix-timestamp magnitudes, and exactly 0.0 at 1e8 with a small spread. |
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
touched and how much memory that is allowed to cost. A model that resolves to
`recompute_window` must also declare a `grain` — there is no window to recompute
without one.

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

## Tier three: recomputing a window from source

Some aggregates cannot be folded at all. A median, an exact `count(distinct …)`,
anything order-dependent, and any UDF over a whole window — there is no pair of
partial answers that combine into the true one. For these duckstream re-derives
the affected windows from source, which is the only correct strategy and is what
`recompute_window` means.

```python
Model(
    name="minute_spectrum",
    source=FileSource("landing/", marker="_READY"),
    time_column="event_ts",
    grain="minute",
    key=["window_ts", "sensor_id"],
    strategy="recompute_window",          # inferred from the tier anyway
    memory_profile="materialising",
    udfs=["my_pkg.signal:spectrum"],
    aggregates={"bins": "spectrum(list(value ORDER BY event_ts))"},
    sink=TableSink("marts.minute_spectrum", mode="update"),
    limits=BatchLimits(max_rows_per_trigger=200_000),
)
```

What happens on each trigger:

1. the batch is read to find **which windows it touched** — untouched history is
   never re-read;
2. those windows are grouped into **chunks** sized from an estimated row count,
   bounded by `max_rows_per_trigger`;
3. for each chunk, duckstream asks the catalog **which consumed files can hold a
   row in that range**, reads exactly those, and aggregates them;
4. the chunk's window range in the sink is **cleared and re-inserted**, inside
   the same transaction as the offset — so a reader never sees it empty, and a
   key whose source rows have gone disappears with them.

Three consequences worth planning for.

**Cost scales with the window, not the batch.** A one-row batch landing in a
busy hour recomputes that whole hour. Measured on a dev box at `threads=2`, the
recompute step is ~17.5 ms for a window held in one file and ~31.3 ms for one
held in a hundred — an intercept of about 17 ms, which every write pays, plus
about **0.14 ms per file in the window**.

If that is too expensive the lever is a finer `grain`, or fewer files per window
(retention and compaction). It is emphatically **not** a smaller
`max_files_per_trigger`: that makes it worse, by recomputing the same window
more often.

**`max_rows_per_trigger` is the memory knob and it bounds both halves** — the
batch the source plans *and* the rows one recompute holds. Memory is governed by
DuckDB's buffer manager materialising `LIST(...)`, so bounding rows in flight is
the only control that works; a faster UDF buys no headroom at all.

**File selection is a hint, never truth.** `duckstream.consumed_files` carries
`min_ts`/`max_ts`/`n_rows` per consumed file, and the recompute uses them to
avoid opening files it cannot need — which matters because opening a file costs
~0.1 ms whether or not its contents are read, and a year at one file a minute is
525,000 of them. A file the index cannot place is **read**, never skipped, so a
missing or stale entry costs time and never an answer. Files consumed before the
index existed, or by a model that was not tier three at the time, carry no
bounds and are read by every recompute.

## Ingesting from MQTT

MQTT cannot be a duckstream source. Once a message is acknowledged it is gone
from the broker, so there is nothing to resume from and nothing to replay — and
replayability is what exactly-once rests on. Declaring `type: mqtt` on a model is
refused, with the alternative in the error.

The alternative is two processes and a directory between them:

```
broker  ->  MqttLandingWriter  ->  landing/  ->  file source  ->  engine
              at least once                       exactly once
```

```python
# subscriber.py -- a daemon, under systemd or supervisor
from duckstream.sources.mqtt import MqttLandingWriter

MqttLandingWriter(
    "landing/",
    "sensors/#",
    host="broker.local",
    qos=1,                       # 1 or 2: at QoS 0 the broker never re-delivers
    client_id="duckstream-accel",  # stable, so the broker keeps your session
    flush_rows=10_000,
    flush_seconds=60,
).run_forever()
```

The model then reads the landing tree like any other file source, under cron:

```yaml
source: {type: file, path: "landing/", marker: _READY}
```

`pip install duckstream[mqtt]` — `paho-mqtt` is an **optional** dependency, so a
deployment with no MQTT in it carries none.

### What "at least once" costs you, exactly

Every message is acknowledged **only after** the completion marker is on disk.
That is the guarantee, and it is the opposite of what an MQTT client does by
default: `paho` acks a QoS-1 message the moment it arrives, so anything still
buffered when a process dies is gone with the broker believing it was delivered.
duckstream sets `manual_ack` and releases each message only once it is durable.

The price is duplicates. After a crash the broker re-delivers whatever was never
acked, so the same reading can land **twice, in two different files**. duckstream
does not de-duplicate that and cannot: the two files are genuinely different
files holding genuinely different rows, and nothing marks one as a repeat.

So "exactly-once" is exactly-once over **files**, not over sensor readings. If a
duplicate reading matters, give the model a merge key and `mode="update"`, which
converges to one row per key however many times a reading arrives. A windowed
`append` model will count it twice, correctly and visibly.

### Two things to get right

**Run it as its own process.** The writer must stay connected; the engine is a
drain-and-exit job under cron. Fusing them would put a long-lived network loop
inside the process holding the catalog, which is what the trigger model exists to
avoid — and a DuckDB *file* held open locks every other reader out.

**A quiet topic still needs flushing.** `run_forever` polls the time trigger on
its own thread, so a sensor that stops reporting still lands what it was holding.
Embedding the writer in your own event loop instead? Call `tick()` on a timer —
otherwise the last readings before a sensor went silent sit in memory until a
message that never comes.

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

## Running it on a Raspberry Pi

The target duckstream was built for, and the constraint that shapes every number
below: **on a Pi the bottleneck is storage, not CPU.** SD and USB are poor at
small random reads and have a finite write budget, so the costs that matter are
files opened and bytes written, not cycles.

**Batch fewer, larger files.** Inlining is disabled (it routes small writes into
DuckLake's buggiest path), so every trigger writes one parquet file per table. A
one-minute cron is 1,440 files a day per table, and reading a window out of them
later costs **~0.1 ms per file listed whether or not it is skipped** — statistics
pruning skips data pages, never the file open. Prefer minutes to seconds, and
size `max_rows_per_trigger` so a batch fills a file worth writing.

**The offset no longer grows, but the file count still does.** The file source
used to carry every file it had ever consumed inside its offset, rewritten in
full on every trigger — 7.97 MB of writes per trigger after a year at one file a
minute, ~11.2 GB a day, for nothing but re-recording file names. Those files are
now rows in `duckstream.consumed_files`, and a trigger writes **4.9 KB**
regardless of how much has been read. `duckstream status` reports
`consumed_files` and still warns if an offset ever passes 1 MB, which after this
change should mean a source that has reintroduced the problem.

What is *not* fixed is the number of files. `latest_offset()` still walks the
whole landing tree every trigger, and reading a window out of many small files
costs ~0.1 ms per file listed. Keep the landing tree drained: retention at the
source is the lever, and it is now a speed problem rather than one that wears
the card out.

**Set `memory_limit` and `threads` yourself.** DuckDB defaults to most of the
machine, which on a 4 GB Pi sharing space with everything else is not what you
want. Memory is governed by the buffer manager materialising a batch, so bounding
rows in flight is the control that works:

```yaml
settings:
  memory_limit: "1GB"
  threads: 2
```

**Expect seconds, not sub-second.** Process start alone is ~235 ms on a dev box
before any work happens, and a committing trigger costs ~15 ms more. A
sub-second cron interval is meaningless; a `AvailableNow(max_batches=N)` bound
keeps one tick from outrunning the next when catching up.

## Limits, and what is not in v1

- **Tumbling windows only.** `minute`, `hour` and `day`. Sliding and session
  windows are post-v1, and `month` is absent on purpose: its length varies, and
  the seal boundary is computed as a single fixed offset from the watermark.
- **One horizon per model, and no per-source horizons.** A model with several
  sources would need a watermark per source and a rule for combining them; v1
  has one source per model, so it has one watermark.
- **All three tiers execute, and tier three costs what it costs.** A
  `recompute_window` model re-reads every consumed file that can hold a row in
  the windows a batch touched, so its per-trigger cost scales with the *window*
  rather than with the batch. That is not an implementation detail to be tuned
  away — a median or an FFT has no decomposition, so there is no cheaper correct
  answer. Keep tier three off the hot path where a foldable tier will do, and
  note that a query containing a Python UDF is forced onto a single thread.
- **`recompute_window` needs a `grain`.** "Recompute the affected window" has no
  meaning without windows, and the only other consistent reading is re-deriving
  the whole history on every trigger. Unwindowed `append` is exempt: it never
  folds and never revises a row, so any tier is fine there.
- **Windowed `append` is still additive-only.** It folds into an accumulator
  while a window is open, and tiers two and three do not fold. Use
  `mode="update"` for those, which recomputes or maintains state as the tier
  requires.
- **A recomputed window replaces what was there.** The range is cleared and
  re-inserted, so a key whose source rows have gone disappears with them. That
  is the correct behaviour and it does mean a recompute writes a tombstone per
  window range — deliberate, and the reason chunking is sized rather than
  per-window.
- **The file → time-range index is a hint.** It only narrows which files a
  recompute opens; a file it cannot place is read rather than skipped, so a
  wrong or missing entry costs time and never an answer. Rows written before
  the index existed, and by models that were not tier three at the time, carry
  no bounds and are therefore read by every recompute.
- **Single writer.** One process, models run sequentially. An advisory lock
  (`duckstream/lock.py`) reports an overlapping run in words, but the actual
  safety comes from DuckDB's own lock on the catalog file — never promote the
  advisory one to the guarantee.
- **One parquet file per trigger.** Data inlining is disabled unconditionally,
  because it defaults to capturing any write under 10 rows into the code path
  carrying DuckLake's open correctness bugs — which would make behaviour depend
  on batch size. The cost is small files; compaction is phase 4.
- **MQTT is a landing writer, not a source, and never will be one.** Once a
  message is acked it is gone from the broker, so there is no offset to resume
  from and nothing to replay — exactly-once is unformable directly. duckstream
  subscribes, writes durably to disk, and acknowledges only once the data is on
  disk; a `file` source then reads that tree replayably. `type: mqtt` on a model
  is refused with the alternative spelled out. See "Ingesting from MQTT".

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
  record — plus a third, the watermark, for a model with a lateness horizon, and
  one row per file consumed in `duckstream.consumed_files`. Nothing reclaims it
  automatically. That is deliberate: append-only state measured about 4x faster
  than mutating one row per model, because a matching DuckLake `DELETE` writes a
  tombstone file and costs ~26 ms, and it is strictly safer under a crash, since
  an uncommitted append is simply invisible. `DuckLakeStateStore.prune()` bounds
  the growth; schedule it with your other maintenance.
- **`prune()` will not touch `consumed_files`, and neither should you.** Every
  other state table keeps a *history* of positions and only its newest row is
  read, which is what makes dropping the rest safe. Those rows **are** the
  position: delete one and duckstream forgets it read that file, reads it again,
  and folds its rows into the mart a second time. A year of them is 8.8 MB.
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
