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
MODEL          TIER      STRATEGY     SOURCE          SINK
hourly_counts  additive  delta_merge  file(landing/)  table(marts.hourly_counts, update)

$ duckstream run --config models.yaml
hourly_counts: 1 batch, 100 source rows, through batch 1

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

The floor is the snapshot, not the query. A DuckLake transaction that writes
nothing costs about what plain DuckDB costs; the moment it writes anything, it
pays for the commit.

**The practical conclusion: seconds, not sub-second.** Under cron the real floor
is roughly 0.3 s before any work happens, so a sub-second trigger interval is
meaningless. Choose a cadence in seconds or minutes, and size
`max_rows_per_trigger` / `max_files_per_trigger` so a batch finishes well inside
it. Memory is governed by DuckDB's buffer manager materialising the batch, so
bounding rows in flight is the control that works — a faster UDF buys no
headroom.

## Limits, and what is not in v1

- **No event time yet.** Watermarks, window sealing and lateness policy are
  phase 2. `grain` gives tumbling windows over a timestamp column, but late data
  simply lands in its window whenever it arrives; nothing is dropped, and
  nothing is counted as late.
- **Only the additive tier executes.** Tiers two and three are classified,
  reported by `duckstream models`, and then **refused rather than executed** by
  `mode="update"`. The `sufficient_statistics` and `recompute_window` strategies
  arrive in phase 3. Until then a tier-two or tier-three model either uses
  `mode="append"` or is reduced to `count`/`sum`/`min`/`max`.
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
- **State grows by three rows per trigger** — offset, watermark, batch record —
  and nothing reclaims it automatically. That is deliberate: append-only state
  measured about 4x faster than mutating one row per model, because a matching
  DuckLake `DELETE` writes a tombstone file and costs ~26 ms, and it is strictly
  safer under a crash, since an uncommitted append is simply invisible.
  `DuckLakeStateStore.prune()` bounds the growth; schedule it with your other
  maintenance.
- **`rows_out` is recorded as NULL.** Obtaining it would mean running the
  aggregation twice. It arrives with the metrics module, along with the `status`
  command.

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
