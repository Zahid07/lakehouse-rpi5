# The accelerometer pipeline, on duckstream

A replacement for `run_pipeline.py` and `subscriber.py`, built on the
`duckstream` package in this repository. Everything here is a *consumer* of
duckstream — the package imports nothing from this folder and is meant to be
extracted to its own repository.

```
MQTT ──► ingest.py ──► landing/*.parquet ──► pipeline.py ──► DuckLake
         at-least-once   durable, queryable    exactly-once   fact + 2 marts + SCD2 dim
```

## Running it

Two processes. The first stays up; the second is the loop.

```bash
./run_ingest.sh                       # MQTT -> landing tree (systemd/supervisor)
./run.sh                              # drain + star schema, every 2 seconds
./run.sh --interval "10 seconds"      # slower
./run.sh --once                       # a single cycle, then exit
```

Environment (all defaulted, override as needed):

| Variable | Default | |
|---|---|---|
| `DS_ROOT` | `~/duckstream-accel` | catalog and lake data |
| `DS_LANDING` | `$DS_ROOT/landing` | the landing tree |
| `DS_MEMORY_LIMIT` | `1200MB` | DuckDB buffer manager |
| `DS_MQTT_HOST` / `DS_MQTT_PORT` / `DS_MQTT_TOPIC` | `localhost` / `1883` / `sensors/accel` | same broker as `subscriber.py` |
| `DUCKSTREAM_SAMPLE_RATE_HZ` | `100` | the FFT frequency axis |

Check the models before deploying — this runs every load-time validation
without opening a catalog:

```bash
duckstream validate --config duckstream_pipeline/models.yaml
duckstream models   --config duckstream_pipeline/models.yaml   # resolved tiers
```

## What is here

| File | |
|---|---|
| `models.yaml` | the three models — the fact and both marts |
| `udfs.py` | FFT in **Arrow** mode (2x native, `CONTEXT.md` 1.2) |
| `decode.py` | MQTT payload → a row, with a real `TIMESTAMP` |
| `ingest.py` | MQTT → landing. Replaces `subscriber.py` |
| `dimensions.py` | the SCD2 dimension duckstream does not own |
| `pipeline.py` | the loop: drain, then maintain the star schema |
| `sql/curated/` | `location_hlp`, `location_dim` |
| `sql/marts/views.sql` | read-time join and rounding |

## The three models, and why each is that tier

Run `duckstream models` and it prints:

```
fact_accelerometer     additive               delta_merge         -
accel_hourly_summary   sufficient_statistics  sufficient_statistics  hour
accel_minute_spectrum  non_foldable           recompute_window    minute
```

**`fact_accelerometer`** — row level, `mode: update`, keyed on
`(timestamp, location)`. Update rather than append because MQTT is
at-least-once: the same reading can land in two files and duckstream does not
de-duplicate — exactly-once is over *files*, not readings. A merge key converges
to one row per reading however many times it arrives.

The aggregates are `min(x)`, not `any_value(x)`, and the classifier is what
settled it: `any_value` is non-deterministic, so it classifies **non_foldable**
and is refused. That refusal is right — a replayed batch must reproduce the row
it produced the first time, and `any_value` may not. Over a one-row group `min`
is the identity.

**`accel_hourly_summary`** — tiers 1 and 2 mixed. The old mart recomputed every
touched hour in full; with 30-second dumps an hour is touched ~120 times. Here
`avg` and `stddev` are maintained as `(n, mean, M2)` and the hour is never
re-read.

> **Do not put `round()` in these aggregates.** Measured with
> `classify_model`: `round(avg(x), 4)` classifies as **non_foldable**, because
> the classifier only proves foldability for a *bare* aggregate. Wrapping them
> silently demotes the whole model to tier three and recomputes every hour.
> Rounding lives in `sql/marts/views.sql` — and must, since tier two stores
> `(n, mean, M2)` and rounding the stored value would corrupt the next merge.

**`accel_minute_spectrum`** — tier 3, `recompute_window`. No pair of partial
spectra combines into the spectrum of a concatenation, so the window is
re-derived. Cost follows the **window**, not the batch: a one-row batch landing
in a busy minute re-derives that minute, so turning `max_files_per_trigger`
*down* makes it worse.

## What stays yours

`location_hlp` and `location_dim` are unchanged in behaviour. duckstream has no
notion of a surrogate key or a validity interval and should not acquire one.
They run inside the engine's own attached session, between the drain and the
detach, so they inherit the same catalog lock.

**The marts key on `location`, the natural key**, and `sql/marts/views.sql`
resolves `location_key` at read time. That is not a limitation being worked
around — a surrogate assigned by another process in another transaction cannot
join the one transaction that commits rows and offset together.

It also **fixes a bug the old mart had**: that one denormalised `city` and
`country` into the mart row, so a corrected dimension left every already-written
row stale until its hour happened to be recomputed — which for a sealed hour is
never. A view is always current.

## Verified on this Pi

One cycle over 240,000 real readings takes ~2.3 s. Against a full recompute of
the same landing files, after **two** runs so windows are revised across
batches:

| Tier | Result |
|---|---|
| fact, 420,000 readings | exact |
| hourly `avg`/`stddev`/`min`/`max` | 0 mismatches, worst relative error **7.9e-16** |
| minute FFT, 70 windows | 0 bin-count mismatches, 0 value mismatches, **exact** |

That last row is `CONTEXT.md` section 4's FFT bug — 51 spectrum bins where the
truth was 201 — shown to be impossible here.

## Two things to know before changing it

**Idle cycles must write no snapshots.** `CREATE TABLE IF NOT EXISTS`,
`CREATE OR REPLACE VIEW` and an empty SCD2 transaction each write a *catalog
snapshot* even when nothing changes. The first version of `dimensions.py` ran
all three every cycle: **three snapshots per idle cycle**, which at a
two-second interval is ~130,000 a day on a pipeline doing nothing. Every entry
point now checks before acting. If you add a maintenance step here, measure its
idle cost the same way:

```bash
# snapshots before, N idle cycles, snapshots after — the delta must be 0
./run.sh --interval "1 seconds" --max-cycles 5
```

**Strip SQL comments before splitting on `;`, not after.** A comment containing
a semicolon gets cut in half the other way round, and its tail reaches the
parser as prose. `dimensions._statements` does it in the right order and says
why.

---

# The dashboard

`app/` is a read-only web view over the lakehouse. Standard library plus the
`duckdb` the pipeline already needs — no web framework, because serving seven
JSON endpoints on a 4 GB Pi does not warrant one.

```bash
.venv/bin/python -u app/server.py --port 8080      # then http://<pi-ip>:8080/
```

It binds `0.0.0.0` and sends `Access-Control-Allow-Origin: *`, so a React dev
server on another machine can call it without a proxy. Every endpoint is
read-only and carries no credentials.

## The constraint that shaped it, and the bug that proved it

The catalog is **single-attach**: while one process holds a DuckLake catalog a
second cannot `ATTACH` it, *even `READ_ONLY`*, and even while the holder sits
idle. That is `CONTEXT.md` 1.6 applied to the `.ducklake` file, which is itself
a DuckDB database.

The obvious backend design — attach per request, detach immediately — **starved
the pipeline**. Measured: 60 API requests during active writes all returned
200, while **six of eight pipeline cycles failed** with
`Conflicting lock is held ... by PID <the server>`. Holding the catalog briefly
is not enough if you take it *often*: one open page made four requests every
three seconds. Worse, the reader retried on conflict and the pipeline does not,
so every collision was resolved in the reader's favour.

So the shape is:

- **one background thread** reads the catalog, in a *single* attach that fetches
  everything the page needs;
- **every HTTP request is served from that cached snapshot** and never touches
  the catalog. Ten browsers cost exactly what one costs;
- **the reader yields** — one attempt, no retry loop, and a longer wait after a
  conflict, because the writer has no retry of its own;
- **when it loses**, the previous figures are kept and marked `stale` rather
  than blanking the page.

## Push, not polling

The pipeline pokes `POST /api/refresh` after every cycle, from `on_cycle` —
which runs *after* `Engine.close()`, so the catalog is already released. **That
is the one read guaranteed not to collide.** Measured: cache age drops to
0.1–0.6 s right after each cycle, against 10 s on the timer alone.

```bash
.venv/bin/python -u -m duckstream_pipeline.pipeline --interval "3 seconds" --notify
```

`--notify` is best-effort with a 1 s timeout and swallows every error: a
dashboard that is down, slow or absent can never delay or fail a cycle. The
server keeps a slow 15 s fallback timer for when no notification arrives.

## Endpoints

| Route | Source | Notes |
|---|---|---|
| `/api/status` | cache | counts, state, chunk progress, dimension |
| `/api/hourly` | cache | `marts.v_accel_hourly_summary` |
| `/api/spectrum` | cache | 4 newest windows, first 240 bins |
| `/api/recent` | cache | newest 600 readings with x, y, z |
| `/api/throughput` | cache | last 120 batches, with timings |
| `/api/readings` | **live** | history paging — see below |
| `/api/refresh` | — | `202`; wakes the refresher |

`/api/readings` is the **only** endpoint that reads the catalog live, and the
exception is deliberate: browsing history is user-initiated and occasional, not
a three-second poll, so a single attach now and then is a different proposition
from the per-request attaching that caused the starvation above. The discipline
is the same — one attempt, `503` rather than competing.

It uses **keyset pagination** (`before=<timestamp>`), not `OFFSET`. An offset
scan re-reads every skipped row, so paging back through a day of 100 Hz data
would get slower with every click; a keyset page starts exactly where the last
one ended and costs the same every time.

## Two numbers that were wrong, and what they mean now

### "Waiting in landing" → **Chunks processed / Chunks remaining**

The old tile counted *every* parquet file in the landing tree. That is not a
backlog: the pipeline never deletes a consumed chunk, because a tier-three model
reads chunks back when it recomputes a window. The number therefore only ever
grew, and measured "how long has this been running".

Now:

- `chunks_total` — every chunk that has landed (from the files; no catalog)
- `chunks_processed` — chunks **every** model has consumed
- `chunks_remaining` — `total − processed`, clamped at zero

A chunk counts as processed only when *all three* models have taken it, not when
any one has. The models consume independently, and reporting the fastest would
show zero remaining while a slower model was still behind — which is exactly the
reassuring-but-wrong number this replaced. Verified: 8 landed / 5 processed →
3 remaining; pipeline catches up → 0.

### The throughput total disagreed with "Readings stored"

It summed `rows_in` across models, and **every model reads every chunk** — so
139,928 readings showed as 419,784, exactly 3x. It now reports the largest
single model's total and states the multiplier: *"258,131 readings × 3 models"*.

It is also a **window**, not a lifetime: `/api/throughput` returns the last 120
batches, the same ones the chart plots. It will legitimately read lower than the
"Readings stored" tile once the pipeline has run past that window, so the pill
says **"last 120 batches"**.

## How `TOOK` is calculated

Straight from the engine's own record — the dashboard adds no timing of its own:

```sql
date_diff('millisecond', b.started_at, b.committed_at) AS ms
FROM duckstream.batches b
```

| Timestamp | Set where | Marks |
|---|---|---|
| `started_at` | `engine.py`, `state.record_batch_start` | after the batch is planned and its id assigned, **before** the source view is bound |
| `committed_at` | `engine.py`, `state.record_batch_end` | **inside** the transaction, immediately before `COMMIT` |

So `TOOK` spans: bind the view → scan for counts and event time → aggregate and
write to the sink → record consumed-file rows → record the batch row → `COMMIT`
issued.

**Two things it does not include, and both matter when reading it:**

*The commit itself.* `committed_at` is stamped just before `COMMIT`, because
that is the only timestamp available from inside a transaction that is about to
become one. The DuckLake commit costs **~26 ms on this Pi** and is often the
largest single term, so `TOOK` is a slight **under**-estimate of wall clock.

*Other models.* Each model has its own batch and its own row, so one chunk
produces three rows. A whole cycle is roughly the three plus three commits —
which is why the pipeline log reports ~1,000 ms for a cycle while the individual
batches read 70–120 ms. **That gap is the commit floor, three times over.**

For work done, read `TOOK`. For wall clock, read the cycle duration in the
pipeline log. Neither is wrong; they measure different things.

## Nothing was instrumented for any of this

Every figure above comes from tables duckstream already writes:

| Table | Carries |
|---|---|
| `duckstream.batches` | `started_at`, `committed_at`, `rows_in`, `rows_out` per batch |
| `duckstream.consumed_files` | `relpath`, `size` in bytes, `batch_id`, per chunk per model |

They only needed joining. No timers were added, nothing runs in the hot path,
and the numbers are the engine's own rather than something the dashboard
measured about itself.

## What the throughput actually shows

The three models process the *same* chunk at very different speeds, consistently
across every batch on this Pi:

| Model | Tier | Throughput |
|---|---|---|
| `accel_hourly_summary` | 2 — folds incrementally | **~830k rows/s** |
| `fact_accelerometer` | 1 — but writes every row out | **~540k rows/s** |
| `accel_minute_spectrum` | 3 — FFT through a Python UDF | **~510k rows/s** |

That is why the legend reports rows/s **per model** rather than averaging: the
average would hide the one number worth watching. It is also `CONTEXT.md` 1.22's
Python-UDF penalty showing up in a real pipeline rather than in a benchmark.
