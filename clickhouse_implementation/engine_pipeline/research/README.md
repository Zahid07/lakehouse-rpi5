# Research record: engine vibration pipeline on RabbitMQ + ClickHouse (Raspberry Pi 5)

What was built, what went wrong, how each problem was diagnosed and fixed, and
the measurements before and after. Work done 26–27 September 2026. All
measurement files are in [data/](data/), raw logs and test scripts are in
[raw/](raw/), and the code changes are in
[before_after_code.md](before_after_code.md).

## 1. System and goal

**Hardware:** Raspberry Pi 5, 4 GB RAM, 4 cores, Debian 13 (trixie), SD-card
storage. Producers run on a Windows 11 laptop on the same WiFi network.

**Workload:** simulated engine vibration sensors, 500 Hz each, one JSON reading
per MQTT message (QoS 1). **Target: 6 producers × 500 msg/s = 3,000 msg/s**,
without data loss.

**Starting point:** an existing pipeline (`lakehouse-rpi5/engine_pipeline`) on
DuckDB/DuckLake with a custom micro-batch engine (duckstream). MQTT readings
were written to parquet "landing" files, then folded into a star schema
(a raw-readings fact table, a machine dimension with SCD2, per-minute health
statistics, a per-minute STFT spectrogram, and an anomaly score). A Python
dashboard read it. Much of that code worked around DuckLake allowing only one
process to open the catalog at a time.

**New design:**
```
producer (laptop) --MQTT--> RabbitMQ (MQTT plugin) --> durable queue
  --> ClickHouse RabbitMQ engine --> fact_engine_vibration (raw, 6 h TTL)
        |-- fact_minutes            (readings per machine-minute, insert-time MV)
        |-- calc_* views            (each calculation defined once, over raw)
        |-- refreshable MVs store settled results:
        |     engine_minute_health        (minutes older than 10 min)
        |     engine_minute_spectrogram   (closed minutes)
        '-- v_* views = stored rows UNION ALL live calculation of the newest data
```
The stored/live split was the user's own design decision: store calculated
values for settled data, and calculate only the newest data live. The
spectrogram's live part is the still-filling minute only; health and anomaly
calculate the last ~10 minutes live.

## 2. Method

- **Correctness:** every calculation was compared against the original numpy
  code on simulated data from the original signal model (`signal.py`).
- **Load tests:** a temporary second ClickHouse server on the Pi, installed
  with the same `setup.sh`, and its own RabbitMQ queue. 6 real producer
  processes ran on the Pi at 500 msg/s each. The table held one hour of history
  for 6 machines (10.8 M rows), and the dashboard server was running. The
  harness ([raw/scripts/load_test.sh](raw/scripts/load_test.sh)) sampled the
  queue, ClickHouse ingest and CPU every 10 s.
- **CPU attribution:** ClickHouse's `OSCPUVirtualTimeMicroseconds` counter in
  `system.events` (difference over the run), and per-query CPU from
  `system.query_log` ProfileEvents, grouped by normalised query.
  **Correction, found later:** that counter only covers threads doing query
  work. The RabbitMQ engine's background consumer threads are not in it.
  Measured afterwards from `/proc` per-thread counters, they used ~110% of a
  core at ~2,400 single-reading messages/s. So the "CPU-s" figures below are
  **query CPU** (dashboard, jobs, views), not total process CPU. The live
  "CPU ch" samples (from `top`, summing both ClickHouse servers on the Pi)
  were 110–290% throughout the runs. Future runs use `research/tools/sysmon.py`,
  which reads process CPU from `/proc`.
- **Loss:** per-producer `seq` counters. Loss = 1 − distinct(seq) / (max(seq)
  − min(seq) + 1).

## 3. What went wrong, and how it was fixed

The full list (17 items, with symptom, cause, fix and evidence) is in
[data/issues_log.csv](data/issues_log.csv). The main story:

### 3.1 Production incident: 20–80% of readings lost at 6 producers

Six producers ran from the laptop. The dashboard showed "0 producers" while
listing 6 machines. Investigation ([data/incident_*.csv](data/)) found:

- Readings reached ClickHouse **~90 s late**, at **174–217 rows/s per machine
  instead of 500**.
- RabbitMQ's queue had **no backlog** (0 ready), so ClickHouse was keeping up
  with what the queue delivered. The bottleneck was upstream of the queue.
- 2 of the 6 MQTT connections were in RabbitMQ's **`blocked`** state (flow
  control). ClickHouse used 160% CPU and RabbitMQ 130%.
- The producers buffered locally up to 20,000 messages (about 40 s). Once full,
  paho refused new publishes, and the producer ignored the return code. **From
  19:44, each machine lost 20–80% of its readings per minute.**
- The "0 producers" display was a separate bug: machines counted as active
  only if a reading *timestamped* in the last 60 s existed, and every reading
  was 90 s old.

### 3.2 Diagnosis: the broker and ingest were not the bottleneck

| Test | Result |
|---|---|
| RabbitMQ alone, 6 publishers, 1 reading/msg | **7,797 msg/s** (2.6× target) |
| RabbitMQ alone, 50 readings/msg | 225,862 readings/s |
| Full pipeline, 6 × 500 msg/s, **no dashboard** (run 1) | 0% loss, 0.5–1.2 s behind |
| Full pipeline **with dashboard** and 1 h history (run 3) | 0% loss here, but ClickHouse used 98 CPU-s in 153 s |

The per-query profile of run 3 showed that **the dashboard's queries, not
ingest, used almost all of ClickHouse's CPU**:

| Component (run 3, before) | CPU-s | Share |
|---|---|---|
| Dashboard: spectrogram view (live STFT) | 42.4 | 43% |
| Dashboard: anomaly view | 16.8 | 17% |
| Job: store spectrogram minutes | 12.4 | 13% |
| Dashboard: fact summary (full scan) | 10.5 | 11% |
| Dashboard: health view | 5.9 | 6% |
| Everything else | ~10 | 10% |

On the real server, the table held several hours of data, and the full-scan
summary cost grows with the table. Together, these starved ClickHouse's
RabbitMQ consumer, RabbitMQ throttled the publishers, and the producers' full
buffers dropped readings.

### 3.3 Fixes

1. **STFT as an executable UDF.** ClickHouse has no FFT, so the first port
   computed a direct DFT in SQL: bit-identical to numpy, but **~800 ms per
   machine-minute versus 0.9 ms for numpy's FFT**. Replaced by the original
   numpy code as a ClickHouse executable UDF: RowBinary I/O read with
   `np.frombuffer`, and a persistent process (`executable_pool`). **4 ms per
   machine-minute**, identical results ([data/stft_implementations.csv](data/stft_implementations.csv)).
   pandas and pyarrow were considered; neither helps, because the costs are
   parsing and process start-up, not dataframes.
2. **Anomaly view in one pass.** ClickHouse re-runs a `WITH` subquery at every
   reference, so the ported view ran the live health calculation about 5 times
   per query. The baseline now comes from stored minutes, with median and MAD
   computed from per-machine arrays: **1.2–2.5 s → 0.23 s per query**.
   Trade-off: a new machine shows "calibrating" for about 13 minutes instead
   of 3.
3. **Tiered dashboard refresh:** 5 s for loss, waveforms and backlog; 10 s for
   spectrogram and ingest rate; 30 s for minute health, anomaly and machines.
   Minute values change once a minute.
4. **Metadata summary:** `count()` and `max(timestamp)` from part metadata:
   **11 M rows / 435 ms → 8 rows / 3 ms**, independent of retention.
5. **Producer reports drops** (paho return code), shows its backlog, and has a
   5-minute buffer.
6. **Per-machine planning of stored minutes** (`fact_minutes`). This fixed a
   bug found during testing: a single shared high-water mark skipped late
   minutes for one machine (the backfill stalled at 126 of 366 minutes). It
   also recalculates a stored minute when late readings arrive.

**PREWHERE** was evaluated explicitly ([data/prewhere_benchmark.csv](data/prewhere_benchmark.csv)).
ClickHouse already moves `WHERE` filters to PREWHERE automatically
(`optimize_move_to_prewhere`). For a non-key filter, the automatic move cut
CPU from 772 to 574 ms, and explicit PREWHERE measured the same (558 ms,
equal rows read). The large savings came instead from filtering on the primary
key `(machine, timestamp)` (the live 15-minute calculation reads 0.67 M of 11 M
rows) and from part metadata.

## 4. Before and after

Same test rig, 6 producers × 500 msg/s, dashboard open, one hour of history
([data/load_test_summary.csv](data/load_test_summary.csv),
[data/query_cpu_profiles.csv](data/query_cpu_profiles.csv),
[data/load_test_timeseries.csv](data/load_test_timeseries.csv)):

| | Run 3 (before) | Run 4 (UDF only) | Run 5 (after) | Run 6 (after, 6 min) |
|---|---|---|---|---|
| ClickHouse **query** CPU (see correction in §2) | 98 CPU-s / 153 s | 98 CPU-s / 148 s | **16 CPU-s / 149 s** | **53 CPU-s / ~390 s** |
| ≈ cores, query work only | 0.64 | 0.66 | **0.11** | **0.14** |
| Readings lost | 0 | 0 | 0 | **0 of 1,080,000** |
| Producer drops | not measured | not measured | 0 | 0 |
| Sensor → ClickHouse | 0.2–4.7 s | 0.8–1.5 s | 0.1–1.6 s | 0.5–1.6 s |

| Component | Before (run 3) | After (run 5) |
|---|---|---|
| Spectrogram view (dashboard) | 42.4 CPU-s (4,310 ms/read) | 2.3 CPU-s (262 ms/read) |
| Anomaly view | 16.8 CPU-s (1,162 ms/run) | 1.3 CPU-s (338 ms/run) |
| Store spectrogram minutes | 12.4 CPU-s | 0.7 CPU-s |
| Fact summary | 10.5 CPU-s | 0.1 CPU-s (metadata) |
| Health view | 5.9 CPU-s | 1.1 CPU-s |

Run 4 shows why the UDF alone was not enough: the spectrogram cost fell from
42.4 to 2.6 CPU-s, but the anomaly view became the largest cost (51.5 CPU-s),
because of the repeated CTE execution. Its per-run cost also rose from run 3 to
run 4 (1,162 → 2,510 ms) as the test database accumulated more machines and
minutes, which is the growth the one-pass rewrite removed.

### 4.1 Later findings: one reading per message was the real ceiling

With the dashboard fixed, a later run from the laptop still showed 12–29%
loss and ~48 s of delay at 6 producers. Diagnosis (details in
[data/issues_log.csv](data/issues_log.csv), items 18–21):

- RabbitMQ was not CPU-bound (23%) and the SD card was not busy (4%), but its
  **QoS 1 confirmations took ~1.4 s** under load (a probe: 300 messages took
  1.46 s, i.e. 205 msg/s for one connection, against 7,800 msg/s idle).
- Each producer allowed **500 unconfirmed messages** (paho), so each was capped
  at ~350–500 msg/s. Measured: **~400 readings/s per machine**.
- The excess waited in the producer's 20,000-message buffer (20,000 ÷ 400 ≈
  50 s = the delay) and was dropped once it was full. The gaps averaged 100
  readings, two whole 50-reading blocks.
- Separately, a leftover RabbitMQ table (`iot.accel_queue`, whose queue had
  been deleted) was restarting ~75 times/s and writing 24,000 log lines a
  minute. Removed; not the CPU cause.

**Fix:** 50 readings per MQTT message (newline-separated JSON, which
ClickHouse's JSONEachRow reads as 50 rows) and a 10,000-message in-flight
window. That is 10 messages/s per producer instead of 500. Verified:
**20,000 of 20,000 readings, exactly 500 per second, 2.000 ms apart**; then
from the laptop, 2–3 producers at 500/s each with 0 missing. The DuckDB
pipeline's ingest and producer were changed the same way, for a fair
comparison.

**Measurement artifacts found and fixed:**
- The dashboard averaged incomplete 10-s buckets (showed 421 and 430 rows/s
  while 500/s arrived).
- The producer stamped readings from a start time truncated to the second
  (up to 1 s of false delay).
- The laptop's clock was ~0.75 s behind the Pi's: the minimum
  sent-to-received time was 757 ms, where the network takes a few ms. See
  [AUDIT.md](AUDIT.md) §4.

## 5. Correctness

[data/correctness_checks.csv](data/correctness_checks.csv):

- **Minute health:** all 42 test minutes matched numpy exactly on RMS, peak,
  crest, kurtosis, skew, axial ratio, RPM mean and RPM standard deviation,
  whether stored or live.
- **Spectrogram:** SQL-DFT and UDF versions both matched numpy exactly
  (15,163/15,163 cells).
- **End to end:** the real producer through RabbitMQ gave 0 lost, 0 duplicated
  readings, with an average delay of 1.1 s.
- **SCD2:** re-rating a machine produced the correct history.
- **Arrival summary:** `fact_minutes` totals equal the raw table exactly,
  including after re-running setup.

## 6. Limitations (threats to validity)

- In the load tests, the producers ran **on the Pi itself**, not on the laptop
  over WiFi, so they competed for the Pi's CPU. The laptop-and-WiFi path of the
  original incident has not been re-tested after the fixes.
- History was one hour of synthetic data. The incident server held several
  hours of real simulator data.
- Each configuration was run once (runs 3–5 for 2 minutes, run 6 for 6
  minutes). There are no repeated trials or confidence intervals.
- The live "CPU ch" column in the time series sums **both** ClickHouse servers
  on the Pi (test and production). The per-run totals and per-query figures
  come from the test server's own counters and are not affected.
- The run 1 log was printed to the terminal only; its file is a verbatim
  transcription ([raw/logs/run1_no_dashboard_90s_TRANSCRIBED.txt](raw/logs/)).
  The query profiles and incident figures were likewise captured from
  terminal output into the CSVs.
- The q1 PREWHERE test read 0 rows because nothing was arriving during the
  benchmark, so it does not show a PREWHERE effect either way.

## 7. Files

| File | Contents |
|---|---|
| `data/issues_log.csv` | all 17 problems: symptom, root cause, fix, evidence |
| `data/incident_seq_loss_per_minute.csv` | per machine-minute loss during the incident |
| `data/incident_ingest_rate.csv`, `incident_rabbitmq_connections.csv`, `incident_system_state.csv` | incident diagnosis |
| `data/broker_capacity.csv` | RabbitMQ MQTT throughput alone |
| `data/load_test_summary.csv` | runs 1–6: configuration, loss, CPU |
| `data/load_test_timeseries.csv` | 10-s samples of every run (105 rows) |
| `data/query_cpu_profiles.csv`, `query_cpu_profile_run6_6min.csv` | per-query CPU, before/UDF/after |
| `data/stft_implementations.csv` | numpy vs SQL DFT vs UDF: time, memory, correctness |
| `data/backfill_timing.csv` | late-minute bug and fix |
| `data/prewhere_benchmark.csv`, `view_costs_after_fixes.csv` | PREWHERE evaluation, view costs |
| `data/correctness_checks.csv` | validation results |
| `before_after_code.md` | the code of each optimisation, before and after |
| `raw/logs/` | raw load-test logs |
| `raw/scripts/` | load test harness, broker benchmark, PREWHERE benchmark |
