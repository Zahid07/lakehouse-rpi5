# Audit columns and experiment protocol

How to compare the two implementations of the engine vibration pipeline:

- **ClickHouse**: RabbitMQ + ClickHouse, this repository
- **DuckDB**: the original DuckDB/duckstream pipeline, `~/python_scripts/lakehouse-rpi5/engine_pipeline`

Both record the same audit data under the same names, so one query or one
analysis script works on either. Both were tested end to end:

| Implementation | Test | Readings | Missing | End-to-end lag |
|---|---|---|---|---|
| DuckDB | 1 producer, 150 s | 75,050 | 0 | 6.3 s avg, 11.2 s p99 |
| ClickHouse | 3 producers | 347,550 | 0 | 1.3 s avg, 1.9 s p99 |

The ClickHouse lags include the laptop's ~0.75 s clock offset, which was not
yet corrected (see §4).

## 1. Where the audit data comes from

Every message from a producer carries, per reading:

| field | set by | meaning |
|---|---|---|
| `timestamp` | producer | sample time, exactly `t0 + seq / 500 Hz` |
| `seq` | producer | per-run sample index; gaps = lost readings |
| `run_id` | producer `--run-id` | experiment label |
| `sent_at` | producer | wall clock (UTC) when the message carrying the reading was published |

Producers send 50 readings per MQTT message (one 100 ms block); `--batch 1`
sends one per message. Use the same producer settings for both
implementations. Either producer works: `engine_pipeline/producer.py` in
either repository sends the same signal and fields.

Each implementation then records:

| column | ClickHouse | DuckDB |
|---|---|---|
| `received_at` | when ClickHouse's RabbitMQ consumer handed the block to storage | when the ingest process decoded the message |
| `landed_at` | — (no landing step; NULL) | when the parquet chunk holding the reading was written |
| `stored_at` | `inserted_at`: when the row became queryable | commit time of the duckstream batch that loaded the reading's chunk, derived via `chunk` → `duckstream.consumed_files` → `duckstream.batches` |

## 2. The two audit views (same columns in both)

| ClickHouse | DuckDB |
|---|---|
| `engine.v_audit_reading` | `curated.v_audit_reading` |
| `engine.v_audit_minute` | `marts.v_audit_minute` |

### `v_audit_reading`: one row per reading

`implementation, run_id, machine, seq, timestamp, sent_at, received_at,
landed_at, stored_at`, plus these lags in ms:

| lag | from → to | ClickHouse | DuckDB |
|---|---|---|---|
| `producer_lag_ms` | timestamp → sent_at | simulator artifact (see §4) | same |
| `transport_lag_ms` | sent_at → received_at | WiFi, RabbitMQ, consumer flush (≤ 1 s) | WiFi, RabbitMQ |
| `landing_lag_ms` | received_at → landed_at | NULL | landing flush (≤ 10 s) |
| `store_lag_ms` | received/landed → stored_at | ~0 (same insert) | wait for the next pipeline cycle (2 s) |
| `end_to_end_ms` | sent_at → stored_at | the comparable total | the comparable total |
| `sample_to_stored_ms` | timestamp → stored_at | includes producer_lag | same |

### `v_audit_minute`: one row per machine-minute

`samples, expected_samples, missing_samples` (from the `seq` range),
`first/last_sent_at`, `first/last_received_at`, `last_stored_at`, and:

| column | ClickHouse | DuckDB |
|---|---|---|
| `health_ready_at` | when the minute's last reading was stored (the live view reflects it at once) | commit of the health batch that consumed the minute's newest chunk |
| `health_stored_at` | first write to the stored table (minutes older than 10 min) | = ready_at (stored when committed) |
| `spectrogram_ready_at` | as health | commit of the spectrogram batch for its newest chunk |
| `spectrogram_stored_at` | first write to the stored table (closed minutes) | = ready_at |
| `*_store_count` | number of times stored (> 1 = recalculated for late readings) | NULL (not tracked) |
| `*_lag_s` | seconds from the minute's end to the matching time | same |

**Ready** is the fair comparison: when the minute's final value could be
read. **Stored** shows each design's storage policy.

## 3. System metrics: `research/tools/sysmon.py`

One neutral instrument for both implementations. It writes one CSV row every
5 s with the same columns whichever implementation runs:

- **host:** CPU (mean and busiest core), load averages, temperature, clock
  speed, throttling flags, memory, swap, SD-card and network throughput;
- **per component:** CPU % of one core, resident memory and process count for
  `clickhouse`, `rabbitmq`, `mosquitto`, `duck_pipeline`, `duck_ingest`,
  `stft_udf`, `ch_dashboard`, `duck_dashboard`, `producers_local`;
- **RabbitMQ:** remote producers connected, messages waiting, publish and
  deliver rates;
- **storage:** bytes of the ClickHouse `engine` database (if `CH_PASSWORD` is
  set) and of any `--path` (e.g. the DuckDB lake).

Process CPU is read from `/proc`, so it includes background threads.
ClickHouse's own `OSCPUVirtualTimeMicroseconds` counter misses them: its
RabbitMQ consumer threads used ~110% of a core at 2,400 msg/s and appeared in
no query log.

## 4. Things to know before trusting the numbers

- **Clock sync.** `timestamp` and `sent_at` are the laptop's clock;
  `received_at` and `stored_at` are the Pi's. Every lag that crosses machines
  includes the clock difference. Sync the laptop to the Pi (chrony on the Pi,
  `w32tm` on the laptop) **before** starting producers, never during a run.
  Record the offset at the start and end of each run with
  `w32tm /stripchart /computer:192.168.0.104 /samples:5 /dataonly`.
  - Check: `min(transport_lag_ms)` should be a few ms. It was ~757 ms before
    syncing, i.e. the offset.
  - Counts, loss and the 500 Hz rate use `seq` and one clock, so they are not
    affected.
- **`producer_lag_ms` is about −50 ms, by design of the simulator.** It
  generates each 100 ms block at the start of the block's window, so a message
  leaves up to 100 ms before its last sample's timestamp. Identical in both
  producers; don't read it as a real latency.
- **The last minute of a run is partial**, and its `*_ready_lag_s` can be
  negative (it completed before its clock end). Exclude it from averages.
- **The newest minutes** in ClickHouse's `v_audit_minute` have NULL
  `health_stored_at` until they are ~12 minutes old. That's the storage policy,
  not a failure.
- **Retention.** ClickHouse keeps raw readings (and so `v_audit_reading`)
  6 hours, and `fact_minutes` (the source of `v_audit_minute`) 30 days. For
  runs longer than 6 hours, export `v_audit_reading` periodically (§6), or
  raise the TTL first (`ALTER TABLE engine.fact_engine_vibration MODIFY TTL
  toDateTime(timestamp) + INTERVAL 48 HOUR`). The DuckDB fact table keeps
  everything.
- **Throttling.** The Pi reported under-voltage and throttling since boot
  (`throttled = 0xf0000`). `sysmon` records the flag each sample; discard or
  report runs where the "now" bits (0–3) are set.

## 5. Running a comparison

For each implementation, with **only that implementation running**:

1. **Prepare a clean state.**
   - ClickHouse: `./reset.sh` (keeps the UDF). Stop the DuckDB ingest and
     pipeline.
   - DuckDB: a fresh `ENG_ROOT` (its model tables gained columns), and stop
     ClickHouse's consumer so it doesn't consume the same messages:
     `sudo systemctl stop clickhouse-server`, or delete the
     `clickhouse_engine` queue and re-run `./setup.sh` afterwards.
2. **Sync the clocks** and note the offset.
3. **Start `sysmon`** with the run label:
   ```bash
   # ClickHouse
   CH_PASSWORD=... python3 research/tools/sysmon.py --implementation clickhouse \
       --run-id ch_6p_24h --out ch_6p_24h.csv
   # DuckDB
   python3 research/tools/sysmon.py --implementation duckdb \
       --run-id duck_6p_24h --path ~/engine-lake-audit --out duck_6p_24h.csv
   ```
4. **Start the pipeline.** The DuckDB pipeline has two timing profiles,
   documented at the top of its `run.sh` and `run_ingest.sh`. Run both
   profiles, and say in the paper which one each result used:
   - **designed** (default): `ENG_FLUSH_SECONDS=10`, `ENG_PIPELINE_INTERVAL="2 seconds"`,
     as its authors tuned it;
   - **matched**: `ENG_FLUSH_SECONDS=1`, `ENG_PIPELINE_INTERVAL="1 second"`, the
     same ~1 s freshness target as ClickHouse, whose consumer writes about once
     a second. Its extra cost (a landing directory per second, back-to-back
     cycles logged as `OVERRUN`) is part of the result.

   - ClickHouse: it's already running.
   - DuckDB:
     ```bash
     ENG_ROOT=~/engine-lake-audit ENG_MQTT_PASSWORD=... ./engine_pipeline/run_ingest.sh
     ENG_ROOT=~/engine-lake-audit ./engine_pipeline/run.sh
     ```
5. **Start the producers** on the laptop with the same run label, e.g.
   `--run-id ch_6p_24h`, one per machine.
   - To study the effect of more producers, add them one at a time at noted
     times (or use separate runs such as `ch_1p`, `ch_2p` … `ch_6p`). `sysmon`
     records `producers_connected` automatically.
6. At the end: stop the producers, wait a minute, stop `sysmon`, and export
   (§6).

## 6. Comparison queries

**Per-run summary, ClickHouse:**
```sql
SELECT run_id, count() AS readings, uniqExact(machine) AS machines,
       round(avg(end_to_end_ms)) AS e2e_avg_ms,
       round(quantile(0.5)(end_to_end_ms)) AS e2e_p50_ms,
       round(quantile(0.99)(end_to_end_ms)) AS e2e_p99_ms,
       round(min(transport_lag_ms)) AS min_transport_ms   -- ~clock offset
FROM engine.v_audit_reading WHERE run_id = 'ch_6p_24h' GROUP BY run_id;

SELECT run_id, count() AS minutes, sum(samples) AS samples,
       sum(missing_samples) AS missing,
       round(100 * sum(missing_samples) / sum(expected_samples), 3) AS loss_pct,
       round(quantile(0.5)(health_ready_lag_s), 2) AS health_ready_p50_s,
       round(quantile(0.99)(health_ready_lag_s), 2) AS health_ready_p99_s,
       round(quantile(0.5)(spectrogram_ready_lag_s), 2) AS spectro_ready_p50_s
FROM engine.v_audit_minute WHERE run_id = 'ch_6p_24h' GROUP BY run_id;
```

**The same for DuckDB** (DuckDB SQL; open the catalog read-only while the
pipeline is stopped: it allows one process at a time):
```sql
ATTACH 'ducklake:/home/zahid/engine-lake-audit/catalog.ducklake' AS l (READ_ONLY);
SELECT run_id, count(*) AS readings, count(DISTINCT machine) AS machines,
       round(avg(end_to_end_ms)) AS e2e_avg_ms,
       round(quantile_cont(end_to_end_ms, 0.5)) AS e2e_p50_ms,
       round(quantile_cont(end_to_end_ms, 0.99)) AS e2e_p99_ms,
       round(min(transport_lag_ms)) AS min_transport_ms
FROM l.curated.v_audit_reading WHERE run_id = 'duck_6p_24h' GROUP BY run_id;

SELECT run_id, count(*) AS minutes, sum(samples) AS samples,
       sum(missing_samples) AS missing,
       round(100.0 * sum(missing_samples) / sum(expected_samples), 3) AS loss_pct,
       round(quantile_cont(health_ready_lag_s, 0.5), 2) AS health_ready_p50_s,
       round(quantile_cont(health_ready_lag_s, 0.99), 2) AS health_ready_p99_s,
       round(quantile_cont(spectrogram_ready_lag_s, 0.5), 2) AS spectro_ready_p50_s
FROM l.marts.v_audit_minute WHERE run_id = 'duck_6p_24h' GROUP BY run_id;
```

**Behaviour over a long run** (lag per 10 minutes; ClickHouse shown, DuckDB is
the same with `time_bucket(INTERVAL 10 MINUTE, stored_at)`):
```sql
SELECT toStartOfInterval(stored_at, INTERVAL 10 MINUTE) AS t, count() AS readings,
       round(quantile(0.5)(end_to_end_ms)) AS p50_ms,
       round(quantile(0.99)(end_to_end_ms)) AS p99_ms
FROM engine.v_audit_reading WHERE run_id = 'ch_6p_24h' GROUP BY t ORDER BY t;
```

**Export for plotting** (both write CSV):
```bash
clickhouse-client --password -q "SELECT * FROM engine.v_audit_minute WHERE run_id = 'ch_6p_24h' FORMAT CSVWithNames" > ch_6p_24h_minutes.csv
duckdb -c "ATTACH 'ducklake:$HOME/engine-lake-audit/catalog.ducklake' AS l (READ_ONLY);
           COPY (SELECT * FROM l.marts.v_audit_minute WHERE run_id = 'duck_6p_24h') TO 'duck_6p_24h_minutes.csv' (HEADER)"
```
Join these with the `sysmon` CSVs on time (`ts_utc`, UTC) to relate lag and
loss to CPU, memory, temperature and producer count.
