# Engine vibration pipeline on ClickHouse

A port of `~/python_scripts/lakehouse-rpi5/engine_pipeline` (DuckDB/DuckLake +
duckstream) and its dashboard `engine_app` to RabbitMQ + ClickHouse on the Pi.

```
producer.py (laptop) --MQTT engine/vibration--> RabbitMQ :1883
   --> queue clickhouse_engine --> engine.vibration_queue (RabbitMQ engine)
   --> engine.fact_engine_vibration        raw readings, 6 h TTL
         |-- engine.fact_minutes           readings per machine-minute, as they arrive
         |-- calc views (the calculations, defined once, over raw)
         |-- refreshable MVs store settled results:
         |     engine.engine_minute_health        minutes older than 10 min
         |     engine.engine_minute_spectrogram   minutes that have closed
         '-- v_* views = stored rows + live calculation of the newest data
```

## Stored vs live

Each calculation is a view over the raw table (`calc_minute_health`,
`calc_minute_spectrogram`). A refreshable materialized view uses it to store
results once they have settled, and the `v_*` view you query returns the
stored rows plus a live calculation of anything not stored yet. The
`source` column says which each row is.

| | stored when | live part |
|---|---|---|
| minute health | older than 10 min, and no new reading for 2 min | newest ~10 minutes |
| spectrogram | minute closed (+15 s grace), no new reading for 10 s | minute still filling |

The storing jobs plan from `engine.fact_minutes`, a small table of readings
per machine per minute kept up to date as rows arrive. Each machine gets its
own to-do list, and a stored minute that later receives more readings (for
example a backlog replayed after a restart) is recalculated.

The STFT is the executable UDF `stft_db` ([udf/stft_db.py](udf/stft_db.py)):
the numpy code of the original `spectro.py`, run by ClickHouse as a
persistent process. It takes about 4 ms per machine-minute and gives exactly
the same result as the original; this was checked cell by cell.

## Star schema

- **Fact**: `engine.fact_engine_vibration` (natural key `machine`)
- **Dimension**: `engine.machine_hlp` assigns surrogate keys to new machines
  every 30 s. `engine.machine_dim` is SCD2, derived from the append-only
  `engine.machine_dim_history`. To re-rate a machine:
  ```sql
  ALTER TABLE engine.machine_hlp UPDATE nominal_rpm = 1500, upd_tmstmp = now()
  WHERE machine_name = 'Karachi_ENG01';
  ```
  Within 30 s the old version gets a `valid_to` and every minute in the views
  shows the new value, because the dimension is resolved when you query.
- **Views**: `v_fact_engine_vibration`, `v_engine_minute_health`,
  `v_engine_minute_spectrogram`, `v_engine_minute_anomaly`. The anomaly
  baseline (each machine's quietest sixth of minutes) is built from stored
  minutes, so a new machine shows `calibrating` for its first ~13 minutes.

## Setup (on the Pi)

```bash
sudo ./udf/install_udf.sh   # once: installs the stft_db function
./setup.sh                  # asks for the RabbitMQ 'iot' and ClickHouse passwords
```

`setup.sh` creates the RabbitMQ queue and applies `sql/01..06`. It stops if
the UDF isn't installed yet. It always ends by re-attaching the RabbitMQ
consumer: if the queue is ever deleted, RabbitMQ cancels ClickHouse's
consumer and it does not come back on its own.

To start from scratch (**deletes all data** in the `engine` database):

```bash
./reset.sh                # drops everything, deletes the queue, runs setup.sh
./reset.sh --keep-queue   # same, but keeps messages waiting in the queue
```

`reset.sh` lists what will be deleted and asks you to type `reset`. The drop
statements are in [sql/drop_all.sql](sql/drop_all.sql) if you'd rather run them
yourself (for example in DBeaver) and then `./setup.sh`. The `stft_db` UDF
stays installed. Check the
background jobs with:

```sql
SELECT view, status, last_success_time, exception
FROM system.view_refreshes WHERE database = 'engine';
```

## Producer (laptop)

```bash
pip install numpy paho-mqtt
python producer.py --host 192.168.0.104 --machine Karachi_ENG01
python producer.py --host 192.168.0.104 --schedule "healthy:120,imbalance:90,misalignment:90,bearing:120,looseness:90"
```

Copy `producer.py` and `engine_signal.py` together. It samples at 500 Hz
(one timestamp every 2 ms) and sends **50 readings per MQTT message** (one
100 ms block, newline-separated JSON), i.e. 10 messages/s per producer, with
up to 10,000 unconfirmed messages in flight. Every 10 s it prints readings/s,
how many the broker acknowledged, its backlog, and any readings dropped
because its local buffer (5 minutes) filled. `--batch 1` sends one reading
per message, as before.

Why batching: sending one reading per message capped each producer at
~400 readings/s on the Pi. RabbitMQ took ~1.4 s to confirm QoS 1 messages
under load, and paho allowed only 500 unconfirmed messages per connection.
Readings then waited ~50 s in the producer's buffer and were dropped once it
was full (17–20% loss). With batching, a test producer delivered 20,000 of
20,000 readings, exactly 500 per second, 2.000 ms apart.

## Dashboard (Pi)

```bash
cd app && RMQ_PASSWORD=... python3 server.py      # http://192.168.0.104:8090
```

It asks for the ClickHouse password (or reads `CH_PASSWORD`). Without
`RMQ_PASSWORD`, the queue-backlog tile is empty. It refreshes loss, waveforms
and backlog every 5 s, the spectrogram and ingest rate every 10 s, and minute
health, anomaly and machines every 30 s (`--fast/--medium/--slow`).

## Capacity (measured on this Pi 5)

6 producers x 500 msg/s (one reading per message, QoS 1), dashboard open,
one hour of history: 0 readings lost, ~1 s from sensor to ClickHouse, and
ClickHouse's query work (dashboard, jobs, views) used 16 CPU-seconds per
2.5 minutes. That excludes the RabbitMQ consumer's background threads, which
use about a core at ~2,400 single-reading messages/s; batching 50 readings
per message cuts that per-message cost. Measure process CPU with
`research/tools/sysmon.py`.
RabbitMQ alone accepted 7,800 msg/s.

What made the difference: the STFT as a numpy UDF (4 ms instead of ~800 ms
per machine-minute), the anomaly baseline in one pass (ClickHouse re-runs a
WITH subquery at every reference), the tiered dashboard refresh, and a
summary answered from part metadata instead of scanning the whole table.

PREWHERE: ClickHouse already moves WHERE filters to PREWHERE by itself
(`optimize_move_to_prewhere`); writing it explicitly measured the same. The
real savings come from filtering on the primary key `(machine, timestamp)`,
which lets ClickHouse skip whole blocks, and from metadata for count/min/max.

## Differences from the original

- MQTT → parquet landing → duckstream is replaced by RabbitMQ → ClickHouse
  RabbitMQ engine. Unacknowledged messages wait in the queue.
- The fact has a 6 h TTL (`ENG_FACT_RETAIN_HOURS` was declared but never
  enforced). Stored minutes are kept forever.
- Kurtosis and skew use ClickHouse `kurtPop`/`skewPop` instead of raw-moment
  formulas (same definition, more numerically stable). Medians are
  `quantileExactInclusive(0.5)`, which matches DuckDB's `median`.
- SCD2 is an append-only history instead of UPDATEs in a transaction, so a
  machine can never be left without a current row.
- The anomaly baseline comes from stored minutes (see above).
- The dashboard's catalog-lock workarounds are gone. "Chunks remaining" is
  now the RabbitMQ backlog, and the batch-timing chart is now ingest rate per
  machine. Host metrics go to `engine.host_samples` (72 h TTL) instead of a
  DuckDB file.

IOT Password = BiEaZqaFtQknvvc7