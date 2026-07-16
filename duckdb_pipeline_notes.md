# DuckDB + DuckLake Pipeline Notes

## How to enter DuckDB and connect DuckLake

From the pipeline folder, open an in-memory DuckDB session:

~~~bash
duckdb :memory:
~~~

Then run:

~~~sql
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:metadata.ducklake' AS my_ducklake (DATA_PATH 'ducklake_data');
USE my_ducklake;
~~~

If the catalog already exists, attach without DATA_PATH:

~~~sql
ATTACH 'ducklake:metadata.ducklake' AS my_ducklake;
USE my_ducklake;
~~~

## What we implemented

1. Added step-level benchmarking.
- Captures step name, status, rows ingested, bytes ingested, duration, start and end timestamps.
- Persists records in marts.pipeline_benchmark.

2. Fixed catalog attach behavior.
- First-time attach uses DATA_PATH.
- Re-attach of existing catalog does not require DATA_PATH.

3. Added CDC file tracking.
- Tracks processed files in staging.processed_files_cdc.
- Processes only unprocessed files.
- Early exits when no new folders and no new files exist.

4. Improved throughput.
- Staging now reads multiple parquet files in one go instead of one file at a time.

5. Controlled memory pressure.
- Added DuckDB runtime tuning (memory_limit, threads, preserve_insertion_order).
- Added chunked file batches via max_files_per_batch.

## Error we hit

- During marts FFT step, we got OOM:
  - Step: sql/marts/accel_fft_mart.sql
  - Error: Out of Memory Error while allocating memory block
- Context observed:
  - Staging had 19,440,000 rows for that run.
- Trade-off after fix:
  - Runtime increased because processing is split into smaller batches.

### Captured failure snapshot (marts_fft)

~~~text
data_ingested_rows:   0
data_ingested_bytes:  358088202
data_mb:              341.5
duration_ms:          8003.81
start_ts:             2026-07-13 19:09:24.283566
end_ts:               2026-07-13 19:09:32.286227
error_message:        Out of Memory Error: could not allocate block of size 256.0 KiB (3.1 GiB/3.1 GiB used)
~~~

### What likely caused OOM in marts_fft

Even though input parquet size was ~341.5 MB, the FFT step can use much more memory than input size because:

1. It groups data by minute and location, then materializes ordered LIST arrays for x, y, z, and magnitude.
2. FFT UDFs run on those arrays, creating additional in-memory intermediate vectors.
3. MERGE execution also needs memory for source + target processing.
4. Parallel threads multiply peak memory usage under heavy aggregation.

So the effective working set can grow to multiple GB, which explains OOM at ~3.1 GB used.

## What the FFT mart query does

The FFT mart step:

1. Reads fact accelerometer rows for the current batch_id.
2. Groups by minute window and location_key.
3. Builds ordered arrays of x, y, z, and magnitude.
4. Applies FFT UDFs to produce frequency and spectrum arrays.
5. Merges results into marts.accel_fft_mart.

## Get size of data where it failed

Use benchmark table data_ingested_bytes for failure rows:

~~~sql
SELECT
  run_id,
  batch_id,
  step_name,
  sql_file,
  status,
  data_ingested_rows,
  data_ingested_bytes,
  ROUND(data_ingested_bytes / 1024.0 / 1024.0, 2) AS data_mb,
  duration_ms,
  start_ts,
  end_ts,
  error_message
FROM marts.pipeline_benchmark
WHERE status = 'FAILED'
  AND step_name = 'marts_fft'
ORDER BY ins_tmstmp DESC;
~~~

If you want all failed steps (not only FFT):

~~~sql
SELECT
  step_name,
  sql_file,
  batch_id,
  data_ingested_rows,
  data_ingested_bytes,
  ROUND(data_ingested_bytes / 1024.0 / 1024.0, 2) AS data_mb,
  error_message,
  ins_tmstmp
FROM marts.pipeline_benchmark
WHERE status = 'FAILED'
ORDER BY ins_tmstmp DESC;
~~~

## Useful config knobs

Add these in config env files as needed:

~~~ini
duckdb_memory_limit=2GB
duckdb_threads=2
duckdb_preserve_insertion_order=false
max_files_per_batch=10
~~~

## Realtime Queue Worker (New)

Added a new worker script: `realtime_queue_worker.py`.

This worker is designed for cron-triggered mini-batch processing and includes:

1. Ready marker gating.
- Reads only folders that contain a marker file (default `_READY`).
- Helps avoid reading partially written files.

2. Settle delay.
- Marker must be older than `worker_settle_seconds` (default 60).
- Adds safety buffer for in-flight writes.

3. Single-instance lock.
- Uses a lock file (default `.realtime_worker.lock`).
- If another worker instance is running, it exits early.

4. Queue table with retries.
- Table: `staging.realtime_job_queue`.
- Tracks file path, size, mtime, status, attempts, next retry time, last error.
- Status lifecycle: `QUEUED -> RUNNING -> SUCCESS` or `FAILED`.

5. Exponential backoff retry.
- Failed batch files are marked failed and rescheduled with increasing delay.
- Controlled by retry config values.

6. CDC + queue integration.
- Uses existing CDC success records to avoid duplicate reprocessing.
- New/changed files from ready folders are upserted into the queue.

7. Mini-batch execution.
- Due files are chunked by `max_files_per_batch`.
- Each chunk is processed through the existing pipeline function `run_for_files(...)`.

### Realtime worker config keys

~~~ini
ready_marker_name=_READY
worker_settle_seconds=60
worker_max_files_per_run=500
worker_max_retry_attempts=10
worker_retry_delay_seconds=120
max_files_per_batch=10
~~~

### Example run

~~~bash
python realtime_queue_worker.py dev
~~~

Optional custom lock file:

~~~bash
python realtime_queue_worker.py dev --lock-file /tmp/realtime_worker.lock
~~~


<!-- add in the readme and in the dubdbnotes that we installed MQTT, and we used
sudo apt update
sudo apt install -y mosquitto mosquitto-clients
sudo systemctl enable mosquitto
sudo systemctl start mosquitto -->

Now what I've done further is that I have implemented a solution where 
Data is dumped every 30 secs, and every 40secs a cronjob runs that takes these new files and processes this.
This is implementing microbatching.

I have used a thread, that is called when we have to implement dumping of data, 
because we dont need to block our data fetch process.

Here we have a buffer of 10s, so in these 10s we need our processing to be done, and that is doable.
Next I am going to do benchmarking on this
