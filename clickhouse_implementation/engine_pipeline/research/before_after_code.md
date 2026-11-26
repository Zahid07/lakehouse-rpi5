# Key code, before and after each optimisation

The "before" versions are copied from earlier revisions of the files in
`engine_pipeline/`. The "after" versions are what is in the repository now.

## 1. STFT: pure-SQL DFT → numpy executable UDF

**Before** (`sql/04_spectrogram.sql`): a direct DFT. ClickHouse has no FFT, so
each of the 257 bins is a sum over all 512 samples of every frame:
59 × 257 × 512 × 2 ≈ 15.5 M trig-multiplies per machine-minute. It took about
800 ms per machine-minute, and 56 MB even with `max_block_size = 2048`.

```sql
CREATE OR REPLACE VIEW engine.calc_minute_spectrogram AS
WITH 500.0 AS fs, 512 AS frame, 500 AS hop, intDiv(frame, 2) + 1 AS n_bins,
     arrayMap(n -> 0.5 - 0.5 * cos(2 * pi() * n / (frame - 1)), range(frame)) AS hann,
     arraySum(hann) AS gain
SELECT window_ts, machine AS machine_name, any(n) AS sample_count, ...,
       arrayFlatten(arrayMap(t -> t.2, arraySort(groupArray((f, bins))))) AS spec_db
FROM (
  SELECT window_ts, machine, n, last_ins, f,
         arrayMap(t -> toFloat32(t.2), arraySort(groupArray((b, db)))) AS bins
  FROM (
    SELECT window_ts, machine, n, last_ins, f, b,
           sqrt(pow(arraySum(arrayMap((x, k) -> x * cos(2 * pi() * b * k / frame), yw, range(frame))), 2)
              + pow(arraySum(arrayMap((x, k) -> x * sin(2 * pi() * b * k / frame), yw, range(frame))), 2))
             * (2.0 / gain) * if(b = 0 OR b = n_bins - 1, 0.5, 1.0) AS mag,
           round(greatest(20 * log10(greatest(mag, 1e-12)), -120.0), 1) AS db
    FROM (
      SELECT window_ts, machine, n, last_ins, f,
             arraySlice(vals, f * hop + 1, frame) AS y, arrayAvg(y) AS mu,
             arrayMap((x, w) -> (x - mu) * w, y, hann) AS yw
      FROM (SELECT ... groupArray((timestamp, vib_r)) ... GROUP BY window_ts, machine HAVING n >= frame)
      ARRAY JOIN range(intDiv(length(vals) - frame, hop) + 1) AS f
    )
    ARRAY JOIN range(n_bins) AS b
  )
  GROUP BY window_ts, machine, n, last_ins, f
)
GROUP BY window_ts, machine
SETTINGS max_block_size = 2048;
```

**After**: the original numpy code as an executable UDF (`udf/stft_db.py`,
`udf/stft_db_function.xml`). It runs as a persistent process
(`executable_pool`) and reads RowBinary, so each array arrives as raw floats
that `np.frombuffer` reads with no parsing. About 4 ms per machine-minute,
identical to the original cell for cell.

```sql
CREATE OR REPLACE VIEW engine.calc_minute_spectrogram AS
SELECT window_ts, machine AS machine_name, n AS sample_count,
       toUInt16(512) AS frame_size, toUInt16(500) AS hop_size, 500.0 AS sample_rate,
       stft_db(vals) AS spec_db, last_ins AS last_inserted_at
FROM (SELECT toDateTime(toStartOfMinute(timestamp), 'UTC') AS window_ts, machine,
             count() AS n, max(inserted_at) AS last_ins,
             arrayMap(t -> t.2, arraySort(groupArray((timestamp, vib_r)))) AS vals
      FROM engine.fact_engine_vibration
      GROUP BY window_ts, machine HAVING n >= 512);
```

## 2. Anomaly view: CTE chain → one-pass stored baseline

**Before**: the method of the original DuckDB view. In ClickHouse, a `WITH`
subquery is re-run at every reference. `h` (the health view, which includes the
live 10-minute calculation over raw readings) was reached through `ranked →
quiet → centre`, `quiet → spread` (twice, via the join with `centre`) and `z`,
so it ran about 5 times per query: 1.2–2.5 s.

```sql
WITH
h AS (SELECT ... FROM engine.v_engine_minute_health WHERE rms_r IS NOT NULL),
ranked AS (SELECT *, row_number() OVER (PARTITION BY machine_name ORDER BY rms_r) AS quiet_rank,
                     count() OVER (PARTITION BY machine_name) AS n_minutes FROM h),
quiet AS (SELECT * FROM ranked WHERE quiet_rank <= greatest(3, toUInt64(ceil(0.15 * n_minutes)))),
centre AS (SELECT machine_name, count() AS baseline_minutes,
                  quantileExactInclusive(0.5)(rms_r) AS c_rms, ... FROM quiet GROUP BY machine_name),
spread AS (SELECT q.machine_name, quantileExactInclusive(0.5)(abs(q.rms_r - c.c_rms)) AS s_rms, ...
           FROM quiet AS q INNER JOIN centre AS c USING (machine_name) GROUP BY q.machine_name),
z AS (SELECT ... FROM h INNER JOIN centre AS c USING (machine_name) INNER JOIN spread AS s USING (machine_name))
SELECT ... FROM z;
```

**After**: the baseline reads stored minutes only, and median and MAD come from
per-machine arrays in one aggregation. The live view is read exactly once.
229 ms of CPU per query.

```sql
WITH
baseline AS (
  SELECT machine_name, count() AS baseline_minutes,
         groupArray(rms_r) AS a_rms, ...,
         arrayReduce('quantileExactInclusive(0.5)', a_rms) AS c_rms, ...,
         arrayReduce('quantileExactInclusive(0.5)', arrayMap(x -> abs(x - c_rms), a_rms)) AS s_rms, ...
  FROM (SELECT ..., row_number() OVER (PARTITION BY machine_name ORDER BY rms_r) AS quiet_rank,
                    count() OVER (PARTITION BY machine_name) AS n_minutes
        FROM engine.engine_minute_health FINAL)
  WHERE quiet_rank <= greatest(3, toUInt64(ceil(0.15 * n_minutes)))
  GROUP BY machine_name),
z AS (SELECT ... FROM engine.v_engine_minute_health AS h
      LEFT JOIN baseline AS b ON b.machine_name = h.machine_name ...)
SELECT ... FROM z;
```

## 3. Dashboard summary: full scan → part metadata

**Before**, every 5 s. It read all 11 M rows in the test (435 ms), and its cost
grew with retention:

```sql
SELECT count() AS readings, uniqExact(machine) AS machines,
       max(timestamp) AS latest, dateDiff('second', max(timestamp), now64(3)) AS lag
FROM fact_engine_vibration
```

**After**: `count()` and `max(timestamp)` are answered from each part's stored
row count and min/max index (8 rows read, 3 ms). The machine count comes from
the dimension helper.

```sql
SELECT count() AS readings, max(timestamp) AS latest,
       dateDiff('second', max(timestamp), now64(3)) AS lag FROM fact_engine_vibration;
SELECT count() AS n FROM machine_hlp FINAL;
```

## 4. Storing jobs: global high-water mark → per-machine arrival summary

**Before**: one high-water mark for all machines. Minutes older than
(oldest machine's newest stored minute − 20 min) were never considered, so
late data for one machine was skipped. The backfill stalled at 126 of 366
minutes.

```sql
WHERE window_ts < toStartOfMinute(now('UTC') - INTERVAL 15 SECOND)
  AND window_ts >= greatest(
        toStartOfMinute(now('UTC')) - INTERVAL 5 HOUR,
        coalesce((SELECT minOrNull(mx) FROM (SELECT max(window_ts) AS mx
                  FROM engine.engine_minute_spectrogram ... GROUP BY machine_name)) - INTERVAL 20 MINUTE,
                 toStartOfMinute(now('UTC')) - INTERVAL 5 HOUR))
  AND last_inserted_at < now64(3, 'UTC') - INTERVAL 10 SECOND
  AND (machine_name, window_ts) NOT IN (SELECT machine_name, window_ts FROM engine.engine_minute_spectrogram ...)
```

**After**: `engine.fact_minutes` counts readings per machine per minute as they
arrive (an insert-time materialized view). A minute is due when it has more
readings than its stored row counted, so missing minutes are stored and minutes
that received late readings are recalculated.

```sql
WITH todo AS (
  SELECT fm.machine AS machine_name, fm.window_ts AS window_ts
  FROM (SELECT machine, window_ts, sum(rows) AS n, max(last_inserted_at) AS last_ins
        FROM engine.fact_minutes WHERE window_ts >= ... GROUP BY machine, window_ts) AS fm
  LEFT JOIN (SELECT machine_name, window_ts, max(sample_count) AS done
             FROM engine.engine_minute_spectrogram ... GROUP BY machine_name, window_ts) AS s
         ON s.machine_name = fm.machine AND s.window_ts = fm.window_ts
  WHERE fm.last_ins < now64(3, 'UTC') - INTERVAL 10 SECOND AND fm.n >= 512 AND fm.n > s.done)
SELECT *, now64(3, 'UTC') AS computed_at FROM engine.calc_minute_spectrogram
WHERE (machine_name, window_ts) IN (SELECT machine_name, window_ts FROM todo)
  AND window_ts >= (SELECT min(window_ts) FROM todo) AND window_ts <= (SELECT max(window_ts) FROM todo);
```

## 5. Producer: silent drops → reported drops

**Before**: the paho return code was ignored. With the local buffer full
(20,000 messages, about 40 s), readings were discarded without any sign.

```python
self.client.max_queued_messages_set(20000)
def emit(self, record):
    self.client.publish(self.topic, json.dumps(record), qos=1)
```

**After**: drops are counted and printed, backlog is shown, and the buffer
holds 5 minutes.

```python
MAX_QUEUED = 150_000
def emit(self, record):
    info = self.client.publish(self.topic, json.dumps(record), qos=1)
    if info.rc == mqtt.MQTT_ERR_QUEUE_SIZE:
        self.dropped += 1
    else:
        self.sent += 1
```
