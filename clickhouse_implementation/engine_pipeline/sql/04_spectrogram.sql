-- Minute spectrogram: stored once each minute closes, live only for the minute
-- still filling.
--
-- The STFT is the executable UDF stft_db (udf/stft_db.py, installed by
-- udf/install_udf.sh): the numpy code of engine_pipeline/spectro.py, run by
-- ClickHouse as a persistent process. ~4 ms per machine-minute, identical to
-- the original cell for cell:
--
--     fs 500 Hz, frame 512 (Hann), hop 500 -> 59 frames x 257 bins per minute
--     per-frame mean removed; |X| * 2/sum(w); bins 0 and Nyquist not doubled
--     20*log10, floored at -120 dB re 1 g, rounded to 0.1 dB
--     layout: spec_db[f*257 + b + 1]  (1-based), bin b is b*fs/frame Hz
--
-- (An earlier version computed a direct DFT in pure SQL: same result, but
-- ~800 ms per machine-minute, which made the live minute expensive.)

-- 1. The calculation, over raw readings. A filter on window_ts is pushed down
--    to the raw scan. A minute with fewer than one frame of samples yields no
--    row: a partial minute that short is no answer, not a zero answer.
CREATE OR REPLACE VIEW engine.calc_minute_spectrogram AS
SELECT
    window_ts,
    machine                  AS machine_name,
    n                        AS sample_count,
    toUInt16(512)            AS frame_size,
    toUInt16(500)            AS hop_size,
    500.0                    AS sample_rate,
    stft_db(vals)            AS spec_db,
    last_ins                 AS last_inserted_at
FROM
(
    SELECT toDateTime(toStartOfMinute(timestamp), 'UTC')                 AS window_ts,
           machine,
           count()                                                       AS n,
           max(inserted_at)                                              AS last_ins,
           arrayMap(t -> t.2, arraySort(groupArray((timestamp, vib_r)))) AS vals
    FROM engine.fact_engine_vibration
    GROUP BY window_ts, machine
    HAVING n >= 512
);

-- 2. The calculated values. frame_size / hop_size / sample_rate are stored on
--    every row so each is self-describing: changing the parameters later can
--    never reshape history into plausible-looking noise.
CREATE TABLE IF NOT EXISTS engine.engine_minute_spectrogram
(
    window_ts         DateTime('UTC'),
    machine_name      LowCardinality(String),
    sample_count      UInt64,
    frame_size        UInt16,
    hop_size          UInt16,
    sample_rate       Float64,
    spec_db           Array(Float32),
    last_inserted_at  DateTime64(3, 'UTC'),
    computed_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (machine_name, window_ts);

-- 3. Store each minute once it has closed (plus 15 s of grace for readings
--    still in flight) and has received no reading for 10 s -- and again if
--    more readings arrive for it later. Planned from engine.fact_minutes,
--    per machine and per minute, exactly as minute_health_mv is.
DROP VIEW IF EXISTS engine.spectrogram_mv;
CREATE MATERIALIZED VIEW engine.spectrogram_mv
REFRESH EVERY 20 SECOND APPEND TO engine.engine_minute_spectrogram AS
WITH todo AS
(
    SELECT fm.machine AS machine_name, fm.window_ts AS window_ts
    FROM
    (
        SELECT machine, window_ts, sum(rows) AS n, max(last_inserted_at) AS last_ins
        FROM engine.fact_minutes
        WHERE window_ts >= toStartOfMinute(now('UTC')) - INTERVAL 5 HOUR
          AND window_ts <  toStartOfMinute(now('UTC') - INTERVAL 15 SECOND)
        GROUP BY machine, window_ts
    ) AS fm
    LEFT JOIN
    (
        SELECT machine_name, window_ts, max(sample_count) AS done
        FROM engine.engine_minute_spectrogram
        WHERE window_ts >= toStartOfMinute(now('UTC')) - INTERVAL 5 HOUR
        GROUP BY machine_name, window_ts
    ) AS s ON s.machine_name = fm.machine AND s.window_ts = fm.window_ts
    WHERE fm.last_ins < now64(3, 'UTC') - INTERVAL 10 SECOND
      AND fm.n >= 512                        -- shorter than one frame: no answer
      AND fm.n > s.done                      -- unmatched: done = 0
)
SELECT *, now64(3, 'UTC') AS computed_at
FROM engine.calc_minute_spectrogram
WHERE (machine_name, window_ts) IN (SELECT machine_name, window_ts FROM todo)
  AND window_ts >= (SELECT min(window_ts) FROM todo)
  AND window_ts <= (SELECT max(window_ts) FROM todo);

-- 4. What you query: stored minutes plus a live STFT of any minute in the
--    last 3 not stored yet -- normally just the one still filling, a few ms
--    per machine.
--    n_bins / n_frames come from each row's own frame_size.
CREATE OR REPLACE VIEW engine.v_engine_minute_spectrogram AS
SELECT
    s.window_ts                                          AS window_ts,
    d.machine_key                                        AS machine_key,
    s.machine_name                                       AS machine_name,
    s.sample_count                                       AS sample_count,
    toInt32(s.frame_size)                                AS frame_size,
    toInt32(s.hop_size)                                  AS hop_size,
    s.sample_rate                                        AS sample_rate_hz,
    intDiv(toInt32(s.frame_size), 2) + 1                 AS n_bins,
    intDiv(length(s.spec_db), intDiv(toInt32(s.frame_size), 2) + 1) AS n_frames,
    s.sample_rate / s.frame_size                         AS df_hz,
    s.hop_size / s.sample_rate                           AS hop_seconds,
    s.frame_size / s.sample_rate                         AS frame_seconds,
    s.spec_db                                            AS spec_db,
    s.source                                             AS source
FROM
(
    SELECT window_ts, toString(machine_name) AS machine_name, sample_count,
           frame_size, hop_size, sample_rate, spec_db, 'stored' AS source
    FROM engine.engine_minute_spectrogram FINAL

    UNION ALL

    SELECT window_ts, toString(machine_name) AS machine_name, sample_count,
           frame_size, hop_size, sample_rate, spec_db, 'live' AS source
    FROM engine.calc_minute_spectrogram
    WHERE window_ts >= toStartOfMinute(now('UTC')) - INTERVAL 3 MINUTE
      AND (machine_name, window_ts) NOT IN
          (SELECT machine_name, window_ts FROM engine.engine_minute_spectrogram
           WHERE window_ts >= toStartOfMinute(now('UTC')) - INTERVAL 3 MINUTE)
) AS s
LEFT JOIN
(
    SELECT machine_key, machine_name FROM engine.machine_dim WHERE is_current
) AS d ON lower(d.machine_name) = lower(s.machine_name)
SETTINGS join_use_nulls = 1;
