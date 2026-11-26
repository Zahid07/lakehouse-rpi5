-- Minute health: stored for settled minutes, calculated live for the newest ~10.
--
--   engine.fact_engine_vibration        raw readings
--        |
--   engine.calc_minute_health           THE calculation (a view over raw)
--        |                     \
--   engine.minute_health_mv     \       every minute: store minutes older than
--        |                       \      10 minutes that are not stored yet
--   engine.engine_minute_health   \     calculated values, kept forever
--        |                         |
--   engine.v_engine_minute_health  +--  stored rows + live rows for any minute
--                                       not stored yet (normally the last ~10)
--
-- One calculation feeds both halves, so a stored minute and a live one can
-- never disagree about what "RMS" means. The `source` column says which half
-- a row came from.

-- 1. The calculation. Any filter on minute_ts is pushed down to the raw scan,
--    so querying ten minutes of it reads ten minutes of readings.
CREATE OR REPLACE VIEW engine.calc_minute_health AS
SELECT
    window_ts                                              AS minute_ts,
    machine                                                AS machine_name,
    n                                                      AS sample_count,
    round(n / 60.0, 1)                                     AS effective_hz,
    round(sqrt(m2_r), 5)                                   AS rms_r,
    round(peak_raw, 5)                                     AS peak_r,
    round(peak_raw / nullIf(sqrt(m2_r), 0), 3)             AS crest_r,
    -- Population kurtosis and skew (not excess), as the original computed
    -- from raw moments -- here from ClickHouse's own numerically stable forms.
    round(kurt_raw, 3)                                     AS kurtosis_r,
    round(skew_raw, 3)                                     AS skew_r,
    round(sqrt(m2_a), 5)                                   AS rms_a,
    -- The misalignment discriminator.
    round(sqrt(m2_a) / nullIf(sqrt(m2_r), 0), 3)           AS axial_ratio,
    round(avg_rpm_raw, 1)                                  AS avg_rpm,
    round(min_rpm_raw, 1)                                  AS min_rpm,
    round(max_rpm_raw, 1)                                  AS max_rpm,
    round(sd_rpm_raw, 2)                                   AS sd_rpm,
    round(avg_rpm_raw / 60.0, 3)                           AS shaft_hz,
    mode_min,
    mode_max,
    mode_min != mode_max                                   AS mode_changed,
    -- Missing readings, exactly. Zero when every seq in the range arrived.
    toInt64(seq_hi) - toInt64(seq_lo) + 1 - toInt64(n)     AS seq_deficit,
    last_inserted_at
FROM
(
    SELECT
        toDateTime(toStartOfMinute(timestamp), 'UTC')  AS window_ts,
        machine,
        count()                    AS n,
        min(seq)                   AS seq_lo,
        max(seq)                   AS seq_hi,
        avg(vib_r * vib_r)         AS m2_r,
        max(abs(vib_r))            AS peak_raw,
        kurtPop(vib_r)             AS kurt_raw,
        skewPop(vib_r)             AS skew_raw,
        avg(vib_a * vib_a)         AS m2_a,
        avg(rpm)                   AS avg_rpm_raw,
        min(rpm)                   AS min_rpm_raw,
        max(rpm)                   AS max_rpm_raw,
        stddevSamp(rpm)            AS sd_rpm_raw,
        min(toString(mode))        AS mode_min,
        max(toString(mode))        AS mode_max,
        max(inserted_at)           AS last_inserted_at
    FROM engine.fact_engine_vibration
    GROUP BY window_ts, machine
);

-- 2. The calculated values. ReplacingMergeTree so a minute that is ever
--    written twice keeps one row (read it with FINAL).
CREATE TABLE IF NOT EXISTS engine.engine_minute_health
(
    minute_ts         DateTime('UTC'),
    machine_name      LowCardinality(String),
    sample_count      UInt64,
    effective_hz      Float64,
    rms_r             Float64,
    peak_r            Float64,
    crest_r           Nullable(Float64),
    kurtosis_r        Float64,
    skew_r            Float64,
    rms_a             Float64,
    axial_ratio       Nullable(Float64),
    avg_rpm           Float64,
    min_rpm           Float64,
    max_rpm           Float64,
    sd_rpm            Float64,
    shaft_hz          Float64,
    mode_min          LowCardinality(String),
    mode_max          LowCardinality(String),
    mode_changed      Bool,
    seq_deficit       Int64,
    last_inserted_at  DateTime64(3, 'UTC'),
    computed_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (machine_name, minute_ts);

-- 3. Store each minute once it is older than 10 minutes AND has received no
--    reading for 2 minutes -- and store it again if more readings arrive for
--    it later (a backlog replayed after a restart). The to-do list comes from
--    the arrival summary engine.fact_minutes, per machine and per minute:
--    a minute is due when it has more readings than its stored row counted.
--    Only those minutes are then read from the raw table.
DROP VIEW IF EXISTS engine.minute_health_mv;
CREATE MATERIALIZED VIEW engine.minute_health_mv
REFRESH EVERY 1 MINUTE APPEND TO engine.engine_minute_health AS
WITH todo AS
(
    SELECT fm.machine AS machine_name, fm.window_ts AS minute_ts
    FROM
    (
        SELECT machine, window_ts, sum(rows) AS n, max(last_inserted_at) AS last_ins
        FROM engine.fact_minutes
        WHERE window_ts >= toStartOfMinute(now('UTC')) - INTERVAL 5 HOUR
          AND window_ts <  toStartOfMinute(now('UTC')) - INTERVAL 10 MINUTE
        GROUP BY machine, window_ts
    ) AS fm
    LEFT JOIN
    (
        SELECT machine_name, minute_ts, max(sample_count) AS done
        FROM engine.engine_minute_health
        WHERE minute_ts >= toStartOfMinute(now('UTC')) - INTERVAL 5 HOUR
        GROUP BY machine_name, minute_ts
    ) AS s ON s.machine_name = fm.machine AND s.minute_ts = fm.window_ts
    WHERE fm.last_ins < now64(3, 'UTC') - INTERVAL 2 MINUTE
      AND fm.n > s.done                      -- unmatched: done = 0
)
SELECT *, now64(3, 'UTC') AS computed_at
FROM engine.calc_minute_health
WHERE (machine_name, minute_ts) IN (SELECT machine_name, minute_ts FROM todo)
  -- the range lets the raw scan skip everything outside the to-do minutes
  AND minute_ts >= (SELECT min(minute_ts) FROM todo)
  AND minute_ts <= (SELECT max(minute_ts) FROM todo);

-- 4. What you query: stored minutes, plus a live calculation for any minute
--    not stored yet (normally the newest ~10), with the dimension resolved at
--    read time -- so re-rating a machine reaches every historical minute.
CREATE OR REPLACE VIEW engine.v_engine_minute_health AS
SELECT
    h.minute_ts        AS minute_ts,
    d.machine_key      AS machine_key,
    h.machine_name     AS machine_name,
    d.site             AS site,
    d.asset_type       AS asset_type,
    d.nominal_rpm      AS nominal_rpm,
    h.sample_count, h.effective_hz, h.rms_r, h.peak_r, h.crest_r,
    h.kurtosis_r, h.skew_r, h.rms_a, h.axial_ratio,
    h.avg_rpm, h.min_rpm, h.max_rpm, h.sd_rpm, h.shaft_hz,
    h.mode_min, h.mode_max, h.mode_changed, h.seq_deficit,
    h.source           AS source
FROM
(
    SELECT minute_ts, toString(machine_name) AS machine_name, sample_count, effective_hz,
           rms_r, peak_r, crest_r, kurtosis_r, skew_r, rms_a, axial_ratio,
           avg_rpm, min_rpm, max_rpm, sd_rpm, shaft_hz,
           toString(mode_min) AS mode_min, toString(mode_max) AS mode_max,
           mode_changed, seq_deficit, 'stored' AS source
    FROM engine.engine_minute_health FINAL

    UNION ALL

    SELECT minute_ts, toString(machine_name) AS machine_name, sample_count, effective_hz,
           rms_r, peak_r, crest_r, kurtosis_r, skew_r, rms_a, axial_ratio,
           avg_rpm, min_rpm, max_rpm, sd_rpm, shaft_hz,
           mode_min, mode_max, mode_changed, seq_deficit, 'live' AS source
    FROM engine.calc_minute_health
    WHERE minute_ts >= toStartOfMinute(now('UTC')) - INTERVAL 15 MINUTE
      AND (machine_name, minute_ts) NOT IN
          (SELECT machine_name, minute_ts FROM engine.engine_minute_health
           WHERE minute_ts >= toStartOfMinute(now('UTC')) - INTERVAL 15 MINUTE)
) AS h
LEFT JOIN
(
    SELECT machine_key, machine_name, site, asset_type, nominal_rpm
    FROM engine.machine_dim WHERE is_current
) AS d ON lower(d.machine_name) = lower(h.machine_name)
SETTINGS join_use_nulls = 1;
