-- accel_fft_mart.sql
-- FFT mart (MERGE - upsert by window_ts + location_key)
--
-- Scope is the [window_lo, window_hi) range the runner derives from the minute
-- windows this batch actually touched, so untouched history is never read. Every
-- window in range is recomputed from all of its rows: a 30s dump never fills a
-- 1-minute window, so transforming only the batch's own rows produced a spectrum
-- over a partial window.
--
-- LIST() materializes every row in range into arrays (x4 axes) and marshals them
-- into Python for numpy, so peak memory here is O(rows), not O(groups). This is
-- the step that OOM'd at 19.4M rows, so the runner feeds it a bounded number of
-- windows per call via fft_windows_per_chunk.

CREATE TABLE IF NOT EXISTS :marts_schema.accel_fft_mart (
    window_ts       TIMESTAMP,
    location_key    INTEGER,
    sample_count    INTEGER,
    freq_hz         DOUBLE[],
    fft_x           DOUBLE[],
    fft_y           DOUBLE[],
    fft_z           DOUBLE[],
    fft_magnitude   DOUBLE[],
    ins_tmstmp      TIMESTAMP,
    upd_tmstmp      TIMESTAMP,
    batch_id        VARCHAR
);

MERGE INTO :marts_schema.accel_fft_mart AS target
USING (
    SELECT
        DATE_TRUNC('minute', timestamp)                             AS window_ts,
        location_key,
        COUNT(*)                                                    AS sample_count,
        fft_freqs(
            LIST(magnitude ORDER BY timestamp),
            CAST(:sample_rate_hz AS DOUBLE)
        )                                                           AS freq_hz,
        fft_magnitude(LIST(x ORDER BY timestamp))                   AS fft_x,
        fft_magnitude(LIST(y ORDER BY timestamp))                   AS fft_y,
        fft_magnitude(LIST(z ORDER BY timestamp))                   AS fft_z,
        fft_magnitude(LIST(magnitude ORDER BY timestamp))           AS fft_magnitude,
        current_timestamp                                           AS ins_tmstmp,
        current_timestamp                                           AS upd_tmstmp,
        ':batch_id'                                                 AS batch_id
    FROM :curated_schema.fact_accelerometer
    -- Literal bounds so DuckLake can prune files/row groups on timestamp stats.
    WHERE timestamp >= TIMESTAMP ':window_lo'
      AND timestamp <  TIMESTAMP ':window_hi'
    GROUP BY 1, 2
) AS source
ON target.window_ts = source.window_ts
-- Null-safe: location_key is NULL when a reading has no matching current dim row.
AND target.location_key IS NOT DISTINCT FROM source.location_key
WHEN MATCHED THEN
    UPDATE SET
        sample_count  = source.sample_count,
        freq_hz       = source.freq_hz,
        fft_x         = source.fft_x,
        fft_y         = source.fft_y,
        fft_z         = source.fft_z,
        fft_magnitude = source.fft_magnitude,
        upd_tmstmp    = current_timestamp,
        batch_id      = source.batch_id
WHEN NOT MATCHED THEN
    INSERT (
        window_ts, location_key, sample_count,
        freq_hz, fft_x, fft_y, fft_z, fft_magnitude,
        ins_tmstmp, upd_tmstmp, batch_id
    )
    VALUES (
        source.window_ts, source.location_key, source.sample_count,
        source.freq_hz, source.fft_x, source.fft_y, source.fft_z, source.fft_magnitude,
        source.ins_tmstmp, source.upd_tmstmp, source.batch_id
    );
