-- accel_fft_mart.sql
-- FFT mart (MERGE - upsert by window_ts + location_key)

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
        DATE_TRUNC('minute', timestamp)                          AS window_ts,
        location_key,
        COUNT(*)                                                 AS sample_count,
        fft_freqs(LIST(magnitude ORDER BY timestamp), 100.0)    AS freq_hz,
        fft_magnitude(LIST(x ORDER BY timestamp))               AS fft_x,
        fft_magnitude(LIST(y ORDER BY timestamp))               AS fft_y,
        fft_magnitude(LIST(z ORDER BY timestamp))               AS fft_z,
        fft_magnitude(LIST(magnitude ORDER BY timestamp))        AS fft_magnitude,
        current_timestamp                                        AS ins_tmstmp,
        current_timestamp                                        AS upd_tmstmp,
        ':batch_id'                                              AS batch_id
    FROM :curated_schema.fact_accelerometer
    WHERE batch_id = ':batch_id'
    GROUP BY 1, 2
) AS source
ON target.window_ts = source.window_ts
AND target.location_key = source.location_key
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
