-- accel_summary_mart.sql
-- Hourly aggregated summary mart (MERGE - upsert by hour_ts + location_key)

CREATE TABLE IF NOT EXISTS :marts_schema.accel_hourly_summary (
    hour_ts          TIMESTAMP,
    location_key     INTEGER,
    location_name    VARCHAR,
    city             VARCHAR,
    country          VARCHAR,
    sample_count     BIGINT,
    avg_x            DOUBLE,
    avg_y            DOUBLE,
    avg_z            DOUBLE,
    avg_magnitude    DOUBLE,
    max_magnitude    DOUBLE,
    min_magnitude    DOUBLE,
    stddev_magnitude DOUBLE,
    ins_tmstmp       TIMESTAMP,
    upd_tmstmp       TIMESTAMP,
    batch_id         VARCHAR
);

MERGE INTO :marts_schema.accel_hourly_summary AS target
USING (
    SELECT
        DATE_TRUNC('hour', f.timestamp)   AS hour_ts,
        f.location_key,
        l.location_name,
        l.city,
        l.country,
        COUNT(*)                          AS sample_count,
        ROUND(AVG(f.x), 4)               AS avg_x,
        ROUND(AVG(f.y), 4)               AS avg_y,
        ROUND(AVG(f.z), 4)               AS avg_z,
        ROUND(AVG(f.magnitude), 4)       AS avg_magnitude,
        ROUND(MAX(f.magnitude), 4)       AS max_magnitude,
        ROUND(MIN(f.magnitude), 4)       AS min_magnitude,
        ROUND(STDDEV(f.magnitude), 4)    AS stddev_magnitude,
        current_timestamp                AS ins_tmstmp,
        current_timestamp                AS upd_tmstmp,
        ':batch_id'                      AS batch_id
    FROM :curated_schema.fact_accelerometer f
    LEFT JOIN :curated_schema.location_dim l
        ON f.location_key = l.location_key
       AND l.is_current = TRUE
    WHERE f.batch_id = ':batch_id'
    GROUP BY 1, 2, 3, 4, 5
) AS source
ON target.hour_ts = source.hour_ts
AND target.location_key = source.location_key
WHEN MATCHED THEN
    UPDATE SET
        sample_count     = target.sample_count + source.sample_count,
        avg_x            = ROUND((target.avg_x + source.avg_x) / 2, 4),
        avg_y            = ROUND((target.avg_y + source.avg_y) / 2, 4),
        avg_z            = ROUND((target.avg_z + source.avg_z) / 2, 4),
        avg_magnitude    = ROUND((target.avg_magnitude + source.avg_magnitude) / 2, 4),
        max_magnitude    = GREATEST(target.max_magnitude, source.max_magnitude),
        min_magnitude    = LEAST(target.min_magnitude, source.min_magnitude),
        stddev_magnitude = source.stddev_magnitude,
        upd_tmstmp       = current_timestamp,
        batch_id         = source.batch_id
WHEN NOT MATCHED THEN
    INSERT (
        hour_ts, location_key, location_name, city, country,
        sample_count, avg_x, avg_y, avg_z, avg_magnitude,
        max_magnitude, min_magnitude, stddev_magnitude,
        ins_tmstmp, upd_tmstmp, batch_id
    )
    VALUES (
        source.hour_ts, source.location_key, source.location_name, source.city, source.country,
        source.sample_count, source.avg_x, source.avg_y, source.avg_z, source.avg_magnitude,
        source.max_magnitude, source.min_magnitude, source.stddev_magnitude,
        source.ins_tmstmp, source.upd_tmstmp, source.batch_id
    );
