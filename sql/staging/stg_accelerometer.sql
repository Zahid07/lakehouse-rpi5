-- stg_accelerometer.sql
-- Load all selected raw parquet files into staging table

CREATE OR REPLACE TABLE :staging_schema.stg_accelerometer AS
SELECT
    timestamp::TIMESTAMP                AS timestamp,
    location                            AS location,
    x::DOUBLE                           AS x,
    y::DOUBLE                           AS y,
    z::DOUBLE                           AS z,
    SQRT(x*x + y*y + z*z)              AS magnitude,
    current_timestamp                   AS batch_ts,
    ':batch_id'                         AS batch_id
FROM read_parquet(:raw_files_expr);
