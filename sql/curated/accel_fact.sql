-- accel_fact.sql
-- Insert new accelerometer readings into fact table

CREATE TABLE IF NOT EXISTS :curated_schema.fact_accelerometer (
    timestamp      TIMESTAMP,
    location_key   INTEGER,
    x              DOUBLE,
    y              DOUBLE,
    z              DOUBLE,
    magnitude      DOUBLE,
    ins_tmstmp     TIMESTAMP,
    batch_id       VARCHAR
);

INSERT INTO :curated_schema.fact_accelerometer (
    timestamp,
    location_key,
    x,
    y,
    z,
    magnitude,
    ins_tmstmp,
    batch_id
)
SELECT
    STG.timestamp,
    LOC.location_key,
    STG.x,
    STG.y,
    STG.z,
    STG.magnitude,
    current_timestamp AS ins_tmstmp,
    ':batch_id'       AS batch_id
FROM :staging_schema.stg_accelerometer STG
LEFT JOIN :curated_schema.location_dim LOC
    ON lower(STG.location) = lower(LOC.location_name)
   AND LOC.is_current = TRUE;
