-- accel_location_hlp.sql
-- Insert new locations not yet in helper table

CREATE TABLE IF NOT EXISTS :curated_schema.location_hlp (
    location_key  INTEGER,
    location_name VARCHAR,
    city          VARCHAR,
    country       VARCHAR,
    ins_tmstmp    TIMESTAMP,
    upd_tmstmp    TIMESTAMP,
    batch_id      VARCHAR
);

INSERT INTO :curated_schema.location_hlp (
    location_key,
    location_name,
    city,
    country,
    ins_tmstmp,
    upd_tmstmp,
    batch_id
)
SELECT
    max_key + SUM(1) OVER (ROWS UNBOUNDED PRECEDING) AS location_key,
    STG.location                                      AS location_name,
    SPLIT_PART(STG.location, '_', 1)                  AS city,
    'Pakistan'                                        AS country,
    current_timestamp                                 AS ins_tmstmp,
    current_timestamp                                 AS upd_tmstmp,
    ':batch_id'                                       AS batch_id
FROM (
    SELECT DISTINCT location FROM :staging_schema.stg_accelerometer
) STG
LEFT JOIN :curated_schema.location_hlp HLP
    ON lower(STG.location) = lower(HLP.location_name)
CROSS JOIN (
    SELECT COALESCE(MAX(location_key), 0) AS max_key
    FROM :curated_schema.location_hlp
) key_max
WHERE HLP.location_key IS NULL;
