-- accel_location_dim.sql
-- SCD Type 2 MERGE into location_dim

CREATE TABLE IF NOT EXISTS :curated_schema.location_dim (
    location_key   INTEGER,
    location_name  VARCHAR,
    city           VARCHAR,
    country        VARCHAR,
    is_current     BOOLEAN,
    valid_from     TIMESTAMP,
    valid_to       TIMESTAMP,
    ins_tmstmp     TIMESTAMP,
    upd_tmstmp     TIMESTAMP,
    oper           VARCHAR,
    batch_id       VARCHAR
);

-- Expire old records where attributes changed
UPDATE :curated_schema.location_dim AS tgt
SET
    is_current = FALSE,
    valid_to   = current_timestamp,
    upd_tmstmp = current_timestamp,
    oper       = 'U'
FROM :curated_schema.location_hlp AS src
WHERE tgt.location_key = src.location_key
  AND tgt.is_current = TRUE
  AND (
      tgt.city    <> src.city OR
      tgt.country <> src.country
  );

-- Insert new or changed records
INSERT INTO :curated_schema.location_dim (
    location_key,
    location_name,
    city,
    country,
    is_current,
    valid_from,
    valid_to,
    ins_tmstmp,
    upd_tmstmp,
    oper,
    batch_id
)
SELECT
    src.location_key,
    src.location_name,
    src.city,
    src.country,
    TRUE              AS is_current,
    current_timestamp AS valid_from,
    NULL              AS valid_to,
    current_timestamp AS ins_tmstmp,
    current_timestamp AS upd_tmstmp,
    'I'               AS oper,
    ':batch_id'       AS batch_id
FROM :curated_schema.location_hlp AS src
LEFT JOIN :curated_schema.location_dim AS tgt
    ON src.location_key = tgt.location_key
   AND tgt.is_current = TRUE
WHERE tgt.location_key IS NULL;
