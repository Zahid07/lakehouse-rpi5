-- location_dim: SCD Type 2 over the helper table.
--
-- Ported from sql/curated/accel_location_dim.sql, unchanged in behaviour. This
-- is the half of the pipeline duckstream does **not** own and should not: it
-- has no notion of a surrogate key, a validity interval or a slowly-changing
-- attribute, and giving it one would be scope it was explicitly designed
-- without (PLAN.md, "Non-goals" -- not IVM for arbitrary SQL).
--
-- Two statements, run in order inside one transaction by `dimensions.py`:
-- expire what changed, then insert the new version. Splitting them across
-- transactions would leave a window in which a location has *no* current row,
-- and every read-time join in sql/marts/views.sql would drop those rows.

-- 1. Expire the current row wherever a tracked attribute changed.
UPDATE curated.location_dim AS tgt
SET is_current = FALSE,
    valid_to   = current_timestamp,
    upd_tmstmp = current_timestamp,
    oper       = 'U'
FROM curated.location_hlp AS src
WHERE tgt.location_key = src.location_key
  AND tgt.is_current = TRUE
  AND (tgt.city <> src.city OR tgt.country <> src.country);

-- 2. Insert a current row for anything that has none -- which is both a brand
--    new location and one whose previous version step 1 just expired.
INSERT INTO curated.location_dim (
    location_key, location_name, city, country,
    is_current, valid_from, valid_to, ins_tmstmp, upd_tmstmp, oper
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
    'I'               AS oper
FROM curated.location_hlp AS src
WHERE NOT EXISTS (
    SELECT 1 FROM curated.location_dim d
    WHERE d.location_key = src.location_key AND d.is_current = TRUE
);
