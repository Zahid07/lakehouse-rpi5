-- location_hlp: assign a surrogate key to each location seen, once.
--
-- Ported from sql/curated/accel_location_hlp.sql with one deliberate change.
--
-- The original assigned keys with:
--
--     max_key + SUM(1) OVER (ROWS UNBOUNDED PRECEDING)
--     CROSS JOIN (SELECT COALESCE(MAX(location_key), 0) AS max_key FROM location_hlp)
--
-- which reads the maximum and assigns from it in the same statement. That is
-- safe under a single driver processing one batch at a time, and it is not safe
-- once anything can overlap. `AvailableNow` drains until empty, so a backlog can
-- make one tick outlast the interval that started it -- and CONTEXT.md 2.5 was
-- corrected on exactly this point in phase 2b: what prevents the overlap is not
-- the trigger, it is DuckDB's catalog file lock (constraint 25 -- a second
-- process cannot even ATTACH, READ_ONLY or not).
--
-- Under `ProcessingTime` the catalog is *released* between cycles, which is the
-- whole point of the daemon -- so the window in which two writers could exist is
-- real again, briefly. This runs inside the engine's own attached session,
-- between the drain and the detach, so it inherits the same lock. Keeping the
-- read-modify-write in one statement is still the fragile part, so it is written
-- to be **idempotent and monotonic** instead: only genuinely new locations are
-- inserted, and re-running it inserts nothing.

INSERT INTO curated.location_hlp (
    location_key, location_name, city, country, ins_tmstmp, upd_tmstmp
)
SELECT
    (SELECT COALESCE(MAX(location_key), 0) FROM curated.location_hlp)
        + ROW_NUMBER() OVER (ORDER BY new_location.location_name)  AS location_key,
    new_location.location_name,
    -- The naming convention is "<City>_<Sector>", so the city is the prefix.
    -- Unchanged from the original, including its assumption about Pakistan.
    SPLIT_PART(new_location.location_name, '_', 1)                 AS city,
    'Pakistan'                                                     AS country,
    current_timestamp                                              AS ins_tmstmp,
    current_timestamp                                              AS upd_tmstmp
FROM (
    SELECT DISTINCT f.location AS location_name
    FROM curated.fact_accelerometer f
    WHERE NOT EXISTS (
        SELECT 1 FROM curated.location_hlp h
        WHERE lower(h.location_name) = lower(f.location)
    )
) AS new_location;
