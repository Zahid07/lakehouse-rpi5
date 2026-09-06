-- machine_hlp: assign a surrogate key to each machine seen, once.
--
-- Ported from duckstream_pipeline/sql/curated/location_hlp.sql, with the same
-- correction already applied there: the original assigned keys with a
-- CROSS JOIN on MAX(key) inside the same statement that inserted them, which is
-- a read-modify-write and is only safe under a single driver.
--
-- Written idempotent and monotonic instead: only genuinely new machines are
-- inserted, so re-running inserts nothing. That matters more here than it did
-- for the accelerometer, because `ProcessingTime` releases the catalog between
-- cycles, so the window in which a second writer could exist is real again.

INSERT INTO curated.machine_hlp (
    machine_key, machine_name, site, asset_type, nominal_rpm, bearing_balls,
    ins_tmstmp, upd_tmstmp
)
SELECT
    (SELECT COALESCE(MAX(machine_key), 0) FROM curated.machine_hlp)
        + ROW_NUMBER() OVER (ORDER BY new_machine.machine_name)  AS machine_key,
    new_machine.machine_name,
    -- "<Site>_<Unit>" by convention, so the site is the prefix.
    SPLIT_PART(new_machine.machine_name, '_', 1)                 AS site,
    'engine'                                                     AS asset_type,
    1800                                                         AS nominal_rpm,
    8                                                            AS bearing_balls,
    current_timestamp                                            AS ins_tmstmp,
    current_timestamp                                            AS upd_tmstmp
FROM (
    SELECT DISTINCT f.machine AS machine_name
    FROM curated.fact_engine_vibration f
    WHERE NOT EXISTS (
        SELECT 1 FROM curated.machine_hlp h
        WHERE lower(h.machine_name) = lower(f.machine)
    )
) AS new_machine;
