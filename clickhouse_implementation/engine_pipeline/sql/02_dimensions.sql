-- The star schema's dimension: machine_hlp (surrogate keys) and machine_dim (SCD2).
--
-- The original ran two SQL scripts from Python every cycle, inside one
-- transaction, with UPDATEs to expire rows. ClickHouse has no multi-statement
-- transactions, so SCD2 is built the way ClickHouse prefers: an append-only
-- history of versions, with valid_to / is_current derived at read time. A
-- machine can therefore never be left without a current row -- the failure the
-- original needed a transaction to prevent cannot happen.
--
-- Both maintenance jobs are refreshable materialized views: ClickHouse runs
-- them every 30 seconds, one at a time, so key assignment is single-writer.

-- 1. Helper: one row per machine, with its surrogate key and attributes.
--    This is the table you edit. Re-rate a machine with:
--
--      ALTER TABLE engine.machine_hlp
--      UPDATE nominal_rpm = 1500, upd_tmstmp = now()
--      WHERE machine_name = 'Karachi_ENG01';
--
--    and within 30 s machine_dim gains a new current version and the old one
--    gets a valid_to. Always read it with FINAL.
CREATE TABLE IF NOT EXISTS engine.machine_hlp
(
    machine_key    UInt32,
    machine_name   String,
    site           LowCardinality(String),
    asset_type     LowCardinality(String),
    nominal_rpm    UInt32,
    bearing_balls  UInt16,
    ins_tmstmp     DateTime('UTC'),
    upd_tmstmp     DateTime('UTC')
)
-- No version column: ClickHouse refuses to UPDATE a version column, and the
-- ALTER above must be able to set upd_tmstmp. Without one, the most recently
-- written row wins, which is the same thing here.
ENGINE = ReplacingMergeTree
ORDER BY machine_name;

-- Assign keys to machines seen in the data but not yet in the helper.
-- Monotonic and idempotent, as machine_hlp.sql was: only new machines insert.
-- Reads the arrival summary's last 10 minutes: a machine is registered within
-- 30 s of its first reading, so older minutes can hold no new machine.
--
-- Refreshable views hold no data (they only write into their target table),
-- so setup drops and recreates them: a changed definition always takes effect.
DROP VIEW IF EXISTS engine.machine_hlp_mv;
CREATE MATERIALIZED VIEW engine.machine_hlp_mv
REFRESH EVERY 30 SECOND APPEND TO engine.machine_hlp AS
SELECT
    toUInt32((SELECT coalesce(max(machine_key), 0) FROM engine.machine_hlp)
             + row_number() OVER (ORDER BY machine_name))   AS machine_key,
    machine_name,
    -- "<Site>_<Unit>" by convention, so the site is the prefix.
    splitByChar('_', machine_name)[1]                       AS site,
    'engine'                                                AS asset_type,
    toUInt32(1800)                                          AS nominal_rpm,
    toUInt16(8)                                             AS bearing_balls,
    now('UTC')                                              AS ins_tmstmp,
    now('UTC')                                              AS upd_tmstmp
FROM
(
    SELECT DISTINCT toString(machine) AS machine_name
    FROM engine.fact_minutes
    WHERE window_ts >= now('UTC') - INTERVAL 10 MINUTE
      AND lower(toString(machine)) NOT IN (SELECT lower(machine_name) FROM engine.machine_hlp)
);

-- 2. SCD2 history: one row per version of a machine's tracked attributes.
CREATE TABLE IF NOT EXISTS engine.machine_dim_history
(
    machine_key    UInt32,
    machine_name   String,
    site           LowCardinality(String),
    asset_type     LowCardinality(String),
    nominal_rpm    UInt32,
    bearing_balls  UInt16,
    valid_from     DateTime('UTC'),
    ins_tmstmp     DateTime('UTC')
)
ENGINE = MergeTree
ORDER BY (machine_key, valid_from);

-- Append a version for every helper row whose attributes differ from its
-- latest version, or that has no version yet -- the two cases the original's
-- expire-then-insert pair handled.
DROP VIEW IF EXISTS engine.machine_dim_mv;
CREATE MATERIALIZED VIEW engine.machine_dim_mv
REFRESH EVERY 30 SECOND APPEND TO engine.machine_dim_history AS
SELECT
    machine_key, machine_name, site, asset_type, nominal_rpm, bearing_balls,
    now('UTC') AS valid_from,
    now('UTC') AS ins_tmstmp
FROM engine.machine_hlp FINAL
WHERE (machine_key, site, asset_type, nominal_rpm, bearing_balls) NOT IN
(
    SELECT machine_key,
           argMax(site, valid_from),
           argMax(asset_type, valid_from),
           argMax(nominal_rpm, valid_from),
           argMax(bearing_balls, valid_from)
    FROM engine.machine_dim_history
    GROUP BY machine_key
);

-- 3. The dimension, in the original's shape: every version, with validity.
--    oper is 'I' for the current version and 'U' for one that was superseded,
--    matching what the original's UPDATE/INSERT pair left behind.
CREATE OR REPLACE VIEW engine.machine_dim AS
SELECT
    machine_key, machine_name, site, asset_type, nominal_rpm, bearing_balls,
    valid_to IS NULL                   AS is_current,
    valid_from,
    valid_to,
    ins_tmstmp,
    coalesce(valid_to, ins_tmstmp)     AS upd_tmstmp,
    if(valid_to IS NULL, 'I', 'U')     AS oper
FROM
(
    SELECT *,
           leadInFrame(toNullable(valid_from)) OVER (
               PARTITION BY machine_key ORDER BY valid_from
               ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS valid_to
    FROM engine.machine_dim_history
);
