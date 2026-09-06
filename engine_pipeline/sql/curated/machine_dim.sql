-- machine_dim: SCD Type 2 over the helper table.
--
-- Two statements, run in order inside ONE transaction by dimensions.maintain().
-- Splitting them would leave a window in which a machine has no current row,
-- and every read-time join in views.sql is `is_current = TRUE` -- so those rows
-- would silently vanish from the marts until the next successful run.
--
-- SCD2 earns its keep here in a way the accelerometer's location_dim does not.
-- `nominal_rpm` is both a tracked attribute AND a real input: the order-tracking
-- markers on the spectrogram are placed from shaft frequency. Re-rating a
-- machine from 1800 to 1500 RPM is a genuine dimension change with a genuine
-- downstream effect, and because the view resolves the dimension at READ time
-- the change appears on every historical minute the instant it is made. Try it:
--
--   UPDATE curated.machine_hlp SET nominal_rpm = 1500
--    WHERE machine_name = 'Karachi_ENG01';
--
-- then watch valid_to appear on the old row and the order markers move.

-- 1. Expire the current row wherever a tracked attribute changed.
UPDATE curated.machine_dim AS tgt
SET is_current = FALSE,
    valid_to   = current_timestamp,
    upd_tmstmp = current_timestamp,
    oper       = 'U'
FROM curated.machine_hlp AS src
WHERE tgt.machine_key = src.machine_key
  AND tgt.is_current = TRUE
  AND (tgt.site <> src.site
       OR tgt.asset_type <> src.asset_type
       OR tgt.nominal_rpm <> src.nominal_rpm
       OR tgt.bearing_balls <> src.bearing_balls);

-- 2. Insert a current row for anything that has none -- both a brand new
--    machine and one whose previous version step 1 just expired.
INSERT INTO curated.machine_dim (
    machine_key, machine_name, site, asset_type, nominal_rpm, bearing_balls,
    is_current, valid_from, valid_to, ins_tmstmp, upd_tmstmp, oper
)
SELECT
    src.machine_key, src.machine_name, src.site, src.asset_type,
    src.nominal_rpm, src.bearing_balls,
    TRUE              AS is_current,
    current_timestamp AS valid_from,
    NULL              AS valid_to,
    current_timestamp AS ins_tmstmp,
    current_timestamp AS upd_tmstmp,
    'I'               AS oper
FROM curated.machine_hlp AS src
WHERE NOT EXISTS (
    SELECT 1 FROM curated.machine_dim d
    WHERE d.machine_key = src.machine_key AND d.is_current = TRUE
);
