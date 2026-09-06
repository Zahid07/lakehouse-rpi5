-- Read-time views: the surrogate key, the dimension attributes, the rounding.
--
-- The marts store the **natural** key and full-precision numbers. Everything
-- the old marts denormalised is restored here, and doing it at read time is
-- better than storing it for two independent reasons.
--
-- **It fixes a staleness bug.** The old accel_hourly_summary joined
-- `ON l.is_current = TRUE` and *stored* location_name, city and country in the
-- mart row. So when the SCD2 dimension changed -- a city corrected -- every
-- mart row already written kept the old value until that hour happened to be
-- recomputed, which for a sealed hour is never. A view is always current.
--
-- **And rounding must not be stored.** Tier two keeps (n, mean, M2) and derives
-- avg/stddev from it; a rounded stored value would corrupt the state the next
-- batch merges into. Separately, `round(avg(x), 4)` classifies as
-- **non_foldable** -- measured -- so putting it in the model would demote the
-- whole thing to tier three and recompute every touched hour.
--
-- The join is LEFT and null-safe on purpose: a reading whose location has not
-- yet reached the dimension still appears, with NULL attributes, rather than
-- vanishing from the mart. The old MERGE made the same choice with
-- `IS NOT DISTINCT FROM`.

CREATE OR REPLACE VIEW marts.v_accel_hourly_summary AS
SELECT
    m.window_ts                     AS hour_ts,
    d.location_key,
    m.location                      AS location_name,
    d.city,
    d.country,
    m.sample_count,
    round(m.avg_x, 4)               AS avg_x,
    round(m.avg_y, 4)               AS avg_y,
    round(m.avg_z, 4)               AS avg_z,
    round(m.avg_magnitude, 4)       AS avg_magnitude,
    round(m.max_magnitude, 4)       AS max_magnitude,
    round(m.min_magnitude, 4)       AS min_magnitude,
    round(m.stddev_magnitude, 4)    AS stddev_magnitude
FROM marts.accel_hourly_summary m
LEFT JOIN curated.location_dim d
       ON lower(d.location_name) = lower(m.location)
      AND d.is_current = TRUE;


CREATE OR REPLACE VIEW marts.v_accel_minute_spectrum AS
SELECT
    m.window_ts,
    d.location_key,
    m.location                      AS location_name,
    m.sample_count,
    m.freq_hz,
    m.fft_x,
    m.fft_y,
    m.fft_z,
    m.fft_magnitude
FROM marts.accel_minute_spectrum m
LEFT JOIN curated.location_dim d
       ON lower(d.location_name) = lower(m.location)
      AND d.is_current = TRUE;


-- The fact, with its surrogate key attached. This is what the old
-- fact_accelerometer stored directly; storing it there would have required the
-- key to be assigned inside duckstream's own commit, which is precisely what
-- cannot be arranged across two processes.
CREATE OR REPLACE VIEW curated.v_fact_accelerometer AS
SELECT
    f.timestamp,
    d.location_key,
    f.location                      AS location_name,
    f.x, f.y, f.z, f.magnitude
FROM curated.fact_accelerometer f
LEFT JOIN curated.location_dim d
       ON lower(d.location_name) = lower(f.location)
      AND d.is_current = TRUE;
