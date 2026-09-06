-- Read-time views: the surrogate key, the derived scalars, the rounding.
--
-- Everything derived lives here rather than in a model, for two reasons that
-- both bite. Tier two stores (n, mean, M2) and derives the visible column from
-- the merged state, so a rounded *stored* value would corrupt the next merge.
-- And `round(avg(x),4)` classifies non_foldable, which would silently demote a
-- whole model to tier three and recompute every window.
--
-- Storing the dimension attributes instead of joining them would also reproduce
-- the staleness bug the accelerometer README describes: a corrected attribute
-- would never reach an already-written row. A view is always current.

-- RMS, crest, skewness and kurtosis, all from the raw moments the tier-2 model
-- folded. Kurtosis from raw moments cancels badly when the mean is large
-- relative to the spread; vib_r is AC-coupled with m1 ~ 0, so it does not bite
-- here -- and `verify --check truth` measures the residual against a two-pass
-- computation rather than asserting it is fine.
CREATE OR REPLACE VIEW marts.v_engine_minute_health AS
SELECT
    h.window_ts                                     AS minute_ts,
    d.machine_key,
    h.machine                                       AS machine_name,
    d.site,
    d.asset_type,
    d.nominal_rpm,
    h.sample_count,
    round(h.sample_count / 60.0, 1)                 AS effective_hz,
    round(sqrt(h.m2_r), 5)                          AS rms_r,
    round(h.peak_r, 5)                              AS peak_r,
    round(h.peak_r / nullif(sqrt(h.m2_r), 0), 3)    AS crest_r,
    round((h.m4_r - 4*h.m1_r*h.m3_r + 6*h.m1_r*h.m1_r*h.m2_r - 3*pow(h.m1_r, 4))
          / nullif(pow(h.m2_r - h.m1_r*h.m1_r, 2), 0), 3)          AS kurtosis_r,
    round((h.m3_r - 3*h.m1_r*h.m2_r + 2*pow(h.m1_r, 3))
          / nullif(pow(sqrt(h.m2_r - h.m1_r*h.m1_r), 3), 0), 3)    AS skew_r,
    round(sqrt(h.m2_a), 5)                          AS rms_a,
    -- The misalignment discriminator, and a scalar rather than a picture.
    round(sqrt(h.m2_a) / nullif(sqrt(h.m2_r), 0), 3) AS axial_ratio,
    round(h.avg_rpm, 1)                             AS avg_rpm,
    round(h.min_rpm, 1)                             AS min_rpm,
    round(h.max_rpm, 1)                             AS max_rpm,
    round(h.sd_rpm, 2)                              AS sd_rpm,
    round(h.avg_rpm / 60.0, 3)                      AS shaft_hz,
    h.mode_min,
    h.mode_max,
    (h.mode_min <> h.mode_max)                      AS mode_changed,
    -- Missing readings, exactly. Zero when every seq in the range arrived.
    (h.seq_hi - h.seq_lo + 1) - h.sample_count      AS seq_deficit
FROM marts.engine_minute_health h
LEFT JOIN curated.machine_dim d
       ON lower(d.machine_name) = lower(h.machine)
      AND d.is_current = TRUE;


-- The spectrogram, plus the reshape contract. n_bins and n_frames are derived
-- from the row's OWN frame_size, not from a global -- which is what makes a
-- historical row still readable after ENG_FRAME_SIZE is changed.
CREATE OR REPLACE VIEW marts.v_engine_minute_spectrogram AS
SELECT
    s.window_ts,
    d.machine_key,
    s.machine                                       AS machine_name,
    s.sample_count,
    CAST(s.frame_size AS INTEGER)                   AS frame_size,
    CAST(s.hop_size AS INTEGER)                     AS hop_size,
    s.sample_rate                                   AS sample_rate_hz,
    CAST(s.frame_size / 2 AS INTEGER) + 1           AS n_bins,
    CAST(len(s.spec_db) / (CAST(s.frame_size / 2 AS INTEGER) + 1) AS INTEGER)
                                                    AS n_frames,
    s.sample_rate / s.frame_size                    AS df_hz,
    s.hop_size / s.sample_rate                      AS hop_seconds,
    s.frame_size / s.sample_rate                    AS frame_seconds,
    s.spec_db
FROM marts.engine_minute_spectrogram s
LEFT JOIN curated.machine_dim d
       ON lower(d.machine_name) = lower(s.machine)
      AND d.is_current = TRUE;


-- The fact with its surrogate key attached at read time.
CREATE OR REPLACE VIEW curated.v_fact_engine_vibration AS
SELECT
    f.timestamp,
    d.machine_key,
    f.machine                                       AS machine_name,
    f.seq, f.vib_r, f.vib_a, f.rpm, f.mode
FROM curated.fact_engine_vibration f
LEFT JOIN curated.machine_dim d
       ON lower(d.machine_name) = lower(f.machine)
      AND d.is_current = TRUE;
