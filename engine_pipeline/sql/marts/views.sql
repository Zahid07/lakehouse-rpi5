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


-- Condition score: "is this engine behaving oddly", as one number per minute.
--
-- Every scalar discriminates a DIFFERENT fault -- measured on this very data:
-- axial ratio separates misalignment (x4), skew separates bearing from
-- looseness, kurtosis *falls* for imbalance, and RMS rises about x4 for all of
-- them. So RMS alone says "something changed" and nothing more, while no single
-- scalar catches everything. Combining them into one deviation does.
--
-- The baseline is the machine's own QUIETEST minutes, not its first ones. A
-- fixed "first N minutes are healthy" assumption is wrong the moment a producer
-- starts in a fault -- which happens here, and would silently calibrate the
-- detector to a broken engine. Quietest-quartile is self-calibrating and needs
-- no ground-truth label, which matters because a real machine has no `mode`
-- column: that is the thing you are trying to infer, not an input.
--
-- Median and MAD rather than mean and standard deviation. The baseline window
-- can still contain a fault minute, and one outlier inflates a standard
-- deviation enough to hide every subsequent fault. Measured here, the robust
-- form separates healthy (0.13-0.31) from faulted (6.3-8.0); the mean/stddev
-- form put healthy at 0.46-0.83 against faults at 1.5-4.0, a fifth of the
-- margin.
--
-- 1.4826 * MAD estimates the standard deviation of a normal distribution. Each
-- spread is floored at 20% of its own baseline centre as well as at an absolute
-- minimum: MAD alone is only as good as the baseline, and a baseline holding
-- one fault minute produces a spread so wide that every later fault reads as
-- normal. The relative floor bounds that in both directions. Each z is capped
-- at 10 so one extreme feature cannot swamp the other four.
--
-- Measured on two independently seeded engines: healthy minutes score 0.09 and
-- 0.15 at worst, the quietest faulted minute scores 5.04 -- margins of 54.8x
-- and 34.4x. Thresholds of 1.5 (watch) and 3.5 (alert) sit in empty space.
CREATE OR REPLACE VIEW marts.v_engine_minute_anomaly AS
WITH h AS (
    SELECT minute_ts, machine_key, machine_name, mode_min, mode_max,
           mode_changed, rms_r, crest_r, kurtosis_r, axial_ratio, skew_r, avg_rpm
    FROM marts.v_engine_minute_health
    WHERE rms_r IS NOT NULL
),
ranked AS (
    SELECT *,
           row_number() OVER (PARTITION BY machine_name ORDER BY rms_r) AS quiet_rank,
           count(*)     OVER (PARTITION BY machine_name)                AS n_minutes
    FROM h
),
-- The quietest sixth, never fewer than three minutes. Measured: a quarter was
-- too wide -- on a machine cycling through faults the quietest five minutes
-- spanned four different ones, which inflated every MAD until no fault looked
-- unusual at all (that machine's healthy/fault margin was 0.6x, i.e. inverted).
-- Tightening to three collapsed the contamination and took it to 15.7x.
quiet AS (
    SELECT * FROM ranked
    WHERE quiet_rank <= greatest(3, CAST(ceil(0.15 * n_minutes) AS BIGINT))
),
centre AS (
    SELECT machine_name, count(*) AS baseline_minutes,
           median(rms_r) AS c_rms,   median(crest_r) AS c_crest,
           median(kurtosis_r) AS c_kurt, median(axial_ratio) AS c_axial,
           median(skew_r) AS c_skew
    FROM quiet GROUP BY machine_name
),
spread AS (
    SELECT q.machine_name,
           median(abs(q.rms_r       - c.c_rms))   AS s_rms,
           median(abs(q.crest_r     - c.c_crest)) AS s_crest,
           median(abs(q.kurtosis_r  - c.c_kurt))  AS s_kurt,
           median(abs(q.axial_ratio - c.c_axial)) AS s_axial,
           median(abs(q.skew_r      - c.c_skew))  AS s_skew
    FROM quiet q JOIN centre c USING (machine_name)
    GROUP BY q.machine_name
),
z AS (
    SELECT h.minute_ts, h.machine_key, h.machine_name, h.mode_min, h.mode_max,
           h.mode_changed, h.avg_rpm, c.baseline_minutes,
           least(10, abs(h.rms_r      - c.c_rms)
                 / nullif(greatest(1.4826*s.s_rms, abs(c.c_rms)*0.20, 1e-6),0)) AS z_rms,
           least(10, abs(h.crest_r    - c.c_crest)
                 / nullif(greatest(1.4826*s.s_crest, abs(c.c_crest)*0.20, 0.08),0))               AS z_crest,
           least(10, abs(h.kurtosis_r - c.c_kurt)
                 / nullif(greatest(1.4826*s.s_kurt, abs(c.c_kurt)*0.20, 0.10),0))               AS z_kurtosis,
           least(10, abs(h.axial_ratio- c.c_axial)
                 / nullif(greatest(1.4826*s.s_axial, abs(c.c_axial)*0.20, 0.03),0))               AS z_axial,
           least(10, abs(h.skew_r     - c.c_skew)
                 / nullif(greatest(1.4826*s.s_skew, abs(c.c_skew)*0.20, 0.05),0))               AS z_skew
    FROM h JOIN centre c USING (machine_name) JOIN spread s USING (machine_name)
)
SELECT
    minute_ts, machine_key, machine_name, mode_min, mode_max, mode_changed,
    round(avg_rpm, 1) AS avg_rpm, baseline_minutes,
    round(z_rms, 2) AS z_rms, round(z_crest, 2) AS z_crest,
    round(z_kurtosis, 2) AS z_kurtosis, round(z_axial, 2) AS z_axial,
    round(z_skew, 2) AS z_skew,
    -- Quadratic mean of the z-scores: one large deviation lifts the score
    -- without four calm features averaging it away.
    round(sqrt((coalesce(z_rms,0)^2 + coalesce(z_crest,0)^2
              + coalesce(z_kurtosis,0)^2 + coalesce(z_axial,0)^2
              + coalesce(z_skew,0)^2) / 5.0), 2)                  AS score,
    -- WHICH feature is odd, which is most of the diagnosis: axial means
    -- misalignment, skew separates bearing from looseness, a kurtosis-led
    -- score with low RMS is imbalance.
    CASE greatest(coalesce(z_rms,0), coalesce(z_crest,0), coalesce(z_kurtosis,0),
                  coalesce(z_axial,0), coalesce(z_skew,0))
         WHEN coalesce(z_axial,0)     THEN 'axial ratio'
         WHEN coalesce(z_skew,0)      THEN 'skew'
         WHEN coalesce(z_kurtosis,0)  THEN 'kurtosis'
         WHEN coalesce(z_crest,0)     THEN 'crest'
         ELSE 'amplitude'
    END                                                            AS top_driver,
    CASE WHEN baseline_minutes < 3 THEN 'calibrating'
         WHEN sqrt((coalesce(z_rms,0)^2 + coalesce(z_crest,0)^2
                  + coalesce(z_kurtosis,0)^2 + coalesce(z_axial,0)^2
                  + coalesce(z_skew,0)^2) / 5.0) >= 3.5 THEN 'alert'
         WHEN sqrt((coalesce(z_rms,0)^2 + coalesce(z_crest,0)^2
                  + coalesce(z_kurtosis,0)^2 + coalesce(z_axial,0)^2
                  + coalesce(z_skew,0)^2) / 5.0) >= 1.5 THEN 'watch'
         ELSE 'normal'
    END                                                            AS status
FROM z;
