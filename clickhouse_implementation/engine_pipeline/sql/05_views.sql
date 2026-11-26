-- The remaining read-time views of the star schema.

-- The fact with its surrogate key attached at read time.
CREATE OR REPLACE VIEW engine.v_fact_engine_vibration AS
SELECT
    f.timestamp       AS timestamp,
    d.machine_key     AS machine_key,
    f.machine         AS machine_name,
    f.seq, f.vib_r, f.vib_a, f.rpm, f.mode
FROM engine.fact_engine_vibration AS f
LEFT JOIN
(
    SELECT machine_key, machine_name FROM engine.machine_dim WHERE is_current
) AS d ON lower(d.machine_name) = lower(f.machine)
SETTINGS join_use_nulls = 1;


-- Condition score: "is this engine behaving oddly", one number per minute.
-- Same method as the original views.sql:
--
-- * Five scalars, because each discriminates a different fault: axial ratio
--   (misalignment), skew (bearing vs looseness), kurtosis (falls for
--   imbalance), crest, and RMS (rises for all of them).
-- * Baseline = the machine's own quietest sixth of minutes (never fewer than
--   three), so it self-calibrates and needs no ground-truth label.
-- * Robust centre and spread: median and 1.4826 * MAD, each spread floored at
--   20% of its centre and at an absolute minimum; each z capped at 10.
-- * Score = quadratic mean of the z-scores; watch at 1.5, alert at 3.5.
--
-- Two deliberate differences, both about cost:
-- * The baseline is built from STORED minutes only (settled data, older than
--   ~10 minutes). A new machine therefore reads 'calibrating' until it has
--   three stored minutes, about 13 minutes after it starts.
-- * Median and MAD come from per-machine arrays in ONE pass. ClickHouse
--   re-runs a WITH subquery every time it is referenced, so the original's
--   centre/spread CTEs re-ran the live health calculation about five times
--   per query. Here the live view is read exactly once.
--
-- median = quantileExactInclusive(0.5): exact, and for an even count the mean
-- of the two middle values -- what DuckDB's median returned.
CREATE OR REPLACE VIEW engine.v_engine_minute_anomaly AS
WITH
baseline AS
(
    SELECT
        machine_name,
        count()                                                      AS baseline_minutes,
        groupArray(rms_r)        AS a_rms,    groupArray(crest_r)     AS a_crest,
        groupArray(kurtosis_r)   AS a_kurt,   groupArray(axial_ratio) AS a_axial,
        groupArray(skew_r)       AS a_skew,
        arrayReduce('quantileExactInclusive(0.5)', a_rms)            AS c_rms,
        arrayReduce('quantileExactInclusive(0.5)', a_crest)          AS c_crest,
        arrayReduce('quantileExactInclusive(0.5)', a_kurt)           AS c_kurt,
        arrayReduce('quantileExactInclusive(0.5)', a_axial)          AS c_axial,
        arrayReduce('quantileExactInclusive(0.5)', a_skew)           AS c_skew,
        arrayReduce('quantileExactInclusive(0.5)', arrayMap(x -> abs(x - c_rms),   a_rms))   AS s_rms,
        arrayReduce('quantileExactInclusive(0.5)', arrayMap(x -> abs(x - c_crest), a_crest)) AS s_crest,
        arrayReduce('quantileExactInclusive(0.5)', arrayMap(x -> abs(x - c_kurt),  a_kurt))  AS s_kurt,
        arrayReduce('quantileExactInclusive(0.5)', arrayMap(x -> abs(x - c_axial), a_axial)) AS s_axial,
        arrayReduce('quantileExactInclusive(0.5)', arrayMap(x -> abs(x - c_skew),  a_skew))  AS s_skew
    FROM
    (
        SELECT toString(machine_name) AS machine_name, rms_r, crest_r, kurtosis_r, axial_ratio, skew_r,
               row_number() OVER (PARTITION BY machine_name ORDER BY rms_r) AS quiet_rank,
               count()      OVER (PARTITION BY machine_name)                AS n_minutes
        FROM engine.engine_minute_health FINAL
    )
    WHERE quiet_rank <= greatest(3, toUInt64(ceil(0.15 * n_minutes)))
    GROUP BY machine_name
),
z AS
(
    SELECT h.minute_ts AS minute_ts, h.machine_key AS machine_key,
           h.machine_name AS machine_name, h.mode_min AS mode_min,
           h.mode_max AS mode_max, h.mode_changed AS mode_changed,
           h.avg_rpm AS avg_rpm, b.baseline_minutes AS baseline_minutes,
           least(10, abs(h.rms_r - b.c_rms)
                 / nullIf(greatest(1.4826 * b.s_rms, abs(b.c_rms) * 0.20, 1e-6), 0))        AS z_rms,
           least(10, abs(h.crest_r - b.c_crest)
                 / nullIf(greatest(1.4826 * b.s_crest, abs(b.c_crest) * 0.20, 0.08), 0))    AS z_crest,
           least(10, abs(h.kurtosis_r - b.c_kurt)
                 / nullIf(greatest(1.4826 * b.s_kurt, abs(b.c_kurt) * 0.20, 0.10), 0))      AS z_kurtosis,
           least(10, abs(h.axial_ratio - b.c_axial)
                 / nullIf(greatest(1.4826 * b.s_axial, abs(b.c_axial) * 0.20, 0.03), 0))    AS z_axial,
           least(10, abs(h.skew_r - b.c_skew)
                 / nullIf(greatest(1.4826 * b.s_skew, abs(b.c_skew) * 0.20, 0.05), 0))      AS z_skew
    FROM engine.v_engine_minute_health AS h
    LEFT JOIN baseline AS b ON b.machine_name = h.machine_name
    WHERE h.rms_r IS NOT NULL
    SETTINGS join_use_nulls = 1
)
SELECT
    minute_ts, machine_key, machine_name, mode_min, mode_max, mode_changed,
    round(avg_rpm, 1) AS avg_rpm, coalesce(baseline_minutes, 0) AS baseline_minutes,
    round(z_rms, 2) AS z_rms, round(z_crest, 2) AS z_crest,
    round(z_kurtosis, 2) AS z_kurtosis, round(z_axial, 2) AS z_axial,
    round(z_skew, 2) AS z_skew,
    if(coalesce(baseline_minutes, 0) < 3, NULL,
       round(sqrt((pow(coalesce(z_rms, 0), 2) + pow(coalesce(z_crest, 0), 2)
                 + pow(coalesce(z_kurtosis, 0), 2) + pow(coalesce(z_axial, 0), 2)
                 + pow(coalesce(z_skew, 0), 2)) / 5.0), 2))           AS score,
    -- WHICH feature is odd, which is most of the diagnosis.
    multiIf(coalesce(baseline_minutes, 0) < 3, NULL,
            greatest(coalesce(z_rms, 0), coalesce(z_crest, 0), coalesce(z_kurtosis, 0),
                     coalesce(z_axial, 0), coalesce(z_skew, 0)) = coalesce(z_axial, 0),    'axial ratio',
            greatest(coalesce(z_rms, 0), coalesce(z_crest, 0), coalesce(z_kurtosis, 0),
                     coalesce(z_axial, 0), coalesce(z_skew, 0)) = coalesce(z_skew, 0),     'skew',
            greatest(coalesce(z_rms, 0), coalesce(z_crest, 0), coalesce(z_kurtosis, 0),
                     coalesce(z_axial, 0), coalesce(z_skew, 0)) = coalesce(z_kurtosis, 0), 'kurtosis',
            greatest(coalesce(z_rms, 0), coalesce(z_crest, 0), coalesce(z_kurtosis, 0),
                     coalesce(z_axial, 0), coalesce(z_skew, 0)) = coalesce(z_crest, 0),    'crest',
            'amplitude')                                              AS top_driver,
    multiIf(coalesce(baseline_minutes, 0) < 3, 'calibrating',
            score >= 3.5, 'alert',
            score >= 1.5, 'watch',
            'normal')                                                 AS status
FROM z;
