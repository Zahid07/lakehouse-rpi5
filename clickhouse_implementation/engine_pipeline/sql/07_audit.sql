-- Audit views: the same names and columns as the ones added to the original
-- DuckDB pipeline (lakehouse-rpi5/engine_pipeline/sql/marts/audit.sql), so
-- one comparison query works on both. Definitions in research/AUDIT.md.
--
-- All times are UTC. Lags that involve sent_at compare the producer's clock
-- with the Pi's: only meaningful when both are NTP-synced.

-- One row per reading: when it was sampled, sent, received and stored.
-- ClickHouse has no landing step, so landed_at / landing_lag_ms are NULL, and
-- its consumer receives and stores in the same insert, so store_lag_ms ~ 0.
CREATE OR REPLACE VIEW engine.v_audit_reading AS
SELECT
    'clickhouse'                                             AS implementation,
    toString(run_id)                                         AS run_id,
    toString(machine)                                        AS machine,
    seq,
    timestamp,
    sent_at,
    received_at,
    CAST(NULL AS Nullable(DateTime64(6, 'UTC')))             AS landed_at,
    inserted_at                                              AS stored_at,
    dateDiff('millisecond', timestamp, sent_at)              AS producer_lag_ms,   -- sample -> sent
    dateDiff('millisecond', sent_at, received_at)            AS transport_lag_ms,  -- sent -> received (MQTT, RabbitMQ)
    CAST(NULL AS Nullable(Int64))                            AS landing_lag_ms,    -- received -> landed
    dateDiff('millisecond', received_at, inserted_at)        AS store_lag_ms,      -- received/landed -> queryable
    dateDiff('millisecond', sent_at, inserted_at)            AS end_to_end_ms,     -- sent -> queryable
    dateDiff('millisecond', timestamp, inserted_at)          AS sample_to_stored_ms
FROM engine.fact_engine_vibration;

-- One row per machine-minute: completeness, and when the minute's values
-- became queryable.
--
-- ready_at: when the minute's final value could first be read from the
-- v_engine_minute_* views. In this implementation the newest minutes are
-- calculated live from raw readings, so that is the moment its last reading
-- was stored. stored_at: when the calculated value was first written to the
-- stored table (health: minutes older than 10 min; spectrogram: closed
-- minutes). store_count > 1 means it was recalculated for late readings.
CREATE OR REPLACE VIEW engine.v_audit_minute AS
SELECT
    'clickhouse'                                                   AS implementation,
    fm.run_id                                                      AS run_id,
    toString(fm.machine)                                           AS machine,
    fm.window_ts                                                   AS minute_ts,
    fm.samples                                                     AS samples,
    toInt64(fm.seq_hi) - toInt64(fm.seq_lo) + 1                    AS expected_samples,
    toInt64(fm.seq_hi) - toInt64(fm.seq_lo) + 1 - toInt64(fm.samples) AS missing_samples,
    fm.first_sent_at                                               AS first_sent_at,
    fm.last_sent_at                                                AS last_sent_at,
    fm.first_received_at                                           AS first_received_at,
    fm.last_received_at                                            AS last_received_at,
    fm.last_stored_at                                              AS last_stored_at,
    fm.last_stored_at                                              AS health_ready_at,
    h.first_computed_at                                            AS health_stored_at,
    h.n                                                            AS health_store_count,
    fm.last_stored_at                                              AS spectrogram_ready_at,
    s.first_computed_at                                            AS spectrogram_stored_at,
    s.n                                                            AS spectrogram_store_count,
    dateDiff('millisecond', fm.window_ts + INTERVAL 1 MINUTE, fm.last_stored_at) / 1000   AS health_ready_lag_s,
    dateDiff('millisecond', fm.window_ts + INTERVAL 1 MINUTE, h.first_computed_at) / 1000 AS health_stored_lag_s,
    dateDiff('millisecond', fm.window_ts + INTERVAL 1 MINUTE, fm.last_stored_at) / 1000   AS spectrogram_ready_lag_s,
    dateDiff('millisecond', fm.window_ts + INTERVAL 1 MINUTE, s.first_computed_at) / 1000 AS spectrogram_stored_lag_s
FROM
(
    SELECT window_ts, machine,
           max(run_id) AS run_id, sum(rows) AS samples, min(seq_lo) AS seq_lo, max(seq_hi) AS seq_hi,
           min(first_sent_at) AS first_sent_at, max(last_sent_at) AS last_sent_at,
           min(first_received_at) AS first_received_at, max(last_received_at) AS last_received_at,
           max(last_inserted_at) AS last_stored_at
    FROM engine.fact_minutes
    GROUP BY window_ts, machine
) AS fm
LEFT JOIN
(
    SELECT machine_name, minute_ts, min(computed_at) AS first_computed_at, count() AS n
    FROM engine.engine_minute_health GROUP BY machine_name, minute_ts
) AS h ON h.machine_name = fm.machine AND h.minute_ts = fm.window_ts
LEFT JOIN
(
    SELECT machine_name, window_ts, min(computed_at) AS first_computed_at, count() AS n
    FROM engine.engine_minute_spectrogram GROUP BY machine_name, window_ts
) AS s ON s.machine_name = fm.machine AND s.window_ts = fm.window_ts
SETTINGS join_use_nulls = 1;
