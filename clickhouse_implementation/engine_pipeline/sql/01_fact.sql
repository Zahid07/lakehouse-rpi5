-- Ingest: RabbitMQ -> engine.fact_engine_vibration, one row per reading.
--
--   producer --MQTT engine/vibration--> amq.topic "engine.vibration" --> queue {{RMQ_QUEUE}}
--            --> engine.vibration_queue (RabbitMQ engine) --> engine.vibration_mv --> fact
--
-- Replaces ingest.py, decode.py and the parquet landing tree. The queue is
-- durable, so readings published while ClickHouse is down wait in RabbitMQ
-- instead of being lost -- the same guarantee the landing tree gave.

CREATE DATABASE IF NOT EXISTS engine;

-- The fact, row level. Plain MergeTree (append), not Replacing, for the same
-- reason the original used `mode: append`: an MQTT redelivery stays visible and
-- measurable as duplicates = count() - uniqExact(seq), rather than being hidden.
--
-- Retention is 6 hours, the intent recorded in ENG_FACT_RETAIN_HOURS. Nothing
-- in the original enforced it; here TTL does, dropping whole hourly partitions.
-- The marts are derived as rows arrive and are kept forever.
CREATE TABLE IF NOT EXISTS engine.fact_engine_vibration
(
    timestamp    DateTime64(6, 'UTC') CODEC(Delta, ZSTD(1)),  -- producer's sample time, UTC
    machine      LowCardinality(String),
    seq          UInt64               CODEC(Delta, ZSTD(1)),  -- per-producer sample index
    vib_r        Float64,
    vib_a        Float64,
    rpm          Float64,
    mode         LowCardinality(String),  -- simulator's commanded fault, ground truth only
    inserted_at  DateTime64(3, 'UTC') DEFAULT now64(3),       -- when ClickHouse stored it
    -- audit (see research/AUDIT.md): experiment label, producer's publish time,
    -- and when ClickHouse's consumer took the reading from RabbitMQ
    run_id       LowCardinality(String) DEFAULT '',
    sent_at      Nullable(DateTime64(6, 'UTC')),
    received_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toStartOfHour(timestamp)
ORDER BY (machine, timestamp)
TTL toDateTime(timestamp) + INTERVAL 6 HOUR
SETTINGS ttl_only_drop_parts = 1;

-- Audit columns for installs created before they existed.
ALTER TABLE engine.fact_engine_vibration
    ADD COLUMN IF NOT EXISTS run_id      LowCardinality(String) DEFAULT '',
    ADD COLUMN IF NOT EXISTS sent_at     Nullable(DateTime64(6, 'UTC')),
    ADD COLUMN IF NOT EXISTS received_at DateTime64(3, 'UTC') DEFAULT now64(3);

-- Consumer. Columns are Nullable so a message with a missing field arrives as
-- NULL and is refused by the view below, as decode.py refused it -- rather
-- than landing with a silent default of 0 or ''.
--
-- Dropped and recreated on every setup: it holds no data (unconsumed messages
-- wait in RabbitMQ), so a column change always takes effect, and recreating it
-- also re-subscribes a consumer RabbitMQ cancelled when the queue was deleted.
DROP VIEW  IF EXISTS engine.vibration_mv SYNC;
DROP TABLE IF EXISTS engine.vibration_queue SYNC;
CREATE TABLE engine.vibration_queue
(
    timestamp  Nullable(String),
    machine    Nullable(String),
    seq        Nullable(Int64),
    vib_r      Nullable(Float64),
    vib_a      Nullable(Float64),
    rpm        Nullable(Float64),
    mode       Nullable(String),
    run_id     Nullable(String),   -- audit, optional: older producers omit them
    sent_at    Nullable(String)
)
ENGINE = RabbitMQ
SETTINGS
    rabbitmq_host_port            = 'localhost:5672',
    rabbitmq_username             = '{{RMQ_USER}}',
    rabbitmq_password             = '{{RMQ_PASSWORD}}',
    rabbitmq_exchange_name        = 'amq.topic',   -- informational; nothing is declared
    rabbitmq_format               = 'JSONEachRow',
    rabbitmq_queue_base           = '{{RMQ_QUEUE}}',
    rabbitmq_queue_consume        = 1,             -- use the pre-created queue as-is
    rabbitmq_skip_broken_messages = 1000,          -- unparseable JSON is dropped, not fatal
    rabbitmq_max_block_size       = 5000,          -- rows, not messages
    rabbitmq_flush_interval_ms    = 1000,
    -- A message may carry many readings (the producer batches 50 per message,
    -- one per line), which JSONEachRow reads as that many rows. Unknown keys
    -- (e.g. audit fields from a newer producer) are skipped, not fatal.
    input_format_skip_unknown_fields = 1;

-- Validation and typing, in the inner query so no alias can shadow a column
-- the filter is testing.
CREATE MATERIALIZED VIEW engine.vibration_mv TO engine.fact_engine_vibration AS
SELECT
    assumeNotNull(ts)                 AS timestamp,
    assumeNotNull(machine)            AS machine,
    toUInt64(assumeNotNull(seq))      AS seq,
    assumeNotNull(vib_r)              AS vib_r,
    assumeNotNull(vib_a)              AS vib_a,
    assumeNotNull(rpm)                AS rpm,
    assumeNotNull(mode)               AS mode,
    coalesce(run_id, '')              AS run_id,
    sent_ts                           AS sent_at,
    now64(3, 'UTC')                   AS received_at   -- this block left the consumer now
FROM
(
    SELECT parseDateTime64BestEffortOrNull(timestamp, 6, 'UTC') AS ts,
           parseDateTime64BestEffortOrNull(sent_at, 6, 'UTC')   AS sent_ts,
           machine, seq, vib_r, vib_a, rpm, mode, run_id
    FROM engine.vibration_queue
    WHERE parseDateTime64BestEffortOrNull(timestamp, 6, 'UTC') IS NOT NULL
      AND machine != '' AND mode != ''
      AND seq >= 0
      AND vib_r IS NOT NULL AND vib_a IS NOT NULL AND rpm IS NOT NULL
);

-- Arrival summary: readings per machine per minute, and when the last one
-- arrived, maintained as rows are inserted. Tiny (one row per machine-minute
-- per insert block, merged in the background), and it is what the storing
-- jobs plan from: each machine gets its own exact to-do list, and a stored
-- minute that later receives more readings (a replayed backlog) is noticed
-- and recalculated.
--
-- It doubles as the per-minute audit record (engine.v_audit_minute), so it is
-- kept 30 days -- far longer than the raw readings -- at one row per machine-
-- minute.
CREATE TABLE IF NOT EXISTS engine.fact_minutes
(
    window_ts          DateTime('UTC'),
    machine            LowCardinality(String),
    rows               SimpleAggregateFunction(sum, UInt64),
    last_inserted_at   SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    -- audit
    run_id             SimpleAggregateFunction(max, String),
    seq_lo             SimpleAggregateFunction(min, UInt64),
    seq_hi             SimpleAggregateFunction(max, UInt64),
    first_sent_at      SimpleAggregateFunction(min, Nullable(DateTime64(6, 'UTC'))),
    last_sent_at       SimpleAggregateFunction(max, Nullable(DateTime64(6, 'UTC'))),
    first_received_at  SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_received_at   SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY (window_ts, machine)
TTL window_ts + INTERVAL 30 DAY;

-- For installs created before the audit columns (their values start filling
-- only after ./reset.sh, see the note on fact_minutes_mv below).
ALTER TABLE engine.fact_minutes
    ADD COLUMN IF NOT EXISTS run_id            SimpleAggregateFunction(max, String),
    ADD COLUMN IF NOT EXISTS seq_lo            SimpleAggregateFunction(min, UInt64),
    ADD COLUMN IF NOT EXISTS seq_hi            SimpleAggregateFunction(max, UInt64),
    ADD COLUMN IF NOT EXISTS first_sent_at     SimpleAggregateFunction(min, Nullable(DateTime64(6, 'UTC'))),
    ADD COLUMN IF NOT EXISTS last_sent_at      SimpleAggregateFunction(max, Nullable(DateTime64(6, 'UTC'))),
    ADD COLUMN IF NOT EXISTS first_received_at SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    ADD COLUMN IF NOT EXISTS last_received_at  SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    MODIFY TTL window_ts + INTERVAL 30 DAY;

-- NOTE: an install created before the audit columns needs ./reset.sh to pick
-- up this definition (it is not dropped here: dropping it while readings flow
-- would miss rows and corrupt the counts the storing jobs plan from).
CREATE MATERIALIZED VIEW IF NOT EXISTS engine.fact_minutes_mv TO engine.fact_minutes AS
SELECT
    toDateTime(toStartOfMinute(timestamp), 'UTC') AS window_ts,
    machine,
    count()                                       AS rows,
    max(inserted_at)                              AS last_inserted_at,
    max(toString(run_id))                         AS run_id,
    min(seq)                                      AS seq_lo,
    max(seq)                                      AS seq_hi,
    min(sent_at)                                  AS first_sent_at,
    max(sent_at)                                  AS last_sent_at,
    min(received_at)                              AS first_received_at,
    max(received_at)                              AS last_received_at
FROM engine.fact_engine_vibration
GROUP BY window_ts, machine;

-- One-time backfill of readings that arrived before the view above existed.
-- Bounded by the view's creation time so no reading is counted twice, and
-- skipped once done (any row older than that time means it already ran).
INSERT INTO engine.fact_minutes
    (window_ts, machine, rows, last_inserted_at, run_id, seq_lo, seq_hi,
     first_sent_at, last_sent_at, first_received_at, last_received_at)
SELECT toDateTime(toStartOfMinute(timestamp), 'UTC'), machine, count(), max(inserted_at),
       max(toString(run_id)), min(seq), max(seq), min(sent_at), max(sent_at),
       min(received_at), max(received_at)
FROM engine.fact_engine_vibration
WHERE inserted_at < (SELECT toDateTime64(metadata_modification_time, 3) FROM system.tables
                     WHERE database = 'engine' AND name = 'fact_minutes_mv')
  AND (SELECT count() FROM engine.fact_minutes
       WHERE last_inserted_at < (SELECT toDateTime64(metadata_modification_time, 3) FROM system.tables
                                 WHERE database = 'engine' AND name = 'fact_minutes_mv')) = 0
GROUP BY 1, 2;
