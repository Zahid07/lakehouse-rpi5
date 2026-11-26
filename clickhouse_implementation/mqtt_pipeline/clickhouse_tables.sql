-- Stream accelerometer readings from RabbitMQ into ClickHouse.
--
--   producer --MQTT--> amq.topic "sensors.#" --> queue clickhouse_accel
--            --> iot.accel_queue (RabbitMQ engine) --> iot.accel_mv --> iot.accel_readings
--
-- The queue 'clickhouse_accel' is created and bound in RabbitMQ beforehand, because
-- RabbitMQ refuses to let clients declare the reserved 'amq.topic' exchange.
-- Replace <RABBITMQ_PASSWORD> below with the 'iot' user's password before running.

CREATE DATABASE IF NOT EXISTS iot;

-- 1. Storage: where the readings live and what you query.
CREATE TABLE IF NOT EXISTS iot.accel_readings
(
    ts           DateTime64(6),              -- producer's timestamp (laptop local time)
    location     LowCardinality(String),
    x            Float32,
    y            Float32,
    z            Float32,
    inserted_at  DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (location, ts);

-- 2. Consumer: reads JSON messages from the existing RabbitMQ queue.
--    Not queryable directly; rows flow through it into the materialized view.
CREATE TABLE IF NOT EXISTS iot.accel_queue
(
    timestamp  String,
    location   String,
    x          Float32,
    y          Float32,
    z          Float32
)
ENGINE = RabbitMQ
SETTINGS
    rabbitmq_host_port            = 'localhost:5672',
    rabbitmq_username             = 'iot',
    rabbitmq_password             = '<RABBITMQ_PASSWORD>',
    rabbitmq_exchange_name        = 'amq.topic',     -- informational; nothing is declared
    rabbitmq_format               = 'JSONEachRow',
    rabbitmq_queue_base           = 'clickhouse_accel',
    rabbitmq_queue_consume        = 1,               -- use the pre-created queue as-is
    rabbitmq_skip_broken_messages = 100,             -- don't stall on bad JSON
    rabbitmq_flush_interval_ms    = 1000;            -- write at least once a second

-- 3. Pipe: moves each batch from the queue into storage, parsing the ISO timestamp.
CREATE MATERIALIZED VIEW IF NOT EXISTS iot.accel_mv TO iot.accel_readings AS
SELECT
    parseDateTime64BestEffort(timestamp, 6) AS ts,
    location,
    x,
    y,
    z
FROM iot.accel_queue;
