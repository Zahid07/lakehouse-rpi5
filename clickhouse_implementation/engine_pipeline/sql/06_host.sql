-- Raspberry Pi resource samples, written by the dashboard's sampler every 5 s.
-- Was a separate DuckDB file because the DuckLake catalog was single-attach;
-- ClickHouse has no such limit, so it lives beside everything else. TTL does
-- the 72-hour retention the sampler used to do with DELETE.
CREATE TABLE IF NOT EXISTS engine.host_samples
(
    ts                DateTime('UTC'),
    cpu_pct           Nullable(Float64),
    cpu_busiest_pct   Nullable(Float64),
    temp_c            Nullable(Float64),
    mem_used_pct      Nullable(Float64),
    mem_available_mb  Nullable(Float64),
    swap_used_mb      Nullable(Float64),
    swap_total_mb     Nullable(Float64),
    load1             Nullable(Float64),
    freq_mhz          Nullable(Float64),
    throttled         Nullable(Int64),
    producers         UInt16,   -- producer.py processes on the Pi itself
    clickhouse_procs  UInt16,   -- clickhouse-server running?
    rabbitmq_procs    UInt16,   -- RabbitMQ (beam.smp) running?
    active_machines   UInt16    -- machines seen publishing in the last 90 s
)
ENGINE = MergeTree
ORDER BY ts
TTL ts + INTERVAL 72 HOUR;
