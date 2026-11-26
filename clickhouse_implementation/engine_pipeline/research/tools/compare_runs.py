#!/usr/bin/env python3
"""Compare ch_run1 (ClickHouse) and duck_run1 (DuckDB) into research/runs/comparison/.

Run with the DuckDB venv (only used as a CSV query engine):
    ~/python_scripts/lakehouse-rpi5/.venv/bin/python research/tools/compare_runs.py

Phases are the number of producers connected (0 = idle baseline). Latency uses
the per machine-minute audit (same columns in both runs). received -> stored is
measured on the Pi's clock only, so it is free of the laptop clock offset.
"""
from pathlib import Path

import duckdb

RUNS = Path(__file__).resolve().parent.parent / "runs"
OUT = RUNS / "comparison"
OUT.mkdir(exist_ok=True)
RUN = {"clickhouse": "ch_run1", "duckdb": "duck_run1"}

c = duckdb.connect()
c.execute("SET memory_limit='1GB'; SET threads=2")
for impl, run in RUN.items():
    c.execute(f"CREATE VIEW sys_{impl} AS SELECT * FROM read_csv_auto('{RUNS / run / 'sysmon.csv'}', types={{'throttled': 'VARCHAR'}}) "
              f"WHERE producers_connected > 0 OR ts_utc < (SELECT min(ts_utc) FROM read_csv_auto('{RUNS / run / 'sysmon.csv'}') WHERE producers_connected > 0)")
    c.execute(f"CREATE VIEW am_{impl} AS SELECT * FROM read_csv_auto('{RUNS / run / 'audit_minute.csv'}')")

# Implementation-specific process CPU / memory (percent of ONE core; 400 = whole Pi).
IMPL_CPU = {"clickhouse": "clickhouse_cpu_pct + stft_udf_cpu_pct",
            "duckdb": "duck_pipeline_cpu_pct + duck_ingest_cpu_pct"}
IMPL_RSS = {"clickhouse": "clickhouse_rss_mb + stft_udf_rss_mb",
            "duckdb": "duck_pipeline_rss_mb + duck_ingest_rss_mb"}


def export(name, sql):
    c.execute(f"COPY ({sql}) TO '{OUT / name}' (HEADER)")
    print(f"\n== {name}")
    print(c.sql(sql))


resources = " UNION ALL ".join(f"""
    SELECT '{impl}' AS impl, producers_connected AS producers, count(*) AS samples,
      round(avg(cpu_pct), 1) AS host_cpu_pct,
      round(avg({IMPL_CPU[impl]}), 1) AS engine_cpu_pct_of_core,
      round(quantile_cont({IMPL_CPU[impl]}, 0.95), 1) AS engine_cpu_p95,
      round(avg(rabbitmq_cpu_pct), 1) AS rabbitmq_cpu_pct,
      round(avg({IMPL_RSS[impl]})) AS engine_rss_mb,
      round(max({IMPL_RSS[impl]})) AS engine_rss_max_mb,
      round(avg(mem_used_pct), 1) AS host_mem_used_pct,
      round(avg(load1), 2) AS load1,
      round(avg(temp_c), 1) AS temp_avg_c, round(max(temp_c), 1) AS temp_max_c,
      round(avg(freq_mhz)) AS freq_avg_mhz,
      round(avg(disk_write_kBps)) AS disk_write_kBps
    FROM sys_{impl} GROUP BY producers""" for impl in RUN)
export("resources_by_producers.csv", f"SELECT * FROM ({resources}) ORDER BY producers, impl")

# Thermal detail: decode the live bits (0x1 under-voltage, 0x2 capped, 0x4 throttled, 0x8 soft temp limit).
thermal = " UNION ALL ".join(f"""
    SELECT '{impl}' AS impl, producers_connected AS producers, count(*) AS samples,
      round(100 * avg(CASE WHEN (throttled::BIGINT & 8) > 0 THEN 1 ELSE 0 END), 1) AS soft_temp_limit_pct,
      round(100 * avg(CASE WHEN (throttled::BIGINT & 6) > 0 THEN 1 ELSE 0 END), 1) AS capped_or_throttled_pct,
      round(100 * avg(CASE WHEN (throttled::BIGINT & 1) > 0 THEN 1 ELSE 0 END), 1) AS undervoltage_pct,
      round(min(freq_mhz)) AS freq_min_mhz
    FROM sys_{impl} GROUP BY producers""" for impl in RUN)
export("thermal_by_producers.csv", f"SELECT * FROM ({thermal}) ORDER BY producers, impl")

lat = " UNION ALL ".join(f"""
    SELECT '{impl}' AS impl, p.producers, count(*) AS machine_minutes, sum(samples) AS readings,
      sum(missing_samples) AS missing,
      round(quantile_cont(epoch(last_stored_at - last_received_at) * 1000, 0.5)) AS recv_to_stored_p50_ms,
      round(quantile_cont(epoch(last_stored_at - last_received_at) * 1000, 0.95)) AS recv_to_stored_p95_ms,
      round(max(epoch(last_stored_at - last_received_at) * 1000)) AS recv_to_stored_max_ms,
      round(quantile_cont(health_ready_lag_s, 0.5), 2) AS health_ready_p50_s,
      round(quantile_cont(spectrogram_stored_lag_s, 0.5), 1) AS spectrogram_stored_p50_s,
      round(quantile_cont(health_stored_lag_s, 0.5), 1) AS health_stored_p50_s
    FROM am_{impl} a
    JOIN (SELECT minute_ts, count(DISTINCT machine) AS producers FROM am_{impl} GROUP BY minute_ts) p USING (minute_ts)
    WHERE a.minute_ts > (SELECT min(minute_ts) FROM am_{impl}) AND samples = expected_samples
    GROUP BY p.producers""" for impl in RUN)
export("latency_by_producers.csv", f"SELECT * FROM ({lat}) ORDER BY producers, impl")

# Latency drift over the run at constant load (6 producers): does it grow with time?
drift = " UNION ALL ".join(f"""
    SELECT '{impl}' AS impl, (date_diff('minute', (SELECT min(minute_ts) FROM am_{impl}), minute_ts) // 30) * 30 AS run_minute,
      round(quantile_cont(epoch(last_stored_at - last_received_at) * 1000, 0.5)) AS recv_to_stored_p50_ms,
      count(DISTINCT machine) AS producers
    FROM am_{impl} WHERE samples = expected_samples GROUP BY ALL""" for impl in RUN)
export("latency_over_time.csv", f"SELECT * FROM ({drift}) ORDER BY run_minute, impl")

totals = " UNION ALL ".join(f"""
    SELECT '{impl}' AS impl, count(DISTINCT machine) AS machines, sum(samples) AS readings,
      sum(expected_samples) AS expected, sum(missing_samples) AS missing,
      min(minute_ts) AS first_minute, max(minute_ts) AS last_minute
    FROM am_{impl}""" for impl in RUN)
export("totals.csv", totals)
