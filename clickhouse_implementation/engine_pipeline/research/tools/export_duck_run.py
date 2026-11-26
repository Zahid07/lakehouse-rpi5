#!/usr/bin/env python3
"""Export one DuckDB-pipeline run into the same files as the ClickHouse export.

Run with the DuckDB pipeline's own Python (it has duckdb 1.5.5 and the
ducklake extension), AFTER stopping its ingest and pipeline -- the DuckLake
catalog allows one process at a time:

    ~/python_scripts/lakehouse-rpi5/.venv/bin/python export_duck_run.py \\
        --root ~/engine-lake-audit --out ../runs/duck_run1 \\
        --from "2026-10-06 10:00:00" --to "2026-10-06 12:10:00"

--from/--to are UTC and select readings by sample timestamp (as the
ClickHouse export did). Writes: audit_minute.csv, latency_per_minute.csv,
audit_reading.parquet, summary_per_producer.csv, storage.csv, batches.csv.
"""

import argparse
import csv
import os
from pathlib import Path

import duckdb


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", required=True, help="the run's ENG_ROOT")
    ap.add_argument("--out", required=True, help="output directory")
    ap.add_argument("--from", dest="t0", required=True, help="UTC, e.g. '2026-10-06 10:00:00'")
    ap.add_argument("--to", dest="t1", required=True, help="UTC")
    args = ap.parse_args()

    root = Path(os.path.expanduser(args.root)).resolve()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("LOAD ducklake")
    con.execute(f"ATTACH 'ducklake:{root / 'catalog.ducklake'}' AS l (READ_ONLY)")
    w = f"timestamp >= TIMESTAMP '{args.t0}' AND timestamp < TIMESTAMP '{args.t1}'"
    wm = f"minute_ts >= TIMESTAMP '{args.t0}' AND minute_ts < TIMESTAMP '{args.t1}'"

    def to_csv(name: str, sql: str) -> None:
        con.execute(f"COPY ({sql}) TO '{out / name}' (HEADER, DELIMITER ',')")
        print(f"  {name}")

    to_csv("audit_minute.csv",
           f"SELECT * FROM l.marts.v_audit_minute WHERE {wm} ORDER BY machine, minute_ts")
    to_csv("latency_per_minute.csv", f"""
        SELECT date_trunc('minute', stored_at) AS minute, machine, run_id, count(*) AS readings,
               round(avg(end_to_end_ms)) AS e2e_avg_ms,
               round(quantile_cont(end_to_end_ms, 0.5)) AS e2e_p50_ms,
               round(quantile_cont(end_to_end_ms, 0.95)) AS e2e_p95_ms,
               round(quantile_cont(end_to_end_ms, 0.99)) AS e2e_p99_ms,
               max(end_to_end_ms) AS e2e_max_ms,
               round(avg(transport_lag_ms)) AS transport_avg_ms, min(transport_lag_ms) AS transport_min_ms,
               round(avg(landing_lag_ms)) AS landing_avg_ms,
               round(avg(store_lag_ms)) AS store_avg_ms, round(avg(producer_lag_ms)) AS producer_lag_avg_ms
        FROM l.curated.v_audit_reading WHERE {w}
        GROUP BY ALL ORDER BY minute, machine""")
    con.execute(f"COPY (SELECT * FROM l.curated.v_audit_reading WHERE {w} ORDER BY machine, timestamp) "
                f"TO '{out / 'audit_reading.parquet'}' (FORMAT PARQUET)")
    print("  audit_reading.parquet")
    to_csv("summary_per_producer.csv", f"""
        SELECT run_id, machine, count(*) AS readings,
               count(DISTINCT seq) AS distinct_seq, max(seq) - min(seq) + 1 AS sent_seq_range,
               count(*) - count(DISTINCT seq) AS duplicates,
               max(seq) - min(seq) + 1 - count(DISTINCT seq) AS missing,
               min(timestamp) AS first, max(timestamp) AS last,
               round(avg(end_to_end_ms)) AS e2e_avg_ms,
               round(quantile_cont(end_to_end_ms, 0.5)) AS e2e_p50_ms,
               round(quantile_cont(end_to_end_ms, 0.99)) AS e2e_p99_ms,
               min(transport_lag_ms) AS min_transport_ms
        FROM l.curated.v_audit_reading WHERE {w}
        GROUP BY run_id, machine ORDER BY first""")
    # duckstream's own per-batch timings: the DuckDB counterpart of
    # ClickHouse's refresh-job and insert timings.
    to_csv("batches.csv", """
        SELECT model_name, batch_id, started_at, committed_at,
               date_diff('millisecond', started_at, committed_at) AS ms, rows_in, rows_out
        FROM l.duckstream.batches ORDER BY committed_at""")

    sizes = {}
    for sub in ("landing", "lake_data", ""):
        p = root / sub if sub else root
        total = sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0
        sizes[sub or "total (ENG_ROOT)"] = total
    with open(out / "storage.csv", "w", newline="") as fh:
        wr = csv.writer(fh); wr.writerow(["part", "bytes", "mib"])
        for k, v in sizes.items():
            wr.writerow([k, v, round(v / 1048576, 1)])
    print("  storage.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
