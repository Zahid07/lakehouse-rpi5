import os
import sys
import uuid
import glob
import time
from pathlib import Path
from utils.connector import get_connection, close_connection
from utils.sql_runner import run_sql_file
from utils.udf_registry import register_fft_udfs

def attach_ducklake(con, config):
    import os
    catalog_file = config["ducklake_catalog"].replace("ducklake:", "")
    if not os.path.exists(catalog_file):
        # first-time creation: must specify data path
        con.execute(f"""
            ATTACH '{config["ducklake_catalog"]}'
            AS my_ducklake
            (DATA_PATH '{config["ducklake_data_path"]}')
        """)
    else:
        # reattaching existing catalog: no DATA_PATH needed, it's already stored
        con.execute(f"""
            ATTACH '{config["ducklake_catalog"]}'
            AS my_ducklake
        """)
    con.execute("USE my_ducklake")


# ─────────────────────────────────────────
# Load env config
# ─────────────────────────────────────────
def load_config(env: str) -> dict:
    config_path = Path(f"config/{env}.env")
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    config = {}
    for line in config_path.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            config[k.strip()] = v.strip()
    return config


# ─────────────────────────────────────────
# Bootstrap schemas
# ─────────────────────────────────────────
def bootstrap_schemas(con, config: dict):
    for schema in [config["staging_schema"], config["curated_schema"], config["marts_schema"]]:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print("  Schemas ready.")


def apply_duckdb_runtime_tuning(con, config: dict):
    # Safer defaults for low-memory systems; can be overridden via env file.
    memory_limit = config.get("duckdb_memory_limit", "2GB")
    threads = config.get("duckdb_threads", "2")
    preserve_order = config.get("duckdb_preserve_insertion_order", "false").lower()

    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET threads={int(threads)}")
    con.execute(f"SET preserve_insertion_order={preserve_order}")
    print(
        "  DuckDB tuning applied -> "
        f"memory_limit={memory_limit}, threads={threads}, "
        f"preserve_insertion_order={preserve_order}"
    )


def ensure_benchmark_table(con, config: dict):
    benchmark_table = f"{config['marts_schema']}.pipeline_benchmark"
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {benchmark_table} (
            run_id VARCHAR,
            env VARCHAR,
            batch_id VARCHAR,
            raw_file VARCHAR,
            step_name VARCHAR,
            sql_file VARCHAR,
            target_table VARCHAR,
            data_ingested_rows BIGINT,
            data_ingested_bytes BIGINT,
            duration_ms DOUBLE,
            start_ts TIMESTAMP,
            end_ts TIMESTAMP,
            status VARCHAR,
            error_message VARCHAR,
            ins_tmstmp TIMESTAMP
        )
        """
    )


def ensure_file_cdc_table(con, config: dict):
    cdc_table = f"{config['staging_schema']}.processed_files_cdc"
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {cdc_table} (
            raw_file VARCHAR,
            file_size_bytes BIGINT,
            run_id VARCHAR,
            batch_id VARCHAR,
            status VARCHAR,
            processed_ts TIMESTAMP,
            ins_tmstmp TIMESTAMP
        )
        """
    )


def get_unprocessed_files(con, config: dict, raw_files: list) -> list:
    if not raw_files:
        return []

    cdc_table = f"{config['staging_schema']}.processed_files_cdc"
    result = con.execute(
        f"""
        SELECT raw_file
        FROM {cdc_table}
        WHERE status = 'SUCCESS'
        """
    ).fetchall()
    processed_files = {row[0] for row in result}
    return [f for f in raw_files if f not in processed_files]


def mark_file_processed(
    con,
    config: dict,
    raw_file: str,
    file_size_bytes: int,
    run_id: str,
    batch_id: str,
    status: str,
):
    cdc_table = f"{config['staging_schema']}.processed_files_cdc"
    con.execute(
        f"""
        INSERT INTO {cdc_table} (
            raw_file,
            file_size_bytes,
            run_id,
            batch_id,
            status,
            processed_ts,
            ins_tmstmp
        )
        VALUES (?, ?, ?, ?, ?, current_timestamp, current_timestamp)
        """,
        [raw_file, file_size_bytes, run_id, batch_id, status],
    )


def get_batch_row_count(con, table_name: str, batch_id: str) -> int:
    try:
        result = con.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE batch_id = ?",
            [batch_id],
        ).fetchone()
        return int(result[0]) if result else 0
    except Exception:
        return 0


def write_benchmark(con, config: dict, metric: dict):
    benchmark_table = f"{config['marts_schema']}.pipeline_benchmark"
    con.execute(
        f"""
        INSERT INTO {benchmark_table} (
            run_id,
            env,
            batch_id,
            raw_file,
            step_name,
            sql_file,
            target_table,
            data_ingested_rows,
            data_ingested_bytes,
            duration_ms,
            start_ts,
            end_ts,
            status,
            error_message,
            ins_tmstmp
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
        """,
        [
            metric["run_id"],
            metric["env"],
            metric["batch_id"],
            metric["raw_file"],
            metric["step_name"],
            metric["sql_file"],
            metric["target_table"],
            metric["data_ingested_rows"],
            metric["data_ingested_bytes"],
            metric["duration_ms"],
            metric["start_ts"],
            metric["end_ts"],
            metric["status"],
            metric["error_message"],
        ],
    )


def run_step_with_benchmark(
    con,
    config: dict,
    params: dict,
    run_id: str,
    env: str,
    raw_file: str,
    raw_file_size: int,
    step_name: str,
    sql_file: str,
    target_table: str,
    catalog_path: str,
):
    start_ts = con.execute("SELECT current_timestamp").fetchone()[0]
    t0 = time.perf_counter()
    before_rows = get_batch_row_count(con, target_table, params["batch_id"])
    status = "SUCCESS"
    error_message = None

    try:
        run_sql_file(sql_file, params, catalog_path)
    except Exception as ex:
        status = "FAILED"
        error_message = str(ex)

    after_rows = get_batch_row_count(con, target_table, params["batch_id"])
    end_ts = con.execute("SELECT current_timestamp").fetchone()[0]
    duration_ms = round((time.perf_counter() - t0) * 1000, 2)
    data_ingested_rows = max(after_rows - before_rows, 0)

    write_benchmark(
        con,
        config,
        {
            "run_id": run_id,
            "env": env,
            "batch_id": params["batch_id"],
            "raw_file": raw_file,
            "step_name": step_name,
            "sql_file": sql_file,
            "target_table": target_table,
            "data_ingested_rows": data_ingested_rows,
            "data_ingested_bytes": raw_file_size,
            "duration_ms": duration_ms,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "status": status,
            "error_message": error_message,
        },
    )

    print(
        f"  Benchmark -> rows: {data_ingested_rows}, "
        f"duration_ms: {duration_ms}, status: {status}"
    )

    if status == "FAILED":
        raise RuntimeError(f"Step failed for {sql_file}: {error_message}")


# ─────────────────────────────────────────
# Get raw parquet files
# ─────────────────────────────────────────
def get_raw_files(data_path: str) -> list:
    return sorted(glob.glob(f"{data_path}/**/*.parquet", recursive=True))


def get_raw_folders(data_path: str) -> list:
    return sorted(
        [
            path
            for path in glob.glob(f"{data_path}/**", recursive=True)
            if os.path.isdir(path)
        ]
    )


def get_unprocessed_folders(con, config: dict, raw_folders: list) -> list:
    if not raw_folders:
        return []

    cdc_table = f"{config['staging_schema']}.processed_files_cdc"
    result = con.execute(
        f"""
        SELECT DISTINCT raw_file
        FROM {cdc_table}
        WHERE status = 'SUCCESS'
        """
    ).fetchall()
    processed_folders = {os.path.dirname(row[0]) for row in result}
    return [folder for folder in raw_folders if folder not in processed_folders]


def build_duckdb_list_literal(file_paths: list) -> str:
    escaped = [p.replace("\\", "\\\\").replace("'", "''") for p in file_paths]
    return "[" + ", ".join(f"'{p}'" for p in escaped) + "]"


def mark_files_processed(
    con,
    config: dict,
    raw_files: list,
    run_id: str,
    batch_id: str,
    status: str,
):
    for raw_file in raw_files:
        mark_file_processed(
            con,
            config,
            raw_file,
            os.path.getsize(raw_file),
            run_id,
            batch_id,
            status,
        )


def chunk_files(file_paths: list, chunk_size: int) -> list:
    if chunk_size <= 0:
        return [file_paths]
    return [file_paths[i:i + chunk_size] for i in range(0, len(file_paths), chunk_size)]


# ─────────────────────────────────────────
# Run pipeline for a batch of raw files
# ─────────────────────────────────────────
def run_for_files(raw_files: list, config: dict, batch_id: str, run_id: str, env: str):
    if not raw_files:
        print("No files in current batch. Skipping.")
        return

    print(f"\n{'='*50}")
    print(f"Processing files: {len(raw_files)}")
    print(f"Batch ID:   {batch_id}")
    print(f"{'='*50}")

    raw_files_expr = build_duckdb_list_literal(raw_files)
    raw_file_label = f"MULTI_FILE_BATCH({len(raw_files)} files)"
    total_raw_file_size = sum(os.path.getsize(f) for f in raw_files)

    params = {
        **config,
        "batch_id": batch_id,
        "raw_files_expr": raw_files_expr,
    }

    catalog_path = config["catalog_path"]
    con = get_connection(catalog_path)

    try:
        # 1. Staging
        print("\n[1/5] Staging")
        run_step_with_benchmark(
            con,
            config,
            params,
            run_id,
            env,
            raw_file_label,
            total_raw_file_size,
            "staging",
            "sql/staging/stg_accelerometer.sql",
            f"{config['staging_schema']}.stg_accelerometer",
            catalog_path,
        )

        # 2. Curated - helper
        print("\n[2/5] Curated - Helper")
        run_step_with_benchmark(
            con,
            config,
            params,
            run_id,
            env,
            raw_file_label,
            total_raw_file_size,
            "curated_helper",
            "sql/curated/accel_location_hlp.sql",
            f"{config['curated_schema']}.location_hlp",
            catalog_path,
        )

        # 3. Curated - dim + fact
        print("\n[3/5] Curated - Dim & Fact")
        run_step_with_benchmark(
            con,
            config,
            params,
            run_id,
            env,
            raw_file_label,
            total_raw_file_size,
            "curated_location_dim",
            "sql/curated/accel_location_dim.sql",
            f"{config['curated_schema']}.location_dim",
            catalog_path,
        )
        run_step_with_benchmark(
            con,
            config,
            params,
            run_id,
            env,
            raw_file_label,
            total_raw_file_size,
            "curated_fact",
            "sql/curated/accel_fact.sql",
            f"{config['curated_schema']}.fact_accelerometer",
            catalog_path,
        )

        # 4. Marts - summary
        print("\n[4/5] Marts - Hourly Summary")
        run_step_with_benchmark(
            con,
            config,
            params,
            run_id,
            env,
            raw_file_label,
            total_raw_file_size,
            "marts_hourly_summary",
            "sql/marts/accel_summary_mart.sql",
            f"{config['marts_schema']}.accel_hourly_summary",
            catalog_path,
        )

        # 5. Marts - FFT
        print("\n[5/5] Marts - FFT")
        run_step_with_benchmark(
            con,
            config,
            params,
            run_id,
            env,
            raw_file_label,
            total_raw_file_size,
            "marts_fft",
            "sql/marts/accel_fft_mart.sql",
            f"{config['marts_schema']}.accel_fft_mart",
            catalog_path,
        )

        mark_files_processed(
            con,
            config,
            raw_files,
            run_id,
            batch_id,
            "SUCCESS",
        )
    except Exception:
        mark_files_processed(
            con,
            config,
            raw_files,
            run_id,
            batch_id,
            "FAILED",
        )
        raise

    print(f"\n✓ Done batch: {len(raw_files)} files")


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────
if __name__ == "__main__":
    env = sys.argv[1] if len(sys.argv) > 1 else "dev"
    print(f"Environment: {env}")

    config = load_config(env)
    run_id = str(uuid.uuid4())

    con = get_connection(config["catalog_path"])
    attach_ducklake(con, config)
    apply_duckdb_runtime_tuning(con, config)
    bootstrap_schemas(con, config)
    ensure_benchmark_table(con, config)
    ensure_file_cdc_table(con, config)

    # Register FFT UDFs
    print("\nRegistering UDFs...")
    register_fft_udfs(con)

    raw_folders = get_raw_folders(config["data_path"])
    raw_files = get_raw_files(config["data_path"])

    if not raw_folders and not raw_files:
        print("No raw folders or files found. Early exit.")
        close_connection()
        sys.exit(0)

    unprocessed_files = get_unprocessed_files(con, config, raw_files)
    unprocessed_folders = get_unprocessed_folders(con, config, raw_folders)
    if not unprocessed_folders and not unprocessed_files:
        print("No new raw folders or files to process (CDC). Early exit.")
        close_connection()
        sys.exit(0)

    print(f"\nFound {len(raw_folders)} raw folders.")
    print(f"Found {len(raw_files)} raw files.")
    print(f"New folders to process: {len(unprocessed_folders)}")
    print(f"New files to process: {len(unprocessed_files)}")

    mode = sys.argv[2] if len(sys.argv) > 2 else "all"

    if mode not in {"all", "backfill", "latest"}:
        print(f"Invalid mode: {mode}. Use one of: all, backfill, latest")
        close_connection()
        sys.exit(1)

    if mode == "latest":
        files_to_process = [unprocessed_files[-1]]
    else:
        # CDC already filters to only unprocessed files, so process all in this run.
        files_to_process = unprocessed_files

    max_files_per_batch = int(config.get("max_files_per_batch", "10"))
    file_batches = chunk_files(files_to_process, max_files_per_batch)
    print(f"Processing in {len(file_batches)} batch(es), max_files_per_batch={max_files_per_batch}")

    for batch_num, batch_files in enumerate(file_batches, start=1):
        print(f"\nStarting batch {batch_num}/{len(file_batches)}")
        run_for_files(
            batch_files,
            config,
            batch_id=str(uuid.uuid4())[:8],
            run_id=run_id,
            env=env,
        )

    close_connection()
    print("\nPipeline complete.")
