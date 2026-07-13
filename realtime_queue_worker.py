import argparse
import glob
import os
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import fcntl

import run_pipeline as rp


def ensure_realtime_queue_table(con, config: dict):
    queue_table = f"{config['staging_schema']}.realtime_job_queue"
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {queue_table} (
            file_path VARCHAR PRIMARY KEY,
            folder_path VARCHAR,
            file_size_bytes BIGINT,
            file_mtime_ns BIGINT,
            status VARCHAR,
            attempts INTEGER,
            next_retry_ts TIMESTAMP,
            last_error VARCHAR,
            last_run_id VARCHAR,
            created_ts TIMESTAMP,
            updated_ts TIMESTAMP
        )
        """
    )


def get_ready_folders(data_path: str, ready_marker_name: str, settle_seconds: int) -> list:
    now = datetime.now().timestamp()
    ready_folders = []

    for folder in glob.glob(f"{data_path}/**", recursive=True):
        if not os.path.isdir(folder):
            continue

        marker = os.path.join(folder, ready_marker_name)
        if not os.path.exists(marker):
            continue

        marker_age = now - os.path.getmtime(marker)
        if marker_age >= settle_seconds:
            ready_folders.append(folder)

    return sorted(ready_folders)


def get_files_from_folders(folders: list) -> list:
    files = []
    for folder in folders:
        files.extend(glob.glob(f"{folder}/**/*.parquet", recursive=True))
    return sorted(files)


def upsert_queue_jobs(con, config: dict, file_paths: list):
    queue_table = f"{config['staging_schema']}.realtime_job_queue"

    for file_path in file_paths:
        stat = os.stat(file_path)
        folder_path = str(Path(file_path).parent)

        con.execute(
            f"""
            MERGE INTO {queue_table} AS tgt
            USING (
                SELECT
                    ? AS file_path,
                    ? AS folder_path,
                    ? AS file_size_bytes,
                    ? AS file_mtime_ns
            ) AS src
            ON tgt.file_path = src.file_path
            WHEN MATCHED AND (
                tgt.file_size_bytes <> src.file_size_bytes
                OR tgt.file_mtime_ns <> src.file_mtime_ns
            ) THEN
                UPDATE SET
                    folder_path = src.folder_path,
                    file_size_bytes = src.file_size_bytes,
                    file_mtime_ns = src.file_mtime_ns,
                    status = 'QUEUED',
                    attempts = 0,
                    next_retry_ts = current_timestamp,
                    last_error = NULL,
                    updated_ts = current_timestamp
            WHEN NOT MATCHED THEN
                INSERT (
                    file_path,
                    folder_path,
                    file_size_bytes,
                    file_mtime_ns,
                    status,
                    attempts,
                    next_retry_ts,
                    last_error,
                    last_run_id,
                    created_ts,
                    updated_ts
                )
                VALUES (
                    src.file_path,
                    src.folder_path,
                    src.file_size_bytes,
                    src.file_mtime_ns,
                    'QUEUED',
                    0,
                    current_timestamp,
                    NULL,
                    NULL,
                    current_timestamp,
                    current_timestamp
                )
            """,
            [file_path, folder_path, stat.st_size, stat.st_mtime_ns],
        )


def get_due_files(con, config: dict, ready_folders: set, max_retry_attempts: int, max_files_per_run: int) -> list:
    queue_table = f"{config['staging_schema']}.realtime_job_queue"

    rows = con.execute(
        f"""
        SELECT file_path, folder_path
        FROM {queue_table}
        WHERE status IN ('QUEUED', 'FAILED')
          AND attempts < ?
          AND COALESCE(next_retry_ts, current_timestamp) <= current_timestamp
        ORDER BY updated_ts ASC
        LIMIT ?
        """,
        [max_retry_attempts, max_files_per_run],
    ).fetchall()

    due = []
    for file_path, folder_path in rows:
        if folder_path not in ready_folders:
            continue
        if not os.path.exists(file_path):
            continue
        due.append(file_path)

    return due


def mark_batch_running(con, config: dict, file_paths: list, run_id: str):
    queue_table = f"{config['staging_schema']}.realtime_job_queue"
    for file_path in file_paths:
        con.execute(
            f"""
            UPDATE {queue_table}
            SET status = 'RUNNING',
                last_run_id = ?,
                updated_ts = current_timestamp
            WHERE file_path = ?
            """,
            [run_id, file_path],
        )


def mark_batch_success(con, config: dict, file_paths: list, run_id: str):
    queue_table = f"{config['staging_schema']}.realtime_job_queue"
    for file_path in file_paths:
        con.execute(
            f"""
            UPDATE {queue_table}
            SET status = 'SUCCESS',
                last_error = NULL,
                last_run_id = ?,
                updated_ts = current_timestamp
            WHERE file_path = ?
            """,
            [run_id, file_path],
        )


def mark_batch_failed(
    con,
    config: dict,
    file_paths: list,
    run_id: str,
    error_message: str,
    retry_delay_seconds: int,
):
    queue_table = f"{config['staging_schema']}.realtime_job_queue"

    for file_path in file_paths:
        attempts = con.execute(
            f"SELECT COALESCE(attempts, 0) FROM {queue_table} WHERE file_path = ?",
            [file_path],
        ).fetchone()[0]

        next_attempt = int(attempts) + 1
        backoff_seconds = retry_delay_seconds * (2 ** min(next_attempt - 1, 6))
        next_retry = datetime.now() + timedelta(seconds=backoff_seconds)

        con.execute(
            f"""
            UPDATE {queue_table}
            SET status = 'FAILED',
                attempts = ?,
                next_retry_ts = ?,
                last_error = ?,
                last_run_id = ?,
                updated_ts = current_timestamp
            WHERE file_path = ?
            """,
            [next_attempt, next_retry, error_message[:2000], run_id, file_path],
        )


def chunk_files(file_paths: list, chunk_size: int) -> list:
    if chunk_size <= 0:
        return [file_paths]
    return [file_paths[i:i + chunk_size] for i in range(0, len(file_paths), chunk_size)]


def run_worker(env: str):
    config = rp.load_config(env)
    run_id = str(uuid.uuid4())

    ready_marker_name = config.get("ready_marker_name", "_READY")
    settle_seconds = int(config.get("worker_settle_seconds", "60"))
    max_files_per_batch = int(config.get("max_files_per_batch", "10"))
    max_files_per_run = int(config.get("worker_max_files_per_run", "500"))
    max_retry_attempts = int(config.get("worker_max_retry_attempts", "10"))
    retry_delay_seconds = int(config.get("worker_retry_delay_seconds", "120"))

    con = rp.get_connection(config["catalog_path"])
    rp.attach_ducklake(con, config)
    rp.apply_duckdb_runtime_tuning(con, config)
    rp.bootstrap_schemas(con, config)
    rp.ensure_benchmark_table(con, config)
    rp.ensure_file_cdc_table(con, config)
    ensure_realtime_queue_table(con, config)

    print("\nRegistering UDFs...")
    rp.register_fft_udfs(con)

    ready_folders = get_ready_folders(config["data_path"], ready_marker_name, settle_seconds)
    if not ready_folders:
        print("No ready folders found. Early exit.")
        rp.close_connection()
        return 0

    folder_files = get_files_from_folders(ready_folders)
    if not folder_files:
        print("Ready folders exist but no parquet files found. Early exit.")
        rp.close_connection()
        return 0

    unprocessed_files = rp.get_unprocessed_files(con, config, folder_files)
    if unprocessed_files:
        upsert_queue_jobs(con, config, unprocessed_files)

    due_files = get_due_files(
        con,
        config,
        set(ready_folders),
        max_retry_attempts=max_retry_attempts,
        max_files_per_run=max_files_per_run,
    )

    if not due_files:
        print("No due files in queue. Early exit.")
        rp.close_connection()
        return 0

    batches = chunk_files(due_files, max_files_per_batch)
    print(f"Found {len(ready_folders)} ready folder(s), {len(due_files)} due file(s), {len(batches)} batch(es).")

    failed_batches = 0

    for idx, batch in enumerate(batches, start=1):
        batch_id = str(uuid.uuid4())[:8]
        print(f"\nRunning batch {idx}/{len(batches)} with {len(batch)} file(s)")

        mark_batch_running(con, config, batch, run_id)

        try:
            rp.run_for_files(batch, config, batch_id=batch_id, run_id=run_id, env=env)
            mark_batch_success(con, config, batch, run_id)
        except Exception as ex:
            failed_batches += 1
            mark_batch_failed(con, config, batch, run_id, str(ex), retry_delay_seconds)
            print(f"Batch {idx} failed and was queued for retry: {ex}")

    rp.close_connection()

    if failed_batches:
        print(f"\nWorker finished with {failed_batches} failed batch(es).")
        return 1

    print("\nWorker finished successfully.")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Realtime mini-batch queue worker")
    parser.add_argument("env", nargs="?", default="dev", help="Config environment name (dev/prod)")
    parser.add_argument(
        "--lock-file",
        default=".realtime_worker.lock",
        help="Path to worker lock file to prevent overlapping runs",
    )
    args = parser.parse_args()

    lock_path = Path(args.lock_file)
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with open(lock_path, "w", encoding="utf-8") as lock_fp:
        try:
            fcntl.flock(lock_fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Another worker instance is already running. Early exit.")
            sys.exit(0)

        exit_code = run_worker(args.env)
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
