"""Report the two numbers the phase-6 soak owes, and append them to a trend file.

`PLAN.md` phase 6 names them and `CONTEXT.md` says why neither is known:

* **the state store past 6,000 rows.** 1.3 measured a DuckLake state table flat
  to 6,000 rows and attached its own caveat -- *"only tested to 6,000 state
  rows; if state reaches millions of open windows, re-measure and add eviction
  of sealed windows"*. State grows two rows per committed trigger, three with a
  horizon, and nothing calls `prune()` (phase-4 maintenance is meant to schedule
  it and does not yet), so a trigger a minute reaches 1.3's ceiling in about two
  days and keeps going.
* **the open-window accumulator, never measured at all.** Only a windowed
  `append` model with a lateness horizon builds one. It is bounded by the
  horizon rather than by the age of the stream, which is a *claim*: what this
  checks is that it fills and drains repeatedly rather than climbing.

A third thing worth watching that no document asks for: the **landing tree**.
1.20's "what is still not measured" is the absolute scan cost at a year of
files, and it says to measure it on the target in phase 6's soak or not at all.
The scan is linear in ready files and paid on every trigger including idle ones,
so the file count and the measured scan time both belong in the trend.

Reads only. It attaches the catalog read-only-ish and never writes to it -- note
that `duckstream status` does *not* have that property (it calls `ensure`), which
is a known open item.

Usage::

    DUCKSTREAM_SOAK=/path python tools/soak/check.py                # one sample
    DUCKSTREAM_SOAK=/path python tools/soak/check.py --csv trend.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

STATE_TABLES = ["offsets", "batches", "watermarks", "consumed_files", "quarantine"]


def attach(catalog: Path):
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute(f"ATTACH 'ducklake:{catalog.as_posix()}' AS lake")
    con.execute("SET temp_directory=''")     # 1.24
    return con


def count(con, table: str) -> int | None:
    try:
        return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    except Exception:
        return None


def list_tables(con, schema: str) -> list[str]:
    try:
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = 'lake' AND table_schema = ?", [schema]
        ).fetchall()
        return sorted(r[0] for r in rows)
    except Exception:
        return []


def scan_cost(landing: Path) -> tuple[int, float]:
    """Files the source would consider, and what walking them costs right now.

    Uses the real `FileSource._scan`, not a reimplementation, so the number is
    the one the engine actually pays on every trigger.
    """
    from duckstream import FileSource

    source = FileSource(landing, marker="_READY")
    t0 = time.perf_counter()
    found = source._scan()
    elapsed = (time.perf_counter() - t0) * 1000.0
    return len(found), elapsed


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.environ.get("DUCKSTREAM_SOAK"))
    ap.add_argument("--csv", default=None)
    args = ap.parse_args()
    if not args.root:
        raise SystemExit("set DUCKSTREAM_SOAK or pass --root")

    root = Path(args.root)
    catalog = root / "catalog.ducklake"
    landing = root / "landing"

    sample: dict[str, object] = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    files, scan_ms = scan_cost(landing)
    sample["landing_files"] = files
    sample["scan_ms"] = round(scan_ms, 2)

    if not catalog.exists():
        print("no catalog yet; the soak has not committed anything")
        print(f"landing: {files} ready file(s), scan {scan_ms:.1f} ms")
        return

    con = attach(catalog)

    print(f"{'state table':<28} {'rows':>10}")
    print("-" * 40)
    total_state = 0
    for name in STATE_TABLES:
        n = count(con, f"lake.duckstream.{name}")
        sample[f"state_{name}"] = n
        if n is not None:
            total_state += n
            print(f"  duckstream.{name:<24} {n:>8}")
    sample["state_total"] = total_state
    print(f"  {'TOTAL':<26} {total_state:>8}   <- CONTEXT.md 1.3 measured to 6,000")
    print()

    marts = list_tables(con, "marts")
    accumulators = [t for t in marts if t.endswith("__open_windows")]
    print(f"{'mart':<34} {'rows':>8}")
    print("-" * 46)
    for t in marts:
        n = count(con, f'lake.marts."{t}"')
        label = t + ("   <- accumulator" if t.endswith("__open_windows") else "")
        print(f"  {label:<32} {n:>8}")
        if t.endswith("__open_windows"):
            sample[f"accum_{t}"] = n
    if not accumulators:
        print("  (no open-window accumulator -- no windowed `append` model has "
              "committed yet)")
    print()

    # Snapshots: one per committed trigger, which is the exactly-once primitive
    # (1.4). Growth here is also what `expire_snapshots` would later bound.
    try:
        snaps = con.execute(
            "SELECT count(*) FROM ducklake_snapshots('lake')"
        ).fetchone()[0]
        sample["snapshots"] = snaps
        print(f"snapshots: {snaps}")
    except Exception as exc:
        print(f"snapshots: unavailable ({type(exc).__name__})")

    print(f"landing:   {files} ready file(s), scan {scan_ms:.1f} ms "
          f"({scan_ms * 1000 / files:.1f} us/file)" if files else
          f"landing:   0 ready files")

    if args.csv:
        path = Path(args.csv)
        new = not path.exists()
        with path.open("a", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(sample))
            if new:
                writer.writeheader()
            writer.writerow(sample)
        print(f"appended to {path}")


if __name__ == "__main__":
    main()
