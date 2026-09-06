"""Synthetic landing feed for the phase-6 soak.

`PLAN.md` phase 6 asks for a soak run measured in days, on real hardware, and
names the two numbers it owes: the state store was only ever measured to 6,000
rows (`CONTEXT.md` 1.3's own caveat) and **the open-window accumulator has never
been measured at all**. Neither is reachable in a session; both need a stream
that keeps arriving.

This writes one small drop per invocation, so cron can drive it at whatever
cadence the soak wants. It is deliberately *not* a daemon: the deployment shape
`PLAN.md` describes is cron opening, doing a little work and exiting (1.6 --
while one process holds a DuckDB file nothing else can open it, even read-only),
and the feed should look like the pipeline it feeds.

**Trap 7 is the whole of the write path.** A landing drop must become visible
atomically: write to a temp name, `os.replace` it, and only *then* drop the
completion marker. A fixture that appends to an already-planned file produces a
genuine double count that reads exactly like an engine bug, and phase 5's
`LandingWriter` makes the same guarantee for the same reason.

Usage::

    python tools/soak/feed.py --landing soak/landing --rows 600
    python tools/soak/feed.py --landing soak/landing --rows 600 --late-every 20

`--late-every N` makes every Nth drop carry rows stamped *behind* the stream's
present, which is what exercises the lateness horizon, the watermark and -- when
a window finally seals -- the accumulator this soak exists to measure.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

MARKER = "_READY"
STATE = ".feed_state.json"


def _load_state(root: Path) -> dict:
    path = root / STATE
    if path.exists():
        try:
            return json.loads(path.read_text())
        except (OSError, ValueError):
            pass
    return {"seq": 0}


def _save_state(root: Path, state: dict) -> None:
    # Same discipline as the drops themselves: never leave a half-written file
    # where the next invocation will read it as authoritative.
    path = root / STATE
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state))
    os.replace(tmp, path)


def make_rows(start: datetime, rows: int, hz: int, sensors: list[str],
              rng: random.Random) -> dict:
    step = timedelta(seconds=1.0 / hz)
    ts, loc, xs, ys, zs = [], [], [], [], []
    for sensor in sensors:
        phase = rng.random() * 6.283
        for i in range(rows):
            t = start + step * i
            ts.append(t)
            loc.append(sensor)
            # A signal with real structure, so an FFT over it is not noise and a
            # wrong window is visible as a wrong spectrum rather than as noise
            # that happens to differ.
            xs.append(0.9 * __import__("math").sin(phase + i / 12.0) + rng.gauss(0, 0.02))
            ys.append(0.4 * __import__("math").cos(phase + i / 30.0) + rng.gauss(0, 0.02))
            zs.append(1.0 + rng.gauss(0, 0.01))
    return {"timestamp": ts, "location": loc, "x": xs, "y": ys, "z": zs}


def write_drop(root: Path, columns: dict, seq: int) -> Path:
    """One drop, atomically: temp file, rename, **then** the marker (trap 7)."""
    directory = root / f"drop{seq:08d}"
    directory.mkdir(parents=True, exist_ok=False)

    table = pa.table({
        "timestamp": pa.array(columns["timestamp"], type=pa.timestamp("us")),
        "location": pa.array(columns["location"], type=pa.string()),
        "x": pa.array(columns["x"], type=pa.float64()),
        "y": pa.array(columns["y"], type=pa.float64()),
        "z": pa.array(columns["z"], type=pa.float64()),
    })

    final = directory / "data.parquet"
    tmp = directory / "data.parquet.partial"
    pq.write_table(table, tmp)
    os.replace(tmp, final)          # visible only once complete
    (directory / MARKER).write_bytes(b"")   # and only now is it readable
    return directory


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", required=True)
    ap.add_argument("--rows", type=int, default=600,
                    help="rows per sensor per drop (600 at 100 Hz = 6 s)")
    ap.add_argument("--hz", type=int, default=100)
    ap.add_argument("--sensors", default="soak_a,soak_b")
    ap.add_argument("--late-every", type=int, default=0,
                    help="every Nth drop is stamped behind the stream (0 = never)")
    ap.add_argument("--late-by", default="90 seconds")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    root = Path(args.landing)
    root.mkdir(parents=True, exist_ok=True)
    state = _load_state(root)
    seq = int(state.get("seq", 0))
    rng = random.Random(args.seed if args.seed is not None else seq)
    sensors = [s.strip() for s in args.sensors.split(",") if s.strip()]

    span = timedelta(seconds=args.rows / args.hz)
    start = datetime.now().replace(microsecond=0) - span
    late = args.late_every and seq and seq % args.late_every == 0
    if late:
        amount = float(args.late_by.split()[0])
        start = start - timedelta(seconds=amount)

    columns = make_rows(start, args.rows, args.hz, sensors, rng)
    directory = write_drop(root, columns, seq)

    state["seq"] = seq + 1
    state["last_written"] = time.time()
    _save_state(root, state)

    print(f"{directory.name}: {args.rows * len(sensors)} rows, "
          f"{start:%Y-%m-%d %H:%M:%S} +{span.total_seconds():.0f}s"
          f"{'  [LATE]' if late else ''}")


if __name__ == "__main__":
    main()
