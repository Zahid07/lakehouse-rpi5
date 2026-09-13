"""Raspberry Pi 5 host metrics: sample, keep a trend, persist it.

Added because the interesting question is no longer "does the pipeline work"
but "what happens when several producers run at once". That is a question about
the *machine*, and none of the existing endpoints could answer it.

Everything is read straight from `/proc` and `/sys`. No `psutil`, which is not
installed and would be a dependency for arithmetic this file does in twenty
lines.

Three deliberate choices
------------------------
**A separate DuckDB file, never the DuckLake catalog.** The catalog is
single-attach -- one process may hold it and `DETACH` is the only release -- and
the pipeline holds it. Writing metrics there would put this sampler in direct
contention with the writer it exists to observe. `hostmetrics.duckdb` is its own
file with its own lock, so the two never meet.

**Batched flushes, with the file closed in between.** Samples accumulate in
memory and are written every `flush_seconds`. A persistent write connection
would hold the file lock for the server's whole lifetime, so
``duckdb ~/engine-lake/hostmetrics.duckdb`` from another terminal would fail --
and being able to query this history by hand is most of the point of storing it
in DuckDB rather than a CSV. The cost of a crash is one flush interval of
samples, which for metrics is nothing.

**Its own thread on a fixed cadence.** The catalog refresher runs when poked and
otherwise on a slow fallback, so sampling from it would make the trend sparse
and irregular exactly when the machine is busy -- which is when the trend
matters. This samples on a steady timer regardless of what the pipeline is doing.

The schema is deliberately flat and the timestamps are naive UTC, matching every
other table in this project::

    SELECT ts, cpu_pct, temp_c, mem_used_pct, producers
    FROM host_samples ORDER BY ts DESC LIMIT 20;
"""

from __future__ import annotations

import os
import re
import subprocess
import threading
import time

import duckdb
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROC_STAT = Path("/proc/stat")
PROC_MEM = Path("/proc/meminfo")
PROC_LOAD = Path("/proc/loadavg")
PROC_SWAPS = Path("/proc/swaps")
THERMAL = Path("/sys/class/thermal/thermal_zone0/temp")
CPUFREQ = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq")

#: Command-line fragments that mark a process as part of this project. Used only
#: to count how many producers/pipelines are running, which is the number the
#: resource trend has to be read against.
PROCESS_MARKERS = (
    ("producers", ("engine_producer", "engine_pipeline.producer")),
    ("pipelines", ("engine_pipeline.pipeline", "duckstream_pipeline.pipeline")),
    ("ingests", ("engine_pipeline.ingest", "duckstream_pipeline.ingest")),
)

#: `vcgencmd get_throttled` bits. The "has occurred" half is the valuable one:
#: it is sticky since boot, so a throttling event during a heavy run is still
#: visible afterwards rather than having to be caught live.
THROTTLE_BITS = {
    0: "under-voltage now",
    1: "arm frequency capped now",
    2: "currently throttled",
    3: "soft temperature limit now",
    16: "under-voltage has occurred",
    17: "arm frequency capped has occurred",
    18: "throttling has occurred",
    19: "soft temperature limit has occurred",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS host_samples (
    ts                TIMESTAMP,
    cpu_pct           DOUBLE,
    cpu_busiest_pct   DOUBLE,
    temp_c            DOUBLE,
    mem_used_pct      DOUBLE,
    mem_available_mb  DOUBLE,
    swap_used_mb      DOUBLE,
    swap_total_mb     DOUBLE,
    load1             DOUBLE,
    freq_mhz          DOUBLE,
    throttled         INTEGER,
    producers         INTEGER,
    pipelines         INTEGER,
    ingests           INTEGER,
    active_machines   INTEGER
)
"""

COLUMNS = ("ts", "cpu_pct", "cpu_busiest_pct", "temp_c", "mem_used_pct",
           "mem_available_mb", "swap_used_mb", "swap_total_mb", "load1", "freq_mhz",
           "throttled", "producers", "pipelines", "ingests", "active_machines")


def _read(path: Path) -> str | None:
    try:
        return path.read_text()
    except OSError:
        return None


def _cpu_counters() -> dict[str, tuple[int, int]]:
    """`{name: (busy, total)}` for the aggregate and each core.

    iowait counts as idle, not busy. A process blocked on the SD card is not
    consuming the CPU, and counting it as load would make disk pressure look
    like compute pressure -- the opposite diagnosis.
    """
    text = _read(PROC_STAT)
    if not text:
        return {}
    out: dict[str, tuple[int, int]] = {}
    for line in text.splitlines():
        if not line.startswith("cpu"):
            continue
        parts = line.split()
        name = parts[0]
        try:
            values = [int(v) for v in parts[1:11]]
        except ValueError:
            continue
        if len(values) < 5:
            continue
        idle = values[3] + values[4]          # idle + iowait
        total = sum(values)
        out[name] = (total - idle, total)
    return out


def _memory() -> dict[str, float]:
    text = _read(PROC_MEM)
    if not text:
        return {}
    kb: dict[str, float] = {}
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        try:
            kb[key] = float(rest.strip().split()[0])
        except (IndexError, ValueError):
            continue
    total = kb.get("MemTotal", 0.0)
    avail = kb.get("MemAvailable", 0.0)
    swap_total = kb.get("SwapTotal", 0.0)
    swap_free = kb.get("SwapFree", 0.0)
    return {
        # MemAvailable, not MemFree. Free excludes the page cache, which the
        # kernel will hand back on demand, so it reads as near-exhausted on a
        # perfectly healthy machine -- and this pipeline reads a lot of parquet,
        # so its cache is large by design.
        "mem_used_pct": round(100.0 * (1.0 - avail / total), 2) if total else None,
        "mem_available_mb": round(avail / 1024.0, 1) if avail else None,
        "swap_used_mb": round((swap_total - swap_free) / 1024.0, 1),
        "swap_total_mb": round(swap_total / 1024.0, 1),
    }


def _swap_info() -> dict:
    """Swap totals, and crucially WHERE it lives.

    On most Pi installs swap is `/dev/zram0`: a compressed block device held in
    RAM. Pages are compressed rather than written to the SD card, so using some
    costs CPU, not disk I/O, and wears nothing out. That is an ordinary healthy
    state -- the kernel squeezing cold pages instead of evicting them.

    Swap on an SD card is a different story entirely: hundreds of times slower
    than RAM, and a process touching a swapped-out page stalls hard. Reporting
    both the same way turns a normal reading into a false alarm, or a real
    problem into a shrug. `/proc/swaps` names the device, so tell them apart.
    """
    text = _read(PROC_SWAPS)
    if not text:
        return {}
    devices, total_kb, used_kb = [], 0.0, 0.0
    for line in text.splitlines()[1:]:          # skip the header
        parts = line.split()
        if len(parts) < 4:
            continue
        try:
            total_kb += float(parts[2])
            used_kb += float(parts[3])
        except ValueError:
            continue
        devices.append(parts[0])
    if not devices:
        return {"swap_total_mb": 0.0, "swap_used_mb": 0.0,
                "swap_compressed": False, "swap_devices": []}
    return {
        "swap_total_mb": round(total_kb / 1024.0, 1),
        "swap_used_mb": round(used_kb / 1024.0, 1),
        # Only "compressed" when EVERY device is zram. A mixed setup can still
        # page to the card, so the slow one decides.
        "swap_compressed": all("zram" in d for d in devices),
        "swap_devices": devices,
    }


def _temperature() -> float | None:
    raw = _read(THERMAL)
    if raw:
        try:
            return round(int(raw.strip()) / 1000.0, 2)
        except ValueError:
            pass
    return None


def _frequency_mhz() -> float | None:
    raw = _read(CPUFREQ)
    if raw:
        try:
            return round(int(raw.strip()) / 1000.0, 1)
        except ValueError:
            pass
    return None


def _load1() -> float | None:
    raw = _read(PROC_LOAD)
    if raw:
        try:
            return float(raw.split()[0])
        except (IndexError, ValueError):
            pass
    return None


_THROTTLE_RE = re.compile(r"0x([0-9a-fA-F]+)")


def _throttled() -> int | None:
    """`vcgencmd get_throttled`, or None where vcgencmd is absent.

    The only subprocess here. It costs a few milliseconds and runs once per
    sample, which at a 5 s cadence is irrelevant -- and under-voltage is a
    genuine failure mode on a Pi driving several producers.
    """
    try:
        out = subprocess.run(["vcgencmd", "get_throttled"], capture_output=True,
                             text=True, timeout=2.0)
    except (OSError, subprocess.SubprocessError):
        return None
    m = _THROTTLE_RE.search(out.stdout or "")
    return int(m.group(1), 16) if m else None


def decode_throttled(value: int | None) -> list[str]:
    """Bitmask -> human labels. Empty list means healthy."""
    if not value:
        return []
    return [label for bit, label in THROTTLE_BITS.items() if value & (1 << bit)]


def _process_counts() -> dict[str, int]:
    """How many producers/pipelines/ingests are running right now.

    The resource trend is close to meaningless without it: 80% CPU is alarming
    under one producer and expected under four, and this is the column that
    tells the two apart.
    """
    counts = {name: 0 for name, _ in PROCESS_MARKERS}
    try:
        entries = os.listdir("/proc")
    except OSError:
        return counts
    for entry in entries:
        if not entry.isdigit():
            continue
        try:
            # `comm` first, and it is not an optimisation. Matching on cmdline
            # alone counts any process whose command line merely *mentions* the
            # marker -- most obviously the shell running a script that starts
            # producers, whose argv contains the whole script text. That made a
            # two-producer test report four. A producer is always a Python
            # process, so requiring that removes the whole class.
            with open(f"/proc/{entry}/comm", "rb") as fh:
                comm = fh.read().strip().decode("utf-8", "replace")
            if not comm.startswith("python"):
                continue
            with open(f"/proc/{entry}/cmdline", "rb") as fh:
                cmdline = fh.read().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            continue
        if not cmdline:
            continue
        for name, markers in PROCESS_MARKERS:
            if any(marker in cmdline for marker in markers):
                counts[name] += 1
                break
    return counts


class HostMetrics:
    """Samples the host, keeps a ring in memory, persists batches to DuckDB."""

    def __init__(self, db_path: Path, *, interval: float = 5.0,
                 flush_seconds: float = 30.0, retain_hours: float = 72.0,
                 ring: int = 4320) -> None:
        self.db_path = Path(db_path)
        self.interval = max(1.0, interval)
        self.flush_seconds = max(self.interval, flush_seconds)
        self.retain_hours = retain_hours
        # 4320 samples at 5 s is six hours of trend held in memory, which is
        # what the page can ask for without touching the file at all.
        self.ring: deque = deque(maxlen=ring)
        self._pending: list[tuple] = []
        self._prev_cpu: dict[str, tuple[int, int]] = {}
        self._lock = threading.Lock()
        self._last_flush = 0.0
        self.writes = 0
        self.write_error: str | None = None
        self.db_ready = False
        # Producers usually run on another machine -- a laptop publishing over
        # MQTT -- so counting local processes reports 0 for the thing the
        # operator actually wants to know. Whoever reads the landing tree sets
        # this to the number of machines that have published recently, which is
        # the honest measure of "how many producers are feeding this Pi" and is
        # independent of where they run.
        self._active_machines = 0

    def set_active_machines(self, n: int) -> None:
        """Machines seen publishing recently. Set from the catalog refresher.

        Updated on the refresher's cadence, not the sampler's, so it can lag a
        sample or two behind a producer starting or stopping. That is fine for
        a trend and is why it is recorded as its own column rather than being
        blended into the local process count.
        """
        with self._lock:
            self._active_machines = max(0, int(n))

    # -- sampling ----------------------------------------------------------

    def sample(self) -> dict | None:
        """One sample. Returns None on the very first call.

        CPU percentage is a *rate*, so it needs two readings of `/proc/stat`
        to exist at all. Returning a fabricated 0.0 for the first one would put
        a false idle point at the start of every trend.
        """
        counters = _cpu_counters()
        prev, self._prev_cpu = self._prev_cpu, counters
        if not prev or not counters:
            return None

        def pct(name: str) -> float | None:
            if name not in prev or name not in counters:
                return None
            busy0, total0 = prev[name]
            busy1, total1 = counters[name]
            dt = total1 - total0
            return round(100.0 * (busy1 - busy0) / dt, 2) if dt > 0 else None

        cores = [p for name in counters if name != "cpu"
                 for p in (pct(name),) if p is not None]
        mem = _memory()
        # /proc/swaps is authoritative and also says where swap lives; meminfo
        # is the fallback for a kernel that does not expose it.
        mem.update({k: v for k, v in _swap_info().items()
                    if k in ("swap_total_mb", "swap_used_mb")})
        counts = _process_counts()
        row = {
            "ts": datetime.now(timezone.utc).replace(tzinfo=None,
                                                     microsecond=0),
            "cpu_pct": pct("cpu"),
            "cpu_busiest_pct": round(max(cores), 2) if cores else None,
            "temp_c": _temperature(),
            "mem_used_pct": mem.get("mem_used_pct"),
            "mem_available_mb": mem.get("mem_available_mb"),
            "swap_used_mb": mem.get("swap_used_mb"),
            "swap_total_mb": mem.get("swap_total_mb"),
            "load1": _load1(),
            "freq_mhz": _frequency_mhz(),
            "throttled": _throttled(),
            "producers": counts.get("producers", 0),
            "pipelines": counts.get("pipelines", 0),
            "ingests": counts.get("ingests", 0),
            "active_machines": self._active_machines,
        }
        with self._lock:
            self.ring.append(row)
            self._pending.append(tuple(row[c] for c in COLUMNS))
        return row

    # -- persistence -------------------------------------------------------

    def flush(self, force: bool = False) -> int:
        """Write pending samples. Opens and closes the file each time."""
        now = time.monotonic()
        if not force and now - self._last_flush < self.flush_seconds:
            return 0
        with self._lock:
            batch, self._pending = self._pending, []
        self._last_flush = now
        if not batch:
            return 0
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            con = duckdb.connect(str(self.db_path))
            try:
                con.execute(SCHEMA)
                # A file written before `active_machines` existed keeps its old
                # shape -- CREATE TABLE IF NOT EXISTS is a no-op on it, and the
                # INSERT would then fail forever on column count. Add anything
                # missing rather than requiring the file be deleted.
                have = {r[1] for r in con.execute(
                    "PRAGMA table_info('host_samples')").fetchall()}
                for column, sqltype in (("active_machines", "INTEGER"),
                                        ("swap_total_mb", "DOUBLE")):
                    if column not in have:
                        con.execute(f"ALTER TABLE host_samples "
                                    f"ADD COLUMN {column} {sqltype}")
                con.executemany(
                    f"INSERT INTO host_samples ({', '.join(COLUMNS)}) "
                    f"VALUES ({', '.join('?' * len(COLUMNS))})", batch)
                if self.retain_hours and self.retain_hours > 0:
                    # The cutoff is computed HERE, in naive UTC, and bound as a
                    # parameter. DuckDB's now() is TIMESTAMP WITH TIME ZONE and
                    # now()::TIMESTAMP renders it in **local** time, so on this
                    # Pi (UTC+5) that cutoff sat five hours ahead of every row
                    # and the first flush deleted the entire table. Nothing
                    # surfaced it: the in-memory ring still drew the chart and
                    # rows_written still climbed, so the page looked correct
                    # while persisting nothing. Keeping the comparison in
                    # Python removes the timezone question from the SQL.
                    cutoff = (datetime.now(timezone.utc).replace(tzinfo=None)
                              - timedelta(hours=float(self.retain_hours)))
                    con.execute(
                        "DELETE FROM host_samples WHERE ts < ?", [cutoff])
            finally:
                con.close()
            self.writes += len(batch)
            self.write_error = None
            self.db_ready = True
            return len(batch)
        except Exception as exc:  # noqa: BLE001
            # Never let a metrics write kill the sampler. Losing the trend is a
            # nuisance; taking the dashboard down with it would not be.
            self.write_error = f"{type(exc).__name__}: {str(exc)[:160]}"
            with self._lock:
                # Put them back, but bounded -- an unwritable disk must not
                # grow this list without limit.
                self._pending = (batch + self._pending)[-5000:]
            return 0

    # -- reading -----------------------------------------------------------

    def series(self, minutes: float = 30.0, points: int = 240) -> dict:
        """The in-memory trend, evenly thinned to at most `points` samples."""
        cutoff = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - timedelta(minutes=minutes))
        with self._lock:
            rows = [r for r in self.ring if r["ts"] >= cutoff]
        if len(rows) > points:
            # Stride rather than average: a mean would smooth away the spikes,
            # and a spike is the entire reason to look at a resource trend.
            step = len(rows) / points
            rows = [rows[min(len(rows) - 1, int(i * step))] for i in range(points)]
        latest = rows[-1] if rows else (self.ring[-1] if self.ring else None)
        return {
            "samples": [
                {**r, "ts": r["ts"].isoformat(sep=" ", timespec="seconds")}
                for r in rows
            ],
            "latest": ({**latest, "ts": latest["ts"].isoformat(sep=" ",
                                                               timespec="seconds")}
                       if latest else None),
            "throttle_flags": decode_throttled(latest["throttled"]) if latest else [],
            "interval_seconds": self.interval,
            "retain_hours": self.retain_hours,
            "db_path": str(self.db_path),
            "db_ready": self.db_ready,
            "rows_written": self.writes,
            "write_error": self.write_error,
            "cores": len([k for k in self._prev_cpu if k != "cpu"]),
            "swap": {k: v for k, v in _swap_info().items()
                     if k in ("swap_compressed", "swap_devices", "swap_total_mb")},
        }

    # -- the thread --------------------------------------------------------

    def run(self, stop: threading.Event) -> None:
        """Sample on a steady cadence until `stop` is set."""
        self.sample()                      # prime the CPU delta
        while not stop.is_set():
            if stop.wait(self.interval):
                break
            try:
                self.sample()
                self.flush()
            except Exception:              # noqa: BLE001
                pass
        self.flush(force=True)             # do not lose the last batch
