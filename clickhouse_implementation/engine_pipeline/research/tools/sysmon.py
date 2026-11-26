#!/usr/bin/env python3
"""Neutral system monitor for comparing the two pipeline implementations.

Writes one CSV row every --interval seconds, with the same columns whichever
implementation is running, so ClickHouse and DuckDB runs are measured by the
same instrument (the two dashboards' own samplers differ, and would add
their own load to the comparison).

Per sample:
  host        CPU % (mean and busiest core), load averages, temperature, clock,
              throttling flags, memory and swap, SD-card read/write, network
  components  CPU % (of one core) and resident memory of each part of either
              stack: clickhouse, rabbitmq, mosquitto, duck_pipeline, duck_ingest,
              stft_udf, ch_dashboard, duck_dashboard, producers_local
  broker      producers connected (MQTT clients from other machines), messages
              waiting in all queues, publish and deliver rates (RabbitMQ API)
  storage     bytes on disk: ClickHouse `engine` database (via HTTP, if
              CH_PASSWORD is set) and any --path given (e.g. the DuckDB lake)

Standard library only. Run on the Pi, e.g.:

    python3 sysmon.py --run-id ch_6p_24h --implementation clickhouse \\
        --out ch_6p_24h.csv
    ENG_ROOT=~/engine-lake-audit python3 sysmon.py --run-id duck_6p_24h \\
        --implementation duckdb --path ~/engine-lake-audit --out duck_6p_24h.csv

Environment: RMQ_API (default http://localhost:15672/api), RMQ_USER /
RMQ_PASSWORD (default guest/guest, which RabbitMQ allows from localhost),
CH_URL (default http://localhost:8123), CH_USER, CH_PASSWORD.
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

CLK_TCK = os.sysconf("SC_CLK_TCK")
PAGE_KB = os.sysconf("SC_PAGE_SIZE") // 1024

COMPONENTS = ("clickhouse", "rabbitmq", "mosquitto", "duck_pipeline", "duck_ingest",
              "stft_udf", "ch_dashboard", "duck_dashboard", "producers_local")


def classify(pid: str) -> str | None:
    """Which component a process belongs to, or None."""
    try:
        comm = Path(f"/proc/{pid}/comm").read_text().strip()
    except OSError:
        return None
    if comm.startswith("clickhouse"):
        return "clickhouse"
    if comm in ("beam.smp", "erl_child_setup", "epmd", "inet_gethost"):
        return "rabbitmq"
    if comm == "mosquitto":
        return "mosquitto"
    # ClickHouse starts the UDF through its #! line, so the process is named
    # after the script, not after python -- and matching by name (rather than
    # by "stft_db.py" appearing in some command line) avoids counting any
    # process that merely mentions the file.
    if comm == "stft_db.py":
        return "stft_udf"
    if not comm.startswith("python"):
        return None
    try:
        cmd = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
    except OSError:
        return None
    if "sysmon.py" in cmd:
        return None
    if "engine_pipeline.pipeline" in cmd:
        return "duck_pipeline"
    if "engine_pipeline.ingest" in cmd:
        return "duck_ingest"
    if "producer.py" in cmd or "engine_pipeline.producer" in cmd:
        return "producers_local"
    if "server.py" in cmd:
        try:
            cwd = os.readlink(f"/proc/{pid}/cwd")
        except OSError:
            cwd = ""
        where = cmd + " " + cwd
        if "engine_app" in where:
            return "duck_dashboard"
        if "engine_pipeline/app" in where or "clickhouse_implementation" in where:
            return "ch_dashboard"
    return None


def process_ticks() -> dict[str, tuple[str, int, int]]:
    """{pid: (component, cpu_ticks, rss_kb)} for every process we care about."""
    out = {}
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        comp = classify(pid)
        if comp is None:
            continue
        try:
            stat = Path(f"/proc/{pid}/stat").read_text()
            fields = stat[stat.rindex(")") + 2:].split()
            ticks = int(fields[11]) + int(fields[12])          # utime + stime
            rss_kb = int(fields[21]) * PAGE_KB
        except (OSError, ValueError, IndexError):
            continue
        out[pid] = (comp, ticks, rss_kb)
    return out


def cpu_counters() -> dict[str, tuple[int, int]]:
    out = {}
    for line in Path("/proc/stat").read_text().splitlines():
        if line.startswith("cpu"):
            parts = line.split()
            vals = [int(v) for v in parts[1:11]]
            idle = vals[3] + vals[4]
            out[parts[0]] = (sum(vals) - idle, sum(vals))
    return out


def meminfo() -> dict[str, float]:
    kb = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, _, rest = line.partition(":")
        try:
            kb[key] = float(rest.split()[0])
        except (IndexError, ValueError):
            pass
    total, avail = kb.get("MemTotal", 0), kb.get("MemAvailable", 0)
    return {"mem_used_pct": round(100 * (1 - avail / total), 2) if total else None,
            "mem_available_mb": round(avail / 1024, 1),
            "swap_used_mb": round((kb.get("SwapTotal", 0) - kb.get("SwapFree", 0)) / 1024, 1)}


def disk_sectors() -> tuple[int, int]:
    """Sectors read/written on whole disks (mmcblk*, nvme*, sd*), not partitions."""
    rd = wr = 0
    for line in Path("/proc/diskstats").read_text().splitlines():
        p = line.split()
        if len(p) > 9 and re.fullmatch(r"mmcblk\d+|nvme\d+n\d+|sd[a-z]", p[2]):
            rd += int(p[5]); wr += int(p[9])
    return rd, wr


def net_bytes() -> tuple[int, int]:
    rx = tx = 0
    for line in Path("/proc/net/dev").read_text().splitlines()[2:]:
        name, _, rest = line.partition(":")
        if name.strip() == "lo":
            continue
        p = rest.split()
        rx += int(p[0]); tx += int(p[8])
    return rx, tx


def read_float(path: str, scale: float) -> float | None:
    try:
        return round(int(Path(path).read_text().strip()) / scale, 2)
    except (OSError, ValueError):
        return None


def throttled() -> str | None:
    try:
        out = subprocess.run(["vcgencmd", "get_throttled"], capture_output=True, text=True, timeout=2).stdout
        m = re.search(r"0x[0-9a-fA-F]+", out)
        return m.group(0) if m else None
    except (OSError, subprocess.SubprocessError):
        return None


def rabbitmq() -> dict:
    api = os.environ.get("RMQ_API", "http://localhost:15672/api")
    token = base64.b64encode(f"{os.environ.get('RMQ_USER', 'guest')}:"
                             f"{os.environ.get('RMQ_PASSWORD', 'guest')}".encode()).decode()

    def get(path):
        req = urllib.request.Request(api + path, headers={"Authorization": f"Basic {token}"})
        with urllib.request.urlopen(req, timeout=3) as r:
            return json.loads(r.read())
    try:
        conns = get("/connections")
        queues = get("/queues")
        overview = get("/overview")
    except (OSError, ValueError):
        return {}
    remote_mqtt = [c for c in conns if "MQTT" in c.get("protocol", "")
                   and c.get("peer_host") not in ("127.0.0.1", "::1", "localhost")]
    stats = overview.get("message_stats", {})
    return {
        "producers_connected": len(remote_mqtt),
        "rmq_messages_waiting": sum(q.get("messages", 0) or 0 for q in queues),
        "rmq_publish_rate": round(stats.get("publish_details", {}).get("rate", 0.0), 1),
        "rmq_deliver_rate": round(stats.get("deliver_get_details", {}).get("rate", 0.0), 1),
        "rmq_recv_kbps": round(sum(c.get("recv_oct_details", {}).get("rate", 0.0) for c in remote_mqtt) / 1024, 1),
    }


def clickhouse_bytes() -> int | None:
    if "CH_PASSWORD" not in os.environ:
        return None
    url = os.environ.get("CH_URL", "http://localhost:8123")
    q = "SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'engine'"
    req = urllib.request.Request(
        f"{url}/?{urllib.parse.urlencode({'query': q})}",
        headers={"X-ClickHouse-User": os.environ.get("CH_USER", "default"),
                 "X-ClickHouse-Key": os.environ["CH_PASSWORD"]})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return int(r.read().strip() or 0)
    except (OSError, ValueError):
        return None


def path_bytes(paths: list[str]) -> int | None:
    if not paths:
        return None
    total = 0
    for root in paths:
        for dirpath, _dirs, files in os.walk(os.path.expanduser(root)):
            for f in files:
                try:
                    total += os.lstat(os.path.join(dirpath, f)).st_size
                except OSError:
                    pass
    return total


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", required=True, help="CSV file (appended to)")
    ap.add_argument("--run-id", required=True, help="experiment label, same as the producers' --run-id")
    ap.add_argument("--implementation", required=True, choices=("clickhouse", "duckdb", "idle"))
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--duration", type=float, default=None, help="stop after this many seconds")
    ap.add_argument("--path", action="append", default=[], help="directory whose size to record (repeatable)")
    ap.add_argument("--storage-every", type=int, default=12, help="measure storage every N samples")
    args = ap.parse_args()

    columns = (["ts_utc", "run_id", "implementation", "producers_connected",
                "cpu_pct", "cpu_busiest_pct", "load1", "load5", "load15", "temp_c", "freq_mhz",
                "throttled", "mem_used_pct", "mem_available_mb", "swap_used_mb",
                "disk_read_kBps", "disk_write_kBps", "net_rx_kBps", "net_tx_kBps"]
               + [f"{c}_{m}" for c in COMPONENTS for m in ("cpu_pct", "rss_mb", "procs")]
               + ["rmq_messages_waiting", "rmq_publish_rate", "rmq_deliver_rate", "rmq_recv_kbps",
                  "clickhouse_bytes", "path_bytes"])
    new_file = not Path(args.out).exists() or Path(args.out).stat().st_size == 0
    fh = open(args.out, "a", newline="")
    writer = csv.DictWriter(fh, fieldnames=columns)
    if new_file:
        writer.writeheader()

    prev_cpu, prev_proc = cpu_counters(), process_ticks()
    prev_disk, prev_net, prev_t = disk_sectors(), net_bytes(), time.monotonic()
    started, n = prev_t, 0
    storage = {"clickhouse_bytes": None, "path_bytes": None}
    print(f"sysmon: run {args.run_id} ({args.implementation}) every {args.interval:g}s -> {args.out}")
    try:
        while args.duration is None or time.monotonic() - started < args.duration:
            time.sleep(max(0.0, args.interval - (time.monotonic() - prev_t)))
            now = time.monotonic(); dt = now - prev_t
            cpu, procs = cpu_counters(), process_ticks()
            disk, net = disk_sectors(), net_bytes()

            def pct(name):
                b0, t0 = prev_cpu.get(name, (0, 0)); b1, t1 = cpu.get(name, (0, 0))
                return round(100 * (b1 - b0) / (t1 - t0), 2) if t1 > t0 else None
            cores = [p for k in cpu if k != "cpu" for p in [pct(k)] if p is not None]
            load = Path("/proc/loadavg").read_text().split()
            row = {"ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                   "run_id": args.run_id, "implementation": args.implementation,
                   "cpu_pct": pct("cpu"), "cpu_busiest_pct": max(cores) if cores else None,
                   "load1": load[0], "load5": load[1], "load15": load[2],
                   "temp_c": read_float("/sys/class/thermal/thermal_zone0/temp", 1000),
                   "freq_mhz": read_float("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq", 1000),
                   "throttled": throttled(),
                   "disk_read_kBps": round((disk[0] - prev_disk[0]) * 0.5 / dt, 1),
                   "disk_write_kBps": round((disk[1] - prev_disk[1]) * 0.5 / dt, 1),
                   "net_rx_kBps": round((net[0] - prev_net[0]) / 1024 / dt, 1),
                   "net_tx_kBps": round((net[1] - prev_net[1]) / 1024 / dt, 1)}
            row.update(meminfo())
            agg = {c: [0.0, 0, 0] for c in COMPONENTS}          # cpu ticks, rss kb, procs
            for pid, (comp, ticks, rss) in procs.items():
                before = prev_proc.get(pid)
                agg[comp][0] += ticks - before[1] if before and before[0] == comp else 0
                agg[comp][1] += rss
                agg[comp][2] += 1
            for comp, (ticks, rss, count) in agg.items():
                row[f"{comp}_cpu_pct"] = round(100 * ticks / CLK_TCK / dt, 1)
                row[f"{comp}_rss_mb"] = round(rss / 1024, 1)
                row[f"{comp}_procs"] = count
            row.update(rabbitmq())
            if n % args.storage_every == 0:
                storage = {"clickhouse_bytes": clickhouse_bytes(), "path_bytes": path_bytes(args.path)}
            row.update(storage)
            writer.writerow(row); fh.flush()
            prev_cpu, prev_proc, prev_disk, prev_net, prev_t = cpu, procs, disk, net, now
            n += 1
    except KeyboardInterrupt:
        pass
    finally:
        fh.close()
        print(f"sysmon: {n} samples written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
