#!/usr/bin/env python3
"""Tiny Raspberry Pi 5 monitor: serves index.html and a /api/stats JSON endpoint.

No third-party dependencies. Run: python3 server.py [port]
"""
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8080


def read_cpu_times():
    """Return {cpu_name: (idle, total)} from /proc/stat."""
    times = {}
    with open("/proc/stat") as f:
        for line in f:
            if not line.startswith("cpu"):
                break
            parts = line.split()
            vals = list(map(int, parts[1:]))
            idle = vals[3] + vals[4]  # idle + iowait
            times[parts[0]] = (idle, sum(vals))
    return times


# Background sampler so every request gets a fresh, accurate CPU %.
_cpu_usage = {}
_lock = threading.Lock()


def cpu_sampler(interval=1.0):
    prev = read_cpu_times()
    while True:
        time.sleep(interval)
        cur = read_cpu_times()
        usage = {}
        for name, (idle, total) in cur.items():
            p_idle, p_total = prev.get(name, (idle, total))
            d_total = total - p_total
            usage[name] = round(100.0 * (1 - (idle - p_idle) / d_total), 1) if d_total else 0.0
        with _lock:
            _cpu_usage.clear()
            _cpu_usage.update(usage)
        prev = cur


def read_temp():
    try:
        with open("/sys/class/thermal/thermal_zone0/temp") as f:
            return round(int(f.read()) / 1000, 1)
    except OSError:
        return None


def read_memory():
    info = {}
    with open("/proc/meminfo") as f:
        for line in f:
            key, val = line.split(":")
            info[key] = int(val.split()[0])  # kB
    total = info["MemTotal"]
    available = info.get("MemAvailable", info["MemFree"])
    used = total - available
    return {
        "total_mb": round(total / 1024),
        "used_mb": round(used / 1024),
        "available_mb": round(available / 1024),
        "percent": round(100 * used / total, 1),
    }


def read_cpu_freq():
    try:
        with open("/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq") as f:
            return round(int(f.read()) / 1000)  # MHz
    except OSError:
        return None


def read_uptime():
    with open("/proc/uptime") as f:
        return int(float(f.read().split()[0]))


def app_name(pid, comm):
    """Friendly app name so related processes (e.g. VS Code's node workers) group together."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            cmd = f.read().replace(b"\0", b" ").decode(errors="replace")
    except OSError:
        cmd = ""
    # Check Claude/Copilot first: their binaries live inside ~/.vscode-server/extensions.
    if comm == "claude" or "claude-code" in cmd:
        return "Claude Code"
    if "copilot" in comm:
        return "GitHub Copilot"
    if ".vscode-server" in cmd or comm.startswith("code-"):
        return "VS Code Server"
    if comm.startswith("xdg-desktop-por"):
        return "Desktop portals"
    if comm in ("labwc", "wf-panel-pi", "pcmanfm", "lxsession", "wayvnc", "squeekboard"):
        return "Desktop (" + comm + ")"
    if comm == "python3" and "server.py" in cmd:
        return "Pi monitor"
    return comm


def read_top_processes(limit=8):
    """Resident memory (RSS) grouped by app, largest first."""
    groups = {}
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/status") as f:
                fields = dict(line.split(":", 1) for line in f if ":" in line)
        except OSError:
            continue  # process exited
        rss = fields.get("VmRSS")
        if not rss:
            continue  # kernel thread
        name = app_name(pid, fields["Name"].strip())
        g = groups.setdefault(name, {"name": name, "rss_mb": 0.0, "count": 0})
        g["rss_mb"] += int(rss.split()[0]) / 1024
        g["count"] += 1
    top = sorted(groups.values(), key=lambda g: g["rss_mb"], reverse=True)[:limit]
    for g in top:
        g["rss_mb"] = round(g["rss_mb"])
    return top


def get_stats():
    with _lock:
        usage = dict(_cpu_usage)
    return {
        "top_processes": read_top_processes(),
        "cpu_percent": usage.get("cpu", 0.0),
        "cpu_cores": [usage[k] for k in sorted(usage) if k != "cpu"],
        "cpu_freq_mhz": read_cpu_freq(),
        "temp_c": read_temp(),
        "memory": read_memory(),
        "load_avg": list(os.getloadavg()),
        "uptime_s": read_uptime(),
        "timestamp": time.time(),
    }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/stats":
            body = json.dumps(get_stats()).encode()
            ctype = "application/json"
        elif self.path in ("/", "/index.html"):
            with open(os.path.join(HERE, "index.html"), "rb") as f:
                body = f.read()
            ctype = "text/html; charset=utf-8"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # keep the console quiet


if __name__ == "__main__":
    threading.Thread(target=cpu_sampler, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Pi monitor running at http://0.0.0.0:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
