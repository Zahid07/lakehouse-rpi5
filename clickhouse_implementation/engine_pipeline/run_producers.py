#!/usr/bin/env python3
"""Run a whole experiment's producers on a schedule, from one command.

Starts producer.py once per machine at its planned offset, using the SAME
Python interpreter that runs this script (so run it with the environment you
already use for producer.py). Each producer gets --seconds computed so that
every producer ends at the same moment, the end of the run.

Default plan (for the "effect of adding producers" analysis): one producer
every 30 minutes up to 6, then all 6 until the 6-hour mark.

    python run_producers.py --run-id ch_run1                         # ClickHouse
    python run_producers.py --run-id duck_run1 --topic engine/vibration_duck   # DuckDB

Other plans:
    --hours 2 --plan "Karachi_ENG01@0,Karachi_ENG02@13m,Karachi_ENG03@19m"
    --plan "Karachi_ENG01@0,Karachi_ENG02@30m:bearing"      # per-machine mode
    --delay 10m        # wait before the first producer (e.g. for an idle baseline)

The RabbitMQ password comes from RMQ_PASSWORD or one prompt, and reaches the
producers through the environment, never the command line. Each producer's
output goes to <run_id>_logs/<machine>.log. Ctrl+C stops every producer.
Standard library only; works on Windows and Linux.
"""

from __future__ import annotations

import argparse
import getpass
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
PRODUCER = HERE / "producer.py"
DEFAULT_PLAN = ",".join(f"Karachi_ENG0{i + 1}@{30 * i}m" for i in range(6))


def duration(text: str) -> float:
    """'90' / '90s' -> 90, '13m' -> 780, '1.5h' -> 5400 (seconds)."""
    m = re.fullmatch(r"\s*([\d.]+)\s*([smh]?)\s*", text)
    if not m:
        raise argparse.ArgumentTypeError(f"not a duration: {text!r} (use e.g. 90s, 13m, 1.5h)")
    return float(m.group(1)) * {"": 1, "s": 1, "m": 60, "h": 3600}[m.group(2)]


def parse_plan(text: str) -> list[tuple[str, float, str | None]]:
    """'NAME@OFFSET[:MODE],...' -> [(name, offset_seconds, mode or None)]."""
    plan = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        name, _, rest = part.partition("@")
        offset, _, mode = rest.partition(":")
        if not name or not offset:
            raise SystemExit(f"bad plan entry {part!r}; expected NAME@OFFSET[:MODE], e.g. Karachi_ENG02@13m")
        plan.append((name.strip(), duration(offset), mode.strip() or None))
    plan.sort(key=lambda p: p[1])
    if len({p[0] for p in plan}) != len(plan):
        raise SystemExit("each machine may appear only once in --plan")
    return plan


def utc(t: datetime) -> str:
    return t.strftime("%H:%M:%S UTC")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run-id", required=True, help="experiment label, e.g. ch_run1 or duck_run1")
    ap.add_argument("--hours", type=float, default=6.0, help="run length from the first producer (default 6)")
    ap.add_argument("--plan", default=DEFAULT_PLAN, help=f"NAME@OFFSET[:MODE],... (default: {DEFAULT_PLAN})")
    ap.add_argument("--delay", type=duration, default=0.0, help="wait before the first producer, e.g. 10m")
    ap.add_argument("--host", default=os.environ.get("ENG_MQTT_HOST", "192.168.0.104"))
    ap.add_argument("--topic", default="engine/vibration",
                    help="engine/vibration for ClickHouse, engine/vibration_duck for DuckDB")
    ap.add_argument("--user", default=os.environ.get("RMQ_USER", "iot"))
    ap.add_argument("--producer-args", default="", help="extra arguments for every producer, quoted")
    args = ap.parse_args()

    if not PRODUCER.exists() or not (HERE / "engine_signal.py").exists():
        raise SystemExit(f"producer.py and engine_signal.py must be next to this script ({HERE})")
    # The producers run with THIS interpreter. Check it has their packages now,
    # rather than letting each producer die on import, unnoticed, hours apart.
    check = subprocess.run([sys.executable, "-c", "import paho.mqtt, numpy"],
                           capture_output=True, text=True)
    if check.returncode != 0:
        raise SystemExit(f"this Python cannot run the producers:\n  {sys.executable}\n  "
                         f"{(check.stderr.strip().splitlines() or ['?'])[-1]}\n"
                         f"Activate the environment you use for producer.py, or install the "
                         f"packages: {sys.executable} -m pip install paho-mqtt numpy")
    plan = parse_plan(args.plan)
    total = args.hours * 3600
    for name, offset, _ in plan:
        if offset >= total:
            raise SystemExit(f"{name} starts at {offset / 60:g} min, after the run ends ({args.hours:g} h)")

    password = os.environ.get("RMQ_PASSWORD") or getpass.getpass(f"RabbitMQ password for {args.user}: ")
    child_env = {**os.environ, "RMQ_PASSWORD": password, "RMQ_USER": args.user}
    logs = HERE / f"{args.run_id}_logs"
    logs.mkdir(exist_ok=True)

    t0 = datetime.now(timezone.utc) + timedelta(seconds=args.delay)
    end = t0 + timedelta(seconds=total)
    print(f"python   : {sys.executable}")
    print(f"run      : {args.run_id}  topic {args.topic}  host {args.host}")
    print(f"schedule : first producer {utc(t0)}, all end {utc(end)} ({args.hours:g} h)")
    for name, offset, mode in plan:
        print(f"  {utc(t0 + timedelta(seconds=offset))}  {name:16} --seconds {round(total - offset)}"
              + (f"  mode {mode}" if mode else ""))
    print(f"logs     : {logs}\n")

    procs: dict[str, subprocess.Popen] = {}
    files = []

    def stop_all(reason: str) -> None:
        running = {n: p for n, p in procs.items() if p.poll() is None}
        if not running:
            return
        print(f"\n{reason}: stopping {len(running)} producer(s)...")
        for p in running.values():
            try:
                p.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT)
            except (OSError, ValueError):
                p.terminate()
        deadline = time.monotonic() + 15
        for p in running.values():
            try:
                p.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                p.kill()

    try:
        for name, offset, mode in plan:
            start_at = t0 + timedelta(seconds=offset)
            while datetime.now(timezone.utc) < start_at:
                time.sleep(min(1.0, (start_at - datetime.now(timezone.utc)).total_seconds()))
                for n, p in procs.items():         # report early exits while waiting
                    if p.poll() is not None and not getattr(p, "_reported", False):
                        p._reported = True
                        print(f"{utc(datetime.now(timezone.utc))}  {n} EXITED early (code {p.returncode}) "
                              f"-- see {logs / (n + '.log')}")
            remaining = (end - datetime.now(timezone.utc)).total_seconds()
            cmd = [sys.executable, "-u", str(PRODUCER), "--host", args.host, "--topic", args.topic,
                   "--user", args.user, "--machine", name, "--run-id", args.run_id,
                   "--seconds", str(round(remaining))]
            if mode:
                cmd += ["--mode", mode]
            cmd += args.producer_args.split()
            log = open(logs / f"{name}.log", "a", buffering=1)
            files.append(log)
            flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
            procs[name] = subprocess.Popen(cmd, cwd=HERE, env=child_env, stdout=log,
                                           stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                           creationflags=flags)
            print(f"{utc(datetime.now(timezone.utc))}  started {name} (pid {procs[name].pid}, "
                  f"{round(remaining)} s){' mode ' + mode if mode else ''}")

        # All started: wait for them to finish, reporting each as it ends.
        while any(p.poll() is None for p in procs.values()):
            time.sleep(2)
            for n, p in procs.items():
                if p.poll() is not None and not getattr(p, "_reported", False):
                    p._reported = True
                    print(f"{utc(datetime.now(timezone.utc))}  {n} finished (code {p.returncode})")
    except KeyboardInterrupt:
        stop_all("Ctrl+C")
    finally:
        stop_all("exiting")
        for f in files:
            f.close()

    print("\nsummary (last line of each producer log):")
    for name in procs:
        lines = (logs / f"{name}.log").read_text(errors="replace").strip().splitlines()
        print(f"  {name:16} code {procs[name].returncode}: {lines[-1] if lines else '(no output)'}")
    return 0 if all(p.returncode == 0 for p in procs.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
