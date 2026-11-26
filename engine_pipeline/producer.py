"""The engine simulator. **This is the script you run.**

Publishes one JSON reading per MQTT message at 500 Hz, or writes parquet
straight to a landing tree with `--landing` (no broker at all, for benchmarking
the pipeline in isolation).

    ./engine_pipeline/run_producer.sh                       # healthy, forever
    ./engine_pipeline/run_producer.sh --mode bearing
    ./engine_pipeline/run_producer.sh --schedule "healthy:120,imbalance:90,misalignment:90,bearing:120,looseness:90"

The schedule loops forever and is what makes an unattended demo worth watching:
all five signatures and four transitions inside about eight minutes.

Switching mode while it runs
----------------------------
Four ways, all cheap:

* **control file** (primary) -- ``echo bearing > $ENG_CONTROL_FILE``. Polled by
  one ``os.stat`` per block and re-read only when mtime changes, so it costs
  nothing. Works over ssh, survives a producer restart, needs no dependency. A
  second line may carry ``rpm=1500``.
* ``--mode <name>`` -- the starting mode.
* ``--schedule "mode:seconds,..."`` -- loops forever.
* ``SIGUSR1`` -- advance to the next mode.

Every switch **cross-fades over `--transition` seconds** (default 4). A step
change in amplitude is a discontinuity that sprays broadband energy across one
STFT frame -- a bright vertical stripe that reads as a fault -- and a gradient is
more legible on a spectrogram than a hard edge anyway.

Timing
------
Sample timestamps are ``t0 + n/fs`` exactly, so the STFT's uniform-sampling
assumption is true rather than approximately true. The wall clock is only
consulted to decide *when to sleep*, never to stamp a sample, and sleeps use an
**absolute** deadline so error cannot accumulate. Timestamps follow the
monotonic clock from the start instant, so they do NOT follow later wall-clock
changes: if the system clock is stepped during a run (e.g. an NTP resync),
timestamps carry on unchanged while `sent_at` jumps. Sync the clock BEFORE
starting a run. (An earlier version of this note promised a resync on drift;
no such code exists.)

Audit fields (for the ClickHouse comparison): each message also carries
`run_id` (--run-id / ENG_RUN_ID) and `sent_at`, the wall clock (UTC) when it
was published. RabbitMQ requires a login: --user/--password or
ENG_MQTT_USER / ENG_MQTT_PASSWORD (prompted for if missing).

Batching: readings are still sampled at --rate (500 Hz), but --batch of them
(default 50 = one 100 ms block) travel in one MQTT message, one JSON reading
per line; the in-flight window is 10,000 messages. This matches the ClickHouse
comparison's producer (per-message confirmations capped one-reading messages
near 400 msg/s on the Pi). --batch 1 restores one reading per message.

`seq` is a monotonic per-run sample index. It costs eight bytes and it is what
makes "are we actually achieving 500 Hz" an exact measurement rather than a
guess: `verify --check rate` computes loss as
``1 - count(*)/(max(seq)-min(seq)+1)``.
"""

from __future__ import annotations

import argparse
import getpass
import json
import math
import os
import signal as signal_module
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from engine_pipeline.signal import MODES, PROFILES, EngineSignal, blend  # noqa: E402

BLOCK_SECONDS = 0.1          # generate and publish in 100 ms bursts


def parse_schedule(text: str) -> list[tuple[str, float]]:
    """``"healthy:120,bearing:90"`` -> [("healthy",120.0), ("bearing",90.0)]."""
    out: list[tuple[str, float]] = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        name, _, secs = part.partition(":")
        name = name.strip()
        if name not in MODES:
            raise SystemExit(
                f"unknown mode {name!r} in --schedule; expected one of "
                f"{', '.join(MODES)}"
            )
        out.append((name, float(secs or 60)))
    if not out:
        raise SystemExit("--schedule parsed to nothing")
    return out


class ModeController:
    """Decides which profile is in force, and cross-fades between them."""

    def __init__(self, start: str, control_file: Path | None,
                 schedule: list[tuple[str, float]] | None,
                 transition: float) -> None:
        self.current = start
        self.target = start
        self.transition = max(0.0, transition)
        self._fade_left = 0.0
        self.control_file = control_file
        self.schedule = schedule
        self._sched_i = 0
        self._sched_left = schedule[0][1] if schedule else 0.0
        self._mtime: float | None = None
        self.rpm_override: float | None = None
        self.switches = 0

    def request(self, mode: str) -> None:
        if mode not in MODES or mode == self.target:
            return
        self.current = self.blended_name()
        self.target = mode
        self._fade_left = self.transition
        self.switches += 1
        print(f"  mode -> {mode}"
              f"{'' if self.transition <= 0 else f' (fading {self.transition:g}s)'}",
              flush=True)

    def blended_name(self) -> str:
        return self.target if self._fade_left <= 0 else self.current

    def _poll_control_file(self) -> None:
        if self.control_file is None:
            return
        try:
            mtime = self.control_file.stat().st_mtime
        except OSError:
            return
        if mtime == self._mtime:
            return
        self._mtime = mtime
        try:
            lines = self.control_file.read_text().split()
        except OSError:
            return
        for token in lines:
            if token in MODES:
                self.request(token)
            elif token.startswith("rpm="):
                try:
                    self.rpm_override = float(token[4:])
                    print(f"  rpm -> {self.rpm_override:g}", flush=True)
                except ValueError:
                    pass

    def advance(self, dt: float):
        """Move time forward; return the profile in force for this block."""
        self._poll_control_file()

        if self.schedule and self._fade_left <= 0:
            self._sched_left -= dt
            if self._sched_left <= 0:
                self._sched_i = (self._sched_i + 1) % len(self.schedule)
                name, secs = self.schedule[self._sched_i]
                self._sched_left = secs
                self.request(name)

        if self._fade_left > 0:
            self._fade_left = max(0.0, self._fade_left - dt)
            w = 1.0 - (self._fade_left / self.transition) if self.transition else 1.0
            return blend(PROFILES[self.current], PROFILES[self.target], w)
        self.current = self.target
        return PROFILES[self.target]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default=os.environ.get("ENG_MQTT_HOST", "localhost"))
    parser.add_argument("--port", type=int,
                        default=int(os.environ.get("ENG_MQTT_PORT", "1883")))
    parser.add_argument("--topic",
                        default=os.environ.get("ENG_MQTT_TOPIC", "engine/vibration"))
    parser.add_argument("--machine",
                        default=os.environ.get("ENG_MACHINE", "Karachi_ENG01"))
    parser.add_argument("--rate", type=float,
                        default=float(os.environ.get("ENG_SAMPLE_RATE_HZ", "500")))
    parser.add_argument("--rpm", type=float,
                        default=float(os.environ.get("ENG_NOMINAL_RPM", "1800")))
    parser.add_argument("--mode", default="healthy", choices=list(MODES))
    parser.add_argument("--schedule", default=None,
                        help='e.g. "healthy:120,imbalance:90,bearing:120" — loops')
    parser.add_argument("--transition", type=float, default=4.0,
                        help="cross-fade seconds between modes (default 4)")
    parser.add_argument("--rpm-profile", default="drift",
                        choices=("steady", "drift", "steps"))
    parser.add_argument("--seconds", type=float, default=None,
                        help="stop after this long (default: run forever)")
    parser.add_argument("--seed", type=int, default=12345)
    parser.add_argument("--landing", default=None, metavar="DIR",
                        help="write parquet chunks here instead of publishing to "
                             "MQTT — for benchmarking the pipeline with no broker")
    parser.add_argument("--landing-seconds", type=float, default=10.0,
                        help="with --landing, seconds of data per chunk")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--batch", type=int, default=int(os.environ.get("ENG_BATCH", "50")),
                        help="readings per MQTT message (default 50; 1 = unbatched)")
    parser.add_argument("--user", default=os.environ.get("ENG_MQTT_USER", "iot"))
    parser.add_argument("--password", default=os.environ.get("ENG_MQTT_PASSWORD"))
    parser.add_argument("--run-id", default=os.environ.get("ENG_RUN_ID")
                        or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ"),
                        help="experiment label carried in every message (audit)")
    args = parser.parse_args(argv)
    if not args.landing and args.user and args.password is None:
        args.password = getpass.getpass(f"MQTT password for {args.user}: ")

    control = os.environ.get("ENG_CONTROL_FILE")
    control_path = Path(control) if control else None
    if control_path is not None:
        control_path.parent.mkdir(parents=True, exist_ok=True)

    schedule = parse_schedule(args.schedule) if args.schedule else None
    start_mode = schedule[0][0] if schedule else args.mode
    modes = ModeController(start_mode, control_path, schedule, args.transition)

    signal_module.signal(
        signal_module.SIGUSR1,
        lambda *_: modes.request(MODES[(MODES.index(modes.target) + 1) % len(MODES)]),
    )

    engine = EngineSignal(fs=args.rate, nominal_rpm=args.rpm, seed=args.seed,
                          rpm_profile=args.rpm_profile)

    sink = _LandingSink(Path(args.landing), args.machine, args.rate,
                        args.landing_seconds) if args.landing else \
        _MqttSink(args.host, args.port, args.topic, args.user, args.password,
                  batch=max(1, args.batch), rate=args.rate)

    print(f"machine : {args.machine}")
    print(f"rate    : {args.rate:g} Hz   nominal {args.rpm:g} RPM "
          f"(shaft {args.rpm/60:.1f} Hz)")
    print(f"mode    : {start_mode}"
          + (f"   schedule {args.schedule}" if schedule else ""))
    if control_path:
        print(f"control : echo bearing > {control_path}")
    print(f"sink    : {sink.describe()}")
    print(f"run_id  : {args.run_id}")
    print()

    block_n = max(1, int(round(args.rate * BLOCK_SECONDS)))
    # Start on the next whole second, so sample timestamps stay on round
    # boundaries AND are the true generation times. (The original truncated
    # the current time to the second, stamping every reading up to 1 s
    # earlier than it was made -- which read as up to 1 s of false delay.)
    now_wall = datetime.now(timezone.utc).replace(tzinfo=None)
    t0 = now_wall.replace(microsecond=0) + timedelta(seconds=1)
    time.sleep((t0 - now_wall).total_seconds())
    started = time.monotonic()
    seq = 0
    emitted = 0
    last_report = started

    try:
        while True:
            if args.seconds is not None and engine.elapsed >= args.seconds:
                break
            if modes.rpm_override is not None:
                engine.nominal_rpm = modes.rpm_override
                modes.rpm_override = None

            profile = modes.advance(block_n / args.rate)
            radial, axial, rpm = engine.block(block_n, profile)
            label = modes.blended_name()

            for i in range(block_n):
                # Exact: never the wall clock, so the STFT's uniform sampling
                # assumption is true rather than nearly true.
                ts = t0 + timedelta(seconds=(seq + i) / args.rate)
                sink.emit({
                    "timestamp": ts.isoformat(sep=" ", timespec="microseconds"),
                    "machine": args.machine,
                    "seq": seq + i,
                    "vib_r": round(float(radial[i]), 6),
                    "vib_a": round(float(axial[i]), 6),
                    "rpm": round(float(rpm[i]), 2),
                    "mode": label,
                    "run_id": args.run_id,
                })
            seq += block_n
            emitted += block_n
            sink.flush_if_due()

            # Absolute deadline: error cannot accumulate the way it does with
            # a cumulative sleep.
            deadline = started + seq / args.rate
            delay = deadline - time.monotonic()
            if delay > 0:
                time.sleep(delay)

            now = time.monotonic()
            if not args.quiet and now - last_report >= 10.0:
                achieved = emitted / (now - last_report)
                print(f"  {label:<13} {achieved:6.1f} msg/s  "
                      f"rpm {rpm[-1]:6.1f}  seq {seq:,}"
                      f"{'  BEHIND' if achieved < args.rate * 0.97 else ''}",
                      flush=True)
                emitted = 0
                last_report = now
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        sink.close()
        print(f"emitted {seq:,} readings, {modes.switches} mode change(s)")
    return 0


class _MqttSink:
    """Readings batched into QoS 1 MQTT messages, one JSON reading per line.

    QoS **must** be >= 1: the broker delivers at min(publish, subscribe) qos, so
    publishing at 0 would silently defeat the ingest side's whole acknowledgement
    discipline. Each message is stamped with `sent_at` when it is published.
    A publish refused because paho's local buffer (5 minutes of readings) is
    full is counted in `dropped` rather than lost silently.
    """

    def __init__(self, host: str, port: int, topic: str,
                 user: str | None = None, password: str | None = None,
                 batch: int = 50, rate: float = 500.0) -> None:
        import paho.mqtt.client as mqtt

        self.topic = topic
        self.batch = batch
        self.dropped = 0
        self._pending: list[dict] = []
        self._err_queue_size = mqtt.MQTT_ERR_QUEUE_SIZE
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if user:
            self.client.username_pw_set(user, password)
        self.client.max_inflight_messages_set(10_000)
        self.client.max_queued_messages_set(max(1000, int(300 * rate / batch)))
        self.client.connect(host, port, keepalive=60)
        self.client.loop_start()
        self._host, self._port = host, port

    def describe(self) -> str:
        return (f"mqtt {self._host}:{self._port} topic {self.topic!r} qos 1, "
                f"{self.batch} reading(s) per message")

    def emit(self, record: dict) -> None:
        self._pending.append(record)
        if len(self._pending) >= self.batch:
            self._publish()

    def _publish(self) -> None:
        if not self._pending:
            return
        records, self._pending = self._pending, []
        sent_at = (datetime.now(timezone.utc).replace(tzinfo=None)
                   .isoformat(sep=" ", timespec="microseconds"))
        payload = "\n".join(json.dumps({**r, "sent_at": sent_at}) for r in records)
        if self.client.publish(self.topic, payload, qos=1).rc == self._err_queue_size:
            self.dropped += len(records)
            print(f"  DROPPED {len(records)} readings (local buffer full; "
                  f"{self.dropped:,} so far)", flush=True)

    def flush_if_due(self) -> None:
        pass

    def close(self) -> None:
        self._publish()
        time.sleep(1.0)
        self.client.loop_stop()
        self.client.disconnect()


class _LandingSink:
    """Write parquet chunks straight to a landing tree. No broker.

    Same durability discipline as `duckstream.landing`: temp file, `os.replace`,
    **then** the `_READY` marker. For benchmarking the pipeline without MQTT in
    the picture, and for eyeballing a fault's spectrogram before any plumbing
    exists.
    """

    def __init__(self, root: Path, machine: str, rate: float, seconds: float) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.rows: list[dict] = []
        self.limit = max(1, int(rate * seconds))
        self.n = 0

    def describe(self) -> str:
        return f"landing {self.root} ({self.limit} rows per chunk)"

    def emit(self, record: dict) -> None:
        self.rows.append(record)

    def flush_if_due(self) -> None:
        if len(self.rows) >= self.limit:
            self._write()

    def _write(self) -> None:
        if not self.rows:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%f")
        directory = self.root / f"{stamp}_{self.n:04d}"
        directory.mkdir(parents=True, exist_ok=False)
        table = pa.table({
            "timestamp": pa.array(
                [datetime.fromisoformat(r["timestamp"]) for r in self.rows],
                type=pa.timestamp("us")),
            "machine": pa.array([r["machine"] for r in self.rows], pa.string()),
            "seq": pa.array([r["seq"] for r in self.rows], pa.int64()),
            "vib_r": pa.array([r["vib_r"] for r in self.rows], pa.float64()),
            "vib_a": pa.array([r["vib_a"] for r in self.rows], pa.float64()),
            "rpm": pa.array([r["rpm"] for r in self.rows], pa.float64()),
            "mode": pa.array([r["mode"] for r in self.rows], pa.string()),
        })
        tmp = directory / "data.parquet.partial"
        pq.write_table(table, tmp)
        os.replace(tmp, directory / "data.parquet")
        (directory / "_READY").touch()
        self.n += 1
        self.rows = []

    def close(self) -> None:
        self._write()


if __name__ == "__main__":
    raise SystemExit(main())
