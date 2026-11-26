"""The engine simulator. **This is the script you run** (on the laptop).

Samples at 500 Hz and publishes the readings to RabbitMQ's MQTT plugin on the
Pi, topic ``engine/vibration``, 50 readings per message by default (see
Batching). RabbitMQ routes them to the ClickHouse queue as
``engine.vibration``.

Ported from lakehouse-rpi5/engine_pipeline/producer.py. The signal and the
reading format are unchanged; what changed is that RabbitMQ requires a login,
readings are batched, audit fields are added, and the parquet `--landing` mode
is gone because there is no landing tree.

Clock sync matters: `timestamp` and `sent_at` come from THIS machine's clock,
while arrival and storage times come from the Pi's. Every reported delay is
the difference between the two clocks, so an unsynced clock shows up as extra
(or negative) delay. Counts, loss and the sample rate are unaffected: they use
`seq` and this machine's own timestamps only.

    pip install numpy paho-mqtt
    python producer.py --host 192.168.0.104                  # healthy, forever
    python producer.py --host 192.168.0.104 --mode bearing
    python producer.py --host 192.168.0.104 --schedule "healthy:120,imbalance:90,misalignment:90,bearing:120,looseness:90"

The password comes from --password, then RMQ_PASSWORD, then a prompt.

Batching: readings are still sampled at --rate (500 Hz, one timestamp every
2 ms), but --batch of them (default 50 = one 100 ms block) travel in one MQTT
message as newline-separated JSON, one reading per line -- the JSONEachRow
format ClickHouse reads. That is 10 messages/s per producer instead of 500,
which removes the per-message ceiling measured on the Pi (RabbitMQ's QoS 1
confirmations took ~1.4 s under load, and 500 unconfirmed messages per
connection then capped each producer near 400 msg/s). --batch 1 restores
one reading per message.

Audit fields, for comparing implementations: every message carries `run_id`
(--run-id / ENG_RUN_ID, default a UTC start stamp) and `sent_at`, this
machine's wall clock (UTC) at the moment of publishing. Lags computed from
sent_at are only as good as the clock sync between this machine and the Pi.

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

`seq` is a monotonic per-run sample index. It costs eight bytes and it is what
makes "are we actually achieving 500 Hz" an exact measurement rather than a
guess: `verify --check rate` computes loss as
``1 - count(*)/(max(seq)-min(seq)+1)``.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import signal as signal_module
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from engine_signal import MODES, PROFILES, EngineSignal, blend  # noqa: E402

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
    parser.add_argument("--host", default=os.environ.get("ENG_MQTT_HOST", "192.168.0.104"))
    parser.add_argument("--port", type=int,
                        default=int(os.environ.get("ENG_MQTT_PORT", "1883")))
    parser.add_argument("--user", default=os.environ.get("RMQ_USER", "iot"))
    parser.add_argument("--password", default=os.environ.get("RMQ_PASSWORD"),
                        help="or set RMQ_PASSWORD; prompted for if neither is given")
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
    parser.add_argument("--run-id", default=os.environ.get("ENG_RUN_ID")
                        or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ"),
                        help="experiment label carried in every message (audit)")
    parser.add_argument("--batch", type=int, default=int(os.environ.get("ENG_BATCH", "50")),
                        help="readings per MQTT message (default 50 = 100 ms at 500 Hz; 1 = unbatched)")
    parser.add_argument("--inflight", type=int, default=10_000,
                        help="unconfirmed messages allowed per connection (default 10,000)")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    if args.batch < 1:
        parser.error("--batch must be >= 1")
    if not args.password:
        args.password = getpass.getpass(f"RabbitMQ password for {args.user}: ")

    control = os.environ.get("ENG_CONTROL_FILE")
    control_path = Path(control) if control else None
    if control_path is not None:
        control_path.parent.mkdir(parents=True, exist_ok=True)

    schedule = parse_schedule(args.schedule) if args.schedule else None
    start_mode = schedule[0][0] if schedule else args.mode
    modes = ModeController(start_mode, control_path, schedule, args.transition)

    # Windows: run_producers.py stops producers with Ctrl+Break, whose default
    # action kills the process outright. Treat it like Ctrl+C instead, so the
    # last batch is published and the summary printed.
    if hasattr(signal_module, "SIGBREAK"):
        def _on_break(*_):
            raise KeyboardInterrupt
        signal_module.signal(signal_module.SIGBREAK, _on_break)

    # SIGUSR1 does not exist on Windows, where the producer usually runs.
    if hasattr(signal_module, "SIGUSR1"):
        signal_module.signal(
            signal_module.SIGUSR1,
            lambda *_: modes.request(MODES[(MODES.index(modes.target) + 1) % len(MODES)]),
        )

    engine = EngineSignal(fs=args.rate, nominal_rpm=args.rpm, seed=args.seed,
                          rpm_profile=args.rpm_profile)

    sink = _MqttSink(args.host, args.port, args.topic, args.user, args.password,
                     batch=args.batch, inflight=args.inflight, rate=args.rate)

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
                    # audit: which experiment; sent_at is stamped by the sink
                    # when the message carrying this reading is published
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
                print(f"  {label:<13} {achieved:6.1f} readings/s  "
                      f"rpm {rpm[-1]:6.1f}  seq {seq:,}  acked {sink.acked:,}  "
                      f"backlog {sink.backlog:,}"
                      f"{f'  DROPPED {sink.dropped:,}' if sink.dropped else ''}"
                      f"{'  BEHIND' if achieved < args.rate * 0.97 else ''}",
                      flush=True)
                emitted = 0
                last_report = now
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        sink.close()
        print(f"emitted {seq:,} readings in {sink.messages:,} messages, "
              f"{sink.acked:,} readings acknowledged by the broker, "
              f"{sink.dropped:,} dropped (local buffer full), "
              f"{modes.switches} mode change(s)")
    return 0


class _MqttSink:
    """Batches readings into MQTT messages, QoS 1, logged in to RabbitMQ.

    `emit()` takes one reading; every `batch` readings become one message of
    newline-separated JSON, stamped with the `sent_at` of that publish. All
    counters are in READINGS, so they compare directly with the sample rate.

    QoS **must** be >= 1: the broker delivers at min(publish, subscribe) qos, so
    publishing at 0 would silently defeat the queue's durability.

    Connection trouble is reported, never hidden: the constructor waits for
    the broker to accept the login and exits with the reason if it refuses,
    and `acked` counts what the broker actually confirmed -- publishing while
    disconnected only queues locally.

    When the broker cannot keep up, paho buffers locally, up to
    `max_queued_messages` (sized to hold 5 minutes of readings); past that,
    publish() refuses the message. The original ignored that return code, so a
    slow broker turned into silent gaps. It is counted in `dropped` and printed.
    """

    BUFFER_SECONDS = 300

    def __init__(self, host: str, port: int, topic: str, user: str, password: str,
                 *, batch: int = 50, inflight: int = 10_000, rate: float = 500.0) -> None:
        import paho.mqtt.client as mqtt

        self.topic = topic
        self.batch = batch
        self._host, self._port = host, port
        self.acked = 0          # readings the broker confirmed
        self.sent = 0           # readings handed to paho
        self.dropped = 0        # readings refused because the local buffer was full
        self.messages = 0
        self._pending: list[dict] = []
        self._in_flight: dict[int, int] = {}      # mid -> readings in that message
        self._early: set[int] = set()             # mids confirmed before recorded
        # Guards only the two containers above, and is never held while calling
        # paho: paho may run on_publish holding its own lock, so holding ours
        # across publish() could deadlock the two threads.
        self._lock = threading.Lock()
        self._err_queue_size = mqtt.MQTT_ERR_QUEUE_SIZE
        self._connected = threading.Event()
        self._refused: str | None = None

        def on_connect(client, userdata, flags, rc, props):
            if rc.is_failure:
                self._refused = str(rc)
                return
            self._connected.set()

        def on_disconnect(client, userdata, flags, rc, props):
            self._connected.clear()
            if self._refused is None:
                print(f"  disconnected ({rc}), reconnecting...", flush=True)

        def on_publish(client, userdata, mid, rc, props):
            with self._lock:
                n = self._in_flight.pop(mid, None)
                if n is None:
                    self._early.add(mid)    # confirmed before _publish recorded it
                else:
                    self.acked += n

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.username_pw_set(user, password)
        self.client.on_connect = on_connect
        self.client.on_disconnect = on_disconnect
        self.client.on_publish = on_publish
        self.client.max_inflight_messages_set(inflight)
        self.client.max_queued_messages_set(
            max(1000, int(self.BUFFER_SECONDS * rate / batch)))
        try:
            self.client.connect(host, port, keepalive=60)
        except OSError as exc:
            sys.exit(f"cannot reach {host}:{port}: {exc}")
        self.client.loop_start()
        deadline = time.monotonic() + 10
        while not self._connected.is_set() and time.monotonic() < deadline:
            if self._refused:
                self.client.loop_stop()
                sys.exit(f"broker refused the login: {self._refused} "
                         f"(check --user / --password)")
            time.sleep(0.05)
        if not self._connected.is_set():
            self.client.loop_stop()
            sys.exit(f"no answer from {host}:{port} within 10 s")

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
        n = len(records)
        info = self.client.publish(self.topic, payload, qos=1)
        if info.rc == self._err_queue_size:
            self.dropped += n          # local buffer full: these readings are lost
            return
        with self._lock:
            self.sent += n
            self.messages += 1
            if info.mid in self._early:
                self._early.discard(info.mid)
                self.acked += n
            else:
                self._in_flight[info.mid] = n

    @property
    def backlog(self) -> int:
        """Readings published but not yet confirmed by the broker."""
        return self.sent - self.acked

    def flush_if_due(self) -> None:
        pass

    def close(self) -> None:
        self._publish()               # the last partial batch
        deadline = time.monotonic() + 5
        while self.backlog and time.monotonic() < deadline:
            time.sleep(0.1)           # let in-flight confirmations arrive
        self.client.loop_stop()
        self.client.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
