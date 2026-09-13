#!/usr/bin/env python3
"""Engine vibration producer -- **run this on your laptop**.

The replacement for `old_producer.py`. Standalone: one file, no repo checkout,
no `engine_pipeline` package. Only `paho-mqtt` and `numpy` are needed::

    pip install paho-mqtt numpy
    python engine_producer.py --host 192.168.0.104

It publishes 500 readings a second to `engine/vibration`, simulating a
single-shaft machine at 1800 RPM with five switchable fault modes. The Pi's
ingest lands them, duckstream processes them, and the dashboard draws the
spectrogram.

Why this is not just `old_producer.py` with a new payload
--------------------------------------------------------
Four things in that file are harmless at 100 Hz and fatal at 500:

1. **`client.loop_start()` was never called.** Without paho's network thread,
   `publish()` only appends to an internal queue; nothing is written to the
   socket until something services it. At 100 Hz the OS buffer absorbed enough
   that it appeared to work. At 500 Hz it becomes an unbounded memory leak that
   delivers nothing, and it looks like the broker is down.
2. **`time.sleep(1/500)` per sample cannot hold 500 Hz.** A sleep has hundreds
   of microseconds of overhead and rounds up to the scheduler tick (~1 ms on
   Linux/macOS, ~15 ms on Windows), so the achieved rate lands far below the
   requested one. Readings are generated in 100 ms blocks against an
   **absolute** deadline instead, so timing error cannot accumulate.
3. **QoS 0.** A broker delivers at `min(publish_qos, subscribe_qos)`, so
   publishing at 0 silently defeats the ingest side's entire acknowledgement
   discipline -- it only acks once a chunk is durably on disk, which is
   meaningless if the broker never expected an ack. This publishes at QoS 1.
4. **`datetime.now().isoformat()` is naive *local* time.** The decoder treats a
   naive stamp as UTC, so a laptop in PKT would have written readings five hours
   in the future, and every window would be wrong without anything erroring.
   This uses naive UTC.

Also raised: paho's in-flight window, which defaults to 20. That is the first
thing that bites at 500 msg/s with QoS 1, because each message must be acked
before the 21st can go out.

Timing
------
Sample stamps are `t0 + n/fs` exactly and never come from the wall clock, so the
STFT's uniform-sampling assumption is true rather than approximately true. The
clock is consulted only to decide when to sleep. `seq` is a monotonic per-run
index; it is what makes "are we really achieving 500 Hz" an exact measurement,
since the Pi computes loss as `1 - count(*)/(max(seq)-min(seq)+1)`.

Switching mode while it runs
----------------------------
* `--schedule "healthy:120,bearing:90"` -- loops forever (best for a demo)
* `--mode bearing` -- the starting mode
* control file -- `echo bearing > engine.mode` (polled once per block)
* SIGUSR1 -- advance to the next mode (not available on Windows)

Every switch cross-fades over `--transition` seconds. A step change in amplitude
sprays broadband energy across one STFT frame -- a bright vertical stripe that
reads as a fault.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

# ---------------------------------------------------------------- the machine
# A single-shaft machine at a nominal 1800 RPM -> shaft f_r = 30 Hz.
# A rolling-element bearing, 8 balls, d/D = 0.35, gives the standard tones.
# Every ratio below is deliberately NON-INTEGER, so bearing lines sit visibly
# *between* the shaft harmonics -- which is how the two are told apart in the
# field, and what makes the spectrogram diagnostic rather than decorative.

N_BALLS = 8
D_OVER_D = 0.35

ORDER_FTF = 0.5 * (1 - D_OVER_D)                        # 0.325  cage
ORDER_BSF = (1 / (2 * D_OVER_D)) * (1 - D_OVER_D ** 2)  # 1.254  ball spin
ORDER_BPFO = (N_BALLS / 2) * (1 - D_OVER_D)             # 2.600  outer race
ORDER_BPFI = (N_BALLS / 2) * (1 + D_OVER_D)             # 5.400  inner race
ORDER_VANE = 7.0                                        # vane pass

RESONANCE_HZ = 180.0
RESONANCE_Q = 8.0

MODES = ("healthy", "imbalance", "misalignment", "bearing", "looseness")


@dataclass(frozen=True)
class Profile:
    """Component amplitudes in g-peak, one per fault mode.

    `harmonics[k]` is the amplitude of the k-th shaft order on the radial
    channel. Fractional keys (0.5, 1.5) are the looseness sub-harmonics, which
    is why the phase accumulator has to survive a half turn.
    """

    harmonics: dict
    axial: dict
    bpfo_impulse: float = 0.0
    bpfo_lines: tuple = ()
    resonance_gain: float = 1.0
    noise_sigma: float = 0.004
    skew_phase: float = 0.0


#: Each fault is a different *shape*, not merely brighter. A spectrogram where
#: every fault just looks louder proves nothing.
PROFILES = {
    "healthy": Profile(
        harmonics={1: 0.020, 2: 0.006, 3: 0.003, 4: 0.0015,
                   5: 0.0010, 6: 0.0007, 7: 0.0040},
        axial={1: 0.006, 2: 0.002},
        resonance_gain=1.0, noise_sigma=0.004,
    ),
    # One line gains ~16 dB and nothing else moves. Amplitude scales with
    # (f_r/f_nom)^2, so the 1x line brightens and dims as the RPM drifts --
    # physically right, and good to watch.
    "imbalance": Profile(
        harmonics={1: 0.130, 2: 0.008, 3: 0.004, 4: 0.002,
                   5: 0.0010, 6: 0.0007, 7: 0.0040},
        axial={1: 0.010, 2: 0.002},
        resonance_gain=1.0, noise_sigma=0.004,
    ),
    # 2x overtakes 1x and 3x appears; the axial channel triples, which is the
    # textbook discriminator and shows up as a scalar (axial_ratio) too.
    "misalignment": Profile(
        harmonics={1: 0.045, 2: 0.075, 3: 0.030, 4: 0.008,
                   5: 0.0020, 6: 0.0012, 7: 0.0040},
        axial={1: 0.070, 2: 0.090},
        resonance_gain=1.2, noise_sigma=0.005,
    ),
    # Impulse train through the structural resonance: a broad 150-220 Hz band
    # with 78 Hz sidebands, not a line. See the note on kurtosis below.
    "bearing": Profile(
        harmonics={1: 0.022, 2: 0.007, 3: 0.004, 4: 0.002,
                   5: 0.0010, 6: 0.0007, 7: 0.0040},
        axial={1: 0.006, 2: 0.002},
        bpfo_impulse=0.050, bpfo_lines=(0.006, 0.004),
        resonance_gain=3.0, noise_sigma=0.004,
    ),
    # A forest: every order raised to a flat roll-off, plus 0.5x and 1.5x, plus
    # a lifted floor. Unmistakable at a glance.
    "looseness": Profile(
        harmonics={0.5: 0.030, 1: 0.055, 1.5: 0.018, 2: 0.040, 3: 0.030,
                   4: 0.024, 5: 0.020, 6: 0.020, 7: 0.020},
        axial={1: 0.012, 2: 0.010},
        resonance_gain=1.6, noise_sigma=0.010, skew_phase=0.6,
    ),
}


def blend(a: Profile, b: Profile, w: float) -> Profile:
    """Linear cross-fade between two profiles, `w` in [0, 1] toward `b`."""
    keys = set(a.harmonics) | set(b.harmonics)
    ax = set(a.axial) | set(b.axial)
    lerp = lambda x, y: x + (y - x) * w          # noqa: E731
    return Profile(
        harmonics={k: lerp(a.harmonics.get(k, 0.0), b.harmonics.get(k, 0.0))
                   for k in keys},
        axial={k: lerp(a.axial.get(k, 0.0), b.axial.get(k, 0.0)) for k in ax},
        bpfo_impulse=lerp(a.bpfo_impulse, b.bpfo_impulse),
        bpfo_lines=tuple(
            lerp(a.bpfo_lines[i] if i < len(a.bpfo_lines) else 0.0,
                 b.bpfo_lines[i] if i < len(b.bpfo_lines) else 0.0)
            for i in range(max(len(a.bpfo_lines), len(b.bpfo_lines)) or 2)
        ),
        resonance_gain=lerp(a.resonance_gain, b.resonance_gain),
        noise_sigma=lerp(a.noise_sigma, b.noise_sigma),
        skew_phase=lerp(a.skew_phase, b.skew_phase),
    )


def resonator_ir(fs: float, f0: float = RESONANCE_HZ, q: float = RESONANCE_Q,
                 taps: int = 64) -> np.ndarray:
    """The structural mode as an FIR kernel: a decaying sinusoid.

    tau = 2Q/w0 ~ 14 ms ~ 7 samples at 500 Hz, so 64 taps is generous.
    Normalised to unit peak so `resonance_gain` means what it says.
    """
    tau = 2.0 * q / (2.0 * math.pi * f0)
    n = np.arange(taps)
    ir = np.exp(-n / (tau * fs)) * np.sin(2.0 * math.pi * f0 * n / fs)
    peak = np.abs(ir).max()
    return ir / peak if peak else ir


@dataclass
class EngineSignal:
    """Stateful generator. Call `block(n)` repeatedly; state carries across.

    Everything that must survive a block boundary lives here: the phase
    accumulator, the resonator tail, the fractional impulse position and the
    RPM random walk. Four of the five traps below are about exactly that.
    """

    fs: float = 500.0
    nominal_rpm: float = 1800.0
    seed: int = 12345
    rpm_profile: str = "drift"

    phase: float = 0.0                  # shaft phase, wrapped at 4*pi
    bearing_phase: float = 0.0          # BPFO impulse position, in samples
    elapsed: float = 0.0
    _walk: float = 0.0
    _load: float = 1.0
    _load_target: float = 1.0
    _tail: np.ndarray = field(default_factory=lambda: np.zeros(0))

    def __post_init__(self) -> None:
        self._rng = np.random.default_rng(self.seed)
        self._ir = resonator_ir(self.fs)
        self._tail = np.zeros(len(self._ir) - 1)

    def _rpm_series(self, n: int) -> np.ndarray:
        """Per-sample RPM. Drift makes harmonics wobble; steps kink them all."""
        t = self.elapsed + np.arange(n) / self.fs
        if self.rpm_profile == "steady":
            factor = np.ones(n)
        else:
            factor = (1.0
                      + 0.030 * np.sin(2 * math.pi * t / 47.0)
                      + 0.008 * np.sin(2 * math.pi * t / 11.0))
        step = self._rng.normal(0.0, 0.0005, n)
        walk = self._walk + np.cumsum(step)
        np.clip(walk, -0.01, 0.01, out=walk)      # bounded: wanders, never escapes
        self._walk = float(walk[-1])

        if self.rpm_profile == "steps":
            # A first-order lag toward an occasionally-restepped target: every
            # harmonic kinks at the same instant, which is the single most
            # convincing "this is a real machine" artefact.
            if self._rng.random() < n / (self.fs * 20.0):
                self._load_target = 1.0 + self._rng.choice([-0.05, 0.05])
            alpha = 1.0 - math.exp(-1.0 / (self.fs * 4.0))
            load = np.empty(n)
            cur = self._load
            for i in range(n):
                cur += alpha * (self._load_target - cur)
                load[i] = cur
            self._load = cur
        else:
            load = np.ones(n)

        return self.nominal_rpm * factor * (1.0 + walk) * load

    def block(self, n: int, profile: Profile):
        """`n` samples of (radial, axial, rpm)."""
        rpm = self._rpm_series(n)
        f_r = rpm / 60.0

        # TRAP 1: integrate the phase. Writing sin(2*pi*f(t)*t) with a varying f
        # produces phase jumps that smear the spectrum into mush -- and it looks
        # like broadband noise, not like a bug.
        # TRAP 2: wrap at 4*pi, not 2*pi, so sin(0.5*phi) stays continuous for
        # the looseness sub-harmonic.
        dphi = 2.0 * math.pi * f_r / self.fs
        phi = self.phase + np.cumsum(dphi)
        self.phase = float(phi[-1] % (4.0 * math.pi))

        nyq_limit = 0.45 * self.fs
        radial = np.zeros(n)
        for order, amp in profile.harmonics.items():
            if amp <= 0:
                continue
            if np.median(order * f_r) >= nyq_limit:   # TRAP 3: band-limit
                continue
            # Imbalance force grows with the square of speed; applied to the 1x
            # term only, which makes that line breathe with the RPM.
            scale = ((f_r / (self.nominal_rpm / 60.0)) ** 2) if order == 1 else 1.0
            ph = profile.skew_phase if order == 2 else 0.0
            radial += amp * scale * np.sin(order * phi + ph)

        if profile.bpfo_lines:
            for i, amp in enumerate(profile.bpfo_lines, start=1):
                if amp > 0 and np.median(i * ORDER_BPFO * f_r) < nyq_limit:
                    radial += amp * np.sin(i * ORDER_BPFO * phi)

        # Noise and impulses take SEPARATE paths through the resonance. A
        # bearing defect excites the structural mode with impulses; it does not
        # also triple the broadband floor. Amplifying both together raised RMS
        # 5x above healthy and *lowered* crest, because a louder Gaussian
        # background is exactly what buries an impulse.
        noise = self._rng.normal(0.0, profile.noise_sigma, n)
        if profile.bpfo_impulse > 0:
            impulses = self._impulse_train(n, f_r, profile.bpfo_impulse)
            excitation = noise + impulses * profile.resonance_gain
        else:
            excitation = noise

        radial = radial + self._filter(excitation)

        axial = np.zeros(n)
        for order, amp in profile.axial.items():
            if amp > 0 and np.median(order * f_r) < nyq_limit:
                axial += amp * np.sin(order * phi + 0.4)
        axial += self._rng.normal(0.0, profile.noise_sigma * 0.6, n)

        self.elapsed += n / self.fs
        return radial, axial, rpm

    def _impulse_train(self, n, f_r, amp):
        """BPFO impulses, placed FRACTIONALLY.

        TRAP 5: BPFO at 78 Hz has a period of 6.41 samples. Snapping to integers
        adds +/-0.5 sample jitter that broadens the sidebands into a smear.
        """
        out = np.zeros(n)
        period = self.fs / (ORDER_BPFO * float(np.median(f_r)))
        pos = self.bearing_phase
        while pos < n:
            i = int(pos)
            frac = pos - i
            if 0 <= i < n:
                out[i] += amp * (1.0 - frac)
            if 0 <= i + 1 < n:
                out[i + 1] += amp * frac
            pos += period
        self.bearing_phase = pos - n
        return out

    def _filter(self, x):
        """Convolve through the resonance, CARRYING THE TAIL.

        TRAP 4: the impulse response is ~7 samples. Resetting the filter each
        block puts a click at every boundary -- at 10 blocks/s that is a bright
        horizontal line at 10 Hz and its harmonics, which looks exactly like a
        real fault.
        """
        padded = np.concatenate([self._tail, x])
        y = np.convolve(padded, self._ir, mode="full")
        keep = len(self._tail)
        out = y[keep: keep + len(x)]
        self._tail = padded[-(len(self._ir) - 1):] if len(self._ir) > 1 else np.zeros(0)
        return out


# --------------------------------------------------------------- mode control

def parse_schedule(text):
    """``"healthy:120,bearing:90"`` -> [("healthy",120.0), ("bearing",90.0)]."""
    out = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        name, _, secs = part.partition(":")
        name = name.strip()
        if name not in MODES:
            raise SystemExit(f"unknown mode {name!r} in --schedule; "
                             f"expected one of {', '.join(MODES)}")
        out.append((name, float(secs or 60)))
    if not out:
        raise SystemExit("--schedule parsed to nothing")
    return out


class ModeController:
    """Decides which profile is in force, and cross-fades between them."""

    def __init__(self, start, control_file, schedule, transition):
        self.current = start
        self.target = start
        self.transition = max(0.0, transition)
        self._fade_left = 0.0
        self.control_file = control_file
        self.schedule = schedule
        self._sched_i = 0
        self._sched_left = schedule[0][1] if schedule else 0.0
        self._mtime = None
        self.rpm_override = None
        self.switches = 0

    def request(self, mode):
        if mode not in MODES or mode == self.target:
            return
        self.current = self.blended_name()
        self.target = mode
        self._fade_left = self.transition
        self.switches += 1
        print(f"  mode -> {mode}"
              f"{'' if self.transition <= 0 else f' (fading {self.transition:g}s)'}",
              flush=True)

    def blended_name(self):
        return self.target if self._fade_left <= 0 else self.current

    def _poll_control_file(self):
        # One os.stat per block, re-read only when mtime changes, so it costs
        # nothing. Works over ssh and survives a producer restart.
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
            tokens = self.control_file.read_text().split()
        except OSError:
            return
        for token in tokens:
            if token in MODES:
                self.request(token)
            elif token.startswith("rpm="):
                try:
                    self.rpm_override = float(token[4:])
                    print(f"  rpm -> {self.rpm_override:g}", flush=True)
                except ValueError:
                    pass

    def advance(self, dt):
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


class MqttSink:
    """One reading per message, QoS 1.

    QoS must be >= 1: the broker delivers at min(publish, subscribe) qos, so
    publishing at 0 would silently defeat the ingest side's acknowledgement
    discipline. The in-flight window is raised well above paho's default of 20,
    which is the first thing that bites at 500 msg/s -- with QoS 1 each message
    must be acked before the 21st can go out.
    """

    def __init__(self, host, port, topic):
        import paho.mqtt.client as mqtt

        self.topic = topic
        try:                                   # paho 2.x
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:                 # paho 1.x
            self.client = mqtt.Client()
        self.client.max_inflight_messages_set(500)
        self.client.max_queued_messages_set(20000)
        self.client.connect(host, port, keepalive=60)
        self.client.loop_start()               # without this, nothing is sent
        self._host, self._port = host, port

    def describe(self):
        return f"mqtt {self._host}:{self._port} topic {self.topic!r} qos 1"

    def emit(self, record):
        self.client.publish(self.topic, json.dumps(record), qos=1)

    def close(self):
        self.client.loop_stop()
        self.client.disconnect()


class NullSink:
    """--dry-run: generate and time everything, publish nothing."""

    def describe(self):
        return "dry run (nothing published)"

    def emit(self, record):
        pass

    def close(self):
        pass


BLOCK_SECONDS = 0.1          # generate and publish in 100 ms bursts


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Engine vibration producer -- run this on your laptop.")
    p.add_argument("--host", default=os.environ.get("ENG_MQTT_HOST", "192.168.0.104"),
                   help="the Pi's IP (default %(default)s)")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--topic", default="engine/vibration")
    p.add_argument("--machine", default="Karachi_ENG01")
    p.add_argument("--rate", type=float, default=500.0, help="samples/s")
    p.add_argument("--rpm", type=float, default=1800.0, help="nominal RPM")
    p.add_argument("--mode", default="healthy", choices=list(MODES))
    p.add_argument("--schedule", default=None,
                   help='e.g. "healthy:120,imbalance:90,bearing:120" -- loops')
    p.add_argument("--transition", type=float, default=4.0,
                   help="cross-fade seconds between modes (default 4)")
    p.add_argument("--rpm-profile", default="drift",
                   choices=("steady", "drift", "steps"))
    p.add_argument("--seconds", type=float, default=None,
                   help="stop after this long (default: run forever)")
    p.add_argument("--seed", type=int, default=12345)
    p.add_argument("--control-file", default="engine.mode",
                   help="echo a mode into this file to switch (default %(default)s)")
    p.add_argument("--dry-run", action="store_true",
                   help="generate but publish nothing -- checks the rate first")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args(argv)

    control_path = Path(args.control_file) if args.control_file else None
    schedule = parse_schedule(args.schedule) if args.schedule else None
    start_mode = schedule[0][0] if schedule else args.mode
    modes = ModeController(start_mode, control_path, schedule, args.transition)

    # SIGUSR1 does not exist on Windows; the control file covers that case.
    try:
        import signal as signal_module
        signal_module.signal(
            signal_module.SIGUSR1,
            lambda *_: modes.request(
                MODES[(MODES.index(modes.target) + 1) % len(MODES)]),
        )
    except (AttributeError, ValueError, OSError):
        pass

    engine = EngineSignal(fs=args.rate, nominal_rpm=args.rpm, seed=args.seed,
                          rpm_profile=args.rpm_profile)
    sink = NullSink() if args.dry_run else MqttSink(args.host, args.port, args.topic)

    print(f"machine : {args.machine}")
    print(f"rate    : {args.rate:g} Hz   nominal {args.rpm:g} RPM "
          f"(shaft {args.rpm/60:.1f} Hz)")
    print(f"mode    : {start_mode}"
          + (f"   schedule {args.schedule}" if schedule else ""))
    if control_path:
        print(f"control : echo bearing > {control_path}")
    print(f"sink    : {sink.describe()}")
    print()

    block_n = max(1, int(round(args.rate * BLOCK_SECONDS)))
    # Naive UTC. The decoder treats a naive stamp as UTC, so local time here
    # would silently place every reading hours away and corrupt every window.
    t0 = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
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
                # Exact: t0 + n/fs, never the wall clock, so the STFT's uniform
                # sampling assumption is true rather than nearly true.
                ts = t0 + timedelta(seconds=(seq + i) / args.rate)
                sink.emit({
                    "timestamp": ts.isoformat(sep=" ", timespec="microseconds"),
                    "machine": args.machine,
                    "seq": seq + i,
                    "vib_r": round(float(radial[i]), 6),
                    "vib_a": round(float(axial[i]), 6),
                    "rpm": round(float(rpm[i]), 2),
                    "mode": label,
                })
            seq += block_n
            emitted += block_n

            # Absolute deadline: error cannot accumulate the way it does with a
            # per-sample cumulative sleep.
            delay = (started + seq / args.rate) - time.monotonic()
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


if __name__ == "__main__":
    raise SystemExit(main())
