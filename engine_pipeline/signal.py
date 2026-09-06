"""The engine vibration signal model. Pure numpy, no MQTT, no duckstream.

Separated from `producer.py` so it can be generated to a file and looked at
before any of the plumbing exists. If this is wrong the whole demo is wrong, and
no amount of pipeline correctness fixes it.

The machine
-----------
A single-shaft machine at a nominal 1800 RPM, so the shaft turns at
**f_r = 30 Hz**. A rolling-element bearing with 8 balls and d/D = 0.35 gives the
standard defect tones::

    FTF  (cage)      0.5*(1 - d/D)*f_r            =  9.75 Hz
    BSF  (ball)      (D/2d)*(1 - (d/D)^2)*f_r     = 37.6  Hz
    BPFO (outer)     (n/2)*(1 - d/D)*f_r          = 78.0  Hz
    BPFI (inner)     (n/2)*(1 + d/D)*f_r          = 162.0 Hz
    vane pass        7*f_r                        = 210   Hz
    resonance        fixed structural mode        = 180   Hz, Q ~ 8

Those ratios are chosen so **every bearing tone is a non-integer multiple of
f_r**. That is not cosmetic: on the spectrogram the bearing lines then sit
visibly *between* the shaft harmonics, which is exactly how the two are told
apart in the field.

Measured, and it corrected the design
------------------------------------
The plan for this pipeline predicted the bearing fault would show **crest > 6 and
kurtosis > 6**, those being the classical impulsive-defect indicators. Measured
here, it does not: crest 3.13 against a healthy 3.03, kurtosis 2.70 against 2.00.

That is not a modelling bug, and it is not the sample rate -- kurtosis was
measured at 500, 1000, 2000, 5000 and 10000 Hz and stayed between 1.9 and 3.2
throughout. The cause is the geometry. The structural mode has Q = 8 at 180 Hz,
so its decay is tau = 2Q/omega0 = 14.1 ms, while BPFO at 78 Hz puts an impulse
every 12.8 ms. **The rings overlap before they decay**, so the "impulse train" is
continuous ringing rather than a series of separated events -- and a continuously
ringing resonance has the kurtosis of a tone, not of an impulse.

This is why real bearing diagnostics use *envelope* analysis (band-pass around
the resonance, then Hilbert) rather than raw time-domain kurtosis: the raw
statistic is routinely unrevealing for exactly this reason.

So the bearing discriminator here is **spectral, not statistical**: BPFO sits
+37.6 dB above healthy, which is a far stronger and more specific signal than
kurtosis would have been. `verify --check faults` asserts that, not the
kurtosis. Kurtosis stays on the dashboard because it *does* discriminate
imbalance -- a dominant sine pulls it to 1.47 against a healthy 2.00 -- but it is
not claimed as a bearing indicator anywhere.

Measured signatures, relative to healthy (dB), 60 s at steady RPM::

                 0.5x     1x     2x     3x    BPFO   HF150-220   ax/rad
    healthy         -      -      -      -       -           -     0.30
    imbalance    +0.1  +16.2   +2.5   +2.5    +0.0        +0.0     0.08
    misalignment +2.0   +7.0  +21.9  +20.0    +2.0        +1.9     1.22
    bearing      -0.0   +0.8   +1.3   +2.5   +37.6        +4.1     0.08
    looseness   +44.7   +8.8  +16.4  +20.0    +8.0        +9.4     0.19

Each fault is a different *shape*, which is the property that matters: a
spectrogram where every fault merely looks brighter proves nothing.

Five traps, each a silent wrongness
-----------------------------------
1. **Integrate the phase.** ``phi[n] = phi[n-1] + 2*pi*f_r[n]/fs``. Writing
   ``sin(2*pi*f(t)*t)`` with a varying ``f`` produces phase jumps that smear the
   whole spectrum into mush -- and it looks like broadband noise, not like a bug.
2. **Wrap phi at 4*pi, not 2*pi.** The looseness signature has a 0.5x term, and
   ``sin(0.5*phi)`` is discontinuous if the accumulator wraps at 2*pi.
3. **Band-limit, and do not clip.** Only harmonics below 0.45*fs are synthesised.
   Real mechanical looseness clips, but clipping generates unbounded harmonics
   that alias back into the band; doing it properly needs oversampling and a
   decimation filter. The harmonic forest is synthesised explicitly instead, and
   the negative skew comes from a phase-offset 2x term. A choice, not an oversight.
4. **Carry the resonator's tail across blocks.** The 180 Hz / Q 8 impulse
   response is ~7 samples long. Resetting the filter each block puts a click at
   every boundary -- at 10 Hz that is a bright horizontal line at 10 Hz and its
   harmonics, which looks exactly like a real fault.
5. **Place impulses fractionally.** BPFO at 78 Hz has a period of 6.41 samples.
   Snapping to integers adds +/-0.5 sample jitter that broadens the sidebands
   into a smear.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np

# -- the machine -------------------------------------------------------------

N_BALLS = 8
D_OVER_D = 0.35

#: Multiples of shaft frequency. Every one is deliberately non-integer.
ORDER_FTF = 0.5 * (1 - D_OVER_D)                        # 0.325
ORDER_BSF = (1 / (2 * D_OVER_D)) * (1 - D_OVER_D ** 2)  # 1.254
ORDER_BPFO = (N_BALLS / 2) * (1 - D_OVER_D)             # 2.600
ORDER_BPFI = (N_BALLS / 2) * (1 + D_OVER_D)             # 5.400
ORDER_VANE = 7.0

RESONANCE_HZ = 180.0
RESONANCE_Q = 8.0

MODES = ("healthy", "imbalance", "misalignment", "bearing", "looseness")


@dataclass(frozen=True)
class Profile:
    """Component amplitudes in g-peak. One per fault mode.

    `harmonics[k]` is the amplitude of the k-th shaft order on the radial
    channel. Fractional keys (0.5, 1.5) are the looseness sub-harmonics, which
    is why the phase accumulator has to survive a half-turn (trap 2).
    """

    harmonics: dict[float, float]
    axial: dict[float, float]
    bpfo_impulse: float = 0.0      # peak of the resonance burst, g
    bpfo_lines: tuple[float, ...] = ()   # discrete BPFO, 2*BPFO lines
    resonance_gain: float = 1.0    # broadband energy through the 180 Hz mode
    noise_sigma: float = 0.004     # white noise, g RMS
    skew_phase: float = 0.0        # 2x phase offset -> asymmetric waveform


#: Amplitudes chosen so each fault is a different **shape**, not just brighter.
PROFILES: dict[str, Profile] = {
    "healthy": Profile(
        harmonics={1: 0.020, 2: 0.006, 3: 0.003, 4: 0.0015,
                   5: 0.0010, 6: 0.0007, 7: 0.0040},
        axial={1: 0.006, 2: 0.002},
        resonance_gain=1.0, noise_sigma=0.004,
    ),
    # One line gains ~16 dB and nothing else moves. Amplitude scales with
    # (f_r/f_nom)^2, applied in the generator, so the 1x line brightens and
    # dims as the RPM drifts -- physically right, and good to watch.
    "imbalance": Profile(
        harmonics={1: 0.130, 2: 0.008, 3: 0.004, 4: 0.002,
                   5: 0.0010, 6: 0.0007, 7: 0.0040},
        axial={1: 0.010, 2: 0.002},
        resonance_gain=1.0, noise_sigma=0.004,
    ),
    # 2x overtakes 1x and 3x appears; the axial channel triples, which is the
    # textbook discriminator and shows up as a *scalar* (axial_ratio) as well as
    # in the picture.
    "misalignment": Profile(
        harmonics={1: 0.045, 2: 0.075, 3: 0.030, 4: 0.008,
                   5: 0.0020, 6: 0.0012, 7: 0.0040},
        axial={1: 0.070, 2: 0.090},
        resonance_gain=1.2, noise_sigma=0.005,
    ),
    # RMS barely moves; crest and kurtosis go through the roof. The energy is an
    # impulse train through the structural resonance, so it appears as a broad
    # band at 150-220 Hz with 78 Hz sidebands, not as a line.
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
    """Linear cross-fade between two profiles, `w` in [0, 1] toward `b`.

    Every switch is faded over a few seconds rather than stepped. A step change
    in amplitude is a discontinuity that sprays broadband energy across one
    frame -- a bright vertical stripe that reads as a fault -- and a gradient is
    far more legible on a spectrogram than a hard edge anyway.
    """
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
    """A decaying sinusoid: the structural mode, as an FIR kernel.

    tau = 2Q/omega0 ~ 14 ms ~ 7 samples at 500 Hz, so 64 taps is generous.
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
    accumulator (trap 1/2), the resonator tail (trap 4), the fractional impulse
    position (trap 5) and the RPM random walk.
    """

    fs: float = 500.0
    nominal_rpm: float = 1800.0
    seed: int = 12345
    rpm_profile: str = "drift"

    phase: float = 0.0                  # shaft phase, wrapped at 4*pi
    bearing_phase: float = 0.0          # BPFO impulse position, in samples
    elapsed: float = 0.0                # seconds since start
    _walk: float = 0.0
    _load: float = 1.0
    _load_target: float = 1.0
    _tail: np.ndarray = field(default_factory=lambda: np.zeros(0))
    _rng: np.random.Generator = field(init=False)
    _ir: np.ndarray = field(init=False)

    def __post_init__(self) -> None:
        self._rng = np.random.default_rng(self.seed)
        self._ir = resonator_ir(self.fs)
        self._tail = np.zeros(len(self._ir) - 1)

    # -- RPM ---------------------------------------------------------------

    def _rpm_series(self, n: int) -> np.ndarray:
        """Per-sample RPM. Drift makes the harmonics wobble; steps kink them all."""
        t = self.elapsed + np.arange(n) / self.fs
        if self.rpm_profile == "steady":
            factor = np.ones(n)
        else:
            factor = (1.0
                      + 0.030 * np.sin(2 * math.pi * t / 47.0)
                      + 0.008 * np.sin(2 * math.pi * t / 11.0))
        # A bounded random walk, so it wanders without escaping.
        step = self._rng.normal(0.0, 0.0005, n)
        walk = self._walk + np.cumsum(step)
        np.clip(walk, -0.01, 0.01, out=walk)
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

    # -- the block ---------------------------------------------------------

    def block(self, n: int, profile: Profile) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """`n` samples of (radial, axial, rpm)."""
        rpm = self._rpm_series(n)
        f_r = rpm / 60.0

        # Trap 1: integrate. Trap 2: wrap at 4*pi so sin(0.5*phi) is continuous.
        dphi = 2.0 * math.pi * f_r / self.fs
        phi = self.phase + np.cumsum(dphi)
        self.phase = float(phi[-1] % (4.0 * math.pi))

        nyq_limit = 0.45 * self.fs
        radial = np.zeros(n)
        for order, amp in profile.harmonics.items():
            if amp <= 0:
                continue
            if np.median(order * f_r) >= nyq_limit:   # trap 3
                continue
            # Imbalance force grows with the square of speed; applied to the 1x
            # term only, which is what makes that line breathe with the RPM.
            scale = ((f_r / (self.nominal_rpm / 60.0)) ** 2) if order == 1 else 1.0
            ph = profile.skew_phase if order == 2 else 0.0
            radial += amp * scale * np.sin(order * phi + ph)

        if profile.bpfo_lines:
            for i, amp in enumerate(profile.bpfo_lines, start=1):
                if amp > 0 and np.median(i * ORDER_BPFO * f_r) < nyq_limit:
                    radial += amp * np.sin(i * ORDER_BPFO * phi)

        # Noise and impulses take **separate** paths through the resonance, and
        # the split matters. A bearing defect excites the structural mode with
        # impulses; it does not also triple the broadband noise floor. Amplifying
        # both together (the first version did) raised RMS 5x above healthy and
        # *lowered* crest, because a louder Gaussian background is exactly what
        # buries an impulse. Now `resonance_gain` applies only to the impulse
        # path, so the bearing signature is impulsive energy on an unchanged
        # floor -- which is what it physically is.
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

    def _impulse_train(self, n: int, f_r: np.ndarray, amp: float) -> np.ndarray:
        """BPFO impulses, placed fractionally (trap 5)."""
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

    def _filter(self, x: np.ndarray) -> np.ndarray:
        """Convolve through the resonance, carrying the tail (trap 4)."""
        padded = np.concatenate([self._tail, x])
        y = np.convolve(padded, self._ir, mode="full")
        keep = len(self._tail)
        out = y[keep: keep + len(x)]
        self._tail = padded[-(len(self._ir) - 1):] if len(self._ir) > 1 else np.zeros(0)
        return out
