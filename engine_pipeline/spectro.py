"""The STFT, as plain numpy. No duckstream import, on purpose.

`udfs.py` is a three-line wrapper that turns `stft_db` into an `ArrowUDF`; the
maths lives here so `verify.py` can test it with no catalog, no broker and no
engine -- which is what lets the calibration be checked against an analytically
known answer rather than against the pipeline's own output.

**Why an STFT inside a UDF at all.** duckstream's finest window grain is a
minute (`GRAINS = ('minute','hour','day')`), and a spectrogram with one column
per minute is not a spectrogram. So one minute window goes in and a whole
time x frequency grid comes out, flattened. That is a good fit for tier three
rather than a workaround for it: no pair of partial spectrograms combines into
the spectrogram of a concatenation, which is exactly what `non_foldable` means.

The parameters, and the arithmetic behind them::

    fs       = 500 Hz          one minute -> N = 30,000 samples
    FRAME    = 512  (1.024 s)  HOP = 500 (1.000 s)
    n_bins   = 257             df  = fs/FRAME = 0.977 Hz     f_max = 250 Hz
    n_frames = (30000-512)//500 + 1 = 59
    cells    = 59 x 257 = 15,163 doubles per window

HOP = fs makes one column exactly one second, so the time axis needs no
arithmetic to be correct. The cell count is nearly invariant to the choice --
cells ~ N x FRAME/(2*HOP) ~ N -- so FRAME/HOP only decides *where* the budget is
spent, not how much. 512/500 spends it on 0.977 Hz resolution, which resolves a
+/-2% RPM drift on the 1x line, and on 257 rows, which maps 1:1 onto a ~260 px
canvas.

For scale: the accelerometer pipeline's `accel_minute_spectrum` already stores
15,005 doubles per row across five arrays. This is the same order in one column.
"""

from __future__ import annotations

import os

import numpy as np

#: Read at **import time**. Every entry point must therefore call its
#: `defaults()` before importing this module -- `verify.py` asserts it. Get it
#: wrong and the frequency axis is silently scaled while everything still runs,
#: which is the worst failure mode available.
FS = float(os.environ.get("ENG_SAMPLE_RATE_HZ", "500"))
FRAME = int(os.environ.get("ENG_FRAME_SIZE", "512"))
HOP = int(os.environ.get("ENG_HOP_SIZE", "500"))

N_BINS = FRAME // 2 + 1
DF_HZ = FS / FRAME

#: Hann, built once. `GAIN` is what makes the output a physical unit rather than
#: an arbitrary scale: `|rfft(frame*w)| * 2/sum(w)` returns the amplitude in
#: g-peak for a tone at a bin centre, so the stored dB is re 1 g.
WINDOW = np.hanning(FRAME)
GAIN = WINDOW.sum()

#: Anything quieter than this is floor. -120 dB re 1 g is 1 micro-g, well below
#: any real sensor's noise, so it never clips a real measurement.
DB_FLOOR = -120.0

#: Stored to 0.1 dB. Legal here *because this model is already tier three* --
#: the "never wrap an aggregate in round()" rule is about foldable tier-1/2
#: state, and there is none here. It is lossless at display precision (no
#: 256-entry colour ramp can show 0.1 dB) and cuts parquet entropy sharply.
DB_DECIMALS = 1


def frame_count(n_samples: int) -> int:
    """How many frames `n_samples` yields. Negative counts clamp to zero."""
    if n_samples < FRAME:
        return 0
    return (n_samples - FRAME) // HOP + 1


def stft_db(values) -> np.ndarray:
    """One window of samples -> a flattened magnitude spectrogram in dB re 1 g.

    Layout, and four separate things depend on getting it right::

        value(frame f, bin b) = spec_db[f*N_BINS + b]        0-based (numpy, JS)
                              = spec_db[f*N_BINS + b + 1]    1-based (DuckDB SQL)

        frame f spans [window_ts + f*HOP/FS, window_ts + (f*HOP + FRAME)/FS)
        bin b is     b*FS/FRAME Hz

    A window shorter than one frame returns an **empty** array, not a
    zero-filled one: a partial minute is *no answer*, and `ArrowUDF(empty=...)`
    turns that into SQL NULL. Zeros would be a confident wrong answer.

    Returns an ndarray rather than a list. The Arrow wrapper calls
    `np.asarray(value)` anyway, so `.tolist()` would materialise 15,163 Python
    floats per window for nothing.
    """
    v = np.asarray(values, dtype=np.float64)
    n_frames = frame_count(v.size)
    if n_frames <= 0:
        return np.empty(0, dtype=np.float64)

    # Zero-copy view: (n_frames, FRAME) strided over `v`, no data movement.
    strided = np.lib.stride_tricks.sliding_window_view(v, FRAME)
    frames = strided[::HOP][:n_frames]

    # Per-frame mean removal, so bin 0 carries nothing. Without it DC dominates
    # the colour scale and every real feature is flattened -- the accelerometer
    # dashboard has to drop bin 0 at render time for exactly this reason, and
    # doing it here means nothing downstream has to know.
    frames = frames - frames.mean(axis=1, keepdims=True)

    # One rfft over the whole (n_frames, FRAME) block, not n_frames calls.
    spec = np.fft.rfft(frames * WINDOW, axis=1)

    mag = np.abs(spec) * (2.0 / GAIN)
    # Bins 0 and Nyquist are not doubled -- they have no negative-frequency twin.
    mag[:, 0] *= 0.5
    if FRAME % 2 == 0:
        mag[:, -1] *= 0.5

    db = 20.0 * np.log10(np.maximum(mag, 1e-12))
    np.maximum(db, DB_FLOOR, out=db)
    return np.round(db, DB_DECIMALS).ravel()


def stft_db_naive(values) -> np.ndarray:
    """The same thing, written the obvious way. Used only by `verify --check udf`.

    `stft_db` uses stride tricks and a single batched FFT; this loops one frame
    at a time. `verify` asserts the two are **bit-identical**, which is what
    makes the fast path trustworthy -- a stride-tricks bug that shifted every
    frame by one sample would otherwise produce a perfectly plausible picture.
    """
    v = np.asarray(values, dtype=np.float64)
    n_frames = frame_count(v.size)
    if n_frames <= 0:
        return np.empty(0, dtype=np.float64)

    out = np.empty((n_frames, N_BINS), dtype=np.float64)
    for f in range(n_frames):
        frame = v[f * HOP: f * HOP + FRAME]
        frame = frame - frame.mean()
        mag = np.abs(np.fft.rfft(frame * WINDOW)) * (2.0 / GAIN)
        mag[0] *= 0.5
        if FRAME % 2 == 0:
            mag[-1] *= 0.5
        db = 20.0 * np.log10(np.maximum(mag, 1e-12))
        out[f] = np.maximum(db, DB_FLOOR)
    return np.round(out, DB_DECIMALS).ravel()


def describe() -> dict:
    """The parameters, for logging and for `verify` to assert against."""
    return {
        "fs_hz": FS, "frame": FRAME, "hop": HOP,
        "n_bins": N_BINS, "df_hz": DF_HZ,
        "f_max_hz": FS / 2.0,
        "frame_seconds": FRAME / FS,
        "hop_seconds": HOP / FS,
        "frames_per_minute": frame_count(int(FS * 60)),
        "cells_per_minute": frame_count(int(FS * 60)) * N_BINS,
    }
