"""FFT UDFs for the tier-three mart, in Arrow mode.

Ported from ``utils/udf_registry.py``, which registers the same two functions in
**native** mode. The computation is identical -- ``numpy.fft.rfft`` and
``rfftfreq`` -- and the mode is the whole difference: ``CONTEXT.md`` 1.2 measured
the same transform both ways over 720,000 rows at **219.6 ms native against
106.5 ms Arrow**, byte-identical output, because the Arrow path never
materialises a Python float.

Read this alongside 1.22 before reaching for a wider mart. Measured on the Pi 5,
a Python UDF costs **7.5x** the equivalent native SQL at four threads and is
about half serial, so every extra axis here is paid for twice: once in the UDF
and once in the ``LIST`` that feeds it (1.21 -- memory follows the materialised
list structure, not the row count).

**Why ``fft_freqs`` takes no sample rate.** The original is
``fft_freqs(values, sample_rate)``, and :class:`~duckstream.udf.ArrowUDF` is
single-argument by design. The rate is a property of the sensor rather than of
the row, so it is closed over here from ``DUCKSTREAM_SAMPLE_RATE_HZ`` (default
100, matching ``config/dev.env``). That is not a workaround: passing the same
constant on every row of every window was always redundant, and a second
argument that must be identical within a group is a shape the aggregate cannot
enforce.
"""

from __future__ import annotations

import os

import numpy as np

from duckstream.udf import ArrowUDF

#: Sensor sample rate. ``config/dev.env`` carries the same number as
#: ``sample_rate_hz``; the env var is how a deployment overrides it without
#: editing code.
SAMPLE_RATE_HZ = float(os.environ.get("DUCKSTREAM_SAMPLE_RATE_HZ", "100"))


def _magnitude(window) -> list:
    """Magnitude spectrum of one whole window.

    The tier-three shape, and the reason this mart cannot fold: no pair of
    partial spectra combines into the spectrum of a concatenation.
    ``CONTEXT.md`` section 4 records what happens when somebody tries -- a
    one-minute window fed by 30-second batches held **51 spectrum bins where
    the truth was 201**, and it never failed, it was just wrong.
    """
    values = np.asarray(window, dtype=np.float64)
    if values.size == 0:
        return []
    return np.abs(np.fft.rfft(values)).tolist()


def _freqs(window) -> list:
    """The frequency axis for a window of this length, in Hz.

    Depends only on the sample count and the rate, so it is constant for every
    window of the same length -- stored per row for parity with the existing
    ``accel_fft_mart``, which a consumer already reads that way.
    """
    n = len(window)
    if n == 0:
        return []
    return np.fft.rfftfreq(n, d=1.0 / SAMPLE_RATE_HZ).tolist()


#: Registrars, named by dotted path in ``models.yaml``. They must be the
#: *registrars* and not the computation: a dotted path cannot carry a SQL name,
#: argument types or a return type, so the object at the end of it declares its
#: own signature.
fft_magnitude = ArrowUDF("fft_magnitude", _magnitude, returns="DOUBLE[]")
fft_freqs = ArrowUDF("fft_freqs", _freqs, returns="DOUBLE[]")
