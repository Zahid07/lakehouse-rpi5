"""UDF registrars the tier-three scenarios name by dotted path.

A separate importable module rather than a fixture, because ``Model.udfs``
carries dotted paths and the YAML door resolves them in a **subprocess**: a
closure defined inside a test cannot be reached from there, and a scenario that
only worked through the Python door would quietly stop being a parity test.

``conftest.py`` puts this directory on ``sys.path``, so ``_udfs:spread`` resolves
from either door.

The functions are deliberately **order-dependent or non-decomposable**, because
that is the whole of tier three. ``spread`` is a range over a whole window,
``mid`` is a median, and ``first_last_gap`` depends on the order the rows arrive
in -- none of them can be reconstructed from partial answers, which is what makes
recomputing the window the only correct strategy.
"""

from __future__ import annotations

import numpy as np

from duckstream.udf import ArrowUDF

#: Peak-to-peak over the window. Not foldable: the range of two ranges is not
#: the range of the union unless you also kept both endpoints.
spread = ArrowUDF("ds_spread", lambda w: float(np.max(w) - np.min(w)))

#: A median, computed in Python so the model is tier three by way of a UDF
#: rather than by way of DuckDB's own ``median``. Both routes must behave the
#: same, and only this one exercises the UDF registration path.
mid = ArrowUDF("ds_mid", lambda w: float(np.median(w)))

#: The one that is genuinely order-dependent: last value minus first, in the
#: order the ``LIST(... ORDER BY ...)`` produced. A batch-at-a-time fold cannot
#: express it at all -- which is precisely ``CONTEXT.md`` section 4's FFT mart,
#: reduced to something a test can assert exactly.
first_last_gap = ArrowUDF("ds_gap", lambda w: float(w[-1] - w[0]))

#: A spectrum: the shape ``CONTEXT.md`` 1.2 measured at 2x in Arrow mode, and
#: the one whose bin *count* is a function of the window's row count -- so a
#: mart built from partial windows has visibly the wrong number of bins, which
#: is exactly how the production bug was eventually found (51 against 201).
spectrum = ArrowUDF(
    "ds_spectrum",
    lambda w: np.abs(np.fft.rfft(w)),
    returns="DOUBLE[]",
)
