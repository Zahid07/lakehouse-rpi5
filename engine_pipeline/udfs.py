"""The tier-three registrar. Three lines of duckstream over `spectro.py`.

The maths lives in `spectro.py` with no duckstream import, so `verify --check
udf` can prove the calibration against an analytically known tone without a
catalog, a broker or an engine. This module only gives it a SQL name and a
return type, which a dotted path cannot carry.

**One UDF, one list aggregate, and that restraint is deliberate.** The
accelerometer model materialises five `LIST`s per window; this pipeline runs at
five times the row rate, so it spends its budget on exactly one channel.
`CONTEXT.md` 1.21 (memory follows the materialised list, not the row count) and
1.22 (a Python UDF costs 7.5x native on this hardware) are both five times
larger here than there.

`stft_db` was checked against `duckdb_functions()` and is free -- unlike
`entropy`, which `duckstream/udf.py` records as a real collision.
"""

from __future__ import annotations

from duckstream.udf import ArrowUDF

from engine_pipeline import spectro

#: `spectro.stft_db` returns an ndarray, not a list: the Arrow wrapper calls
#: `np.asarray` on whatever it gets, so `.tolist()` would materialise 15,163
#: Python floats per window for nothing.
stft_db = ArrowUDF("stft_db", spectro.stft_db, returns="DOUBLE[]")
