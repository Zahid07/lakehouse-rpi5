import numpy as np
import duckdb
import typing

def register_fft_udfs(con: duckdb.DuckDBPyConnection):
    """Register FFT UDFs into DuckDB connection."""

    def fft_magnitude(values: list) -> list:
        return np.abs(np.fft.rfft(values)).tolist()

    def fft_freqs(values: list, sample_rate: float) -> list:
        n = len(values)
        return np.fft.rfftfreq(n, d=1.0 / sample_rate).tolist()

    LIST_DOUBLE = duckdb.list_type(duckdb.sqltype("DOUBLE"))

    con.create_function(
        "fft_magnitude",
        fft_magnitude,
        [LIST_DOUBLE],
        LIST_DOUBLE,
        null_handling="special"
    )

    con.create_function(
        "fft_freqs",
        fft_freqs,
        [LIST_DOUBLE, duckdb.sqltype("DOUBLE")],
        LIST_DOUBLE,
        null_handling="special"
    )

    print("  FFT UDFs registered: fft_magnitude, fft_freqs")
