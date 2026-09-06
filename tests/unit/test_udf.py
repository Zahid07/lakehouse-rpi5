"""Arrow-mode UDF helpers.

Tier three computes something no merge can reconstruct, over a whole window, and
``CONTEXT.md`` 2.2 establishes there is exactly one way to say that from Python:
``LIST(x ORDER BY t)`` into a scalar function. ``duckstream.udf`` packages the
shape 1.2 measured at **2x** the native one, so that nobody has to rediscover
why iterating an Arrow array is the slow way to read it.

The tests below are mostly about the edges the docs do not cover: a window that
is NULL, a window that is empty, a ``ChunkedArray`` arriving instead of an
``Array``, and a list return type that is officially undocumented and merely
verified.
"""

from __future__ import annotations

import duckdb
import numpy as np
import pyarrow as pa
import pytest

from duckstream.errors import DuckstreamError
from duckstream.udf import ArrowUDF, arrow_udf


@pytest.fixture
def con():
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE t AS "
        "SELECT i // 4 AS g, (i % 4)::DOUBLE AS v FROM range(12) s(i)"
    )
    try:
        yield connection
    finally:
        connection.close()


# --------------------------------------------------------------------------
# The two shapes
# --------------------------------------------------------------------------


def test_a_scalar_per_window(con):
    ArrowUDF("peak", lambda w: float(np.max(w))).register(con)
    rows = con.execute(
        "SELECT g, peak(list(v ORDER BY v)) FROM t GROUP BY g ORDER BY g"
    ).fetchall()
    assert rows == [(0, 3.0), (1, 3.0), (2, 3.0)]


def test_a_list_per_window(con):
    """``LIST(DOUBLE) -> LIST(DOUBLE)`` in Arrow mode is undocumented.

    ``CONTEXT.md`` 1.2 verified it on 1.5.5 and says to re-check it before the
    pin moves. This is that check, run every time the suite runs.
    """
    ArrowUDF(
        "spectrum", lambda w: np.abs(np.fft.rfft(w)), returns="DOUBLE[]"
    ).register(con)
    rows = con.execute(
        "SELECT g, spectrum(list(v ORDER BY v)) FROM t GROUP BY g ORDER BY g"
    ).fetchall()
    assert len(rows) == 3
    for _g, bins in rows:
        assert len(bins) == 3          # rfft of 4 samples gives 3 bins
        assert bins[0] == pytest.approx(6.0)


def test_the_result_matches_numpy_exactly(con):
    """The wrapper must not perturb the arithmetic it is only transporting."""
    ArrowUDF("total", lambda w: float(np.sum(w))).register(con)
    got = con.execute(
        "SELECT g, total(list(v)) FROM t GROUP BY g ORDER BY g"
    ).fetchall()
    truth = con.execute(
        "SELECT g, sum(v) FROM t GROUP BY g ORDER BY g"
    ).fetchall()
    assert got == truth


# --------------------------------------------------------------------------
# The edges
# --------------------------------------------------------------------------


def test_a_null_window_returns_the_empty_value(con):
    """NULL in, NULL out -- and the function is never asked to handle it.

    Registered with ``FunctionNullHandling.SPECIAL`` so the wrapper sees the
    NULL rather than DuckDB short-circuiting, because "no rows" and "no answer"
    are different questions and only the caller knows which one it means.
    """
    ArrowUDF("peak", lambda w: float(np.max(w))).register(con)
    assert con.execute("SELECT peak(NULL::DOUBLE[])").fetchone() == (None,)


def test_an_empty_window_returns_the_empty_value(con):
    """``np.max([])`` raises. The wrapper must never let the function see it."""
    ArrowUDF("peak", lambda w: float(np.max(w))).register(con)
    assert con.execute("SELECT peak([]::DOUBLE[])").fetchone() == (None,)


def test_the_empty_value_is_configurable(con):
    """Some functions do have a meaningful answer for no data. Most do not.

    The default is NULL because returning 0.0 for an absent window is a claim
    about data that is not there -- the failure mode this framework exists to
    remove. But a count-like function can say 0 honestly, so it can ask to.
    """
    ArrowUDF("howmany", lambda w: float(w.size), empty=0.0).register(con)
    assert con.execute("SELECT howmany([]::DOUBLE[])").fetchone() == (0.0,)
    assert con.execute("SELECT howmany(NULL::DOUBLE[])").fetchone() == (0.0,)


def test_mixed_null_and_real_windows_keep_their_positions(con):
    """The result must be exactly as long as the input, NULLs included.

    A short return is a length error from deep inside DuckDB with nothing
    pointing back at the function that caused it.
    """
    ArrowUDF("peak", lambda w: float(np.max(w))).register(con)
    rows = con.execute(
        "SELECT k, peak(vals) FROM (VALUES "
        "  (1, [1.0, 9.0]), (2, NULL), (3, []::DOUBLE[]), (4, [4.0])"
        ") AS v(k, vals) ORDER BY k"
    ).fetchall()
    assert rows == [(1, 9.0), (2, None), (3, None), (4, 4.0)]


def test_a_chunked_array_is_handled(con):
    """Inputs may arrive as ``ChunkedArray`` rather than ``Array``.

    Exercised directly rather than hoped for: whether DuckDB chunks a given
    query is an execution detail that can change between versions, so the
    wrapper is fed one by hand.
    """
    udf = ArrowUDF("peak", lambda w: float(np.max(w)))
    wrapper = udf._arrow_wrapper()
    chunked = pa.chunked_array(
        [pa.array([[1.0, 5.0]]), pa.array([[7.0, 2.0], None])],
        type=pa.list_(pa.float64()),
    )
    assert wrapper(chunked).to_pylist() == [5.0, 7.0, None]


def test_registration_is_idempotent(con):
    """Two models naming one UDF must not collide on the second registration."""
    udf = ArrowUDF("peak", lambda w: float(np.max(w)))
    udf.register(con)
    udf.register(con)
    assert con.execute("SELECT peak([2.0, 8.0])").fetchone() == (8.0,)


# --------------------------------------------------------------------------
# The registrar contract
# --------------------------------------------------------------------------


def test_an_instance_satisfies_both_halves_of_the_udfs_contract(con):
    """``Model.udfs`` accepts an object with ``register(con)`` or a callable.

    ``ArrowUDF`` is both, so a dotted path pointing at one works whichever way
    the engine chooses to call it.
    """
    udf = ArrowUDF("peak", lambda w: float(np.max(w)))
    assert callable(getattr(udf, "register", None))
    udf(con)  # the callable half
    assert con.execute("SELECT peak([3.0])").fetchone() == (3.0,)


def test_the_decorator_produces_a_registrar(con):
    # Not "entropy": DuckDB already has an aggregate by that name, and a
    # scalar function cannot shadow one. See the collision test below.
    @arrow_udf("win_entropy")
    def entropy(window):
        p = np.abs(window) / (np.abs(window).sum() or 1.0)
        p = p[p > 0]
        return float(-(p * np.log2(p)).sum())

    assert isinstance(entropy, ArrowUDF)
    entropy.register(con)
    value = con.execute("SELECT win_entropy([1.0, 1.0, 1.0, 1.0])").fetchone()[0]
    assert value == pytest.approx(2.0), "four equal parts is exactly 2 bits"


def test_the_engine_accepts_one_by_dotted_path(tmp_path, monkeypatch):
    """End to end through the contract the engine actually applies."""
    module = tmp_path / "sig_pkg.py"
    module.write_text(
        "import numpy as np\n"
        "from duckstream.udf import ArrowUDF\n"
        "peak = ArrowUDF('peak', lambda w: float(np.max(w)))\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    from duckstream.registry import resolve_udf

    registrar = resolve_udf("sig_pkg:peak")
    connection = duckdb.connect()
    try:
        registrar.register(connection)
        assert connection.execute("SELECT peak([1.0, 6.0])").fetchone() == (6.0,)
    finally:
        connection.close()


# --------------------------------------------------------------------------
# Refusals
# --------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["", "not a name", "has-dash", "select*"])
def test_a_name_that_is_not_an_identifier_is_refused(name):
    """The name is what an aggregate expression calls, unquoted."""
    with pytest.raises(DuckstreamError, match="identifier"):
        ArrowUDF(name, lambda w: 0.0)


def test_a_non_callable_is_refused():
    with pytest.raises(DuckstreamError, match="callable"):
        ArrowUDF("peak", "np.max")


def test_an_unmappable_return_type_says_what_to_do_instead():
    """The helper covers the common types; it does not pretend to cover all.

    Raised at construction rather than at registration, and certainly not on
    the first row of the first batch -- which is where it would have surfaced
    if the type were only resolved inside the wrapper.
    """
    with pytest.raises(DuckstreamError) as excinfo:
        ArrowUDF("odd", lambda w: 0.0, returns="STRUCT(a DOUBLE)")
    message = str(excinfo.value)
    assert "DOUBLE" in message
    assert "create_function" in message, (
        "a refusal has to say what to do instead, or it just gets worked around"
    )


def test_a_name_that_collides_with_a_builtin_aggregate_is_refused(con):
    """``entropy`` is an obvious name for a windowed UDF, and already taken.

    DuckDB refuses the registration itself, but its message -- "entropy is not
    an scalar function" -- describes the collision from the inside and never
    mentions that the *name* is the problem. Found by writing this module's own
    decorator test and naming the function the obvious thing.
    """
    udf = ArrowUDF("entropy", lambda w: 0.0)
    with pytest.raises(DuckstreamError) as excinfo:
        udf.register(con)
    message = str(excinfo.value)
    assert "already a DuckDB" in message and "aggregate" in message
    assert "entropy_win" in message, "the refusal should suggest a way out"


def test_a_name_that_shadows_a_builtin_scalar_is_allowed(con):
    """Only *aggregate* collisions are fatal; replacing a scalar is legal.

    Refusing both would be over-reach -- DuckDB itself permits the second.
    """
    ArrowUDF("degrees", lambda w: float(w.size)).register(con)
    assert con.execute("SELECT degrees([1.0, 2.0])").fetchone() == (2.0,)
