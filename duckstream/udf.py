"""Arrow-mode UDF helpers: the shape that works, packaged so nobody re-derives it.

A ``non_foldable`` model computes something no merge can reconstruct -- an FFT,
a median, an entropy -- over a whole window. The only way to express that in
DuckDB from Python is ``LIST(x ORDER BY t)`` into a scalar function, and
``CONTEXT.md`` 2.2 explains why there is no alternative: ``create_function`` is
scalar-only and there is no ``create_aggregate_function`` in the Python API.
Custom aggregates are C++.

That leaves two decisions, and both were measured rather than reasoned:

**Arrow mode, not native.** ``CONTEXT.md`` 1.2 measured the same numpy ``rfft``
registered both ways over 720,000 rows: **219.6 ms native against 106.5 ms
Arrow**, byte-identical output. The Arrow version never materialises a Python
float -- it takes one zero-copy view over the flattened child buffer and slices
per row using the offsets buffer. That is the whole difference, and it is the
shape :class:`ArrowUDF` implements.

**It buys speed, not headroom.** ``CONTEXT.md`` 1.1 measured the memory ceiling
as DuckDB's buffer manager materialising the list aggregate: 256 MB for
``LIST(...)`` with *or* without a UDF, against 64 MB for a plain ``GROUP BY``.
A faster UDF does not help. Memory is controlled by bounding rows in flight, and
by nothing else -- so ``memory_profile: materialising`` on a tier-three model is
a statement about the ``LIST``, not about the function.

One more constraint worth knowing before reaching for this: a query containing a
Python UDF is forced onto a single thread (``CONTEXT.md`` 2.1, duckdb#14817).
On a four-core Pi that means one pipeline will not use the machine. Keep UDFs
off the hot path wherever a foldable tier applies, and parallelise across
processes rather than expecting intra-query threads.

Registering one
---------------

``Model.udfs`` carries dotted paths, and each must resolve to something that
**registers** SQL functions on a connection -- not to the computation itself.
That contract is not incidental: ``create_function`` needs a SQL name, argument
types and a return type, and a dotted path cannot carry any of them. So the
registrar declares its own signature::

    # my_pkg/signal.py
    import numpy as np
    from duckstream.udf import ArrowUDF

    spectrum = ArrowUDF("spectrum", lambda w: np.abs(np.fft.rfft(w)),
                        returns="DOUBLE[]")

and the model names it::

    Model(
        ...,
        udfs=["my_pkg.signal:spectrum"],
        aggregates={"bins": "spectrum(list(value ORDER BY event_ts))"},
        strategy="recompute_window",
        memory_profile="materialising",
    )
"""

from __future__ import annotations

from typing import Any, Callable

from duckstream.errors import DuckstreamError

__all__ = ["ArrowUDF", "LIST_DOUBLE", "arrow_udf"]

#: The argument type every helper here takes: one window's values, in order.
LIST_DOUBLE = "DOUBLE[]"


class ArrowUDF:
    """A registrar for one Arrow-mode scalar function over a list of doubles.

    Args:
        name: The SQL name. This is what an aggregate expression calls, and it
            is deliberately separate from the Python function's own name --
            the two have no reason to agree and pretending they do is how a
            rename becomes a Catalog Error at 03:00.
        fn: Called once per row with a ``numpy`` array of that row's values, and
            returns either a scalar or an array. It never sees a Python list, a
            ``None``, or a ragged shape -- the wrapper deals with all three.
        returns: The SQL return type. ``DOUBLE`` for a scalar per window,
            ``DOUBLE[]`` for a spectrum. ``LIST(DOUBLE) -> LIST(DOUBLE)`` in
            Arrow mode is **undocumented** and was verified working on 1.5.5
            (``CONTEXT.md`` 1.2); re-check it before moving the pinned version.
        empty: What to return for a window with no values. Defaults to ``None``,
            which is what SQL means by "no answer" -- returning 0.0 would be a
            claim about data that is not there, and this framework exists to not
            do that.

    The instance is the registrar the ``udfs`` contract expects: it exposes
    ``register(con)`` and the engine calls it before planning.
    """

    def __init__(
        self,
        name: str,
        fn: Callable[[Any], Any],
        *,
        returns: str = "DOUBLE",
        empty: Any = None,
    ) -> None:
        if not name or not name.replace("_", "").isalnum():
            raise DuckstreamError(
                f"UDF name {name!r} is not a plain SQL identifier. It is what "
                f"an aggregate expression calls, so it has to be spellable "
                f"there unquoted."
            )
        if not callable(fn):
            raise DuckstreamError(
                f"UDF {name!r} needs a callable to compute; got "
                f"{type(fn).__name__}"
            )
        self.name = name
        self.fn = fn
        self.returns = returns
        self.empty = empty
        # Resolved now rather than inside the wrapper, so an unmappable type is
        # a construction error naming the type -- not a surprise on the first
        # row of the first batch, hours later, from inside Arrow.
        _arrow_type(returns.replace("[]", ""))

    # -- the contract ------------------------------------------------------

    def register(self, con: Any) -> None:
        """Register this function on ``con``. Idempotent.

        Re-registering the same name would raise on a connection that already
        has it, and the engine registers per model -- so two models naming the
        same UDF would collide on the second one. Replacing is the behaviour
        that makes that a non-event.
        """
        # `duckdb.func`, not `duckdb.functional`. CONTEXT.md 1.2 wrote the
        # latter and it does not exist on 1.5.5 -- the measurement it records is
        # sound, the import line beside it was not, and nothing executed that
        # line until now.
        from duckdb.func import FunctionNullHandling, PythonUDFType

        self._refuse_builtin_collision(con)

        try:
            con.remove_function(self.name)
        except Exception:
            pass  # not registered yet, which is the normal case

        con.create_function(
            self.name,
            self._arrow_wrapper(),
            [LIST_DOUBLE],
            self.returns,
            type=PythonUDFType.ARROW,
            # The wrapper returns a value for a NULL window rather than letting
            # DuckDB short-circuit, because "no rows" and "no answer" are
            # different and only the function knows which it means.
            null_handling=FunctionNullHandling.SPECIAL,
        )

    def _refuse_builtin_collision(self, con: Any) -> None:
        """Refuse a name DuckDB already uses for an aggregate.

        DuckDB will not let a scalar function shadow a built-in aggregate, and
        the error it gives -- ``Catalog Error: entropy is not an scalar
        function`` -- describes the collision from the inside and does not
        mention that the name is the problem. ``entropy`` is a real example:
        an obvious name for a windowed UDF, and already taken.
        """
        try:
            taken = con.execute(
                "SELECT DISTINCT function_type FROM duckdb_functions() "
                "WHERE function_name = ?",
                [self.name],
            ).fetchall()
        except Exception:  # pragma: no cover - defensive
            return
        kinds = {row[0] for row in taken}
        if kinds and "scalar" not in kinds:
            raise DuckstreamError(
                f"UDF name {self.name!r} is already a DuckDB "
                f"{'/'.join(sorted(kinds))} function, and a scalar function "
                f"cannot shadow one. DuckDB reports this as "
                f"\"{self.name} is not an scalar function\", which describes "
                f"the collision from the inside and never mentions the name. "
                f"Pick another — prefixing your own is the usual answer, e.g. "
                f"{self.name}_win."
            )

    def __call__(self, con: Any) -> None:
        """So a bare instance also satisfies the callable-taking-con contract."""
        self.register(con)

    # -- the wrapper -------------------------------------------------------

    def _arrow_wrapper(self) -> Callable[[Any], Any]:
        """Slice the Arrow list array per row without building Python objects.

        This is ``CONTEXT.md`` 1.2's verified shape, and each line of it is
        there for a reason the docs do not state:

        * the input may arrive as a ``ChunkedArray`` rather than an ``Array``,
          so it is combined first;
        * ``flatten()`` plus the offsets buffer gives one zero-copy numpy view
          over every window's values at once, and slicing it per row costs
          nothing. Iterating the Arrow array instead would materialise a Python
          float per sample, which is the entire 2x;
        * the result must be exactly as long as the input, including the rows
          that were NULL or empty -- a short return is a length error from deep
          inside DuckDB with nothing pointing back here.
        """
        import numpy as np
        import pyarrow as pa

        fn, empty, returns = self.fn, self.empty, self.returns
        scalar = "[]" not in returns

        def wrapper(array: Any) -> Any:
            if isinstance(array, pa.ChunkedArray):
                array = array.combine_chunks()
            flat = array.flatten().to_numpy(zero_copy_only=False)
            offsets = array.offsets.to_numpy()
            valid = array.is_valid().to_pylist()

            out = []
            for index in range(len(array)):
                if not valid[index]:
                    out.append(empty)
                    continue
                window = flat[offsets[index] : offsets[index + 1]]
                out.append(empty if window.size == 0 else fn(window))

            if scalar:
                return pa.array(out, type=_arrow_type(returns))
            return pa.array(
                [None if value is None else np.asarray(value) for value in out],
                type=pa.list_(_arrow_type(returns.replace("[]", ""))),
            )

        wrapper.__name__ = f"{self.name}_arrow"
        return wrapper

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        return f"ArrowUDF({self.name!r}, returns={self.returns!r})"


def _arrow_type(sql_type: str) -> Any:
    import pyarrow as pa

    mapping = {
        "DOUBLE": pa.float64(),
        "FLOAT": pa.float32(),
        "BIGINT": pa.int64(),
        "INTEGER": pa.int32(),
    }
    try:
        return mapping[sql_type.strip().upper()]
    except KeyError:
        raise DuckstreamError(
            f"UDF return type {sql_type!r} is not one duckstream's Arrow helper "
            f"maps; expected one of {', '.join(mapping)} (optionally with '[]'). "
            f"For anything else, register the function yourself with "
            f"con.create_function — the udfs contract only asks for a registrar."
        ) from None


def arrow_udf(
    name: str,
    fn: Callable[[Any], Any] | None = None,
    *,
    returns: str = "DOUBLE",
    empty: Any = None,
) -> Any:
    """:class:`ArrowUDF` as a decorator, when the function is defined inline.

    ::

        @arrow_udf("spectrum", returns="DOUBLE[]")
        def spectrum(window):
            return np.abs(np.fft.rfft(window))

    ``spectrum`` is then the registrar, and ``my_pkg.signal:spectrum`` is what
    the model's ``udfs`` names.
    """
    if fn is not None:
        return ArrowUDF(name, fn, returns=returns, empty=empty)

    def decorate(inner: Callable[[Any], Any]) -> ArrowUDF:
        return ArrowUDF(name, inner, returns=returns, empty=empty)

    return decorate
