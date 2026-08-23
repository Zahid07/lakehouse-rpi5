"""SQL construction primitives: identifier quoting, literal rendering, name splitting.

Every module that builds SQL needs these four functions, and hand-rolling them
per module is how an injection bug or a broken identifier gets in. They live
here, alone, with no duckdb import and no duckstream import beyond
:mod:`duckstream.errors`, so anything may depend on them.

Two design points are load-bearing rather than stylistic.

**Literals exist because bounds cannot be subqueries.** ``CONTEXT.md`` section
1.5 measured a window-scoped ``MERGE`` whose join condition read
``ON f.timestamp >= (SELECT lo FROM bounds)``. Against DuckLake that raises
``Out of buffer`` — and only on the *second* merge, the first one to take the
``WHEN MATCHED`` branch. The identical statement passes on in-memory DuckDB. The
fix is to compute bounds in Python and inline them, which is exactly what
:func:`quote_literal` is for. It also lets DuckLake prune data files on
timestamp statistics, so it is faster as well as correct.

**Nothing is rendered by accident.** :func:`quote_literal` refuses a type it does
not know rather than falling back to ``str(value)``. A silent ``str()`` is the
mechanism by which an unexpected object — a user-supplied offset, a numpy
scalar, a dict — becomes raw SQL text. Extending the accepted set is a
deliberate edit to this file.
"""

from __future__ import annotations

import datetime as _dt
import math
from decimal import Decimal
from typing import Any

from duckstream.errors import DuckstreamError

__all__ = [
    "quote_ident",
    "qualified",
    "quote_literal",
    "split_qualified",
    "DEFAULT_SCHEMA",
]


#: Where an unqualified table name lives when nothing says otherwise. DuckDB's
#: own default schema, so ``TableSink("counts")`` behaves like plain SQL would.
DEFAULT_SCHEMA = "main"


# --------------------------------------------------------------------------
# Identifiers
# --------------------------------------------------------------------------


def quote_ident(name: Any) -> str:
    """Render ``name`` as one double-quoted SQL identifier.

    Embedded double quotes are doubled, which is the SQL standard escape and
    what DuckDB implements, so ``He said "hi"`` comes back wrapped in quotes
    with each inner quote written twice.
    Everything else — spaces, dots, semicolons, unicode, mixed case — is
    preserved verbatim, and quoting is what makes that safe. Mixed case
    survives: an unquoted identifier is folded to lower case by DuckDB, a quoted
    one is not.

    Note that the *whole* string becomes a single identifier. ``quote_ident``
    never splits on a dot; use :func:`qualified` or :func:`split_qualified` when
    the input may be schema-qualified.

    Raises:
        DuckstreamError: if ``name`` is not a string, is empty, or contains a
            NUL. An empty identifier is always a bug rather than a legal
            oddity, and DuckDB strings cannot carry a NUL at all, so both are
            refused here instead of producing a statement that fails obscurely
            much later.
    """
    if not isinstance(name, str):
        raise DuckstreamError(
            f"SQL identifier must be a string, got {type(name).__name__}: {name!r}"
        )
    if name == "":
        raise DuckstreamError("SQL identifier is empty")
    if "\x00" in name:
        raise DuckstreamError(f"SQL identifier contains a NUL byte: {name!r}")
    return '"' + name.replace('"', '""') + '"'


def _split_parts(name: str) -> list[str]:
    """Split a possibly-quoted qualified name into its unescaped parts.

    Handles the four shapes that actually turn up:

    ==========================  ==========================
    input                       parts
    ==========================  ==========================
    ``marts.hourly``            ``['marts', 'hourly']``
    ``"odd schema"."a.b"``      ``['odd schema', 'a.b']``
    ``Weird Name``              ``['Weird Name']``
    ``he"llo``                  ``['he"llo']``
    ==========================  ==========================

    Quoting only takes effect when a part *starts* with ``"``; a quote appearing
    later is an ordinary character. That is what makes the last row work — a
    user writing a raw table name with a quote in it should not have to know
    this function exists.
    """
    parts: list[str] = []
    buf: list[str] = []
    in_quotes = False
    quoted_part = False
    index = 0
    length = len(name)

    while index < length:
        char = name[index]
        if in_quotes:
            if char == '"':
                if index + 1 < length and name[index + 1] == '"':
                    buf.append('"')
                    index += 2
                    continue
                in_quotes = False
                index += 1
                continue
            buf.append(char)
            index += 1
            continue
        if char == '"' and not buf and not quoted_part:
            in_quotes = True
            quoted_part = True
            index += 1
            continue
        if char == ".":
            parts.append("".join(buf))
            buf = []
            quoted_part = False
            index += 1
            continue
        buf.append(char)
        index += 1

    if in_quotes:
        raise DuckstreamError(
            f"unterminated quoted identifier in {name!r}. A double quote opens a "
            f'quoted name and must be closed by another; write "" for a literal '
            f"quote inside one."
        )
    parts.append("".join(buf))
    return parts


def qualified(*parts: Any) -> str:
    """Build a quoted, dotted name such as ``"marts"."hourly"``.

    Two calling styles, and the difference matters:

    - **One string** is parsed as a possibly-qualified name, so
      ``qualified("marts.hourly")`` gives ``"marts"."hourly"`` and
      ``qualified("hourly")`` gives ``"hourly"``. Already-quoted input is
      understood, so ``qualified('"odd schema"."a.b"')`` round-trips.
    - **Several arguments** are each taken verbatim as one identifier, so
      ``qualified("marts", "a.b")`` gives ``"marts"."a.b"`` — the dot stays
      inside the table name. Pass parts separately whenever the name may
      legitimately contain a dot.

    ``None`` parts are skipped, so ``qualified(schema_or_none, table)`` works
    without the caller branching.

    Unlike :func:`split_qualified` this never invents a schema: a bare name
    stays bare, which is what a temporary view or a CTE alias needs.
    """
    given = [part for part in parts if part is not None]
    if not given:
        raise DuckstreamError("qualified() needs at least one name part")
    if len(given) == 1 and isinstance(given[0], str):
        pieces = _split_parts(given[0])
    else:
        pieces = list(given)
    return ".".join(quote_ident(piece) for piece in pieces)


def split_qualified(
    name: Any, *, default_schema: str = DEFAULT_SCHEMA
) -> tuple[str, str]:
    """Split ``"marts.hourly"`` into ``("marts", "hourly")``, unquoted.

    A bare name gets ``default_schema``, matching what DuckDB would resolve it
    to. Quoted parts are understood and unescaped, so
    ``split_qualified('"odd schema"."a.b"')`` gives ``("odd schema", "a.b")``.

    The returned parts are *raw* names, not SQL — feed them to
    :func:`quote_ident` or :func:`qualified` before they reach a statement.
    Returning raw names is deliberate: a schema name also has to be compared
    against catalog metadata, where the quoting would be wrong.

    Raises:
        DuckstreamError: on an empty name, an empty part (``marts.``), or more
            than two parts. Three-part names are refused rather than silently
            reinterpreted, because ``lake.marts.hourly`` means the caller is
            trying to pick a catalog and duckstream decides that by ``USE``.
    """
    if not isinstance(name, str) or not name.strip():
        raise DuckstreamError(f"table name must be a non-empty string, got {name!r}")
    parts = _split_parts(name)
    if any(part == "" for part in parts):
        raise DuckstreamError(
            f"table name {name!r} has an empty part; expected 'table' or "
            f"'schema.table'"
        )
    if len(parts) == 1:
        return default_schema, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise DuckstreamError(
        f"table name {name!r} has {len(parts)} parts; expected 'table' or "
        f"'schema.table'. duckstream chooses the catalog with USE, so a "
        f"catalog-qualified name is not accepted here."
    )


# --------------------------------------------------------------------------
# Literals
# --------------------------------------------------------------------------


def _quote_string(value: str) -> str:
    if "\x00" in value:
        raise DuckstreamError(
            "string literal contains a NUL byte, which DuckDB strings cannot hold"
        )
    return "'" + value.replace("'", "''") + "'"


def _quote_float(value: float) -> str:
    if math.isnan(value):
        return "CAST('NaN' AS DOUBLE)"
    if math.isinf(value):
        return "CAST('%s' AS DOUBLE)" % ("Infinity" if value > 0 else "-Infinity")
    return repr(value)


def quote_literal(value: Any) -> str:
    """Render ``value`` as a SQL literal that can be pasted into a statement.

    This is the safe alternative to a scalar subquery. ``CONTEXT.md`` section
    1.5 measured that a ``(SELECT ...)`` inside a DuckLake ``MERGE`` join
    condition fails with ``Out of buffer`` on the second batch, so window bounds
    and any other computed constant are worked out in Python and inlined here.

    Supported, in the order the checks run:

    ==========================  ================================================
    Python                      SQL
    ==========================  ================================================
    ``None``                    ``NULL``
    ``bool``                    ``TRUE`` / ``FALSE``
    ``int``                     ``42``
    ``float``                   ``1.5``, ``CAST('NaN' AS DOUBLE)``, ``±Infinity``
    ``Decimal``                 ``1.50`` (scale preserved)
    ``str``                     ``'it''s'``
    ``bytes`` / ``bytearray``   ``CAST('\\x00\\xff' AS BLOB)``
    ``datetime`` (naive)        ``TIMESTAMP '2026-01-01 00:00:00'``
    ``datetime`` (aware)        ``TIMESTAMPTZ '2026-01-01 00:00:00+00:00'``
    ``date``                    ``DATE '2026-01-01'``
    ``time``                    ``TIME '00:00:00'``
    ``timedelta``               ``INTERVAL '86400000000 microseconds'``
    ==========================  ================================================

    ``bool`` is checked before ``int`` and ``datetime`` before ``date`` because
    each is a subclass of the other and the wrong order would silently render
    ``True`` as ``1`` and a timestamp as a date — the second of which would
    truncate a window bound to midnight.

    Raises:
        DuckstreamError: for any other type. Falling back to ``str(value)`` is
            precisely how an unexpected object becomes executable SQL, so an
            unknown type is a hard error and adding one is an edit to this
            function.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return _quote_float(value)
    if isinstance(value, Decimal):
        if value.is_nan() or value.is_infinite():
            return _quote_float(float(value))
        return str(value)
    if isinstance(value, str):
        return _quote_string(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        escaped = "".join("\\x%02x" % byte for byte in raw)
        return f"CAST('{escaped}' AS BLOB)"
    if isinstance(value, _dt.datetime):
        rendered = value.isoformat(sep=" ")
        keyword = "TIMESTAMPTZ" if value.tzinfo is not None else "TIMESTAMP"
        return f"{keyword} {_quote_string(rendered)}"
    if isinstance(value, _dt.date):
        return f"DATE {_quote_string(value.isoformat())}"
    if isinstance(value, _dt.time):
        return f"TIME {_quote_string(value.isoformat())}"
    if isinstance(value, _dt.timedelta):
        microseconds = (
            value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds
        )
        return f"INTERVAL '{microseconds} microseconds'"
    raise DuckstreamError(
        f"cannot render {type(value).__name__} as a SQL literal: {value!r}. "
        f"duckstream refuses unknown types rather than calling str() on them, "
        f"because that is how an unexpected object becomes raw SQL. Convert it "
        f"to a supported type at the call site."
    )
