"""Foldability classification -- the framework's reason to exist.

An incremental merge is correct only when the aggregate forms a monoid over
batches: a partial result combined with a partial result must equal the true
combined result. Most streaming tools will happily let you fold something that
does not, and the result is a mart holding plausible, wrong numbers. Two real
examples from this repository are recorded in ``CONTEXT.md`` section 4 -- an
hourly mart that folded averages as ``(target.avg + source.avg) / 2`` and held
3.0 where the truth was 2.0, and an FFT mart that transformed only the current
batch and so held a spectrum over half a window.

This module classifies an aggregate expression into one of three tiers so the
engine can pick a strategy, and so a wrong strategy can be refused at load time
rather than discovered months later by a reconciliation query.

Classification parses; it does not pattern-match on text. DuckDB's own parser is
used via ``json_serialize_sql``, which returns the statement's AST as JSON. That
matters: a regular expression cannot reliably tell ``sum(a)`` from
``sum(a)/count(*)`` from ``my_udf(list(v ORDER BY ts))``, and getting that
distinction wrong is exactly the failure this module exists to prevent.

Membership of the aggregate-function set also comes from DuckDB
(``duckdb_functions()``), not from a hardcoded list, so aggregates added by an
extension are recognised too. Anything DuckDB does not know is a user UDF and is
treated as ``non_foldable``: the rule is *unknown means non-foldable, never
additive*.
"""

from __future__ import annotations

import json
import threading
from enum import StrEnum
from functools import lru_cache
from typing import Any, Iterator, NamedTuple

from duckstream.errors import ModelValidationError

__all__ = [
    "Tier",
    "FunctionRef",
    "STRATEGY_FOR_TIER",
    "STRATEGIES",
    "ADDITIVE_FUNCTIONS",
    "SUFFICIENT_STATISTIC_FUNCTIONS",
    "aggregate_functions",
    "unknown_functions",
    "classify_expression",
    "classify_model",
    "fold_expression",
    "strategy_satisfies_tier",
    "worst_aggregate",
]


class Tier(StrEnum):
    """How an aggregate may be maintained incrementally.

    Ordered worst-last: :func:`classify_expression` takes the worst tier present
    in an expression, so the ordering below is load-bearing.

    A :class:`enum.StrEnum`, not a ``(str, Enum)`` mixin, so that ``str(tier)``
    and ``f"{tier}"`` give ``"additive"`` rather than ``"Tier.ADDITIVE"``. That
    matters because tiers are rendered into log lines, CLI output and config.
    Note that pyyaml refuses to dump *any* Enum -- ``SafeRepresenter`` matches
    on exact type -- so pass ``str(tier)`` when serialising.
    """

    ADDITIVE = "additive"
    SUFFICIENT_STATISTICS = "sufficient_statistics"
    NON_FOLDABLE = "non_foldable"


_TIER_SEVERITY: dict[Tier, int] = {
    Tier.ADDITIVE: 0,
    Tier.SUFFICIENT_STATISTICS: 1,
    Tier.NON_FOLDABLE: 2,
}


STRATEGY_FOR_TIER: dict[Tier, str] = {
    Tier.ADDITIVE: "delta_merge",
    Tier.SUFFICIENT_STATISTICS: "sufficient_statistics",
    Tier.NON_FOLDABLE: "recompute_window",
}

#: Every strategy name the framework accepts, in increasing order of cost.
STRATEGIES: tuple[str, ...] = (
    "delta_merge",
    "sufficient_statistics",
    "recompute_window",
)

#: How much work each strategy does, so a model can be checked for whether its
#: declared strategy is at least as strong as its tier requires.
_STRATEGY_STRENGTH: dict[str, int] = {
    "delta_merge": 0,
    "sufficient_statistics": 1,
    "recompute_window": 2,
}


#: Aggregates that fold by combining two partial results directly. ``DISTINCT``
#: destroys this for every one of them, because de-duplication is not local to a
#: batch, so the distinct flag is checked separately.
ADDITIVE_FUNCTIONS: frozenset[str] = frozenset(
    {
        "count",
        "count_star",
        "sum",
        "min",
        "max",
        "bit_and",
        "bit_or",
        "bit_xor",
        "bool_and",
        "bool_or",
    }
)

#: Aggregates that are exactly reconstructible from additive components
#: (``count``, ``sum``, ``sum_sq``, and the cross terms for the bivariate ones).
#: Still no rescan of the source is needed -- but the *stored* value must be the
#: components, never the derived number.
SUFFICIENT_STATISTIC_FUNCTIONS: frozenset[str] = frozenset(
    {
        "avg",
        "mean",
        "stddev",
        "stddev_samp",
        "stddev_pop",
        "var_samp",
        "var_pop",
        "variance",
        "corr",
        "covar_pop",
        "covar_samp",
    }
)


class FunctionRef(NamedTuple):
    """One function node found in an expression's AST.

    ``is_aggregate`` reflects DuckDB's own catalog: it is true only for names
    ``duckdb_functions()`` reports with ``function_type = 'aggregate'``. Scalar
    operators show up as function nodes too -- ``sum(a)/count(*)`` contains a
    ``/`` node -- which is precisely why this flag exists.
    """

    name: str
    distinct: bool
    is_aggregate: bool


# ---------------------------------------------------------------------------
# The parser connection.
#
# One module-level in-memory connection, opened lazily and reused. Opening a
# DuckDB connection per expression would dominate validation cost, and the
# function catalogue it is queried for does not change within a process.
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_con: Any = None
_aggregate_names: frozenset[str] | None = None
_known_names: frozenset[str] | None = None


def _connection() -> Any:
    global _con
    if _con is None:
        with _lock:
            if _con is None:
                import duckdb  # lazy: model.py must be importable without duckdb

                _con = duckdb.connect(":memory:")
    return _con


def _catalogue() -> tuple[frozenset[str], frozenset[str]]:
    """``(aggregate names, all known function names)``, cached for the process."""
    global _aggregate_names, _known_names
    if _aggregate_names is None or _known_names is None:
        rows = _connection().execute(
            "SELECT lower(function_name), function_type FROM duckdb_functions()"
        ).fetchall()
        _aggregate_names = frozenset(n for n, t in rows if t == "aggregate")
        _known_names = frozenset(n for n, _ in rows)
    return _aggregate_names, _known_names


#: Statement-level clauses that must be absent. An aggregate expression is
#: wrapped in ``SELECT <expr>`` purely so DuckDB will parse it -- anything the
#: user manages to attach to that synthetic statement is SQL smuggled past
#: validation, and would either fail at runtime or, worse, execute with a scope
#: the engine never authorised.
_FORBIDDEN_CLAUSES: tuple[tuple[str, str], ...] = (
    ("where_clause", "a WHERE clause"),
    ("having", "a HAVING clause"),
    ("qualify", "a QUALIFY clause"),
    ("sample", "a SAMPLE clause"),
)


def _parse(expr: str) -> dict[str, Any]:
    """Return the AST of the single select-list item in ``expr``.

    ``json_serialize_sql`` reports parse failures in-band as ``error: true``
    rather than raising, so a malformed expression never escapes as a raw duckdb
    exception -- the caller always gets a :class:`ModelValidationError` naming
    the expression and the parser's own message.

    The parse is also where SQL smuggling is caught. ``SELECT {expr}`` is a
    synthetic statement, so every part of it other than the single select-list
    item must be empty; see :func:`_reject_smuggled_sql`.
    """
    if not expr.strip():
        raise ModelValidationError(
            "aggregate expression is empty",
            field="aggregates",
            remedy="Give a SQL aggregate expression, for example 'sum(value)'.",
        )

    try:
        raw = _connection().execute(
            "SELECT json_serialize_sql(?)", [f"SELECT {expr}"]
        ).fetchone()
    except Exception as exc:  # pragma: no cover - serialisation is total in 1.5.5
        raise ModelValidationError(
            f"could not parse aggregate expression {expr!r}: {exc}",
            field="aggregates",
        ) from None

    doc = json.loads(raw[0])
    if doc.get("error"):
        message = doc.get("error_message") or "unparseable expression"
        raise ModelValidationError(
            f"could not parse aggregate expression {expr!r}: {message}",
            field="aggregates",
            remedy="Fix the SQL. The expression is parsed by DuckDB itself, so "
            "anything valid in a SELECT list is accepted.",
        )

    statements = doc.get("statements") or []
    if len(statements) != 1:
        raise ModelValidationError(
            f"aggregate expression {expr!r} must be one expression, not "
            f"{len(statements)} statements",
            field="aggregates",
        )
    node = statements[0].get("node", {})
    select_list = node.get("select_list") or []
    if len(select_list) != 1:
        raise ModelValidationError(
            f"aggregate expression {expr!r} produces {len(select_list)} columns; "
            f"exactly one is required",
            field="aggregates",
            remedy="Declare one entry in `aggregates` per output column.",
        )

    root = select_list[0]
    _reject_smuggled_sql(expr, node, root)
    return root


def _reject_smuggled_sql(
    expr: str, node: dict[str, Any], root: dict[str, Any]
) -> None:
    """Refuse anything that is not a self-contained expression over the batch.

    Every case below used to be accepted, and each is a live hazard:

    ``count(*) FROM other_table``
        The ``FROM`` is parsed and kept. Validation passed and the statement
        then failed at runtime, long after ``duckstream validate`` had said the
        model was fine.
    ``count(*) AS x``
        Same shape. The alias is discarded by the engine, which names output
        columns from the ``aggregates`` keys, so the declaration and the mart
        would quietly disagree.
    ``sum((SELECT max(v) FROM other_table))``
        The dangerous one. It parses, classifies, and *executes* -- reading a
        table outside the micro-batch, so the value depends on when the trigger
        happened to run rather than on the batch. It also lands squarely in
        ``CONTEXT.md`` section 1.5: a scalar subquery inside a DuckLake MERGE
        gives ``Out of buffer``, and only on the second batch, which is exactly
        the failure a single-batch test cannot see.
    ``sum(x) OVER ()``
        A window function is AST class ``WINDOW``, not ``FUNCTION``, so it
        carries no aggregate node and used to be reported as "contains no
        aggregate function" -- true, but a misleading diagnosis.
    """
    remedy = (
        "An aggregate expression must be self-contained over the current batch: "
        "an aggregate call over the source's own columns and nothing else. The "
        "engine supplies the FROM, the grouping and the output column name."
    )

    from_type = (node.get("from_table") or {}).get("type")
    if from_type not in (None, "EMPTY"):
        raise ModelValidationError(
            f"aggregate expression {expr!r} has its own FROM clause "
            f"({from_type}); it may only read the batch the engine binds",
            field="aggregates",
            remedy=remedy,
        )

    for key, description in _FORBIDDEN_CLAUSES:
        if node.get(key) is not None:
            raise ModelValidationError(
                f"aggregate expression {expr!r} carries {description}, which is "
                f"not part of an aggregate expression",
                field="aggregates",
                remedy=remedy,
            )

    if node.get("group_expressions") or node.get("group_sets"):
        raise ModelValidationError(
            f"aggregate expression {expr!r} carries its own GROUP BY; grouping "
            f"is set by the model's `key`, not by the expression",
            field="aggregates",
            remedy=remedy,
        )

    if node.get("modifiers") or (node.get("cte_map") or {}).get("map"):
        raise ModelValidationError(
            f"aggregate expression {expr!r} carries statement modifiers "
            f"(ORDER BY, LIMIT or a CTE), which an aggregate expression cannot",
            field="aggregates",
            remedy=remedy,
        )

    if root.get("alias"):
        raise ModelValidationError(
            f"aggregate expression {expr!r} declares the alias "
            f"{root['alias']!r}; the output column name comes from the key in "
            f"`aggregates`, so an alias here would be silently ignored",
            field="aggregates",
            remedy=remedy,
        )

    for sub_node in _walk(root):
        node_class = sub_node.get("class")
        if node_class == "SUBQUERY":
            raise ModelValidationError(
                f"aggregate expression {expr!r} contains a subquery. A subquery "
                f"reads outside the micro-batch, so the value would depend on "
                f"when the trigger ran rather than on the batch's own rows",
                field="aggregates",
                remedy=remedy,
            )
        if node_class == "WINDOW":
            raise ModelValidationError(
                f"aggregate expression {expr!r} is a window function, which is "
                f"not supported as an aggregate expression. A window function "
                f"produces one row per input row rather than one per group, so "
                f"it has no foldability tier",
                field="aggregates",
                remedy=remedy,
            )


def _walk(node: Any) -> Iterator[dict[str, Any]]:
    """Pre-order walk over every node of a serialised expression tree."""
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


class _Analysis(NamedTuple):
    """Everything classification needs from one parse of an expression."""

    refs: tuple[FunctionRef, ...]
    is_bare_aggregate: bool
    unknown: tuple[str, ...]


def _analysis_for(expr: Any) -> _Analysis:
    """Type-check ``expr``, then delegate to the cached parse.

    The guard has to live *outside* the cache. ``lru_cache`` hashes its argument
    before the wrapped function runs, so a guard inside :func:`_analyse` never
    got the chance to fire: a non-string escaped as a raw ``TypeError:
    unhashable type: 'list'`` instead of a :class:`ModelValidationError`. YAML
    produces exactly that shape -- ``n: [count(*)]`` is a list, not a string --
    so the config loader is the first thing to hit it.
    """
    if not isinstance(expr, str):
        raise ModelValidationError(
            f"aggregate expression must be a SQL string, not "
            f"{type(expr).__name__} ({expr!r})",
            field="aggregates",
            remedy="In YAML, write the expression as a plain scalar: "
            "`n: \"count(*)\"`. A list or mapping here is usually a stray "
            "dash or an over-indented line.",
        )
    return _analyse(expr)


@lru_cache(maxsize=512)
def _analyse(expr: str) -> _Analysis:
    """Parse ``expr`` once and cache what classification reads off it.

    Validation, folding and SQL generation all ask the same questions about the
    same handful of expressions, and each parse is a round trip through DuckDB.

    Call :func:`_analysis_for` rather than this directly -- see its docstring
    for why the type guard cannot live here.
    """
    root = _parse(expr)
    aggregate_names, known = _catalogue()

    refs: list[FunctionRef] = []
    for node in _walk(root):
        if node.get("class") != "FUNCTION":
            continue
        name = str(node.get("function_name", "")).lower()
        if not name:
            continue
        refs.append(
            FunctionRef(
                name=name,
                distinct=bool(node.get("distinct", False)),
                is_aggregate=name in aggregate_names,
            )
        )

    return _Analysis(
        refs=tuple(refs),
        is_bare_aggregate=_bareness(root, refs, known),
        unknown=tuple(dict.fromkeys(r.name for r in refs if r.name not in known)),
    )


def _bareness(
    root: dict[str, Any], refs: list[FunctionRef], known: frozenset[str]
) -> bool:
    """True when the whole expression is one aggregate call and nothing else.

    Bareness is read off the **root AST node**, not off the first function node,
    so a wrapper that is not itself a function still counts as wrapping:
    ``cast(sum(a) AS INTEGER)`` is a ``CAST`` node and is not bare, even though
    its only function node is an additive aggregate. That matters -- folding
    ``cast(sum(a) AS INTEGER)`` per batch rounds each delta before adding, which
    is not the same number as rounding the total once.

    ``sum(a)``, ``sum(a + b)`` and ``sum(a) FILTER (WHERE c)`` are bare;
    ``sum(a)/count(*)``, ``sum(a) + 1`` and the cast above are not. Nested
    aggregates cannot occur inside an aggregate in SQL, so it is enough that the
    root is an aggregate call carrying no other tier beneath it.
    """
    if root.get("class") != "FUNCTION":
        return False
    root_name = str(root.get("function_name", "")).lower()
    aggregate_names, _ = _catalogue()
    if root_name not in aggregate_names:
        return False
    # The root itself is refs[0]; anything else carrying a tier means the
    # aggregate is not alone in the expression.
    return not any(r.is_aggregate or r.name not in known for r in refs[1:])


def aggregate_functions(expr: str) -> list[FunctionRef]:
    """Every function node in ``expr``, pre-order, flagged as aggregate or not.

    Scalar operators appear as function nodes in DuckDB's AST, so they are
    returned too::

        aggregate_functions("count(*)")
            -> [("count_star", False, True)]
        aggregate_functions("count(DISTINCT id)")
            -> [("count", True, True)]
        aggregate_functions("sum(a)/count(*)")
            -> [("/", False, False), ("sum", False, True),
                ("count_star", False, True)]
        aggregate_functions("arrow_fft(list(v ORDER BY ts))")
            -> [("arrow_fft", False, False), ("list", False, True)]

    Raises :class:`ModelValidationError` if ``expr`` does not parse.
    """
    return list(_analysis_for(expr).refs)


def unknown_functions(expr: str) -> list[str]:
    """Function names in ``expr`` that DuckDB's catalog does not contain.

    These are user UDFs. They make an expression ``non_foldable`` on their own,
    and :class:`~duckstream.model.Model` additionally refuses a model that
    references one without declaring anything in ``udfs`` -- otherwise the model
    validates cleanly and then dies at runtime with a Catalog Error.
    """
    return list(_analysis_for(expr).unknown)


def _tier_of_function(ref: FunctionRef, known: frozenset[str]) -> Tier | None:
    """Tier contributed by one function node, or ``None`` if it contributes none.

    A scalar function DuckDB knows about contributes no tier of its own -- it is
    the wrapping rule in :func:`classify_expression` that downgrades such an
    expression. A name DuckDB does *not* know is a user UDF, which may well be a
    transform over a whole window, so it is non-foldable. Unknown never means
    additive.
    """
    if ref.name not in known:
        return Tier.NON_FOLDABLE
    if not ref.is_aggregate:
        return None
    if ref.distinct:
        # DISTINCT is not local to a batch: de-duplicating within a batch and
        # then folding double-counts any value appearing in two batches.
        return Tier.NON_FOLDABLE
    if ref.name in ADDITIVE_FUNCTIONS:
        return Tier.ADDITIVE
    if ref.name in SUFFICIENT_STATISTIC_FUNCTIONS:
        return Tier.SUFFICIENT_STATISTICS
    return Tier.NON_FOLDABLE


def classify_expression(expr: str) -> Tier:
    """Classify one aggregate expression into its foldability tier.

    The rules, in order:

    1. The expression must parse, and must carry at least one tier -- an
       aggregate, or an unknown function assumed to be a user aggregate. An
       expression with no aggregate at all is a :class:`ModelValidationError`:
       there is nothing to maintain incrementally.
    2. The tier is the **worst** tier present. ``sum(a) + median(b)`` is
       ``non_foldable``.
    3. Only a **bare** aggregate call -- the whole expression being one call
       and nothing else -- can be ``additive`` or ``sufficient_statistics``.
       Anything wrapping aggregates in arithmetic, a cast, a ``CASE`` or a
       scalar function is ``non_foldable``.

    Rule 3 is deliberately blunt. Both tiers one and two name a concrete way to
    maintain the value: fold the delta in, or store ``count``/``sum``/``sum_sq``
    and derive on read. Neither survives wrapping. ``sum(a)/count(*)`` has
    additive components but a ratio does not fold, and for ``max(a)+max(b)``
    there is no decomposition at all -- calling it "sufficient statistics" would
    name a strategy that cannot be written. ``non_foldable`` means recompute the
    window, which is always correct and merely slower, so erring here costs time
    rather than correctness. This is the mart bug from ``CONTEXT.md`` section 4
    encoded as a rule: the taxonomy stays honest about what it can actually do.
    """
    analysis = _analysis_for(expr)
    _, known = _catalogue()

    tiers = [
        t
        for t in (_tier_of_function(r, known) for r in analysis.refs)
        if t is not None
    ]
    if not tiers:
        raise ModelValidationError(
            f"aggregate expression {expr!r} contains no aggregate function",
            field="aggregates",
            remedy="Every entry in `aggregates` must aggregate rows, for example "
            "'sum(value)' or 'count(*)'. A plain column belongs in `key`.",
        )

    if not analysis.is_bare_aggregate:
        # Neither foldable tier can express a wrapped value: tier one stores one
        # number and adds into it, tier two stores fixed components. A scalar
        # wrapper defeats both, so recompute the window instead.
        return Tier.NON_FOLDABLE

    # A bare call has exactly one tier-carrying function -- the root -- so the
    # worst tier present is that call's own tier.
    return max(tiers, key=lambda t: _TIER_SEVERITY[t])


def classify_model(aggregates: dict[str, str]) -> tuple[Tier, dict[str, Tier]]:
    """Classify every output column: ``(model tier, per-column tiers)``.

    The model's tier is the worst of its columns, because the engine runs one
    strategy per model: a single median in an otherwise additive model makes the
    whole model ``non_foldable``.
    """
    if not aggregates:
        raise ModelValidationError(
            "no aggregates declared",
            field="aggregates",
            remedy="Declare at least one output column, e.g. {'n': 'count(*)'}.",
        )

    per_column: dict[str, Tier] = {}
    for column, expr in aggregates.items():
        try:
            per_column[column] = classify_expression(expr)
        except ModelValidationError as exc:
            raise ModelValidationError.bad_expression(
                model=None,
                column=column,
                expression=expr if isinstance(expr, str) else repr(expr),
                detail=exc.reason,
                remedy=exc.remedy,
            ) from None

    model_tier = max(per_column.values(), key=lambda t: _TIER_SEVERITY[t])
    return model_tier, per_column


def worst_aggregate(expr: str) -> str:
    """Name the function responsible for ``expr``'s tier, for error messages.

    A rejection is only actionable if it points at the offending aggregate, so
    ``"avg(x) + median(y)"`` reports ``median`` and ``"count(DISTINCT id)"``
    reports ``count(DISTINCT ...)`` rather than a bare ``count``.
    """
    analysis = _analysis_for(expr)
    _, known = _catalogue()

    worst_ref: FunctionRef | None = None
    worst_rank = -1
    for ref in analysis.refs:
        tier = _tier_of_function(ref, known)
        if tier is None:
            continue
        if _TIER_SEVERITY[tier] > worst_rank:
            worst_rank, worst_ref = _TIER_SEVERITY[tier], ref

    if worst_ref is None:
        return expr
    if not analysis.is_bare_aggregate and worst_rank < _TIER_SEVERITY[Tier.NON_FOLDABLE]:
        # Every aggregate present is foldable on its own; what demoted the
        # expression is the scalar wrapping, so say that rather than blaming an
        # innocent `sum`.
        return f"a scalar expression wrapping {worst_ref.name}(...)"
    if worst_ref.distinct:
        return f"{worst_ref.name}(DISTINCT ...)"
    return worst_ref.name


def strategy_satisfies_tier(strategy: str, tier: Tier) -> bool:
    """Whether ``strategy`` is a *correct* way to maintain ``tier``.

    A stronger strategy than the tier needs is allowed: recomputing an additive
    model's windows is correct, merely slower, and a user may well want it while
    reconciling. A weaker one is refused -- that is the whole point.
    """
    if strategy not in _STRATEGY_STRENGTH:
        return False
    return _STRATEGY_STRENGTH[strategy] >= _TIER_SEVERITY[tier]


# ---------------------------------------------------------------------------
# Fold generation
# ---------------------------------------------------------------------------


def _quote(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _absorbing(operation: str, lhs: str, rhs: str) -> str:
    """Wrap a binary fold so that a NULL operand yields the other operand.

    Every SQL binary operator is NULL-propagating: ``NULL + 5`` is ``NULL``, not
    ``5``. In a fold that is catastrophic rather than merely wrong once. A
    single batch whose delta is all-NULL -- a quiet interval, a sensor dropping
    out, a column absent from one file -- sets the stored total to NULL, and
    every later ``WHEN MATCHED`` then folds into that NULL and stays NULL. The
    running total is destroyed permanently by one empty batch.

    ``coalesce(t.c + s.c, t.c, s.c)`` gives the monoid its identity back: both
    present adds them, either NULL yields the other, both NULL stays NULL. That
    is the same behaviour ``least``/``greatest`` already have, so all the folds
    now agree with each other, and it matches what the aggregates themselves do
    -- ``sum`` over a batch of NULLs is NULL, and adding it to a total must be a
    no-op, not an erasure.
    """
    return f"coalesce({operation}, {lhs}, {rhs})"


def _fold_sql(name: str, lhs: str, rhs: str) -> str:
    if name in ("sum", "count", "count_star"):
        return _absorbing(f"{lhs} + {rhs}", lhs, rhs)
    # least/greatest already ignore NULL operands, so they need no wrapper.
    if name == "min":
        return f"least({lhs}, {rhs})"
    if name == "max":
        return f"greatest({lhs}, {rhs})"
    if name == "bit_and":
        return _absorbing(f"{lhs} & {rhs}", lhs, rhs)
    if name == "bit_or":
        return _absorbing(f"{lhs} | {rhs}", lhs, rhs)
    if name == "bit_xor":
        return _absorbing(f"xor({lhs}, {rhs})", lhs, rhs)
    if name == "bool_and":
        return _absorbing(f"{lhs} AND {rhs}", lhs, rhs)
    if name == "bool_or":
        return _absorbing(f"{lhs} OR {rhs}", lhs, rhs)
    raise ModelValidationError(  # pragma: no cover - unreachable via public API
        f"no fold is defined for aggregate {name!r}",
        field="aggregates",
    )


def fold_expression(column: str, expr: str, target: str, source: str) -> str:
    """SQL folding a batch delta into the stored value -- additive tier only.

    ``fold_expression("total", "sum(value)", "t", "s")`` gives
    ``t."total" + s."total"``.

    ``target`` and ``source`` are emitted verbatim so they may be aliases or
    qualified names; ``column`` is quoted. NULL handling is deliberately left to
    the caller building the MERGE: this function encodes the monoid operation
    and nothing else.

    Raises :class:`ModelValidationError` for anything not additive. That refusal
    is the point -- a caller must not be able to obtain a fold for an average
    and silently get a wrong one. ``CONTEXT.md`` section 4 records a production
    mart that folded averages as ``(target.avg + source.avg) / 2`` and held 3.0
    where the truth was 2.0.
    """
    tier = classify_expression(expr)
    if tier is not Tier.ADDITIVE:
        raise ModelValidationError(
            f"cannot fold column {column!r}: expression {expr!r} classifies as "
            f"tier {tier.value!r}, and only {Tier.ADDITIVE.value!r} aggregates "
            f"fold by combining a stored value with a delta",
            field="aggregates",
            remedy=f"Use strategy {STRATEGY_FOR_TIER[tier]!r} for this model "
            f"instead of folding this column.",
        )

    outermost = _analysis_for(expr).refs[0]
    return _fold_sql(
        outermost.name,
        f"{target}.{_quote(column)}",
        f"{source}.{_quote(column)}",
    )
