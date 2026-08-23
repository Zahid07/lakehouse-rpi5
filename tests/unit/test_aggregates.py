"""Tests for the foldability classifier.

These are the specification for the taxonomy in ``PLAN.md``. They are written
table-driven on purpose: the value of the classifier is that its verdict is the
same for every expression a user might write, so the interesting content is the
table, not the assertions around it.
"""

from __future__ import annotations

import pytest

from duckstream.aggregates import (
    ADDITIVE_FUNCTIONS,
    SUFFICIENT_STATISTIC_FUNCTIONS,
    STRATEGY_FOR_TIER,
    Tier,
    aggregate_functions,
    classify_expression,
    classify_model,
    fold_expression,
    strategy_satisfies_tier,
    unknown_functions,
    worst_aggregate,
)
from duckstream.errors import DuckstreamError, ModelValidationError

# ---------------------------------------------------------------------------
# The tier table
# ---------------------------------------------------------------------------

TIER_CASES: list[tuple[str, Tier]] = [
    # -- additive: fold by combining stored value with delta -----------------
    ("count(*)", Tier.ADDITIVE),
    ("count(value)", Tier.ADDITIVE),
    ("sum(value)", Tier.ADDITIVE),
    ("min(value)", Tier.ADDITIVE),
    ("max(value)", Tier.ADDITIVE),
    ("bit_and(flags)", Tier.ADDITIVE),
    ("bit_or(flags)", Tier.ADDITIVE),
    ("bit_xor(flags)", Tier.ADDITIVE),
    ("bool_and(ok)", Tier.ADDITIVE),
    ("bool_or(ok)", Tier.ADDITIVE),
    # a scalar expression *inside* the aggregate is still additive: it is
    # applied per row, before any folding happens
    ("sum(a + b)", Tier.ADDITIVE),
    ("sum(abs(value))", Tier.ADDITIVE),
    # FILTER selects rows, which is also per-row and therefore foldable
    ("sum(value) FILTER (WHERE value > 0)", Tier.ADDITIVE),
    # -- sufficient statistics: store the components, derive on read ---------
    ("avg(value)", Tier.SUFFICIENT_STATISTICS),
    ("mean(value)", Tier.SUFFICIENT_STATISTICS),
    ("stddev(value)", Tier.SUFFICIENT_STATISTICS),
    ("stddev_samp(value)", Tier.SUFFICIENT_STATISTICS),
    ("stddev_pop(value)", Tier.SUFFICIENT_STATISTICS),
    ("var_samp(value)", Tier.SUFFICIENT_STATISTICS),
    ("var_pop(value)", Tier.SUFFICIENT_STATISTICS),
    ("variance(value)", Tier.SUFFICIENT_STATISTICS),
    ("corr(a, b)", Tier.SUFFICIENT_STATISTICS),
    ("covar_pop(a, b)", Tier.SUFFICIENT_STATISTICS),
    ("covar_samp(a, b)", Tier.SUFFICIENT_STATISTICS),
    # -- non-foldable: no shortcut exists, recompute the window --------------
    ("median(value)", Tier.NON_FOLDABLE),
    ("quantile(value, 0.5)", Tier.NON_FOLDABLE),
    ("quantile_cont(value, 0.9)", Tier.NON_FOLDABLE),
    ("quantile_disc(value, 0.9)", Tier.NON_FOLDABLE),
    ("approx_count_distinct(id)", Tier.NON_FOLDABLE),
    ("list(value)", Tier.NON_FOLDABLE),
    ("array_agg(value)", Tier.NON_FOLDABLE),
    ("string_agg(name, ',')", Tier.NON_FOLDABLE),
    ("first(value)", Tier.NON_FOLDABLE),
    ("last(value)", Tier.NON_FOLDABLE),
    ("arg_min(value, ts)", Tier.NON_FOLDABLE),
    ("arg_max(value, ts)", Tier.NON_FOLDABLE),
    # DISTINCT is never local to a batch, so it demotes even additive names
    ("count(DISTINCT id)", Tier.NON_FOLDABLE),
    ("sum(DISTINCT value)", Tier.NON_FOLDABLE),
    # a function DuckDB does not know is a user UDF: unknown means
    # non-foldable, never additive
    ("arrow_fft(list(value ORDER BY ts))", Tier.NON_FOLDABLE),
    ("totally_made_up_function(value)", Tier.NON_FOLDABLE),
    # the worst tier present wins
    ("sum(a) + median(b)", Tier.NON_FOLDABLE),
    ("avg(a) + median(b)", Tier.NON_FOLDABLE),
    # -- the wrapping rule ---------------------------------------------------
    # Only a bare call can be foldable. Both foldable tiers name a concrete
    # maintenance strategy -- fold the delta in, or store count/sum/sum_sq and
    # derive on read -- and neither survives a scalar wrapper. `sum(a)/count(*)`
    # has additive components but a ratio does not fold; `max(a)+max(b)` has no
    # decomposition at all. Recomputing the window is always correct, so this
    # errs towards cost rather than towards wrong numbers.
    ("sum(a)/count(*)", Tier.NON_FOLDABLE),
    ("sum(a) + 1", Tier.NON_FOLDABLE),
    ("sum(a) - sum(b)", Tier.NON_FOLDABLE),
    ("max(a) + max(b)", Tier.NON_FOLDABLE),
    ("cast(sum(a) AS INTEGER)", Tier.NON_FOLDABLE),
    ("coalesce(sum(a), 0)", Tier.NON_FOLDABLE),
    ("CASE WHEN count(*) > 0 THEN sum(a) ELSE 0 END", Tier.NON_FOLDABLE),
    ("-sum(a)", Tier.NON_FOLDABLE),
    ("median(a) / 2", Tier.NON_FOLDABLE),
    ("avg(a) * 2", Tier.NON_FOLDABLE),
]


@pytest.mark.parametrize("expr,expected", TIER_CASES, ids=[c[0] for c in TIER_CASES])
def test_tier_table(expr: str, expected: Tier) -> None:
    assert classify_expression(expr) is expected


def test_tier_two_is_exactly_a_bare_call_to_the_whitelist() -> None:
    """Tier two names a strategy: store count/sum/sum_sq, derive on read.

    That strategy only exists for a bare call to one of the eleven functions
    PLAN.md lists. Wrapping one takes the expression out of tier two rather than
    keeping it there -- there is no decomposition of `avg(a) * 2` that the
    sufficient-statistics machinery could maintain.
    """
    for name in SUFFICIENT_STATISTIC_FUNCTIONS:
        arity_two = name in ("corr", "covar_pop", "covar_samp")
        expr = f"{name}(a, b)" if arity_two else f"{name}(a)"
        assert classify_expression(expr) is Tier.SUFFICIENT_STATISTICS, name
        assert classify_expression(f"{expr} * 2") is Tier.NON_FOLDABLE, name


def test_additive_function_set_is_complete_and_conservative() -> None:
    """Every name we call additive really does classify as additive, alone."""
    for name in ADDITIVE_FUNCTIONS:
        expr = "count(*)" if name == "count_star" else f"{name}(value)"
        assert classify_expression(expr) is Tier.ADDITIVE, name


def test_strategy_for_tier_is_total() -> None:
    assert STRATEGY_FOR_TIER == {
        Tier.ADDITIVE: "delta_merge",
        Tier.SUFFICIENT_STATISTICS: "sufficient_statistics",
        Tier.NON_FOLDABLE: "recompute_window",
    }
    assert set(STRATEGY_FOR_TIER) == set(Tier)


def test_tier_is_a_str_enum_that_renders_as_its_value() -> None:
    """`str(tier)` is what reaches log lines, CLI output and config.

    Under a plain ``(str, Enum)`` mixin this gave ``'Tier.ADDITIVE'``, which is
    not a tier name and does not round-trip through anything.
    """
    assert Tier.ADDITIVE == "additive"
    assert Tier("non_foldable") is Tier.NON_FOLDABLE

    for tier in Tier:
        assert str(tier) == tier.value
        assert f"{tier}" == tier.value
        assert "{}".format(tier) == tier.value
        assert f"tier={tier}" == f"tier={tier.value}"


def test_tier_survives_a_yaml_round_trip_as_a_string() -> None:
    import yaml

    for tier in Tier:
        text = yaml.safe_dump({"tier": str(tier)}, sort_keys=False)
        assert yaml.safe_load(text)["tier"] == tier


def test_pyyaml_will_not_dump_the_enum_object_itself() -> None:
    """Pinned so nobody assumes otherwise.

    pyyaml's SafeRepresenter dispatches on *exact* type, so it refuses every
    Enum -- StrEnum included, despite `isinstance(tier, str)` being true.
    Callers serialising a tier must pass `str(tier)` or `tier.value`; the
    previous test shows that now produces the right string.
    """
    import yaml

    with pytest.raises(yaml.representer.RepresenterError):
        yaml.safe_dump(Tier.ADDITIVE)


# ---------------------------------------------------------------------------
# Parsing, not regexing
# ---------------------------------------------------------------------------


def test_aggregate_functions_walks_the_real_ast() -> None:
    assert aggregate_functions("count(*)") == [("count_star", False, True)]
    assert aggregate_functions("count(DISTINCT id)") == [("count", True, True)]
    # the arithmetic operator is a FUNCTION node too, which is exactly why a
    # regex cannot do this job
    assert aggregate_functions("sum(a)/count(*)") == [
        ("/", False, False),
        ("sum", False, True),
        ("count_star", False, True),
    ]
    assert aggregate_functions("arrow_fft(list(v ORDER BY ts))") == [
        ("arrow_fft", False, False),
        ("list", False, True),
    ]


def test_aggregate_functions_flags_scalars_as_non_aggregate() -> None:
    refs = {r.name: r.is_aggregate for r in aggregate_functions("sum(abs(a)) + 1")}
    assert refs["sum"] is True
    assert refs["abs"] is False
    assert refs["+"] is False


def test_a_string_that_looks_like_sql_is_not_enough() -> None:
    """A column literally named 'sum' is not an aggregate; parsing knows that."""
    with pytest.raises(ModelValidationError):
        classify_expression("sum")


@pytest.mark.parametrize(
    "expr",
    ["sum(", "count(*", "sum(a))", "", "   ", "SELECT 1 FROM t"],
)
def test_malformed_expressions_raise_model_validation_error(expr: str) -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        classify_expression(expr)
    # never a raw duckdb exception, and the message names the expression
    assert isinstance(excinfo.value, DuckstreamError)
    if expr.strip():
        assert repr(expr) in str(excinfo.value)


def test_parse_error_message_carries_duckdb_s_own_diagnosis() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        classify_expression("sum(")
    message = str(excinfo.value)
    assert "'sum('" in message
    assert "syntax error" in message.lower()


SMUGGLING_CASES: list[tuple[str, str]] = [
    # id -> expression that must not survive validation
    ("own_from", "count(*) FROM other_table"),
    ("alias", "count(*) AS x"),
    ("scalar_subquery", "sum((SELECT max(v) FROM other_table))"),
    ("subquery_operand", "sum(a) + (SELECT 1)"),
    ("in_subquery", "count(*) FILTER (WHERE a IN (SELECT id FROM other_table))"),
    ("own_where", "count(*) WHERE 1=1"),
    ("own_group_by", "count(*) FROM t GROUP BY a"),
    ("statement_modifier", "count(*) FROM t ORDER BY 1"),
    ("cte", "count(*) FROM (WITH c AS (SELECT 1) SELECT * FROM c)"),
]


@pytest.mark.parametrize(
    "expr", [e for _, e in SMUGGLING_CASES], ids=[i for i, _ in SMUGGLING_CASES]
)
def test_an_aggregate_expression_cannot_smuggle_sql_past_validation(
    expr: str,
) -> None:
    """`SELECT {expr}` is synthetic; everything but the select item must be empty.

    Accepting these was a hole rather than a nicety. The FROM and alias cases
    validated and then failed at runtime, long after `duckstream validate` had
    passed. The subquery cases are worse: they validate, classify *and execute*,
    reading outside the micro-batch -- and a scalar subquery inside a DuckLake
    MERGE is CONTEXT.md section 1.5, `Out of buffer` on the second batch only.
    """
    with pytest.raises(ModelValidationError) as excinfo:
        classify_expression(expr)
    message = str(excinfo.value)
    assert repr(expr) in message
    assert "self-contained" in message


def test_the_subquery_rejection_says_why() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        classify_expression("sum((SELECT max(v) FROM other_table))")
    assert "subquery" in str(excinfo.value)
    assert "outside the micro-batch" in str(excinfo.value)


def test_a_legitimate_expression_is_not_caught_by_the_smuggling_checks() -> None:
    """The checks must not fire on the constructs an aggregate legitimately uses."""
    for expr in (
        "sum(a) FILTER (WHERE b > 1)",
        "list(v ORDER BY ts)",
        "sum(CASE WHEN a > 1 THEN a END)",
        "count(DISTINCT id)",
        "string_agg(name, ',' ORDER BY ts)",
    ):
        classify_expression(expr)


# ---- window functions -----------------------------------------------------


@pytest.mark.parametrize(
    "expr",
    [
        "sum(x) OVER ()",
        "sum(x) OVER (PARTITION BY g)",
        "row_number() OVER (ORDER BY ts)",
    ],
)
def test_a_window_function_is_refused_with_the_right_diagnosis(expr: str) -> None:
    """A window function is AST class WINDOW, not FUNCTION.

    It was already refused, but as "contains no aggregate function" -- true, and
    useless: the user wrote an aggregate name and would go looking for a typo.
    """
    with pytest.raises(ModelValidationError) as excinfo:
        classify_expression(expr)
    message = str(excinfo.value)
    assert "window function" in message
    assert "no aggregate function" not in message


def test_expression_with_no_aggregate_is_rejected() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        classify_expression("value + 1")
    assert "no aggregate function" in str(excinfo.value)


@pytest.mark.parametrize(
    "expr", [["count(*)"], {"n": "count(*)"}, ("count(*)",), 42, None, 3.5, {1, 2}]
)
def test_a_non_string_expression_raises_model_validation_error(expr: object) -> None:
    """YAML produces these shapes, so the config loader hits them first.

    The guard has to sit outside the lru_cache: the cache hashes its argument
    before the wrapped function body runs, so a check inside it never fired and
    a list escaped as `TypeError: unhashable type: 'list'`.
    """
    with pytest.raises(ModelValidationError) as excinfo:
        classify_expression(expr)  # type: ignore[arg-type]
    assert "must be a SQL string" in str(excinfo.value)


def test_the_unhashable_case_specifically_does_not_raise_type_error() -> None:
    try:
        classify_expression(["count(*)"])  # type: ignore[arg-type]
    except ModelValidationError:
        pass
    except TypeError as exc:  # pragma: no cover - the regression
        pytest.fail(f"leaked a raw TypeError instead of a validation error: {exc}")


def test_classify_model_rejects_a_non_string_expression() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        classify_model({"n": ["count(*)"]})  # type: ignore[dict-item]
    assert "'n'" in str(excinfo.value)


def test_expression_producing_two_columns_is_rejected() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        classify_expression("sum(a), sum(b)")
    assert "exactly one" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Model-level classification
# ---------------------------------------------------------------------------


def test_classify_model_returns_worst_tier_and_per_column_detail() -> None:
    tier, columns = classify_model(
        {"n": "count(*)", "mean_v": "avg(v)", "p50": "median(v)"}
    )
    assert tier is Tier.NON_FOLDABLE
    assert columns == {
        "n": Tier.ADDITIVE,
        "mean_v": Tier.SUFFICIENT_STATISTICS,
        "p50": Tier.NON_FOLDABLE,
    }


def test_classify_model_of_all_additive_columns_is_additive() -> None:
    tier, columns = classify_model({"n": "count(*)", "total": "sum(v)"})
    assert tier is Tier.ADDITIVE
    assert set(columns.values()) == {Tier.ADDITIVE}


def test_classify_model_rejects_an_empty_declaration() -> None:
    with pytest.raises(ModelValidationError):
        classify_model({})


def test_classify_model_names_the_offending_column() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        classify_model({"n": "count(*)", "oops": "value"})
    message = str(excinfo.value)
    assert "'oops'" in message
    assert "'value'" in message


# ---------------------------------------------------------------------------
# Strategy compatibility
# ---------------------------------------------------------------------------


def test_a_stronger_strategy_than_the_tier_needs_is_allowed() -> None:
    # correct, merely slower -- a reasonable thing to ask for while reconciling
    assert strategy_satisfies_tier("recompute_window", Tier.ADDITIVE)
    assert strategy_satisfies_tier("sufficient_statistics", Tier.ADDITIVE)
    assert strategy_satisfies_tier("recompute_window", Tier.SUFFICIENT_STATISTICS)


def test_a_weaker_strategy_than_the_tier_needs_is_refused() -> None:
    assert not strategy_satisfies_tier("delta_merge", Tier.SUFFICIENT_STATISTICS)
    assert not strategy_satisfies_tier("delta_merge", Tier.NON_FOLDABLE)
    assert not strategy_satisfies_tier("sufficient_statistics", Tier.NON_FOLDABLE)


def test_an_unknown_strategy_never_satisfies_anything() -> None:
    assert not strategy_satisfies_tier("delta_merge_fast", Tier.ADDITIVE)


# ---------------------------------------------------------------------------
# Folding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "expr,expected",
    [
        ("sum(value)", 'coalesce(t."v" + s."v", t."v", s."v")'),
        ("count(*)", 'coalesce(t."v" + s."v", t."v", s."v")'),
        ("count(value)", 'coalesce(t."v" + s."v", t."v", s."v")'),
        # least/greatest already ignore a NULL operand, so no wrapper is needed
        ("min(value)", 'least(t."v", s."v")'),
        ("max(value)", 'greatest(t."v", s."v")'),
        ("bit_and(flags)", 'coalesce(t."v" & s."v", t."v", s."v")'),
        ("bit_or(flags)", 'coalesce(t."v" | s."v", t."v", s."v")'),
        ("bit_xor(flags)", 'coalesce(xor(t."v", s."v"), t."v", s."v")'),
        ("bool_and(ok)", 'coalesce(t."v" AND s."v", t."v", s."v")'),
        ("bool_or(ok)", 'coalesce(t."v" OR s."v", t."v", s."v")'),
    ],
)
def test_fold_expression_for_each_additive_aggregate(expr: str, expected: str) -> None:
    assert fold_expression("v", expr, "t", "s") == expected


def test_fold_expression_quotes_the_column_but_not_the_qualifier() -> None:
    assert (
        fold_expression("total rows", "sum(v)", "target", "source")
        == 'coalesce(target."total rows" + source."total rows", '
        'target."total rows", source."total rows")'
    )
    assert (
        fold_expression('we"ird', "sum(v)", "t", "s")
        == 'coalesce(t."we""ird" + s."we""ird", t."we""ird", s."we""ird")'
    )


# ---- NULL safety, executed rather than asserted on the string -------------

NULL_FOLD_CASES: list[tuple[str, str, object, object]] = [
    # expression, column SQL type, stored total, value a NULL delta must leave
    ("sum(value)", "BIGINT", 300, 300),
    ("count(*)", "BIGINT", 300, 300),
    ("min(value)", "BIGINT", 7, 7),
    ("max(value)", "BIGINT", 7, 7),
    ("bit_and(flags)", "INTEGER", 6, 6),
    ("bit_or(flags)", "INTEGER", 6, 6),
    ("bit_xor(flags)", "INTEGER", 6, 6),
    ("bool_and(ok)", "BOOLEAN", True, True),
    ("bool_or(ok)", "BOOLEAN", False, False),
]


@pytest.mark.parametrize(
    "expr,sql_type,stored,expected",
    NULL_FOLD_CASES,
    ids=[c[0] for c in NULL_FOLD_CASES],
)
def test_a_null_delta_never_destroys_the_running_total(
    expr: str, sql_type: str, stored: object, expected: object
) -> None:
    """One quiet batch must not wipe the stored value -- permanently.

    Every SQL binary operator propagates NULL, so a bare `t.c + s.c` turns the
    total to NULL the first time a batch aggregates to NULL (a quiet interval, a
    sensor dropping out, a column missing from one file). Every later
    WHEN MATCHED then folds into that NULL and stays NULL: the running total is
    destroyed by a single empty batch, which is the CONTEXT.md section 4 bug
    class reappearing inside the fold itself.
    """
    import duckdb

    con = duckdb.connect()
    fold = fold_expression("c", expr, "t", "s")
    row = con.execute(
        f"SELECT {fold} FROM (SELECT ?::{sql_type} AS c) t, "
        f"(SELECT NULL::{sql_type} AS c) s",
        [stored],
    ).fetchone()
    assert row[0] == expected, f"{fold} destroyed the stored value"


@pytest.mark.parametrize(
    "expr,sql_type,stored,expected",
    NULL_FOLD_CASES,
    ids=[c[0] for c in NULL_FOLD_CASES],
)
def test_a_null_stored_value_is_replaced_by_the_first_real_delta(
    expr: str, sql_type: str, stored: object, expected: object
) -> None:
    """The mirror case: NULL is the identity, so folding is symmetric."""
    import duckdb

    con = duckdb.connect()
    fold = fold_expression("c", expr, "t", "s")
    row = con.execute(
        f"SELECT {fold} FROM (SELECT NULL::{sql_type} AS c) t, "
        f"(SELECT ?::{sql_type} AS c) s",
        [stored],
    ).fetchone()
    assert row[0] == expected


def test_two_null_operands_stay_null() -> None:
    """NULL is the identity, not zero: duckstream must not invent a value."""
    import duckdb

    con = duckdb.connect()
    for expr, sql_type, _, _ in NULL_FOLD_CASES:
        fold = fold_expression("c", expr, "t", "s")
        row = con.execute(
            f"SELECT {fold} FROM (SELECT NULL::{sql_type} AS c) t, "
            f"(SELECT NULL::{sql_type} AS c) s"
        ).fetchone()
        assert row[0] is None, expr


def test_the_fold_still_actually_folds_two_real_values() -> None:
    """NULL safety must not have broken the arithmetic it wraps."""
    import duckdb

    con = duckdb.connect()
    expectations = [
        ("sum(value)", "BIGINT", 300, 100, 400),
        ("min(value)", "BIGINT", 7, 3, 3),
        ("max(value)", "BIGINT", 7, 3, 7),
        ("bit_and(flags)", "INTEGER", 6, 3, 2),
        ("bit_or(flags)", "INTEGER", 6, 3, 7),
        ("bit_xor(flags)", "INTEGER", 6, 3, 5),
        ("bool_and(ok)", "BOOLEAN", True, False, False),
        ("bool_or(ok)", "BOOLEAN", False, True, True),
    ]
    for expr, sql_type, left, right, expected in expectations:
        fold = fold_expression("c", expr, "t", "s")
        row = con.execute(
            f"SELECT {fold} FROM (SELECT ?::{sql_type} AS c) t, "
            f"(SELECT ?::{sql_type} AS c) s",
            [left, right],
        ).fetchone()
        assert row[0] == expected, f"{expr}: {fold}"


def test_folding_a_sequence_of_batches_matches_a_single_recompute() -> None:
    """The monoid property, checked end to end with NULL batches interleaved."""
    import duckdb

    con = duckdb.connect()
    fold = fold_expression("c", "sum(value)", "t", "s")
    total: object = None
    for delta in (100, None, 200, None, None, 50):
        row = con.execute(
            f"SELECT {fold} FROM (SELECT ?::BIGINT AS c) t, "
            f"(SELECT ?::BIGINT AS c) s",
            [total, delta],
        ).fetchone()
        total = row[0]
    assert total == 350

@pytest.mark.parametrize(
    "expr",
    [
        "avg(value)",
        "stddev(value)",
        "median(value)",
        "count(DISTINCT id)",
        "sum(a)/count(*)",
        "arrow_fft(list(value ORDER BY ts))",
    ],
)
def test_fold_expression_refuses_everything_that_is_not_additive(expr: str) -> None:
    """The caller must not be able to silently obtain a wrong fold.

    ``CONTEXT.md`` section 4: a production mart folded averages as
    ``(target.avg + source.avg) / 2`` and held 3.0 where the truth was 2.0.
    """
    with pytest.raises(ModelValidationError) as excinfo:
        fold_expression("v", expr, "t", "s")
    message = str(excinfo.value)
    assert repr(expr) in message
    assert classify_expression(expr).value in message


def test_the_average_of_averages_bug_cannot_be_generated() -> None:
    with pytest.raises(ModelValidationError) as excinfo:
        fold_expression("avg_value", "avg(value)", "target", "source")
    assert "'avg_value'" in str(excinfo.value)
    assert "sufficient_statistics" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Error-message quality
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "expr,expected_fragment",
    [
        ("median(value)", "median"),
        ("count(DISTINCT id)", "count(DISTINCT ...)"),
        ("arrow_fft(list(value ORDER BY ts))", "arrow_fft"),
        ("avg(a) + median(b)", "median"),
        ("sum(a)/count(*)", "a scalar expression wrapping sum(...)"),
        ("avg(a) * 2", "a scalar expression wrapping avg(...)"),
        # a genuinely non-foldable aggregate outranks the wrapping
        ("median(a) / 2", "median"),
    ],
)
def test_worst_aggregate_points_at_the_culprit(
    expr: str, expected_fragment: str
) -> None:
    assert expected_fragment in worst_aggregate(expr)


# ---------------------------------------------------------------------------
# Unknown functions
# ---------------------------------------------------------------------------


def test_unknown_functions_lists_only_what_duckdb_does_not_know() -> None:
    assert unknown_functions("arrow_fft(list(v ORDER BY ts))") == ["arrow_fft"]
    assert unknown_functions("sum(abs(v))") == []
    assert unknown_functions("count(*)") == []


def test_unknown_functions_deduplicates_and_keeps_order() -> None:
    assert unknown_functions("my_a(sum(v)) + my_b(my_a(sum(w)))") == ["my_a", "my_b"]


# ---------------------------------------------------------------------------
# Connection reuse
# ---------------------------------------------------------------------------


def test_one_connection_is_shared_across_calls() -> None:
    """Validation classifies every expression of every model; a connection per
    call would dominate the cost of `duckstream validate`."""
    from duckstream import aggregates

    classify_expression("sum(a)")
    first = aggregates._con
    for _ in range(50):
        classify_expression("median(b)")
    assert aggregates._con is first
    assert first is not None
