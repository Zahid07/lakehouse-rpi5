"""Unit tests for ``duckstream.sql`` and ``duckstream.sinks.table``.

Four rules shape this file, and each of them comes from a measurement rather
than from taste.

**Every merge test runs at least two batches.** ``CONTEXT.md`` section 1.5
records a DuckLake failure that appeared only on the *second* merge — the first
one to take the ``WHEN MATCHED`` branch. A single-batch test would have passed
while the bug shipped. Most tests here run four.

**Everything runs against a real DuckLake catalog.** The same measurement found
the failing statement passing on in-memory DuckDB. An in-memory test is
therefore not evidence about the code path the engine actually takes, so there
is no in-memory shortcut anywhere below.

**Correctness is asserted as a diff against a full recompute, not as expected
literals.** Both production bugs in ``CONTEXT.md`` section 4 produced plausible
numbers, and both were caught by recomputing from source. The ground-truth tests
run ``TableSink.aggregation_sql`` over every row at once and compare, so what is
under test is the *fold*, not the aggregation text.

**Row counts are asserted, not just values.** A NULL grouping key that fails to
match itself inserts a duplicate row per batch whose individual values all look
right. Only the count reveals it.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal

import duckdb
import pytest

from duckstream.errors import DuckstreamError
from duckstream.lake import attach_lake, data_file_count, list_files, snapshot_count
from duckstream.model import Model
from duckstream.protocols import BatchContext, BatchPlan
from duckstream.sinks import TableSink
from duckstream.sinks.table import MODES
from duckstream.sources.files import FileSource
from duckstream.sql import qualified, quote_ident, quote_literal, split_qualified

# ==========================================================================
# duckstream.sql
# ==========================================================================

#: Names that break naive quoting. Every one of these is legal in DuckDB when
#: quoted, and every one of them corrupts a statement when it is not.
HOSTILE_NAMES = [
    "plain",
    "MixedCase",
    "with space",
    'embedded "quote"',
    "it's",
    "semi;colon",
    "dot.ted",
    "trailing--comment",
    "/* block */",
    "unicode_é中文",
    "ÉMÉ",
    "select",
    "1leading_digit",
    "tab\tchar",
]


@pytest.fixture(scope="module")
def plain_con():
    """A throwaway connection for testing quoting, with no DuckLake attached.

    Quoting is pure SQL syntax, so this is the one place an in-memory database
    is the right tool: it is faster and the DuckLake catalog would add nothing.
    Every test that writes goes through the ``lake`` fixture instead.
    """
    con = duckdb.connect()
    try:
        yield con
    finally:
        con.close()


@pytest.mark.parametrize("name", HOSTILE_NAMES)
def test_quote_ident_survives_a_round_trip_through_duckdb(plain_con, name):
    # The strongest available assertion: DuckDB itself must give the name back
    # unchanged. Mixed case is the interesting one — an unquoted identifier is
    # folded to lower case, so a quoter that dropped the quotes would silently
    # rename the column.
    cursor = plain_con.execute(f"SELECT 1 AS {quote_ident(name)}")
    assert cursor.description[0][0] == name


def test_quote_ident_doubles_embedded_quotes():
    assert quote_ident('a"b') == '"a""b"'
    assert quote_ident('"') == '""""'


def test_quote_ident_does_not_split_on_dots():
    # A dotted name is one identifier here; splitting is qualified()'s job. If
    # this ever changed, a table legitimately named "a.b" would become a
    # reference to schema "a".
    assert quote_ident("a.b") == '"a.b"'


@pytest.mark.parametrize("bad", ["", None, 7, b"bytes", "nul\x00byte"])
def test_quote_ident_refuses_what_cannot_be_an_identifier(bad):
    with pytest.raises(DuckstreamError):
        quote_ident(bad)


def test_qualified_parses_a_single_dotted_string():
    assert qualified("marts.hourly") == '"marts"."hourly"'
    assert qualified("hourly") == '"hourly"'


def test_qualified_takes_several_parts_verbatim():
    # Passing parts separately is how a dot inside a table name is expressed
    # unambiguously.
    assert qualified("marts", "a.b") == '"marts"."a.b"'


def test_qualified_understands_already_quoted_input():
    assert qualified('"odd schema"."a.b"') == '"odd schema"."a.b"'


def test_qualified_skips_none_parts():
    assert qualified(None, "hourly") == '"hourly"'
    with pytest.raises(DuckstreamError):
        qualified(None)


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("hourly", ("main", "hourly")),
        ("marts.hourly", ("marts", "hourly")),
        ('"odd schema"."a.b"', ("odd schema", "a.b")),
        ('"He said ""hi"""', ("main", 'He said "hi"')),
        ("MixedCase.TableName", ("MixedCase", "TableName")),
        ("he\"llo", ("main", 'he"llo')),
        ("has space", ("main", "has space")),
    ],
)
def test_split_qualified(given, expected):
    assert split_qualified(given) == expected


def test_split_qualified_honours_a_different_default_schema():
    assert split_qualified("hourly", default_schema="marts") == ("marts", "hourly")


@pytest.mark.parametrize("bad", ["", "   ", "marts.", ".hourly", "a.b.c", None, 7])
def test_split_qualified_refuses_malformed_names(bad):
    with pytest.raises(DuckstreamError):
        split_qualified(bad)


def test_split_qualified_and_qualified_round_trip_hostile_names():
    schema, table = "Odd Schema", 'He said "hi"; DROP TABLE x --'
    rendered = qualified(schema, table)
    assert split_qualified(rendered) == (schema, table)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "NULL"),
        (True, "TRUE"),
        (False, "FALSE"),
        (0, "0"),
        (-42, "-42"),
        (10**30, "1" + "0" * 30),
        ("plain", "'plain'"),
        ("it's", "'it''s'"),
        ("'; DROP TABLE t; --", "'''; DROP TABLE t; --'"),
        (Decimal("1.50"), "1.50"),
        (dt.date(2026, 3, 1), "DATE '2026-03-01'"),
        (dt.time(1, 2, 3), "TIME '01:02:03'"),
        (
            dt.datetime(2026, 3, 1, 8, 30, 15),
            "TIMESTAMP '2026-03-01 08:30:15'",
        ),
        (
            dt.datetime(2026, 3, 1, 8, 30, tzinfo=dt.timezone.utc),
            "TIMESTAMPTZ '2026-03-01 08:30:00+00:00'",
        ),
        (dt.timedelta(days=1), "INTERVAL '86400000000 microseconds'"),
    ],
)
def test_quote_literal_renders(value, expected):
    assert quote_literal(value) == expected


def test_quote_literal_checks_bool_before_int():
    # bool subclasses int, so a naive isinstance order renders True as 1. That
    # would type a boolean column as an integer the first time a table is
    # created from a literal.
    assert quote_literal(True) == "TRUE"
    assert quote_literal(1) == "1"


def test_quote_literal_checks_datetime_before_date():
    # datetime subclasses date. Getting this backwards truncates a window bound
    # to midnight, which is the single most damaging silent error in this file.
    rendered = quote_literal(dt.datetime(2026, 3, 1, 23, 59))
    assert rendered.startswith("TIMESTAMP ")


@pytest.mark.parametrize(
    "value",
    [
        "it's",
        '"double"',
        "back\\slash",
        "new\nline",
        "unicode é中文",
        "'; DROP TABLE t; --",
        -1.5,
        0.0,
        10**25,
        None,
        True,
        dt.datetime(2026, 3, 1, 8, 30, 15),
        dt.date(1999, 12, 31),
        b"\x00\x01\xff",
    ],
)
def test_quote_literal_round_trips_through_duckdb(plain_con, value):
    (result,) = plain_con.execute(f"SELECT {quote_literal(value)}").fetchone()
    assert result == value


def test_quote_literal_of_an_injection_attempt_stays_one_string(plain_con):
    hostile = "'; DROP TABLE victim; SELECT '"
    plain_con.execute("CREATE TABLE victim (i INTEGER)")
    (result,) = plain_con.execute(f"SELECT {quote_literal(hostile)}").fetchone()
    assert result == hostile
    # Still there: the payload was data, not statements.
    assert plain_con.execute("SELECT count(*) FROM victim").fetchone() == (0,)
    plain_con.execute("DROP TABLE victim")


def test_quote_literal_handles_non_finite_floats(plain_con):
    for value, expected in ((float("nan"), "nan"), (float("inf"), "inf")):
        (result,) = plain_con.execute(f"SELECT {quote_literal(value)}").fetchone()
        assert repr(result) == expected


@pytest.mark.parametrize("bad", [object(), {"a": 1}, [1, 2], (1, 2), set()])
def test_quote_literal_refuses_unknown_types(bad):
    # Falling back to str() is how an unexpected object becomes executable SQL.
    with pytest.raises(DuckstreamError, match="cannot render"):
        quote_literal(bad)


# ==========================================================================
# Fixtures and helpers for the sink
# ==========================================================================


class _StubSource:
    """The smallest object satisfying the ``Source`` protocol.

    ``Model.validate`` checks that the source has the right methods, and some
    tests below want a genuinely valid model so that the sink is shown refusing
    a *correct* declaration rather than a broken one. Nothing here is called.
    """

    type_name = "stub"

    def latest_offset(self):
        return {"n": 0}

    def plan(self, start, end, limits=None):
        return BatchPlan(
            start=start, end=end, payload={}, is_empty=True, has_more=False
        )

    def bind(self, con, plan):  # pragma: no cover - never reached
        raise NotImplementedError

    def to_config(self):
        return {"type": "stub"}


class _Recorder:
    """A connection proxy that remembers the SQL it was asked to run.

    Used only where the assertion really is about *which statements were
    issued* — proving the target table is created once and not on every write.
    """

    def __init__(self, con):
        self._con = con
        self.statements: list[str] = []

    def execute(self, sql, *args, **kwargs):
        self.statements.append(sql)
        return self._con.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._con, name)

    def count_starting_with(self, prefix: str) -> int:
        return sum(1 for s in self.statements if s.lstrip().startswith(prefix))


@pytest.fixture
def lake(tmp_path):
    """A real DuckLake catalog under ``tmp_path``, with inlining disabled.

    ``attach_lake`` asserts the inlining setting itself, so every test in this
    file is implicitly on the non-inlined path — which is the one the engine
    runs and the one ``CONTEXT.md`` 1.7 says to stay on.
    """
    con = duckdb.connect()
    attach_lake(con, tmp_path / "catalog.ducklake", data_path=tmp_path / "lake_data")
    try:
        yield con
    finally:
        con.close()


BASE = dt.datetime(2026, 3, 1, 8, 0)
_COLUMNS = ("event_ts", "sensor", "val")
_TYPES = ("TIMESTAMP", "VARCHAR", "BIGINT")

#: count/sum/min/max together, so one ground-truth diff exercises every fold
#: shape at once: the coalesce-wrapped additions and the least/greatest pair.
ADDITIVE = {
    "n": "count(*)",
    "total": "sum(val)",
    "lo": "min(val)",
    "hi": "max(val)",
}


def row(minutes: int, sensor, val):
    return (BASE + dt.timedelta(minutes=minutes), sensor, val)


#: Four batches over three sensors and three hourly windows. Batch 4 arrives
#: out of order, carrying rows for the earliest window after later ones have
#: already been written, so folding into an existing row is exercised for a
#: window the sink has not seen most recently.
BATCHES = [
    [row(0, "a", 1), row(5, "a", 2), row(10, "b", 3)],
    [row(15, "a", 4), row(70, "b", 5), row(75, "c", 6)],
    [row(20, "b", 7), row(80, "a", 8), row(140, "c", 9), row(145, "a", 10)],
    [row(2, "c", 11), row(65, "c", 12), row(150, "b", 13), row(30, "a", 14)],
]


def make_view(con, rows, *, columns=_COLUMNS, types=_TYPES) -> str:
    """Register a uniquely named temp view over ``rows`` and return its name.

    Every column is explicitly cast. Without that, a batch whose measure column
    happens to be all NULL would come back typed differently from a batch that
    is not, and the merge would be testing type coercion instead of folding.
    The view name carries a uuid so two batches never collide on one connection.
    """
    name = "batch_" + uuid.uuid4().hex
    values = ", ".join(
        "(" + ", ".join(quote_literal(value) for value in r) + ")" for r in rows
    )
    projection = ", ".join(
        f"CAST(c{i} AS {types[i]}) AS {quote_ident(columns[i])}"
        for i in range(len(columns))
    )
    raw_columns = ", ".join(f"c{i}" for i in range(len(columns)))
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {quote_ident(name)} AS "
        f"SELECT {projection} FROM (VALUES {values}) AS raw({raw_columns})"
    )
    return name


def make_model(sink, **overrides) -> Model:
    defaults = dict(
        name="hourly_counts",
        source=_StubSource(),
        sink=sink,
        aggregates=dict(ADDITIVE),
        key=["window_ts", "sensor"],
        time_column="event_ts",
        grain="hour",
    )
    defaults.update(overrides)
    return Model(**defaults)


def make_append_model(sink, **overrides) -> Model:
    """The **unwindowed** append model: a per-batch row, no fold, any tier.

    Append without a grain is the tier-agnostic escape hatch, and it is what
    the tests below are about. Append *with* a grain is a different mechanism
    entirely -- it folds into an open-window accumulator and emits each window
    once when the watermark seals it -- so it needs its own model, its own
    horizon and its own tests; see the sealed-append section.
    """
    defaults = dict(grain=None, time_column=None, key=["sensor"])
    defaults.update(overrides)
    return make_model(sink, **defaults)


def make_sealing_model(sink, **overrides) -> Model:
    """The **windowed** append model: fold while open, emit once when sealed."""
    defaults = dict(lateness="0 seconds")
    defaults.update(overrides)
    return make_model(sink, **defaults)


def make_ctx(model: Model, batch_id: int, watermark=None) -> BatchContext:
    return BatchContext(
        model_name=model.name,
        batch_id=batch_id,
        watermark=watermark,
        plan=BatchPlan(
            start=None,
            end={"batch": batch_id},
            payload={},
            is_empty=False,
            has_more=False,
        ),
    )


def write_batches(
    con, sink, model, batches, *, columns=_COLUMNS, types=_TYPES, watermarks=None
):
    """Run every batch through the sink, one write per batch.

    ``watermarks`` supplies one watermark per batch for the sealed-append path,
    where the sink is told how far event time has advanced. It stays ``None``
    everywhere else, which is what a model with no lateness horizon carries.
    """
    for batch_id, rows in enumerate(batches, start=1):
        view = make_view(con, rows, columns=columns, types=types)
        watermark = None if watermarks is None else watermarks[batch_id - 1]
        sink.write(con, view, model, make_ctx(model, batch_id, watermark))


def sink_rows(con, sink, model) -> list[tuple]:
    columns = ", ".join(quote_ident(c) for c in sink.output_columns(model))
    return con.execute(f"SELECT {columns} FROM {sink.qualified_name}").fetchall()


def full_recompute(con, sink, model, batches, *, columns=_COLUMNS, types=_TYPES):
    """The same aggregation over every source row at once.

    Deliberately reuses ``sink.aggregation_sql``: the claim under test is that
    folding batch by batch equals aggregating the whole history in one pass, and
    the aggregation itself must be identical on both sides for that claim to be
    about folding at all.
    """
    every_row = [r for batch in batches for r in batch]
    view = make_view(con, every_row, columns=columns, types=types)
    return con.execute(sink.aggregation_sql(view, model)).fetchall()


def canonical(rows) -> list:
    """Sort rows into a total order that tolerates NULLs in any column."""
    return sorted(rows, key=repr)


# ==========================================================================
# Construction and configuration
# ==========================================================================


def test_mode_is_validated_in_the_constructor():
    with pytest.raises(DuckstreamError, match="mode 'upsert'"):
        TableSink("marts.hourly", mode="upsert")


@pytest.mark.parametrize("mode", MODES)
def test_both_modes_construct(mode):
    assert TableSink("marts.hourly", mode=mode).mode == mode


def test_default_mode_is_update():
    assert TableSink("marts.hourly").mode == "update"


def test_unqualified_table_lands_in_main():
    sink = TableSink("hourly")
    assert (sink.schema, sink.name) == ("main", "hourly")
    assert sink.qualified_name == '"main"."hourly"'


def test_malformed_table_name_fails_at_construction():
    with pytest.raises(DuckstreamError):
        TableSink("lake.marts.hourly")


def test_to_config_round_trips():
    sink = TableSink("marts.hourly", mode="append")
    config = sink.to_config()
    assert config == {"type": "table", "table": "marts.hourly", "mode": "append"}
    rebuilt = TableSink(config["table"], mode=config["mode"])
    assert rebuilt == sink
    assert rebuilt.to_config() == config


def test_to_config_always_states_the_mode():
    # The default is not omitted: 'update' deduplicates and 'append' does not,
    # and a reader of the YAML should not have to know which way it falls.
    assert TableSink("marts.hourly").to_config()["mode"] == "update"


def test_equality_is_on_the_resolved_name():
    assert TableSink("main.hourly") == TableSink("hourly")
    assert TableSink("hourly") != TableSink("hourly", mode="append")
    assert TableSink("hourly") != TableSink("other")
    assert TableSink("hourly") != "hourly"


# ==========================================================================
# Generated SQL
# ==========================================================================


def test_merge_has_no_scalar_subquery_in_its_join_condition():
    # CONTEXT.md 1.5: a (SELECT ...) in the ON clause makes DuckLake raise
    # 'Out of buffer' on the second merge. A table subquery as the USING source
    # is fine, so the assertion has to be scoped to the ON clause specifically.
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    sql = sink.merge_sql("batch_view", model)
    on_clause = sql.split("\n   ON ", 1)[1].split("\n WHEN MATCHED", 1)[0]
    assert "select" not in on_clause.lower()
    assert "(" not in on_clause


def test_merge_matches_keys_with_is_not_distinct_from():
    sink = TableSink("marts.hourly")
    sql = sink.merge_sql("batch_view", make_model(sink))
    assert sql.count("IS NOT DISTINCT FROM") == 2
    # No bare equality join on a key: that is what loses NULL keys.
    on_clause = sql.split("\n   ON ", 1)[1].split("\n WHEN MATCHED", 1)[0]
    assert " = " not in on_clause


def test_window_column_is_derived_not_read():
    sink = TableSink("marts.hourly")
    model = make_model(sink, grain="minute")
    keys = dict(sink.key_expressions(model))
    assert keys["window_ts"] == "date_trunc('minute', \"event_ts\")"
    assert keys["sensor"] == '"sensor"'
    # It is a key, so it must also appear in the GROUP BY.
    assert "GROUP BY date_trunc('minute', \"event_ts\"), \"sensor\"" in (
        sink.aggregation_sql("v", model)
    )


def test_without_a_grain_the_key_is_read_verbatim():
    sink = TableSink("marts.by_sensor")
    model = make_model(sink, key=["sensor"], grain=None, time_column=None)
    assert dict(sink.key_expressions(model)) == {"sensor": '"sensor"'}
    assert "date_trunc" not in sink.aggregation_sql("v", model)


def test_grain_is_inlined_as_a_literal_not_interpolated():
    sink = TableSink("marts.hourly")
    model = make_model(sink, grain="day")
    assert "date_trunc('day'," in sink.aggregation_sql("v", model)


def test_unsupported_grain_is_refused():
    sink = TableSink("marts.hourly")
    model = make_model(sink, grain="fortnight")
    with pytest.raises(DuckstreamError, match="grain"):
        sink.aggregation_sql("v", model)


def test_create_table_sql_filters_outside_the_aggregation():
    # An aggregation with no GROUP BY returns one row over zero input rows, so
    # WHERE false has to wrap the aggregation rather than sit inside it or the
    # table would be created holding a bogus row.
    sink = TableSink("marts.hourly")
    sql = sink.create_table_sql("v", make_model(sink))
    assert sql.rstrip().endswith("WHERE false")
    assert sql.index("GROUP BY") < sql.index("WHERE false")


# ==========================================================================
# ensure()
# ==========================================================================


def test_ensure_creates_the_schema_but_not_the_table(lake):
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    sink.ensure(lake, model)

    schemas = lake.execute(
        "SELECT count(*) FROM duckdb_schemas() "
        "WHERE database_name = current_database() AND schema_name = 'marts'"
    ).fetchone()
    assert schemas == (1,)
    # No data yet, so no types to infer: the table waits for the first write.
    assert sink.existing_columns(lake) == []


def test_ensure_is_idempotent(lake):
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    sink.ensure(lake, model)
    sink.ensure(lake, model)
    write_batches(lake, sink, model, BATCHES[:2])
    sink.ensure(lake, model)
    assert len(sink_rows(lake, sink, model)) > 0


def test_ensure_accepts_an_existing_table_that_matches(lake):
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    write_batches(lake, sink, model, BATCHES[:2])
    sink.ensure(lake, model)  # must not raise


def test_ensure_rejects_an_existing_table_missing_a_column(lake):
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    lake.execute("CREATE SCHEMA IF NOT EXISTS marts")
    lake.execute(
        'CREATE TABLE marts.hourly ("window_ts" TIMESTAMP, "sensor" VARCHAR, '
        '"n" BIGINT, "total" BIGINT, "lo" BIGINT)'
    )
    with pytest.raises(DuckstreamError) as excinfo:
        sink.ensure(lake, model)
    message = str(excinfo.value)
    assert "'hi'" in message
    assert "marts.hourly" in message
    # The message names both sides so the operator does not have to diff by eye.
    assert "'lo'" in message


def test_ensure_names_every_missing_column(lake):
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    lake.execute("CREATE SCHEMA IF NOT EXISTS marts")
    lake.execute(
        'CREATE TABLE marts.hourly ("window_ts" TIMESTAMP, "sensor" VARCHAR, '
        '"n" BIGINT)'
    )
    with pytest.raises(DuckstreamError) as excinfo:
        sink.ensure(lake, model)
    for column in ("total", "lo", "hi"):
        assert repr(column) in str(excinfo.value)


# ==========================================================================
# A pre-existing target: column types
# ==========================================================================
#
# ensure() compares column names and stops there, because it is handed a model
# and no data and the type of sum(value) is a property of the data. The type
# check therefore lives in write(), which has a bound batch. Without it a table
# that predates the model passes ensure() and then raises
# 'No function matches ... +(VARCHAR, BIGINT)' from inside the MERGE, part-way
# through the engine's transaction.


def _make_target(con, columns: str, *, table: str = "marts.pre") -> None:
    schema, name = table.split(".")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}")
    con.execute(f"CREATE TABLE {qualified(schema, name)} ({columns})")


GOOD_COLUMNS = (
    '"window_ts" TIMESTAMP, "sensor" VARCHAR, "n" BIGINT, '
    '"total" HUGEINT, "lo" BIGINT, "hi" BIGINT'
)


def test_a_hand_made_target_with_the_right_types_is_accepted(lake):
    sink = TableSink("marts.pre")
    model = make_model(sink)
    _make_target(lake, GOOD_COLUMNS)
    write_batches(lake, sink, model, BATCHES[:2])
    assert canonical(sink_rows(lake, sink, model)) == canonical(
        full_recompute(lake, sink, model, BATCHES[:2])
    )


def test_write_refuses_a_target_column_that_cannot_receive_the_aggregate(lake):
    # The reviewer's repro: right names, wrong type. VARCHAR and BIGINT have no
    # implicit cast in either direction, so the fold cannot be built.
    sink = TableSink("marts.pre")
    model = make_model(sink)
    _make_target(lake, GOOD_COLUMNS.replace('"n" BIGINT', '"n" VARCHAR'))

    sink.ensure(lake, model)  # names match, so ensure has nothing to say

    with pytest.raises(DuckstreamError) as excinfo:
        sink.write(lake, make_view(lake, BATCHES[0]), model, make_ctx(model, 1))
    message = str(excinfo.value)
    assert "'n'" in message
    assert "VARCHAR" in message  # the target type
    assert "BIGINT" in message  # the incoming type
    assert "count(*)" in message  # the aggregate that produced it
    assert "predates" in message  # and what to do about it


def test_the_refusal_happens_before_any_statement_writes(lake):
    # The whole point of moving the check earlier: nothing may run that could
    # fail inside the engine's transaction.
    sink = TableSink("marts.pre")
    model = make_model(sink)
    _make_target(lake, GOOD_COLUMNS.replace('"n" BIGINT', '"n" VARCHAR'))

    recorder = _Recorder(lake)
    with pytest.raises(DuckstreamError):
        sink.write(recorder, make_view(lake, BATCHES[0]), model, make_ctx(model, 1))

    issued = " ".join(recorder.statements).upper()
    assert "MERGE" not in issued
    assert "INSERT" not in issued
    assert lake.execute("SELECT count(*) FROM marts.pre").fetchone() == (0,)


def test_a_refused_write_leaves_the_transaction_usable(lake):
    sink = TableSink("marts.pre")
    model = make_model(sink)
    _make_target(lake, GOOD_COLUMNS.replace('"lo" BIGINT', '"lo" BOOLEAN'))
    view = make_view(lake, BATCHES[0])

    before = snapshot_count(lake)
    lake.execute("BEGIN")
    with pytest.raises(DuckstreamError):
        sink.write(lake, view, model, make_ctx(model, 1))
    # Refused cleanly rather than poisoning the transaction the engine owns.
    assert lake.execute("SELECT 1").fetchone() == (1,)
    lake.execute("ROLLBACK")
    assert snapshot_count(lake) == before


@pytest.mark.parametrize(
    ("declared", "why"),
    [
        ('"n" HUGEINT', "widening: BIGINT count into a HUGEINT column"),
        ('"n" INTEGER', "narrowing: BIGINT count into an INTEGER column"),
        ('"n" DOUBLE', "BIGINT count into a DOUBLE column"),
        ('"total" BIGINT', "narrowing: HUGEINT sum into a BIGINT column"),
        ('"total" DOUBLE', "HUGEINT sum into a DOUBLE column"),
    ],
)
def test_types_duckdb_can_bridge_are_accepted_and_fold_correctly(lake, declared, why):
    # Deliberate permissiveness. Narrowing is allowed because DuckDB performs
    # it and raises only on a value that does not fit, so a hand-made table
    # declaring 'n INTEGER, total BIGINT' works today. Refusing a working
    # pipeline is the worse error, and CONTEXT.md's bug class is silent wrong
    # numbers, not an overflow that announces itself.
    column = declared.split()[0]
    columns = ", ".join(
        declared if piece.strip().startswith(column) else piece
        for piece in GOOD_COLUMNS.split(", ")
    )
    sink = TableSink("marts.pre")
    model = make_model(sink)
    _make_target(lake, columns)

    write_batches(lake, sink, model, BATCHES[:3])

    stored = {(r[0], r[1]): r[2:] for r in sink_rows(lake, sink, model)}
    expected = {
        (r[0], r[1]): r[2:]
        for r in full_recompute(lake, sink, model, BATCHES[:3])
    }
    assert set(stored) == set(expected)
    for key, values in expected.items():
        assert [float(v) for v in stored[key]] == [float(v) for v in values], why


def test_a_timestamptz_key_column_is_accepted(lake):
    # Split out from the fold table above because reading a TIMESTAMP WITH TIME
    # ZONE back into Python needs pytz, which is not a duckstream dependency --
    # the same limitation lake.snapshots() works around by casting in SQL. So
    # the comparison is done entirely in SQL here.
    sink = TableSink("marts.pre")
    model = make_model(sink)
    _make_target(
        lake,
        GOOD_COLUMNS.replace(
            '"window_ts" TIMESTAMP', '"window_ts" TIMESTAMP WITH TIME ZONE'
        ),
    )
    write_batches(lake, sink, model, BATCHES[:3])

    rows = lake.execute(
        'SELECT "window_ts"::VARCHAR, "sensor", "n", "total" '
        "FROM marts.pre ORDER BY 1, 2 NULLS LAST"
    ).fetchall()
    view = make_view(lake, [r for batch in BATCHES[:3] for r in batch])
    # Cast the expected side the same way the column does, so what is compared
    # is the stored instant and not the session's rendering of it.
    expected = lake.execute(
        f'SELECT "window_ts"::TIMESTAMPTZ::VARCHAR, "sensor", "n", "total" '
        f"FROM ({sink.aggregation_sql(view, model)}) AS g ORDER BY 1, 2 NULLS LAST"
    ).fetchall()
    assert rows == expected


@pytest.mark.parametrize(
    ("incoming", "target", "compatible"),
    [
        ("BIGINT", "BIGINT", True),  # identical
        ("INTEGER", "BIGINT", True),  # widening store
        ("BIGINT", "INTEGER", True),  # narrowing store, DuckDB will cast
        ("HUGEINT", "BIGINT", True),  # sum(BIGINT) into a hand-made BIGINT
        ("BIGINT", "DOUBLE", True),
        ("DECIMAL(38,1)", "DECIMAL(38,2)", True),
        ("DATE", "TIMESTAMP", True),
        ("BIGINT", "VARCHAR", False),  # no implicit cast either way
        ("VARCHAR", "BIGINT", False),
        ("BIGINT", "BOOLEAN", False),
        ("TIMESTAMP", "VARCHAR", False),
        ("BIGINT[]", "BIGINT", False),
    ],
)
def test_the_permitted_coercions_are_duckdbs_own(
    plain_con, incoming, target, compatible
):
    # Documents the rule as a table so a future change to it is a visible diff:
    # compatible when DuckDB has an implicit cast in *either* direction.
    sink = TableSink("marts.pre")
    assert sink._types_are_compatible(plain_con, incoming, target) is compatible


def test_an_unquotable_type_name_is_never_interpolated(plain_con):
    # A type name cannot be passed as a literal, so it is screened instead. A
    # name carrying a quote falls back to exact comparison: stricter, and the
    # probe is never built from it.
    sink = TableSink("marts.pre")
    plain_con.execute("CREATE TABLE victim (i INTEGER)")
    hostile = "BIGINT); DROP TABLE victim; --"
    assert sink._types_are_compatible(plain_con, hostile, "BIGINT") is False
    assert sink._types_are_compatible(plain_con, hostile, hostile) is True  # identical
    assert plain_con.execute("SELECT count(*) FROM victim").fetchone() == (0,)
    plain_con.execute("DROP TABLE victim")


def test_append_tolerates_a_type_the_fold_could_not_bind(lake):
    # Measured, not assumed: INSERT applies an assignment cast, so a BIGINT
    # count(*) into a VARCHAR column works today and stores '7'. The fold
    # cannot bind the same pair. Refusing append here would break a working
    # pipeline to prevent a failure that does not occur, so the type check is
    # scoped to update mode.
    sink = TableSink("marts.pre", mode="append")
    model = make_append_model(sink)
    _make_target(lake, GOOD_COLUMNS.replace('"n" BIGINT', '"n" VARCHAR'))

    write_batches(lake, sink, model, BATCHES[:2])

    rows = lake.execute(
        'SELECT "n", typeof("n") FROM marts.pre ORDER BY "n"'
    ).fetchall()
    assert rows
    assert all(kind == "VARCHAR" for _, kind in rows)


def test_append_still_reports_a_missing_column(lake):
    # append skips the *type* check, not the name check.
    sink = TableSink("marts.pre", mode="append")
    model = make_append_model(sink)
    _make_target(lake, '"window_ts" TIMESTAMP, "sensor" VARCHAR, "n" BIGINT')
    with pytest.raises(DuckstreamError, match="'total'"):
        sink.write(lake, make_view(lake, BATCHES[0]), model, make_ctx(model, 1))


def test_extra_columns_on_the_target_are_kept_and_left_null(lake):
    # Pinned deliberately. The generated INSERT/MERGE name their columns, so a
    # column the model does not write survives untouched and is NULL on rows
    # the sink inserts. A table may legitimately carry an annotation column, so
    # this is allowed — but it also means a *renamed* aggregate leaves its
    # predecessor behind, silently NULL, which is why a rename wants a
    # migration rather than a redeploy.
    sink = TableSink("marts.pre")
    model = make_model(sink)
    _make_target(lake, GOOD_COLUMNS + ', "note" VARCHAR, "owner" VARCHAR')

    sink.ensure(lake, model)  # extra columns are not a mismatch
    write_batches(lake, sink, model, BATCHES[:2])

    assert sink.existing_columns(lake)[-2:] == ["note", "owner"]
    assert lake.execute(
        'SELECT count(*) FROM marts.pre WHERE "note" IS NOT NULL '
        'OR "owner" IS NOT NULL'
    ).fetchone() == (0,)
    # The columns the model does write are still exactly a full recompute.
    assert canonical(sink_rows(lake, sink, model)) == canonical(
        full_recompute(lake, sink, model, BATCHES[:2])
    )


def test_an_annotation_on_an_extra_column_survives_a_later_fold(lake):
    # The useful half of the previous test: an extra column is not merely
    # tolerated, it is preserved across merges into the same row.
    sink = TableSink("marts.pre")
    model = make_model(sink)
    _make_target(lake, GOOD_COLUMNS + ', "note" VARCHAR')

    write_batches(lake, sink, model, [BATCHES[0]])
    lake.execute("UPDATE marts.pre SET \"note\" = 'checked' WHERE \"sensor\" = 'a'")
    write_batches(lake, sink, model, [BATCHES[1]])

    assert lake.execute(
        'SELECT "note" FROM marts.pre WHERE "sensor" = \'a\' AND "note" IS NOT NULL'
    ).fetchall() == [("checked",)]


def test_ensure_passes_a_type_mismatch_that_write_refuses(lake):
    # Pins the documented split rather than leaving it to the docstring: ensure
    # has no data and so no types, and must not pretend otherwise.
    sink = TableSink("marts.pre")
    model = make_model(sink)
    _make_target(lake, GOOD_COLUMNS.replace('"total" HUGEINT', '"total" VARCHAR'))

    sink.ensure(lake, model)  # no exception: names all match

    with pytest.raises(DuckstreamError, match="'total'"):
        sink.write(lake, make_view(lake, BATCHES[0]), model, make_ctx(model, 1))


def test_incoming_column_types_reports_what_duckdb_will_produce(lake):
    sink = TableSink("marts.pre")
    model = make_model(sink)
    types = sink.incoming_column_types(lake, make_view(lake, BATCHES[0]), model)
    assert types["window_ts"] == "TIMESTAMP"
    assert types["n"] == "BIGINT"
    # sum over BIGINT widens, and DuckDB is the one that decided that.
    assert types["total"] == "HUGEINT"


def test_the_type_check_costs_nothing_on_a_freshly_created_table(lake):
    # The first write creates the table from this very aggregation, so its
    # types are the incoming ones by construction and there is nothing to
    # compare. Asserted so the check is not quietly moved onto that path.
    sink = TableSink("marts.fresh_types")
    model = make_model(sink)
    recorder = _Recorder(lake)
    sink.write(recorder, make_view(lake, BATCHES[0]), model, make_ctx(model, 1))
    assert recorder.count_starting_with("DESCRIBE") == 0


# ==========================================================================
# update mode: the fold
# ==========================================================================


def test_four_batches_equal_a_full_recompute(lake):
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    sink.ensure(lake, model)

    write_batches(lake, sink, model, BATCHES)

    expected = full_recompute(lake, sink, model, BATCHES)
    assert canonical(sink_rows(lake, sink, model)) == canonical(expected)
    # Three sensors across three hourly windows, not every combination present.
    assert len(expected) == 9


def test_every_intermediate_state_equals_a_full_recompute(lake):
    # Not just the end state. A fold that is wrong on batch 2 and accidentally
    # right on batch 4 is still wrong, and CONTEXT.md 1.5's failure showed up on
    # the second merge specifically.
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    sink.ensure(lake, model)

    for count in range(1, len(BATCHES) + 1):
        rows = BATCHES[count - 1]
        view = make_view(lake, rows)
        sink.write(lake, view, model, make_ctx(model, count))
        expected = full_recompute(lake, sink, model, BATCHES[:count])
        assert canonical(sink_rows(lake, sink, model)) == canonical(expected), (
            f"diverged after batch {count}"
        )


def test_a_replayed_batch_keeps_the_row_set_but_doubles_the_values(lake):
    # Stated exactly, because "merge is idempotent" is the comfortable version
    # and it is not true of an additive fold. The key keeps the row *set*
    # stable; the values genuinely double, and only the engine committing
    # output and offset together stops a replay from happening at all.
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    write_batches(lake, sink, model, [BATCHES[0]])
    once = canonical(sink_rows(lake, sink, model))
    write_batches(lake, sink, model, [BATCHES[0]])
    twice = canonical(sink_rows(lake, sink, model))

    assert len(twice) == len(once) == 2
    assert [r[:2] for r in twice] == [r[:2] for r in once]
    assert [r[2] for r in twice] == [r[2] * 2 for r in once]  # count doubled
    assert [r[4:] for r in twice] == [r[4:] for r in once]  # min/max unchanged


def test_out_of_order_batches_fold_into_the_right_window(lake):
    # Batch 2 is entirely in a later window; batch 3 comes back to the first.
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    batches = [
        [row(0, "a", 1), row(5, "a", 2)],
        [row(70, "a", 100)],
        [row(10, "a", 4), row(15, "a", 8)],
    ]
    write_batches(lake, sink, model, batches)

    rows = {(r[0], r[1]): r[2:] for r in sink_rows(lake, sink, model)}
    assert rows[(BASE, "a")] == (4, 15, 1, 8)
    assert rows[(BASE + dt.timedelta(hours=1), "a")] == (1, 100, 100, 100)
    assert canonical(sink_rows(lake, sink, model)) == canonical(
        full_recompute(lake, sink, model, batches)
    )


def test_null_grouping_key_merges_into_one_row(lake):
    # The reason the ON clause uses IS NOT DISTINCT FROM. Under plain '=' a NULL
    # key never matches itself, every batch takes WHEN NOT MATCHED, and the
    # table grows one duplicate row per batch whose values all look plausible.
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    batches = [
        [row(0, None, 1), row(5, "a", 2)],
        [row(10, None, 3), row(15, "a", 4)],
        [row(20, None, 5)],
        [row(25, None, 7), row(30, "a", 8)],
    ]
    write_batches(lake, sink, model, batches)

    rows = sink_rows(lake, sink, model)
    # The count is the assertion that matters: two keys, one window.
    assert len(rows) == 2
    null_rows = [r for r in rows if r[1] is None]
    assert len(null_rows) == 1
    assert null_rows[0][2:] == (4, 16, 1, 7)
    assert canonical(rows) == canonical(full_recompute(lake, sink, model, batches))


def test_null_key_stays_one_row_across_many_batches(lake):
    sink = TableSink("marts.by_sensor")
    model = make_model(sink, key=["sensor"], grain=None, time_column=None)
    batches = [[row(i * 5, None, i)] for i in range(1, 7)]
    write_batches(lake, sink, model, batches)

    assert lake.execute(f"SELECT count(*) FROM {sink.qualified_name}").fetchone() == (
        1,
    )
    assert sink_rows(lake, sink, model) == [(None, 6, 21, 1, 6)]


def test_an_all_null_batch_does_not_destroy_the_running_total(lake):
    # W1's coalesce(t + s, t, s) fold, proved end to end. Plain 't.total +
    # s.total' would be NULL here, and every later batch would fold into that
    # NULL and stay NULL: one quiet interval permanently erases the total.
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    batches = [
        [row(0, "a", 1), row(5, "a", 2)],
        [row(10, "a", None), row(15, "a", None)],
        [row(20, "a", 4)],
    ]
    write_batches(lake, sink, model, batches)

    rows = sink_rows(lake, sink, model)
    assert len(rows) == 1
    window_ts, sensor, n, total, lo, hi = rows[0]
    assert (window_ts, sensor) == (BASE, "a")
    assert n == 5  # count(*) counts the NULL-valued rows
    assert total == 7  # 1 + 2 + 4, the NULL batch contributed nothing
    assert (lo, hi) == (1, 4)
    assert canonical(rows) == canonical(full_recompute(lake, sink, model, batches))


def test_an_all_null_batch_arriving_first_is_still_correct(lake):
    # The mirror case: the stored value is NULL and the delta is not, so the
    # coalesce has to pick the delta rather than propagate the stored NULL.
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    batches = [
        [row(0, "a", None)],
        [row(5, "a", 3)],
        [row(10, "a", None)],
        [row(15, "a", 4)],
    ]
    write_batches(lake, sink, model, batches)

    assert canonical(sink_rows(lake, sink, model)) == canonical(
        full_recompute(lake, sink, model, batches)
    )
    assert sink_rows(lake, sink, model)[0][3] == 7


# ==========================================================================
# append mode
# ==========================================================================


def test_append_inserts_without_merging(lake):
    sink = TableSink("marts.appended", mode="append")
    model = make_append_model(sink)
    sink.ensure(lake, model)
    write_batches(lake, sink, model, BATCHES)

    # One row per (window, sensor) *per batch*, never folded across batches.
    expected = 0
    for batch in BATCHES:
        view = make_view(lake, batch)
        expected += len(lake.execute(sink.aggregation_sql(view, model)).fetchall())
    assert (
        lake.execute(f"SELECT count(*) FROM {sink.qualified_name}").fetchone()[0]
        == expected
    )
    assert expected == 11


def test_append_repeats_a_replayed_batch(lake):
    # Documented behaviour, asserted so nobody "fixes" it into a merge: append
    # does not deduplicate, and idempotency for append comes from the engine's
    # offset transaction alone.
    sink = TableSink("marts.appended", mode="append")
    model = make_append_model(sink)
    write_batches(lake, sink, model, [BATCHES[0], BATCHES[0]])
    assert lake.execute(f"SELECT count(*) FROM {sink.qualified_name}").fetchone() == (
        4,
    )


def test_append_totals_equal_the_sum_of_per_batch_aggregates(lake):
    sink = TableSink("marts.appended", mode="append")
    model = make_append_model(sink)
    write_batches(lake, sink, model, BATCHES)
    total = lake.execute(f"SELECT sum(total) FROM {sink.qualified_name}").fetchone()[0]
    assert total == sum(r[2] for batch in BATCHES for r in batch)


def test_append_accepts_a_non_additive_model(lake):
    # The phase-3 refusal is scoped to folding. Appending a per-batch average is
    # a legitimate thing to want and involves no fold at all.
    sink = TableSink("marts.avg_by_batch", mode="append")
    model = make_append_model(sink, aggregates={"mean_val": "avg(val)"})
    sink.ensure(lake, model)
    write_batches(lake, sink, model, BATCHES[:2])
    assert lake.execute(f"SELECT count(*) FROM {sink.qualified_name}").fetchone()[0] > 0


# ==========================================================================
# append over windows: fold while open, emit once when sealed
# ==========================================================================
#
# The mechanism phase 2 adds, and the one that makes `append` mean what it
# says. Phase 1's windowed append wrote a partial row per window per batch;
# Model.validate now refuses that shape outright, and this is what replaces it.
# The end-to-end contract -- against DuckLake, through both front doors, with
# the watermark coming from the engine -- is tests/conformance/test_event_time.py.


def normalise_rows(rows):
    return sorted(tuple(r) for r in rows)


def test_the_accumulator_sits_beside_the_target(lake):
    sink = TableSink("marts.sealed", mode="append")
    assert sink.open_windows_name == "sealed__open_windows"
    assert sink.qualified_open_windows == '"marts"."sealed__open_windows"'


def test_windowed_append_is_recognised_only_with_a_grain():
    sink = TableSink("marts.sealed", mode="append")
    assert sink.windowed_append(make_sealing_model(sink))
    assert not sink.windowed_append(make_append_model(sink))
    assert not TableSink("marts.x", mode="update").windowed_append(
        make_sealing_model(TableSink("marts.x", mode="update"))
    )


def test_an_open_window_is_held_back_and_a_sealed_one_is_written(lake):
    """The whole contract in one test: nothing before the watermark passes.

    ``lateness='0 seconds'`` in ``make_sealing_model`` keeps the arithmetic
    readable -- a window seals the moment the watermark reaches its end -- and
    the horizon's own width is tested in test_watermark.py.
    """
    sink = TableSink("marts.sealed", mode="append")
    model = make_sealing_model(sink)
    sink.ensure(lake, model)

    # BASE is 08:00; batch one lands wholly inside the 08:00 window.
    write_batches(lake, sink, model, [BATCHES[0]], watermarks=[BASE])
    assert lake.execute(f"SELECT count(*) FROM {sink.qualified_name}").fetchone() == (0,)
    assert lake.execute(
        f"SELECT count(*) FROM {sink.qualified_open_windows}"
    ).fetchone() == (2,)

    # A watermark of 09:00 ends the 08:00 window.
    write_batches(
        lake, sink, model, [BATCHES[0]], watermarks=[BASE + dt.timedelta(hours=1)]
    )
    assert lake.execute(
        f"SELECT count(*) FROM {sink.qualified_open_windows}"
    ).fetchone() == (0,)
    assert lake.execute(f"SELECT count(*) FROM {sink.qualified_name}").fetchone() == (2,)


def test_a_window_is_emitted_once_however_many_batches_fed_it(lake):
    """The phase-1 defect, asserted from the other side.

    Four batches touch the 08:00 window; exactly one row per key comes out of
    it, carrying the fold of all four.
    """
    sink = TableSink("marts.sealed", mode="append")
    model = make_sealing_model(sink)
    sink.ensure(lake, model)

    open_ = [None, None, None, BASE + dt.timedelta(hours=1)]
    write_batches(lake, sink, model, BATCHES, watermarks=open_)

    rows = sink_rows(lake, sink, model)
    windows = [row[0] for row in rows]
    assert len(windows) == len(set(zip(windows, [r[1] for r in rows]))), rows
    assert all(row[0] == BASE for row in rows), (
        f"only the 08:00 window should have sealed at this watermark: {rows}"
    )

    # And its values are the fold of every batch that fed it, not of the last.
    expected = lake.execute(
        sink.aggregation_sql(make_view(lake, [r for b in BATCHES for r in b]), model)
        + f" HAVING {sink._window_expression(model)} = TIMESTAMP '{BASE}'"
    ).fetchall()
    assert normalise_rows(rows) == normalise_rows(expected)


def test_nothing_seals_before_a_watermark_exists(lake):
    """No watermark, no completeness claim -- but the mart still exists, empty.

    An empty mart that is visibly empty is worth more than a mart that does not
    exist: the first is a stream that has not sealed anything yet, the second
    is indistinguishable from a broken deployment.
    """
    sink = TableSink("marts.sealed", mode="append")
    model = make_sealing_model(sink)
    write_batches(lake, sink, model, BATCHES[:2], watermarks=[None, None])

    assert sink.existing_columns(lake) == sink.output_columns(model)
    assert lake.execute(f"SELECT count(*) FROM {sink.qualified_name}").fetchone() == (0,)
    assert lake.execute(
        f"SELECT count(*) FROM {sink.qualified_open_windows}"
    ).fetchone()[0] > 0


def test_a_later_batch_at_the_same_watermark_emits_nothing_further(lake):
    """Emission and eviction are one transaction, so nothing is left to re-emit.

    The second batch lands entirely in the *next* window, which the unchanged
    watermark has not reached, so the mart must be untouched. If eviction had
    not accompanied emission, the already-emitted 08:00 window would still be
    sitting in the accumulator and would be written a second time here.
    """
    sink = TableSink("marts.sealed", mode="append")
    model = make_sealing_model(sink)
    high = BASE + dt.timedelta(hours=1)
    write_batches(lake, sink, model, [BATCHES[0]], watermarks=[high])
    first = sink_rows(lake, sink, model)
    assert first, "the 08:00 window should have sealed"

    write_batches(lake, sink, model, [[row(80, "a", 8)]], watermarks=[high])
    assert sink_rows(lake, sink, model) == first


def test_the_sink_does_not_filter_late_rows_itself(lake):
    """A contract boundary worth pinning: the engine filters, the sink folds.

    Handed a row belonging to a window it has already emitted, the sink folds
    it and emits that window again -- it has no watermark history and no way to
    know. That is not a defect to be fixed here; duplicating the check would put
    the same decision in two places, and the sink's copy would be the one
    without the committed watermark to check against.
    ``duckstream.engine`` removes such rows before ``write`` ever sees them, and
    ``tests/conformance/test_event_time.py`` is where that is proved end to end.
    """
    sink = TableSink("marts.sealed", mode="append")
    model = make_sealing_model(sink)
    high = BASE + dt.timedelta(hours=1)
    write_batches(lake, sink, model, [BATCHES[0]], watermarks=[high])
    emitted = len(sink_rows(lake, sink, model))

    write_batches(lake, sink, model, [[row(1, "a", 99)]], watermarks=[high])
    assert len(sink_rows(lake, sink, model)) == emitted + 1


def test_the_seal_and_evict_statements_carry_no_scalar_subquery(lake):
    """``CONTEXT.md`` 1.5, applied to the statements phase 2 adds.

    A ``(SELECT ...)`` here would fail against DuckLake with ``Out of buffer``
    and, as in 1.5, only once the matched branch had been reached -- so a
    structural check is cheap insurance that a behavioural one cannot give.
    """
    sink = TableSink("marts.sealed", mode="append")
    model = make_sealing_model(sink)
    cutoff = BASE + dt.timedelta(hours=1)

    for statement in (sink.seal_sql(model, cutoff), sink.evict_sql(cutoff)):
        assert "(SELECT" not in statement.upper().replace("( SELECT", "(SELECT")
        assert quote_literal(cutoff) in statement

    # The accumulator merge is the one that takes a WHEN MATCHED branch, so it
    # gets the same check the target merge already has elsewhere.
    merge = sink.merge_sql(
        make_view(lake, BATCHES[0]), model, into=sink.qualified_open_windows
    )
    on_clause = merge.split("\n   ON ", 1)[1].split("\n WHEN ", 1)[0]
    assert "(SELECT" not in on_clause.upper()
    assert sink.qualified_open_windows in merge
    assert f"MERGE INTO {sink.qualified_name} AS" not in merge


def test_the_accumulator_is_type_checked_because_it_is_merged_into(lake):
    """The fold binds against the accumulator, so that is where types matter.

    The target only ever receives an ``INSERT ... SELECT`` from the accumulator,
    which is why the type check follows the *fold* rather than the mode name.
    """
    sink = TableSink("marts.sealed", mode="append")
    model = make_sealing_model(sink)
    lake.execute("CREATE SCHEMA IF NOT EXISTS marts")
    lake.execute(
        f"CREATE TABLE {sink.qualified_open_windows} ("
        f'"window_ts" TIMESTAMP, "sensor" VARCHAR, "n" VARCHAR, "total" DOUBLE)'
    )
    with pytest.raises(DuckstreamError) as excinfo:
        write_batches(lake, sink, model, [BATCHES[0]], watermarks=[None])
    message = str(excinfo.value)
    assert "open_windows" in message and "'n'" in message


def test_a_non_additive_windowed_append_is_refused(lake):
    """Sealing folds across batches, so it needs a foldable aggregate.

    Unwindowed append accepts any tier because it never folds. Windowed append
    does fold -- that is the whole mechanism -- so the phase-3 refusal applies
    to it exactly as it does to update mode.
    """
    sink = TableSink("marts.sealed", mode="append")
    model = make_sealing_model(sink, aggregates={"mean_val": "avg(val)"})
    with pytest.raises(DuckstreamError, match="sufficient_statistics"):
        sink.ensure(lake, model)
    with pytest.raises(DuckstreamError, match="sufficient_statistics"):
        write_batches(lake, sink, model, [BATCHES[0]], watermarks=[None])


# ==========================================================================
# Table creation
# ==========================================================================


def test_table_is_created_on_the_first_write_and_not_recreated(lake):
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    sink.ensure(lake, model)
    assert sink.existing_columns(lake) == []

    recorder = _Recorder(lake)
    write_batches(recorder, sink, model, BATCHES)

    assert recorder.count_starting_with("CREATE TABLE") == 1
    assert sink.existing_columns(lake) == [
        "window_ts",
        "sensor",
        "n",
        "total",
        "lo",
        "hi",
    ]
    # And batch 1's rows survived, which a recreate would have dropped.
    assert canonical(sink_rows(lake, sink, model)) == canonical(
        full_recompute(lake, sink, model, BATCHES)
    )


def test_write_works_without_a_prior_ensure(lake):
    # A library user driving the sink directly need not have called ensure, so
    # the first write creates the schema as well as the table.
    sink = TableSink("brand_new_schema.hourly")
    model = make_model(sink)
    write_batches(lake, sink, model, BATCHES[:2])
    assert len(sink_rows(lake, sink, model)) > 0


def test_column_types_come_from_duckdb_not_from_a_guess(lake):
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    write_batches(lake, sink, model, BATCHES[:2])
    types = dict(
        lake.execute(
            "SELECT column_name, data_type FROM duckdb_columns() "
            "WHERE database_name = current_database() "
            "  AND schema_name = 'marts' AND table_name = 'hourly'"
        ).fetchall()
    )
    assert types["window_ts"] == "TIMESTAMP"
    assert types["sensor"] == "VARCHAR"
    assert types["n"] == "BIGINT"
    # sum(BIGINT) widens, and the point is that DuckDB decided that, not us.
    assert types["total"] in ("HUGEINT", "INT128", "BIGINT")


# ==========================================================================
# The phase-3 refusal
# ==========================================================================


def _sufficient_statistics_model(sink):
    return make_model(
        sink, name="hourly_avg", aggregates={"mean_val": "avg(val)"}
    )


def _non_foldable_model(sink):
    return make_model(
        sink,
        name="hourly_median",
        aggregates={"mid": "median(val)"},
        strategy="recompute_window",
        memory_profile="materialising",
    )


@pytest.mark.parametrize(
    ("build", "tier"),
    [
        (_sufficient_statistics_model, "sufficient_statistics"),
        (_non_foldable_model, "non_foldable"),
    ],
)
def test_update_mode_refuses_a_non_additive_model(lake, build, tier):
    sink = TableSink("marts.refused")
    model = build(sink)
    model.validate()  # a perfectly valid model; the sink simply cannot fold it
    assert model.tier == tier

    for call in (
        lambda: sink.ensure(lake, model),
        lambda: sink.write(
            lake, make_view(lake, BATCHES[0]), model, make_ctx(model, 1)
        ),
        lambda: sink.merge_sql("v", model),
    ):
        with pytest.raises(DuckstreamError) as excinfo:
            call()
        message = str(excinfo.value)
        assert tier in message
        assert "phase 3" in message

    # Nothing was written. Refusing loudly and writing wrong numbers anyway
    # would be worse than not refusing at all.
    assert sink.existing_columns(lake) == []


def test_the_refusal_explains_why_rather_than_just_failing(lake):
    # A refusal is only useful if the operator can act on it, so the message has
    # to name the model, the tier, the strategy that would be needed, and what
    # to do instead.
    sink = TableSink("marts.refused")
    model = _sufficient_statistics_model(sink)
    with pytest.raises(DuckstreamError) as excinfo:
        sink.write(lake, make_view(lake, BATCHES[0]), model, make_ctx(model, 1))
    message = str(excinfo.value)
    for fragment in (
        "hourly_avg",
        "sufficient_statistics",
        "delta_merge",
        "phase 3",
        "append",
    ):
        assert fragment in message, f"{fragment!r} missing from: {message}"


def test_a_declared_stronger_strategy_over_additive_aggregates_is_also_refused(lake):
    # recompute_window over count/sum is correct but is not what this sink
    # implements, so it must say so rather than quietly folding instead.
    sink = TableSink("marts.refused")
    model = make_model(sink, strategy="recompute_window")
    model.validate()
    with pytest.raises(DuckstreamError, match="phase 3"):
        sink.write(lake, make_view(lake, BATCHES[0]), model, make_ctx(model, 1))


# ==========================================================================
# DuckLake accounting
# ==========================================================================


def test_the_sink_table_lives_in_the_ducklake_catalog(lake):
    # CONTEXT.md 1.9: one transaction can write to only one attached database,
    # so a sink in memory.main and a state store in the lake cannot share a
    # transaction — and the one-snapshot-per-trigger guarantee dies with it.
    # The sink names its table relative to the current catalog, and attach_lake
    # has issued USE, so the target lands in DuckLake with no extra ceremony.
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    write_batches(lake, sink, model, BATCHES[:2])

    assert lake.execute("SELECT current_database()").fetchone() == ("lake",)
    assert lake.execute(
        "SELECT database_name FROM duckdb_tables() "
        "WHERE schema_name = 'marts' AND table_name = 'hourly'"
    ).fetchall() == [("lake",)]


def test_two_writes_in_one_transaction_produce_one_snapshot(lake):
    # CONTEXT.md 1.4: DuckLake commits one snapshot per transaction, not per
    # statement. That is the primitive the exactly-once claim rests on, so the
    # sink must not open a transaction of its own or issue a stray commit.
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    sink.ensure(lake, model)

    # Views are registered before BEGIN. A temp view is DDL against the `temp`
    # database, and CONTEXT.md 1.9 says a transaction may write to only one
    # attached database — so the engine binds its batch outside the transaction
    # and the test does the same rather than relying on a view being exempt.
    views = [make_view(lake, rows) for rows in BATCHES[:2]]

    before = snapshot_count(lake)
    lake.execute("BEGIN")
    for batch_id, view in enumerate(views, start=1):
        sink.write(lake, view, model, make_ctx(model, batch_id))
    lake.execute("COMMIT")

    assert snapshot_count(lake) - before == 1
    assert canonical(sink_rows(lake, sink, model)) == canonical(
        full_recompute(lake, sink, model, BATCHES[:2])
    )


def test_the_first_write_and_its_ddl_still_make_only_one_snapshot(lake):
    # The first trigger is the awkward one: it creates the table *and* merges
    # into it. Both must land in the same snapshot, or a crash between them
    # would leave a committed empty table with no rows and no offset.
    sink = TableSink("marts.fresh")
    model = make_model(sink)
    views = [make_view(lake, rows) for rows in BATCHES[:2]]

    before = snapshot_count(lake)
    lake.execute("BEGIN")
    for batch_id, view in enumerate(views, start=1):
        sink.write(lake, view, model, make_ctx(model, batch_id))
    lake.execute("COMMIT")

    assert snapshot_count(lake) - before == 1


def test_a_rolled_back_transaction_leaves_no_snapshot_and_no_rows(lake):
    sink = TableSink("marts.hourly")
    model = make_model(sink)
    sink.ensure(lake, model)
    view = make_view(lake, BATCHES[0])

    before = snapshot_count(lake)
    lake.execute("BEGIN")
    sink.write(lake, view, model, make_ctx(model, 1))
    lake.execute("ROLLBACK")

    assert snapshot_count(lake) == before
    assert sink.existing_columns(lake) == []


def test_a_small_batch_still_writes_a_parquet_file(lake):
    # CONTEXT.md 1.7: inlining defaults to 10 rows and silently captures any
    # smaller write into the catalog, which is DuckLake's buggiest code path.
    # attach_lake disables it; this is that measurement applied to the sink's
    # own table, with a batch deliberately under the threshold.
    sink = TableSink("marts.small")
    model = make_model(sink)
    small_batch = [row(0, "a", 1), row(5, "a", 2), row(10, "b", 3)]
    assert len(small_batch) < 10

    sink.ensure(lake, model)
    sink.write(lake, make_view(lake, small_batch), model, make_ctx(model, 1))

    assert list_files(lake, "lake", "marts.small") != []
    assert data_file_count(lake, "lake", "marts.small") >= 1


# ==========================================================================
# Identifier hostility
# ==========================================================================

HOSTILE_SCHEMA = "Odd Marts"
HOSTILE_TABLE = 'He said "hi"'
HOSTILE_KEY = 'Sensor Id "raw"'
HOSTILE_MEASURE = "Total Sum"


def test_hostile_identifiers_survive_creation_and_two_merges(lake):
    # A key column and a table name each carrying a space, a quote and mixed
    # case. Model.validate would refuse these key names — it holds identifiers
    # to a plain [A-Za-z_]\w* — so the sink is exercised directly. It still has
    # to quote correctly, because a sink is also reachable from a user's own
    # code and quoting is not something to get right only when it is checked.
    sink = TableSink(qualified(HOSTILE_SCHEMA, HOSTILE_TABLE))
    assert (sink.schema, sink.name) == (HOSTILE_SCHEMA, HOSTILE_TABLE)

    columns = ("event_ts", HOSTILE_KEY, "val")
    model = make_model(
        sink,
        name="hostile",
        key=["window_ts", HOSTILE_KEY],
        aggregates={HOSTILE_MEASURE: "sum(val)", "Row Count": "count(*)"},
    )
    batches = [
        [row(0, "a", 1), row(5, "a", 2), row(70, "b", 3)],
        [row(10, "a", 4), row(75, "b", 5)],
        [row(15, None, 6), row(20, None, 7)],
    ]

    sink.ensure(lake, model)
    write_batches(lake, sink, model, batches, columns=columns)

    assert canonical(sink_rows(lake, sink, model)) == canonical(
        full_recompute(lake, sink, model, batches, columns=columns)
    )
    assert sink.existing_columns(lake) == [
        "window_ts",
        HOSTILE_KEY,
        HOSTILE_MEASURE,
        "Row Count",
    ]
    rows = {(r[0], r[1]): r[2:] for r in sink_rows(lake, sink, model)}
    assert rows[(BASE, "a")] == (7, 3)
    assert rows[(BASE, None)] == (13, 2)


def test_a_semicolon_in_a_table_name_is_data_not_a_statement(lake):
    sink = TableSink(qualified("marts", "drop; DROP TABLE victim; --"))
    model = make_model(sink)
    lake.execute("CREATE SCHEMA IF NOT EXISTS marts")
    lake.execute("CREATE TABLE marts.victim (i INTEGER)")

    write_batches(lake, sink, model, BATCHES[:2])

    assert lake.execute("SELECT count(*) FROM marts.victim").fetchone() == (0,)
    assert len(sink_rows(lake, sink, model)) > 0


# ==========================================================================
# A realistic batch view
# ==========================================================================


def test_folds_correctly_over_a_real_file_source_batch(lake, tmp_path):
    # Everything above binds a VALUES list. This one goes through the actual
    # phase-1 ingestion path — parquet files on disk, planned and bound by
    # FileSource — so the sink is shown working over a reader view rather than
    # only over a literal one.
    landing = tmp_path / "landing"
    landing.mkdir()
    writer = duckdb.connect()
    for index, batch in enumerate(BATCHES):
        view = make_view(writer, batch)
        target = (landing / f"part{index}.parquet").as_posix()
        writer.execute(
            f"COPY (SELECT * FROM {quote_ident(view)}) TO '{target}' (FORMAT parquet)"
        )
    writer.close()

    source = FileSource(landing, marker=None)
    sink = TableSink("marts.from_files")
    model = make_model(sink, source=source)
    sink.ensure(lake, model)

    # Two passes so the WHEN MATCHED branch is reached, with the offset from the
    # first pass carried into the second exactly as the engine will do it.
    offset = None
    seen = 0
    while True:
        plan = source.plan(offset, source.latest_offset(), None)
        if plan.is_empty:
            break
        view = source.bind(lake, plan)
        seen += 1
        sink.write(lake, view, model, make_ctx(model, seen))
        offset = plan.end
    assert seen >= 1

    assert canonical(sink_rows(lake, sink, model)) == canonical(
        full_recompute(lake, sink, model, BATCHES)
    )
