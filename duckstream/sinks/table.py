"""The table sink: append rows, or fold them into a keyed table with ``MERGE``.

This module is where ``CONTEXT.md`` section 4's bug class lives or dies. A mart
in this repository folded averages as ``(target.avg + source.avg) / 2`` and held
**3.0** where the truth was **2.0**; another overwrote standard deviation with
the last batch's value, storing **0.0** instead of **1.7342**. Both were merges
exactly like the one generated below, written by hand, over aggregates that do
not fold. :meth:`TableSink.write` refuses to build such a merge at all —
:func:`duckstream.aggregates.fold_expression` is the only way a fold is
obtained, and it raises for anything outside the additive tier.

Three DuckLake-specific decisions are recorded here because they are not
obvious and were measured, not reasoned:

**No scalar subquery reaches the MERGE.** ``CONTEXT.md`` section 1.5 measured a
``MERGE`` whose join condition contained ``(SELECT lo FROM bounds)`` failing on
DuckLake with ``Out of buffer`` — and only on the *second* batch, the first one
to take the ``WHEN MATCHED`` branch. The identical statement passes on in-memory
DuckDB. A table subquery as the ``USING`` source is fine and is what this module
emits; anything else that would need a subquery is computed in Python and
inlined with :func:`duckstream.sql.quote_literal`.

**Keys match with ``IS NOT DISTINCT FROM``.** That same measurement explicitly
cleared it of blame. It matters: with plain ``=``, a NULL grouping key never
matches itself, so every batch takes the ``WHEN NOT MATCHED`` branch and inserts
another row for the same key. The table grows a duplicate per batch and the
numbers are quietly wrong.

**The table is created on the first write, not by ``ensure``.** ``ensure``
receives a model but no data, so it cannot know that ``sum(value)`` is a
``DECIMAL(38,1)`` and ``count(*)`` a ``BIGINT``. Guessing produces a table whose
types silently coerce — the sort of thing that turns up as a rounding complaint
months later. Instead ``ensure`` creates the *schema* and, when the table
already exists, checks its columns against the model; the *table* is created by
``CREATE TABLE ... AS SELECT ... WHERE false`` off the real aggregation, so
DuckDB assigns every type from the actual expression. Verified on DuckLake:
that DDL and the merge that follows sit happily in one transaction and produce
one snapshot.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, ClassVar

from duckstream.aggregates import fold_expression
from duckstream.errors import DuckstreamError
from duckstream.model import GRAINS, WINDOW_COLUMN
from duckstream.sql import (
    DEFAULT_SCHEMA,
    qualified,
    quote_ident,
    quote_literal,
    split_qualified,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from duckstream.model import Model
    from duckstream.protocols import BatchContext

__all__ = ["TableSink", "MODES", "TARGET_ALIAS", "SOURCE_ALIAS"]


#: The two output modes phase 1 supports. ``append`` writes every batch's rows;
#: ``update`` folds them into one row per key.
MODES: tuple[str, ...] = ("append", "update")

#: Aliases used in the generated MERGE. Short on purpose — they appear in every
#: fold expression, and :func:`duckstream.aggregates.fold_expression` emits them
#: verbatim.
TARGET_ALIAS = "t"
SOURCE_ALIAS = "s"

#: Alias for the shape-only subquery in ``CREATE TABLE ... AS``. Never queried,
#: but DuckDB requires a subquery in ``FROM`` to be named.
_SHAPE_ALIAS = "ds_shape"

#: What a type string may contain before duckstream will interpolate it into a
#: castability probe. A type name is not a value and cannot be passed as a
#: literal, so it is screened instead. Every built-in type passes —
#: ``DECIMAL(38,1)``, ``TIMESTAMP WITH TIME ZONE``, ``BIGINT[]``,
#: ``STRUCT(a VARCHAR)``, ``MAP(VARCHAR, BIGINT)``. A user-defined type whose
#: *name* carries a quote does not, and falls back to exact-match comparison:
#: stricter, never unsafe.
_SAFE_TYPE = re.compile(r"^[A-Za-z0-9_ ,()\[\]]+$")


class TableSink:
    """Write a model's aggregated batch to a DuckLake (or DuckDB) table.

    Parameters
    ----------
    table:
        ``"name"`` or ``"schema.name"``. Quoted parts are understood, so
        ``'"odd schema"."a.b"'`` names a table whose name contains a dot. An
        unqualified name lands in ``main``, which is what plain SQL would do.
    mode:
        ``"update"`` (default) merges on the model's key, folding each aggregate
        into the stored value. ``"append"`` inserts the batch's aggregated rows
        and never merges.

    Schema and table creation
    -------------------------
    :meth:`ensure` creates the *schema* only, and validates an existing table's
    columns against the model. It cannot create the table: with no data in hand
    it would have to guess column types, and a guessed type is a silent
    coercion. The *table* is created on the first :meth:`write` with
    ``CREATE TABLE IF NOT EXISTS ... AS SELECT ... WHERE false`` over the real
    aggregation, so every type comes from DuckDB's own inference on the
    expression the model declared. Subsequent writes leave it alone.

    Idempotency
    -----------
    Neither mode is idempotent on its own, and it is worth being exact about
    why rather than repeating the usual "merge is idempotent" line.

    ``update`` keeps the *row set* stable — the merge key decides which row a
    batch lands on, so a replay updates an existing row instead of adding one —
    but an additive fold is not idempotent in its *values*: replaying a batch
    adds its counts and sums a second time. ``append`` is not idempotent in
    either sense.

    Exactly-once therefore comes from the engine writing output rows and the
    source offset in one transaction, so a batch that was never committed is
    the only kind that gets replayed. What the merge key buys is that the sink
    converges to one row per key rather than accumulating duplicates, which is
    what makes a *corrected* re-run — and a NULL grouping key — behave.

    Foldability
    -----------
    ``update`` supports the **additive** tier only in phase 1. A model whose
    resolved strategy is ``sufficient_statistics`` or ``recompute_window`` is
    refused, loudly, rather than folded as if it were additive. That refusal is
    the framework's reason to exist; see ``CONTEXT.md`` section 4.
    """

    type_name: ClassVar[str] = "table"

    def __init__(self, table: str, *, mode: str = "update") -> None:
        if mode not in MODES:
            raise DuckstreamError(
                f"TableSink mode {mode!r} is not supported; expected one of "
                f"{', '.join(repr(m) for m in MODES)}. 'update' folds each batch "
                f"into one row per key; 'append' adds the batch's rows and does "
                f"not deduplicate."
            )
        self.table = table
        self.mode = mode
        # Parsed eagerly so a malformed name fails at construction, where the
        # traceback points at the user's declaration, not at 03:00 in a cron log.
        self.schema, self.name = split_qualified(table)

    # -- naming ------------------------------------------------------------

    @property
    def qualified_name(self) -> str:
        """The target as SQL: ``"marts"."hourly"``."""
        return qualified(self.schema, self.name)

    # -- shape derived from the model --------------------------------------

    def key_expressions(self, model: "Model") -> list[tuple[str, str]]:
        """``[(output column, SQL expression)]`` for the grouping columns.

        Every key column is a plain column reference on the batch view, with one
        exception: when the model declares a ``grain``, ``window_ts`` is
        *derived*, not read. It becomes
        ``date_trunc('<grain>', "<time_column>")``. The user never writes it —
        the window column is named ``window_ts`` at every grain precisely so
        that the "merge key must equal the window grain key" invariant is
        checkable mechanically.
        """
        expressions: list[tuple[str, str]] = []
        for column in self._key_columns(model):
            if column == WINDOW_COLUMN and model.grain is not None:
                expressions.append((column, self._window_expression(model)))
            else:
                expressions.append((column, quote_ident(column)))
        return expressions

    def _window_expression(self, model: "Model") -> str:
        grain = model.grain
        if grain not in GRAINS:
            raise DuckstreamError(
                f"TableSink cannot window model {model.name!r} at grain "
                f"{grain!r}; expected one of {', '.join(repr(g) for g in GRAINS)}"
            )
        if not model.time_column:
            raise DuckstreamError(
                f"model {model.name!r} declares grain {grain!r} but no "
                f"time_column, so there is no column to truncate into "
                f"{WINDOW_COLUMN!r}"
            )
        # The grain is inlined as a literal rather than interpolated raw: it
        # arrives from config, and quote_literal is the only thing standing
        # between a config file and the SQL text.
        return f"date_trunc({quote_literal(grain)}, {quote_ident(model.time_column)})"

    def _key_columns(self, model: "Model") -> list[str]:
        key = list(model.key or [])
        if not key:
            raise DuckstreamError(
                f"model {model.name!r} declares no merge key, so TableSink has "
                f"nothing to group by and no way to make a re-run idempotent"
            )
        return key

    def output_columns(self, model: "Model") -> list[str]:
        """Every column the sink writes: key columns first, then aggregates."""
        key = self._key_columns(model)
        overlap = [column for column in model.aggregates if column in set(key)]
        if overlap:
            raise DuckstreamError(
                f"model {model.name!r} declares {overlap[0]!r} as both a merge "
                f"key and an aggregate; it cannot be a grouping column and a "
                f"computed one at the same time"
            )
        return key + list(model.aggregates)

    # -- SQL ---------------------------------------------------------------

    def aggregation_sql(self, batch_view: str, model: "Model") -> str:
        """The per-batch aggregation, as a standalone ``SELECT``.

        ``SELECT <key columns>, <aggregate expressions AS output names>
        FROM <batch_view> GROUP BY <key columns>``.

        Key expressions are repeated in the ``GROUP BY`` rather than referenced
        by ordinal. Ordinals are shorter and are the sort of thing that silently
        groups by the wrong column the day someone reorders the key.

        The aggregate expressions themselves are emitted verbatim: they are user
        SQL, already parsed and classified by
        :mod:`duckstream.aggregates`, which rejects smuggled statements.
        """
        keys = self.key_expressions(model)
        projections = [
            f"{expression} AS {quote_ident(column)}" for column, expression in keys
        ]
        projections += [
            f"{expression} AS {quote_ident(column)}"
            for column, expression in model.aggregates.items()
        ]
        self.output_columns(model)  # raises on a key/aggregate collision
        grouping = ", ".join(expression for _, expression in keys)
        select_list = ",\n       ".join(projections)
        return (
            f"SELECT {select_list}\n"
            f"  FROM {qualified(batch_view)}\n"
            f" GROUP BY {grouping}"
        )

    def create_table_sql(self, batch_view: str, model: "Model") -> str:
        """DDL creating the target from the aggregation's *shape*, with no rows.

        ``WHERE false`` is applied outside the aggregation, not inside it. An
        aggregation with an empty ``GROUP BY`` still returns one row over zero
        input rows — ``SELECT count(*) FROM t WHERE false`` is ``0``, not
        nothing — so filtering inside would create the table pre-populated with
        a bogus row. Wrapping it guarantees zero rows whatever the shape.
        """
        aggregation = self.aggregation_sql(batch_view, model)
        return (
            f"CREATE TABLE IF NOT EXISTS {self.qualified_name} AS\n"
            f"SELECT * FROM (\n{aggregation}\n) AS {quote_ident(_SHAPE_ALIAS)} "
            f"WHERE false"
        )

    def insert_sql(self, batch_view: str, model: "Model") -> str:
        """``INSERT INTO ... SELECT`` for ``append`` mode.

        Columns are listed explicitly so the statement does not depend on the
        target's physical column order, which schema evolution may change.
        """
        columns = ", ".join(quote_ident(c) for c in self.output_columns(model))
        aggregation = self.aggregation_sql(batch_view, model)
        return f"INSERT INTO {self.qualified_name} ({columns})\n{aggregation}"

    def merge_sql(self, batch_view: str, model: "Model") -> str:
        """The fold, as one ``MERGE INTO`` statement.

        Shape::

            MERGE INTO <target> AS t
            USING (<aggregation>) AS s
               ON t."k" IS NOT DISTINCT FROM s."k" AND ...
             WHEN MATCHED THEN UPDATE SET "c" = <fold(t.c, s.c)>, ...
             WHEN NOT MATCHED THEN INSERT (...) VALUES (s...., ...)

        Two properties are non-negotiable and both come from measurement.

        ``IS NOT DISTINCT FROM``, not ``=``: a NULL grouping key does not equal
        itself, so under ``=`` every batch would fall through to
        ``WHEN NOT MATCHED`` and insert another row for the same key.
        ``CONTEXT.md`` 1.5 measured ``IS NOT DISTINCT FROM`` against DuckLake and
        cleared it — the ``Out of buffer`` failure was caused by a scalar
        subquery, not by this operator.

        No scalar subquery, anywhere. The ``USING`` source is a *table*
        subquery, which is fine; a ``(SELECT ...)`` in the ``ON`` condition is
        what fails, and only on the second merge.

        Raises:
            DuckstreamError: if the model is not additive. See
                :meth:`_require_additive`.
        """
        self._require_additive(model)
        aggregation = self.aggregation_sql(batch_view, model)
        target = TARGET_ALIAS
        source = SOURCE_ALIAS

        on_clause = "\n   AND ".join(
            f"{target}.{quote_ident(column)} IS NOT DISTINCT FROM "
            f"{source}.{quote_ident(column)}"
            for column in self._key_columns(model)
        )
        updates = ",\n         ".join(
            f"{quote_ident(column)} = "
            f"{fold_expression(column, expression, target, source)}"
            for column, expression in model.aggregates.items()
        )
        columns = self.output_columns(model)
        insert_columns = ", ".join(quote_ident(c) for c in columns)
        insert_values = ", ".join(f"{source}.{quote_ident(c)}" for c in columns)

        indented = "\n".join("       " + line for line in aggregation.splitlines())
        return (
            f"MERGE INTO {self.qualified_name} AS {target}\n"
            f"USING (\n{indented}\n) AS {source}\n"
            f"   ON {on_clause}\n"
            f" WHEN MATCHED THEN UPDATE SET\n         {updates}\n"
            f" WHEN NOT MATCHED THEN INSERT ({insert_columns})\n"
            f"      VALUES ({insert_values})"
        )

    # -- foldability guard --------------------------------------------------

    def _require_additive(self, model: "Model") -> None:
        """Refuse to merge anything but the additive tier, in phase 1.

        ``CONTEXT.md`` section 4 is the reason this is an exception and not a
        best-effort fold. A mart that folds an average as if it were additive
        does not fail; it returns a plausible wrong number, and nobody notices
        until it is compared against a full recompute. Silence is the failure
        mode being designed out.
        """
        strategy = model.resolved_strategy
        if strategy == "delta_merge":
            return
        tier = model.tier
        raise DuckstreamError(
            f"TableSink(mode='update') cannot maintain model {model.name!r}: it "
            f"classifies as tier {str(tier)!r} and resolves to strategy "
            f"{strategy!r}, but phase 1 implements the additive tier "
            f"('delta_merge') only. Folding a {str(tier)!r} aggregate with an "
            f"additive merge does not fail — it stores a plausible wrong number "
            f"(CONTEXT.md section 4 records a mart that held 3.0 where the truth "
            f"was 2.0), so duckstream refuses instead. The {strategy!r} strategy "
            f"arrives in phase 3; until then use mode='append', or reduce the "
            f"model to count/sum/min/max."
        )

    # -- catalog introspection ---------------------------------------------

    def existing_column_types(self, con: Any) -> dict[str, str]:
        """``{column name: declared type}`` for the target, in physical order.

        Empty when the table does not exist — a table always has at least one
        column, so the two cases do not overlap. Read from ``duckdb_columns()``
        rather than by probing with a ``SELECT``, because a failing statement is
        a poor thing to issue inside the engine's transaction and :meth:`write`
        runs inside it. Verified on DuckLake that this sees a table created
        earlier in the *same* uncommitted transaction.
        """
        rows = con.execute(
            "SELECT column_name, data_type FROM duckdb_columns() "
            "WHERE database_name = current_database() "
            f"  AND schema_name = {quote_literal(self.schema)} "
            f"  AND table_name = {quote_literal(self.name)} "
            "ORDER BY column_index"
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def existing_columns(self, con: Any) -> list[str]:
        """Column names of the target as the catalog currently sees it."""
        return list(self.existing_column_types(con))

    def incoming_column_types(
        self, con: Any, batch_view: str, model: "Model"
    ) -> dict[str, str]:
        """``{column name: type}`` the aggregation would produce for this batch.

        ``DESCRIBE`` over the aggregation with ``WHERE false`` binds the query
        and returns its result schema without scanning anything. This is the
        information :meth:`ensure` structurally cannot have: it is handed a
        model but no data, and the type of ``sum(value)`` is a property of the
        data, not of the declaration.
        """
        aggregation = self.aggregation_sql(batch_view, model)
        return {
            row[0]: row[1]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM (\n{aggregation}\n) "
                f"AS {quote_ident(_SHAPE_ALIAS)} WHERE false"
            ).fetchall()
        }

    def _types_are_compatible(self, con: Any, incoming: str, target: str) -> bool:
        """Whether DuckDB has an implicit cast between the two types, either way.

        The rule is deliberately DuckDB's own rather than a compatibility matrix
        maintained here, and deliberately *bidirectional*:

        - **Same type** — trivially fine.
        - **Incoming casts implicitly to target** (an ``INTEGER`` delta into a
          ``BIGINT`` column) — a widening store, nothing is lost.
        - **Target casts implicitly to incoming** (a ``BIGINT`` column receiving
          a ``HUGEINT`` sum, or an ``INTEGER`` column receiving a ``BIGINT``
          count) — a narrowing store. DuckDB performs it and raises only if an
          individual value does not fit, so the pipeline works for the range it
          actually sees. This direction is permitted on purpose: ``sum`` over a
          ``BIGINT`` widens to ``HUGEINT`` and ``count(*)`` is ``BIGINT``, so a
          sensible hand-made table declaring ``total BIGINT, n INTEGER`` would
          otherwise be refused despite working today. Breaking a working
          pipeline is the worse error.
        - **Neither direction** (``VARCHAR`` against ``BIGINT``, ``BOOLEAN``
          against ``BIGINT``, ``BIGINT[]`` against ``BIGINT``) — refused. There
          is no cast DuckDB will apply, so the fold cannot bind and the write
          would fail part-way through the engine's transaction.

        A type string that does not pass :data:`_SAFE_TYPE` is never
        interpolated into the probe; the comparison degrades to exact equality,
        which is stricter and never unsafe. The same fallback covers
        ``can_cast_implicitly`` being absent on some future build — a probe that
        cannot run must not silently pass.
        """
        if incoming == target:
            return True
        if not (_SAFE_TYPE.match(incoming) and _SAFE_TYPE.match(target)):
            return False
        try:
            row = con.execute(
                f"SELECT can_cast_implicitly(CAST(NULL AS {incoming}), "
                f"CAST(NULL AS {target})) "
                f"OR can_cast_implicitly(CAST(NULL AS {target}), "
                f"CAST(NULL AS {incoming}))"
            ).fetchone()
        except Exception:
            return False
        return bool(row and row[0])

    def _check_target_matches(
        self, con: Any, batch_view: str, model: "Model", existing: dict[str, str]
    ) -> None:
        """Refuse a pre-existing table this batch cannot be written into.

        Runs before any statement that writes. The alternative is a
        ``BinderException`` raised from inside the ``MERGE`` — after the engine
        has opened its transaction, with the model's aggregate expression buried
        in a message about ``+(VARCHAR, BIGINT)``. Failing here costs one
        ``DESCRIBE`` and names the column, the two types and the likely cause.

        Both halves of a mismatch are checked: a missing column, which
        :meth:`ensure` can also catch, and a column whose type cannot receive
        the aggregate, which :meth:`ensure` cannot — it has no data and so no
        types. Extra columns on the target are deliberately not an error; see
        :meth:`write`.

        **The type check applies to ``update`` only, and that is measured, not
        assumed.** A fold has to bind an operator between the stored value and
        the delta, and ``VARCHAR + BIGINT`` resolves to no function at all --
        the reported failure. An ``INSERT`` has no such requirement: DuckDB
        applies an assignment cast, so ``append`` of a ``BIGINT`` ``count(*)``
        into a ``VARCHAR`` column succeeds today and stores ``'7'``. Refusing
        that would break a working pipeline to prevent a failure that does not
        happen, so ``append`` gets the name check and no more. Where append
        genuinely cannot convert (``TIMESTAMP`` into ``BIGINT``, a scalar into a
        list column) DuckDB raises a conversion error of its own naming both
        types.
        """
        self._validate_existing(model, list(existing))
        if self.mode != "update":
            return
        incoming = self.incoming_column_types(con, batch_view, model)
        for column, incoming_type in incoming.items():
            target_type = existing.get(column)
            if target_type is None:  # already reported by _validate_existing
                continue
            if self._types_are_compatible(con, incoming_type, target_type):
                continue
            origin = (
                f" (from {model.aggregates[column]!r})"
                if column in model.aggregates
                else ""
            )
            raise DuckstreamError(
                f"table {self.table!r} cannot receive model {model.name!r}: "
                f"column {column!r} is {target_type} in the table but the model "
                f"produces {incoming_type}{origin}. DuckDB has no implicit cast "
                f"in either direction between {incoming_type} and "
                f"{target_type}, so the merge cannot even be built. Either the "
                f"table predates this model or the model changed: drop or alter "
                f"{self.table!r}, or change the aggregate so its type matches. "
                f"duckstream checks this before writing rather than letting the "
                f"MERGE fail part-way through your transaction."
            )

    # -- protocol ----------------------------------------------------------

    def ensure(self, con: Any, model: "Model") -> None:
        """Create the schema, and validate the table if it already exists.

        Idempotent, and deliberately *not* the place the table is created — see
        the class docstring. What it does do is fail early: an ``update`` model
        the sink cannot fold is rejected here, at DDL time, rather than after
        the first batch has been planned.

        What it checks is column *names* only. Types are not knowable here —
        ``ensure`` is handed a model and no data, and ``sum(value)`` has no type
        until there is a value — so the type check lives in :meth:`write`, which
        has a bound batch and can ask DuckDB directly. That split is a
        constraint, not a preference.

        Raises:
            DuckstreamError: if the target exists but is missing a column the
                model writes, naming the columns rather than leaving the
                operator to diff two schemas by eye.
        """
        if self.mode == "update":
            self._require_additive(model)
        self.output_columns(model)
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(self.schema)}")
        existing = self.existing_columns(con)
        if not existing:
            return
        self._validate_existing(model, existing)

    def _validate_existing(self, model: "Model", existing: list[str]) -> None:
        present = set(existing)
        required = self.output_columns(model)
        missing = [column for column in required if column not in present]
        if not missing:
            return
        raise DuckstreamError(
            f"table {self.table!r} already exists but does not match model "
            f"{model.name!r}: missing column"
            f"{'s' if len(missing) > 1 else ''} "
            f"{', '.join(repr(c) for c in missing)}. The model writes "
            f"{', '.join(repr(c) for c in required)}; the table has "
            f"{', '.join(repr(c) for c in existing)}. Column names are compared "
            f"exactly, because duckstream quotes every identifier and a quoted "
            f"name is case-sensitive. Drop or alter the table, or rename the "
            f"model's columns to match."
        )

    def write(
        self,
        con: Any,
        batch_view: str,
        model: "Model",
        ctx: "BatchContext",
    ) -> None:
        """Write one batch. Called inside the engine's transaction, never outside.

        In ``update`` mode this is a ``MERGE`` on the model's key, folding each
        aggregate into the stored value. One row per key, whatever the batch
        boundaries — but see the class docstring: folding twice adds twice.

        In ``append`` mode this is a plain ``INSERT`` of the batch's aggregated
        rows. **Append does not deduplicate.** It has no key to deduplicate on
        and no memory of previous batches; replaying a batch appends its rows a
        second time. Idempotency for append comes entirely from the engine
        committing output rows and the source offset in one transaction, so a
        crash before ``COMMIT`` replays a batch that was never durable. It never
        comes from the sink, and no amount of retry logic in the sink could
        provide it.

        The target table is created here on the first write if it is absent,
        from the aggregation's own shape. If it is *present*, it is checked
        against the batch first — columns always, and in ``update`` mode the
        column types too, which :meth:`ensure` cannot do — so a table that
        predates the model is refused before any statement runs rather than
        raising a binder error part-way through the transaction.

        **Extra columns on the target are allowed and left alone.** The
        generated ``INSERT``/``MERGE`` name their columns explicitly, so a
        column the model does not write keeps whatever it had and is NULL on
        rows the sink inserts. That is deliberate: a table may legitimately
        carry an annotation column, or a column belonging to a wider schema the
        model is only one contributor to, and refusing it would make schema
        evolution needlessly painful. It does mean a *renamed* aggregate leaves
        its predecessor behind, silently NULL, so a rename wants a migration.

        ``ctx`` is accepted for the protocol and is not used: idempotency comes
        from the merge key, not from the batch id, so nothing about the write
        depends on which batch this is.
        """
        if self.mode == "update":
            self._require_additive(model)
        self._prepare_target(con, batch_view, model)
        statement = (
            self.insert_sql(batch_view, model)
            if self.mode == "append"
            else self.merge_sql(batch_view, model)
        )
        con.execute(statement)

    def _prepare_target(self, con: Any, batch_view: str, model: "Model") -> None:
        """Create the target if absent, otherwise check the batch fits it."""
        existing = self.existing_column_types(con)
        if existing:
            self._check_target_matches(con, batch_view, model, existing)
            return
        # write() may be the first thing that ever touches this sink — the
        # engine calls ensure(), but a library user driving the sink directly
        # need not have. CREATE SCHEMA IF NOT EXISTS is idempotent and cheap.
        # Nothing to type-check on this path: the table is about to be created
        # from this very aggregation, so its types are the incoming ones.
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(self.schema)}")
        con.execute(self.create_table_sql(batch_view, model))

    def to_config(self) -> dict[str, Any]:
        """Round-trippable declaration: ``type``, ``table``, ``mode``.

        ``mode`` is always emitted, even at its default. It is the difference
        between a table that deduplicates and one that does not, and a reader of
        the YAML should not have to know which way the default falls.

        ``table`` is emitted exactly as given rather than re-rendered from the
        parsed parts, so a config file round-trips to itself instead of growing
        a ``main.`` prefix nobody wrote.
        """
        return {"type": self.type_name, "table": self.table, "mode": self.mode}

    # -- equality ----------------------------------------------------------
    #
    # The config round-trip test compares whole `Model` objects, so a sink
    # without value equality would fail it for identity reasons alone. Equality
    # is on the *parsed* name, which makes "counts" and "main.counts" the same
    # sink — they name the same table, and a round trip through a config file
    # that normalises one to the other should still compare equal.

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TableSink):
            return NotImplemented
        return (self.schema, self.name, self.mode) == (
            other.schema,
            other.name,
            other.mode,
        )

    def __hash__(self) -> int:
        return hash((type(self).__name__, self.schema, self.name, self.mode))

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        suffix = "" if self.schema == DEFAULT_SCHEMA else f", schema={self.schema!r}"
        return f"TableSink({self.table!r}, mode={self.mode!r}{suffix})"
