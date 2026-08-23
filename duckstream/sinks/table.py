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

from duckstream.aggregates import (
    Tier,
    classify_expression,
    fold_expression,
    statistic_derive_sql,
    statistic_merge_sql,
    statistic_seed_sql,
    statistic_state,
)
from duckstream.errors import DuckstreamError
from duckstream.model import GRAINS, WINDOW_COLUMN
from duckstream.sql import (
    DEFAULT_SCHEMA,
    qualified,
    quote_ident,
    quote_literal,
    split_qualified,
)
from duckstream.windows import seal_cutoff, window_expression

if TYPE_CHECKING:  # pragma: no cover - typing only
    from duckstream.model import Model
    from duckstream.protocols import BatchContext

__all__ = ["TableSink", "MODES", "TARGET_ALIAS", "SOURCE_ALIAS"]


#: The two output modes. ``update`` folds every batch into one row per key.
#: ``append`` means one of two things depending on whether the model windows:
#: with no ``grain`` it writes the batch's rows and never merges; with a
#: ``grain`` (and therefore a lateness horizon) it folds each window in an
#: accumulator and writes it once, when the watermark seals it.
MODES: tuple[str, ...] = ("append", "update")

#: Aliases used in the generated MERGE. Short on purpose — they appear in every
#: fold expression, and :func:`duckstream.aggregates.fold_expression` emits them
#: verbatim.
TARGET_ALIAS = "t"
SOURCE_ALIAS = "s"

#: Alias for the shape-only subquery in ``CREATE TABLE ... AS``. Never queried,
#: but DuckDB requires a subquery in ``FROM`` to be named.
_SHAPE_ALIAS = "ds_shape"

#: Suffix of the open-window accumulator that ``append`` mode folds into while
#: a window is still open. Beside the target, in the target's own schema.
_OPEN_SUFFIX = "__open_windows"

#: Alias for the state subquery a tier-two aggregation nests its derived columns
#: over. Never queried by name from outside the generated statement.
_STATE_ALIAS = "ds_state"

#: What a type string may contain before duckstream will interpolate it into a
#: castability probe. A type name is not a value and cannot be passed as a
#: literal, so it is screened instead. Every built-in type passes —
#: ``DECIMAL(38,1)``, ``TIMESTAMP WITH TIME ZONE``, ``BIGINT[]``,
#: ``STRUCT(a VARCHAR)``, ``MAP(VARCHAR, BIGINT)``. A user-defined type whose
#: *name* carries a quote does not, and falls back to exact-match comparison:
#: stricter, never unsafe.
_SAFE_TYPE = re.compile(r"^[A-Za-z0-9_ ,()\[\]]+$")


def _affected(result: Any) -> int | None:
    """The affected-row count DuckDB returns from a write, or ``None``.

    ``INSERT``, ``MERGE`` and ``DELETE`` each come back as a single row holding
    a single count -- verified on 1.5.5 against DuckLake tables, for a MERGE
    taking the matched branch as well as the not-matched one. Wrapped in a
    ``try`` because this is bookkeeping: a build that stopped returning a count,
    or returned some other shape, must cost a NULL in the metrics rather than
    the batch.
    """
    try:
        row = result.fetchall()
    except Exception:  # pragma: no cover - defensive
        return None
    if not row or not row[0] or row[0][0] is None:
        return None
    try:
        return int(row[0][0])
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return None


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
        into the stored value. ``"append"`` writes each output row once and
        never revises it -- see "Output modes" below, because what that takes
        depends on whether the model windows.

    Output modes
    ------------
    ``update`` merges the batch into the target on the model's key. A row keeps
    being revised for as long as its window can still receive data, which -- if
    the model declares a lateness horizon -- means until that window seals.

    ``append`` **without a grain** inserts the batch's aggregated rows. There is
    no key to deduplicate on and no memory of previous batches; replaying a
    batch appends its rows again. Any tier is accepted, because nothing folds.

    ``append`` **with a grain** is the sealed-window path, and it is a different
    mechanism rather than a variation on the first. The batch is folded into an
    open-window accumulator beside the target -- ``<name>__open_windows``, same
    schema, so it shares the catalog as ``CONTEXT.md`` 1.9 requires -- using
    exactly the ``MERGE`` ``update`` mode uses. When ``ctx.watermark`` passes a
    window's end, that window is inserted into the target and evicted from the
    accumulator, in the same transaction. So each window reaches the target
    once, complete, and is never revised: the target is genuinely append-only
    and a reader can treat a row it has seen as final.

    A model in that shape necessarily declares a lateness horizon --
    ``Model.validate`` refuses it otherwise, because without a watermark no
    window is ever knowably complete and each batch would append a partial row
    per window. It also necessarily folds across batches, so unlike unwindowed
    append it requires the additive tier.

    **This sink does not decide what is late.** Rows whose window has already
    sealed are removed by the engine before ``write`` is called; handed one, the
    sink folds it and emits that window a second time, because it has no
    watermark history to check against. Duplicating the check here would put the
    decision in two places and this would be the copy without the committed
    watermark.

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
    Every path that folds -- ``update``, and windowed ``append`` -- supports the
    **additive** tier only so far. A model whose resolved strategy is
    ``sufficient_statistics`` or ``recompute_window`` is refused, loudly, rather
    than folded as if it were additive. That refusal is the framework's reason
    to exist; see ``CONTEXT.md`` section 4. Unwindowed ``append`` is exempt
    because it never folds.
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

    @property
    def open_windows_name(self) -> str:
        """Unqualified name of the open-window accumulator for ``append`` mode."""
        return f"{self.name}{_OPEN_SUFFIX}"

    @property
    def qualified_open_windows(self) -> str:
        """The accumulator as SQL: ``"marts"."hourly__open_windows"``.

        It lives in the **target's own schema**, deliberately. It must be in the
        same catalog as the target, because sealing moves rows from one to the
        other inside the engine's single transaction and ``CONTEXT.md`` 1.9
        measured that a transaction cannot span two attached databases. Putting
        it beside the target rather than hiding it in the state schema also
        makes it inspectable: it holds the user's own not-yet-complete windows,
        and "why is this hour missing from my mart" is answered by selecting
        from it.
        """
        return qualified(self.schema, self.open_windows_name)

    def windowed_append(self, model: "Model") -> bool:
        """Is this the sealed-window append path?

        ``append`` over a windowed aggregation. ``Model.validate`` guarantees
        such a model also declares a lateness horizon, so a watermark exists
        and windows can actually seal; ``append`` with no grain is the plain
        per-batch insert and is not this.
        """
        return self.mode == "append" and model.grain is not None

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
        """``date_trunc(<grain>, <time column>)``, from :mod:`duckstream.windows`.

        The arithmetic is not duplicated here. Whether a window has sealed is
        decided in Python against a cutoff, and the row-to-window mapping is
        decided in SQL by this expression; the two have to agree exactly, so
        both come from the one module that owns window boundaries.
        """
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
        return window_expression(grain, model.time_column)

    def _key_columns(self, model: "Model") -> list[str]:
        key = list(model.key or [])
        if not key:
            raise DuckstreamError(
                f"model {model.name!r} declares no merge key, so TableSink has "
                f"nothing to group by and no way to make a re-run idempotent"
            )
        return key

    def output_columns(self, model: "Model") -> list[str]:
        """Every column the sink writes: keys, then aggregates, then any state.

        A ``sufficient_statistics`` model carries three more columns per
        distinct statistic *argument* -- ``n``, ``mean`` and ``M2``. They are
        ordinary columns of the ordinary table, which is what keeps the mart
        time-travellable and readable with plain SQL; the ``_ds_`` prefix is
        what stops them colliding with anything a model declares.
        """
        key = self._key_columns(model)
        overlap = [column for column in model.aggregates if column in set(key)]
        if overlap:
            raise DuckstreamError(
                f"model {model.name!r} declares {overlap[0]!r} as both a merge "
                f"key and an aggregate; it cannot be a grouping column and a "
                f"computed one at the same time"
            )
        return key + list(model.aggregates) + self.state_columns(model)

    def statistic_states(self, model: "Model") -> dict[str, Any]:
        """The distinct statistic states this model needs, keyed by slug.

        Keyed by the *argument*, so ``avg(value)`` and ``stddev(value)`` share
        one state. Keying by output column would store the same numbers twice
        and let the copies drift apart under a partial write.
        """
        if model.resolved_strategy != "sufficient_statistics":
            return {}
        states: dict[str, Any] = {}
        for column in self.statistic_columns(model):
            state = statistic_state(model.aggregates[column])
            states.setdefault(state.slug, state)
        return states

    def statistic_columns(self, model: "Model") -> list[str]:
        """Output columns needing a statistic state rather than a fold.

        A model's tier is the worst of its columns, so a single ``avg`` makes
        the whole model ``sufficient_statistics`` -- but a ``count(*)`` sitting
        beside it is still additive and still folds by addition. Giving it a
        state would store three numbers to reconstruct one it already has, and
        the merge formula has no meaning for it.
        """
        if model.resolved_strategy != "sufficient_statistics":
            return []
        return [
            column
            for column, expression in model.aggregates.items()
            if classify_expression(expression) is Tier.SUFFICIENT_STATISTICS
        ]

    def state_for(self, model: "Model", column: str) -> Any:
        """The state backing one output column."""
        return self.statistic_states(model)[
            statistic_state(model.aggregates[column]).slug
        ]

    def state_columns(self, model: "Model") -> list[str]:
        """Every statistic-state column, in a stable order."""
        return [
            column
            for state in self.statistic_states(model).values()
            for column in state.columns
        ]

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
        self.output_columns(model)  # raises on a key/aggregate collision
        grouping = ", ".join(expression for _, expression in keys)
        key_projections = [
            f"{expression} AS {quote_ident(column)}" for column, expression in keys
        ]

        states = self.statistic_states(model)
        if not states:
            projections = key_projections + [
                f"{expression} AS {quote_ident(column)}"
                for column, expression in model.aggregates.items()
            ]
            select_list = ",\n       ".join(projections)
            return (
                f"SELECT {select_list}\n"
                f"  FROM {qualified(batch_view)}\n"
                f" GROUP BY {grouping}"
            )

        # Tier two never aggregates the declared expression. It computes this
        # batch's mergeable *state* and derives the answer from it, so what
        # lands in the table can be folded with the next batch -- which a
        # finished average cannot be (CONTEXT.md 1.14, and section 4 for what
        # happens when someone tries).
        #
        # Nested rather than flat: the derived columns read the state columns,
        # and a flat SELECT would be relying on being able to reference an alias
        # declared beside them. The subquery makes the dependency explicit and
        # costs nothing.
        statistic = set(self.statistic_columns(model))
        seeds: dict[str, str] = {}
        for state in states.values():
            seeds.update(statistic_seed_sql(state))
        # Additive columns in a tier-two model fold exactly as they always
        # did, so they are aggregated in the inner query beside the seeds.
        inner = ",\n           ".join(
            key_projections
            + [
                f"{expression} AS {quote_ident(column)}"
                for column, expression in model.aggregates.items()
                if column not in statistic
            ]
            + [f"{sql} AS {quote_ident(column)}" for column, sql in seeds.items()]
        )
        derived = ",\n       ".join(
            f"{statistic_derive_sql(model.aggregates[column], self.state_for(model, column), _STATE_ALIAS)}"
            f" AS {quote_ident(column)}"
            for column in model.aggregates
            if column in statistic
        )
        return (
            f"SELECT {quote_ident(_STATE_ALIAS)}.*,\n       {derived}\n"
            f"  FROM (\n"
            f"       SELECT {inner}\n"
            f"         FROM {qualified(batch_view)}\n"
            f"        GROUP BY {grouping}\n"
            f"       ) AS {quote_ident(_STATE_ALIAS)}"
        )

    def create_table_sql(
        self, batch_view: str, model: "Model", *, into: str | None = None
    ) -> str:
        """DDL creating the target from the aggregation's *shape*, with no rows.

        ``WHERE false`` is applied outside the aggregation, not inside it. An
        aggregation with an empty ``GROUP BY`` still returns one row over zero
        input rows — ``SELECT count(*) FROM t WHERE false`` is ``0``, not
        nothing — so filtering inside would create the table pre-populated with
        a bogus row. Wrapping it guarantees zero rows whatever the shape.
        """
        aggregation = self.aggregation_sql(batch_view, model)
        return (
            f"CREATE TABLE IF NOT EXISTS {into or self.qualified_name} AS\n"
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

    def merge_sql(
        self, batch_view: str, model: "Model", *, into: str | None = None
    ) -> str:
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
        into = into or self.qualified_name
        target = TARGET_ALIAS
        source = SOURCE_ALIAS

        on_clause = "\n   AND ".join(
            f"{target}.{quote_ident(column)} IS NOT DISTINCT FROM "
            f"{source}.{quote_ident(column)}"
            for column in self._key_columns(model)
        )
        states = self.statistic_states(model)
        statistic = set(self.statistic_columns(model))
        assignments: list[str] = []

        # Additive columns fold as they always did, in a tier-two model as much
        # as in a tier-one one.
        assignments += [
            f"{quote_ident(column)} = "
            f"{fold_expression(column, expression, target, source)}"
            for column, expression in model.aggregates.items()
            if column not in statistic
        ]

        # The state merges by Chan's formula, and each derived column is
        # computed from the *merged* state rather than read back from it. Every
        # right-hand side of an UPDATE SET sees the pre-update row, so a plain
        # column reference here would silently use the old state; substituting
        # the merge expressions keeps it to one statement. The alternative --
        # a second UPDATE afterwards -- would rewrite every row in the table on
        # every batch instead of the ones this merge touched.
        merged: dict[str, str] = {}
        for state in states.values():
            merged.update(statistic_merge_sql(state, target, source))
        assignments += [
            f"{quote_ident(column)} = "
            f"{statistic_derive_sql(model.aggregates[column], self.state_for(model, column), sources=merged)}"
            for column in model.aggregates
            if column in statistic
        ]
        assignments += [
            f"{quote_ident(column)} = {sql}" for column, sql in merged.items()
        ]
        updates = ",\n         ".join(assignments)
        columns = self.output_columns(model)
        insert_columns = ", ".join(quote_ident(c) for c in columns)
        insert_values = ", ".join(f"{source}.{quote_ident(c)}" for c in columns)

        indented = "\n".join("       " + line for line in aggregation.splitlines())
        return (
            f"MERGE INTO {into} AS {target}\n"
            f"USING (\n{indented}\n) AS {source}\n"
            f"   ON {on_clause}\n"
            f" WHEN MATCHED THEN UPDATE SET\n         {updates}\n"
            f" WHEN NOT MATCHED THEN INSERT ({insert_columns})\n"
            f"      VALUES ({insert_values})"
        )

    # -- sealing ------------------------------------------------------------

    def seal_sql(self, model: "Model", cutoff: Any) -> str:
        """Move every sealed window from the accumulator into the target.

        ``cutoff`` is the largest ``window_ts`` that is complete, computed in
        Python by :func:`duckstream.windows.seal_cutoff` and inlined here as a
        single literal. That is not a style choice: ``CONTEXT.md`` 1.5 measured
        a scalar subquery in a DuckLake statement of this shape failing with
        ``Out of buffer`` on the *second* batch, and a literal additionally
        lets DuckLake prune data files on the ``window_ts`` statistics it keeps.

        Columns are named explicitly so the statement does not depend on either
        table's physical column order.
        """
        columns = ", ".join(quote_ident(c) for c in self.output_columns(model))
        return (
            f"INSERT INTO {self.qualified_name} ({columns})\n"
            f"SELECT {columns} FROM {self.qualified_open_windows}\n"
            f" WHERE {quote_ident(WINDOW_COLUMN)} <= {quote_literal(cutoff)}"
        )

    def evict_sql(self, cutoff: Any) -> str:
        """Drop the windows :meth:`seal_sql` has just emitted.

        Runs in the same transaction as the insert, so the two are one snapshot
        and a window can never be both emitted and still open, nor evicted
        without being emitted.

        This is the one ``DELETE`` on duckstream's write path, and it is
        deliberate. ``CONTEXT.md`` 1.10 measured a matching DuckLake ``DELETE``
        at ~26 ms because it writes a tombstone, which is why *per-trigger*
        state is append-only -- but this is not per-trigger state. It fires
        only when a window actually seals, it is what keeps the accumulator
        bounded by the lateness horizon rather than by the age of the stream,
        and ``CONTEXT.md`` 1.3's caveat asks for exactly this ("if state
        reaches millions of open windows, add eviction of sealed windows").
        """
        return (
            f"DELETE FROM {self.qualified_open_windows} "
            f"WHERE {quote_ident(WINDOW_COLUMN)} <= {quote_literal(cutoff)}"
        )

    # -- foldability guard --------------------------------------------------

    def _require_additive(self, model: "Model") -> None:
        """Refuse a strategy this sink cannot maintain by merging.

        ``CONTEXT.md`` section 4 is why this is an exception and not a
        best-effort fold. A mart that folds an average as if it were additive
        does not fail; it returns a plausible wrong number, and nobody notices
        until it is compared against a full recompute. Silence is the failure
        mode being designed out.

        Two strategies merge. ``delta_merge`` folds each aggregate into the
        stored value. ``sufficient_statistics`` folds a mergeable *state* --
        ``(n, mean, M2)`` per statistic argument -- and derives the answer from
        it, which is exact and still needs no rescan. ``recompute_window`` is
        neither: there is no decomposition, the window has to be read again from
        source, and no merge can stand in for that.
        """
        strategy = model.resolved_strategy
        if strategy in ("delta_merge", "sufficient_statistics"):
            return
        tier = model.tier
        raise DuckstreamError(
            f"TableSink(mode='update') cannot maintain model {model.name!r}: it "
            f"classifies as tier {str(tier)!r} and resolves to strategy "
            f"{strategy!r}, which cannot be expressed as a merge at all. An "
            f"aggregate at tier {str(tier)!r} has no decomposition to fold — the "
            f"affected windows must be recomputed from source — and folding one "
            f"anyway does not fail, it stores a plausible wrong number "
            f"(CONTEXT.md section 4 records a mart that held 3.0 where the truth "
            f"was 2.0). The {strategy!r} strategy is not built yet; until it is, "
            f"drop `grain` and use mode='append', which never folds, or reduce "
            f"the model to aggregates that decompose."
        )

    # -- catalog introspection ---------------------------------------------

    def existing_column_types(
        self, con: Any, name: str | None = None
    ) -> dict[str, str]:
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
            f"  AND table_name = {quote_literal(name or self.name)} "
            "ORDER BY column_index"
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def existing_columns(self, con: Any, name: str | None = None) -> list[str]:
        """Column names of the target as the catalog currently sees it."""
        return list(self.existing_column_types(con, name))

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
        self,
        con: Any,
        batch_view: str,
        model: "Model",
        existing: dict[str, str],
        *,
        table: str | None = None,
        folding: bool | None = None,
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
        table = table or self.table
        if folding is None:
            folding = self.mode == "update"
        self._validate_existing(model, list(existing), table=table)
        if not folding:
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
                f"table {table!r} cannot receive model {model.name!r}: "
                f"column {column!r} is {target_type} in the table but the model "
                f"produces {incoming_type}{origin}. DuckDB has no implicit cast "
                f"in either direction between {incoming_type} and "
                f"{target_type}, so the merge cannot even be built. Either the "
                f"table predates this model or the model changed: drop or alter "
                f"{table!r}, or change the aggregate so its type matches. "
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
        if self.mode == "update" or self.windowed_append(model):
            self._require_additive(model)
        self.output_columns(model)
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(self.schema)}")
        existing = self.existing_columns(con)
        if not existing:
            return
        self._validate_existing(model, existing)

    def _validate_existing(
        self, model: "Model", existing: list[str], *, table: str | None = None
    ) -> None:
        table = table or self.table
        present = set(existing)
        required = self.output_columns(model)
        missing = [column for column in required if column not in present]
        if not missing:
            return
        raise DuckstreamError(
            f"table {table!r} already exists but does not match model "
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
    ) -> int | None:
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

        Returns the number of rows written, which DuckDB hands back from the
        ``INSERT`` or ``MERGE`` itself -- so it costs nothing. Phase 1 left
        ``rows_out`` NULL on the belief that obtaining it meant running the
        aggregation a second time; measured on 1.5.5, ``con.execute`` on an
        ``INSERT``, a ``MERGE`` and a ``DELETE`` each return a one-row, one-
        column result carrying the affected count, and this method was already
        throwing it away.

        For sealed ``append`` the count reported is the number of rows that
        reached the **target** -- windows that actually sealed -- not the number
        folded into the accumulator. That is the honest reading of "rows out":
        an open window has not been output yet.

        ``ctx.watermark`` is what the sealed-append path uses; ``ctx`` is
        otherwise accepted for the protocol and unused, because idempotency
        comes from the merge key rather than from the batch id.
        """
        if self.windowed_append(model):
            return self._write_sealed(con, batch_view, model, ctx)
        if self.mode == "update":
            self._require_additive(model)
        self._prepare_table(con, batch_view, model)
        statement = (
            self.insert_sql(batch_view, model)
            if self.mode == "append"
            else self.merge_sql(batch_view, model)
        )
        return _affected(con.execute(statement))

    def _write_sealed(
        self, con: Any, batch_view: str, model: "Model", ctx: "BatchContext"
    ) -> int | None:
        """``append`` over windows: fold while open, emit once when sealed.

        Three statements, all inside the engine's one transaction, so they are
        one DuckLake snapshot and the intermediate states are never observable:

        1. **fold** the batch into the accumulator, with exactly the ``MERGE``
           ``update`` mode uses on the target — same key match, same
           ``IS NOT DISTINCT FROM``, same additive fold expressions. A window
           accumulates there for as long as it is open, however many batches
           touch it;
        2. **seal**: insert every window the watermark has passed into the real
           target;
        3. **evict** those windows from the accumulator.

        This is what makes ``append`` mean what it says. Each window reaches the
        target exactly once, complete, and is never updated afterwards — so the
        target is genuinely append-only and a downstream reader can treat a row
        it has seen as final. Phase 1's append over a windowed model wrote a
        *partial* row per window per batch, which was equal to the truth only
        when no two batches shared a window; ``Model.validate`` now refuses that
        shape rather than letting it be silently wrong.

        A model reaching here has a lateness horizon (``Model.validate``
        guarantees it), so ``ctx.watermark`` is the watermark this batch is
        about to commit. It may still be ``None`` on the very first batch of a
        stream whose every row was undated, in which case nothing seals and the
        target is created empty — an empty mart being visibly empty is worth
        more than a mart that does not exist.
        """
        self._require_additive(model)
        self._prepare_table(
            con,
            batch_view,
            model,
            table=f"{self.table}{_OPEN_SUFFIX}",
            name=self.open_windows_name,
            into=self.qualified_open_windows,
            folding=True,
        )
        con.execute(self.merge_sql(batch_view, model, into=self.qualified_open_windows))

        # The target takes its shape from the accumulator rather than from the
        # aggregation, so the two can never disagree about a column type: the
        # rows about to be inserted come from the accumulator, not the batch.
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {self.qualified_name} AS "
            f"SELECT * FROM {self.qualified_open_windows} WHERE false"
        )
        self._validate_existing(model, self.existing_columns(con))

        cutoff = seal_cutoff(ctx.watermark, model.grain)
        if cutoff is None:
            return 0
        sealed = _affected(con.execute(self.seal_sql(model, cutoff)))
        con.execute(self.evict_sql(cutoff))
        return sealed

    def _prepare_table(
        self,
        con: Any,
        batch_view: str,
        model: "Model",
        *,
        table: str | None = None,
        name: str | None = None,
        into: str | None = None,
        folding: bool | None = None,
    ) -> None:
        """Create a destination if absent, otherwise check the batch fits it.

        Serves both the target and, for sealed ``append``, the open-window
        accumulator. ``folding`` says whether rows will be *merged* into it,
        which is what decides whether column types are checked as well as
        column names — see :meth:`_check_target_matches`.
        """
        existing = self.existing_column_types(con, name)
        if existing:
            self._check_target_matches(
                con, batch_view, model, existing, table=table, folding=folding
            )
            return
        # write() may be the first thing that ever touches this sink — the
        # engine calls ensure(), but a library user driving the sink directly
        # need not have. CREATE SCHEMA IF NOT EXISTS is idempotent and cheap.
        # Nothing to type-check on this path: the table is about to be created
        # from this very aggregation, so its types are the incoming ones.
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(self.schema)}")
        con.execute(self.create_table_sql(batch_view, model, into=into))

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
