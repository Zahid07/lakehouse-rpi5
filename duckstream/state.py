"""Durable offsets, watermarks and batch history — the exactly-once boundary.

The whole guarantee in ``PLAN.md`` reduces to one sentence: the sink rows, the
state and the source offset all land in **one** transaction. ``CONTEXT.md`` 1.4
measured that on DuckLake such a transaction becomes exactly one snapshot, so a
crash before ``COMMIT`` replays from the stored offset and a crash after it is
durable. There is no third outcome, and that is the entire mechanism.

:meth:`DuckLakeStateStore.commit` is where that boundary lives. Everything else
in this module exists to keep it honest:

* ``begin`` refuses to nest rather than silently joining someone else's
  transaction, because a nested ``BEGIN`` would hand back a commit that is not
  actually a commit.
* ``commit`` leaves the transaction rolled back if anything inside it fails.
  Half-open is the one state the engine cannot recover from.
* Offsets are stored as JSON **text**. The coupling to ``duckstream.offsets`` is
  exactly that and nothing more, which is why this module does not import it.

Two SQL rules are obeyed throughout, both from ``CONTEXT.md``:

* **No scalar subquery in a MERGE or JOIN condition** (1.5). Against DuckLake
  that raises ``Out of buffer``, and only on the *second* write — the first to
  take the ``WHEN MATCHED`` branch — so it survives any single-batch test. Every
  value here is computed in Python and bound as a parameter.
* **Offsets, watermarks and batch records are append-only.** Never updated,
  never deleted; the current value is the newest row, read with ``ORDER BY
  batch_id DESC LIMIT 1``. ``CONTEXT.md`` 1.10 measured why: a DuckLake
  ``DELETE`` that matches a row writes a tombstone file and costs ~26 ms, an
  ``UPDATE`` ~30 ms, an ``INSERT`` ~8 ms — so one mutable row per model made a
  trigger cost 106 ms against the 17 ms floor of 1.8. Append-only is also
  strictly safer for crash recovery: an uncommitted append is simply invisible,
  so there is no partially-overwritten value to reason about. The cost is
  unbounded growth, which :meth:`_StateStoreBase.prune` bounds and phase-4
  maintenance will schedule.

Timestamps are stored naive, in UTC. ``TIMESTAMP WITH TIME ZONE`` values need
``pytz`` to reach Python, which is not a duckstream dependency.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from duckstream.errors import DuckstreamError
from duckstream.lake import _quote_identifier

__all__ = [
    "DEFAULT_STATE_SCHEMA",
    "DuckLakeStateStore",
    "MemoryStateStore",
    "decode_offset",
    "encode_offset",
]


#: Schema the state tables live in.
DEFAULT_STATE_SCHEMA = "duckstream"

_NESTED_TXN_MARKERS = ("within a transaction", "already a transaction")
_NO_TXN_MARKERS = ("no transaction is active",)


def _utcnow() -> datetime:
    """Naive UTC now — see the module note on ``TIMESTAMP WITH TIME ZONE``."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _is_nested_transaction_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in _NESTED_TXN_MARKERS)


def _is_no_transaction_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in _NO_TXN_MARKERS)


def encode_offset(offset: Any) -> str:
    """Render an offset as the JSON text that goes into the state table.

    A ``str`` is taken to be already-encoded JSON and is stored verbatim, which
    is how the offset codec in ``duckstream.offsets`` stays the only place that
    decides an offset's shape. Anything else is encoded here with
    ``sort_keys=True`` so that byte-comparing two stored offsets is meaningful.
    """
    if offset is None:
        raise DuckstreamError(
            "refusing to commit a null offset. An offset is the position a "
            "restart replays from; committing None would make the next run "
            "start over from the beginning and duplicate rows."
        )
    if isinstance(offset, str):
        try:
            json.loads(offset)
        except (TypeError, ValueError) as exc:
            raise DuckstreamError(
                f"offset was given as a string but is not valid JSON: {exc}. "
                f"Pass a dict, or already-encoded JSON text."
            ) from exc
        return offset
    try:
        return json.dumps(offset, sort_keys=True, ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise DuckstreamError(
            f"offset is not JSON-serialisable: {exc}. Offsets are committed as "
            f"JSON text alongside the data, so every value in one must survive "
            f"json.dumps/json.loads unchanged."
        ) from exc


def decode_offset(text: str | None) -> Any:
    """Inverse of :func:`encode_offset`. ``None`` in, ``None`` out."""
    if text is None:
        return None
    try:
        return json.loads(text)
    except (TypeError, ValueError) as exc:
        raise DuckstreamError(
            f"stored offset is not valid JSON: {exc}. The state table has been "
            f"written by something other than duckstream, or is corrupt."
        ) from exc


def _coerce_timestamp(value: Any, *, what: str) -> datetime | None:
    """Accept ``None``, ``datetime``, ``date`` or ISO text; return naive UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, str):
        try:
            return _coerce_timestamp(datetime.fromisoformat(value), what=what)
        except ValueError as exc:
            raise DuckstreamError(
                f"{what} {value!r} is not an ISO-8601 timestamp: {exc}"
            ) from exc
    raise DuckstreamError(
        f"{what} must be a datetime, a date, ISO-8601 text or None, got "
        f"{type(value).__name__}"
    )


class _StateStoreBase:
    """Shared implementation of the ``StateStore`` protocol.

    Concrete subclasses differ only in what they document and what they are
    pointed at; the SQL is identical, which is the point — a divergence between
    the fast in-memory path and the real DuckLake one is exactly the kind of
    thing ``CONTEXT.md`` 1.5 warns about.
    """

    #: Set by subclasses; used only in error messages.
    backend: str = "duckdb"

    def __init__(
        self,
        schema: str = DEFAULT_STATE_SCHEMA,
        *,
        catalog: str | None = None,
    ) -> None:
        self.schema = schema
        self.catalog = catalog
        self._schema_sql = _quote_identifier(schema, what="state schema")
        self._catalog_sql = (
            None if catalog is None else _quote_identifier(catalog, what="catalog alias")
        )
        # Batches opened by ``record_batch_start`` and not yet written.
        # ``record_batch_start`` deliberately writes nothing: the batch row is a
        # single append at the end, carrying both timestamps, because two
        # statements against ``batches`` cost ~31 ms of tombstone (CONTEXT.md
        # 1.10) for what is only bookkeeping. ``commit`` also reads the batch id
        # from here so the committed offset records which batch produced it.
        self._open_batches: dict[str, dict[str, Any]] = {}
        # Highest batch id this store has committed, per model. Avoids a
        # ``max(batch_id)`` scan of the offsets table inside every trigger's
        # transaction, which measured ~11 ms — see :meth:`_resolve_batch_id`.
        self._last_batch_id: dict[str, int] = {}

    # -- naming ---------------------------------------------------------

    def _qualified(self, table: str) -> str:
        parts = [] if self._catalog_sql is None else [self._catalog_sql]
        parts.append(self._schema_sql)
        parts.append(_quote_identifier(table, what="state table"))
        return ".".join(parts)

    @property
    def offsets_table(self) -> str:
        """Fully qualified name of the offsets table."""
        return self._qualified("offsets")

    @property
    def watermarks_table(self) -> str:
        """Fully qualified name of the watermarks table."""
        return self._qualified("watermarks")

    @property
    def batches_table(self) -> str:
        """Fully qualified name of the batch-history table."""
        return self._qualified("batches")

    def _schema_qualified(self) -> str:
        if self._catalog_sql is None:
            return self._schema_sql
        return f"{self._catalog_sql}.{self._schema_sql}"

    # -- DDL ------------------------------------------------------------

    def _ddl(self) -> list[str]:
        return [
            f"CREATE SCHEMA IF NOT EXISTS {self._schema_qualified()}",
            f"""CREATE TABLE IF NOT EXISTS {self.offsets_table} (
                    model_name VARCHAR,
                    offset_json VARCHAR,
                    batch_id BIGINT,
                    updated_at TIMESTAMP
                )""",
            f"""CREATE TABLE IF NOT EXISTS {self.watermarks_table} (
                    model_name VARCHAR,
                    watermark TIMESTAMP,
                    batch_id BIGINT,
                    updated_at TIMESTAMP
                )""",
            f"""CREATE TABLE IF NOT EXISTS {self.batches_table} (
                    model_name VARCHAR,
                    batch_id BIGINT,
                    started_at TIMESTAMP,
                    committed_at TIMESTAMP,
                    rows_in BIGINT,
                    rows_out BIGINT
                )""",
        ]

    def ensure(self, con) -> None:
        """Create the state schema and tables. Idempotent, safe every run.

        Wrapped in its own transaction when one is not already open, so first
        setup costs a single DuckLake snapshot rather than one per statement,
        and a second call costs none at all.
        """
        owns_transaction = self._begin_if_possible(con)
        try:
            for statement in self._ddl():
                con.execute(statement)
            if owns_transaction:
                con.execute("COMMIT")
        except BaseException:
            if owns_transaction:
                self._rollback_quietly(con)
            raise

    # -- transaction control --------------------------------------------

    def begin(self, con) -> None:
        """Open the trigger's transaction.

        Refuses to nest. A nested ``BEGIN`` in DuckDB is not a savepoint — the
        inner ``COMMIT`` would not make anything durable, so the engine would
        believe it had checkpointed when it had not.
        """
        try:
            con.execute("BEGIN TRANSACTION")
        except Exception as exc:
            if _is_nested_transaction_error(exc):
                raise DuckstreamError(
                    "a transaction is already open on this connection. "
                    "duckstream runs exactly one transaction per trigger — the "
                    "sink rows, the state and the offset must commit together "
                    "or not at all — so nesting is refused rather than "
                    "silently joining the outer transaction. Commit or roll "
                    "back the open transaction first."
                ) from exc
            raise

    def rollback(self, con) -> None:
        """Abandon the trigger's transaction. Nothing written becomes durable.

        With append-only state there is nothing half-written to repair: the rows
        this transaction appended simply never become visible.
        """
        self._open_batches.clear()
        con.execute("ROLLBACK")

    def _begin_if_possible(self, con) -> bool:
        """``BEGIN`` unless one is already open; True if we now own it."""
        try:
            con.execute("BEGIN TRANSACTION")
        except Exception as exc:
            if _is_nested_transaction_error(exc):
                return False
            raise
        return True

    @staticmethod
    def _rollback_quietly(con) -> None:
        try:
            con.execute("ROLLBACK")
        except Exception:
            # Already rolled back by the failure itself; nothing to undo.
            pass

    # -- reads ------------------------------------------------------------

    def load_offset(self, con, model_name: str) -> Any | None:
        """The committed offset for ``model_name``, or ``None``.

        ``None`` is not an error and not an empty offset: it means this model
        has never committed, and it is what makes a first run replay from the
        beginning of the source.

        The table is append-only, so "the committed offset" is the newest row.
        ``batch_id`` is the ordering key rather than ``updated_at``: it is
        assigned once per model per commit and strictly increases, whereas two
        timestamps could in principle tie.
        """
        row = con.execute(
            f"SELECT offset_json FROM {self.offsets_table} WHERE model_name = ? "
            f"ORDER BY batch_id DESC LIMIT 1",
            [model_name],
        ).fetchone()
        if row is None:
            return None
        return decode_offset(row[0])

    def load_watermark(self, con, model_name: str) -> datetime | None:
        """The committed watermark for ``model_name``, or ``None``.

        Newest row wins, exactly as in :meth:`load_offset`.
        """
        row = con.execute(
            f"SELECT watermark FROM {self.watermarks_table} WHERE model_name = ? "
            f"ORDER BY batch_id DESC LIMIT 1",
            [model_name],
        ).fetchone()
        return None if row is None else row[0]

    def next_batch_id(self, con, model_name: str) -> int:
        """One past the highest recorded batch id for ``model_name``; 1-based.

        Computed in Python from a scalar read rather than inlined as a subquery,
        for the reason in the module docstring.
        """
        row = con.execute(
            f"SELECT max(batch_id) FROM {self.batches_table} WHERE model_name = ?",
            [model_name],
        ).fetchone()
        current = row[0] if row and row[0] is not None else 0
        return int(current) + 1

    # -- writes -----------------------------------------------------------

    def _resolve_batch_id(self, con, model_name: str) -> int:
        """The batch id this commit's rows carry, and the append ordering key.

        Taken from an open :meth:`record_batch_start` when the engine used one,
        then from what this store last committed, and only failing both from a
        ``max(batch_id)`` scan of the offsets table. The scan is the expensive
        branch: measured on this box, doing it inside the trigger's transaction
        costs ~11 ms of the trigger, so it runs once per model per process and
        the in-memory value serves every trigger after that.

        Caching is sound because v1 is single-writer under ``AvailableNow``
        (``CONTEXT.md`` 2.5), so nothing else advances a model's batch id while
        this store is running. It is populated only from a *successful* commit,
        so a rolled-back batch leaves it where it was.
        """
        open_batch = self._open_batches.get(model_name)
        if open_batch is not None:
            return int(open_batch["batch_id"])
        cached = self._last_batch_id.get(model_name)
        if cached is not None:
            return cached + 1
        row = con.execute(
            f"SELECT max(batch_id) FROM {self.offsets_table} WHERE model_name = ?",
            [model_name],
        ).fetchone()
        current = row[0] if row and row[0] is not None else 0
        return int(current) + 1

    def _append_offset(
        self, con, model_name: str, offset: Any, batch_id: int, now: datetime
    ) -> None:
        payload = encode_offset(offset)
        con.execute(
            f"INSERT INTO {self.offsets_table} "
            f"(model_name, offset_json, batch_id, updated_at) VALUES (?, ?, ?, ?)",
            [model_name, payload, int(batch_id), now],
        )

    def _append_watermark(
        self, con, model_name: str, watermark: Any, batch_id: int, now: datetime
    ) -> None:
        value = _coerce_timestamp(watermark, what=f"watermark for {model_name!r}")
        con.execute(
            f"INSERT INTO {self.watermarks_table} "
            f"(model_name, watermark, batch_id, updated_at) VALUES (?, ?, ?, ?)",
            [model_name, value, int(batch_id), now],
        )

    def commit(
        self,
        con,
        offsets: dict[str, Any],
        watermarks: dict[str, Any],
    ) -> None:
        """Append every offset and watermark, then ``COMMIT``.

        **The exactly-once boundary.** Called with the trigger's transaction
        already open and the sink rows already written into it, so that the
        output, the state and the offset become durable as one DuckLake
        snapshot (``CONTEXT.md`` 1.4).

        Every write here is an append. Nothing is updated and nothing is
        deleted, so a crash anywhere before ``COMMIT`` leaves the previous
        state untouched rather than half-overwritten, and the rows this
        transaction added never become visible at all.

        On any failure the transaction is rolled back before the exception
        propagates. Leaving it half-open would strand the connection and the
        engine's next ``begin`` would refuse to nest.

        With nothing to persist this writes nothing and opens nothing.
        ``CONTEXT.md`` 1.8 measured a DuckLake transaction that writes nothing
        at ~1.3 ms against ~16.8 ms the moment it writes one state row, so an
        idle trigger has to stay on the cheap side of that. If a transaction is
        open it is still closed — an empty one produces no snapshot — and if
        none is open this is a complete no-op rather than an error, so the
        engine may skip ``begin`` entirely on an empty batch.
        """
        if not offsets and not watermarks:
            self._open_batches.clear()
            try:
                con.execute("COMMIT")
            except Exception as exc:
                if _is_no_transaction_error(exc):
                    return
                self._rollback_quietly(con)
                raise
            return
        try:
            now = _utcnow()
            # One batch id per model, shared by its offset and its watermark, so
            # the two tables order identically on replay.
            batch_ids = {
                model_name: self._resolve_batch_id(con, model_name)
                for model_name in {*(offsets or {}), *(watermarks or {})}
            }
            for model_name, offset in (offsets or {}).items():
                self._append_offset(
                    con, model_name, offset, batch_ids[model_name], now
                )
            for model_name, watermark in (watermarks or {}).items():
                self._append_watermark(
                    con, model_name, watermark, batch_ids[model_name], now
                )
            con.execute("COMMIT")
            self._last_batch_id.update(batch_ids)
        except BaseException:
            self._rollback_quietly(con)
            raise
        finally:
            self._open_batches.clear()

    # -- batch history ----------------------------------------------------

    def record_batch_start(
        self,
        con,
        model_name: str,
        batch_id: int,
        *,
        started_at: Any | None = None,
    ) -> None:
        """Open a batch. **Writes nothing to the database.**

        The start time and batch id are held in memory until
        :meth:`record_batch_end` appends the finished row carrying both
        timestamps. ``CONTEXT.md`` 1.10 is the reason: writing the row here and
        completing it later meant an ``INSERT`` plus an ``UPDATE`` against
        ``batches``, ~31 ms of the 106 ms a trigger used to cost, for what is
        only bookkeeping. One append costs ~8 ms and says the same thing.

        ``con`` is accepted and unused so the call site reads like the other
        state operations and so a future implementation may need it.

        A batch that is opened and never ended leaves no row, which is the same
        outcome as before — an unfinished batch was rolled back with its
        transaction anyway.
        """
        del con  # held in memory until record_batch_end; see the docstring
        self._open_batches[model_name] = {
            "batch_id": int(batch_id),
            "started_at": _coerce_timestamp(started_at, what="started_at")
            or _utcnow(),
        }

    def record_batch_end(
        self,
        con,
        model_name: str,
        batch_id: int,
        *,
        rows_in: int | None = None,
        rows_out: int | None = None,
        committed_at: Any | None = None,
    ) -> None:
        """Append the finished batch row: one insert, both timestamps.

        Call it **inside** the trigger's transaction, immediately before
        :meth:`commit`, so the row lands in the same snapshot as the data and
        the one-snapshot-per-trigger accounting holds. ``committed_at`` is then
        the moment the commit was issued, which is the only timestamp available
        from inside the transaction that is about to become it.

        ``started_at`` comes from the matching :meth:`record_batch_start`; with
        no matching start it is left NULL rather than invented, so history never
        silently loses a batch that ran.
        """
        finished = _coerce_timestamp(committed_at, what="committed_at") or _utcnow()
        open_batch = self._open_batches.get(model_name)
        started = None
        if open_batch is not None and open_batch["batch_id"] == int(batch_id):
            started = open_batch["started_at"]
        con.execute(
            f"INSERT INTO {self.batches_table} "
            f"(model_name, batch_id, started_at, committed_at, rows_in, rows_out) "
            f"VALUES (?, ?, ?, ?, ?, ?)",
            [
                model_name,
                int(batch_id),
                started,
                finished,
                None if rows_in is None else int(rows_in),
                None if rows_out is None else int(rows_out),
            ],
        )

    def batch_history(self, con, model_name: str) -> list[dict[str, Any]]:
        """Recorded batches for ``model_name``, oldest first."""
        cursor = con.execute(
            f"SELECT model_name, batch_id, started_at, committed_at, rows_in, "
            f"rows_out FROM {self.batches_table} WHERE model_name = ? "
            f"ORDER BY batch_id",
            [model_name],
        )
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    # -- maintenance --------------------------------------------------------

    def prune(
        self,
        con,
        model_name: str | None = None,
        *,
        keep_last: int = 1,
    ) -> dict[str, int]:
        """Drop all but the newest ``keep_last`` rows per model, per table.

        Append-only state grows by three rows per trigger and nothing here
        reclaims that on its own, so this is the tool that bounds it. Phase-4
        maintenance is expected to schedule it alongside
        ``ducklake_expire_snapshots``; nothing calls it automatically yet.

        Pruning is the one place in this module that deletes, and a matching
        DuckLake ``DELETE`` costs ~26 ms (``CONTEXT.md`` 1.10) — which is fine
        for maintenance and is exactly why it is not on the trigger path. The
        cutoff is read into Python and bound as a literal rather than inlined as
        a subquery, per ``CONTEXT.md`` 1.5.

        Returns rows deleted per table. Runs in its own transaction, so it is
        one snapshot, unless the caller already opened one.
        """
        if keep_last < 1:
            raise DuckstreamError(
                f"keep_last must be at least 1, got {keep_last}. Pruning every "
                f"row would discard the committed offset and make the next run "
                f"replay the source from the beginning."
            )
        models = (
            [model_name] if model_name is not None else self._known_models(con)
        )
        deleted = {"offsets": 0, "watermarks": 0, "batches": 0}
        owns_transaction = self._begin_if_possible(con)
        try:
            for table_key, table in (
                ("offsets", self.offsets_table),
                ("watermarks", self.watermarks_table),
                ("batches", self.batches_table),
            ):
                for name in models:
                    deleted[table_key] += self._prune_table(
                        con, table, name, keep_last
                    )
            if owns_transaction:
                con.execute("COMMIT")
        except BaseException:
            if owns_transaction:
                self._rollback_quietly(con)
            raise
        return deleted

    def _known_models(self, con) -> list[str]:
        rows = con.execute(
            f"SELECT DISTINCT model_name FROM {self.offsets_table} "
            f"UNION SELECT DISTINCT model_name FROM {self.watermarks_table} "
            f"UNION SELECT DISTINCT model_name FROM {self.batches_table}"
        ).fetchall()
        return sorted(row[0] for row in rows if row[0] is not None)

    def _prune_table(self, con, table: str, model_name: str, keep_last: int) -> int:
        cutoff = con.execute(
            f"SELECT batch_id FROM {table} WHERE model_name = ? "
            f"ORDER BY batch_id DESC LIMIT 1 OFFSET ?",
            [model_name, keep_last - 1],
        ).fetchone()
        if cutoff is None or cutoff[0] is None:
            return 0
        result = con.execute(
            f"DELETE FROM {table} WHERE model_name = ? AND batch_id < ?",
            [model_name, int(cutoff[0])],
        ).fetchall()
        return int(result[0][0]) if result and result[0] else 0


class DuckLakeStateStore(_StateStoreBase):
    """The state store duckstream actually runs on, and the conformance target.

    Writes ``duckstream.offsets``, ``duckstream.watermarks`` and
    ``duckstream.batches`` as ordinary DuckLake tables in the attached catalog,
    so that :meth:`commit` folds them into the same snapshot as the batch's
    output rows. That is the whole exactly-once mechanism (``PLAN.md``,
    "Exactly-once"; ``CONTEXT.md`` 1.4).

    Expects :func:`duckstream.lake.attach_lake` to have run on the connection,
    which leaves the catalog current, so the default unqualified schema name
    resolves inside it. Pass ``catalog=`` to name the alias explicitly instead.
    """

    backend = "ducklake"


class MemoryStateStore(_StateStoreBase):
    """Plain-DuckDB state store. **For unit-test speed only.**

    Not a supported production backend, and never the sole test gate.
    ``PLAN.md`` is emphatic about this and ``CONTEXT.md`` 1.5 shows why: a
    ``MERGE`` with a scalar subquery in its join condition passes on in-memory
    DuckDB and raises ``Out of buffer`` against DuckLake — and only on the
    second batch, the first to take the ``WHEN MATCHED`` branch. In-memory
    DuckDB demonstrably hides real DuckLake failures, so anything that passes
    here must also be proved against :class:`DuckLakeStateStore`.

    It exists because a DuckLake catalog costs a temporary directory and a
    handful of snapshots per test, and some tests only need the SQL exercised.
    """

    backend = "duckdb"
