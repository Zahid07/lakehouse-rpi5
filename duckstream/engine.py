"""The engine: the trigger loop, the transaction boundary, the batch lifecycle.

Everything else in duckstream describes *what* to compute. This module is the
only place that decides *when* a thing is written and *what it is written with*,
and both decisions were measured rather than reasoned.

The batch lifecycle, and why the order is what it is
----------------------------------------------------

::

    start = state.load_offset(con, model.name)      # where the last run got to
    end   = source.latest_offset()                  # what is there right now
    plan  = source.plan(start, end, model.limits)   # a bounded batch
    if plan.is_empty: return                        # (1) no transaction at all
    view  = source.bind(con, plan)                  # (2) DDL against `temp`
    state.begin(con)                                # ---- transaction opens
    sink.write(con, view, model, ctx)               #      output rows
    state.record_batch_end(con, ...)                #      batch history
    state.commit(con, {model.name: plan.end}, {})   # ---- one DuckLake snapshot

**(1) An empty batch opens nothing.** ``CONTEXT.md`` 1.8: a DuckLake transaction
that writes nothing costs ~1.28 ms, one that writes anything pays ~16.8 ms of
commit. So an idle pass is not merely cheap, it adds **zero snapshots** — a
property a test can assert, and one that keeps the snapshot history a record of
work done rather than of cron ticks that happened.

**(2) The view is bound outside the transaction.** ``CREATE TEMP VIEW`` is DDL
against the ``temp`` database, and ``CONTEXT.md`` 1.9 measured that one DuckDB
transaction cannot write to two attached databases: doing it inside would raise
``TransactionContext Error``. The same constraint is why the sink and the state
store must both live in the DuckLake catalog — which is what makes the single
commit below possible at all.

Event time sits inside step (2) and step (3). A model that declares a
``lateness`` horizon has its bound batch scanned once for its counts and its
newest event time, rows whose window already sealed are filtered out through a
second temp view — created only when there is actually something to drop — and
the new watermark is committed in the same transaction as the offset. A model
that declares no horizon skips all of it and behaves exactly as it did in phase
1: no watermark is read, none is written, and no row is filtered.

**The commit is the entire exactly-once guarantee.** Output rows, batch history
and the source offset become durable together, as one DuckLake snapshot
(``CONTEXT.md`` 1.4). A crash before it replays from the stored offset because
nothing was written; a crash after it is durable because everything was. There
is no third outcome, and nothing else in duckstream provides idempotency — the
sink explicitly does not (``duckstream.sinks.table``, "Idempotency").

Anything raising between ``begin`` and ``commit`` rolls back and then
propagates. A half-open transaction is the one state the next trigger cannot
recover from, since :meth:`~duckstream.state._StateStoreBase.begin` refuses to
nest rather than silently joining it.

Fault injection
---------------

The exactly-once claim is only worth what its fault-injection test proves, so
the hooks that test needs are a designed part of the engine rather than
something a test reaches in by monkeypatching privates. See :class:`FaultHooks`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, Mapping

from duckstream.errors import BatchFailed, DuckstreamError
from duckstream.lake import DEFAULT_ALIAS, attach_lake
from duckstream.lock import RunLock
from duckstream.model import Model
from duckstream.protocols import BatchContext, BatchPlan, Offset
from duckstream.sql import quote_ident
from duckstream.state import DEFAULT_STATE_SCHEMA, DuckLakeStateStore, Position
from duckstream.trigger import AvailableNow, Trigger
from duckstream.watermark import WatermarkPolicy, policy_for

if TYPE_CHECKING:  # pragma: no cover - typing only
    from duckstream.config import ConfigDocument

__all__ = [
    "Engine",
    "RunLock",
    "BatchResult",
    "RunReport",
    "FaultEvent",
    "FaultHooks",
    "FAULT_POINTS",
]


#: Every moment a fault hook can fire, in the order a non-empty batch reaches
#: them. Named rather than positional so a test says what it means, and closed
#: so a typo raises at install time instead of never firing.
FAULT_POINTS: tuple[str, ...] = (
    "after_plan",
    "after_bind",
    "after_sink_write",
    "before_commit",
    "after_commit",
)


# --------------------------------------------------------------------------
# Fault injection
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class FaultEvent:
    """What a fault hook is told about the moment it fired.

    One argument rather than several, so a hook written today keeps working
    when a later phase has more to say. A hook that only means to kill the
    process ignores it entirely.
    """

    point: str
    """One of :data:`FAULT_POINTS`."""

    engine: "Engine"
    model: Model
    con: Any
    plan: BatchPlan
    ctx: BatchContext
    view: str | None = None
    """The bound batch view; ``None`` before :meth:`Engine._bind` has run."""


FaultHook = Callable[[FaultEvent], None]


class FaultHooks:
    """Named, explicit interception points on one :class:`Engine`.

    The headline claim in ``PLAN.md`` is that a process killed between the sink
    write and the commit loses nothing and duplicates nothing. Proving that
    needs a hard kill — ``os._exit`` — at an exact moment inside the batch
    lifecycle, so the interception point has to be a designed feature. A test
    that reached in by monkeypatching a private method would be testing the
    engine's current shape rather than its guarantee.

    Usage, which is what the conformance suite does::

        import os

        engine.faults.install("after_sink_write", lambda event: os._exit(9))
        engine.run()          # never returns; the transaction was never committed

    ``engine.faults["after_sink_write"] = hook`` is the same thing, since a dict
    of callables is the obvious mental model.

    **Safe by default, and impossible to arm accidentally in production.** The
    only way a hook is ever installed is an explicit call on a live
    :class:`Engine` object. There is no config key, no environment variable and
    no CLI flag that reaches this — ``duckstream run --config models.yaml``, the
    cron entry point, cannot arm one however the YAML is written. And an unknown
    point name raises immediately rather than being stored and never fired,
    because a fault test that silently never faults passes for the wrong reason.

    Hooks are called with a single :class:`FaultEvent` and their return value is
    ignored. Whatever a hook raises propagates through the engine's ordinary
    error handling, so raising at ``after_sink_write`` rolls the transaction
    back exactly as a sink failure would — the in-process shadow of the real
    process kill.
    """

    __slots__ = ("_hooks",)

    #: Mirrors :data:`FAULT_POINTS` for callers holding only the hooks object.
    POINTS = FAULT_POINTS

    def __init__(self) -> None:
        self._hooks: dict[str, FaultHook] = {}

    # -- installation ---------------------------------------------------

    def install(self, point: str, hook: FaultHook) -> None:
        """Arm ``hook`` at ``point``. Replaces any hook already there."""
        if point not in FAULT_POINTS:
            raise DuckstreamError(
                f"unknown fault point {point!r}; expected one of "
                f"{', '.join(repr(p) for p in FAULT_POINTS)}. A misspelled point "
                f"is refused rather than stored, because a fault-injection test "
                f"whose hook never fires passes for the wrong reason."
            )
        if not callable(hook):
            raise DuckstreamError(
                f"fault hook for {point!r} must be callable, got "
                f"{type(hook).__name__}"
            )
        self._hooks[point] = hook

    def remove(self, point: str) -> None:
        """Disarm ``point``. Doing so when nothing is armed is not an error."""
        self._hooks.pop(point, None)

    def clear(self) -> None:
        """Disarm everything."""
        self._hooks.clear()

    def installed(self) -> list[str]:
        """Armed points, in lifecycle order."""
        return [point for point in FAULT_POINTS if point in self._hooks]

    # -- dict-shaped sugar ----------------------------------------------

    def __setitem__(self, point: str, hook: FaultHook) -> None:
        self.install(point, hook)

    def __getitem__(self, point: str) -> FaultHook:
        return self._hooks[point]

    def __delitem__(self, point: str) -> None:
        del self._hooks[point]

    def __contains__(self, point: object) -> bool:
        return point in self._hooks

    def __iter__(self) -> Iterator[str]:
        return iter(self.installed())

    def __len__(self) -> int:
        return len(self._hooks)

    def __bool__(self) -> bool:
        return bool(self._hooks)

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        armed = ", ".join(self.installed()) or "none armed"
        return f"FaultHooks({armed})"

    # -- firing -----------------------------------------------------------

    def fire(self, point: str, event: FaultEvent) -> None:
        """Call the hook at ``point``, if one is armed.

        The empty case is the production case and is a single dict lookup, so
        the mechanism costs nothing when nothing is armed.
        """
        hook = self._hooks.get(point)
        if hook is not None:
            hook(event)


# --------------------------------------------------------------------------
# Results
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class BatchResult:
    """What one pass of the batch lifecycle did.

    An *empty* result — ``is_empty`` — means the pass found nothing to read and
    therefore opened no transaction, wrote no checkpoint and produced no
    DuckLake snapshot. It still appears in the report, because "the engine ran
    and there was nothing to do" is information a cron log wants.
    """

    model: str
    is_empty: bool
    has_more: bool
    batch_id: int | None = None
    rows_in: int | None = None
    start_offset: Offset | None = None
    end_offset: Offset | None = None
    view: str | None = None

    rows_out: int | None = None
    """Output rows the sink wrote, as the sink itself reported them.

    ``None`` when the sink does not report a count. For a sealed ``append``
    model this counts rows that reached the target -- windows that sealed --
    rather than rows folded into the open-window accumulator.
    """

    rows_late: int | None = None
    """Rows dropped because their window had already sealed.

    ``None`` for a model with no lateness horizon, which is not the same as
    ``0``: that model drops nothing because it has no horizon, not because
    nothing arrived late. ``PLAN.md`` requires late data to be "dropped **and
    counted**, never silently absorbed", and this is the count.
    """

    rows_undated: int | None = None
    """Rows dropped because their event-time column was NULL.

    Under event-time semantics a row with no event time belongs to no window,
    so it can never be sealed or emitted. ``None`` when no horizon is declared,
    where a NULL event time still produces a NULL ``window_ts`` as in phase 1.
    """

    watermark: datetime | None = None
    """The watermark this batch committed, or ``None`` if the model has none."""

    outcome: str = "committed"
    """What became of this pass.

    ``committed``
        Ordinary success: output rows written, offset advanced, one snapshot.
    ``empty``
        Nothing to read, so no transaction was opened and no snapshot appeared.
    ``failed``
        The batch raised. The attempt was recorded and it will be retried.
    ``quarantined``
        The attempts ran out under ``on_failure='quarantine'``. The batch was
        skipped and the loss recorded permanently.
    ``halted``
        The attempts ran out under ``on_failure='halt'``. Nothing was skipped
        and nothing further was written; the model retries every tick until the
        underlying problem is fixed.
    ``backoff``
        A previous attempt failed too recently to try again yet.
    """

    attempt: int = 0
    """Failed attempts at this position, including this one if it failed."""

    error: str | None = None
    """Why this pass failed, when it did."""

    @property
    def rows_dropped(self) -> int:
        """Rows this batch read and deliberately did not aggregate."""
        return (self.rows_late or 0) + (self.rows_undated or 0)

    @property
    def committed(self) -> bool:
        """True when this pass wrote output rows and advanced the offset."""
        return self.outcome == "committed"

    @property
    def quarantined(self) -> bool:
        """True when this pass gave up on a batch and skipped past it."""
        return self.outcome == "quarantined"

    @property
    def failed(self) -> bool:
        """True when this pass could not process its batch and did not skip it."""
        return self.outcome in ("failed", "halted")


@dataclass(frozen=True)
class RunReport:
    """Every batch one :meth:`Engine.run` performed, in order."""

    results: tuple[BatchResult, ...] = ()

    @property
    def batches(self) -> tuple[BatchResult, ...]:
        """Only the passes that committed something."""
        return tuple(r for r in self.results if r.committed)

    @property
    def model_names(self) -> list[str]:
        """Models this run touched, in the order they were run."""
        seen: list[str] = []
        for result in self.results:
            if result.model not in seen:
                seen.append(result.model)
        return seen

    @property
    def rows_in(self) -> int:
        """Total source rows read across committed batches."""
        return sum(r.rows_in or 0 for r in self.results)

    @property
    def rows_out(self) -> int:
        """Total output rows written across committed batches."""
        return sum(r.rows_out or 0 for r in self.results)

    @property
    def quarantined(self) -> tuple["BatchResult", ...]:
        """Passes that gave up on a batch and skipped past it.

        Non-empty means this run lost data on purpose, on the model's declared
        policy. The CLI exits non-zero when it is, because losing data should
        page somebody exactly once.
        """
        return tuple(r for r in self.results if r.quarantined)

    @property
    def failures(self) -> tuple["BatchResult", ...]:
        """Passes that raised and are due to be retried."""
        return tuple(r for r in self.results if r.failed)

    @property
    def rows_late(self) -> int:
        """Total rows dropped across this run because their window had sealed."""
        return sum(r.rows_late or 0 for r in self.results)

    @property
    def rows_undated(self) -> int:
        """Total rows dropped across this run for having no event time."""
        return sum(r.rows_undated or 0 for r in self.results)

    @property
    def rows_dropped(self) -> int:
        """Every row this run read and deliberately did not aggregate."""
        return self.rows_late + self.rows_undated

    def for_model(self, name: str) -> tuple[BatchResult, ...]:
        """Every result belonging to ``name``."""
        return tuple(r for r in self.results if r.model == name)

    def __iter__(self) -> Iterator[BatchResult]:
        return iter(self.results)

    def __len__(self) -> int:
        return len(self.results)


# --------------------------------------------------------------------------
# The engine
# --------------------------------------------------------------------------


def _fingerprint(offset: Offset | None) -> str:
    """A stable string for an offset, used only to detect a stalled loop.

    Never persisted and never compared against anything the state store wrote,
    so it may be lossy: ``default=repr`` keeps an exotic value from turning a
    liveness check into a crash.
    """
    try:
        return json.dumps(offset, sort_keys=True, default=repr)
    except Exception:  # pragma: no cover - defensive; repr handles almost all
        return repr(offset)


class Engine:
    """Runs declared models against a DuckLake catalog, one transaction a batch.

    The Python front door from ``PLAN.md``::

        con = duckdb.connect()
        engine = Engine(con, catalog="ducklake:catalog.ducklake",
                        data_path="lake_data")
        engine.add(Model(...))
        engine.run(trigger=AvailableNow())

    and the config front door, which returns an ordinary engine that Python may
    keep modifying::

        engine = Engine.from_config("models.yaml")
        engine.add(Model(...))            # still just an Engine
        engine.run()

    Args:
        con: An open DuckDB connection. The engine attaches DuckLake to it and
            uses it for everything, but does not own it — a connection passed in
            is never closed by :meth:`close`. ``CONTEXT.md`` 1.6 is why
            connection ownership is explicit: a DuckDB *file* held open by one
            process cannot be opened by another, even read-only.
        catalog: DuckLake catalog path or DSN, with or without a leading
            ``ducklake:``.
        data_path: Where parquet data files go. Used only when the catalog is
            being created; an existing catalog already records it.
        alias: Catalog alias, default ``"lake"``.
        settings: ``SET`` values applied to the connection, e.g.
            ``{"memory_limit": "2GB", "threads": 2}``. Data inlining is disabled
            unconditionally and a settings dict cannot re-enable it
            (``CONTEXT.md`` 1.7).
        state: A :class:`~duckstream.protocols.StateStore`. Defaults to a
            :class:`~duckstream.state.DuckLakeStateStore` in the attached
            catalog, which is the only configuration for which the exactly-once
            guarantee holds: ``CONTEXT.md`` 1.9 measured that a transaction
            cannot span two attached databases, so the sink and the state store
            must be in the same one.
        state_schema: Schema the state tables live in, default ``duckstream``.
    """

    def __init__(
        self,
        con: Any,
        catalog: Any,
        *,
        data_path: Any | None = None,
        alias: str = DEFAULT_ALIAS,
        settings: Mapping[str, Any] | None = None,
        state: Any | None = None,
        state_schema: str = DEFAULT_STATE_SCHEMA,
        lock: bool = True,
        _owns_connection: bool = False,
    ) -> None:
        if con is None:
            raise DuckstreamError(
                "Engine needs an open DuckDB connection. duckstream drives an "
                "embedded DuckDB rather than owning one (PLAN.md, 'Running "
                "it'); pass duckdb.connect(), or use Engine.from_config, which "
                "opens one for you."
            )
        self.con = con
        self.catalog = catalog
        self.data_path = data_path
        self.alias = alias
        self.settings: dict[str, Any] = dict(settings or {})
        self._owns_connection = _owns_connection

        #: Guards one catalog against two concurrent runs. See
        #: :mod:`duckstream.lock` -- this exists to turn DuckDB's
        #: "Unique file handle conflict" into a sentence about what actually
        #: happened. Pass ``lock=False`` for a second engine that deliberately
        #: shares a catalog within one process, which is what some tests do.

        #: Fault-injection points. Empty, and only ever filled by an explicit
        #: call — see :class:`FaultHooks`.
        self.faults = FaultHooks()

        #: The document this engine was built from, when it came through the
        #: config front door. ``None`` for a hand-built engine.
        self.document: "ConfigDocument | None" = None

        self._models: dict[str, Model] = {}
        self._prepared = False
        self._prepared_models: set[str] = set()
        self._registered_udfs: set[str] = set()
        self._next_ids: dict[str, int] = {}
        self._policies: dict[str, WatermarkPolicy | None] = {}
        self._watermarks: dict[str, datetime | None] = {}

        attach_lake(
            con,
            catalog,
            data_path=data_path,
            alias=alias,
            settings=self.settings,
        )
        self.state = state if state is not None else DuckLakeStateStore(
            state_schema, catalog=alias
        )
        self.lock = RunLock(catalog, enabled=lock)

    # -- construction from config ------------------------------------------

    @classmethod
    def from_config(
        cls,
        path: Any,
        *,
        con: Any | None = None,
        env: Mapping[str, str] | None = None,
    ) -> "Engine":
        """Build an engine from a YAML document.

        The loader is a deserialiser and nothing more (``PLAN.md``, "Two front
        doors, one canonical model"): it produces the same
        :class:`~duckstream.model.Model` objects the Python API produces, runs
        the same validation, and hands them to the same :meth:`add`. What comes
        back is an ordinary :class:`Engine` — models can still be added, hooks
        installed, settings inspected. There is no config-driven execution path.

        Args:
            path: The YAML file.
            con: A connection to use. When omitted the engine opens an in-memory
                one and closes it in :meth:`close`, which is what the CLI wants:
                the catalog holds the data, the session is disposable.
            env: Environment for ``${VAR}`` substitution; defaults to
                ``os.environ``.
        """
        from duckstream.config import load_config

        return cls.from_document(load_config(path, env=env), con=con)

    @classmethod
    def from_document(
        cls, document: "ConfigDocument", *, con: Any | None = None
    ) -> "Engine":
        """Build an engine from an already-parsed :class:`ConfigDocument`."""
        owns = con is None
        if owns:
            import duckdb  # lazily: importing duckstream stays cheap

            con = duckdb.connect()
        try:
            engine = cls(
                con,
                document.catalog,
                data_path=document.data_path,
                settings=document.settings,
                _owns_connection=owns,
            )
            for model in document.models:
                engine.add(model)
        except BaseException:
            if owns:
                try:
                    con.close()
                except Exception:  # pragma: no cover - best effort
                    pass
            raise
        engine.document = document
        return engine

    # -- model registration -------------------------------------------------

    def add(self, model: Model) -> Model:
        """Validate ``model`` and register it. Returns it, so calls can chain.

        Validation happens here rather than at the first trigger because
        refusing an incorrect model is the framework's reason to exist, and a
        rejection at 03:00 in a cron log is worth much less than one at the
        moment the model is declared. ``Model.validate`` is idempotent, so a
        model that arrived through the config front door — already validated
        there — is simply validated again.
        """
        if not isinstance(model, Model):
            raise DuckstreamError(
                f"Engine.add expects a duckstream Model, got "
                f"{type(model).__name__}"
            )
        model.validate()
        existing = self._models.get(model.name)
        if existing is not None and existing is not model:
            raise DuckstreamError(
                f"a different model called {model.name!r} is already registered "
                f"on this engine. A model's name keys its offsets in the state "
                f"store, so two models sharing one would overwrite each other's "
                f"checkpoints."
            )
        self._models[model.name] = model
        return model

    @property
    def models(self) -> list[Model]:
        """Registered models, in the order they were added."""
        return list(self._models.values())

    def model(self, name: str) -> Model:
        """The registered model called ``name``."""
        try:
            return self._models[name]
        except KeyError:
            known = ", ".join(repr(n) for n in self._models) or "none"
            raise DuckstreamError(
                f"no model named {name!r} on this engine. Registered: {known}."
            ) from None

    # -- running ------------------------------------------------------------

    def run(
        self,
        trigger: Trigger | None = None,
        *,
        model: str | Iterable[str] | None = None,
    ) -> RunReport:
        """Drain the registered models under ``trigger`` and return what ran.

        Args:
            trigger: :class:`~duckstream.trigger.AvailableNow` by default —
                drain what is available, then return, which is what a cron tick
                wants. :class:`~duckstream.trigger.Once` runs a single batch per
                model.
            model: A model name, or several, to restrict the run to. ``None``
                runs every registered model, in registration order.

        Models run sequentially, each in its own transactions. That is not a
        limitation to be lifted later: ``CONTEXT.md`` 2.5 records that DuckLake
        commits are optimistic and that many small concurrent committers exhaust
        their retries, so fewer, fatter, serialised commits are the measured
        right answer on one machine.
        """
        trigger = trigger if trigger is not None else AvailableNow()
        for attribute in ("should_continue",):
            if not callable(getattr(trigger, attribute, None)):
                raise DuckstreamError(
                    f"trigger {type(trigger).__name__!r} does not implement the "
                    f"Trigger protocol: missing {attribute}. Use AvailableNow() "
                    f"or Once()."
                )

        selected = self._select(model)
        if not selected:
            raise DuckstreamError(
                "no models to run. Declare one with Engine.add(Model(...)) or "
                "load a configuration with Engine.from_config(path)."
            )

        with self.lock:
            self._prepare()
            results: list[BatchResult] = []
            for target in selected:
                results.extend(self._drain(target, trigger))
        report = RunReport(tuple(results))

        # Every model got its turn before this fires. A failure that raised
        # where it happened would stop the models after it from running at all,
        # which is the wrong trade: one corrupt file in one model should not
        # stop an unrelated model from draining.
        failures = report.failures
        if failures:
            detail = "; ".join(
                f"{r.model!r} attempt {r.attempt}: {r.error}" for r in failures
            )
            raise BatchFailed(
                f"{len(failures)} batch(es) failed and will be retried: {detail}",
                report=report,
            )
        return report

    def _select(self, model: str | Iterable[str] | None) -> list[Model]:
        if model is None:
            return self.models
        names = [model] if isinstance(model, str) else list(model)
        return [self.model(name) for name in names]

    # -- setup --------------------------------------------------------------

    def _prepare(self) -> None:
        """Create the state tables. Idempotent, and cheap once they exist.

        Runs outside any trigger transaction, on the first :meth:`run` only.
        ``CREATE TABLE IF NOT EXISTS`` against tables that already exist changes
        nothing, and a DuckLake transaction that changes nothing produces no
        snapshot — verified, so a second run over an existing catalog still
        adds zero snapshots when there is no data.
        """
        if self._prepared:
            return
        self.state.ensure(self.con)
        self._prepared = True

    def _prepare_model(self, model: Model) -> None:
        """Register the model's UDFs and let its sink do its DDL. Once each.

        UDFs come first and, per ``PLAN.md``, **before planning**: an aggregate
        expression may call one, and the config loader deliberately records only
        a dotted path so that nothing is imported at load time.
        """
        if model.name in self._prepared_models:
            return
        self._register_udfs(model)
        with self._model_context(model):
            model.sink.ensure(self.con, model)
        self._prepared_models.add(model.name)

    def _register_udfs(self, model: Model) -> None:
        """Resolve each declared UDF and let it register itself on ``con``.

        The contract, stated here because this is the only place it is applied:
        a dotted path in ``udfs`` resolves to something that **registers SQL
        functions on a connection**, not to the SQL function itself. Either an
        object with a ``register(con)`` method, or a callable whose first
        parameter is named ``con``/``conn``/``connection``.

        It has to be that way round. ``create_function`` needs a name, argument
        types and a return type, and ``CONTEXT.md`` 1.2 shows that the useful
        ones are Arrow-mode with a ``LIST(DOUBLE) -> LIST(DOUBLE)`` signature —
        none of which a dotted path can carry. So the registrar owns its own
        signature::

            # my_pkg/signal.py
            def arrow_fft(con):
                con.create_function("arrow_fft", _fft, [LIST_DOUBLE],
                                    LIST_DOUBLE, type=PythonUDFType.ARROW)

        A bare computation function is refused rather than called with a
        connection, which would otherwise fail somewhere inside numpy with a
        message about the wrong thing. Phase 3's ``duckstream.udf`` helpers will
        provide ready-made registrars; the contract does not change.
        """
        for path in model.udfs:
            token = f"{model.name}\x00{path}"
            if token in self._registered_udfs:
                continue
            with self._model_context(model):
                registrar = self._udf_registrar(path, model)
                try:
                    registrar(self.con)
                except DuckstreamError:
                    raise
                except Exception as exc:
                    raise DuckstreamError(
                        f"model {model.name!r}: registering udf {path!r} failed: "
                        f"{type(exc).__name__}: {exc}"
                    ) from exc
            self._registered_udfs.add(token)

    def _udf_registrar(self, path: str, model: Model) -> Callable[[Any], Any]:
        from duckstream.registry import resolve_udf

        obj = resolve_udf(path)
        register = getattr(obj, "register", None)
        if callable(register):
            return register
        if callable(obj) and self._takes_connection(obj):
            return obj
        raise DuckstreamError(
            f"model {model.name!r}: udf {path!r} resolved to "
            f"{type(obj).__name__}, which duckstream cannot register. A udf "
            f"entry must resolve to something that registers SQL functions on a "
            f"connection: an object with a `register(con)` method, or a "
            f"callable whose first parameter is named 'con', 'conn' or "
            f"'connection'. It is not the computation itself — DuckDB's "
            f"create_function needs a SQL name, argument types and a return "
            f"type (Arrow mode for anything over LIST; CONTEXT.md 1.2), and a "
            f"dotted path cannot carry those, so the registrar declares them."
        )

    @staticmethod
    def _takes_connection(obj: Any) -> bool:
        import inspect

        try:
            signature = inspect.signature(obj)
        except (TypeError, ValueError):
            return False
        parameters = [
            p
            for p in signature.parameters.values()
            if p.kind
            in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.VAR_POSITIONAL)
        ]
        if not parameters:
            return False
        first = parameters[0]
        if first.kind is first.VAR_POSITIONAL:
            return True
        return first.name in ("con", "conn", "connection")

    # -- the loop ------------------------------------------------------------

    def _drain(self, model: Model, trigger: Trigger) -> list[BatchResult]:
        """Run batches for one model until the trigger says stop.

        The loop has one guard that is not about triggers at all: if a batch
        commits without the offset moving, it stops with an error rather than
        going round again. A source whose ``plan`` returns the same ``end``
        twice would otherwise spin forever, re-reading and re-folding the same
        rows into the sink — an unbounded corruption, not just a hang.
        """
        self._prepare_model(model)
        results: list[BatchResult] = []
        committed = 0
        previous = object()
        while True:
            result = self._run_batch(model)
            results.append(result)
            if result.is_empty or not result.committed:
                # Empty, backed off, failed, or quarantined -- all of them end
                # this model's turn. Quarantine stops the drain too, and that is
                # the point: a source where *every* batch is unprocessable would
                # otherwise burn through the whole backlog in a single run,
                # quarantining it batch by batch before anyone saw the first
                # one. One quarantine per model per run bounds the damage to
                # something an operator can still catch on the next tick.
                break
            committed += 1

            marker = _fingerprint(result.end_offset)
            if marker == _fingerprint(result.start_offset) or marker == previous:
                raise DuckstreamError(
                    f"model {model.name!r}: batch {result.batch_id} committed but "
                    f"the source offset did not advance, so the next batch would "
                    f"read the same rows again and fold them into the sink a "
                    f"second time. Stopping instead of looping. This is a bug in "
                    f"source {type(model.source).__name__!r}: a non-empty "
                    f"BatchPlan must carry an `end` offset strictly beyond its "
                    f"`start`."
                )
            previous = marker

            if not trigger.should_continue(
                batches=committed, has_more=result.has_more
            ):
                break
        return results

    def _run_batch(self, model: Model) -> BatchResult:
        """One pass of the lifecycle. See the module docstring for the order."""
        position = self._position(model)
        start = position.offset

        if position.failing:
            waiting = self._backoff_remaining(position)
            if waiting is not None:
                return BatchResult(
                    model=model.name,
                    is_empty=False,
                    has_more=False,
                    outcome="backoff",
                    attempt=position.attempt,
                    error=position.error,
                    start_offset=start,
                    end_offset=start,
                )

        source = model.source
        with self._model_context(model):
            end = source.latest_offset()
            plan = source.plan(start, end, model.limits)

        if plan.is_empty:
            # No transaction, no checkpoint, no snapshot. CONTEXT.md 1.8.
            return BatchResult(
                model=model.name,
                is_empty=True,
                has_more=False,
                outcome="empty",
                start_offset=start,
                end_offset=start,
            )

        try:
            return self._attempt_batch(model, plan, position)
        except Exception as exc:
            # The batch's own transaction is already rolled back -- the inner
            # handler in _attempt_batch does that, and it is what makes the
            # failure safe. What is left is to record the attempt, decide
            # whether this batch has had enough of them, and hand back a result
            # rather than an exception, so the models after this one still run.
            return self._handle_failure(model, plan, position, exc)

    def _attempt_batch(
        self, model: Model, plan: BatchPlan, position: Position
    ) -> BatchResult:
        """The batch lifecycle proper, for a plan already known to be non-empty."""
        source = model.source
        sink = model.sink
        start = position.offset

        batch_id = self._batch_id(model.name)
        ctx = BatchContext(model_name=model.name, batch_id=batch_id, plan=plan)
        self.faults.fire(
            "after_plan",
            FaultEvent("after_plan", self, model, self.con, plan, ctx),
        )

        self.state.record_batch_start(self.con, model.name, batch_id)

        # Bound outside the transaction: a temp view is DDL against `temp`, and
        # one transaction cannot write two attached databases (CONTEXT.md 1.9).
        with self._model_context(model):
            view = source.bind(self.con, plan)
        views = [view]
        try:
            with self._model_context(model):
                event_time = self._observe_event_time(model, view)
            written = event_time.view
            if written != view:
                views.append(written)
            ctx = replace(ctx, watermark=event_time.watermark)
            self.faults.fire(
                "after_bind",
                FaultEvent("after_bind", self, model, self.con, plan, ctx, written),
            )

            self.state.begin(self.con)
            try:
                with self._model_context(model):
                    rows_out = sink.write(self.con, written, model, ctx)
                self.faults.fire(
                    "after_sink_write",
                    FaultEvent(
                        "after_sink_write", self, model, self.con, plan, ctx, written
                    ),
                )
                self.state.record_batch_end(
                    self.con,
                    model.name,
                    batch_id,
                    rows_in=event_time.rows_in,
                    rows_out=rows_out if isinstance(rows_out, int) else None,
                    rows_late=event_time.rows_late,
                    rows_undated=event_time.rows_undated,
                )
                self.faults.fire(
                    "before_commit",
                    FaultEvent(
                        "before_commit", self, model, self.con, plan, ctx, written
                    ),
                )
            except BaseException:
                # Everything above happened inside the transaction, so undoing
                # it is one ROLLBACK. Leaving it half-open would strand the
                # connection: the next trigger's begin() refuses to nest.
                self._rollback()
                raise

            # Sink rows, batch record, watermark and offset become durable
            # together, as one DuckLake snapshot. This call is the exactly-once
            # guarantee, and the watermark riding along in it is what makes the
            # sealing decision recoverable: a killed process resumes from the
            # same horizon it resumes reading from.
            watermarks = (
                {} if event_time.watermark is None
                else {model.name: event_time.watermark}
            )
            self.state.commit(self.con, {model.name: plan.end}, watermarks)
            self._next_ids[model.name] = batch_id + 1
            if event_time.watermark is not None:
                self._watermarks[model.name] = event_time.watermark
            self.faults.fire(
                "after_commit",
                FaultEvent("after_commit", self, model, self.con, plan, ctx, written),
            )
        finally:
            for name in views:
                self._drop_view(name)

        return BatchResult(
            model=model.name,
            is_empty=False,
            has_more=bool(plan.has_more),
            batch_id=batch_id,
            rows_in=event_time.rows_in,
            rows_out=rows_out if isinstance(rows_out, int) else None,
            rows_late=event_time.rows_late,
            rows_undated=event_time.rows_undated,
            watermark=event_time.watermark,
            start_offset=plan.start,
            end_offset=plan.end,
            view=view,
        )

    # -- failure --------------------------------------------------------------

    def _position(self, model: Model) -> Position:
        """Where this model is and how it is going, in one read.

        The same single read the engine has always done to learn its offset --
        ``load_position`` returns the retry state from the same row, so knowing
        whether the last attempt failed costs nothing extra. ``CONTEXT.md`` 1.11
        is the reason that matters: a second scalar read of a DuckLake state
        table would have added ~10 ms to *every* trigger to carry information
        that is only interesting when something is broken.
        """
        with self._model_context(model):
            return self.state.load_position(self.con, model.name)

    @staticmethod
    def _backoff_remaining(position: Position) -> "timedelta | None":
        """How much longer this model must wait, or ``None`` if it may run now.

        Capped exponential on the time since the last failure. Under cron this
        almost never bites -- consecutive attempts are already a whole tick
        apart -- and that is fine, because the case it exists for is the drain
        loop, where a source that fails instantly would otherwise spend its
        whole attempt budget in a few hundred milliseconds and quarantine data
        that a two-second-old transient would have let through.
        """
        ready = position.ready_at()
        if ready is None:
            return None
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        return ready - now if ready > now else None

    def _handle_failure(
        self, model: Model, plan: BatchPlan, position: Position, exc: Exception
    ) -> BatchResult:
        """Record a failed attempt, and decide whether this batch gets another.

        The two policies differ in exactly one respect -- whether the offset is
        ever allowed past data that would not process:

        ``halt``
            never. The attempt is recorded, the position stays put, and every
            later trigger retries the same batch until a human intervenes. A gap
            in the output is worse than a stall. Once the attempts are spent the
            verdict stops being re-recorded -- a halted model retries on every
            tick, cheaply and silently, so that fixing the underlying problem is
            all it takes to recover, but it writes nothing further.

        ``quarantine`` (the default)
            once ``max_attempts`` is reached, the offset moves past the batch
            and a permanent row in ``duckstream.quarantine`` records exactly
            what was skipped and why. The argument is that halting does not
            preserve the unprocessable data either -- it just stops collecting
            everything that arrives after it as well -- so continuing loses
            strictly less.

        Either way the failure is durable before this returns, so a process
        killed immediately afterwards resumes with the attempt already counted
        rather than starting the budget over.
        """
        batch_id = self._batch_id(model.name)
        # A model that has already spent its budget is one that halted: the
        # verdict is on record, so this attempt adds an attempt but no new
        # information. Anything below the ceiling counts up to it, and the
        # attempt that *reaches* the ceiling is recorded like any other -- the
        # stored counter has to be able to say "max_attempts attempts were
        # made", or a halted model reads as one try short of its own limit.
        at_ceiling = position.attempt >= model.max_attempts
        attempt = position.attempt if at_ceiling else position.attempt + 1
        exhausted = attempt >= model.max_attempts
        wrote = True

        with self._model_context(model):
            if exhausted and model.on_failure == "quarantine":
                self.state.quarantine(
                    self.con,
                    model.name,
                    batch_id,
                    position,
                    plan.end,
                    payload=plan.payload,
                    attempts=attempt,
                    error=exc,
                )
                outcome, end_offset = "quarantined", plan.end
            elif exhausted and at_ceiling:
                # Halted, and already recorded as such. Writing the same verdict
                # again on every tick would append a row and a DuckLake snapshot
                # a minute for as long as nobody fixes the underlying problem,
                # which is exactly the situation where a growing catalog helps
                # least. The stored record already says how many attempts it
                # took and when; nothing new has been learned.
                outcome, end_offset, wrote = "halted", position.offset, False
            else:
                self.state.record_failure(
                    self.con, model.name, batch_id, position, exc
                )
                outcome = "halted" if exhausted else "failed"
                end_offset = position.offset

        if wrote:
            # Recording spent this batch id, so the next attempt must not reuse
            # it -- two rows sharing an id would make "newest row wins"
            # ambiguous in the offsets table.
            self._next_ids[model.name] = batch_id + 1

        return BatchResult(
            model=model.name,
            is_empty=False,
            has_more=False,
            outcome=outcome,
            batch_id=batch_id,
            attempt=attempt,
            error=f"{type(exc).__name__}: {exc}",
            start_offset=position.offset,
            end_offset=end_offset,
        )

    # -- event time ----------------------------------------------------------

    def _committed_watermark(self, model: Model) -> datetime | None:
        """The watermark previous batches committed, read once per process.

        Straight from ``CONTEXT.md`` 1.10, which measured the same shape for
        the batch id: a scalar read of a DuckLake state table inside the
        trigger costs ~11 ms, and here it measured **10.4 ms** — a third of
        everything a lateness horizon adds to a trigger, spent re-reading a
        value this process wrote itself. So it is read once and then kept in
        memory.

        The rule that makes it safe is the same one: the cache is written
        **only after a successful commit**, so a rolled-back batch leaves it at
        the last durable value and the next attempt filters against exactly the
        horizon a fresh process would have loaded. Sound because v1 is
        single-writer under ``AvailableNow`` (``CONTEXT.md`` 2.5) — nothing else
        advances this model's watermark while the engine runs. It is on the
        list to revisit the day a second writer exists, alongside the memoised
        batch id it copies.

        ``None`` is a real cached value (no dated row has been seen yet), so
        membership decides whether to read, not truthiness.
        """
        if model.name in self._watermarks:
            return self._watermarks[model.name]
        watermark = self.state.load_watermark(self.con, model.name)
        self._watermarks[model.name] = watermark
        return watermark

    def _watermark_policy(self, model: Model) -> WatermarkPolicy | None:
        """The model's event-time policy, resolved once per model per process.

        ``None`` for a model with no lateness horizon, and that ``None`` is the
        whole phase-1 path: no watermark read, none written, no row filtered.
        """
        try:
            return self._policies[model.name]
        except KeyError:
            policy = policy_for(model)
            self._policies[model.name] = policy
            return policy

    def _observe_event_time(self, model: Model, view: str) -> "_EventTime":
        """Measure the bound batch in event time and decide what the sink sees.

        For a model with no horizon this is the phase-1 ``count(*)`` and
        nothing else. For one with a horizon it is a single scan yielding the
        row count, both drop counts and the batch's newest event time — and
        then, **only if something would actually be dropped**, a second temp
        view with those rows filtered out. In the healthy case no extra view is
        created and no row is read twice, which matters because this is on
        every trigger of every event-time model.

        The filter uses the **committed** watermark, never the one this batch is
        about to write; :mod:`duckstream.watermark` explains why at length, but
        the short version is that a batch may legitimately span a wide range of
        event times and must not be judged against its own maximum.
        """
        policy = self._watermark_policy(model)
        if policy is None:
            return _EventTime(view=view, rows_in=self._count_rows(view))

        previous = self._committed_watermark(model)
        observation = policy.observe(self.con, view, previous)
        written = (
            policy.on_time_view(self.con, view, previous)
            if observation.drops_anything
            else view
        )
        return _EventTime(
            view=written,
            rows_in=observation.rows_in,
            rows_late=observation.rows_late,
            rows_undated=observation.rows_undated,
            watermark=policy.advance(previous, observation.max_event_ts),
        )

    # -- helpers -------------------------------------------------------------

    def _batch_id(self, model_name: str) -> int:
        """The id this batch will carry; 1-based, one per committed batch.

        Read from the state store once per model per process and then kept in
        memory. ``CONTEXT.md`` 1.10 measured a ``max(batch_id)`` scan inside the
        trigger's transaction at ~11 ms — more than half of what a small trigger
        should cost — and memoising it is what took ``sink + offset`` from 39 ms
        to 14.9 ms there. Sound because v1 is single-writer (``CONTEXT.md``
        2.5); it is advanced only after a successful commit, so a rolled-back
        batch reuses its id.
        """
        cached = self._next_ids.get(model_name)
        if cached is not None:
            return cached
        batch_id = int(self.state.next_batch_id(self.con, model_name))
        self._next_ids[model_name] = batch_id
        return batch_id

    def _count_rows(self, view: str) -> int | None:
        """Rows in the bound batch, for the metrics row. ``None`` if unavailable.

        Cheap for the format that matters: ``count(*)`` over parquet is answered
        from the footer rather than by reading data pages, which is the same
        property the file source's row limiting relies on. A failure here is
        never allowed to fail the batch — it is bookkeeping, and the batch is
        the work.
        """
        try:
            row = self.con.execute(
                f"SELECT count(*) FROM {quote_ident(view)}"
            ).fetchone()
        except Exception:
            return None
        return int(row[0]) if row and row[0] is not None else None

    def _rollback(self) -> None:
        rollback = getattr(self.state, "rollback", None)
        try:
            if callable(rollback):
                rollback(self.con)
            else:  # pragma: no cover - every shipped store has rollback
                self.con.execute("ROLLBACK")
        except Exception:
            # commit() rolls back on its own failures, so by the time we get
            # here there may be no transaction left to abandon. The original
            # exception is the one worth propagating.
            pass

    def _drop_view(self, view: str) -> None:
        """Drop the batch's temp view once the transaction is closed.

        Outside the transaction, always: dropping it inside would be a second
        attached database in one transaction (``CONTEXT.md`` 1.9). One view per
        batch would otherwise accumulate for the life of the connection.
        """
        try:
            self.con.execute(f"DROP VIEW IF EXISTS {quote_ident(view)}")
        except Exception:  # pragma: no cover - best effort cleanup
            pass

    def _model_context(self, model: Model) -> "_ModelContext":
        return _ModelContext(model)

    # -- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Close the connection **only if this engine opened it**.

        A connection passed to :meth:`__init__` belongs to the caller; one
        opened by :meth:`from_config` belongs to the engine. Getting this wrong
        matters more than usual: ``CONTEXT.md`` 1.6 measured that a DuckDB file
        held open by one process cannot be opened by another at all.
        """
        if self._owns_connection and self.con is not None:
            self.con.close()
            self._owns_connection = False

    def __enter__(self) -> "Engine":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        names = ", ".join(self._models) or "no models"
        return f"Engine(catalog={self.catalog!r}, alias={self.alias!r}, {names})"


@dataclass(frozen=True)
class _EventTime:
    """What one batch looked like in event time, and what the sink will read.

    ``view`` is the batch view the sink is handed: the source's own view when
    nothing is dropped, and a filtered temp view over it when something is. The
    counts stay ``None`` for a model with no lateness horizon, so "no horizon"
    and "horizon that dropped nothing" remain distinguishable all the way out
    to :class:`BatchResult`.
    """

    view: str
    rows_in: int | None = None
    rows_late: int | None = None
    rows_undated: int | None = None
    watermark: datetime | None = None


class _ModelContext:
    """Names the model in any bare :class:`DuckstreamError` raised inside.

    Phase 1's sink refuses a non-additive model with a long, specific message
    that already names it; this exists so the ones that do not — a source that
    cannot read a file, a malformed identifier — still reach the CLI as
    ``model 'x': ...`` rather than as an anonymous traceback. Only the base
    class is rewritten: a :class:`ModelValidationError` or a
    :class:`ConfigError` keeps its type and its structured fields.
    """

    __slots__ = ("model",)

    def __init__(self, model: Model) -> None:
        self.model = model

    def __enter__(self) -> "_ModelContext":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if exc_type is not DuckstreamError:
            return False
        message = str(exc)
        if f"{self.model.name!r}" in message or f"model {self.model.name}" in message:
            return False
        raise DuckstreamError(f"model {self.model.name!r}: {message}") from exc
