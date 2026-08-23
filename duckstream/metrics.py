"""Lag, and everything else ``duckstream status`` needs to answer.

``PLAN.md`` calls lag "the operational metric that matters", and it is right,
but "lag" means three different things for a micro-batch engine and conflating
them is how a dashboard ends up reassuring. They fail independently, so this
module reports all three:

**Event-time lag** — ``now - watermark``. How far behind real time the *data*
is. It is the one that matters for correctness questions: if it exceeds the
lateness horizon then windows are sealing on data that has not arrived, and
``rows_late`` will already be climbing. ``None`` for a model with no horizon,
because such a model has no opinion about time.

**Processing lag** — ``now - last committed batch``. How long since the engine
last did anything. A pipeline whose cron entry was deleted has perfect
event-time lag right up until you notice this one.

**Backlog** — what the source has that the engine has not consumed. The two lags
above are both zero for a stream nobody is feeding *and* for a stream whose
source has silently stopped being readable; the backlog separates them. It is
source-defined and optional, because only the source can know.

Nothing here writes. Everything it reports was recorded by the engine as part of
a transaction it was committing anyway (``CONTEXT.md`` 1.11), so ``status`` is a
read-only view of the catalog and can be pointed at a live deployment from
another process -- which is exactly what makes DuckLake the right substrate
(``CONTEXT.md`` 1.6: the catalog is not a file one process holds).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from duckstream.protocols import BatchLimits
from duckstream.state import Position

__all__ = ["ModelStatus", "collect", "status_for", "utcnow"]


def utcnow() -> datetime:
    """Naive UTC now, matching how the state store stores timestamps."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _age(moment: datetime | None, now: datetime) -> timedelta | None:
    if moment is None:
        return None
    return now - moment


@dataclass
class ModelStatus:
    """Everything known about one model, from the catalog alone."""

    name: str

    watermark: datetime | None = None
    event_lag: timedelta | None = None
    """``now - watermark``. ``None`` when the model declares no horizon."""

    lateness: timedelta | None = None
    """The declared horizon, so ``event_lag`` can be judged against something."""

    last_batch_id: int | None = None
    last_committed_at: datetime | None = None
    processing_lag: timedelta | None = None
    """``now - last committed batch``. ``None`` if the model never committed."""

    batches: int = 0
    rows_in: int = 0
    rows_out: int = 0
    rows_late: int = 0
    rows_undated: int = 0

    attempt: int = 0
    """Consecutive failed attempts at the current position; 0 when healthy."""

    error: str | None = None
    failed_at: datetime | None = None
    retry_at: datetime | None = None
    """When the next attempt may run, if this model is backing off."""

    quarantined: int = 0
    quarantined_rows: int | None = None
    last_quarantine: datetime | None = None

    offset: Any = None
    backlog: Any = None
    """Source-defined, and ``None`` when the source cannot say."""

    # -- verdicts ---------------------------------------------------------

    @property
    def healthy(self) -> bool:
        """Nothing is currently failing and nothing has ever been skipped.

        Quarantine counts against health permanently and on purpose. It is the
        record that data was lost; a status that went green again once the
        stream recovered would be quietly retiring the only evidence.
        """
        return self.attempt == 0 and self.quarantined == 0

    @property
    def behind_horizon(self) -> bool:
        """Is event-time lag past the lateness horizon?

        When it is, the watermark is being driven by data that is older than the
        horizon allows for, so windows are sealing before their late arrivals
        turn up and ``rows_late`` is the number to look at next.
        """
        if self.event_lag is None or self.lateness is None:
            return False
        return self.event_lag > self.lateness

    @property
    def state(self) -> str:
        """One word for the whole model, worst-first.

        Ordered by what an operator should look at first rather than by
        severity in the abstract: ``failing`` is actionable now, ``quarantined``
        is actionable but historical, ``behind`` is a tuning problem, ``idle``
        means the model exists and has never run.
        """
        if self.attempt:
            return "failing"
        if self.quarantined:
            return "quarantined"
        if self.behind_horizon:
            return "behind"
        if self.last_committed_at is None:
            return "idle"
        return "ok"


@dataclass
class Snapshot:
    """Status for every model, plus what they share."""

    models: list[ModelStatus] = field(default_factory=list)
    taken_at: datetime = field(default_factory=utcnow)

    @property
    def healthy(self) -> bool:
        return all(m.healthy for m in self.models)

    def by_name(self, name: str) -> ModelStatus:
        for model in self.models:
            if model.name == name:
                return model
        raise KeyError(name)


def status_for(
    con: Any,
    store: Any,
    model: Any,
    *,
    now: datetime | None = None,
    include_backlog: bool = True,
) -> ModelStatus:
    """Read one model's status out of the catalog.

    ``include_backlog`` is separable because it is the one part that touches the
    *source* rather than the catalog: for a file source it means walking the
    landing tree, which is cheap but is I/O against something that may be a
    network mount and may be exactly what is broken. ``status`` asks for it;
    anything on a hot path should not.
    """
    now = now or utcnow()
    status = ModelStatus(name=model.name)

    position = store.load_position(con, model.name)
    status.offset = position.offset
    status.attempt = position.attempt
    status.error = position.error
    status.failed_at = position.failed_at
    status.retry_at = position.ready_at()

    status.watermark = store.load_watermark(con, model.name)
    status.event_lag = _age(status.watermark, now)
    lateness = getattr(model, "lateness", None)
    if lateness is not None:
        from duckstream.watermark import parse_lateness

        try:
            status.lateness = parse_lateness(lateness)
        except Exception:  # pragma: no cover - validate() refuses these first
            status.lateness = None

    # Aggregated in SQL, not read back and summed here: the history grows by a
    # row per committed trigger and nothing prunes it until phase 4, so this is
    # the one part of `status` whose cost would otherwise climb for ever. See
    # DuckLakeStateStore.batch_totals for the measurement.
    totals = store.batch_totals(con, model.name)
    status.batches = totals["batches"]
    status.rows_in = totals["rows_in"]
    status.rows_out = totals["rows_out"]
    status.rows_late = totals["rows_late"]
    status.rows_undated = totals["rows_undated"]
    status.last_batch_id = totals["last_batch_id"]
    status.last_committed_at = totals["last_committed_at"]
    status.processing_lag = _age(status.last_committed_at, now)

    quarantine = store.quarantine_totals(con, model.name)
    status.quarantined = quarantine["count"]
    status.quarantined_rows = quarantine["rows_in"]
    status.last_quarantine = quarantine["last"]

    if include_backlog:
        status.backlog = _backlog(model, position)
    return status


def _backlog(model: Any, position: Position) -> int | None:
    """How many source units are waiting, or ``None`` if the source cannot say.

    Optional by design: only a source can know, and asking is I/O against
    something that may be the very thing that has broken. A source that cannot
    answer -- or that raises trying -- reports ``None`` rather than costing the
    caller a status they could otherwise have read.

    The plan is deliberately made **unbounded** rather than with the model's own
    limits. A backlog reported through ``max_files_per_trigger`` would read
    ``10`` whether ten files were waiting or ten thousand, which is precisely
    backwards: the number is worth having because it distinguishes a stream that
    is keeping up from one that is falling behind, and a limit flattens exactly
    that difference.
    """
    source = getattr(model, "source", None)
    if source is None:
        return None
    describe = getattr(source, "backlog", None)
    if callable(describe):
        try:
            return describe(position.offset)
        except Exception:
            return None
    try:
        end = source.latest_offset()
        plan = source.plan(position.offset, end, BatchLimits())
    except Exception:
        return None
    if getattr(plan, "is_empty", False):
        return 0
    payload = plan.payload if isinstance(plan.payload, dict) else {}
    files = payload.get("files")
    return None if files is None else len(files)


def collect(
    con: Any,
    store: Any,
    models: Any,
    *,
    now: datetime | None = None,
    include_backlog: bool = True,
) -> Snapshot:
    """Status for every model, in declaration order."""
    now = now or utcnow()
    return Snapshot(
        models=[
            status_for(con, store, model, now=now, include_backlog=include_backlog)
            for model in models
        ],
        taken_at=now,
    )
