"""``Model`` -- the canonical declaration, and the load-time validation on it.

``Model`` is the single source of truth in duckstream. The Python API builds one
directly; the YAML loader is a deserialiser that builds the same object and then
runs the same :meth:`Model.validate`. There is no parallel validation and no
parallel execution path, which is what keeps the two front doors from drifting
(``PLAN.md``, "Two front doors, one canonical model").

Validation runs *before* anything executes, and that ordering is the product.
A streaming engine that accepts an incorrect model and produces plausible wrong
numbers is worse than one that refuses to start: the mart bugs recorded in
``CONTEXT.md`` section 4 went unnoticed for a long time precisely because
nothing ever raised. So the checks below are deliberately strict, the messages
deliberately verbose, and the headline rejection -- an additive strategy over a
non-foldable aggregate -- happens here rather than at 03:00 in a cron log.

This module does **not** import duckdb at import time. Classification does need
DuckDB's parser, but it opens its connection lazily, so the declarative surface
of duckstream can be imported and inspected cheaply.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from duckstream.aggregates import (
    STRATEGIES,
    STRATEGY_FOR_TIER,
    Tier,
    classify_model,
    strategy_satisfies_tier,
    unknown_functions,
    worst_aggregate,
)
from duckstream.errors import ModelValidationError
from duckstream.protocols import BatchLimits, Sink, Source

__all__ = ["Model", "GRAINS", "MEMORY_PROFILES", "WINDOW_COLUMN"]


#: Tumbling-window grains. Sliding and session windows are explicitly post-v1.
GRAINS: tuple[str, ...] = ("minute", "hour", "day")

#: How a ``non_foldable`` model is allowed to use memory. ``streaming`` means the
#: recompute can be chunked by window range; ``materialising`` means a whole
#: window must be held at once -- ``LIST(x ORDER BY t)`` into a UDF, which
#: ``CONTEXT.md`` section 1.1 measured needing 256 MB where a plain GROUP BY
#: needed 64 MB.
MEMORY_PROFILES: tuple[str, ...] = ("streaming", "materialising")

#: The window column duckstream emits, whatever the grain. Fixed on purpose:
#: the sink merge key must equal the window grain key, and a single fixed name
#: is what makes that invariant checkable rather than conventional.
WINDOW_COLUMN = "window_ts"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_QUALIFIED_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
_DOTTED_PATH = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*"
    r":[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$"
)

_SOURCE_METHODS = ("latest_offset", "plan", "bind", "to_config")
_SINK_METHODS = ("ensure", "write", "to_config")


@dataclass(kw_only=True)
class Model:
    """One declared aggregate model: where rows come from, how they fold, where they go.

    Construction is keyword-only, because the field order is not something
    callers should have to remember and because new optional fields must never
    shift an existing positional argument.

    Nothing here validates on construction. Call :meth:`validate` -- the engine
    and the CLI both do, and ``duckstream validate`` exists so a bad model is
    caught at deploy time.
    """

    name: str
    source: Source
    sink: Sink
    aggregates: dict[str, str]
    key: list[str]
    time_column: str | None = None
    grain: str | None = None
    strategy: str | None = None
    memory_profile: str | None = None
    udfs: list[str] = field(default_factory=list)
    limits: BatchLimits = BatchLimits()

    # -- classification ---------------------------------------------------

    @property
    def column_tiers(self) -> dict[str, Tier]:
        """Foldability tier of each output column."""
        return classify_model(self.aggregates)[1]

    @property
    def tier(self) -> Tier:
        """The model's tier: the worst tier among its output columns.

        One median in an otherwise additive model makes the whole model
        ``non_foldable``, because the engine runs one strategy per model.
        """
        return classify_model(self.aggregates)[0]

    @property
    def resolved_strategy(self) -> str:
        """The strategy that will actually run: declared, or inferred from tier."""
        return self.strategy or STRATEGY_FOR_TIER[self.tier]

    # -- validation -------------------------------------------------------

    def validate(self) -> None:
        """Raise :class:`ModelValidationError` on the first problem found.

        Idempotent and side-effect free: it may be called by the loader, by the
        CLI's ``validate`` command and again by the engine before its first
        trigger, and must behave identically each time.

        Check order is deliberate. Cheap structural checks come first so that a
        typo is not reported as a classification failure, and the strategy check
        comes before the ``non_foldable`` requirements so that a user who
        declared ``delta_merge`` over a median is told *that* -- the actual
        mistake -- rather than being asked for a memory profile.
        """
        self._check_name()
        self._check_source_and_sink()
        self._check_key()
        self._check_aggregates()
        self._check_grain()
        self._check_memory_profile()
        self._check_window_key()
        self._check_strategy()
        self._check_non_foldable_requirements()
        self._check_udfs()
        self._check_udf_coverage()
        self._check_limits()

    # -- individual rules -------------------------------------------------

    def _check_name(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ModelValidationError(
                "model name is empty",
                field="name",
                remedy="Give the model a name; it identifies its offsets and "
                "watermarks in the state store.",
            )
        if not _QUALIFIED_NAME.match(self.name):
            raise ModelValidationError(
                f"model name {self.name!r} is not a valid identifier. Names must "
                f"be letters, digits and underscores, not starting with a digit, "
                f"optionally schema-qualified with a single dot "
                f"(for example 'hourly_counts' or 'marts.hourly_counts')",
                model=self.name,
                field="name",
                remedy="The name is used unquoted in state-store keys and in log "
                "lines, so it is kept to a plain SQL identifier.",
            )

    def _check_source_and_sink(self) -> None:
        for label, obj, methods in (
            ("source", self.source, _SOURCE_METHODS),
            ("sink", self.sink, _SINK_METHODS),
        ):
            if obj is None:
                raise ModelValidationError(
                    f"no {label} declared",
                    model=self.name,
                    field=label,
                )
            missing = [m for m in methods if not callable(getattr(obj, m, None))]
            if missing:
                raise ModelValidationError(
                    f"{label} {type(obj).__name__!r} does not implement the "
                    f"{label.capitalize()} protocol: missing {', '.join(missing)}",
                    model=self.name,
                    field=label,
                    remedy=f"See duckstream.protocols.{label.capitalize()} for the "
                    f"methods a {label} must provide.",
                )

    def _check_key(self) -> None:
        if not isinstance(self.key, (list, tuple)) or not self.key:
            raise ModelValidationError(
                "no merge key declared",
                model=self.name,
                field="key",
                remedy="The key is what makes a re-run idempotent; without it a "
                "retried batch would append duplicates. Declare the columns "
                "that identify one output row, e.g. ['window_ts', 'sensor_id'].",
            )
        seen: set[str] = set()
        for column in self.key:
            if not isinstance(column, str) or not _IDENTIFIER.match(column):
                raise ModelValidationError(
                    f"merge key entry {column!r} is not a valid column name",
                    model=self.name,
                    field="key",
                )
            if column in seen:
                raise ModelValidationError(
                    f"merge key contains {column!r} more than once",
                    model=self.name,
                    field="key",
                    remedy="A duplicated key column is always a typo; it cannot "
                    "make the key more selective.",
                )
            seen.add(column)

    def _check_aggregates(self) -> None:
        if not isinstance(self.aggregates, dict) or not self.aggregates:
            raise ModelValidationError(
                "no aggregates declared",
                model=self.name,
                field="aggregates",
                remedy="Declare at least one output column, e.g. "
                "{'n': 'count(*)', 'total': 'sum(value)'}.",
            )
        for column, expr in self.aggregates.items():
            if not isinstance(column, str) or not _IDENTIFIER.match(column):
                raise ModelValidationError(
                    f"aggregate output column {column!r} is not a valid column name",
                    model=self.name,
                    field="aggregates",
                )
            if column in self.key:
                raise ModelValidationError(
                    f"column {column!r} is declared both as a merge key and as an "
                    f"aggregate; it cannot be both a grouping column and a "
                    f"computed one",
                    model=self.name,
                    field="aggregates",
                )
        # Classification is the expensive check and it raises with the column
        # already named, so re-raise it against this model for a fuller message.
        try:
            classify_model(self.aggregates)
        except ModelValidationError as exc:
            raise ModelValidationError(
                exc.reason,
                model=self.name,
                field="aggregates",
                remedy=exc.remedy,
            ) from None

    def _check_grain(self) -> None:
        if self.grain is None:
            return
        if self.grain not in GRAINS:
            raise ModelValidationError(
                f"grain {self.grain!r} is not supported; expected one of "
                f"{', '.join(repr(g) for g in GRAINS)}",
                model=self.name,
                field="grain",
                remedy="Sliding and session windows are post-v1; only tumbling "
                "windows at these grains exist today.",
            )
        if not self.time_column:
            raise ModelValidationError(
                f"grain {self.grain!r} is declared but no time_column is, so there "
                f"is no event-time column to derive windows from",
                model=self.name,
                field="time_column",
                remedy="Declare time_column, e.g. time_column='event_ts'.",
            )

    def _check_memory_profile(self) -> None:
        if self.memory_profile is None:
            return
        if self.memory_profile not in MEMORY_PROFILES:
            raise ModelValidationError(
                f"memory_profile {self.memory_profile!r} is not supported; expected "
                f"one of {', '.join(repr(p) for p in MEMORY_PROFILES)}",
                model=self.name,
                field="memory_profile",
            )

    def _check_window_key(self) -> None:
        """The invariant: the sink merge key must equal the window grain key.

        If a model windows by grain but merges on a key that does not include
        the window column, a re-run of a batch overwrites rows belonging to a
        different window -- and it does so silently, producing a mart that looks
        fine and is wrong. ``PLAN.md`` calls this out as an invariant, so it is
        enforced rather than documented.
        """
        if self.grain is None:
            return
        if WINDOW_COLUMN not in self.key:
            raise ModelValidationError(
                f"grain {self.grain!r} is declared, so every output row belongs to "
                f"a window, but the merge key {list(self.key)!r} does not contain "
                f"{WINDOW_COLUMN!r}. Merging windowed output on a key that omits "
                f"the window column silently overwrites other windows' rows "
                f"instead of being idempotent",
                model=self.name,
                field="key",
                remedy=f"Add {WINDOW_COLUMN!r} to key, e.g. "
                f"key=['{WINDOW_COLUMN}', ...]. duckstream always names the "
                f"window column {WINDOW_COLUMN!r}, whatever the grain.",
            )

    def _check_strategy(self) -> None:
        if self.strategy is None:
            return
        if self.strategy not in STRATEGIES:
            raise ModelValidationError(
                f"strategy {self.strategy!r} is not supported; expected one of "
                f"{', '.join(repr(s) for s in STRATEGIES)}",
                model=self.name,
                field="strategy",
            )

        tier, column_tiers = classify_model(self.aggregates)
        if strategy_satisfies_tier(self.strategy, tier):
            # A stronger strategy than the tier requires is accepted without
            # comment: recompute_window over an additive model is correct, just
            # slower, and is a reasonable thing to ask for while reconciling.
            return

        column = next(c for c, t in column_tiers.items() if t is tier)
        expression = self.aggregates[column]
        raise ModelValidationError.strategy_conflict(
            model=self.name,
            declared=self.strategy,
            tier=tier,
            column=column,
            expression=expression,
            aggregate=worst_aggregate(expression),
            allowed=STRATEGY_FOR_TIER[tier],
        )

    def _check_non_foldable_requirements(self) -> None:
        """A ``non_foldable`` model has to say enough to be recomputed safely.

        There is no shortcut for tier three: the affected windows must be
        recomputed from source. That needs a ``time_column`` to identify which
        windows a batch touched, and a ``memory_profile`` because recomputing a
        window may materialise it whole -- 256 MB against 64 MB for a plain
        GROUP BY, measured in ``CONTEXT.md`` section 1.1.
        """
        if classify_model(self.aggregates)[0] is not Tier.NON_FOLDABLE:
            return

        missing = []
        if not self.time_column:
            missing.append("time_column")
        if not self.memory_profile:
            missing.append("memory_profile")
        if not missing:
            return

        offenders = ", ".join(
            f"{column}={self.aggregates[column]!r}"
            for column, t in self.column_tiers.items()
            if t is Tier.NON_FOLDABLE
        )
        raise ModelValidationError(
            f"this model is tier {Tier.NON_FOLDABLE.value!r} ({offenders}), so it "
            f"must be recomputed window by window, but {' and '.join(missing)} "
            f"{'is' if len(missing) == 1 else 'are'} not declared",
            model=self.name,
            field=missing[0],
            remedy="time_column identifies which windows a batch touched; "
            "memory_profile ('streaming' or 'materialising') tells the engine "
            "whether a whole window has to be held in memory at once.",
        )

    def _check_udfs(self) -> None:
        if not isinstance(self.udfs, (list, tuple)):
            raise ModelValidationError(
                f"udfs must be a list of dotted paths, not {type(self.udfs).__name__}",
                model=self.name,
                field="udfs",
            )
        for path in self.udfs:
            # Shape only. Importing at validation time would make `duckstream
            # validate` depend on the runtime environment being fully installed,
            # which defeats the point of running it at deploy time.
            if not isinstance(path, str) or not _DOTTED_PATH.match(path):
                raise ModelValidationError(
                    f"udf entry {path!r} is not a dotted path of the form "
                    f"'package.module:object'",
                    model=self.name,
                    field="udfs",
                    remedy="For example 'my_pkg.signal:arrow_fft'. The object is "
                    "resolved and registered before planning, not now.",
                )

    def _check_udf_coverage(self) -> None:
        """Refuse an expression calling a function nothing will have registered.

        An unknown function name is a user UDF. The classifier already treats it
        as ``non_foldable``, but that alone let a model validate cleanly and then
        die on its first trigger with a Catalog Error -- the exact failure
        ``duckstream validate`` exists to prevent at deploy time.

        The check is deliberately coarse. If ``udfs`` is non-empty the model is
        accepted, because the engine imports and registers those objects before
        planning and can re-check against the live catalog once they are there.
        No attempt is made to match a dotted path such as
        ``my_pkg.signal:arrow_fft`` to the SQL name it registers itself under:
        the two are genuinely independent, and guessing would reject correct
        models.
        """
        if self.udfs:
            return

        offenders: dict[str, list[str]] = {}
        for column, expr in self.aggregates.items():
            names = unknown_functions(expr)
            if names:
                offenders[column] = names

        if not offenders:
            return

        every_name = sorted({n for names in offenders.values() for n in names})
        detail = ", ".join(
            f"{column} calls {', '.join(names)}"
            for column, names in offenders.items()
        )
        many = len(every_name) > 1
        plural = "functions" if many else "a function"
        raise ModelValidationError(
            f"no `udfs` are declared, but this model calls {plural} DuckDB does "
            f"not know ({detail}). Nothing would register "
            f"{'them' if many else 'it'}, so the first trigger would fail with a "
            f"Catalog Error",
            model=self.name,
            field="udfs",
            remedy="Declare the implementation as a dotted path, e.g. "
            "udfs=['my_pkg.signal:arrow_fft']. It is imported and registered "
            "before planning. If the name is a typo, fix the expression.",
        )

    def _check_limits(self) -> None:
        if not isinstance(self.limits, BatchLimits):
            raise ModelValidationError(
                f"limits must be a BatchLimits, not {type(self.limits).__name__}",
                model=self.name,
                field="limits",
            )
        for attr in ("max_rows_per_trigger", "max_files_per_trigger"):
            value = getattr(self.limits, attr)
            if value is None:
                continue
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ModelValidationError(
                    f"limits.{attr} must be a positive integer or None, got {value!r}",
                    model=self.name,
                    field="limits",
                    remedy="Bounding rows in flight is the memory control that "
                    "works; zero or negative would bound it to nothing.",
                )

    # -- serialisation ----------------------------------------------------

    def to_config(self) -> dict[str, Any]:
        """A plain, JSON/YAML-safe dict that reconstructs this model exactly.

        Every field is expressible here. That is enforced by the config
        round-trip test -- ``Model`` -> dict -> YAML -> ``Model`` must give back
        an equal object -- so a field addable in Python but not in config is a
        test failure, not a slow drift between the two front doors.

        Fields left at their default are omitted, because absence and the
        default reconstruct the same object and the YAML reads better for it.
        """
        config: dict[str, Any] = {
            "name": self.name,
            "source": self.source.to_config(),
            "sink": self.sink.to_config(),
            "aggregates": dict(self.aggregates),
            "key": list(self.key),
        }
        if self.time_column is not None:
            config["time_column"] = self.time_column
        if self.grain is not None:
            config["grain"] = self.grain
        if self.strategy is not None:
            config["strategy"] = self.strategy
        if self.memory_profile is not None:
            config["memory_profile"] = self.memory_profile
        if self.udfs:
            config["udfs"] = list(self.udfs)

        limits: dict[str, Any] = {}
        if self.limits.max_rows_per_trigger is not None:
            limits["max_rows_per_trigger"] = self.limits.max_rows_per_trigger
        if self.limits.max_files_per_trigger is not None:
            limits["max_files_per_trigger"] = self.limits.max_files_per_trigger
        if limits:
            config["limits"] = limits

        return config

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        return (
            f"Model(name={self.name!r}, aggregates={list(self.aggregates)!r}, "
            f"key={list(self.key)!r}, grain={self.grain!r}, "
            f"strategy={self.strategy!r})"
        )
