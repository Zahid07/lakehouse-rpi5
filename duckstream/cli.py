"""The config front door: ``run``, ``validate``, ``models``.

Thin on purpose. ``PLAN.md`` describes this module as "arg parsing only, no
logic of its own", and that is the rule the code follows: every command loads a
document with :func:`duckstream.config.load_config`, builds an ordinary
:class:`~duckstream.engine.Engine`, and calls the same methods the Python API
calls. There is no config-driven execution path, which is exactly what stops the
two front doors drifting.

Three behaviours are contractual rather than cosmetic:

**``validate`` is honest.** A model that fails at load makes it exit non-zero.
``PLAN.md`` lists that under Verification because deployment scripts depend on
it — the whole point of running it at deploy time is that a bad model is caught
then, not at 03:00 in a cron log.

**Errors are messages, not tracebacks.** Everything duckstream raises derives
from :class:`~duckstream.errors.DuckstreamError`, so the whole framework is
caught here and printed to stderr as one or more plain lines. A
:class:`~duckstream.errors.ConfigError` reporting several problems at once
carries them as ``.errors``, already formatted one problem per line, and each is
printed on its own line — an operator fixing a config wants the whole list, not
one item per edit-run cycle.

**Nothing here can arm a fault hook.** The engine's fault-injection points are
installed only by an explicit call on a live ``Engine`` object; there is no flag
and no config key that reaches them. The cron entry point is therefore
structurally incapable of injecting a fault.

``status`` is deliberately absent. It reports offset, watermark, last batch and
lag, and lag needs ``metrics.py``; a ``status`` that printed three of the four
would be worse than none.

Exit codes: ``0`` success, ``1`` a duckstream error, ``2`` a usage error from
argparse, ``130`` interrupted.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Sequence, TextIO

from duckstream.errors import DuckstreamError

__all__ = ["main", "build_parser"]

PROGRAM = "duckstream"

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_INTERRUPTED = 130


# --------------------------------------------------------------------------
# Parser
# --------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """The whole CLI surface, in one place."""
    parser = argparse.ArgumentParser(
        prog=PROGRAM,
        description=(
            "Micro-batch streaming for DuckDB and DuckLake. Each run opens the "
            "catalog, drains what has arrived and exits, so cron or a "
            "supervisor owns the cadence."
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {_version()}"
    )
    commands = parser.add_subparsers(dest="command", metavar="COMMAND")
    commands.required = True

    run = commands.add_parser(
        "run",
        help="drain every model once (the cron entry point)",
        description=(
            "One AvailableNow pass: drain what is currently available, then "
            "exit. Exactly-once holds across a kill at any point — an "
            "uncommitted batch replays from the stored offset."
        ),
    )
    _add_config_argument(run)
    run.add_argument(
        "--model",
        metavar="NAME",
        default=None,
        help="run only this model instead of every model in the document",
    )
    run.add_argument(
        "--once",
        action="store_true",
        help=(
            "run a single batch per model instead of draining. Useful for "
            "stepping a backlog forward one bounded chunk at a time."
        ),
    )
    run.add_argument(
        "--interval",
        metavar="DURATION",
        default=None,
        help=(
            "keep running, draining every DURATION (e.g. '2 seconds'). Without "
            "this, run drains once and exits, which is what cron wants. With "
            "it the process stays up and releases the catalog between cycles, "
            "so anything else can still attach. Stops cleanly on SIGINT or "
            "SIGTERM."
        ),
    )
    run.add_argument(
        "--max-cycles",
        metavar="N",
        type=int,
        default=None,
        help="with --interval, stop after N cycles instead of running forever",
    )
    run.set_defaults(handler=_cmd_run)

    validate = commands.add_parser(
        "validate",
        help="load and validate the document; non-zero exit on failure",
        description=(
            "Run this at deploy time. It performs every load-time check — "
            "including the refusal of an additive strategy over a non-foldable "
            "aggregate — without opening a catalog or touching any data."
        ),
    )
    _add_config_argument(validate)
    validate.set_defaults(handler=_cmd_validate)

    status = commands.add_parser(
        "status",
        help="per model: lag, throughput, and anything currently wrong",
        description=(
            "Read-only, against the catalog rather than the engine, so it can "
            "be pointed at a live deployment from another process. Exits "
            "non-zero when any model is unhealthy, so it doubles as a health "
            "check without its output needing to be parsed. 'Lag' is reported "
            "three ways because they fail independently: event-time lag is how "
            "far behind the data is, time-since-run is how long since the "
            "engine did anything, and backlog is what the source is holding."
        ),
    )
    _add_config_argument(status)
    status.add_argument(
        "--model",
        metavar="NAME",
        default=None,
        help="report only this model",
    )
    status.add_argument(
        "--json",
        action="store_true",
        help=(
            "emit one JSON object per model instead of a table, for a "
            "monitoring probe that should not have to parse columns"
        ),
    )
    status.set_defaults(handler=_cmd_status)

    models = commands.add_parser(
        "models",
        help="list declared models with their resolved tier and strategy",
        description=(
            "The tier is computed from the aggregate expressions, not "
            "declared; the strategy is what will actually run."
        ),
    )
    _add_config_argument(models)
    models.set_defaults(handler=_cmd_models)

    return parser


def _add_config_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        metavar="PATH",
        required=True,
        help="path to the YAML document declaring catalog, settings and models",
    )


def _version() -> str:
    from duckstream import __version__

    return __version__


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------


def _cmd_validate(args: argparse.Namespace, out: TextIO) -> int:
    from duckstream.config import load_config

    document = load_config(args.config)
    count = len(document.models)
    plural = "" if count == 1 else "s"
    names = ", ".join(document.names)
    print(f"{args.config}: ok, {count} model{plural} ({names})", file=out)
    return EXIT_OK


def _cmd_models(args: argparse.Namespace, out: TextIO) -> int:
    from duckstream.config import load_config

    document = load_config(args.config)
    rows = [
        (
            model.name,
            # Tier is a StrEnum and pyyaml refuses to dump any Enum, so it is
            # rendered with str() wherever it leaves Python.
            str(model.tier),
            model.resolved_strategy,
            _describe_windowing(model),
            _describe_source(model),
            _describe_sink(model),
        )
        for model in document.models
    ]
    headers = ("MODEL", "TIER", "STRATEGY", "WINDOW", "SOURCE", "SINK")
    for line in _table(headers, rows):
        print(line, file=out)
    return EXIT_OK


def _cmd_status(args: argparse.Namespace, out: TextIO) -> int:
    """Per model: lag, throughput, and whatever is currently wrong.

    Read-only, and against the catalog rather than the engine, so it can be
    pointed at a live deployment from another process -- ``CONTEXT.md`` 1.6 is
    what makes that possible, since a DuckLake catalog is not a file one process
    holds open.

    Exits non-zero when any model is unhealthy, so it doubles as a health check
    for a supervisor or a monitoring probe without needing its output parsed.
    """
    import duckdb

    from duckstream.config import load_config
    from duckstream.lake import attach_lake
    from duckstream.metrics import collect
    from duckstream.state import DuckLakeStateStore

    document = load_config(args.config)
    models = document.models
    if args.model:
        models = [document.model(args.model)]

    con = duckdb.connect()
    try:
        attach_lake(
            con,
            document.catalog,
            data_path=document.data_path,
            settings=document.settings,
        )
        store = DuckLakeStateStore(catalog="lake")
        store.ensure(con)
        snapshot = collect(con, store, models)
    finally:
        con.close()

    if getattr(args, "json", False):
        import json as _json

        for m in snapshot.models:
            print(_json.dumps(_status_json(m), default=str), file=out)
        return EXIT_OK if snapshot.healthy else EXIT_ERROR

    headers = (
        "MODEL", "STATE", "EVENT LAG", "SINCE RUN", "BACKLOG",
        "BATCHES", "ROWS IN", "ROWS OUT", "LATE", "QUARANTINED",
    )
    rows = [
        (
            m.name,
            m.state,
            _duration(m.event_lag),
            _duration(m.processing_lag),
            "-" if m.backlog is None else str(m.backlog),
            str(m.batches),
            str(m.rows_in),
            str(m.rows_out),
            str(m.rows_late + m.rows_undated),
            str(m.quarantined),
        )
        for m in snapshot.models
    ]
    for line in _table(headers, rows):
        print(line, file=out)

    # A table of numbers does not say what to do. Anything unhealthy gets a
    # sentence underneath it that does.
    for m in snapshot.models:
        if m.attempt:
            when = "" if m.retry_at is None else f", next attempt after {m.retry_at}"
            print(
                f"\n{m.name}: {m.attempt} failed attempt(s){when}\n"
                f"  {m.error}",
                file=out,
            )
        if m.quarantined:
            print(
                f"\n{m.name}: {m.quarantined} batch(es) skipped after repeated "
                f"failure, most recently {m.last_quarantine}. "
                f"Data was lost; SELECT * FROM duckstream.quarantine "
                f"WHERE model_name = '{m.name}' for what and why.",
                file=out,
            )
        if m.offset_is_large:
            print(
                f"\n{m.name}: the committed offset is "
                f"{m.offset_bytes / 1e6:.1f} MB and is rewritten in full on "
                f"every trigger — roughly "
                f"{m.offset_bytes * 1440 / 1e9:.1f} GB a day at a one-minute "
                f"cadence. On SD or USB storage that is a wear problem before "
                f"it is a latency one. Reduce it by pruning consumed files out "
                f"of the landing tree, or by batching more per trigger.",
                file=out,
            )
        if m.behind_horizon:
            print(
                f"\n{m.name}: event-time lag ({_duration(m.event_lag)}) exceeds "
                f"the lateness horizon ({_duration(m.lateness)}), so windows are "
                f"sealing before late data arrives. {m.rows_late} row(s) have "
                f"already been dropped as late.",
                file=out,
            )
    return EXIT_OK if snapshot.healthy else EXIT_ERROR


def _status_json(m: Any) -> dict:
    """One model's status as plain JSON.

    Durations go out as **seconds**, not as the human strings the table uses: a
    probe wants to compare against a threshold, and "3m12s" is not a number.
    """

    def seconds(value):
        return None if value is None else value.total_seconds()

    return {
        "model": m.name,
        "state": m.state,
        "healthy": m.healthy,
        "event_lag_seconds": seconds(m.event_lag),
        "lateness_seconds": seconds(m.lateness),
        "behind_horizon": m.behind_horizon,
        "processing_lag_seconds": seconds(m.processing_lag),
        "backlog": m.backlog,
        "batches": m.batches,
        "rows_in": m.rows_in,
        "rows_out": m.rows_out,
        "rows_late": m.rows_late,
        "rows_undated": m.rows_undated,
        "attempt": m.attempt,
        "error": m.error,
        "quarantined": m.quarantined,
        "offset_bytes": m.offset_bytes,
        "watermark": m.watermark,
        "last_committed_at": m.last_committed_at,
    }


def _duration(value: Any) -> str:
    """A timedelta as something readable in a fixed-width column."""
    if value is None:
        return "-"
    seconds = int(value.total_seconds())
    sign = "-" if seconds < 0 else ""
    seconds = abs(seconds)
    if seconds < 60:
        return f"{sign}{seconds}s"
    if seconds < 3600:
        return f"{sign}{seconds // 60}m{seconds % 60:02d}s"
    if seconds < 86400:
        return f"{sign}{seconds // 3600}h{(seconds % 3600) // 60:02d}m"
    return f"{sign}{seconds // 86400}d{(seconds % 86400) // 3600:02d}h"


def _cmd_run(args: argparse.Namespace, out: TextIO) -> int:
    from duckstream.config import load_config
    from duckstream.engine import Engine
    from duckstream.trigger import AvailableNow, Once

    from duckstream.errors import BatchFailed

    document = load_config(args.config)
    trigger = Once() if args.once else AvailableNow()

    if getattr(args, "interval", None) is not None:
        return _cmd_run_forever(args, document, trigger, out)

    engine = Engine.from_document(document)
    try:
        try:
            report = engine.run(trigger=trigger, model=args.model)
        except BatchFailed as exc:
            # Every model already had its turn; the exception is the run's
            # verdict, not an interruption. Print what happened first and let
            # the exit code carry the verdict, rather than replacing a useful
            # per-model summary with a traceback.
            report = exc.report
    finally:
        engine.close()

    return EXIT_OK if _report_run(report, out) else EXIT_ERROR


def _cmd_run_forever(
    args: argparse.Namespace,
    document: Any,
    trigger: Any,
    out: TextIO,
) -> int:
    """``run --interval``: stay up, drain on a schedule, release between cycles.

    The engine is rebuilt each cycle rather than held, and that is the whole
    design: ``CONTEXT.md`` 1.25 measured that a second process cannot ``ATTACH``
    the catalog even ``READ_ONLY`` while one holds it, so a daemon that never
    let go would lock the operator out of their own warehouse. Closing per cycle
    costs ~11 ms to re-attach a warm process and writes **no** extra snapshots,
    both measured, so the release is close to free.

    The exit code follows the *last* cycle, not the worst: a daemon that
    recovered from a transient fault an hour ago is healthy now, and a service
    manager reading a non-zero exit would restart something that is working.
    Every unhealthy cycle is still printed as it happens.
    """
    from duckstream.daemon import ProcessingTime
    from duckstream.engine import Engine
    from duckstream.errors import BatchFailed

    schedule = ProcessingTime(
        interval=args.interval, max_cycles=getattr(args, "max_cycles", None)
    )
    print(f"{PROGRAM}: {schedule.describe()}", file=out)

    state = {"healthy": True}

    def drain(engine: Any) -> Any:
        """One cycle's drain. `BatchFailed` is a verdict, not an interruption —
        every model already had its turn — so it is unwrapped here exactly as
        the single-pass path unwraps it, and the daemon never sees it as a
        crashed cycle."""
        try:
            return engine.run(trigger=trigger, model=args.model)
        except BatchFailed as exc:
            return exc.report

    def on_cycle(cycle: Any) -> None:
        if cycle.error is not None:
            print(f"{PROGRAM}: cycle {cycle.cycle} failed: {cycle.error}", file=out)
            state["healthy"] = False
            return
        state["healthy"] = (
            _report_run(cycle.report, out) if cycle.report is not None else True
        )
        if cycle.overran:
            print(
                f"{PROGRAM}: cycle {cycle.cycle} took "
                f"{cycle.seconds:.1f}s, longer than the {schedule.seconds:g}s "
                f"interval — not keeping up",
                file=out,
            )

    report = schedule.run(
        factory=lambda: Engine.from_document(document),
        drain=drain,
        on_cycle=on_cycle,
    )
    print(f"{PROGRAM}: {report.describe()}", file=out)
    return EXIT_OK if state["healthy"] else EXIT_ERROR


def _report_run(report: Any, out: TextIO) -> bool:
    """Print the per-model summary. Returns whether every model is healthy."""
    healthy = True
    for name in report.model_names:
        results = report.for_model(name)
        committed = [r for r in results if r.committed]
        if committed:
            rows = sum(r.rows_in or 0 for r in committed)
            written = sum(r.rows_out or 0 for r in committed)
            batches = len(committed)
            plural = "" if batches == 1 else "es"
            last = committed[-1]
            print(
                f"{name}: {batches} batch{plural}, {rows} source rows, "
                f"{written} rows out, through batch {last.batch_id}"
                f"{_describe_drops(committed)}",
                file=out,
            )

        # Anything that did not commit gets its own line. Folding these into
        # the summary above -- or worse, letting a model that failed print
        # "nothing to do" because it committed nothing -- is how a cron log
        # ends up reassuring about a pipeline that is stuck.
        for result in results:
            if result.committed or result.is_empty:
                continue
            healthy = False
            print(_describe_outcome(result), file=out)

        if not committed and all(r.is_empty for r in results):
            print(f"{name}: nothing to do", file=out)

    # Returns a *bool*, not an exit code. Both callers turn it into one, and
    # they must: EXIT_OK is 0, which is falsy, so returning the code from here
    # and testing it for truth inverts the verdict — a healthy run exits 1.
    # That is exactly what happened the first time the daemon path ran.
    return healthy


def _describe_outcome(result: Any) -> str:
    """One line for a pass that did not commit, saying what to do about it."""
    name = result.model
    if result.outcome == "quarantined":
        return (
            f"{PROGRAM}: QUARANTINED {name!r} batch {result.batch_id} after "
            f"{result.attempt} attempts and skipped past it. Data was lost: "
            f"{result.error}"
        )
    if result.outcome == "halted":
        return (
            f"{PROGRAM}: {name!r} is halted after {result.attempt} attempts and "
            f"will not advance past this batch until the cause is fixed: "
            f"{result.error}"
        )
    if result.outcome == "backoff":
        return (
            f"{PROGRAM}: {name!r} is waiting out a backoff after "
            f"{result.attempt} failed attempt(s): {result.error}"
        )
    return (
        f"{PROGRAM}: {name!r} attempt {result.attempt} failed, will retry: "
        f"{result.error}"
    )


def _describe_drops(committed: Sequence[Any]) -> str:
    """What this run refused to aggregate, if anything.

    ``PLAN.md`` requires data past the lateness horizon to be dropped **and
    counted**. It is counted durably in ``duckstream.batches`` either way; this
    puts it in front of whoever is reading the cron log, because a drop nobody
    is told about is a drop nobody investigates. Silent when there is nothing to
    say, so a healthy line stays a healthy line.
    """
    late = sum(r.rows_late or 0 for r in committed)
    undated = sum(r.rows_undated or 0 for r in committed)
    parts = []
    if late:
        parts.append(f"{late} late")
    if undated:
        parts.append(f"{undated} undated")
    if not parts:
        return ""
    watermark = next(
        (r.watermark for r in reversed(committed) if r.watermark is not None), None
    )
    suffix = "" if watermark is None else f", watermark {watermark:%Y-%m-%d %H:%M:%S}"
    return f" -- dropped {', '.join(parts)}{suffix}"


def _describe_windowing(model: Any) -> str:
    """``hour +10 minutes`` -- the grain and, when declared, the horizon.

    Worth a column of its own: the horizon is what decides whether windows ever
    seal, and therefore whether an ``append`` mart is being written at all. An
    operator reading this table should not have to open the YAML to find out.
    """
    grain = getattr(model, "grain", None)
    if grain is None:
        return "-"
    lateness = getattr(model, "lateness", None)
    return grain if lateness is None else f"{grain} +{lateness}"


def _describe_source(model: Any) -> str:
    source = model.source
    name = getattr(source, "type_name", type(source).__name__)
    path = getattr(source, "path", None)
    return f"{name}({path})" if path is not None else str(name)


def _describe_sink(model: Any) -> str:
    sink = model.sink
    name = getattr(sink, "type_name", type(sink).__name__)
    table = getattr(sink, "table", None)
    mode = getattr(sink, "mode", None)
    if table is None:
        return str(name)
    return f"{name}({table}, {mode})" if mode else f"{name}({table})"


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    """Left-aligned columns, two spaces apart. Header only when there are rows."""
    if not rows:
        return ["no models declared"]
    widths = [len(h) for h in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))
    lines = ["  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)).rstrip()]
    for row in rows:
        lines.append(
            "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(row)).rstrip()
        )
    return lines


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def _report(error: BaseException, err: TextIO) -> None:
    """Print a duckstream failure as plain lines, never as a traceback.

    A :class:`ConfigError` aggregating several problems carries them as
    ``.errors``, already formatted one problem per line. They are printed one
    per line rather than as the aggregate's embedded block, so that grepping a
    cron log finds each problem on its own.
    """
    problems = getattr(error, "errors", None)
    if problems:
        print(f"{PROGRAM}: {len(problems)} problems:", file=err)
        for problem in problems:
            print(f"  - {problem}", file=err)
        return
    lines = str(error).splitlines() or [repr(error)]
    print(f"{PROGRAM}: {lines[0]}", file=err)
    for line in lines[1:]:
        print(f"  {line}", file=err)


def main(
    argv: Sequence[str] | None = None,
    *,
    out: TextIO | None = None,
    err: TextIO | None = None,
) -> int:
    """Parse ``argv`` and run one command. Returns the process exit code.

    Declared as the ``duckstream`` console script and called by
    ``python -m duckstream``; ``PLAN.md`` asks for both, because cron in a venv
    usually calls the interpreter directly rather than relying on ``PATH``.

    ``out`` and ``err`` exist so tests can capture output without touching
    global state; they default to the real streams.
    """
    out = out if out is not None else sys.stdout
    err = err if err is not None else sys.stderr
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        return int(args.handler(args, out))
    except DuckstreamError as exc:
        _report(exc, err)
        return EXIT_ERROR
    except KeyboardInterrupt:  # pragma: no cover - interactive only
        print(f"{PROGRAM}: interrupted", file=err)
        return EXIT_INTERRUPTED
