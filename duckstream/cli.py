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
            _describe_source(model),
            _describe_sink(model),
        )
        for model in document.models
    ]
    headers = ("MODEL", "TIER", "STRATEGY", "SOURCE", "SINK")
    for line in _table(headers, rows):
        print(line, file=out)
    return EXIT_OK


def _cmd_run(args: argparse.Namespace, out: TextIO) -> int:
    from duckstream.config import load_config
    from duckstream.engine import Engine
    from duckstream.trigger import AvailableNow, Once

    document = load_config(args.config)
    trigger = Once() if args.once else AvailableNow()
    engine = Engine.from_document(document)
    try:
        report = engine.run(trigger=trigger, model=args.model)
    finally:
        engine.close()

    for name in report.model_names:
        results = report.for_model(name)
        committed = [r for r in results if r.committed]
        if not committed:
            print(f"{name}: nothing to do", file=out)
            continue
        rows = sum(r.rows_in or 0 for r in committed)
        batches = len(committed)
        plural = "" if batches == 1 else "es"
        last = committed[-1]
        print(
            f"{name}: {batches} batch{plural}, {rows} source rows, "
            f"through batch {last.batch_id}",
            file=out,
        )
    return EXIT_OK


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
