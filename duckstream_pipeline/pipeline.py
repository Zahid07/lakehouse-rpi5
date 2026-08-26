"""The pipeline: drain the landing tree, then maintain the star schema.

One process, one cycle:

    ATTACH  ->  duckstream drains (fact + both marts)  ->  SCD2  ->  DETACH  ->  sleep

Both halves run inside **one** attached session, which is what the daemon's
`drain` seam is for. That gives the dimension the same catalog lock the engine
already holds, so the whole cycle is single-writer, and it costs no second
`ATTACH`.

Releasing the catalog every cycle is the design rather than an optimisation.
`CONTEXT.md` 1.25 measured that a second process cannot `ATTACH` a DuckLake
catalog even `READ_ONLY`, and even while the holder sits idle -- so a daemon
that never let go would lock you out of your own warehouse for as long as it
ran. Measured on this Pi: at a two-second interval an outside reader got in on
**33 of 38 attempts**.

Run it::

    ./run.sh                       # every 2 seconds, until SIGINT/SIGTERM
    ./run.sh --once                # a single cycle, then exit
    python -m duckstream_pipeline.pipeline --interval "5 seconds"
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Importable as `duckstream_pipeline.pipeline` from the repo root, and runnable
# as a plain script from inside the folder. Neither is worth breaking over an
# import path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from duckstream import AvailableNow, Engine  # noqa: E402
from duckstream.daemon import ProcessingTime  # noqa: E402
from duckstream.errors import BatchFailed  # noqa: E402

from duckstream_pipeline import dimensions  # noqa: E402

HERE = Path(__file__).resolve().parent
CONFIG = HERE / "models.yaml"


def defaults() -> None:
    """Fill in the `${VAR}` the document expects, if the caller has not."""
    root = os.environ.setdefault(
        "DS_ROOT", str(Path.home() / "duckstream-accel")
    )
    os.environ.setdefault("DS_LANDING", str(Path(root) / "landing"))
    os.environ.setdefault("DS_MEMORY_LIMIT", "1200MB")
    Path(os.environ["DS_LANDING"]).mkdir(parents=True, exist_ok=True)


def build_engine() -> Engine:
    """A fresh engine per cycle, with the dimension tables in place.

    Fresh rather than reused, and `CONTEXT.md` 1.10 and 1.11 are why: both
    memoise per-model state -- the batch id and the committed watermark -- and
    both say to revisit that "the moment a second writer exists". Detaching
    between cycles creates exactly that window, so starting each cycle with
    empty caches is the sound choice rather than merely the cheap one.
    """
    engine = Engine.from_config(CONFIG)
    dimensions.ensure(engine.con, alias=engine.alias)
    return engine


def drain(engine: Engine):
    """One cycle: duckstream first, then the star schema, then the views.

    Order matters. `location_hlp` reads distinct locations out of
    `curated.fact_accelerometer`, so the fact has to be loaded before the
    dimension can learn what is in it. A location that arrives in this cycle
    therefore reaches the dimension in this cycle too, not the next one.

    `BatchFailed` is unwrapped rather than propagated: every model has already
    had its turn by the time it is raised, so it is the run's *verdict*, not an
    interruption. Letting it escape would make the daemon count a quarantined
    batch as a crashed cycle and spend it against the give-up budget -- for
    data the pipeline handled exactly as designed.
    """
    try:
        report = engine.run(trigger=AvailableNow())
    except BatchFailed as exc:
        report = exc.report

    # Charged every cycle even when nothing landed. Both are cheap and
    # idempotent -- the dimension inserts only genuinely new locations, and the
    # views write no data -- and paying them unconditionally means a cycle that
    # committed nothing still leaves the warehouse consistent.
    dimensions.maintain(engine.con, alias=engine.alias)
    dimensions.create_views(engine.con, alias=engine.alias)
    return report


def notify(url: str | None, cycle) -> None:
    """Tell the dashboard a cycle has finished, so it can refresh now.

    Push rather than let it poll, and the reason is about the lock rather than
    about freshness. A dashboard on a timer picks its moment blindly and can
    attach mid-write -- which on a single-attach catalog is not a slow read, it
    is a **failed pipeline cycle**. This call happens after
    :meth:`Engine.close`, so the catalog is already released: the read it
    triggers is the one read guaranteed not to collide.

    Best-effort in every direction. A dashboard that is down, slow or absent
    must never affect the pipeline, so this swallows everything and uses a
    short timeout. The server keeps a slow fallback timer for exactly the case
    where this call never arrives.
    """
    if not url:
        return
    try:
        import urllib.request

        request = urllib.request.Request(url, data=b"", method="POST")
        with urllib.request.urlopen(request, timeout=1.0):
            pass
    except Exception:  # noqa: BLE001 - a dashboard is never worth a cycle
        pass


def report_cycle(cycle) -> None:
    """One line per cycle, plus a line for anything that is not healthy.

    The first version printed only models that **committed**, so a model that
    failed, quarantined, halted or was waiting out a backoff produced no output
    at all -- it simply vanished from the log while its neighbours kept
    reporting rows. That is the failure `STATUS.md` describes as "a cron log
    that ends up reassuring about a pipeline that is stuck", and it hid a real
    finding here for two runs: `accel_minute_spectrum` disappearing from a
    cycle looked like an absence rather than an event.

    Every model now accounts for itself every cycle: committed, empty, or
    unhealthy — and unhealthy gets its own line, with the reason.
    """
    if cycle.error is not None:
        print(f"cycle {cycle.cycle}: FAILED {cycle.error}", flush=True)
        return

    parts: list[str] = []
    problems: list[str] = []
    if cycle.report is not None:
        for name in cycle.report.model_names:
            results = cycle.report.for_model(name)
            committed = [r for r in results if r.committed]
            if committed:
                rows = sum(r.rows_in or 0 for r in committed)
                out = sum(r.rows_out or 0 for r in committed)
                parts.append(f"{name} {rows}->{out}")
            elif all(getattr(r, "is_empty", False) for r in results):
                parts.append(f"{name} -")
            for result in results:
                if result.committed or getattr(result, "is_empty", False):
                    continue
                problems.append(
                    f"    {name}: {getattr(result, 'outcome', 'failed')} "
                    f"(attempt {getattr(result, 'attempt', '?')}) "
                    f"{getattr(result, 'error', '')}"
                )

    summary = ", ".join(parts) if parts else "idle"
    flag = "  OVERRUN" if cycle.overran else ""
    marker = "  <-- UNHEALTHY" if problems else ""
    print(
        f"cycle {cycle.cycle}: {summary} "
        f"({cycle.seconds * 1000:.0f} ms){flag}{marker}",
        flush=True,
    )
    for line in problems:
        print(line, flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--interval", default="2 seconds",
                        help="time between cycles (default: '2 seconds')")
    parser.add_argument("--once", action="store_true",
                        help="run a single cycle and exit")
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument(
        "--notify", metavar="URL", nargs="?", default=None,
        const="http://localhost:8080/api/refresh",
        help=(
            "poke the dashboard after each cycle so it refreshes at the one "
            "moment the catalog is provably free, instead of polling blindly. "
            "Bare --notify uses http://localhost:8080/api/refresh. "
            "Best-effort: a dashboard that is down neither delays nor fails "
            "the pipeline."
        ),
    )
    args = parser.parse_args(argv)

    defaults()
    print(f"landing : {os.environ['DS_LANDING']}")
    print(f"catalog : {os.environ['DS_ROOT']}/catalog.ducklake")

    if args.once:
        engine = build_engine()
        try:
            report = drain(engine)
        finally:
            engine.close()
        notify(args.notify, None)
        for name in report.model_names:
            committed = [r for r in report.for_model(name) if r.committed]
            rows = sum(r.rows_in or 0 for r in committed)
            out = sum(r.rows_out or 0 for r in committed)
            print(f"  {name}: {len(committed)} batch(es), {rows} in, {out} out")
        return 0

    schedule = ProcessingTime(args.interval, max_cycles=args.max_cycles)
    print(schedule.describe())
    if args.notify:
        print(f"notify  : {args.notify} after each cycle")

    def on_cycle(cycle) -> None:
        report_cycle(cycle)
        # After `Engine.close`, so the catalog is already released and the
        # refresh this triggers cannot conflict with a write.
        notify(args.notify, cycle)

    result = schedule.run(
        factory=build_engine, drain=drain, on_cycle=on_cycle
    )
    print(result.describe())
    return 0 if result.errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
