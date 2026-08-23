"""The process the fault-injection tests kill.

Run as ``python _faultchild.py '<spec json>'``. It builds the same engine the
harness builds -- through whichever front door the spec names -- optionally arms
one of :data:`duckstream.FAULT_POINTS` with a hook that calls ``os._exit``, and
drains.

Why a separate process at all, when the engine's fault hooks would let a hook
raise in-process and be caught: because ``PLAN.md`` says the headline claim
"needs real fault injection, not a unit test". An exception unwinds through the
engine's ``except BaseException: self._rollback(); raise``, which is duckstream
politely cleaning up after itself. ``os._exit`` gives it no such chance -- no
``finally``, no ``atexit``, no destructor, no ``ROLLBACK``, no ``close()``. The
DuckDB catalog file is simply abandoned mid-transaction, and whether the next
process sees a consistent lakehouse is then a property of the storage layer and
the transaction boundary rather than of duckstream's error handling.

Exit codes: ``9`` when a fault fired (chosen so it cannot be confused with the
CLI's ``1`` for a duckstream error or ``2`` for a usage error), ``0`` on a clean
drain, anything else is a genuine failure and the test says so.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
for entry in (str(REPO_ROOT), str(HERE)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

FAULT_EXIT = 9


def main(argv: list[str]) -> int:
    spec = json.loads(argv[1])

    import duckdb

    from harness import SETTINGS, Landing, build_model, scenario_from_payload

    scenario = scenario_from_payload(spec["scenario"])
    landing = Landing(Path(spec["landing"]))

    from duckstream import AvailableNow, Engine, Once

    con = duckdb.connect()
    try:
        if spec["door"] == "yaml":
            # Exactly the two calls duckstream.cli._cmd_run makes. The CLI
            # itself cannot arm a fault -- there is no flag and no config key
            # that reaches FaultHooks -- so the child does what the CLI does and
            # then installs the hook on the ordinary Engine it got back.
            from duckstream.config import load_config

            engine = Engine.from_document(load_config(spec["yaml"]), con=con)
        else:
            engine = Engine(
                con,
                catalog=spec["catalog"],
                data_path=spec["data_path"],
                settings=dict(SETTINGS),
            )
            engine.add(build_model(scenario, landing))

        point = spec.get("fault")
        if point:
            fired = {"count": 0}
            nth = int(spec.get("nth", 1))

            def kill(event) -> None:
                fired["count"] += 1
                if fired["count"] < nth:
                    return
                sys.stderr.write(
                    f"FAULT {event.point} model={event.ctx.model_name} "
                    f"batch={event.ctx.batch_id} firing={fired['count']}\n"
                )
                sys.stderr.flush()
                os._exit(FAULT_EXIT)  # no unwinding, no rollback, no close

            engine.faults.install(point, kill)
            assert engine.faults.installed() == [point]

        report = engine.run(trigger=Once() if spec.get("once") else AvailableNow())
    finally:
        con.close()

    print(
        json.dumps(
            {
                "committed": [
                    {"batch_id": r.batch_id, "rows_in": r.rows_in}
                    for r in report
                    if r.committed
                ],
                "empty_passes": sum(1 for r in report if r.is_empty),
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
