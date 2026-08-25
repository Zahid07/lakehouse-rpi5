"""Run one mutation, in a throwaway git worktree, and say whether it turned red.

Why a worktree rather than the working tree, which is how every previous audit
in this project was run: the old way rewrites package files in place, so nothing
may be committed while it runs. That is not a rule anyone can be relied on to
remember -- it has already been broken once here, pushing a deliberately mutated
`engine.py` that only the audit's own hash check caught, because the mutation is
restored before the next one starts and `git status` looks clean by then.

A worktree makes the hazard structurally impossible instead of procedurally
forbidden. The mutation is applied to a private checkout, the suite runs there
with `pythonpath = ["."]` putting that copy ahead of the editable install
(verified, not assumed), and the main tree is never written to at all. There is
consequently nothing to restore and no integrity check to forget to run.

Usage:  python audit.py <index> <worktree-dir>
Prints one JSON object on stdout.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from mutations import MUTATIONS  # noqa: E402

PYTHON = str(pathlib.Path(__file__).resolve().parents[2] / ".venv" / "Scripts" / "python.exe")
SUITES = {
    "fast": ["-m", "not conformance"],
    "conf": ["-m", "conformance"],
    "all": [],
}


def apply(root: pathlib.Path, spec: dict) -> None:
    path = root / spec["file"]
    text = path.read_text(encoding="utf-8")
    found = text.count(spec["find"])
    if found != 1:
        raise SystemExit(
            f"anchor matched {found} times in {spec['file']}, expected once. "
            f"A mutation that matches nothing is reported as skipped, not red, "
            f"and a count that folds it in with the reds claims coverage that "
            f"does not exist."
        )
    path.write_text(text.replace(spec["find"], spec["repl"]), encoding="utf-8", newline="\n")


def main() -> None:
    index = int(sys.argv[1])
    root = pathlib.Path(sys.argv[2])
    spec = MUTATIONS[index]

    import os

    if os.name in spec.get("inert_on", ()):
        # Applied textually, semantically a no-op on this platform. Neither red
        # nor a survivor, and saying so beats folding it into either count.
        print(json.dumps({"index": index, "name": spec["name"],
                          "verdict": "not-auditable-here",
                          "why": f"inert on os.name={os.name!r}"}))
        return
    if spec.get("skip"):
        print(json.dumps({"index": index, "name": spec["name"], "verdict": "skipped",
                          "why": spec["skip"]}))
        return

    apply(root, spec)
    if spec.get("also"):
        apply(root, spec["also"])

    # A generous multiple of the honest runtime, not an hour. A mutation that
    # makes the engine loop for ever is a real outcome worth reporting quickly
    # -- one did, on the first pass -- and a timeout measured in tens of minutes
    # turns that into a stalled audit instead of a finding.
    suite = spec.get("suite", "fast")
    budget = {"fast": 420, "conf": 900, "all": 1200}[suite]
    result = subprocess.run(
        [PYTHON, "-m", "pytest", "-q", "-x", "--no-header", "-p", "no:cacheprovider",
         *SUITES[suite]],
        cwd=root, capture_output=True, text=True, timeout=budget,
    )
    tail = (result.stdout or "").strip().splitlines()
    failing = [line for line in tail if line.startswith("FAILED")]
    went_red = result.returncode != 0

    # A mutation that must NOT turn the suite red. Rare, and worth having: the
    # tier-three file index is a *hint*, so widening it has to leave every
    # answer unchanged. A red there would not be coverage, it would be proof
    # that the suite had started depending on the hint for correctness rather
    # than for cost -- exactly the property CONTEXT.md 1.13 says must never be
    # depended on. Reported under its own verdicts so it can never be counted
    # as an ordinary survivor.
    if spec.get("expect_survives"):
        verdict = "held" if not went_red else "HINT-BECAME-TRUTH"
    else:
        verdict = "red" if went_red else "SURVIVED"

    print(json.dumps({
        "index": index,
        "name": spec["name"],
        "suite": spec.get("suite", "fast"),
        "verdict": verdict,
        "returncode": result.returncode,
        "first_failures": failing[:4],
        "summary": tail[-1] if tail else "",
        **({"why": spec["expect_survives"]} if spec.get("expect_survives") else {}),
    }))


if __name__ == "__main__":
    main()
