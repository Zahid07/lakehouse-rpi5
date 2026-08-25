"""Drive the whole mutation audit in parallel, one throwaway worktree each.

Serial, in the working tree, is how every previous audit here was run, and it
cost this project a pushed defect once. This version cannot: each mutation gets
a private `git worktree`, so the main checkout is never written to and there is
nothing to restore afterwards.

Concurrency is deliberately modest. The suites set `threads=2` to approximate a
Pi and DuckLake commits are disk-bound, so oversubscribing turns a wall-clock
win into contention -- and, worse, into timing-sensitive tests failing for a
reason that has nothing to do with the mutation.
"""

from __future__ import annotations

import concurrent.futures as cf
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[1]
PYTHON = REPO / ".venv" / "Scripts" / "python.exe"
# Outside the repository, deliberately. A worktree under `tools/` shows up in
# `git status` as untracked, which is the same "something the audit left in your
# checkout" hazard this design exists to remove -- one `git add -A` away from
# committing a tree full of mutated copies of the package.
WORKTREES = pathlib.Path(tempfile.gettempdir()) / "duckstream-mutation-worktrees"
WORKERS = 4

sys.path.insert(0, str(HERE))
from mutations import MUTATIONS  # noqa: E402


def run_one(index: int) -> dict:
    root = WORKTREES / f"m{index:02d}"
    if root.exists():
        subprocess.run(["git", "worktree", "remove", "--force", str(root)],
                       cwd=REPO, capture_output=True, text=True)
        shutil.rmtree(root, ignore_errors=True)
    made = subprocess.run(
        ["git", "worktree", "add", "--detach", "-q", str(root), "HEAD"],
        cwd=REPO, capture_output=True, text=True,
    )
    if made.returncode != 0:
        return {"index": index, "name": MUTATIONS[index]["name"],
                "verdict": "ERROR", "summary": made.stderr.strip()[:300]}

    # The worktree is at HEAD, which predates this change. Copy the working
    # tree's own duckstream/ and tests/ over it, so the audit tests the code
    # that is about to be committed rather than the code that already was.
    for sub in ("duckstream", "tests"):
        shutil.rmtree(root / sub, ignore_errors=True)
        shutil.copytree(REPO / sub, root / sub,
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))

    started = time.time()
    try:
        proc = subprocess.run(
            [str(PYTHON), str(HERE / "audit.py"), str(index), str(root)],
            capture_output=True, text=True, timeout=1500,
        )
        if proc.returncode != 0:
            out = {"index": index, "name": MUTATIONS[index]["name"],
                   "verdict": "ERROR",
                   "summary": (proc.stderr or proc.stdout).strip()[-400:]}
        else:
            out = json.loads(proc.stdout.strip().splitlines()[-1])
    except subprocess.TimeoutExpired:
        out = {"index": index, "name": MUTATIONS[index]["name"],
               "verdict": "TIMEOUT", "summary": ""}
    finally:
        subprocess.run(["git", "worktree", "remove", "--force", str(root)],
                       cwd=REPO, capture_output=True, text=True)
        shutil.rmtree(root, ignore_errors=True)
    out["seconds"] = round(time.time() - started, 1)
    return out


def main() -> None:
    WORKTREES.mkdir(parents=True, exist_ok=True)
    only = [int(a) for a in sys.argv[1:]] or list(range(len(MUTATIONS)))
    results: list[dict] = []
    # as_completed, not map: map yields in submission order, so one slow
    # mutation hides every finished result behind it and the run looks stalled
    # when it is not. On the first pass that hid ten completed results behind a
    # mutation that had put the engine into an infinite loop.
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(run_one, i): i for i in only}
        for future in cf.as_completed(futures):
            out = future.result()
            results.append(out)
            print(json.dumps(out), flush=True)

    results.sort(key=lambda r: r["index"])
    (HERE / "audit_results.json").write_text(
        json.dumps(results, indent=2), encoding="utf-8"
    )

    # Three categories, kept apart on purpose. "skipped" and
    # "not-auditable-here" are the audit being honest about what it did not
    # test; folding either into the denominator would claim coverage that does
    # not exist, which is the exact mistake recorded in STATUS.md.
    red = [r for r in results if r["verdict"] == "red"]
    excused = [r for r in results
               if r["verdict"] in ("skipped", "not-auditable-here")]
    # A "held" mutation is one that had to survive, and did -- see
    # `expect_survives` in audit.py. It is a passing assertion, not a defect and
    # not a red, so it gets its own line rather than either denominator.
    held = [r for r in results if r["verdict"] == "held"]
    bad = [r for r in results
           if r["verdict"] not in ("red", "held", "skipped", "not-auditable-here")]
    denominator = len(results) - len(excused) - len(held)
    print(f"\n{len(red)}/{denominator} turned the suite red")
    if held:
        print(f"  {len(held)} had to survive, and did (the hint stayed a hint):")
        for r in held:
            print(f"    held: {r['name']}")
    for r in excused:
        print(f"  not tested here: {r['name']} -- {r.get('why', '')}")
    for r in bad:
        print(f"  {r['verdict']}: {r['name']}")
    # The main tree must be exactly as it was. Belt and braces on top of the
    # worktree isolation, because "the audit did not touch it" is the one claim
    # that has been wrong here before.
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=REPO,
                           capture_output=True, text=True).stdout
    print("\nmain tree after the audit:")
    print(dirty or "  (clean)")


if __name__ == "__main__":
    main()
