# The mutation audit

A green suite that tests the wrong invariant is worse than no suite. This is how
duckstream checks that its suite tests what it claims to: deliberate defects are
introduced one at a time, each a *plausible wrong decision* rather than a syntax
error, and every one has to turn the suite red.

```bash
.venv/Scripts/python.exe tools/mutation/run_audit.py            # all of them
.venv/Scripts/python.exe tools/mutation/run_audit.py 3 11 14    # by index
```

Results stream as JSON, one line per mutation, and land in `audit_results.json`
beside the scripts.

## Why it runs in worktrees

Every earlier audit in this project rewrote package files **in the working
tree**, which meant nothing could be committed while one ran. That is a rule
somebody has to remember, and it had already been broken once here: a commit
taken mid-audit captured a deliberately mutated `engine.py` and pushed it. Only
the audit's own hash check caught it, because the mutation is restored before
the next one starts and `git status` looks clean by then.

Each mutation now gets a private `git worktree`, with the current `duckstream/`
and `tests/` copied in. `pytest`'s `pythonpath = ["."]` puts that copy ahead of
the editable install — verified, not assumed — so the suite under test is the
mutated one and the real checkout is never written to. There is nothing to
restore and no integrity check to forget to run.

## Reading the output

Three verdicts, kept apart on purpose:

- **red** — the suite failed. What you want.
- **SURVIVED** — the suite passed with a defect in place. Read it twice: the
  first question is whether the suite has a hole, the second is whether the
  mutation tested what its name claimed. Both have happened here.
- **not-auditable-here** — the mutation is inert on this platform, because it
  changes a branch this OS does not take. Counting these as red would invent
  coverage; counting them as survivors would invent a hole. They are excluded
  from the denominator and listed by name.

`ERROR` and `TIMEOUT` are neither. A mutation that makes the engine loop for
ever produces a timeout, and **a suite that hangs has not caught anything** —
that outcome is a finding about the suite, not a pass. One mutation did exactly
this and it is what produced `Engine._require_recorded`.

## Adding one

Append to `MUTATIONS` in `mutations.py`. `find` must appear **exactly once** in
`file`; the runner refuses otherwise, because an anchor that matches nothing is
reported as *skipped* and a count that folds skips in with the reds claims
coverage that does not exist. That has happened here too.

Pick the cheapest `suite` that should catch it — `fast`, `conf` or `all`.
Choosing too cheap a suite is another way to report a false green, so anything
about snapshot atomicity or DuckLake behaviour goes on a conformance run.

Verify the anchors before a long run:

```bash
.venv/Scripts/python.exe -c "
import pathlib, sys; sys.path.insert(0, 'tools/mutation')
from mutations import MUTATIONS
for i, m in enumerate(MUTATIONS):
    for s in [m] + ([m['also']] if m.get('also') else []):
        n = pathlib.Path(s['file']).read_text(encoding='utf-8').count(s['find'])
        if n != 1: print('stale anchor', i, n, m['name'])
print(len(MUTATIONS), 'mutations')"
```

## Concurrency

Four workers by default. The suites set `threads=2` to approximate a Pi and
DuckLake commits are disk-bound, so oversubscribing turns a wall-clock win into
contention — and into timing-sensitive tests failing for reasons that have
nothing to do with the mutation. **Do not run anything else heavy alongside it**:
one conformance run started during an audit had a CLI subprocess starved past
its 300-second timeout and failed for no reason at all.
