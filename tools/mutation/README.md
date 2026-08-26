# The mutation audit

A green suite that tests the wrong invariant is worse than no suite. This is how
duckstream checks that its suite tests what it claims to: deliberate defects are
introduced one at a time, each a *plausible wrong decision* rather than a syntax
error, and every one has to turn the suite red.

```bash
.venv/bin/python tools/mutation/run_audit.py            # all of them  (POSIX)
.venv/bin/python tools/mutation/run_audit.py 3 11 14    # by index
.venv\Scripts\python.exe tools\mutation\run_audit.py    # the same, on Windows
```

The interpreter is now **resolved**, not assumed. It used to be hard-coded to
`.venv/Scripts/python.exe`, which does not exist on Linux — so the first Pi run
launched nothing at all and every mutation came back `ERROR`, which this file
tells you to read as contention. A broken harness and a starved box are not
distinguishable from the outside, so a missing interpreter is now one loud
failure instead of fifty-nine quiet ones.

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
- **not-auditable-here** / **skipped** — the audit saying what it did not test.
  Counting either as red would invent coverage; counting either as a survivor
  would invent a hole. Both are excluded from the denominator and listed by
  name, with their reason. There are two distinct reasons and they are not the
  same thing:
  - `inert_on=("nt",)` — the mutation is **inert** here, because it changes a
    branch this OS does not take. It applies textually and behaves identically.
  - `requires="dirsymlink"` / `requires="paho"` — the mutation is **live** here
    and the *fixture* may not be buildable. The capability is **probed at audit
    time**, the same way the matching test probes it, so the same declaration
    excuses the mutation on a box that cannot run it and audits it on a box
    that can.

    This used to be an unconditional `skip="..."` string, and the word doing
    the work in its reason — "this platform will not create a directory
    symlink" — was **here**, which the implementation never checked. Both
    mutations carrying one were therefore excused on *every* platform for ever,
    including the ones that could build the fixture. That is the same failure a
    stale anchor produces: an audit reporting `skipped` for coverage it could
    have had. When the probe was added and the two ran for the first time, one
    of them (`manual_ack is never set`) **survived** — a real hole, in the one
    line that carries the whole phase-5 acknowledgement guarantee.

    A bare `skip="..."` still works, for anything genuinely un-auditable
    everywhere. Prefer `requires`.
- **held** — a mutation that had to survive, and did. Set `expect_survives` to a
  sentence saying why. There is one: widening the tier-three file index. The
  index is a *hint* (`CONTEXT.md` 1.13), so making it select more files must
  change no answer at all — a red there would prove the suite had started
  depending on the hint for correctness rather than for cost, which is the one
  thing that measurement says must never happen. It is reported apart from both
  red and SURVIVED, and excluded from the denominator, so it can never be
  miscounted as either. `HINT-BECAME-TRUTH` is the failure of that assertion.

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

**Too *expensive* a suite reports a false hole, which looks identical to a real
one.** Three of tier three's four first-round survivors were this. Two structural
reasons worth knowing, because both will recur:

- a defect in a path no conformance scenario reaches is inert there. Every file
  in a conformance scenario has a real time column, so none of them is ever
  *unmeasured*, so a defect in the unmeasured-file path cannot show up;
- a defect the conformance suite is *designed* not to see is inert there by
  construction. Chunked-equals-unchunked is an assertion that chunk sizing does
  not change the answer — so a change to how chunks are sized is precisely what
  that suite must not be able to detect. Only a unit test on the planner can.

**And a mutation that survives may simply not do what its name says.** That has
now happened three times in this project. Removing one half of a redundant pair
of guards leaves the behaviour intact; a mutation written immediately after
fixing the bug it pins tends to inherit the fix's shape rather than test it. Read
a survivor as *"is the suite holed, or is this mutation weaker than it reads?"*
— and if you wrote it minutes after the fix, assume the second until you have
checked.

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

Four workers by default, and **turn it down when the box is busy**:

```bash
DUCKSTREAM_AUDIT_WORKERS=2 .venv/Scripts/python.exe tools/mutation/run_audit.py
```

The suites set `threads=2` to approximate a Pi and DuckLake commits are
disk-bound, so oversubscribing turns a wall-clock win into contention — and into
timing-sensitive tests failing for reasons that have nothing to do with the
mutation. **Do not run anything else heavy alongside it**: one conformance run
started during an audit had a CLI subprocess starved past its 300-second timeout
and failed for no reason at all.

**And the failure mode is worse than slow, which is why the knob exists.** A
starved suite is killed at its budget and reported as `ERROR` — which is exactly
how a suite that genuinely *hangs* is reported, and that is a real finding this
audit is meant to be able to make (one mutation once put the engine into an
infinite loop, and that is what produced `Engine._require_recorded`). The two
must not be confusable. On a loaded box here, **nine** mutations came back
`ERROR` and every one of them was red when re-run at two workers; all nine were
on the `conf` and `all` suites, while every `fast` one finished cleanly.

So: an `ERROR` on an expensive suite is contention until proven otherwise.
Re-run those indices alone before recording anything about them —
`run_audit.py` takes indices, so it is cheap to do.

### On a Raspberry Pi, contention can produce a **false red**, which is worse

`/tmp` on Raspberry Pi OS is **tmpfs**, capped at 2 GB of RAM. Both the audit's
worktrees *and* every `tmp_path` the suite uses land there, so on a Pi the audit
competes with the suite under test for memory — a hazard that does not exist on
the Windows box, where `%TEMP%` is on disk. Measured during the first Pi run:
**one `all` suite occupies ~700 MB of tmpfs**, so two concurrent expensive
suites sit at ~1.4 GB of a 2.0 GB cap, with system RAM down to ~1.1 GB free.

That matters because it changes the failure mode. On the dev box a starved
suite is *killed at its budget* and reported `ERROR` — visible, and this file
tells you to re-run it. If tmpfs fills instead, tests fail with `ENOSPC` or the
allocator gives up, the suite exits non-zero, and the mutation is recorded
**red**. A false `ERROR` is loud and gets re-run; a false **red** is silent and
inflates the count with coverage nobody demonstrated.

Two mitigations, either of which is enough:

```bash
TMPDIR=/var/tmp/duckstream-audit DUCKSTREAM_AUDIT_WORKERS=2 \
    .venv/bin/python tools/mutation/run_audit.py     # temp on disk, not RAM
DUCKSTREAM_AUDIT_WORKERS=1 .venv/bin/python tools/mutation/run_audit.py
```

And on any Pi run, **verify the reds rather than counting them**: every result
carries `first_failures`, so a red whose failing test has nothing to do with
what the mutation changed is contention, not coverage.
