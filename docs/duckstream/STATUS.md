# duckstream — status and handover

**Start here if you are a new session.** This file says where the work stands, what
is proven, what is not, and what to do next. It is current as of **2026-08-25**.

Read in this order:

1. **this file** — where things are
2. `CONTEXT.md` — measured constraints and settled decisions. **Twenty-two**
   measured constraints; §1.8, §1.9 and §1.10 were measured during the phase-1
   build, §1.11 during phase 2, §1.12 during the phase-2b cleanup, §1.16 during
   phase 4, §1.17–§1.19 during tier three and §1.20 during phase 4's scan work,
   and none of them is derivable from anything else. **§1.20's first draft was
   withdrawn** — read it for what a non-interleaved measurement on this box is
   worth. §1.10, §1.11 and §1.12 are the *same* constraint met three
   times in three disguises — read them together, read §1.16 as the fourth and
   **§1.17 as the fifth**. §1.16 also corrects two numbers in §1.15, which had
   been derived rather than measured
3. `PLAN.md` — the specification: what to build, phase by phase
4. `BUILD_GRAPH.md` — the decision record: frozen interfaces, every ratified
   decision with its reasoning, and the notes each task left for the next

**Phases 1, 2, 2b, 3 and 5 are complete. Phase 4 is under way**: its first item
(the consumed set as rows) is done and the landing-tree scan is now 3–9x
cheaper, but retention and compaction are not. Phase 6 is not started.

**Phase 3 is finished: all three tiers execute.** Tier two is maintained through
sufficient statistics — `(n, mean, M2)`, not `(sum, sum_sq, count)` — and proved
against the production bug in `CONTEXT.md` §4. **Tier three now executes too**: a
`recompute_window` model re-derives every window a batch touched, reading back
exactly the consumed files that can hold a row in it, in chunks sized from an
estimated row count. That was the last of the differentiator and the reason to
build this rather than adopt Arroyo.

Two of tier three's design points were settled by measurements taken while
building it, and both corrected what this file previously told the next session
to do — see §1.17, §1.18 and "What measurement changed" below.

Phase 4's first item — the one `PLAN.md` says outranks compaction — is complete:
**the file source's consumed set is rows, not a JSON cell.** §1.16 measured
7.97 MB of writes per trigger becoming 4.9 KB, and a 1,078 ms commit becoming
13.8 ms, both flat in the number of files consumed.

Phase **2b** was not in `PLAN.md` when phases 1 and 2 were built. It was added
afterwards, because the gap it closes was a gap in the plan: the framework had a
demonstrated answer for what happens when the *process* dies and none at all for
what happens when the *data* will not process.

---

## Where to start

**The Raspberry Pi.** Every number in `CONTEXT.md` is a Windows dev box with
`threads=2` as a Pi proxy, and that proxy is defensible for DuckDB work and
*not* for the two most recent constraints: §1.20 measured the landing scan as
**81% Python path manipulation**, and §1.22 measured a Python UDF reaching only
1.31x on four threads. Both predict the Pi behaves *worse* than this box, which
is the opposite of the usual caveat and is testable.

Standing the current build up there also closes coverage that cannot be closed
here at all:

| | this box | Pi (Linux) |
|---|---|---|
| suite | 1,258 pass, **3 skip** | 3 skips become **0** |
| audit | 3 excused as un-auditable | all three become auditable |

The three are a case-fold branch Windows never takes, a directory symlink
Windows will not create without a privilege, and `paho-mqtt` — which phase 5
made worth installing.

Practical checks before starting: a `linux_arm64` wheel for `duckdb==1.5.5` on
the Pi's Python, and `INSTALL ducklake` once while it has network (§1.7). Both
are day-one problems if they are problems at all.

**Then: retention at the source — the rest of phase 4.** Nothing is half-built:
the scan work below is finished and audited, and phase 5 shipped whole.

After that, only **phase 6** remains — validation on real hardware, the soak,
and release discipline. Everything `PLAN.md` names as code is written.

**The scan is already 3–9x cheaper, and that is a constant factor, not the
fix.** §1.20 profiled `latest_offset()` and found it was never I/O-bound: 81% of
it was Python path manipulation above a 4.7 µs `stat`, with `normcase` called
160,000 times for 2,000 files and every directory `scandir`-ed twice. That is
fixed, with no semantic change. But the scan is **still linear in ready files**
and still paid on every trigger including idle ones, so bounding the number of
files is still the structural answer.

Two things put retention ahead of compaction, and the second is new:

- §1.16 left it a *cost* problem rather than a card-wear one, which moved it
  down the list. Tier three moves it back up, and §1.19 is the number: a
  recompute costs **~17.5 ms plus ~0.14 ms per file in the window**, so a window
  fed by a hundred small files costs nearly twice one fed by ten. Meanwhile
  `latest_offset()` still walks the whole landing tree every trigger at §1.13's
  ~0.1 ms per file. Fewer files is the only lever that helps both, and it is now
  the difference between a recompute that is affordable and one that is not.
- Compaction of the *sink* is still wanted for the reason it always was —
  inlining is off, so every trigger writes a parquet file — and `PLAN.md` phase
  4 also names partitioning sink tables by time grain, which is what makes a
  recomputed window range prune to a few files instead of scanning the mart.
  That one is worth more now than when it was written, because tier three's
  clear-then-insert touches the mart by window range on every trigger.

`PLAN.md` phase 4 additionally names scheduling `prune()`, a migration path for
a renamed aggregate, and compaction of the open-window accumulator. **`prune`
must never be pointed at `consumed_files`** — trap 19, and now doubly so: those
rows are the position *and* they carry the time-range index tier three selects
files with.

One thing to know before touching maintenance: `duckstream.consumed_files` now
carries `min_ts`, `max_ts` and `n_rows`, and anything that rewrites those rows
must carry them across. Losing them is not a correctness failure — an unmeasured
file is stored at the widest range, so it is read by *every* recompute rather
than by none — but it would quietly turn each recompute back into a full scan,
which is §1.13's cost arriving through the maintenance meant to reduce it.

Everything deferred out of the phase-2b sweep was written **explicitly into the
phase that owns it** rather than left as a note here: `PLAN.md` phase 4 now names
scheduling `prune()`, a migration path for a renamed aggregate, and compaction of
the open-window accumulator; phase 6 now names a soak run on real hardware, the
two unmeasured numbers, and release discipline.

## Environment

```bash
cd d:\lakehouse-rpi5
.venv\Scripts\python.exe -m pytest -q                        # 1258 passed, 3 skipped, ~6m
.venv\Scripts\python.exe -m pytest -q -m "not conformance"   # 1120 passed, 3 skipped, ~60s
.venv\Scripts\python.exe -m pytest -q -m conformance         # 137 passed, 1 skipped, ~5m
.venv\Scripts\duckstream.exe --help                          # the console script is installed
```

Never edit a package file while the suite is running. **The mutation audit no
longer has this problem**: it runs each mutation in a throwaway `git worktree`
rather than in the working tree, so nothing it does can reach your checkout and
there is nothing to restore afterwards. That replaces the old rule — "never
commit while an audit is running" — which had already been broken once here,
pushing a deliberately mutated `engine.py` that only the audit's own hash check
caught. A rule nobody can forget beats a rule nobody must forget.

`.venv/` at the repo root, Python 3.14.3, `duckdb==1.5.5` **pinned exactly** —
constraints §1.5 and §1.7 are version-sensitive and the aggregate classifier reads
DuckDB's AST format. The package is installed editable (`pip install -e .`), which
is what makes the console script exist; without it one conformance test skips.

The expected skips are correct and there are now **three**. Two are one test in
two parametrisations: Windows cannot hold two filenames differing only by case,
so the POSIX half of a case-sensitivity pair cannot run here, and that test runs
against both consumed-set shapes. The third is new and has the same shape for a
different reason — the scan's symlink test needs a **directory symlink**, which
Windows will not create without a privilege this box does not hold. Both gaps
are declared in the mutation audit too, so neither reports as missing coverage. A filtered run (`-m conformance`) skips a
third — the front-door exemption audit, which needs an unfiltered collection to
mean anything and says so.

### Git state

Committed on `feat/duckstream`, newest last:

```
693e691  feat: add duckstream files                              phase 1
143e43b  feat: add pushes for phas 2
ee9e3c0  chore(duckstream): normalise line endings to LF
f501eef  fix(duckstream): flat-cost status, guard hole, unused imports
9351d13  feat(duckstream): tier-2 sufficient statistics, Pi measurements
f8c6e1c  feat(duckstream): Arrow-mode UDF registrars for tier three
7992f95  feat(duckstream): consumed files as rows — phase 4's first item
23c53b4  fix(tools): keep the mutation audit's worktrees out of the repository
```

**Tier three is uncommitted in the working tree** as of this handover; the last
commit is `67b852c`. What is new or changed is listed under "How tier three was
built" and in `BUILD_GRAPH.md`. The full suite is green (1,206 passed, 2 skipped)
and the mutation audit is 39/39 red with one `held` and one not auditable on
Windows, so it is committable as it stands.

**The defect that was in `143e43b` is gone.** That commit was taken while a
mutation audit had `engine.py` mutated and captured a batch judging its own rows
against its own new watermark instead of the committed one. It was fixed by the
commits after it; `git show HEAD:duckstream/engine.py` no longer contains it.
Nothing is uncommitted that should not be — and the audit can no longer cause a
repeat, because it does not touch the working tree at all any more.

## Phase 4, item 1: the consumed-file set as rows

`PLAN.md` phase 4 opens by saying its first item is not compaction — it is the
file source's consumed-file map. Done, and `PLAN.md` now says so in the past
tense.

| Requirement | Status | Evidence |
|---|---|---|
| The consumed set is rows, not a JSON cell | met | `duckstream.consumed_files`; `duckstream/consumed.py` |
| **The per-trigger write collapses** | **met** | §1.16: 7.97 MB → **4.9 KB**, 1,665x; 11.2 GB/day → 6.8 MB/day |
| …and stays flat as the stream ages | met | §1.16: planning ~3.4 ms and commit ~14 ms whether 1,000 files have been consumed or 525,600 |
| A v1 catalog migrates instead of replaying | met | automatic, one transaction, carrying the retry state; three engine-level tests |
| **A v2 offset is never mistaken for an empty one** | **met** | `FileOffset.consumed()` raises; the message names the table and the constraint |
| The rows commit with the output they check point | met | one transaction, and the conformance snapshot walk now reads them `AT (VERSION => n)` |
| Quarantine still means "skip past it" | met | the skip is recorded in the same transaction as the record of it |
| Nothing prunes the position | met | `prune` excludes `consumed_files`, asserted |
| The count and the rows cannot drift | met | refused at write time, and asserted at **every** snapshot in history |

### The four decisions, each forced by something

**The set is rows and the offset is a count** — §1.16, above.

**A v2 offset refuses to hand back an empty map.** The single most important
line in the change. Returning `{}` would read as "this model has consumed
nothing", replay the whole landing tree and fold every row into the mart a
second time: `CONTEXT.md` §4's bug class, arriving as an upgrade note.

**`plan()` gains a keyword and the engine injects it from the signature.**
`Source.plan` is a frozen interface, so a source opts in by *declaring*
`consumed` — the same signature-driven injection phase 1 ratified for
`base_dir`. A third-party source is called exactly as before. The hazard is a
source that wraps a file source and forgets to forward it; that lands on the
refusal above, loudly, and a test pins it.

**Migration is automatic rather than an operator step.** Refusing and asking
somebody to run a command reads as the more careful option and is not: the
obvious manual fix for a refused offset is to delete it, which replays
everything.

### Three things the build surfaced that were on no list

**Quarantine stopped meaning what it said.** Advancing past a bad batch *was*
advancing the offset, because the offset was the set. With rows it is two
writes, and only the second one stops the next trigger re-planning the batch.
Caught by an existing phase-2b test — the same hole that audit found from the
other direction.

**The checkpoint moving is no longer proof of progress.** It used to be: a
changed offset meant new files were in it. It is now a counter the source
computes optimistically, so a source that plans files and declares none would
advance it, record nothing, and be handed the same files for ever — an infinite
loop the drain guard cannot see, because the guard watches the checkpoint and
the checkpoint moves. Now checked twice: the count is verified against the rows
written in the same transaction, and the engine refuses to commit a batch that
recorded nothing.

**`status` lied on an un-migrated catalog.** It never runs a batch, so it never
migrates — and it was reading the empty table, reporting every already-consumed
file as backlog. On the deployment §1.15 is written about that is `backlog:
525600` shown to the person checking whether the upgrade went well, and it goes
out over `--json` to whatever is thresholding on it. Found by review, not by the
audit; fixed by asking the source whether the position has moved yet, and
reporting `consumed_files: None` — "cannot say" — rather than `0`.

### The suite was audited by mutation, and it found two real holes

**26 deliberate defects, 25 auditable on this platform, all 25 turn the suite
red.** The one excluded is honest rather than convenient: it changes the code
path that only a case-sensitive filesystem takes, so on Windows it applies
textually and does nothing. Calling that a survivor would invent a hole; calling
it red would invent coverage. It reports as *not auditable here*.

Getting to 25 took three rounds, and what the rounds found is the point.

**A test that could not test what it was named for.** *"the anti-join ignores
mtime"* survived. The probe is narrowed to `BETWEEN min(scan mtime) AND
max(scan mtime)`, and the test used a **one-file** scan — where that is
`BETWEEN t AND t`, which is exactly the equality it was supposed to be
independent of. Deleting the equality changed nothing the test could see. Now
tested with a scan spanning two mtimes and a file rewritten inside that span.

**A guard that hung instead of failing.** *"a committed batch records no
consumed files"* did not go red — it **timed out**, because it puts the engine
into an infinite drain. A suite that hangs has not caught anything. That
produced the `_require_recorded` check above, and then that check survived its
own mutation, because nothing exercised it; so it was extracted into a named
method a test can reach. Both are red now.

**And two process lessons.** The audit runs in throwaway `git worktree`s, so it
cannot touch the working tree — the "never commit during an audit" rule is now
unnecessary rather than merely restated. And results stream as they complete
rather than in submission order: the first run looked stalled at 10 of 22 for
twenty minutes while ten finished results sat behind the one mutation that had
hung.

**The audit is now in the repository**, at `tools/mutation/`, instead of being
rebuilt from scratch by each session as it has been until now:

```bash
.venv/Scripts/python.exe tools/mutation/run_audit.py          # all of them
.venv/Scripts/python.exe tools/mutation/run_audit.py 3 11 14  # by index
```

`tools/mutation/README.md` covers how to add one, how to read the three
verdicts, and why `ERROR`/`TIMEOUT` is a finding about the suite rather than a
pass. Adding mutations for the next phase is now appending to a list. **Its
anchors will go stale as the code moves** — the README has the one-liner that
checks them, and a stale anchor reports as *skipped*, which is how an audit
claims coverage it does not have.

### Reviewed adversarially as well, and it was worth it

Five independent lenses over the diff, each finding then trying to refute its
own findings. It ran **partially** — five of its twelve agents hit a spend limit,
so the migration and claims lenses never reported — and it still found the
`status` defect above from two directions, with a detail the audit missed:
`status` calls `ensure()`, which *creates* the empty table, so the "table does
not exist" fallback that would have saved it never fires.

## Phase 4, the landing-tree scan: 3–9x, and a withdrawn measurement

`latest_offset()` walks the whole landing tree on every trigger, including idle
ones, and `PLAN.md` said §1.13's ~0.1 ms per file applied to that walk. It does
not. §1.13 measured **DuckDB opening a parquet footer**; this is `os.walk` plus
a `stat` plus a glob match, and carrying one constant across to the other is the
derived-figure mistake §1.15 made, in a new place.

**Profiled instead of assumed, and the walk was never I/O-bound.** At 2,000
files a bare `os.stat()` loop is 4.7 µs a file and `latest_offset()` is 24.3 µs
— **81% of it above the syscall**. The offenders were `ntpath.normcase`,
**160,000 calls for 2,000 files**, reached from `FileOffset.relative_path` once
per file; and `nt.scandir` called **twice per directory**, once inside `os.walk`
and again to list the files it had just read.

| Files | Per directory | Before | After | |
|---|---|---|---|---|
| 2,000 | 100 | 59.5 ms | 7.0 ms | **8.5x** |
| 2,000 | 1 *(duckstream's real shape)* | 220.2 ms | 72.1 ms | **3.0x** |
| 5,000 | 50 | 147.7 ms | 16.3 ms | **9.0x** |

One `scandir` per directory, the relative prefix carried down the walk instead
of recomputed per file, size and mtime read off the `DirEntry` the walk already
fetched, and the marker's stat taken from that same listing. **No semantic
change**, which is the entire design constraint — so the tests defend that it
stayed one: the prefix join is asserted equal to `FileOffset.relative_path` on a
nested tree, `DirEntry.stat()` equal to `Path.stat()`, and marker gating,
non-recursive mode and symlink handling unchanged.

It is a **constant factor, not a fix.** The scan is still linear in ready files.
Retention is still the structural lever.

### A measurement was published and then withdrawn

The first version of §1.20 reported per-file constants across five tree sizes
and concluded "minutes per trigger". **Those numbers are gone.** They were taken
one configuration after another rather than interleaved, and a later run
disagreed with them by **7x on the same tree and the same shape** — one pass
said tree shape barely mattered, the next said it mattered enormously, and the
second was right.

§1.11 already says to interleave anything on this box that takes minutes. The
lesson that is *new* is which kinds of number survive that mistake: **a profile
is a ratio measured within one run, so drift cannot flatter it**, and the 81%
and the 160,000 calls stood while every absolute figure around them fell over.
When this box is noisy, measure ratios and measure interleaved — or measure
nothing.

There is a pleasant corollary. This is CPU, not I/O, so it transfers to a Pi
*better* than a DuckDB measurement would: a Pi's cores are slower, so Python
path manipulation costs it more, while `stat` on local storage is comparable.
That is the opposite of this project's usual caveat.

## Phase 5: MQTT, as the landing writer it has to be

`CONTEXT.md` section 4 settled this before phase 1 was built and shipping it did
not change it: **MQTT cannot be a source.** Once a message is acked it is gone
from the broker, so there is no offset to resume from and nothing to replay.
`type: mqtt` on a model is still refused — what changed is that the message now
names the thing that exists.

```
broker  ->  MqttLandingWriter  ->  landing/  ->  file source  ->  engine
              at least once                       exactly once
```

| Requirement | Status | Evidence |
|---|---|---|
| At-least-once into durable storage | met | tokens released only after the marker is on disk; 22 unit tests |
| **Nothing is acked before it is durable** | **met** | the inverse of `paho`'s default *and* of this repo's own `subscriber.py` |
| Replayable downstream | met | four conformance scenarios through both doors, diffed against a recompute of the landing tree |
| A crash before the marker is invisible | met | unmarked directory, never read, never mistaken for a short batch |
| A failed write is a delay, not a loss | met | buffer kept, nothing acked, next flush retries |
| Duplicates are visible, not hidden | met | a redelivery is counted twice and asserted to be |
| No MQTT dependency unless you use MQTT | met | `duckstream[mqtt]`, lazily imported; the durability half needs no broker to test |

### The design decision that carries it

**Durability lives in `landing.py`, which has no MQTT in it at all.** Everything
that makes the guarantee — the write order, the acknowledgement tokens, the
one-directory-per-flush rule — is in a class that takes dicts. So it is tested on
every machine with no broker, and the adapter over it is thin enough to read in
one sitting. A test that needed a broker would run nowhere and prove it nowhere.

### What at-least-once actually costs, stated rather than implied

A redelivery lands the same reading **twice, in two different files**, and
duckstream does not de-duplicate it. It cannot: the two files genuinely differ
and nothing marks one as a repeat. So exactly-once is over **files**, not over
readings — and a model that cares needs a merge key and `mode='update'`, which
converges to one row per key however many times a reading arrives.

That is asserted in a conformance scenario rather than left in a docstring,
because "exactly-once means my rows cannot be duplicated" is the reading a user
will arrive with.

### The audit, and one caveat on it that is about the box, not the code

**59 deliberate defects. All 55 auditable on this platform land where they
should: 54 turn the suite red, and one had to survive and did.** Three cannot be
audited here at all, and all three become auditable on Linux.

Getting that number took two passes, and the reason is worth keeping. The first
full run returned **nine `ERROR`s** — every one on the expensive `conf` and
`all` suites, while every `fast` one finished cleanly. Re-run at two workers,
**all nine came back exactly as before: eight red and the one `held`.** There
was nothing behind them. They were suites starved past their timeout on a
loaded box.

**That this is worth a paragraph at all is the finding.** A starved suite is
killed at its budget and reported as `ERROR` — which is precisely how a suite
that genuinely *hangs* is reported, and that is a real finding this audit exists
to be able to make; one mutation once put the engine into an infinite loop and
that is what produced `Engine._require_recorded`. The two outcomes must not be
confusable. `run_audit.py` now takes `DUCKSTREAM_AUDIT_WORKERS`, and the README
says an `ERROR` on an expensive suite is contention until proven otherwise.

The three legitimately not audited here are all this platform, not gaps: a
case-fold branch Windows never takes, a directory symlink Windows will not
create without a privilege, and a `paho-mqtt` that is deliberately not installed.
**All three become auditable on the Pi**, along with the three tests that skip
here — which is the concrete reason to run the suite there.

### Two things the build surfaced that were on no list

**`pa.Table.from_pylist` infers its schema from the first record only.** A field
that appears later in a batch is *silently dropped* — the write succeeds, the
file looks fine, the column is not there. A sensor that starts reporting a
battery level half way through a flush would lose it with nothing to say so. The
union of keys is now computed before writing. Found by a test, not by review.

**Two of the three phase-5 mutation survivors were weak *tests*, not holes.**
The ordering test hooked `_write`, but both the rename and the marker happen
after `_write` returns, so it passed with the order reversed. And the
time-trigger test used a **single** buffered record, where "oldest" and "newest"
are the same record — trap 16's shape in a new place. Both now do what their
names say.

## Phase 3, tier three: the recompute

`PLAN.md` gives tier three one sentence — *"recompute the affected window from
source; no shortcut exists"* — and that sentence is the design. A median, an
exact `count(distinct …)`, an order-dependent aggregate or a UDF over a whole
window has **no decomposition**: there is no pair of partial answers that
combine into the true one. So the windows a batch touched are read again, in
full, out of the files they came from.

| Requirement | Status | Evidence |
|---|---|---|
| Tier three executes | met | seven conformance scenarios, both doors, diffed against a full recompute after every drain |
| **§4's FFT bug is impossible** | **met** | one hour over four batches gives `n=16`, `spread=100.0`, `gap=43.0`; a batch-wise fold gives 4, 3.0 and 3.0 |
| A window already written is corrected | met | a later batch replaces its window's row; untouched windows are left byte-identical |
| **Chunked equals unchunked** | **met** | budgets of 1, 2, 3 and unbounded compared against each other, not just against ground truth |
| Window-range chunking sized from estimated rows | met | `recompute.py`; a saturated hour takes a chunk of its own while quiet hours pack together |
| The file → time-range index | met | `min_ts`/`max_ts`/`n_rows` on `consumed_files`; §1.17 |
| …and it is a hint, never truth | met | a mutation that *widens* it must not turn the suite red, and is audited for that |
| A window is never split across chunks | met | chunk bounds are window bounds, asserted |
| One trigger, one snapshot | met | the recompute's clear-and-insert is inside the engine's existing transaction |

### How it runs, per trigger

1. the bound batch is scanned for the **distinct windows it touched** — undated
   rows belong to no window and touch none, so they are excluded rather than
   grouped into a NULL window;
2. those windows are packed into **chunks** sized from an estimated row count,
   bounded by `max_rows_per_trigger`;
3. per chunk, `consumed_files` is asked **which files can hold a row in
   `[lo, hi)`** — plus this batch's own files, unioned in Python;
4. the source binds a view over exactly those files, narrowed to the range by
   two inlined literals, and the sink **clears the range and re-inserts it**.

All of step 4 is inside the engine's one transaction, so a trigger is still one
snapshot and no reader ever sees the range empty.

### What measurement changed

**Two things this file and `PLAN.md` both told the next session are wrong**, and
both were found by measuring rather than by review.

*"Close to free — the watermark scan already reads each batch once."* It is a
**second** scan, and it costs 1.4 ms on a one-file batch and 6.7 ms at forty
(§1.18). The watermark scan cannot supply per-file bounds: it runs only for a
model with a lateness horizon, and it reads a view with no `filename` column.
So the bounds are a separate grouped scan, charged only to models that will be
recomputed. Parquet footer statistics are 1.6x cheaper and were rejected — they
come back as **VARCHAR**, and a timestamp re-parsed from DuckDB's rendering of a
logical type is a plausible wrong number that would be wrong in the direction
that *narrows*.

*"Falls back to the whole list when unsure."* That is the right rule and the
obvious encoding of it is the expensive one. Written as
`min_ts IS NULL OR max_ts IS NULL OR (…)`, the disjunction defeats DuckLake's
file pruning outright — and because inlining is off, this table is one small
parquet file per trigger, so selection becomes **O(files ever consumed)**:
117.5 ms against 2,160 files and climbing, against 12.1 ms flat (§1.17). That is
§1.13's cost reappearing *inside* the index built to remove it. An unmeasured
file is stored at `[-infinity, +infinity]` instead, which is a true statement
about a file nobody has measured rather than a workaround, and needs no
disjunction at all.

**§1.17 is §1.10's rule in a fifth disguise**, and this file said to assume there
would be one.

### Four decisions worth keeping

**The sink clears the window range; it does not merge into it.** A recompute
produces the complete truth for `[lo, hi)`, so a key that no longer has source
rows must go with them. A `MERGE` updates the keys the recompute found and
leaves every other key exactly as it was — a mart row nothing supports, still
looking current. The `DELETE` costs a tombstone (§1.10) and fires per window
range recomputed rather than per trigger, which is the trade sealed `append`
already makes for its eviction.

**`BatchContext.window_range` is required, not inferred.** The sink cannot tell
a view holding one batch from a view holding a whole window by looking, and
replacing a window with a batch is §4's FFT mart exactly. So the engine — the
only thing that selected the files — states the claim, and a sink handed a plain
batch view refuses. Keyword-defaulted on a frozen interface, which is the
extension phase 2 ratified for `watermark`.

**The batch's own files are supplied from Python, not read back.** They are
recorded in the same transaction as the write, so whether the index can see them
yet is a question about DuckLake read-your-own-writes that nothing here should
depend on. Doing it in Python makes the recompute correct on a first run and on
a replay after a crash, where those rows do not exist at all.

**And they go to the chunk *planner*, not into its answer.** Merging them into
the selected file list afterwards reads as equivalent and is not: their rows
would then be missing from every estimate, so the row budget would silently
exempt exactly the data being added. That was a real defect, found by a test
that could not construct a second chunk. Files a bounds scan could not place
still take the sentinel range, so they are read by every chunk and never
narrowed away.

### The suite was audited by mutation, and it found four things

**47 deliberate defects, 45 auditable on this platform, and every one of them
lands where it should.** 43 turn the suite red, **two must not and do not**, and
two cannot be tested here for two different declared reasons. Fifteen are tier
three's and six are the landing-tree scan's.

```bash
.venv/Scripts/python.exe tools/mutation/run_audit.py          # all of them
.venv/Scripts/python.exe tools/mutation/run_audit.py 26 27    # by index
```

The two that must not turn it red are worth their own paragraph, because they
are a new kind of assertion here. *"The overlap test is closed at the top, so a
window steals the next one's file"* **widens** the tier-three index — and the
index is a hint, so widening it must change nothing. A red there would not be
coverage; it would be proof that the suite had started depending on the hint for
correctness rather than for cost, which is the one thing §1.13 says must never
happen. *"The scan stops sorting"* is the same shape: planning re-sorts
candidates by `(mtime, relpath)`, so scan order is not load-bearing and
reversing it must change no answer. The audit reports both as `held`, apart from
red and from SURVIVED, so they can never be miscounted as either.

**And two are excused for two different reasons, which the audit now keeps
apart.** One is *inert* here — it changes a branch Windows does not take. The
other is *live* here and its **fixture** cannot be built, because Windows will
not create a directory symlink without a privilege this box lacks; the test that
would catch it skips for the same reason. That one reported SURVIVED first,
which is a false hole, and declaring it is the honest alternative to leaving a
survivor that reads as missing coverage.

**Four mutations survived the first run, and only one was a hole in the suite.**
Reading each twice is what the README asks for, and all four readings were
different:

*A genuine hole.* "A model may recompute windows without declaring a grain"
survived because I added that requirement and never asserted it — I only
adjusted the existing fixtures to satisfy it, which is exactly how a rule gets
added and then quietly deleted later. Three tests now pin it, including the
`append` exemption.

*Two were the wrong **suite**, which reads exactly like a hole.* A defect in the
unmeasured-file path is inert under conformance, because every file in a
conformance scenario has a real time column and is therefore measured. A defect
in chunk *sizing* is inert under conformance **by design** — the suite asserts
chunked equals unchunked, so how chunks are sized is precisely what it must not
be able to see. Both are caught by unit tests that already existed. The README's
"pick the cheapest suite that should catch it" cuts both ways: too cheap reports
a false green, and too expensive reports a false hole.

*One mutation tested nothing.* "Touched windows include undated rows" removed a
SQL `IS NOT NULL` that has a redundant twin in the Python comprehension below
it, so the behaviour survived intact. Mutating one half of a belt-and-braces
pair proves nothing — and removing **both** then exposed a real defect
underneath, which is the second reading a survivor is supposed to get. That is
the undated-row finding below.

**And it happened a second time, to a mutation I wrote for a defect I had just
fixed.** "A recompute's temp views are only dropped when every chunk succeeds"
survived, because as first written it still registered each chunk's views
immediately after creating them — it differed from the real code only if
`_range_view` itself raised. The behavioural version defers the whole list to
the end of the loop, and that one is red. Phase 2b recorded exactly this failure
mode and it is worth restating: **the second question about a survivor is
whether the mutation does what its name says**, and writing the mutation right
after fixing the bug is when you are least likely to check.

**And two defects were found by writing the tests rather than by running them.**
A test that could not construct a second chunk turned out to be right: the
batch's own files were absent from the row estimate, so the budget silently
ignored exactly the rows guaranteed to be read. A test for view cleanup found
that a chunk raising part-way stranded every view its predecessors had made.
Both now have mutations of their own.

### Four things the build surfaced that were on no list

**A tier-three model was silently dropping undated rows.** Found by the mutation
audit, on its second reading of a survivor. A row with no event time belongs to
no window, and a recompute is scoped by a window range — no `[lo, hi)` contains
NULL — so it cannot be re-derived from one. Tier one folds such rows into a NULL
window because it never re-reads anything; tier three cannot, and that is a real
divergence from a full recompute. It was also **invisible**: with no lateness
horizon there is no watermark policy, so `rows_undated` was `None` and nothing
recorded that anything had been dropped. `CONTEXT.md`'s ratified rule is
"dropped **and counted**, never silently absorbed", and "a count that lives only
in a return value or a rotated log has not been counted". It is now counted in
the same single scan as `rows_in` (§1.11: ~0.26 ms for extra aggregates over one
pass), lands in `duckstream.batches`, and both routes to it — with a horizon and
without — are asserted to agree.

**An upgraded catalog would have silently narrowed every recompute.** Adding
`min_ts`/`max_ts` with `ALTER TABLE` leaves existing rows NULL, and NULL fails
the range test — so every file consumed before the upgrade would have stopped
being selected, and each window would have been rebuilt from part of its data
without failing. `ADD COLUMN … DEFAULT TIMESTAMP '-infinity'` is refused by
DuckLake, so the two bound columns are backfilled with an explicit `UPDATE` on
the pass that adds them: 44 ms over 1,000 rows, 109 ms over 100,000, once per
catalog. Found while writing the migration comment — the comment as first
drafted claimed the opposite and was wrong.

**Unwindowed `append` had to be exempted from everything.** It is the
tier-agnostic escape hatch — no grain, no fold, no revision — so a tier-three
model in that shape must not be made to declare a grain and must not take the
recompute path. Requiring the grain unconditionally broke exactly the test that
exists for that shape, which is the test doing its job.

**The engine was reaching into `FileSource._absolute`.** Consumed-file paths are
stored relative to the source's root, so only the source can resolve them. A
private-attribute reach with a `return relpath` fallback would have made a
third-party source read *plausible wrong files* rather than fail — the worst
outcome available. It is now a public `absolute_paths`, duck-typed like
`time_bounds`, and a source without it is refused by name.

**The row budget was not bounding the batch's own rows.** Found by a test that
could not build a second chunk however tight the budget was set. The chunk
planner sized itself from `consumed_files`, and this batch's files are written
in the same transaction as the output — so on a first batch, and on every replay
after a crash, the index knows nothing about them. The estimate came out zero
and the planner returned one unbounded chunk for exactly the rows guaranteed to
be read. §1.1 says memory is bounded by rows in flight and by nothing else, so a
budget that silently exempts the new data is not a budget. The batch's own files
now go to the planner as index entries in their own right, at their measured
bounds or at the sentinel.

**A failing chunk stranded its temp views.** Each chunk binds two — one over the
files it selected, one narrowing to the range — and they were handed to the
caller for dropping only when the *whole* recompute returned. A chunk that raised
left its predecessors' views behind, and a model failing repeatedly stranded
another set on every retry, for the life of the connection. They are now
registered as they are created.

## Phase 3: tier two, and the tooling for tier three

### What executes now

`avg`, `mean`, `stddev`, `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop` and
`variance` are maintained incrementally and exactly, through a mergeable state
rather than by folding a finished number.

| Requirement | Status | Evidence |
|---|---|---|
| Tier two executes | met | five conformance scenarios, both doors, diffed against a full recompute after *every* drain |
| **`CONTEXT.md` §4's bug is impossible** | **met** | 300x1.0 then 100x5.0 across four batches gives **2.0** and **1.7342**; that mart held 3.0 and 0.0 |
| Numerically safe at real magnitudes | met | see §1.14 — the textbook state returns 0.0 where this returns the right answer |
| Additive columns keep folding additively | met | a model's tier is its worst column; its columns keep their own |
| Tier three tooling | met | `udf.py`, 20 tests, including the undocumented list-return re-verified on every run |
| Tier three execution | **not started** | refused with a message saying it is not built |
| Window-range chunking | **not started** | phase 3 |

### The three decisions, each forced by a measurement

**State is `(n, mean, M2)`, not `(sum, sum_sq, count)`** — this contradicted
`PLAN.md` as written, and §1.14 is why. `PLAN.md` has since been corrected and
now specifies the same state, so the two agree; the history is kept because the
reasoning is the point. The textbook triple returns **524** for a true
variance of 0.25 at Unix-timestamp magnitudes, and exactly **0.0** at 1e8 with a
small spread. That second number is the same symptom §4's mart produced, from a
different cause, and it would have been the framework producing it.

**State is keyed by the statistic's *argument*, not its output column**, so
`avg(value)` and `stddev(value)` share one triple instead of keeping two copies
that can drift apart under a partial write.

**Derived values are computed from the merged state inside the same `UPDATE`.**
Every `UPDATE SET` right-hand side reads the pre-update row — verified, not
assumed — so a plain column reference would silently use the old state. The
obvious alternative, a second `UPDATE`, would rewrite every row in the table on
every batch rather than the ones the merge touched.

### Two things the build surfaced that were on no list

**Mixed-tier models.** One `avg` makes a whole model `sufficient_statistics`,
but a `count(*)` beside it is still additive and must keep folding by addition.
The code caught this by refusing to build a "statistic" for `count(*)`.

**The ground-truth diff was comparing different shapes.** State columns are real
columns, so `SELECT *` included them while the recompute did not. `harness`
now projects the model's *declared* columns, which is what the diff is about.

### Still refused, and the message says which kind of refusal it is

`corr`, `covar_pop` and `covar_samp` need cross terms between both arguments.
Refused with "is not built" rather than "cannot be done" — those read very
differently and only one of them is true here.

## Phase 2b definition of done, item by item

The gap: a corrupt file made the offset stop advancing, so every trigger from
then on retried it. Not a crash, nothing raising an alarm — the pipeline simply
stopped, quietly, until somebody noticed.

| Requirement | Status | Evidence |
|---|---|---|
| A bounded retry budget, with backoff | met | `max_attempts`, capped exponential; the delay exists for the drain loop, since under cron attempts are already a tick apart |
| Failures survive the process exiting | met | recorded in the `offsets` row the engine already reads, so it costs no extra read (§1.11) |
| **Unprocessable data does not stop the stream** | **met** | `on_failure='quarantine'` skips and records; a conformance scenario proves the *next* drop gets through |
| **…and is never lost silently** | **met** | skip and record are one transaction, checked `AT (VERSION => n)` on both sides |
| `halt` for when a gap is worse than a stall | met | never advances; also proved to stop everything behind it, so the trade is visible |
| One model's failure does not cost another its trigger | met | `run()` raises only after every model has had its turn |
| `status`, with lag | met | `metrics.py`; three lags because they fail independently |
| Exit codes cron can act on | met | non-zero for failing, halted, backed-off and quarantined |
| `rows_out` | met | free — DuckDB returns it from the write the sink already issues |
| An honest single-writer story | met | `lock.py`; advisory, portable, self-healing on a dead local pid |

### The three findings worth keeping

**`rows_out` was free all along.** Phase 1 recorded it as NULL believing it cost
"running the aggregation twice or coupling to a sink internal". Measured on
1.5.5: `con.execute` on the `INSERT`, `MERGE` or `DELETE` the sink already
issues returns a one-row result carrying the affected count, and the sink was
discarding it. A `0` on a sealed-`append` batch is now informative rather than
alarming — it means nothing sealed.

**Contention was not structurally impossible.** `CONTEXT.md` 2.5 said it was.
`AvailableNow` drains until empty, so a backlog can make one tick outlast the
interval that started it. What actually prevented corruption was DuckDB's file
lock on the catalog (§1.6) — verified by running it — reporting
`Unique file handle conflict`. Safe, but a message about a metadata handle
rather than about two copies of the pipeline running.

**A latent batch-id bug.** `next_batch_id` consulted only `batches`, which
records *committed* batches. Once failures started appending to `offsets`, a
fresh process could hand back an id a failure row had already used — and
`ORDER BY batch_id DESC LIMIT 1`, the whole ordering rule for reading state
back, would then pick between two rows arbitrarily. Found by a test that failed
for a reason I did not expect; pinned by
`test_a_batch_id_is_never_reused_after_a_recorded_failure`.

## Phase 2 definition of done, item by item

`PLAN.md` phase 2: *"Event time. Watermarks, tumbling windows, sealing past the
lateness horizon, `append` and `update` output modes via merge-by-key."*

| Requirement | Status | Evidence |
|---|---|---|
| Watermarks | met | `watermark.py`; derived as `max(event time) - lateness`, monotone, committed in the offset's transaction |
| Tumbling windows | met | `windows.py` owns the boundary; its Python and SQL halves are proved equal against DuckDB across grains and boundary values |
| Sealing past the lateness horizon | met | `window_ts <= watermark - grain`, one inlined literal |
| `update` output mode | met | phase 1's merge-by-key, plus: a sealed window can no longer be modified |
| `append` output mode | met | fold in an open-window accumulator, emit **once** when the window seals |
| **Late data inside the horizon updates its window** | **met** | `PLAN.md`'s named case; the test is the first one in `test_event_time.py` |
| **Data past the horizon is dropped *and counted*** | **met** | `rows_late` / `rows_undated`, durable in `duckstream.batches`, asserted from the catalog |
| Exactly-once still holds | met | the watermark is checkpointed by the same commit; proved under real process kills |
| Both front doors | met | `Parity` now also compares committed watermarks and drop counts |
| One trigger, one snapshot | met | asserted for both modes, including the three extra statements sealing adds |

### The two results worth stating precisely

**Late-within-horizon is decided on the window, not the timestamp.** With
`grain='hour'` and `lateness='10 minutes'`, a row at 10:30 puts the watermark at
10:20. A row for 10:05 then arrives — older than the watermark — and is still
folded, because `[10:00, 11:00)` has not ended. Only when a row at 11:10 or later
pushes the watermark past 11:00 does that window seal and a 10:05 row get
refused. Implementing this on the row's timestamp instead passes every casual
test and silently under-counts exactly the data the horizon was declared to
accommodate.

**Every intermediate snapshot is right, not just the final state.** As in phase 1,
the mart is walked `AT (VERSION => n)` across the whole catalog history, and at
each snapshot it is compared against an independent replay of the batches the
offset says had been consumed by then. For `append` this is the demanding case: a
snapshot in which a window had been evicted but not emitted — or emitted but not
evicted — would be visible.

### The ground truth is a second implementation

`recompute_sql` cannot serve for an event-time model: what the sink *should* hold
depends on the watermark trajectory, which depends on batch boundaries rather than
on file contents. So `harness.replay` writes the contract out again in plain
Python, from `PLAN.md`'s description — including flooring timestamps by **epoch
arithmetic** where duckstream floors them by replacing fields, so the two agreeing
takes a coincidence rather than a shared assumption.

One test pins the two ground truths against each other in the case where both are
valid (an update model with nothing dropped), so the reference is itself checked
rather than merely trusted.

### The suite was itself audited by mutation testing

Same procedure as phase 1, because a green suite that tests the wrong invariant
is worse than no suite. Fifteen deliberate defects were introduced into the
package — each one a plausible wrong decision rather than a syntax error — and
each was checked to turn the suite red. **Fifteen of fifteen did.**

| Mutation | Result |
|---|---|
| late is tested on the row's timestamp, not on its window | red, 0.9s |
| the watermark is allowed to regress | red, 0.9s |
| a window seals one whole grain early | red, 1.0s |
| the seal boundary is `<` instead of `<=`, so a window never seals on time | red, 19.3s |
| the batch is filtered with its own new watermark rather than the committed one | red, 45.6s |
| the watermark is committed in a second transaction | red, 108.2s |
| the memoised watermark advances **before** the commit | red, 11.9s |
| sealed windows are emitted but never evicted | red, 19.8s |
| sealed windows are evicted but never emitted | red, 20.2s |
| undated rows are folded into a NULL window | red, 1.2s |
| late rows are counted but not actually filtered out | red, 53.5s |
| only late rows trigger the filter, so undated ones slip through | red, 64.2s |
| windowed `append` no longer requires a horizon | red, 87.1s |
| a horizon canonicalises to text that does not parse back | red, 1.0s |
| the accumulator merge writes straight into the target | red, 20.6s |

Three of those are worth calling out.

**"the watermark is committed in a second transaction"** was caught by the
fault-injection test, not by a count — the same way phase 1's
offset-before-sink-write mutation was. That is the reassuring kind of catch,
because it means those assertions are load-bearing rather than decorative.

**"the memoised watermark advances before the commit"** is caught only by an
*in-process* test. A killed process starts with an empty cache, so the
subprocess fault tests would have passed either way; the rollback test in
`tests/unit/test_engine.py` is the sole detector. Any future memoisation on the
single-writer assumption needs its own in-process rollback test for the same
reason — the kill tests structurally cannot see it.

**"late is tested on the timestamp"** dies in 0.9 seconds, which is the point of
having put that case first in `test_event_time.py`. It is the single easiest way
to make the framework silently wrong, and it now fails before anything else runs.

Integrity was verified by hashing every mutated file before and after; all five
came back identical. Unlike phase 1, `duckstream/` and `tests/` are now tracked
in git, so `git status` is a second, independent check — but hash first, because
an interrupted mutation run once left a file mutated and only the hash check
would have said so unprompted.


### The phase-2b suite was audited the same way, and it found something

Thirty-two deliberate defects across both phases — the fifteen from phase 2
re-run against the changed suite, plus seventeen aimed at quarantine, the
failure policy, the lock and the metrics. **All thirty-two turn the suite red.**
Getting there took two rounds, and both rounds are worth recording because
neither failure mode was the mutation being wrong about the code.

**One genuine hole.** *"quarantine records the loss but never skips past it"*
survived the whole suite. The conformance scenario asserted that the record
existed and that a *later* drop got through — but the later drop is a separate
file the source plans independently, so it flowed regardless of whether the
offset had moved. Nothing asserted the thing that actually matters: that the
position advanced past the batch that was skipped. A quarantine that recorded
the loss and stayed put would write a permanent "data was lost" row and then
retry the same batch for ever, which is the worst of both policies. Now asserted
against the consumed-file set, and against `BatchResult.end_offset` agreeing
with what the state store committed.

**And the mutation that found it was weaker than it looked.** It changed only
the `end_offset` a `BatchResult` reports, not the `plan.end` handed to
`state.quarantine` — so the offset really did advance and only the report lied.
The behavioural version, which passes `position.offset` to the state store, was
written afterwards and is now in the set as its own mutation. **A mutation that
survives is worth reading twice: the first question is whether the suite has a
hole, the second is whether the mutation tested what its name claims.**

**Two stale anchors.** The `rows_out` change rewrote the line the phase-2 seal
mutations anchored on, so they matched nothing and were reported as *skipped*
rather than red. Skipped is the audit being honest — but a count that folded
them in with the reds would have claimed coverage that did not exist. Both are
re-anchored and both are now red.

**A process lesson worth more than any of them** — and *since fixed*, so read
this paragraph as history: phase 4 moved the audit into throwaway worktrees and
it no longer touches the working tree at all. At the time, the audit rewrote
files in the working tree, so nothing could be committed while it ran. A commit taken
mid-audit captured a mutated `engine.py` and pushed a deliberate defect. The
hash check caught it, which is the reason the hash check exists — `git status`
would not have, because the mutation is restored before the next one starts.
The restore itself also needs care: writing the file back without preserving its
line endings shows up as "not restored" and is indistinguishable, at a glance,
from a mutation that leaked.

## Demonstrated versus merely implemented

Be careful with this distinction when reporting on the project.

**Demonstrated** — everything phase 1 demonstrated, and additionally: watermarks
advancing monotonically and surviving a restart because they are read back out of
the catalog; late-within-horizon folding into its still-open window; out-of-horizon
data dropped and counted durably; a sealed window never modified again; each window
in `append` mode reaching the sink exactly once and complete; the watermark not
advancing when its batch is killed before the commit, verified with a real
`os._exit(9)`; one trigger still one snapshot with event time; both front doors
agreeing on watermarks and drop counts as well as on output.

Phase 2b adds: a corrupt file retried, then skipped, with the loss recorded
permanently and the stream live again afterwards, through both doors; the skip
and its record proved atomic by reading both `AT (VERSION => n)`; `halt` proved
to stop everything behind the bad batch, so the trade against quarantine is
visible rather than asserted; one model failing while another commits in the
same run; a non-zero exit for every unhealthy outcome.

Phase 4 adds: the consumed-file rows landing in the **same snapshot** as the
output they check point and the offset that counts them, walked `AT (VERSION =>
n)` across the whole catalog history and diffed against a replay at every step;
a quarantine's skip and the record of it proved atomic *including the rows*; a
v1 catalog migrating to rows without re-reading a file, keeping its retry state,
and doing it once, driven through the engine rather than by hand.

**Implemented but not demonstrated** — single-writer safety, still. Phase 2 added
a *second* memoised value on that assumption (the committed watermark, §1.11), so
there are two caches that a second writer would invalidate together. Phase 2b's
lock makes an overlap *diagnosable* but is advisory: the guarantee still rests on
DuckDB's catalog file lock, and no test runs two engines concurrently against one
catalog from two processes.

Tier three adds: a window fed by four batches recomputed to the same numbers a
full recompute of the whole landing tree gives, through both doors; a window
written two drains ago corrected by a later batch while an untouched window is
left byte-identical; **chunked equals unchunked** for a `non_foldable` model, at
budgets of 1, 2, 3 and unbounded, compared against *each other* as well as
against ground truth; and a recompute reading files from earlier batches rather
than only its own, proved with values that make the two answers impossible to
confuse.

**Implemented but not demonstrated**, newly: the file → time-range index is
demonstrated to *narrow* correctly and never to exclude, but the **scale** at
which it pays is measured (§1.17) rather than exercised — the conformance suite
runs tens of files, not 525,600. Nothing about correctness depends on that, by
construction, because the index only narrows; what is unproven is the saving.

**Neither** — no tier-three model has been run against a corpus large enough for
the index to matter, and no recompute has been run on a Pi.

**And one number is demonstrated on a dev box, not on a Pi.** §1.16's 1,665x is
measured with `threads=2` against local SSD. The direction is not in doubt — it
is fewer bytes, not faster bytes — but the *day* figures (11.2 GB against
6.8 MB) assume this machine's parquet sizes, and the soak run phase 6 owes is
still owed.

## Known open items

| Item | Where | Notes |
|---|---|---|
| Quarantine is whole-batch | `engine.py` | The unit skipped is the batch, so `max_files_per_trigger: 1` is what makes it precise. Narrowing a failing batch to isolate the offending file is not implemented; it would want the source's cooperation. |
| `status` walks the landing tree for its backlog | `metrics.py` | Deliberately unbounded — a backlog reported through `max_files_per_trigger` would read `10` whether ten files or ten thousand were waiting, which is exactly the difference worth knowing. It is I/O against a tree that may be a network mount, so it is skippable (`include_backlog=False`) and a source that raises costs a number rather than the whole status. Phase 3 gives the source time-range planning, which this should then reuse. |
| A halted model re-reads and re-plans every tick | `engine.py` | It writes nothing after the first verdict, which is the expensive half, but it does redo the planning. Cheap, and it is what lets it recover unattended. |
| First tick costs 2 setup snapshots | `Engine._prepare_model` | Unchanged. Pinned by `test_setup_costs_two_snapshots_even_when_there_is_nothing_to_do`. |
| `prune` exists but nothing calls it | `state.py` | Unchanged. Phase 4 maintenance should schedule it. **It must never be pointed at `consumed_files`** — those rows are the position, not a history of positions. |
| The landing tree is still walked in full every trigger | `sources/files.py` | `latest_offset()` scans every ready file, and §1.13's ~0.1 ms per file listed applies. This is what is left of §1.15 after §1.16: a speed problem rather than a card-wear one, and the lever is retention at the source. |
| `status` creates the state tables | `cli.py` | `_cmd_status` calls `ensure`, so a "read-only" command writes a snapshot the first time it meets a catalog that predates a table. Pre-existing, and now reachable on every *upgraded* catalog rather than only on a fresh one. Harmless, but it is not what the docstring says. |
| A rewrite with identical size *and* mtime is invisible | `offsets.py` | Unchanged; inherent to the identity. |
| `FileSource.__eq__` ignores `base_dir` | `sources/files.py` | Unchanged. Conformance parity compares **output**, never source equality — keep it that way. |
| A window whose key stops reporting seals only on someone else's timestamp | `watermark.py` | The watermark is per model, not per key, so a sensor that goes silent leaves its last window open until any later-timestamped row arrives for that model. Standard for event-time engines, but it surprises people, so it is documented in the README's limits. |
| Two memoised values ride on single-writer | `engine.py` | The batch id (§1.10) and the committed watermark (§1.11). They expire together. |
| The run lock is advisory | `lock.py` | Safety still comes from DuckDB's catalog file lock. The advisory lock exists to say *what happened* in words. Never promote it to the guarantee without a filesystem story. |
| **Nothing bounds group cardinality, and memory follows groups** | `recompute.py`, `model.py` | §1.21, and it is a gap rather than a tuning note. A group is `key × window`. Window-range chunking bounds the window half; **nothing bounds the key half**. A model with 4,000 sensors materialises 4,000 lists in every chunk however small that chunk is, and `max_rows_per_trigger` cannot help — a batch of few rows spread across many keys is strictly worse than many rows in one. §1.1's "bound rows in flight" is incomplete as written. Closing it needs chunking by key range as well as by window range, which is a design question rather than a setting. |
| A tier-three recompute costs a whole window, whatever the batch | `recompute.py` | Inherent, not a defect: a one-row batch landing in a busy hour re-derives that hour. The lever is a finer `grain`, never a smaller batch. Worth saying because the obvious reaction — turn `max_files_per_trigger` down — makes it *worse* by recomputing the same window more often. |
| A recompute writes a tombstone per window range | `sinks/table.py` | §1.10's ~26 ms `DELETE`, bought deliberately: the range is cleared so a key whose source rows are gone disappears with them. Partitioning sink tables by grain (phase 4) is what would make it cheap. |
| The index is not rebuilt for a model promoted to tier three | `engine.py` | Bounds are measured only for models that already recompute, so files consumed while a model was tier one carry the sentinel range and are read by every recompute. Correct, and slower than it needs to be. A backfill would be a maintenance task, not a fix. |
| Nothing removes a `consumed_files` row for a file that no longer exists | `consumed.py` | Unchanged from phase 4, and tier three makes it visible: a recompute selects the file, `read_parquet` does not find it, and the batch fails rather than silently skipping. That is the right way round, but the message comes from DuckDB rather than from duckstream. |

## Measured this session

Constraints §1.11 through §1.20 were measured during phases 2, 2b, 3 and 4, and
**ten** of the rows below overturned something that was already written down —
including two of `CONTEXT.md`'s own numbers and, now, two instructions this very
file gave the session that followed it:

| | Finding | What it overturned |
|---|---|---|
| §1.11 | a watermark read costs 10.4 ms a trigger | — |
| §1.12 | reduce state in SQL; `status` was O(n) at 213 ms, now flat at 48 ms | — |
| §1.13 | statistics pruning does not save the file open: ~0.1 ms per file | my own assumption that it would make tier three cheap |
| §1.14 | `sum_sq` returns 524 for a true variance of 0.25, and 0.0 at 1e8 | **`PLAN.md`'s specified state for tier two** |
| §1.15 | the consumed-file map is rewritten in full every trigger: 45.7 MB encoded | — |
| §1.16 | as rows: **4.9 KB a trigger against 7.97 MB**, 1,665x, and flat in files consumed | — |
| §1.16 | the offset reaches the disk as parquet: **11.2 GB/day, not 65** | **§1.15's own arithmetic**, derived rather than measured |
| §1.16 | zlib saves 2.38x *on the disk*, not 7.4x | **§1.15's second number**, same cause |
| §1.17 | a NULL "unknown" defeats file pruning: **117.5 ms against 12.1 ms** at 2,160 consumed files | **this file's own "falls back to the whole list"**, which is the right rule written the expensive way |
| §1.18 | per-file bounds are a second scan at 1.4–6.7 ms, not free | **this file's own "close to free — the watermark scan already reads each batch once"** |
| §1.18 | parquet footer statistics come back as **VARCHAR** | the obvious cheaper source for the index |
| §1.19 | a recompute is ~17.5 ms plus **~0.14 ms per file** in the window | — |
| §1.20 | the landing-tree scan was **81% Python path manipulation**, not I/O; now 3–9x faster | **`PLAN.md`'s "§1.13's ~0.1 ms applies to this walk"** |
| §1.20 | …and its own first draft, whose figures drifted **7x** between runs | **§1.20's own numbers**, taken without interleaving |
| §2.5 | contention is *not* structurally impossible; the catalog file lock is what saves you | **`CONTEXT.md` §2.5's own claim** |
| §1.2 | the enum is `duckdb.func`, not `duckdb.functional` | **§1.2's own code sample**, which had never been executed |
| §1.9 | temp views, temp tables and `DROP VIEW` all work **inside** a DuckLake transaction | **`engine.py`'s own claim** that binding inside would raise, which had never been executed |

§1.10, §1.11 and §1.12 are the same constraint met three times in three
disguises — do not read state you just wrote, and do not move state to the
arithmetic when the arithmetic can go to the state. §1.16 was the fourth and the
most expensive of them. **§1.17 is the fifth**, which the previous version of
this paragraph said to assume: a predicate that cannot be pruned drags the whole
table back through Python's side of the fence, and it arrived disguised as a
null-safety check. Assume there is a sixth.

**And a second rule this round earns: a handover instruction is an argument, not
a measurement, exactly like a derived number.** Two of the bullets this file gave
the next session were wrong — "close to free" and "falls back to the whole list"
— and both were wrong in the same way, by describing a plan that had never been
run. They were written next to measured constraints and inherited their tone.
Treat "the design is settled, this is implementation rather than research" as
the claim it is.

**And a rule the §1.15 correction earns its own line for: a derived number is an
argument, not a measurement.** 65 GB/day was 45.7 MB times a cadence, and it
went unchallenged for a phase because it was sitting in a table of measured
things. If a number in `CONTEXT.md` was computed rather than observed, it has
the standing of intuition and this project's first rule applies to it.

## Still unmeasured

**Both of `PLAN.md`'s performance items are now measured** (§1.21, §1.22), and
are struck through below rather than deleted because what each *found* is not
what its question assumed. Two remain — one from `PLAN.md`'s phase 6, one from
`CONTEXT.md` §6 — and the attribution is worth stating, because an earlier
version of this section put all four under §6 and a session that went looking
for them there found something else entirely:

- ~~**memory ratio per tier**~~ (`PLAN.md`) — **measured, §1.21**, and the answer
  is not the one the question assumed: memory follows **groups**, not rows. Same
  1.92 M rows, 64 MB at one group and **over 2 GB at 4,000**. It closed the item
  and opened a bigger one — see "Known open items"
- ~~**UDF parallelism penalty**~~ (`PLAN.md`) — **measured, §1.22**: native SQL
  scales 3.18x on four threads, the same query with a Python UDF reaches 1.31x,
  about two thirds serial, and costs 4.2x native at four threads
- **accumulator size under a real workload** (`PLAN.md` phase 6, and §1.3's own
  caveat) — sealing bounds it by the horizon, but §1.3 only measured a state
  table to 6,000 rows
- `PRAGMA platform;` on the actual Pi 5 (**§6**; only matters for extension work)

`CONTEXT.md` §6 additionally lists change-feed cost, whether the change feed
survives `expire_snapshots`, whether community-extensions CI accepts a C-API
extension, and DuckDB v2.0's final scope. None of those blocks anything in
phases 3–6.

## Operating envelope, measured

| | |
|---|---|
| idle trigger, writes nothing | ~1.3 ms |
| committing trigger | ~15 ms |
| full trigger with state | ~25.7 ms |
| process cold start under cron | ~235 ms |
| **a lateness horizon, on top of the same trigger without one** | **~+5 ms** |
| **…when it actually drops rows, so a filter view is built** | **~+6 ms** |
| **sealed `append`: accumulator merge, emit and evict** | **~+20 ms** |
| **a tier-three recompute step, window of 1 file** | **~17.5 ms** |
| **…window of 100 files (20,000 rows)** | **~31.3 ms** |

So **seconds, not sub-second, is still the sensible cron unit**, and event time
does not change that. The horizon's ~5 ms is one extra state append and is
irreducible: the watermark has to become durable in the same transaction as the
offset. The extra *scan* is free — reading the newest event time and both drop
counts alongside the row count costs 0.26 ms more than the `count(*)` it replaced.

The recompute row is **~17.5 ms of intercept and ~0.14 ms per file** (§1.19).
The intercept is the same commit floor everything else pays; the slope is
§1.13's per-file open, met again three phases later by a different method. What
it means operationally is the one thing to remember about this tier: **cost is a
function of the window, not of the batch.** A one-row batch landing in an hour
fed by a hundred files pays for a hundred files.

**Measure interleaved on this box.** A first attempt at these numbers ran the
variants one after another and gave a baseline of 100.8 ms on one run and 67.8 ms
on the next; machine drift over a two-minute run swamped the effect. Running one
trigger of each variant in turn made the deltas reproducible to ~1 ms.

**And interleaving is not sufficient on its own.** A first attempt at the
tier-three row timed whole `engine.run()` calls, interleaved and 40 deep, and
every variant landed between 130 and 165 ms with tier three sometimes measuring
*faster* than tier one. The trigger's own fixed costs swamped the difference, so
the numbers were real and meaningless. They are not in the table: what is
reported is the recompute step in isolation, which is the part that varies. **A
number measured through too much machinery is as unpublishable as a derived
one.**

## Traps waiting for whoever is next

The phase-1 traps still hold. In order of how much damage they do:

1. **Never put a scalar subquery in a MERGE or JOIN condition against DuckLake**
   (§1.5). It fails with `Out of buffer`, and **only on the second batch** — the
   first to take the `WHEN MATCHED` branch. Compute bounds in Python and inline
   them as literals. Every suite must run at least two batches. Phase 3's
   window-range chunking is *exactly* the shape that tempts a subquery here.
2. **Sink and state must stay in the same catalog** (§1.9). A transaction cannot
   span attached databases, so splitting them makes exactly-once unformable, not
   merely slower. Phase 2's open-window accumulator obeys this too: it sits in
   the target's own schema because sealing moves rows between the two inside one
   transaction.
3. **Keep per-trigger state append-only** (§1.10). A matching DuckLake `DELETE`
   costs ~26 ms. Phase 2's eviction is not an exception to this: it fires per
   *window sealed*, not per trigger.
4. **Do not re-read state you just wrote.** Twice now this has been the dominant
   term in a trigger — `max(batch_id)` at ~11 ms (§1.10) and `load_watermark` at
   10.4 ms (§1.11) — and twice the fix was to memoise it and write the cache only
   after a successful commit. Assume the third one is also a read.
5. **Do not `SELECT *` from `ducklake_snapshots()`** — its `TIMESTAMP WITH TIME
   ZONE` column needs `pytz`, which is not a dependency. Use `lake.snapshots()`.
6. **The window column is `window_ts` at every grain**, and `key` must contain it
   when `grain` is set, or idempotency silently breaks.
7. **Any fixture writing a landing tree must write atomically** — temp path,
   rename, *then* the marker. A fixture that appends to an already-planned file
   produces a genuine double-count that looks like an engine bug.

And four that phase 2 added:

8. **Lateness is decided on the row's *window*, not on its timestamp.** Getting
   this backwards is the single easiest way to make the framework silently wrong
   again. See the worked example above.
9. **The sink does not filter late rows and must not start.** The engine removes
   them before `write` is called. Duplicating the check would put the decision in
   two places, and the sink's copy would be the one without the committed
   watermark to check against.
10. **`harness.replay` is a second implementation, not a helper.** Do not
    "simplify" it by importing from `duckstream.windows` — that would make it
    agree with the engine by construction and it would stop being ground truth.
11. **Two `Parity` objects in one test need a landing tree each.** The two
    *doors* of one parity share a tree deliberately; a second *parity* over the
    same tree consumes the first's drops as well as its own. Found the hard way
    while writing the batch-boundary test, where it presented as the engine
    folding a row twice — trap 7's failure mode, from the other direction. Pass
    `make_parity(..., landing=Landing(tmp_path / "elsewhere"))`.

And two that phase 2b added:

12. **A failed batch is not an exception at the call site.** `_run_batch`
    catches, records and returns an outcome; `_drain` breaks on anything that
    did not commit; `run()` raises only once every model has had its turn.
    Anything added inside the batch lifecycle inherits that, so it must leave
    the transaction rolled back before it propagates.
13. **Do not let a stuck model write on every tick.** A halted model records its
    verdict once and then retries silently. The first version appended a row —
    and therefore a DuckLake snapshot — every tick for as long as nobody fixed
    the cause, which grows the catalog fastest exactly when that helps least.

And two from phase 3 so far:

14. **A model's tier is its worst column; its columns keep their own.** One
    `avg` makes a whole model `sufficient_statistics`, but the `count(*)` beside
    it still folds additively and must not be given a statistic state.
15. **Names collide with DuckDB built-ins.** `entropy` is an obvious name for a
    windowed UDF and is already an aggregate; a scalar cannot shadow one, and
    DuckDB's error describes the collision from the inside without mentioning
    the name. `udf.py` refuses it up front. Expect the same class of surprise
    from `median`, `mode`, `kurtosis`.

And five that phase 4 added:

16. **A one-file scan cannot test file identity.** The consumed-file probe is
    narrowed to `BETWEEN min(scan mtime) AND max(scan mtime)`, and with one file
    that is `BETWEEN t AND t` — indistinguishable from the `mtime_ns` equality it
    is meant to be independent of. A test using a single-file scan does not test
    identity at all. This cost a real mutation survivor; see §1.16.

17. **The checkpoint moving is not proof that progress was made.** It was, when
    the offset carried the consumed set. It is now a counter the source computes
    optimistically, so the drain loop's stalled-loop guard can watch it advance
    while nothing is recorded and the same files are re-read for ever. Two
    checks stand in its place — `TableIndex._verify` and
    `Engine._require_recorded` — and anything added to the batch lifecycle that
    advances a position must keep them true.

18. **Anything that advances a position must record the rows in the same
    transaction.** Quarantine is the one that already caught this out: it used
    to skip past a bad batch *by* advancing the offset, and with rows that is
    two writes rather than one. `state.quarantine` takes the index for exactly
    this reason.

19. **`prune` must never be pointed at `consumed_files`.** Every other state
    table keeps a history of positions and only its newest row is read, which is
    what makes trimming safe. These rows *are* the position. Deleting one makes
    duckstream read that file again and fold it into the mart a second time —
    the §4 bug class, produced by the maintenance meant to prevent bloat.

20. **`status` never migrates, so it must not read the migrated shape.** It runs
    no batch by design, so on an upgraded-but-not-yet-run catalog the
    consumed-file table is empty while the position is still in the offset.
    Reading the table there reports every consumed file as backlog. Any new
    reader of that table has to ask the source whether the position has moved
    yet — `metrics._consumed_index` is the pattern.

And five that tier three added:

21. **Do not encode "unknown" as NULL in the file → time-range index.** It is
    the obvious way to write it and §1.17 measured what it costs: `min_ts IS
    NULL OR max_ts IS NULL OR (…)` cannot be pruned, so with inlining off and
    one small parquet file per trigger the selection goes **O(files ever
    consumed)** — 117.5 ms against 2,160 files and climbing, against 12.1 ms
    flat. That is precisely the cost class §1.13 says the index exists to
    remove, reintroduced inside the index. An unmeasured file is stored at
    `[-infinity, +infinity]`, which is *true* rather than a workaround, and
    needs no disjunction.

22. **A new column on `consumed_files` is not safe left NULL.** NULL fails
    `max_ts >= lo AND min_ts < hi`, so an upgraded catalog whose rows kept NULL
    would silently stop selecting files it had already consumed and every
    recompute would build its windows out of part of the data. The two bound
    columns are backfilled to the sentinel in the same transaction that adds
    them. `ADD COLUMN … DEFAULT TIMESTAMP '-infinity'` is refused by DuckLake
    (*"we cannot add a column with a non-literal default value"*), so it is an
    explicit `UPDATE`, paid once per catalog.

23. **A sink must never replace a window from a view it was merely handed.**
    `BatchContext.window_range` is the engine's claim that the view holds
    *every* source row for that range, and the sink refuses to recompute
    without it. Inferring it instead would let a plain batch view overwrite a
    window with whichever rows arrived last — `CONTEXT.md` section 4's FFT mart
    exactly, 51 spectrum bins where the truth was 201, and it never fails.

24. **A recompute clears its range; it does not merge into it.** A `MERGE`
    updates the keys the recompute found and leaves every other key looking
    current, so a key whose source rows have gone survives as a plausible wrong
    row. The `DELETE` costs a tombstone (§1.10) and fires per window range
    recomputed, not per trigger — the same trade sealed `append` already makes.

25. **Unwindowed `append` is exempt from everything tier three requires.** It
    is the tier-agnostic escape hatch: no grain, no fold, no revision, so a
    tier-three model in that shape must *not* be given a grain requirement and
    must *not* take the recompute path. Both the validator and the engine check
    `grain is not None` as well as the strategy, and a model reaching the
    recompute planner with no grain would be asked for the windows of something
    that has none.

26. **A tier-three model drops undated rows; a tier-one model folds them into a
    NULL window.** The tiers genuinely differ here and it is not a bug: a
    recompute is scoped by `[lo, hi)` and no range contains NULL, so a row
    belonging to no window cannot be re-derived from one. The rule is that the
    difference must be *counted* rather than silent — `rows_undated` is now
    populated for a recomputing model even when it declares no horizon. Anything
    that adds a tier-three code path has to keep that true, and note that a
    model *with* a horizon never reaches it, because undated rows are filtered
    upstream. That asymmetry is what let a mutation survive here.

And two that phase 4's scan work added:

27. **`FileSource._walk` is a pure optimisation and every part of it is load
    bearing.** It reproduces `os.walk`'s behaviour deliberately, not
    incidentally: `is_dir(follow_symlinks=False)` for descending and plain
    `is_file()` for files, so a symlink *to* a file still counts and a symlinked
    *directory* is still not descended into. Get that backwards and a symlink
    pointing up its own tree makes every file consumed twice under two
    different relative paths — which the anti-join cannot catch, because the
    paths really are different. The test for it **skips on Windows**, which
    cannot create a directory symlink without a privilege, and the matching
    mutation is declared skipped for the same reason rather than left looking
    like a hole.

28. **Do not reintroduce `relative_path` per file.** It is called once per
    *directory* and the prefix is carried down the walk. Per file it reaches
    `os.path.relpath`, which is sixteen `normcase` calls each — §1.20 measured
    160,000 of them for 2,000 files, and 81% of the whole scan sitting above a
    4.7 µs `stat`. It reads like the more correct way to build a path and it is
    the same string.

29. **Retention cannot delete a file a tier-three model may still recompute
    from.** This trap is written *ahead* of the code, because the code is the
    next thing anyone builds. A `recompute_window` model reads consumed files
    back out of the landing tree, so "every model has consumed it" is no longer
    sufficient grounds to remove one. The extra condition — no tier-three model
    can still be asked to recompute a window the file feeds — is only knowable
    past a **lateness horizon**, because without one any row can arrive for any
    window at any time. Delete anyway and nothing fails: the recompute reads
    what is left and writes a window built from part of its data. `PLAN.md`
    phase 4 states it in full.

And two that phase 5 added:

30. **Never acknowledge a message before its marker is on disk.** `paho` acks a
    QoS-1 message on arrival by default, and this repository's own
    `subscriber.py` relies on that — which is at-*most*-once for anything still
    buffered: the broker is told it was handled, the process dies, and it is
    gone with nothing to report it. `LandingWriter.flush` returns the
    acknowledgement tokens and returns them **only after** the marker exists.
    Anything that acks earlier has chosen at-most-once and should say so out
    loud.

31. **Exactly-once is over files, not over readings.** At-least-once means the
    broker re-delivers whatever was never acked, so the same reading can land in
    two files. duckstream does not de-duplicate that and cannot — the two files
    genuinely differ and nothing marks one as a repeat. A model that cares needs
    a merge key and `mode='update'`. Do not "fix" this by de-duplicating in the
    source: it would need a message identity the broker does not provide, and
    guessing one silently drops real readings.

One phase-1 invariant that phase 2 **deliberately breaks**, so do not restore it:
**"chunked equals unchunked" no longer holds once a horizon exists.** The
watermark is a function of what has been observed, so a batch boundary between
two rows can make the second late when reading both at once would not have.
`max_files_per_trigger` therefore stops being purely a memory knob. This is
inherent to event-time semantics rather than a duckstream quirk, and
`test_batch_boundaries_change_which_rows_are_late` pins it — including an
assertion that the two results *differ*, so the test cannot quietly stop testing
anything.

## Package layout

| Path | Contents |
|---|---|
| `model.py` | `Model`, load-time validation, the invariants — the canonical representation |
| `aggregates.py` | foldability tiers, classification from DuckDB's AST, fold SQL |
| `windows.py` | tumbling-window arithmetic: the boundary, in one place |
| `watermark.py` | lateness parsing, the watermark, what falls outside the horizon |
| `udf.py` | Arrow-mode UDF registrars — the shape §1.2 measured at 2x native |
| `recompute.py` | tier three's planner: touched windows, chunk sizing, file selection |
| `metrics.py` | the three lags, per-model status, the health verdict |
| `lock.py` | one writer per catalog, and a sentence when there is not |
| `protocols.py` | `Source`, `Sink`, `StateStore`, `BatchPlan`, `BatchLimits`, `BatchContext` |
| `engine.py` | trigger loop, the transaction boundary, batch lifecycle, fault hooks |
| `trigger.py` | `AvailableNow`, `Once` |
| `state.py` | append-only offsets/watermarks/batches; DuckLake and in-memory stores |
| `lake.py` | attach, settings, inlining enforcement, snapshot introspection |
| `offsets.py` | offset encoding, and both file-offset shapes — v2 counts, v1 carried a map |
| `consumed.py` | the consumed-file set as rows: the table, the anti-join, the two index shapes, and the file → time-range index |
| `landing.py` | the landing writer: buffer, atomic write, marker last, tokens released only when durable |
| `sources/files.py` | directory tailing with completion markers |
| `sources/mqtt.py` | the MQTT adapter over `landing.py`. `paho-mqtt` is optional and lazily imported |
| `sinks/table.py` | append, update-by-merge, and sealed windowed append |
| `sql.py` | identifier and literal quoting |
| `config.py` | YAML deserialiser, `${VAR}` substitution — parsing isolated here |
| `registry.py` | built-in names plus dotted-path resolution |
| `cli.py` | `run`, `validate`, `status`, `models` |

Everything `PLAN.md` names is now written.

## How tier three was built

One work unit, for the third time and for the same reason. The file index, the
chunk planner and the sink's write path are one decision wearing three hats:
what the index selects decides what a chunk reads, and what a chunk reads
decides whether the sink may replace a range or must merge into it. There was no
interface to negotiate between agents.

**Measurement came before code, and it changed the design twice.** Six
measurement scripts ran before the first line of `recompute.py`: where per-file
bounds can come from and what they cost, whether the index actually beats the
whole-list scan, why the first answer to that was *"barely"*, whether the
sentinel round-trips through three different writers, whether `filename=true`
works on all three formats, and whether chunking really bounds memory. The
second of those returned 1.6x where §1.13 predicted ~100x, and chasing that
number is what produced §1.17 — the single most valuable hour of the build, and
it would not have happened if the index had been written first and measured
afterwards.

**The audit was run twice, and the first run was thrown away.** Partway through
it, reviewing my own diff, I found the engine reaching into
`FileSource._absolute` with a `return relpath` fallback — a private-attribute
reach that would make a third-party source read *plausible wrong files* rather
than fail. Auditing code that is about to change proves nothing about the code
that ships, so the run was stopped, the fix made, and the audit restarted
against the settled tree. Nothing was lost but time, because the audit runs in
throwaway worktrees and touches nothing.

## How the phase-4 build was run

One work unit again, and for the same reason phase 2 was: the offset shape, the
source's planning and the engine's transaction boundary are one decision wearing
three hats, so there was no interface to negotiate between agents.

What did fan out was the *checking*, and it earned its cost twice. The mutation
audit ran 26 defects across four parallel worktrees and found two holes review
had missed — a test that could not test the thing it was named for, and a guard
that hung instead of failing. An adversarial review ran five independent lenses
over the diff, each trying to refute its own findings, and found the `status`
defect that the audit had no mutation for. Neither would have found what the
other did.

**The review ran partially and it is worth saying so.** Five of its twelve
agents hit a spend limit, so the migration and claims lenses never reported.
What it did return was confirmed and acted on; what those two lenses would have
found is unknown, and re-running them is cheap if the next session wants the
coverage.

## How the phase-2 build was run

One work unit in one session, unlike phase 1's eight parallel units with separate
feedback agents. The pieces are too tightly coupled to fan out usefully — the
watermark decides what the sink sees, and the sink's output mode decides what the
watermark is *for* — and phase 1's frozen interfaces meant there was no interface
to negotiate. The verification burden that the feedback agents carried in phase 1
was met instead by the mutation audit above, which is the part that actually
caught things.
