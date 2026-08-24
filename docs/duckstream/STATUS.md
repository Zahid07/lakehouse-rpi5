# duckstream — status and handover

**Start here if you are a new session.** This file says where the work stands, what
is proven, what is not, and what to do next. It is current as of **2026-08-24**.

Read in this order:

1. **this file** — where things are
2. `CONTEXT.md` — measured constraints and settled decisions. **Sixteen** measured
   constraints; §1.8, §1.9 and §1.10 were measured during the phase-1 build,
   §1.11 during phase 2, §1.12 during the phase-2b cleanup and §1.16 during
   phase 4, and none of them is derivable from anything else. §1.10, §1.11 and
   §1.12 are the *same* constraint met three times in three disguises — read
   them together, and read §1.16 as the fourth. **§1.16 also corrects two
   numbers in §1.15**, which had been derived rather than measured
3. `PLAN.md` — the specification: what to build, phase by phase
4. `BUILD_GRAPH.md` — the decision record: frozen interfaces, every ratified
   decision with its reasoning, and the notes each task left for the next

**Phases 1, 2 and 2b are complete. Phase 3 is half done. Phase 4's first item is
done.** Phases 5–6 are not started.

Phase 3 is foldability. **Tier two executes** — `avg`, `stddev`, `var`,
population and sample — maintained through sufficient statistics and proved
against the production bug in `CONTEXT.md` §4. **`udf.py` is written**, so tier
three has its tooling. **Tier three itself does not execute yet**: a
`recompute_window` model is still refused, loudly, with a message that says it
is not built rather than that it cannot be done.

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

**Tier three (`recompute_window`), and it is now the cheaper job it should
always have been.** Finishing phase 3 is the last of the differentiator and the
reason to build this rather than adopt Arroyo. Its design is settled by
measurement, so this is implementation rather than research:

- Re-derive a window from source. The rows are in files consumed long ago —
  still on disk and still identifiable, because the position records every file
  consumed rather than a high-water mark. It is now a **table** you can query,
  which is the part that changed.
- **Do not hand DuckDB the whole file list with a time predicate.** §1.13
  measured ~0.1 ms per file listed *whether or not it is skipped*: statistics
  pruning skips data pages, never the file open. 1.7 ms against 217 ms at 2,160
  files, and on a Pi that constant multiplies.
- So build a **file → time-range index, as a hint and never as truth**: it only
  narrows, never removes entries, and falls back to the whole list when unsure.
  Over-selecting reads extra files and is still right; under-selecting is
  silently wrong. Close to free — the watermark scan already reads each batch
  once, so grouping it by `read_parquet`'s `filename` yields per-file bounds.
- **That index is now two columns on `duckstream.consumed_files`.** Add `min_ts`
  and `max_ts` beside `relpath`, and file selection becomes
  `WHERE max_ts >= lo AND min_ts < hi` — a query against a table that already
  exists, with the same anti-join machinery and the same tests. This is why
  phase 4's first item was done first: measured, putting that index in the old
  JSON offset took it from 45.1 MB to 71.2 MB per trigger, **1.58x worse on the
  project's worst number**, and the alternative was building a separate table
  that this change would then have had to fold back in.
- `udf.py` is done and is what tier three's aggregates call.
- Window-range chunking sized from estimated rows comes with it (§1.1: memory is
  bounded by rows in flight, never by making the UDF faster).

The rest of phase 4 is unchanged and still waiting. Retention at the source
moved *down* that list rather than off it — see "Known open items".

Everything deferred out of the phase-2b sweep was written **explicitly into the
phase that owns it** rather than left as a note here: `PLAN.md` phase 4 now names
scheduling `prune()`, a migration path for a renamed aggregate, and compaction of
the open-window accumulator; phase 6 now names a soak run on real hardware, the
two unmeasured numbers, and release discipline.

## Environment

```bash
cd d:\lakehouse-rpi5
.venv\Scripts\python.exe -m pytest -q                        # 1162 passed, 1 skipped, ~5m
.venv\Scripts\python.exe -m pytest -q -m "not conformance"   # 1035, the fast ones, ~70s
.venv\Scripts\python.exe -m pytest -q -m conformance         # 127, the expensive ones, ~4m
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

The expected skips are correct and there are now **two**, which is one test in
two parametrisations: Windows cannot hold two filenames differing only by case,
so the POSIX half of a case-sensitivity pair cannot run here, and that test now
runs against both consumed-set shapes. A filtered run (`-m conformance`) skips a
third — the front-door exemption audit, which needs an unfiltered collection to
mean anything and says so.

### Git state

Committed on `feat/duckstream`, newest last:

```
693e691  phase 1
143e43b  feat: add pushes for phas 2
ee9e3c0  chore(duckstream): normalise line endings to LF
f501eef  fix(duckstream): flat-cost status, guard hole, unused imports
9351d13  feat(duckstream): tier-2 sufficient statistics, Pi measurements
f8c6e1c  feat(duckstream): Arrow-mode UDF registrars for tier three
```

**The defect that was in `143e43b` is gone.** That commit was taken while a
mutation audit had `engine.py` mutated and captured a batch judging its own rows
against its own new watermark instead of the committed one. It was fixed by the
commits after it; `git show HEAD:duckstream/engine.py` no longer contains it.
Nothing is uncommitted that should not be — and the audit can no longer cause a
repeat, because it does not touch the working tree at all any more.

## Phase 4, item 1: the consumed-file set as rows

`PLAN.md` phase 4 says its first item is not compaction — it is the file
source's consumed-file map, and it "outranks everything else in this phase".
Done.

| Requirement | Status | Evidence |
|---|---|---|
| The consumed set is rows, not a JSON cell | met | `duckstream.consumed_files`; `duckstream/consumed.py` |
| **The per-trigger write collapses** | **met** | §1.16: 7.97 MB → **4.9 KB**, 1,665x; 11.2 GB/day → 6.8 MB/day |
| …and stays flat as the stream ages | met | plan 3.6 ms and commit 13.8 ms at 1,000 files and at 525,600 alike |
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

## Phase 3 so far: tier two, and the tooling for tier three

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

**State is `(n, mean, M2)`, not `(sum, sum_sq, count)`** — this contradicts
`PLAN.md`, and §1.14 is why. The textbook triple returns **524** for a true
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

**A process lesson worth more than any of them.** The audit rewrites files in
the working tree, so nothing may be committed while it runs. A commit taken
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

**Neither** — tier three still does not execute; it is refused with a clear
message saying so. "Chunked equals unchunked for every `non_foldable` model"
therefore remains asserted on the foldable tiers only.

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

## Measured this session

Constraints §1.11 through §1.16 were measured during phases 2, 2b, 3 and 4, and
**five** of them overturned something that was already written down — including,
now, one of `CONTEXT.md`'s own measurements:

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
| §2.5 | contention is *not* structurally impossible; the catalog file lock is what saves you | **`CONTEXT.md` §2.5's own claim** |
| §1.2 | the enum is `duckdb.func`, not `duckdb.functional` | **§1.2's own code sample**, which had never been executed |

§1.10, §1.11 and §1.12 are the same constraint met three times in three
disguises — do not read state you just wrote, and do not move state to the
arithmetic when the arithmetic can go to the state. §1.16 is the fourth, and it
was the most expensive of them. Assume there is a fifth.

**And a rule the §1.15 correction earns its own line for: a derived number is an
argument, not a measurement.** 65 GB/day was 45.7 MB times a cadence, and it
went unchallenged for a phase because it was sitting in a table of measured
things. If a number in `CONTEXT.md` was computed rather than observed, it has
the standing of intuition and this project's first rule applies to it.

## Still unmeasured

From `CONTEXT.md` §6:

- **memory ratio per tier** — bisect `memory_limit` against `max_rows_per_trigger`
  and publish the ratio so users can size the knob
- **UDF parallelism penalty** — quantify the single-thread cost (§2.1) so the docs
  can say when to split across processes
- **accumulator size under a real workload** — sealing bounds it by the horizon,
  but §1.3 only measured a state table to 6,000 rows
- `PRAGMA platform;` on the actual Pi 5 (only matters for future extension work)

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

So **seconds, not sub-second, is still the sensible cron unit**, and event time
does not change that. The horizon's ~5 ms is one extra state append and is
irreducible: the watermark has to become durable in the same transaction as the
offset. The extra *scan* is free — reading the newest event time and both drop
counts alongside the row count costs 0.26 ms more than the `count(*)` it replaced.

**Measure interleaved on this box.** A first attempt at these numbers ran the
variants one after another and gave a baseline of 100.8 ms on one run and 67.8 ms
on the next; machine drift over a two-minute run swamped the effect. Running one
trigger of each variant in turn made the deltas reproducible to ~1 ms.

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
| `metrics.py` | the three lags, per-model status, the health verdict |
| `lock.py` | one writer per catalog, and a sentence when there is not |
| `protocols.py` | `Source`, `Sink`, `StateStore`, `BatchPlan`, `BatchLimits`, `BatchContext` |
| `engine.py` | trigger loop, the transaction boundary, batch lifecycle, fault hooks |
| `trigger.py` | `AvailableNow`, `Once` |
| `state.py` | append-only offsets/watermarks/batches; DuckLake and in-memory stores |
| `lake.py` | attach, settings, inlining enforcement, snapshot introspection |
| `offsets.py` | offset encoding, and both file-offset shapes — v2 counts, v1 carried a map |
| `consumed.py` | the consumed-file set as rows: the table, the anti-join, and the two index shapes |
| `sources/files.py` | directory tailing with completion markers |
| `sinks/table.py` | append, update-by-merge, and sealed windowed append |
| `sql.py` | identifier and literal quoting |
| `config.py` | YAML deserialiser, `${VAR}` substitution — parsing isolated here |
| `registry.py` | built-in names plus dotted-path resolution |
| `cli.py` | `run`, `validate`, `models` |

Not yet written, and named in `PLAN.md`: `sources/mqtt.py` (phase 5).

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
