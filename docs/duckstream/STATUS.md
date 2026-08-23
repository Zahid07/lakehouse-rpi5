# duckstream — status and handover

**Start here if you are a new session.** This file says where the work stands, what
is proven, what is not, and what to do next. It is current as of **2026-08-23**.

Read in this order:

1. **this file** — where things are
2. `CONTEXT.md` — measured constraints and settled decisions. **Fifteen** measured
   constraints; §1.8, §1.9 and §1.10 were measured during the phase-1 build,
   §1.11 during phase 2 and §1.12 during the phase-2b cleanup, and none of them
   is derivable from anything else. §1.10, §1.11 and §1.12 are the *same*
   constraint met three times in three disguises — read them together
3. `PLAN.md` — the specification: what to build, phase by phase
4. `BUILD_GRAPH.md` — the decision record: frozen interfaces, every ratified
   decision with its reasoning, and the notes each task left for the next

**Phases 1, 2 and 2b are complete. Phase 3 is half done.** Phases 4–6 are not
started.

Phase 3 is foldability. **Tier two executes** — `avg`, `stddev`, `var`,
population and sample — maintained through sufficient statistics and proved
against the production bug in `CONTEXT.md` §4. **`udf.py` is written**, so tier
three has its tooling. **Tier three itself does not execute yet**: a
`recompute_window` model is still refused, loudly, with a message that says it
is not built rather than that it cannot be done.

Phase **2b** was not in `PLAN.md` when phases 1 and 2 were built. It was added
afterwards, because the gap it closes was a gap in the plan: the framework had a
demonstrated answer for what happens when the *process* dies and none at all for
what happens when the *data* will not process.

---

## Where to start

**There are two candidates and they are a genuine trade. Pick deliberately.**

**A — finish phase 3: tier three (`recompute_window`).** The last of the
differentiator, and the reason to build this rather than adopt Arroyo. Its
design is already settled by measurement, so this is implementation rather than
research:

- Re-derive a window from source. The rows are in files consumed long ago —
  still on disk and still identifiable, because the offset keeps a consumed-file
  *map* rather than a high-water mark.
- **Do not hand DuckDB the whole file list with a time predicate.** §1.13
  measured ~0.1 ms per file listed *whether or not it is skipped*: statistics
  pruning skips data pages, never the file open. 1.7 ms against 217 ms at 2,160
  files, and on a Pi that constant multiplies.
- So build a **file → time-range index, as a hint and never as truth**: it only
  narrows, never removes entries, and falls back to the whole list when unsure.
  Over-selecting reads extra files and is still right; under-selecting is
  silently wrong. Close to free — the watermark scan already reads each batch
  once, so grouping it by `read_parquet`'s `filename` yields per-file bounds.
- `udf.py` is done and is what tier three's aggregates call.
- Window-range chunking sized from estimated rows comes with it (§1.1: memory is
  bounded by rows in flight, never by making the UDF faster).

**B — phase 4's first item: the consumed-file map (§1.15).** The largest
obstacle to running duckstream unattended on a Pi. The offset is rewritten *in
full* every trigger: **45.7 MB after a year at one file a minute, ~65 GB of
writes a day.** Two shortcuts are already measured and rejected in §1.15 —
the reserved high-water mark (silently skips a file with an older mtime) and
compression (7.4x, and *adds* 185 ms a trigger). The fix is storing consumed
files as **rows** rather than one JSON cell, which is §1.12's rule in its most
extreme form.

  This is phase-4-sized: `plan()` and `latest_offset()` are both built around
  the map, `plan()` takes no connection, and nine test files touch it.

**My reading:** A is the product thesis; B is what decides whether it survives a
month on real hardware. If the Pi deployment is near, do B first. `status` now
reports offset size and warns past 1 MB, so the cliff is at least visible either
way.

Everything deferred out of the phase-2b sweep was written **explicitly into the
phase that owns it** rather than left as a note here: `PLAN.md` phase 4 now names
scheduling `prune()`, a migration path for a renamed aggregate, and compaction of
the open-window accumulator; phase 6 now names a soak run on real hardware, the
two unmeasured numbers, and release discipline.

## Environment

```bash
cd d:\lakehouse-rpi5
.venv\Scripts\python.exe -m pytest -q                        # 1112 passed, 1 skipped, ~5m
.venv\Scripts\python.exe -m pytest -q -m "not conformance"   # 985, the fast ones, ~65s
.venv\Scripts\python.exe -m pytest -q -m conformance         # 127, the expensive ones, ~4m
.venv\Scripts\duckstream.exe --help                          # the console script is installed
```

**Never commit while a mutation audit is running**, and never edit a package
file while the suite is running. Both rewrite files under you. A commit taken
mid-audit once pushed a deliberately mutated `engine.py`; only the audit's hash
check caught it, because the mutation is restored before the next one starts and
`git status` looks clean by then.

`.venv/` at the repo root, Python 3.14.3, `duckdb==1.5.5` **pinned exactly** —
constraints §1.5 and §1.7 are version-sensitive and the aggregate classifier reads
DuckDB's AST format. The package is installed editable (`pip install -e .`), which
is what makes the console script exist; without it one conformance test skips.

The one expected skip is correct: Windows cannot hold two filenames differing only
by case, so the POSIX half of a case-sensitivity pair cannot run here.

### Git state — read this before you touch anything

Committed: phase 1 at `693e691`, and `143e43b` "feat: add pushes for phas 2".

**`143e43b` contains a deliberate defect.** It was taken while a mutation audit
had `engine.py` mutated, and captured this:

```python
previous = policy.advance(previous, observation.max_event_ts)   # not real code
```

which makes each batch judge its own rows against its own new watermark instead
of the committed one — the silent under-counting failure. **The working tree is
already correct**; the fix is uncommitted and the diff against that commit is
exactly those two lines coming back out. It goes away with the next commit.

Everything since — phases 2, 2b, step 0, tier two, `udf.py` — is **uncommitted**.
Suggest two commits, because a line-ending normalisation is mixed in:

```bash
git diff --shortstat                    # large
git diff --shortstat --ignore-cr-at-eol # the real change
```

Commit `.gitattributes` plus the files whose *only* change is line endings
first, then the rest. `.gitattributes` is new and scopes LF to duckstream's own
directories, so this does not recur.

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

**Implemented but not demonstrated** — single-writer safety, still. Phase 2 added
a *second* memoised value on that assumption (the committed watermark, §1.11), so
there are two caches that a second writer would invalidate together. Phase 2b's
lock makes an overlap *diagnosable* but is advisory: the guarantee still rests on
DuckDB's catalog file lock, and no test runs two engines concurrently against one
catalog from two processes.

**Neither** — tiers two and three still do not execute; they are refused with a
clear phase-3 message. So "chunked equals unchunked for every `non_foldable`
model" remains asserted on the additive tier only.

## Known open items

| Item | Where | Notes |
|---|---|---|
| Quarantine is whole-batch | `engine.py` | The unit skipped is the batch, so `max_files_per_trigger: 1` is what makes it precise. Narrowing a failing batch to isolate the offending file is not implemented; it would want the source's cooperation. |
| `status` walks the landing tree for its backlog | `metrics.py` | Deliberately unbounded — a backlog reported through `max_files_per_trigger` would read `10` whether ten files or ten thousand were waiting, which is exactly the difference worth knowing. It is I/O against a tree that may be a network mount, so it is skippable (`include_backlog=False`) and a source that raises costs a number rather than the whole status. Phase 3 gives the source time-range planning, which this should then reuse. |
| A halted model re-reads and re-plans every tick | `engine.py` | It writes nothing after the first verdict, which is the expensive half, but it does redo the planning. Cheap, and it is what lets it recover unattended. |
| First tick costs 2 setup snapshots | `Engine._prepare_model` | Unchanged. Pinned by `test_setup_costs_two_snapshots_even_when_there_is_nothing_to_do`. |
| `prune` exists but nothing calls it | `state.py` | Unchanged. Phase 4 maintenance should schedule it. |
| A rewrite with identical size *and* mtime is invisible | `offsets.py` | Unchanged; inherent to the identity. |
| `FileSource.__eq__` ignores `base_dir` | `sources/files.py` | Unchanged. Conformance parity compares **output**, never source equality — keep it that way. |
| A window whose key stops reporting seals only on someone else's timestamp | `watermark.py` | The watermark is per model, not per key, so a sensor that goes silent leaves its last window open until any later-timestamped row arrives for that model. Standard for event-time engines, but it surprises people, so it is documented in the README's limits. |
| Two memoised values ride on single-writer | `engine.py` | The batch id (§1.10) and the committed watermark (§1.11). They expire together. |
| The run lock is advisory | `lock.py` | Safety still comes from DuckDB's catalog file lock. The advisory lock exists to say *what happened* in words. Never promote it to the guarantee without a filesystem story. |

## Measured this session

Constraints §1.11 through §1.15 were all measured during phases 2, 2b and 3, and
three of them overturned something that was already written down:

| | Finding | What it overturned |
|---|---|---|
| §1.11 | a watermark read costs 10.4 ms a trigger | — |
| §1.12 | reduce state in SQL; `status` was O(n) at 213 ms, now flat at 48 ms | — |
| §1.13 | statistics pruning does not save the file open: ~0.1 ms per file | my own assumption that it would make tier three cheap |
| §1.14 | `sum_sq` returns 524 for a true variance of 0.25, and 0.0 at 1e8 | **`PLAN.md`'s specified state for tier two** |
| §1.15 | the consumed-file map is rewritten in full every trigger: 45.7 MB, ~65 GB/day | — |
| §2.5 | contention is *not* structurally impossible; the catalog file lock is what saves you | **`CONTEXT.md` §2.5's own claim** |
| §1.2 | the enum is `duckdb.func`, not `duckdb.functional` | **§1.2's own code sample**, which had never been executed |

§1.10, §1.11 and §1.12 are the same constraint met three times in three
disguises. Read them together; assume there is a fourth.

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

## Traps waiting for phase 3

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
| `offsets.py` | offset encoding and the file-offset shape |
| `sources/files.py` | directory tailing with completion markers |
| `sinks/table.py` | append, update-by-merge, and sealed windowed append |
| `sql.py` | identifier and literal quoting |
| `config.py` | YAML deserialiser, `${VAR}` substitution — parsing isolated here |
| `registry.py` | built-in names plus dotted-path resolution |
| `cli.py` | `run`, `validate`, `models` |

Not yet written, and named in `PLAN.md`: `sources/mqtt.py` (phase 5).

## How the phase-2 build was run

One work unit in one session, unlike phase 1's eight parallel units with separate
feedback agents. The pieces are too tightly coupled to fan out usefully — the
watermark decides what the sink sees, and the sink's output mode decides what the
watermark is *for* — and phase 1's frozen interfaces meant there was no interface
to negotiate. The verification burden that the feedback agents carried in phase 1
was met instead by the mutation audit above, which is the part that actually
caught things.
