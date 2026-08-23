# duckstream — status and handover

**Start here if you are a new session.** This file says where the work stands, what
is proven, what is not, and what to do next. It is current as of **2026-08-23**.

Read in this order:

1. **this file** — where things are
2. `CONTEXT.md` — measured constraints and settled decisions. **Ten** measured
   constraints; §1.8, §1.9 and §1.10 were measured during the phase-1 build and
   are not derivable from anything else
3. `PLAN.md` — the specification: what to build, phase by phase
4. `BUILD_GRAPH.md` — the decision record: frozen interfaces, every ratified
   decision with its reasoning, and the notes each task left for the next

**Phase 1 is complete.** Phases 2–6 are not started.

---

## Where to start

Phase 2 is **event time**: watermarks, tumbling windows, sealing past the lateness
horizon, and `append`/`update` output modes via merge-by-key. `PLAN.md` has the
scope; the traps are at the bottom of this file.

## Environment

```bash
cd d:\lakehouse-rpi5
.venv\Scripts\python.exe -m pytest -q          # 873 passed, 1 skipped, ~2m17s
.venv\Scripts\python.exe -m pytest -q -m "not conformance"   # 788, the fast ones
.venv\Scripts\python.exe -m pytest -q -m conformance         # 86, the expensive ones
.venv\Scripts\duckstream.exe --help            # the console script is installed
```

`.venv/` at the repo root, Python 3.14.3, `duckdb==1.5.5` **pinned exactly** —
constraints §1.5 and §1.7 are version-sensitive and the aggregate classifier reads
DuckDB's AST format. The package is installed editable (`pip install -e .`), which
is what makes the console script exist; without it one conformance test skips.

The one expected skip is correct: Windows cannot hold two filenames differing only
by case, so the POSIX half of a case-sensitivity pair cannot run here.

**Not committed.** All of this is uncommitted on branch `feat/duckstream`.

## Phase 1 definition of done, item by item

`PLAN.md` set these. Each is met, with its evidence.

| Requirement | Status | Evidence |
|---|---|---|
| One file source | met | `duckstream/sources/files.py`, 74 unit tests |
| One `additive` model | met | tiers classified from DuckDB's own AST |
| `AvailableNow` trigger | met | `trigger.py`; no thread, no timer — cron owns the cadence |
| DuckLake state store and sink | met | both in the DuckLake catalog, as §1.9 requires |
| Inlining disabled | met | `ducklake_list_files` non-empty after sub-10-row batches |
| **Fault injection: kill between sink write and commit, prove no loss and no duplication** | **met** | real subprocess kills, see below |
| Both front doors, one canonical `Model` | met | parity enforced in the harness, not per test |
| Config round-trip | met | field-driven off `Model.__dataclass_fields__` |
| Foldability rejection at load | met | 6 expressions × both doors |
| One trigger, one snapshot | met | asserted as a delta, including from a fresh process |

### The fault-injection result, in detail

This is the load-bearing claim, so it is worth stating precisely what was proven.

Every kill is a real child process that really dies: `os._exit(9)` inside a fault
hook, no `finally`, no rollback, no `close()`, the DuckLake catalog abandoned
mid-transaction. Killed at `after_sink_write`, at `before_commit`, and at
`after_commit`; then **seven scripted kills across a six-batch drain**, restarting
after each death.

After the drain, the mart equals a full recompute and `sum(n)` equals the landed
row count exactly. More importantly: **every snapshot in the history is itself a
full recompute of exactly the files the offset recorded as consumed at that same
snapshot**, and the consumed set is monotone across snapshots.

That check is only possible because of §1.9 — since a transaction cannot span two
attached databases, the offset necessarily shares its snapshot with the rows it
checkpoints, so both sides can be read `AT (VERSION => n)` at the same instant of
catalog history. A final state can be right by luck after seven kills; a history
in which every intermediate state is also exactly correct cannot.

### The suite was itself audited by mutation testing

A green suite that tests the wrong invariant is worse than no suite, so eleven
deliberate defects were introduced into the package to check each one turns the
suite red. **Ten of eleven did.**

| Mutation | Result |
|---|---|
| `min` folds with `+` instead of `least` | red, 13.5s |
| `sum` folds as `(t.c + s.c)/2` — the CONTEXT.md §4 production bug | red, 13.0s |
| merge `ON` uses `=` instead of `IS NOT DISTINCT FROM` | red — behaviourally, 3 duplicate NULL rows vs 1 folded |
| idle trigger writes a checkpoint anyway | red, on the snapshot count itself |
| DuckLake inlining re-enabled | red |
| offset committed in a second transaction | red, 0.66s |
| **offset checkpointed before the sink write** | red — **via the fault-injection test itself** |
| YAML door left one step behind | red — from `Parity.assert_agree()`, no per-test assertion involved |
| foldability refusal deferred from load to write time | red, 0.09s |
| additive fold made non-NULL-safe | green at the time — **the one hole, since closed** |

The offset-before-sink-write row is the reassuring one: it failed through the kill
assertions rather than merely through snapshot counting, so those assertions are
load-bearing rather than decorative.

The kills were confirmed real four independent ways: `os._exit` makes the child's
`finally: con.close()` structurally unreachable; a real kill returns `rc=9` with
**empty stdout**, so the child never reached its final print; and wrong-reason
deaths are rejected, because the assertion requires both `returncode == 9` *and* a
`FAULT <point>` line in stderr — an import error or a typo'd fault point fails
both. Ground truth was confirmed independent: no conformance test calls
`aggregation_sql`, `merge_sql` or `fold_expression`, so the suite is not comparing
duckstream to itself.

The one hole was then closed, by
`test_a_null_measure_delta_does_not_erase_a_stored_value`.
It runs through both doors over three keys and three batches, exercising the
identity from both sides: a correct total survives an all-NULL batch, a NULL
stored value plus a real delta yields the delta, and a key that is NULL in every
batch stays NULL rather than becoming 0 — which is what a careless "fix" of
`coalesce(t,0) + coalesce(s,0)` would wrongly produce. Its teeth were verified:
against the non-NULL-safe mutant the whole conformance directory gives
`1 failed, 82 passed`, so the new test is the *only* detector and nothing was
covering it by accident.

The parity escape hatch was closed too. An autouse guard in
`tests/conformance/conftest.py` requires every pipeline-driving test to either go
through `Parity`, parametrise over `DOORS`, or appear in
`SINGLE_DOOR_EXEMPTIONS` with a reason. Four tests are exempt and each genuinely
needs one door. Two audit tests stop the list rotting in either direction.

**Methodology note for anyone repeating this:** `duckstream/` and `tests/` are
entirely **untracked** in git, so a `git diff`-based revert check gives false
comfort. Verify integrity by hashing the files.

## Demonstrated versus merely implemented

Be careful with this distinction when reporting on the project.

**Demonstrated** — exactly-once for a replayable file source into a DuckLake mart
under real process kills at every point in the batch lifecycle, verified against
snapshot history; one trigger = one snapshot; idle triggers are free and write
nothing; inlining genuinely off; the additive tier folded correctly through
`WHEN MATCHED`, including NULL grouping keys; batch-boundary independence
(chunked equals unchunked); load-time refusal of an additive strategy over a
non-foldable aggregate, identically through both doors; byte-for-byte parity
between the Python API and the YAML/CLI path.

**Implemented but not demonstrated** — single-writer safety. The memoised batch id
and the absence of a lock both assume a single writer, and neither is tested under
concurrency, because v1 makes concurrency structurally impossible. If
`ProcessingTime` or a second writer ever arrives, both assumptions need revisiting
(see §1.10).

**Neither** — everything event-time. No watermarks, no lateness horizon, no window
sealing, no drop-and-count of late data. `PLAN.md`'s "late arrival within the
horizon" and "watermark semantics" are untested *because unimplemented*. Tiers two
and three are refused with a clear phase-3 message rather than executed, so
"chunked equals unchunked for every `non_foldable` model" is asserted on the
additive tier only — the only tier where it is reachable.

## Known open items

| Item | Where | Notes |
|---|---|---|
| `rows_out` is never recorded | `duckstream/engine.py`, `record_batch_end` called without it | Column is always NULL. Not a correctness issue, but `status` and lag reporting will want it. Deliberate: obtaining it means running the aggregation twice or coupling to a sink internal. `metrics.py` should own it. |
| First tick costs 2 setup snapshots | `Engine._prepare_model` runs before the plan is known empty | So "an idle trigger adds zero snapshots" holds only *after* setup. Pinned by `test_setup_costs_two_snapshots_even_when_there_is_nothing_to_do`. |
| `prune` exists but nothing calls it | `duckstream/state.py` | State grows three rows per trigger. Phase 4 maintenance should schedule it. |
| A rewrite with identical size *and* mtime is invisible | `duckstream/offsets.py` | Inherent to `(size, mtime_ns)` identity, not a bug. Pinned by `test_rewrite_with_identical_size_and_mtime_is_not_detected`, which names itself as the test to change if a content digest is ever added. |
| `FileSource.__eq__` ignores `base_dir` | `duckstream/sources/files.py` | Two sources reading different trees can compare equal. Required so the config round-trip works. Conformance parity compares **output**, never source equality — keep it that way. |

## Still unmeasured

From `CONTEXT.md` §6, and worth doing before publishing an operating envelope:

- **memory ratio per tier** — bisect `memory_limit` against `max_rows_per_trigger`
  and publish the ratio so users can size the knob
- **UDF parallelism penalty** — quantify the single-thread cost (§2.1) so the docs
  can say when to split across processes
- `PRAGMA platform;` on the actual Pi 5 (only matters for future extension work)

The micro-batch latency floor **is** now measured — §1.8 and §1.10.

## Operating envelope, measured

| | |
|---|---|
| idle trigger, writes nothing | ~1.3 ms |
| committing trigger | ~15 ms |
| full trigger with state | ~25.7 ms |
| process cold start under cron | ~235 ms |

So **seconds, not sub-second, is the sensible cron unit.** A future
`ProcessingTime` daemon skips the 235 ms and lands near the commit floor.

## Traps waiting for phase 2

1. **`test_watermarks_are_not_implemented_in_phase_one` exists on purpose.** It
   fails the day a lateness field appears, so the missing coverage gets noticed
   instead of quietly shipping. When it fails, that is the test doing its job —
   replace it with real watermark coverage rather than deleting it.
2. **Never put a scalar subquery in a MERGE or JOIN condition against DuckLake**
   (§1.5). It fails with `Out of buffer`, and **only on the second batch** — the
   first one to take the `WHEN MATCHED` branch. Compute bounds in Python and inline
   them as literals. Every suite must run at least two batches.
3. **Sink and state must stay in the same catalog** (§1.9). A transaction cannot
   span attached databases, so splitting them makes exactly-once unformable, not
   merely slower.
4. **Keep per-trigger state append-only** (§1.10). A matching DuckLake `DELETE`
   costs ~26 ms because it writes a tombstone. Guarding it with an existence probe
   was measured and is a *net regression*.
5. **Do not `SELECT *` from `ducklake_snapshots()`** — its `TIMESTAMP WITH TIME
   ZONE` column needs `pytz`, which is not a dependency. Use `lake.snapshots()`.
6. **The window column is `window_ts` at every grain**, and `key` must contain it
   when `grain` is set, or idempotency silently breaks.
7. **Any fixture writing a landing tree must write atomically** — temp path,
   rename, *then* the marker. A fixture that appends to an already-planned file
   produces a genuine double-count that looks like an engine bug.

## Package layout

| Path | Contents |
|---|---|
| `model.py` | `Model`, load-time validation, the invariants — the canonical representation |
| `aggregates.py` | foldability tiers, classification from DuckDB's AST, fold SQL |
| `protocols.py` | `Source`, `Sink`, `StateStore`, `BatchPlan`, `BatchLimits`, `BatchContext` |
| `engine.py` | trigger loop, the transaction boundary, batch lifecycle, fault hooks |
| `trigger.py` | `AvailableNow`, `Once` |
| `state.py` | append-only offsets/watermarks/batches; DuckLake and in-memory stores |
| `lake.py` | attach, settings, inlining enforcement, snapshot introspection |
| `offsets.py` | offset encoding and the file-offset shape |
| `sources/files.py` | directory tailing with completion markers |
| `sinks/table.py` | append and update-by-merge |
| `sql.py` | identifier and literal quoting |
| `config.py` | YAML deserialiser, `${VAR}` substitution — parsing isolated here |
| `registry.py` | built-in names plus dotted-path resolution |
| `cli.py` | `run`, `validate`, `models` |

Not yet written, and named in `PLAN.md`: `watermark.py` (phase 2), `windows.py`
(phase 2), `udf.py` (phase 3), `metrics.py` (`status` and lag), `sources/mqtt.py`
(phase 5).

## How the phase-1 build was run

Eight work units, each implemented by one agent and then verified by a separate
feedback agent that could report defects but not fix them. That separation earned
its cost: W1 shipped green and was failed on two blockers, W2a shipped with two
*silent* wrong answers, W2c shipped a type-mismatch hole and retracted one of its
own claims. `BUILD_GRAPH.md` records every ratified decision with its reasoning —
read it before reversing anything, because most of the non-obvious choices there
are backed by a measurement.
