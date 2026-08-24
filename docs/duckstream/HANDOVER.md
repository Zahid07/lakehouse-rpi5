<!-- Paste this into a new session. It is a pointer, not a substitute:
     STATUS.md is the handover document and this just aims someone at it. -->

Read docs/duckstream/STATUS.md first — it is the handover and says exactly where
things stand, including the git state. Then read CONTEXT.md (sixteen measured
constraints; when a number there conflicts with your intuition, trust the number
or re-measure it), PLAN.md (the specification), and BUILD_GRAPH.md (the decision
record — do not reverse anything in it without reading its reasoning first,
because most of those choices are backed by a measurement).

Phases 1, 2 and 2b are complete. Phase 3 is half done: tier two executes, udf.py
is written, tier three does not execute yet. Phase 4's first item is done — the
file source's consumed set is rows in duckstream.consumed_files, not a JSON cell
in the offset. 1162 tests pass, 2 skip correctly on Windows. The working tree is
clean and everything is committed.

Environment: use .venv\Scripts\python.exe for everything. duckdb is pinned at
1.5.5 deliberately. The package is installed editable. Run the fast tests with
-m "not conformance" (1035) and the expensive ones with -m conformance (126).
Never edit a package file while the suite is running. The mutation audit lives
at tools/mutation/ and runs in throwaway git worktrees, so it can no longer
touch your checkout — read its README before adding to it, and re-check its
anchors, which go stale as the code moves.

Your task is tier three (recompute_window), which finishes phase 3. STATUS.md's
"Where to start" has the design, and it is settled by measurement rather than
open: build a file → time-range index as a *hint*, never as truth (§1.13), and
that index is now two columns on duckstream.consumed_files rather than anything
new — min_ts and max_ts beside relpath, with selection becoming
WHERE max_ts >= lo AND min_ts < hi. Window-range chunking sized from estimated
rows comes with it (§1.1).

Before writing code, read the traps at the end of STATUS.md. There are twenty
now. The two that will bite hardest are unchanged since phase 1: never put a
scalar subquery in a MERGE or JOIN condition against DuckLake (it fails only on
the second batch, so every test must run at least two), and keep sink and state
in the same catalog. Of the five phase 4 added, #16 and #17 are the ones aimed
at you — a one-file scan cannot test file identity, and the checkpoint moving is
no longer proof that progress was made.

Three working rules this project runs on, which matter more than any of the
above. Measure rather than reason — six recorded decisions have been overturned
by a measurement, including one of PLAN.md's and three of CONTEXT.md's own, and
the most recent was a number that had been *derived* from a measurement rather
than measured, which is the same failure wearing a lab coat. Refuse rather than
approximate — a plausible wrong number is the failure mode this framework exists
to remove. And audit the suite by mutation before claiming a phase is done; the
last audit found a test that could not test the thing it was named for, and a
guard that hung instead of failing.
