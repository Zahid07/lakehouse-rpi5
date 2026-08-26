"""Deliberate defects for phases 3, 4 and 5.

Each entry is a plausible wrong decision somebody could actually make, not a
syntax error. `find` must appear exactly once in `file`; it is replaced by
`repl`, the suite is run, and the mutation has to turn it red.

`suite` picks the cheapest run that should catch it:
  "fast"   -- pytest -m "not conformance"
  "conf"   -- pytest -m conformance
  "all"    -- everything

Choosing too cheap a suite reports a false green, so anything about snapshot
atomicity or DuckLake behaviour is on a conformance run. Choosing too
*expensive* a one reports a false hole that looks exactly like a real one -- see
the README, which records the two structural reasons a conformance run can be
blind to a defect on purpose.

`expect_survives` marks the rare mutation that must **not** turn the suite red,
with a sentence saying why. There are two: widening the tier-three file index
(it is a hint, so selecting more files must change no answer), and reversing the
landing scan's sort order (planning re-sorts, so scan order is not
load-bearing).

`skip="..."` excuses a mutation that is *live* here but whose fixture cannot be
built on this machine -- a directory symlink, or an installed `paho-mqtt`. That
is different from `inert_on`, which is for a mutation this OS makes a no-op.
Both are excluded from the denominator and listed by name; see the README.
"""

MUTATIONS = [
    # -- the anti-join: identity ------------------------------------------
    dict(
        name="the anti-join ignores size, so a rewrite keeping its mtime is lost",
        file="duckstream/consumed.py",
        find='                f\'AND c."size" = s."size" \'\n                f"AND c.mtime_ns = s.mtime_ns "',
        repl='                f"AND c.mtime_ns = s.mtime_ns "',
        suite="fast",
    ),
    dict(
        name="the anti-join ignores mtime, so a rewrite keeping its size is lost",
        file="duckstream/consumed.py",
        find='                f\'AND c."size" = s."size" \'\n                f"AND c.mtime_ns = s.mtime_ns "\n',
        repl='                f\'AND c."size" = s."size" \'\n',
        suite="fast",
    ),
    dict(
        name="the anti-join ignores model_name, so models steal each other's files",
        file="duckstream/consumed.py",
        find='                f"ON c.model_name = ? "',
        repl='                f"ON (c.model_name = ? OR TRUE) "',
        suite="fast",
    ),
    # -- the anti-join: the mtime window ----------------------------------
    dict(
        name="the mtime window is one nanosecond too narrow at the bottom",
        file="duckstream/consumed.py",
        find='window = f"AND c.mtime_ns BETWEEN {min(mtimes)} AND {max(mtimes)}"',
        repl='window = f"AND c.mtime_ns BETWEEN {min(mtimes) + 1} AND {max(mtimes)}"',
        suite="fast",
    ),
    dict(
        name="the mtime window narrows to the newest file only",
        file="duckstream/consumed.py",
        find='window = f"AND c.mtime_ns BETWEEN {min(mtimes)} AND {max(mtimes)}"',
        repl='window = f"AND c.mtime_ns BETWEEN {max(mtimes)} AND {max(mtimes)}"',
        suite="fast",
    ),
    # -- case folding -------------------------------------------------------
    dict(
        name="relpath_fold is written only where it is read, breaking portability",
        file="duckstream/consumed.py",
        find='                "relpath_fold": pa.array(\n                    [path.casefold() for path in paths], type=pa.string()\n                ),',
        repl='                "relpath_fold": pa.array(\n                    [path.casefold() if CASE_INSENSITIVE_PATHS else path\n                     for path in paths], type=pa.string()\n                ),',
        suite="fast",
    ),
    dict(
        name="the join always uses relpath, so a case-only rename is read twice",
        file="duckstream/consumed.py",
        find='        join_on = "relpath_fold" if CASE_INSENSITIVE_PATHS else "relpath"',
        repl='        join_on = "relpath"',
        suite="fast",
    ),
    dict(
        name="the join always folds, so two real files on POSIX become one",
        file="duckstream/consumed.py",
        find='        join_on = "relpath_fold" if CASE_INSENSITIVE_PATHS else "relpath"',
        repl='        join_on = "relpath_fold"',
        suite="fast",
        # On a case-insensitive filesystem `join_on` is already "relpath_fold",
        # so this mutation applies textually and changes nothing. Reporting that
        # as SURVIVED would invent a hole; reporting it as red would invent
        # coverage. It is neither, and it can only be audited on POSIX.
        inert_on=("nt",),
    ),
    # -- the offset shape ---------------------------------------------------
    dict(
        name="a v2 offset answers 'nothing consumed' instead of refusing",
        file="duckstream/offsets.py",
        find="        version = FileOffset.version_of(offset)\n        if version >= FileOffset.ROWS_VERSION:\n            raise DuckstreamError(",
        repl="        version = FileOffset.version_of(offset)\n        if version >= FileOffset.ROWS_VERSION:\n            return {}\n        if False:\n            raise DuckstreamError(",
        suite="fast",
    ),
    dict(
        name="the checkpoint count never advances, so the drain loop re-reads",
        file="duckstream/consumed.py",
        find="        return FileOffset.rows(FileOffset.entry_count(start) + len(included))",
        repl="        return FileOffset.rows(FileOffset.entry_count(start))",
        suite="fast",
    ),
    dict(
        name="entry_count forgets a v1 map, so a migration restarts the count at 0",
        file="duckstream/offsets.py",
        find="        return len(FileOffset.consumed(offset))",
        repl="        return 0",
        suite="all",
    ),
    # -- recording ----------------------------------------------------------
    dict(
        name="a committed batch records no consumed files at all",
        file="duckstream/engine.py",
        find="                    if index is not None:\n                        recorded = index.record(",
        repl="                    if index is None:\n                        recorded = index.record(",
        suite="fast",
    ),
    dict(
        name="the consumed rows are written after the commit, not inside it",
        file="duckstream/engine.py",
        find="            self.state.commit(self.con, {model.name: plan.end}, watermarks)\n            self._next_ids[model.name] = batch_id + 1",
        repl="            self.state.commit(self.con, {model.name: plan.end}, watermarks)\n            if index is not None:\n                index.record(batch_id, plan.payload, start=plan.start, end=plan.end)\n            self._next_ids[model.name] = batch_id + 1",
        suite="conf",
        note="paired with dropping the in-transaction write below",
        also=dict(
            file="duckstream/engine.py",
            find="""                    if index is not None:
                        recorded = index.record(
                            ctx.batch_id,
                            plan.payload,
                            start=plan.start,
                            end=plan.end,
                            bounds=bounds,
                        )""",
            repl="                    recorded = 0 if index is None else -1",
        ),
    ),
    dict(
        name="the plan declares no entries, so nothing can be recorded",
        file="duckstream/sources/files.py",
        find="            ENTRIES_KEY: included,",
        repl="            ENTRIES_KEY: {},",
        suite="fast",
    ),
    # -- quarantine ---------------------------------------------------------
    dict(
        name="quarantine advances the offset without recording what it skipped",
        file="duckstream/state.py",
        find="            if consumed is not None:\n                consumed.record(",
        repl="            if consumed is None:\n                consumed.record(",
        suite="all",
    ),
    # -- prune --------------------------------------------------------------
    dict(
        name="prune treats consumed_files as ordinary history and trims it",
        file="duckstream/state.py",
        find='                ("batches", self.batches_table),\n            ):',
        repl='                ("batches", self.batches_table),\n                ("consumed_files", self.consumed_files_table),\n            ):',
        suite="fast",
    ),
    # -- migration ----------------------------------------------------------
    dict(
        name="migration writes the new offset but not the rows",
        file="duckstream/state.py",
        find="            adopted = index.append(self._resolve_batch_id(con, model_name), entries)",
        repl="            adopted = len(entries)",
        suite="all",
    ),
    dict(
        name="migration records the rows but never writes the new offset",
        file="duckstream/state.py",
        find="""            self._append_offset(
                con,
                model_name,
                offset,
                batch_id,
                now,
                attempt=0 if position is None else position.attempt,
                failed_at=None if position is None else position.failed_at,
                error=None if position is None else position.error,
            )""",
        repl="            pass",
        suite="all",
    ),
    dict(
        name="migration hands a failing model a fresh attempt budget",
        file="duckstream/state.py",
        find="                attempt=0 if position is None else position.attempt,",
        repl="                attempt=0,",
        suite="fast",
    ),
    dict(
        name="a v1 offset is never recognised, so the whole tree replays",
        file="duckstream/sources/files.py",
        find="        if FileOffset.version_of(offset) >= FileOffset.ROWS_VERSION:\n            return None",
        repl="        return None\n        if FileOffset.version_of(offset) >= FileOffset.ROWS_VERSION:\n            return None",
        suite="all",
    ),
    # -- injection ----------------------------------------------------------
    dict(
        name="the engine never injects the index, so plan falls back to the map",
        file="duckstream/engine.py",
        find="        if not self._takes_consumed(model.source):\n            return None",
        repl="        return None\n        if not self._takes_consumed(model.source):\n            return None",
        suite="fast",
    ),
    dict(
        # The wiring, as distinct from the two halves it wires. Mutations 16-19
        # hit `adopt_consumed` and `migrate_offset`, both of which unit tests
        # cover directly; this one asks whether the engine ever *calls* them.
        name="the engine never migrates, so a v1 catalog plans against no rows",
        file="duckstream/engine.py",
        find="        outcome = migrate(position.offset)\n        if outcome is None:\n            return position",
        repl="        outcome = migrate(position.offset)\n        return position",
        suite="fast",
    ),
    dict(
        name="a batch may commit without recording what it consumed",
        file="duckstream/engine.py",
        find="""        if index is None or recorded is not None:
            return""",
        repl="""        if True:
            return""",
        suite="fast",
    ),
    dict(
        name="status reads the consumed table before the offset has migrated",
        file="duckstream/metrics.py",
        find="""            if migrate(position.offset) is not None:
                return None""",
        repl="""            if False:
                return None""",
        suite="fast",
    ),
    dict(
        # The guard this audit produced. Its own mutation, because a check
        # added in response to a survivor is exactly the kind of code that
        # gets quietly disabled later.
        name="the checkpoint count is never checked against the rows written",
        file="duckstream/consumed.py",
        find="        if start is not None or end is not None:\n            self._verify(written, start, end)",
        repl="        if False:\n            self._verify(written, start, end)",
        suite="fast",
    ),
    dict(
        name="the injection test accepts any source, so a plain source gets a kwarg",
        file="duckstream/engine.py",
        find='        parameter = signature.parameters.get("consumed")\n        if parameter is not None:\n            return parameter.kind is not parameter.POSITIONAL_ONLY',
        repl='        parameter = signature.parameters.get("consumed")\n        if parameter is None:\n            return True\n        if parameter is not None:\n            return parameter.kind is not parameter.POSITIONAL_ONLY',
        suite="fast",
    ),
    # -- phase 3, tier three: recompute_window ------------------------------
    #
    # The tier with no decomposition, so every defect here is a *silent* wrong
    # answer rather than a failure. CONTEXT.md section 4's FFT mart is the
    # reference: it held 51 spectrum bins where the truth was 201, and nothing
    # about it failed.
    dict(
        # THE tier-three bug, exactly as production produced it.
        name="the recompute aggregates the batch instead of the whole window",
        file="duckstream/engine.py",
        find="""            files_for=lambda lo, hi: (
                [] if index is None else index.overlapping(lo, hi)
            ),""",
        repl="""            files_for=lambda lo, hi: [],""",
        suite="conf",
        note="reads only the files this batch consumed -- section 4's mart",
    ),
    dict(
        name="the recompute merges into the target instead of replacing the range",
        file="duckstream/sinks/table.py",
        find="""        con.execute(self.clear_range_sql(lo, hi))
        return _affected(con.execute(self.insert_sql(batch_view, model)))""",
        repl="""        return _affected(con.execute(self.insert_sql(batch_view, model)))""",
        suite="conf",
        note="every recompute appends a second row for the window",
    ),
    dict(
        name="a chunk is not narrowed to its window range, so rows are counted twice",
        file="duckstream/engine.py",
        find="""            f"WHERE {column} >= {quote_literal(chunk.lo)} "
            f"  AND {column} < {quote_literal(chunk.hi)}\"""",
        repl="""            f"WHERE {column} >= {quote_literal(chunk.lo)} "
            f"  AND {column} < {quote_literal(chunk.hi)} OR TRUE\"""",
        suite="conf",
        note="only visible when chunking actually splits the touched span",
    ),
    dict(
        # The measured decision in CONTEXT.md 1.17, from the other side: an
        # unmeasured file must be selected by every range, never by none.
        name="an unmeasured file is stored at NULL bounds instead of the widest",
        file="duckstream/consumed.py",
        find="""            lows.append(UNKNOWN_MIN if low is None else low)
            highs.append(UNKNOWN_MAX if high is None else high)""",
        repl="""            lows.append(low)
            highs.append(high)""",
        suite="fast",
        note=(
            "NULL fails the range test, so the file is silently never re-read. "
            "`fast`, not `conf`: every file in a conformance scenario has a real "
            "time column, so none of them is ever unmeasured and the mutation is "
            "inert there. Ran as `conf` first and survived for exactly that "
            "reason -- the suite was fine, the suite *choice* was not."
        ),
    ),
    dict(
        name="the file index is treated as truth, so the batch's own files are dropped",
        file="duckstream/engine.py",
        find="            own=self._own_files(plan, bounds),",
        repl="            own=[],",
        suite="conf",
        note=(
            "Correct only if the index can already see this batch's rows, and it "
            "cannot -- they are written in the same transaction. Drops them from "
            "the read *and* from the row estimate, which is how the second half "
            "of this was found: a test that could not construct a second chunk."
        ),
    ),
    dict(
        name="the overlap test is closed at the top, so a window steals the next one's file",
        file="duckstream/consumed.py",
        find="            f\"  AND min_ts < {quote_literal(hi)} \"",
        repl="            f\"  AND min_ts <= {quote_literal(hi)} \"",
        suite="conf",
        note="over-selection, so it must NOT change any answer -- see below",
        expect_survives=(
            "The index only narrows. A wider overlap test reads more files and "
            "gets the same numbers, so a red here would mean the suite had "
            "started depending on the hint for correctness rather than for cost."
        ),
    ),
    dict(
        name="touched windows include undated rows, so a NULL window is recomputed",
        file="duckstream/recompute.py",
        find="f\"FROM {quote_ident(view)} WHERE {column} IS NOT NULL ORDER BY w\"",
        repl="f\"FROM {quote_ident(view)} ORDER BY w\"",
        suite="fast",
        note=(
            "Both halves, deliberately. Removing only the SQL filter survives, "
            "and reading that survivor twice is what the README asks for: the "
            "suite is not holed, the guard is simply belt-and-braces and the "
            "Python comprehension still drops the NULL. A mutation of one half "
            "of a redundant pair tests nothing, so this one removes both."
        ),
        also=dict(
            file="duckstream/recompute.py",
            find="    return [row[0] for row in rows if row[0] is not None]",
            repl="    return [row[0] for row in rows]",
        ),
    ),
    dict(
        name="chunks are sized by window count rather than by estimated rows",
        file="duckstream/recompute.py",
        find="        if pending and (estimate is None or estimate > max_rows):",
        repl="        if pending and len(trial) > max_rows:",
        suite="fast",
        note=(
            "PLAN.md: window density varies, so a window count is no memory "
            "bound. `fast`, not `conf`, and the reason is worth keeping: "
            "conformance asserts chunked *equals* unchunked, so a change to how "
            "chunks are sized is precisely what it must NOT be able to see. Only "
            "a unit test on the planner can catch this. Ran as `conf` first and "
            "survived for that reason."
        ),
    ),
    dict(
        name="an unknown row estimate is treated as zero rather than as unbounded",
        file="duckstream/recompute.py",
        find="""    total = 0
    for candidate in selected:
        if candidate.n_rows is None:
            return None""",
        repl="""    total = 0
    for candidate in selected:
        if candidate.n_rows is None:
            continue""",
        suite="fast",
        note="under-estimating is the direction that OOMs",
    ),
    dict(
        name="a plain batch view is accepted as a whole window",
        file="duckstream/sinks/table.py",
        find="""        window_range = getattr(ctx, "window_range", None)
        if window_range is None:""",
        repl="""        window_range = getattr(ctx, "window_range", None)
        if False:""",
        suite="fast",
    ),
    dict(
        name="an upgraded catalog keeps NULL bounds, so consumed files stop being read",
        file="duckstream/state.py",
        find="""            filling = {c: v for c, v in backfill.items() if c in added}""",
        repl="""            filling = {}""",
        suite="fast",
        note="the migration hazard: silently narrows every recompute after upgrade",
    ),
    dict(
        name="a recompute's temp views are only dropped when every chunk succeeds",
        file="duckstream/engine.py",
        find="""        written = 0
        for chunk in chunks:""",
        repl="""        written = 0
        _deferred = []
        for chunk in chunks:""",
        suite="fast",
        note=(
            "Equivalent on the happy path and not on the unhappy one: a chunk "
            "that raises strands every view its predecessors made, and a model "
            "that keeps failing strands another set per retry. Written this way "
            "on the second attempt. The first collected into a local list and "
            "extended `extra` immediately afterwards, which differs only if "
            "`_range_view` itself raises -- so it survived while testing "
            "nothing. A mutation that does not implement its own name is the "
            "other thing a survivor can mean, and this is the second time that "
            "has happened in this project."
        ),
        also=dict(
            file="duckstream/engine.py",
            find="            scoped = self._range_view(model, plan, relpaths, chunk, extra)",
            repl="            scoped = self._range_view(model, plan, relpaths, chunk, _deferred)",
        ),
    ),
    dict(
        name="a source that cannot resolve its paths gets a guessed relative path",
        file="duckstream/engine.py",
        find='''        resolve = getattr(source, "absolute_paths", None)
        if not callable(resolve):''',
        repl='''        resolve = getattr(source, "absolute_paths", None)
        if False:''',
        suite="fast",
        note="a guessed path may open cleanly and hold the wrong rows",
    ),
    dict(
        name="undated rows are dropped by a recompute without being counted",
        file="duckstream/engine.py",
        find="""            if _recomputes(model):
                return self._observe_undated(model, view)""",
        repl="""            if False:
                return self._observe_undated(model, view)""",
        suite="fast",
        note="CONTEXT.md: dropped *and counted*, never silently absorbed",
    ),
    # -- phase 4: the landing-tree scan ------------------------------------
    #
    # CONTEXT.md 1.20 made this a pure optimisation, so every mutation here is
    # about it having quietly stopped being one. A scan that returns a slightly
    # different set of paths, or slightly different identities, does not fail --
    # it re-reads files or skips them, silently.
    dict(
        name="the scan's prefix keeps a leading './' so no path ever matches",
        file="duckstream/sources/files.py",
        find='        stack: list[tuple[str, Path]] = [("", root)]',
        repl='        stack: list[tuple[str, Path]] = [("./", root)]',
        suite="fast",
    ),
    dict(
        name="the walk drops the prefix, so nested files collide on their bare name",
        file="duckstream/sources/files.py",
        find='                stack.append((f"{prefix}{entry.name}/", Path(entry.path)))',
        repl='                stack.append(("", Path(entry.path)))',
        suite="fast",
        note="two directories holding part.parquet become one entry",
    ),
    dict(
        name="the walk descends into symlinked directories, unlike os.walk",
        file="duckstream/sources/files.py",
        find="                        if entry.is_dir(follow_symlinks=False):",
        repl="                        if entry.is_dir():",
        suite="fast",
        # Not inert here -- the branch really does differ -- but the *fixture*
        # cannot be built on this box: Windows refuses to create a directory
        # symlink without a privilege it does not grant, so the test that would
        # catch this skips. It ran as SURVIVED once for exactly that reason,
        # which is a false hole; declaring it is the honest alternative to
        # leaving a survivor that looks like missing coverage.
        # `test_the_walk_does_not_descend_into_symlinked_directories` is red on
        # any platform that can make one.
        requires="dirsymlink",
    ),
    dict(
        name="a non-recursive source walks the whole tree anyway",
        file="duckstream/sources/files.py",
        find="""            if not self.recursive:
                return""",
        repl="""            if False:
                return""",
        suite="fast",
    ),
    dict(
        name="the scan stops sorting, so a replayed batch need not match",
        file="duckstream/sources/files.py",
        find="            files.sort(key=lambda entry: entry.name)",
        repl="            files.sort(key=lambda entry: entry.name, reverse=True)",
        suite="fast",
        note=(
            "Ordering is re-imposed when the batch is planned, so this may well "
            "be a `held` rather than a red -- read the verdict before deciding "
            "which. It is here because 'the scan order does not matter' is a "
            "claim worth having checked rather than assumed."
        ),
        expect_survives=(
            "Planning sorts candidates by (mtime, relpath), so scan order is "
            "not load-bearing and reversing it must change no answer. A red "
            "here would mean something downstream had come to depend on it."
        ),
    ),
    dict(
        name="_is_ready ignores the walk's entries and reports every directory ready",
        file="duckstream/sources/files.py",
        find="""            else:
                return False""",
        repl="""            else:
                return True""",
        suite="fast",
        note="an unmarked directory would be read as if it were complete",
    ),
    # -- phase 5: the landing writer ---------------------------------------
    #
    # The guarantee is at-least-once, and every defect here breaks it in the
    # quiet direction: the broker is told a message was handled and it was not.
    # Nothing observes that until a process dies with a full buffer.
    dict(
        name="the marker is written before the data file, not after",
        file="duckstream/landing.py",
        find="""        os.replace(temp, target)
        if self.marker is not None:
            (directory / self.marker).write_bytes(b"")""",
        repl="""        if self.marker is not None:
            (directory / self.marker).write_bytes(b"")
        os.replace(temp, target)""",
        suite="fast",
        note="a reader may then plan a directory whose data file is not there yet",
    ),
    dict(
        name="tokens are released before the batch is durable",
        file="duckstream/landing.py",
        find="""        rows = len(self._records)
        self._write(temp, self._records)""",
        repl="""        rows = len(self._records)
        tokens = tuple(self._tokens)
        self._records = []
        self._tokens = []
        self._opened_at = None
        return LandedBatch(
            directory=directory, path=target, rows=rows, tokens=tokens
        )""",
        suite="fast",
        note="at-most-once wearing at-least-once's clothes",
    ),
    dict(
        name="a failed write clears the buffer, so the records are lost",
        file="duckstream/landing.py",
        find="""        rows = len(self._records)
        self._write(temp, self._records)
        # Atomic on POSIX and on Windows: a reader sees the whole file or no""",
        repl="""        rows = len(self._records)
        _doomed, self._records = self._records, []
        self._write(temp, _doomed)
        # Atomic on POSIX and on Windows: a reader sees the whole file or no""",
        suite="fast",
        note="a full disk becomes a data loss instead of a delay",
    ),
    dict(
        name="the writer keeps only the first record's keys, dropping later fields",
        file="duckstream/landing.py",
        find="""        columns: dict[str, None] = {}
        for record in records:
            for key in record:
                columns.setdefault(key, None)""",
        repl="""        columns: dict[str, None] = {}
        for record in records[:1]:
            for key in record:
                columns.setdefault(key, None)""",
        suite="fast",
        note=(
            "what pa.Table.from_pylist does on its own, and it is silent -- the "
            "write succeeds and the column is simply absent. Found by a test, "
            "not by review."
        ),
    ),
    dict(
        name="an empty flush lands an empty marked directory",
        file="duckstream/landing.py",
        find="""        if not self._records:
            return None

        directory = self._root / self._directory_name()""",
        repl="""        if False:
            return None

        directory = self._root / self._directory_name()""",
        suite="fast",
    ),
    dict(
        name="two flushes can share a directory, so one marker covers both",
        file="duckstream/landing.py",
        find="        directory.mkdir(parents=True, exist_ok=False)",
        repl="        directory.mkdir(parents=True, exist_ok=True)",
        suite="fast",
        note=(
            "on its own this only weakens a guard; paired with a constant name "
            "it is the trap-7 shape -- a marked directory gaining a file."
        ),
        also=dict(
            file="duckstream/landing.py",
            find='''        stamp = _utcnow().strftime("%Y%m%dT%H%M%S_%f")
        return f"{stamp}_{uuid.uuid4().hex[:8]}"''',
            repl='        return "batch"',
        ),
    ),
    dict(
        name="the time trigger measures the newest record, not the oldest",
        file="duckstream/landing.py",
        find="""        if self._opened_at is None:
            self._opened_at = _utcnow()""",
        repl="        self._opened_at = _utcnow()",
        suite="fast",
        note="a steady topic just under the threshold would never flush at all",
    ),
    dict(
        name="a writer with no flush trigger at all is accepted",
        file="duckstream/landing.py",
        find="        if flush_rows is None and flush_seconds is None:",
        repl="        if False:",
        suite="fast",
        note="looks like working right up until the buffer exhausts memory",
    ),
    dict(
        name="the MQTT adapter acknowledges on arrival, like paho's default",
        file="duckstream/sources/mqtt.py",
        find="""        with self._lock:
            self.writer.add(record, token=message)
            if self.writer.due():
                self._flush_locked()""",
        repl="""        self._ack(message)
        with self._lock:
            self.writer.add(record, token=message)
            if self.writer.due():
                self._flush_locked()""",
        suite="fast",
        note="the exact defect the reference subscriber.py has",
    ),
    dict(
        name="manual_ack is never set, so paho acks everything itself",
        file="duckstream/sources/mqtt.py",
        find="        client.manual_ack = True",
        repl="        client.manual_ack = False",
        suite="fast",
        note=(
            "Was excused as needing paho installed, and is not excused any "
            "more. When the capability probe first let it run it **survived**: "
            "`MqttLandingWriter.connect()` had no test at all, because every "
            "fixture assigns `_client` directly, so the one line carrying the "
            "at-least-once guarantee was never executed. The test that closes "
            "it installs a recording client through `sys.modules`, so it needs "
            "no broker and no paho -- verified by blocking `paho` and watching "
            "this mutation still turn the suite red. Auditable everywhere now."
        ),
    ),
    dict(
        name="a reconnect does not resubscribe",
        file="duckstream/sources/mqtt.py",
        find="""        for topic in self.topics:
            client.subscribe(topic, qos=self.qos)""",
        repl="""        if not getattr(self, "_subscribed_once", False):
            self._subscribed_once = True
            for topic in self.topics:
                client.subscribe(topic, qos=self.qos)""",
        suite="fast",
        note="comes back connected, healthy-looking, and receiving nothing",
    ),
    dict(
        name="an undecodable message is buffered as an empty row",
        file="duckstream/sources/mqtt.py",
        find="""        if record is None:
            self.undecodable += 1
            # Acked anyway: it is not going to decode on redelivery either, and
            # leaving it unacked makes the broker replay it for ever. The count
            # above is the record that it happened.
            self._ack(message)
            return""",
        repl="""        if record is None:
            record = {}
            self.undecodable += 1""",
        suite="fast",
        note=(
            "Written this way on the second attempt. The first assigned to "
            "`record` and left the early return in place, so it changed nothing "
            "and survived while testing nothing -- the third time that has "
            "happened in this project."
        ),
    ),
    dict(
        name="a model may recompute windows without declaring a grain",
        file="duckstream/model.py",
        find="""        if recomputing and not self.grain:
            missing.append("grain")""",
        repl="""        if False:
            missing.append("grain")""",
        suite="fast",
    ),
]
