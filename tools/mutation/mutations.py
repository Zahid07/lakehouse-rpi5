"""Deliberate defects for the phase-4 consumed-file change.

Each entry is a plausible wrong decision somebody could actually make, not a
syntax error. `find` must appear exactly once in `file`; it is replaced by
`repl`, the suite is run, and the mutation has to turn it red.

`suite` picks the cheapest run that should catch it:
  "fast"   -- pytest -m "not conformance"
  "conf"   -- pytest -m conformance
  "all"    -- everything
Choosing too cheap a suite is a way to report a false green, so anything about
snapshot atomicity or DuckLake behaviour is on a conformance run.
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
]
