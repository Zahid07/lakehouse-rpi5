"""Tests for :mod:`duckstream.state` — the exactly-once boundary.

**DuckLake is the gate here.** Almost every test below runs against a real
on-disk catalog under ``tmp_path``; :class:`~duckstream.state.MemoryStateStore`
gets a small parity section at the end and nothing more. That split is
deliberate and comes from ``CONTEXT.md`` 1.5, where a statement that passed
against in-memory DuckDB raised ``Out of buffer`` against DuckLake — and only on
the *second* write, the first to take the ``WHEN MATCHED`` branch. So:

* the round-trip tests commit **at least twice**, exercising the update path
  rather than only the insert path, and
* passing on ``MemoryStateStore`` is never treated as evidence of anything.

State is append-only (``CONTEXT.md`` 1.10), so "the committed value" always
means the newest row and the tests assert that rather than a row count of one.

The load-bearing assertions are :func:`test_a_commit_is_exactly_one_snapshot`
(``CONTEXT.md`` 1.4 — the primitive exactly-once rests on),
:func:`test_rollback_keeps_the_previous_offset_and_adds_no_snapshot` (the
unit-level shadow of W4's fault injection) and
:func:`test_an_empty_commit_writes_nothing_and_adds_no_snapshot`
(``CONTEXT.md`` 1.8 — an idle trigger must not pay the ~15 ms commit).
"""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from duckstream.errors import DuckstreamError
from duckstream.lake import (
    DEFAULT_ALIAS,
    attach_lake,
    data_file_count,
    snapshot_count,
)
from duckstream.state import (
    DEFAULT_STATE_SCHEMA,
    DuckLakeStateStore,
    MemoryStateStore,
    decode_offset,
    encode_offset,
)

OFFSET_A = {"files": ["a.parquet"], "high_water": 1}
OFFSET_B = {"files": ["a.parquet", "b.parquet"], "high_water": 2}

AWKWARD_OFFSETS = {
    "empty": {},
    "nested": {"outer": {"inner": {"deep": [1, 2, {"deeper": True}]}}},
    "unicode": {"sensor": "temperatur-fühler", "unit": "°C", "note": "日本語"},
    "single_quote_path": {"path": "C:/data/o'brien/landing/_READY"},
    "sql_shaped": {"path": "'; DROP TABLE duckstream.offsets; --"},
    "backslash_path": {"path": "D:\\landing\\2026\\08"},
    "nulls_and_numbers": {"a": None, "b": 0, "c": -1, "d": 1.5, "e": False},
    "empty_string_key": {"": ""},
}


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def lake_con(tmp_path):
    """A DuckDB connection with a real on-disk DuckLake catalog attached."""
    connection = duckdb.connect()
    attach_lake(
        connection,
        tmp_path / "catalog.ducklake",
        data_path=tmp_path / "lake_data",
    )
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def store(lake_con):
    """The real state store, tables created."""
    state_store = DuckLakeStateStore()
    state_store.ensure(lake_con)
    return state_store


@pytest.fixture
def memory_con():
    connection = duckdb.connect()
    try:
        yield connection
    finally:
        connection.close()


def commit_once(store, con, model_name, offset, watermark=None):
    """One trigger's worth of state, with a sink write inside the transaction."""
    store.begin(con)
    con.execute("CREATE TABLE IF NOT EXISTS main.sink (model VARCHAR, n BIGINT)")
    con.execute("INSERT INTO main.sink VALUES (?, ?)", [model_name, 1])
    store.commit(con, {model_name: offset}, {model_name: watermark})


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


def test_ensure_creates_the_three_state_tables(lake_con, store):
    tables = {
        row[0]
        for row in lake_con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            [DEFAULT_STATE_SCHEMA],
        ).fetchall()
    }
    assert {"offsets", "watermarks", "batches"} <= tables


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        (
            "offsets",
            {
                "model_name": "VARCHAR",
                "offset_json": "VARCHAR",
                "batch_id": "BIGINT",
                "updated_at": "TIMESTAMP",
            },
        ),
        (
            "watermarks",
            {
                "model_name": "VARCHAR",
                "watermark": "TIMESTAMP",
                "batch_id": "BIGINT",
                "updated_at": "TIMESTAMP",
            },
        ),
        (
            "batches",
            {
                "model_name": "VARCHAR",
                "batch_id": "BIGINT",
                "started_at": "TIMESTAMP",
                "committed_at": "TIMESTAMP",
                "rows_in": "BIGINT",
                "rows_out": "BIGINT",
            },
        ),
    ],
)
def test_state_tables_have_the_declared_columns(lake_con, store, table, expected):
    rows = lake_con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ?",
        [DEFAULT_STATE_SCHEMA, table],
    ).fetchall()
    assert dict(rows) == expected


def test_ensure_twice_is_a_no_op(lake_con, store):
    before = snapshot_count(lake_con)
    store.ensure(lake_con)
    store.ensure(lake_con)
    assert snapshot_count(lake_con) == before


def test_ensure_costs_a_single_snapshot_on_a_fresh_catalog(lake_con):
    fresh = DuckLakeStateStore(schema="other_state")
    before = snapshot_count(lake_con)
    fresh.ensure(lake_con)
    assert snapshot_count(lake_con) - before == 1


def test_the_state_tables_are_parquet_not_inlined(lake_con, store):
    """Small state writes must not take DuckLake's inlining path either."""
    commit_once(store, lake_con, "m", OFFSET_A)
    assert data_file_count(lake_con, DEFAULT_ALIAS, "duckstream.offsets") >= 1
    assert data_file_count(lake_con, DEFAULT_ALIAS, "duckstream.watermarks") >= 1


def test_an_explicit_catalog_alias_qualifies_the_tables(lake_con):
    """``catalog=`` lets the store work when the lake is not the current database.

    Note the constraint this test bumped into and now documents: a single DuckDB
    transaction may only write to one attached database. The sink and the state
    store must therefore both live inside the DuckLake catalog, which is what
    makes the one-transaction, one-snapshot commit possible at all.
    """
    qualified = DuckLakeStateStore(catalog=DEFAULT_ALIAS, schema="qualified_state")
    assert qualified.offsets_table == '"lake"."qualified_state"."offsets"'
    qualified.ensure(lake_con)
    lake_con.execute("USE memory")  # the lake is no longer the current catalog
    try:
        qualified.begin(lake_con)
        lake_con.execute(f"CREATE TABLE IF NOT EXISTS {DEFAULT_ALIAS}.main.sink2 (n BIGINT)")
        lake_con.execute(f"INSERT INTO {DEFAULT_ALIAS}.main.sink2 VALUES (1)")
        qualified.commit(lake_con, {"m": OFFSET_A}, {"m": None})
        assert qualified.load_offset(lake_con, "m") == OFFSET_A
    finally:
        lake_con.execute(f"USE {DEFAULT_ALIAS}")


@pytest.mark.parametrize("schema", ["not-a-schema", "1state", "a b", "s; DROP"])
def test_a_schema_that_is_not_an_identifier_is_refused(schema):
    with pytest.raises(DuckstreamError, match="identifier"):
        DuckLakeStateStore(schema=schema)


# ---------------------------------------------------------------------------
# offsets: the round trip
# ---------------------------------------------------------------------------


def test_load_offset_for_an_unknown_model_is_none(lake_con, store):
    """What makes a first run replay from the beginning of the source."""
    assert store.load_offset(lake_con, "never_ran") is None


def test_load_offset_is_still_none_after_another_model_commits(lake_con, store):
    commit_once(store, lake_con, "other", OFFSET_A)
    assert store.load_offset(lake_con, "never_ran") is None


def test_offset_round_trips_across_two_commits(lake_con, store):
    """Two commits, so the update path runs — see the module docstring."""
    commit_once(store, lake_con, "m", OFFSET_A)
    assert store.load_offset(lake_con, "m") == OFFSET_A

    commit_once(store, lake_con, "m", OFFSET_B)
    assert store.load_offset(lake_con, "m") == OFFSET_B

    # Append-only: both commits are still on disk, and the newest one wins.
    rows = lake_con.execute(
        "SELECT count(*) FROM duckstream.offsets WHERE model_name = 'm'"
    ).fetchone()[0]
    assert rows == 2
    assert lake_con.execute(
        "SELECT batch_id FROM duckstream.offsets WHERE model_name = 'm' "
        "ORDER BY batch_id"
    ).fetchall() == [(1,), (2,)]


def test_offsets_are_isolated_per_model(lake_con, store):
    commit_once(store, lake_con, "alpha", OFFSET_A)
    commit_once(store, lake_con, "beta", OFFSET_B)
    commit_once(store, lake_con, "alpha", OFFSET_B)

    assert store.load_offset(lake_con, "alpha") == OFFSET_B
    assert store.load_offset(lake_con, "beta") == OFFSET_B
    assert (
        lake_con.execute("SELECT count(*) FROM duckstream.offsets").fetchone()[0] == 3
    )


@pytest.mark.parametrize("name", sorted(AWKWARD_OFFSETS))
def test_awkward_offsets_survive_the_round_trip(lake_con, store, name):
    payload = AWKWARD_OFFSETS[name]
    commit_once(store, lake_con, "m", payload)
    assert store.load_offset(lake_con, "m") == payload

    # And again, so the update path sees the same value.
    commit_once(store, lake_con, "m", payload)
    assert store.load_offset(lake_con, "m") == payload


def test_awkward_offsets_are_stored_byte_identically(lake_con, store):
    payload = AWKWARD_OFFSETS["unicode"]
    commit_once(store, lake_con, "m", payload)
    stored = lake_con.execute(
        "SELECT offset_json FROM duckstream.offsets WHERE model_name = 'm'"
    ).fetchone()[0]
    assert stored == encode_offset(payload)
    assert decode_offset(stored) == payload


def test_a_hundred_commits_append_a_hundred_rows_and_the_last_one_wins(
    lake_con, store
):
    """Append-only, over enough commits to be sure nothing rewrites history.

    A hundred commits leave a hundred rows per model and ``load_offset``
    returns the last. Growth is real and deliberate — :meth:`prune` is what
    bounds it, and phase-4 maintenance is what schedules that.
    """
    for i in range(100):
        store.begin(lake_con)
        store.commit(
            lake_con,
            {"alpha": {"n": i}, "beta": {"n": -i}},
            {"alpha": None, "beta": None},
        )

    counts = dict(
        lake_con.execute(
            "SELECT model_name, count(*) FROM duckstream.offsets GROUP BY 1"
        ).fetchall()
    )
    assert counts == {"alpha": 100, "beta": 100}
    assert store.load_offset(lake_con, "alpha") == {"n": 99}
    assert store.load_offset(lake_con, "beta") == {"n": -99}

    watermark_counts = dict(
        lake_con.execute(
            "SELECT model_name, count(*) FROM duckstream.watermarks GROUP BY 1"
        ).fetchall()
    )
    assert watermark_counts == {"alpha": 100, "beta": 100}

    # Batch ids are dense and strictly increasing, which is what makes
    # ORDER BY batch_id DESC LIMIT 1 the right read.
    ids = [
        row[0]
        for row in lake_con.execute(
            "SELECT batch_id FROM duckstream.offsets WHERE model_name = 'alpha' "
            "ORDER BY batch_id"
        ).fetchall()
    ]
    assert ids == list(range(1, 101))


# ---------------------------------------------------------------------------
# the transaction boundary
# ---------------------------------------------------------------------------


def test_a_commit_is_exactly_one_snapshot(lake_con, store):
    """CONTEXT.md 1.4, applied to the shape a trigger actually has.

    More than one statement inside the transaction — a sink write, an offset and
    a watermark — and the snapshot count still advances by exactly one. That
    single snapshot *is* the exactly-once guarantee.
    """
    commit_once(store, lake_con, "m", OFFSET_A)  # first batch, insert path
    before = snapshot_count(lake_con)

    store.begin(lake_con)
    lake_con.execute("INSERT INTO main.sink VALUES ('m', 2)")
    lake_con.execute("INSERT INTO main.sink VALUES ('m', 3)")
    store.record_batch_start(lake_con, "m", 2)
    store.record_batch_end(lake_con, "m", 2, rows_in=2, rows_out=2)
    store.commit(lake_con, {"m": OFFSET_B}, {"m": datetime(2026, 8, 22, 12)})

    assert snapshot_count(lake_con) - before == 1


def test_every_trigger_is_one_snapshot_over_several_triggers(lake_con, store):
    before = snapshot_count(lake_con)
    for i in range(5):
        commit_once(store, lake_con, "m", {"n": i})
    assert snapshot_count(lake_con) - before == 5


def test_rollback_keeps_the_previous_offset_and_adds_no_snapshot(lake_con, store):
    """The unit-level shadow of W4's fault injection: crash before COMMIT.

    Rows were written and the offset was staged, then the transaction was
    abandoned. On the next read the committed state must be exactly what it was
    before, and no snapshot may exist for the work that did not commit.
    """
    commit_once(store, lake_con, "m", OFFSET_A)
    before_snapshots = snapshot_count(lake_con)
    before_rows = lake_con.execute("SELECT count(*) FROM main.sink").fetchone()[0]

    store.begin(lake_con)
    lake_con.execute("INSERT INTO main.sink VALUES ('m', 99)")
    store.record_batch_start(lake_con, "m", 2)
    store._append_offset(lake_con, "m", OFFSET_B, 2, datetime(2026, 8, 22, 12))
    store.rollback(lake_con)

    assert store.load_offset(lake_con, "m") == OFFSET_A
    assert snapshot_count(lake_con) == before_snapshots
    assert lake_con.execute("SELECT count(*) FROM main.sink").fetchone()[0] == before_rows
    assert store.batch_history(lake_con, "m") == []


def test_a_replay_after_rollback_lands_the_batch_exactly_once(lake_con, store):
    """Retry the abandoned batch: neither lost nor duplicated."""
    commit_once(store, lake_con, "m", OFFSET_A)

    store.begin(lake_con)
    lake_con.execute("INSERT INTO main.sink VALUES ('m', 2)")
    store.rollback(lake_con)

    commit_once(store, lake_con, "m", OFFSET_B)

    assert store.load_offset(lake_con, "m") == OFFSET_B
    assert lake_con.execute("SELECT count(*) FROM main.sink").fetchone()[0] == 2


def test_begin_refuses_to_nest(lake_con, store):
    store.begin(lake_con)
    try:
        with pytest.raises(DuckstreamError, match="already open"):
            store.begin(lake_con)
    finally:
        store.rollback(lake_con)


def test_a_failed_commit_leaves_the_transaction_rolled_back(lake_con, store):
    """Half-open is the one state the engine cannot recover from."""
    commit_once(store, lake_con, "m", OFFSET_A)
    before = snapshot_count(lake_con)

    store.begin(lake_con)
    lake_con.execute("INSERT INTO main.sink VALUES ('m', 99)")
    with pytest.raises(DuckstreamError, match="not JSON-serialisable"):
        store.commit(lake_con, {"m": {"bad": object()}}, {"m": None})

    # No transaction is left open, so the next trigger can begin normally.
    store.begin(lake_con)
    store.rollback(lake_con)

    assert store.load_offset(lake_con, "m") == OFFSET_A
    assert snapshot_count(lake_con) == before


def test_an_empty_commit_writes_nothing_and_adds_no_snapshot(lake_con, store):
    """CONTEXT.md 1.8: an idle trigger must not pay the ~15 ms commit.

    A DuckLake transaction that writes nothing costs ~1.3 ms; the moment it
    writes one state row it costs ~16.8 ms. So committing empty offsets and
    empty watermarks must produce no snapshot, whether or not the engine
    bothered to open a transaction first.
    """
    before = snapshot_count(lake_con)

    store.begin(lake_con)
    store.commit(lake_con, {}, {})
    assert snapshot_count(lake_con) == before

    # And with no transaction open at all it is a complete no-op, so the engine
    # may skip ``begin`` entirely on an empty batch.
    store.commit(lake_con, {}, {})
    assert snapshot_count(lake_con) == before
    assert lake_con.execute("SELECT count(*) FROM duckstream.offsets").fetchone()[0] == 0


def test_an_empty_commit_does_not_disturb_committed_state(lake_con, store):
    commit_once(store, lake_con, "m", OFFSET_A)
    store.commit(lake_con, {}, {})
    assert store.load_offset(lake_con, "m") == OFFSET_A


# ---------------------------------------------------------------------------
# watermarks
# ---------------------------------------------------------------------------


def test_watermark_round_trips(lake_con, store):
    when = datetime(2026, 8, 22, 12, 30, 15)
    store.begin(lake_con)
    store.commit(lake_con, {"m": OFFSET_A}, {"m": when})
    assert store.load_watermark(lake_con, "m") == when


def test_watermark_accepts_iso_text(lake_con, store):
    store.begin(lake_con)
    store.commit(lake_con, {"m": OFFSET_A}, {"m": "2026-08-22T12:30:15"})
    assert store.load_watermark(lake_con, "m") == datetime(2026, 8, 22, 12, 30, 15)


def test_an_aware_watermark_is_stored_as_naive_utc(lake_con, store):
    aware = datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc)
    store.begin(lake_con)
    store.commit(lake_con, {"m": OFFSET_A}, {"m": aware})
    assert store.load_watermark(lake_con, "m") == datetime(2026, 8, 22, 12, 0)


def test_a_null_watermark_is_allowed_and_read_back_as_none(lake_con, store):
    store.begin(lake_con)
    store.commit(lake_con, {"m": OFFSET_A}, {"m": None})
    assert store.load_watermark(lake_con, "m") is None
    assert (
        lake_con.execute("SELECT count(*) FROM duckstream.watermarks").fetchone()[0] == 1
    )


def test_an_unparseable_watermark_is_refused(lake_con, store):
    store.begin(lake_con)
    with pytest.raises(DuckstreamError, match="ISO-8601"):
        store.commit(lake_con, {"m": OFFSET_A}, {"m": "not a timestamp"})


def test_watermark_updates_across_two_commits(lake_con, store):
    store.begin(lake_con)
    store.commit(lake_con, {"m": OFFSET_A}, {"m": datetime(2026, 8, 22, 12)})
    store.begin(lake_con)
    store.commit(lake_con, {"m": OFFSET_B}, {"m": datetime(2026, 8, 22, 13)})
    assert store.load_watermark(lake_con, "m") == datetime(2026, 8, 22, 13)
    assert (
        lake_con.execute("SELECT count(*) FROM duckstream.watermarks").fetchone()[0] == 2
    )


def test_load_watermark_for_an_unknown_model_is_none(lake_con, store):
    assert store.load_watermark(lake_con, "never_ran") is None


# ---------------------------------------------------------------------------
# offset encoding — the whole coupling to duckstream.offsets
# ---------------------------------------------------------------------------


def test_encode_offset_sorts_keys_so_stored_text_is_comparable():
    assert encode_offset({"b": 1, "a": 2}) == encode_offset({"a": 2, "b": 1})


def test_encode_offset_passes_through_already_encoded_json():
    text = '{"already": "encoded"}'
    assert encode_offset(text) == text


def test_pre_encoded_json_text_round_trips_through_the_store(lake_con, store):
    text = '{"files": ["a.parquet"], "high_water": 1}'
    store.begin(lake_con)
    store.commit(lake_con, {"m": text}, {"m": None})
    assert store.load_offset(lake_con, "m") == OFFSET_A


def test_encode_offset_refuses_a_string_that_is_not_json():
    with pytest.raises(DuckstreamError, match="not valid JSON"):
        encode_offset("a.parquet")


def test_encode_offset_refuses_none():
    with pytest.raises(DuckstreamError, match="null offset"):
        encode_offset(None)


def test_committing_a_null_offset_is_refused(lake_con, store):
    store.begin(lake_con)
    with pytest.raises(DuckstreamError, match="null offset"):
        store.commit(lake_con, {"m": None}, {})
    store.begin(lake_con)
    store.rollback(lake_con)


def test_decode_offset_of_none_is_none():
    assert decode_offset(None) is None


# ---------------------------------------------------------------------------
# batch history
# ---------------------------------------------------------------------------


def test_next_batch_id_starts_at_one(lake_con, store):
    assert store.next_batch_id(lake_con, "m") == 1


def test_next_batch_id_is_monotonic(lake_con, store):
    seen = []
    for _ in range(4):
        batch_id = store.next_batch_id(lake_con, "m")
        seen.append(batch_id)
        store.begin(lake_con)
        store.record_batch_start(lake_con, "m", batch_id)
        store.record_batch_end(lake_con, "m", batch_id, rows_in=1, rows_out=1)
        store.commit(lake_con, {"m": {"n": batch_id}}, {"m": None})
    assert seen == [1, 2, 3, 4]
    assert store.next_batch_id(lake_con, "m") == 5


def test_next_batch_id_is_per_model(lake_con, store):
    store.begin(lake_con)
    store.record_batch_start(lake_con, "alpha", 1)
    store.record_batch_end(lake_con, "alpha", 1)
    store.commit(lake_con, {"alpha": OFFSET_A}, {})
    assert store.next_batch_id(lake_con, "alpha") == 2
    assert store.next_batch_id(lake_con, "beta") == 1


def test_batch_history_records_times_and_counts(lake_con, store):
    store.begin(lake_con)
    store.record_batch_start(lake_con, "m", 1, started_at=datetime(2026, 8, 22, 12))
    store.record_batch_end(
        lake_con,
        "m",
        1,
        rows_in=10,
        rows_out=4,
        committed_at=datetime(2026, 8, 22, 12, 0, 1),
    )
    store.commit(lake_con, {"m": OFFSET_A}, {"m": None})

    history = store.batch_history(lake_con, "m")
    assert history == [
        {
            "model_name": "m",
            "batch_id": 1,
            "started_at": datetime(2026, 8, 22, 12),
            "committed_at": datetime(2026, 8, 22, 12, 0, 1),
            "rows_in": 10,
            "rows_out": 4,
        }
    ]


def test_record_batch_start_writes_nothing_to_the_database(lake_con, store):
    """One append per batch, not an insert plus a later update.

    CONTEXT.md 1.10: the insert-then-update shape cost ~31 ms of tombstone per
    trigger for pure bookkeeping. The start time is held in memory and
    ``record_batch_end`` appends the finished row carrying both timestamps.
    """
    store.begin(lake_con)
    store.record_batch_start(lake_con, "m", 1)
    store.record_batch_start(lake_con, "m", 1)  # repeated start, still no write
    assert lake_con.execute("SELECT count(*) FROM duckstream.batches").fetchone()[0] == 0

    store.record_batch_end(lake_con, "m", 1, rows_in=1, rows_out=1)
    store.commit(lake_con, {"m": OFFSET_A}, {})
    assert len(store.batch_history(lake_con, "m")) == 1


def test_a_batch_opened_but_never_ended_leaves_no_row(lake_con, store):
    store.begin(lake_con)
    store.record_batch_start(lake_con, "m", 1)
    store.commit(lake_con, {"m": OFFSET_A}, {})
    assert store.batch_history(lake_con, "m") == []


def test_record_batch_end_without_a_start_still_records_the_batch(lake_con, store):
    store.begin(lake_con)
    store.record_batch_end(lake_con, "m", 7, rows_in=1, rows_out=1)
    store.commit(lake_con, {"m": OFFSET_A}, {})

    history = store.batch_history(lake_con, "m")
    assert len(history) == 1
    assert history[0]["batch_id"] == 7
    assert history[0]["started_at"] is None
    assert store.next_batch_id(lake_con, "m") == 8


def test_the_committed_offset_records_the_batch_that_produced_it(lake_con, store):
    store.begin(lake_con)
    store.record_batch_start(lake_con, "m", 42)
    store.commit(lake_con, {"m": OFFSET_A}, {})
    assert (
        lake_con.execute(
            "SELECT batch_id FROM duckstream.offsets WHERE model_name = 'm'"
        ).fetchone()[0]
        == 42
    )


def test_the_offset_batch_id_advances_without_an_explicit_batch_record(lake_con, store):
    """Even a caller that never records batches gets a monotonic id."""
    for expected in (1, 2, 3):
        store.begin(lake_con)
        store.commit(lake_con, {"m": {"n": expected}}, {})
        assert (
            lake_con.execute(
                "SELECT batch_id FROM duckstream.offsets WHERE model_name = 'm' "
                "ORDER BY batch_id DESC LIMIT 1"
            ).fetchone()[0]
            == expected
        )


def test_a_rolled_back_batch_id_is_not_carried_into_the_next_commit(lake_con, store):
    store.begin(lake_con)
    store.record_batch_start(lake_con, "m", 9)
    store.rollback(lake_con)

    store.begin(lake_con)
    store.commit(lake_con, {"m": OFFSET_A}, {})
    assert (
        lake_con.execute(
            "SELECT batch_id FROM duckstream.offsets WHERE model_name = 'm'"
        ).fetchone()[0]
        == 1
    )


# ---------------------------------------------------------------------------
# MemoryStateStore — speed only, never the gate
# ---------------------------------------------------------------------------


def test_memory_store_documents_that_it_is_not_a_production_backend():
    """PLAN.md is emphatic about this, so the docstring is asserted, not trusted."""
    doc = MemoryStateStore.__doc__ or ""
    assert "unit-test speed only" in doc
    assert "Not a supported production backend" in doc
    assert "never the sole test gate" in doc


def test_memory_store_round_trips_two_commits(memory_con):
    store = MemoryStateStore()
    store.ensure(memory_con)
    store.ensure(memory_con)

    assert store.load_offset(memory_con, "m") is None
    store.begin(memory_con)
    store.commit(memory_con, {"m": OFFSET_A}, {"m": datetime(2026, 8, 22, 12)})
    assert store.load_offset(memory_con, "m") == OFFSET_A

    store.begin(memory_con)
    store.commit(memory_con, {"m": OFFSET_B}, {"m": datetime(2026, 8, 22, 13)})
    assert store.load_offset(memory_con, "m") == OFFSET_B
    assert (
        memory_con.execute("SELECT count(*) FROM duckstream.offsets").fetchone()[0] == 2
    )


def test_memory_store_and_ducklake_store_agree(memory_con, lake_con, store):
    """Parity, not substitution: the DuckLake result is the one that counts."""
    memory_store = MemoryStateStore()
    memory_store.ensure(memory_con)

    for payload in (OFFSET_A, OFFSET_B, AWKWARD_OFFSETS["unicode"]):
        memory_store.begin(memory_con)
        memory_store.commit(memory_con, {"m": payload}, {"m": None})
        commit_once(store, lake_con, "m", payload)
        assert memory_store.load_offset(memory_con, "m") == store.load_offset(
            lake_con, "m"
        )


def test_memory_store_begin_also_refuses_to_nest(memory_con):
    store = MemoryStateStore()
    store.ensure(memory_con)
    store.begin(memory_con)
    try:
        with pytest.raises(DuckstreamError, match="already open"):
            store.begin(memory_con)
    finally:
        store.rollback(memory_con)


# ---------------------------------------------------------------------------
# prune — what bounds the growth append-only introduces
# ---------------------------------------------------------------------------


def commit_n(store, con, model_name, n):
    for i in range(n):
        store.begin(con)
        store.record_batch_start(con, model_name, i + 1)
        store.record_batch_end(con, model_name, i + 1, rows_in=1, rows_out=1)
        store.commit(con, {model_name: {"n": i}}, {model_name: None})


def test_prune_keeps_only_the_newest_row_per_table(lake_con, store):
    commit_n(store, lake_con, "m", 5)
    assert store.load_offset(lake_con, "m") == {"n": 4}

    deleted = store.prune(lake_con, "m")

    assert deleted == {"offsets": 4, "watermarks": 4, "batches": 4}
    for table in ("offsets", "watermarks", "batches"):
        assert (
            lake_con.execute(f"SELECT count(*) FROM duckstream.{table}").fetchone()[0]
            == 1
        )
    # The offset that survives is the one a restart must replay from.
    assert store.load_offset(lake_con, "m") == {"n": 4}
    assert store.next_batch_id(lake_con, "m") == 6


def test_prune_keep_last_retains_that_many(lake_con, store):
    commit_n(store, lake_con, "m", 5)
    deleted = store.prune(lake_con, "m", keep_last=3)

    assert deleted == {"offsets": 2, "watermarks": 2, "batches": 2}
    assert (
        lake_con.execute("SELECT count(*) FROM duckstream.offsets").fetchone()[0] == 3
    )
    assert store.load_offset(lake_con, "m") == {"n": 4}


def test_prune_is_a_no_op_when_there_is_nothing_to_drop(lake_con, store):
    commit_n(store, lake_con, "m", 2)
    before = snapshot_count(lake_con)

    assert store.prune(lake_con, "m", keep_last=5) == {
        "offsets": 0,
        "watermarks": 0,
        "batches": 0,
    }
    assert snapshot_count(lake_con) == before


def test_prune_on_empty_state_is_a_no_op(lake_con, store):
    assert store.prune(lake_con) == {"offsets": 0, "watermarks": 0, "batches": 0}


def test_prune_is_one_snapshot(lake_con, store):
    commit_n(store, lake_con, "m", 4)
    before = snapshot_count(lake_con)
    store.prune(lake_con, "m")
    assert snapshot_count(lake_con) - before == 1


def test_prune_covers_every_model_when_none_is_named(lake_con, store):
    commit_n(store, lake_con, "alpha", 3)
    commit_n(store, lake_con, "beta", 3)

    deleted = store.prune(lake_con)

    assert deleted["offsets"] == 4  # two per model
    assert store.load_offset(lake_con, "alpha") == {"n": 2}
    assert store.load_offset(lake_con, "beta") == {"n": 2}
    assert (
        lake_con.execute("SELECT count(*) FROM duckstream.offsets").fetchone()[0] == 2
    )


def test_prune_leaves_other_models_alone(lake_con, store):
    commit_n(store, lake_con, "alpha", 3)
    commit_n(store, lake_con, "beta", 3)

    store.prune(lake_con, "alpha")

    counts = dict(
        lake_con.execute(
            "SELECT model_name, count(*) FROM duckstream.offsets GROUP BY 1"
        ).fetchall()
    )
    assert counts == {"alpha": 1, "beta": 3}


@pytest.mark.parametrize("keep_last", [0, -1])
def test_prune_refuses_to_discard_everything(lake_con, store, keep_last):
    commit_n(store, lake_con, "m", 2)
    with pytest.raises(DuckstreamError, match="keep_last"):
        store.prune(lake_con, "m", keep_last=keep_last)
    assert store.load_offset(lake_con, "m") == {"n": 1}


def test_state_still_reads_correctly_after_a_prune_and_more_commits(lake_con, store):
    """Pruning must not disturb the batch-id sequence the reads depend on."""
    commit_n(store, lake_con, "m", 3)
    store.prune(lake_con, "m")

    store.begin(lake_con)
    batch_id = store.next_batch_id(lake_con, "m")
    store.record_batch_start(lake_con, "m", batch_id)
    store.record_batch_end(lake_con, "m", batch_id)
    store.commit(lake_con, {"m": {"n": 99}}, {"m": None})

    assert batch_id == 4
    assert store.load_offset(lake_con, "m") == {"n": 99}


def test_memory_store_prunes_too(memory_con):
    store = MemoryStateStore()
    store.ensure(memory_con)
    commit_n(store, memory_con, "m", 4)
    assert store.prune(memory_con, "m") == {
        "offsets": 3,
        "watermarks": 3,
        "batches": 3,
    }
    assert store.load_offset(memory_con, "m") == {"n": 3}


def test_a_fresh_store_resumes_the_batch_sequence_from_disk(lake_con, store):
    """The cache-cold path: a new process attaching to existing state.

    ``_resolve_batch_id`` memoises the last committed batch id to keep a
    ``max(batch_id)`` scan out of every trigger's transaction. That makes the
    scan branch the one a restart takes, so it is the one worth testing.
    """
    commit_n(store, lake_con, "m", 3)

    restarted = DuckLakeStateStore()  # no in-memory history at all
    assert restarted.load_offset(lake_con, "m") == {"n": 2}

    restarted.begin(lake_con)
    restarted.commit(lake_con, {"m": {"n": 99}}, {"m": None})

    assert (
        lake_con.execute(
            "SELECT batch_id FROM duckstream.offsets WHERE model_name = 'm' "
            "ORDER BY batch_id DESC LIMIT 1"
        ).fetchone()[0]
        == 4
    )
    assert restarted.load_offset(lake_con, "m") == {"n": 99}


def test_the_cached_batch_id_does_not_advance_on_a_rolled_back_commit(lake_con, store):
    commit_n(store, lake_con, "m", 2)

    store.begin(lake_con)
    with pytest.raises(DuckstreamError):
        store.commit(lake_con, {"m": {"bad": object()}}, {})

    store.begin(lake_con)
    store.commit(lake_con, {"m": {"n": 7}}, {})
    assert (
        lake_con.execute(
            "SELECT batch_id FROM duckstream.offsets WHERE model_name = 'm' "
            "ORDER BY batch_id DESC LIMIT 1"
        ).fetchone()[0]
        == 3
    )
