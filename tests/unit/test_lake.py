"""Tests for :mod:`duckstream.lake` — against a real on-disk DuckLake catalog.

These run against DuckLake rather than in-memory DuckDB because the things
being checked here do not exist on plain DuckDB at all: snapshots, data files,
and the inlining switch. ``CONTEXT.md`` 1.5 is the general reason — in-memory
DuckDB demonstrably hides DuckLake failures — but for this module it is simply
that there is nothing to test without a catalog.

Two of these tests are transcriptions of measurements in ``CONTEXT.md`` rather
than opinions about how the code should behave:

* :func:`test_three_row_insert_writes_a_parquet_file` and its control
  :func:`test_default_row_limit_would_have_inlined_the_same_insert` reproduce
  1.7 — a 3-row insert leaves zero data files at the default limit of 10 and one
  parquet file at 0.
* :func:`test_one_transaction_is_exactly_one_snapshot` and its control
  :func:`test_autocommit_statements_are_one_snapshot_each` reproduce 1.4, which
  is the primitive the whole exactly-once guarantee rests on.

If one of those four ever fails, the finding has changed and the design needs
re-checking — they are not tests of this module's opinions.
"""

from __future__ import annotations

import glob
import os

import duckdb
import pytest

from duckstream.errors import DuckstreamError
from duckstream.lake import (
    DEFAULT_ALIAS,
    INLINING_SETTING,
    apply_settings,
    attach_lake,
    data_file_count,
    list_files,
    normalise_path,
    snapshot_count,
    snapshots,
)


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def catalog_path(tmp_path):
    return tmp_path / "catalog.ducklake"


@pytest.fixture
def data_path(tmp_path):
    return tmp_path / "lake_data"


@pytest.fixture
def con():
    connection = duckdb.connect()
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def lake(con, catalog_path, data_path):
    """An attached, configured DuckLake session on an in-memory connection."""
    attach_lake(con, catalog_path, data_path=data_path)
    return con


def parquet_files(data_path) -> list[str]:
    return glob.glob(os.path.join(str(data_path), "**", "*.parquet"), recursive=True)


def effective_inlining(con) -> int:
    return int(con.execute(f"SELECT current_setting('{INLINING_SETTING}')").fetchone()[0])


# ---------------------------------------------------------------------------
# path and identifier handling
# ---------------------------------------------------------------------------


def test_normalise_path_turns_windows_separators_into_forward_slashes():
    assert normalise_path(r"D:\lake\catalog.ducklake") == "D:/lake/catalog.ducklake"


def test_normalise_path_accepts_pathlib(tmp_path):
    assert "\\" not in normalise_path(tmp_path / "x" / "y")


def test_normalise_path_rejects_none():
    with pytest.raises(DuckstreamError):
        normalise_path(None)


@pytest.mark.parametrize("alias", ["bad-alias", "1lake", "lake;DROP", "", "la ke"])
def test_attach_refuses_an_alias_that_is_not_an_identifier(
    con, catalog_path, data_path, alias
):
    with pytest.raises(DuckstreamError, match="identifier"):
        attach_lake(con, catalog_path, data_path=data_path, alias=alias)


def test_a_single_quote_in_the_path_is_escaped_not_concatenated(tmp_path, con):
    """A path DuckLake must see verbatim, not one that ends the SQL literal."""
    awkward = tmp_path / "o'quote dir"
    awkward.mkdir()
    attach_lake(con, awkward / "c.ducklake", data_path=awkward / "data")
    con.execute("CREATE TABLE t (i INTEGER)")
    con.execute("INSERT INTO t SELECT * FROM range(0, 3)")
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 3
    assert parquet_files(awkward / "data")


# ---------------------------------------------------------------------------
# attach: creation versus re-attach
# ---------------------------------------------------------------------------


def test_attach_creates_the_catalog_and_the_data_path(con, catalog_path, data_path):
    assert not catalog_path.exists()
    attach_lake(con, catalog_path, data_path=data_path)
    con.execute("CREATE TABLE t (i INTEGER)")
    con.execute("INSERT INTO t SELECT * FROM range(0, 3)")

    assert catalog_path.exists()
    assert data_path.exists()
    assert parquet_files(data_path), "expected a parquet file under DATA_PATH"


def test_attach_makes_the_catalog_current(con, catalog_path, data_path):
    attach_lake(con, catalog_path, data_path=data_path, alias="warehouse")
    assert con.execute("SELECT current_database()").fetchone()[0] == "warehouse"


def test_existing_catalog_reattaches_without_data_path(catalog_path, data_path):
    first = duckdb.connect()
    try:
        attach_lake(first, catalog_path, data_path=data_path)
        first.execute("CREATE TABLE t (i INTEGER)")
        first.execute("INSERT INTO t SELECT * FROM range(0, 3)")
    finally:
        first.close()

    second = duckdb.connect()
    try:
        attach_lake(second, catalog_path)  # no data_path at all
        assert second.execute("SELECT count(*) FROM t").fetchone()[0] == 3
    finally:
        second.close()


def test_data_path_is_omitted_when_the_catalog_already_exists(
    catalog_path, data_path, tmp_path
):
    """The point of detecting creation-versus-attach, stated as a test.

    DuckLake raises ``DATA_PATH ... does not match existing data path`` if an
    existing catalog is attached with a different one. Passing a stale or
    differently spelled ``data_path`` on the second run is exactly what a cron
    deployment does, so the second attach must ignore it rather than fail.
    """
    first = duckdb.connect()
    try:
        attach_lake(first, catalog_path, data_path=data_path)
        first.execute("CREATE TABLE t (i INTEGER)")
        first.execute("INSERT INTO t VALUES (1)")
    finally:
        first.close()

    second = duckdb.connect()
    try:
        attach_lake(second, catalog_path, data_path=tmp_path / "somewhere_else")
        assert second.execute("SELECT count(*) FROM t").fetchone()[0] == 1
        # And the original data path is still the one in use.
        second.execute("INSERT INTO t VALUES (2)")
        assert parquet_files(data_path)
        assert not parquet_files(tmp_path / "somewhere_else")
    finally:
        second.close()


def test_attach_accepts_a_ducklake_prefixed_catalog(con, catalog_path, data_path):
    attach_lake(con, f"ducklake:{normalise_path(catalog_path)}", data_path=data_path)
    con.execute("CREATE TABLE t (i INTEGER)")
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 0


def test_attaching_twice_on_one_connection_is_a_no_op(con, catalog_path, data_path):
    attach_lake(con, catalog_path, data_path=data_path)
    con.execute("CREATE TABLE t (i INTEGER)")
    attach_lake(con, catalog_path, data_path=data_path)
    assert con.execute("SELECT current_database()").fetchone()[0] == DEFAULT_ALIAS
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 0


def test_attach_without_data_path_on_a_new_catalog_still_works(con, catalog_path):
    attach_lake(con, catalog_path)
    con.execute("CREATE TABLE t (i INTEGER)")
    con.execute("INSERT INTO t SELECT * FROM range(0, 3)")
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 3


# ---------------------------------------------------------------------------
# inlining — CONTEXT.md 1.7, measured the way 1.7 measured it
# ---------------------------------------------------------------------------


def test_attach_leaves_inlining_disabled(lake):
    assert effective_inlining(lake) == 0


def test_three_row_insert_writes_a_parquet_file(lake, data_path):
    """CONTEXT.md 1.7, the ``= 0`` row: 1 listed file, 1 parquet on disk.

    Three rows is below DuckLake's default inlining limit of 10, so this is the
    write that would silently have gone into the catalog instead.
    """
    lake.execute("CREATE TABLE t (i INTEGER)")
    lake.execute("INSERT INTO t SELECT * FROM range(0, 3)")

    listed = list_files(lake, DEFAULT_ALIAS, "t")
    assert listed, "ducklake_list_files was empty: the batch was inlined"
    assert data_file_count(lake, DEFAULT_ALIAS, "t") == 1
    assert len(parquet_files(data_path)) == 1
    assert os.path.exists(listed[0]["data_file"])


def test_default_row_limit_would_have_inlined_the_same_insert(lake, data_path):
    """The control for the test above — the ``default (10)`` row of 1.7.

    Reaching past :func:`attach_lake` to re-enable inlining is the point: it
    demonstrates the previous test is measuring the setting and not something
    incidental about small tables. If this ever stops reporting zero files, the
    DuckLake behaviour has changed and the reasoning in ``CONTEXT.md`` 1.7 is
    due a re-measurement.
    """
    lake.execute(f"SET {INLINING_SETTING} = 10")
    lake.execute("CREATE TABLE inlined (i INTEGER)")
    lake.execute("INSERT INTO inlined SELECT * FROM range(0, 3)")

    assert list_files(lake, DEFAULT_ALIAS, "inlined") == []
    assert data_file_count(lake, DEFAULT_ALIAS, "inlined") == 0
    assert parquet_files(data_path) == []
    # The rows are readable, which is what makes the default so easy to miss.
    assert lake.execute("SELECT count(*) FROM inlined").fetchone()[0] == 3


@pytest.mark.parametrize("bad", [1, 10, 100, "10", True])
def test_settings_cannot_re_enable_inlining(con, catalog_path, data_path, bad):
    with pytest.raises(DuckstreamError) as excinfo:
        attach_lake(
            con,
            catalog_path,
            data_path=data_path,
            settings={INLINING_SETTING: bad},
        )
    assert INLINING_SETTING in str(excinfo.value)


def test_settings_may_restate_inlining_as_zero(con, catalog_path, data_path):
    attach_lake(
        con, catalog_path, data_path=data_path, settings={INLINING_SETTING: 0}
    )
    assert effective_inlining(con) == 0


def test_inlining_stays_off_even_if_another_setting_follows_it(
    con, catalog_path, data_path
):
    attach_lake(
        con,
        catalog_path,
        data_path=data_path,
        settings={"threads": 2, "memory_limit": "512MB"},
    )
    assert effective_inlining(con) == 0


# ---------------------------------------------------------------------------
# settings validation
# ---------------------------------------------------------------------------


def test_settings_are_applied(con, catalog_path, data_path):
    attach_lake(
        con,
        catalog_path,
        data_path=data_path,
        settings={"threads": 2, "memory_limit": "512MB"},
    )
    assert int(con.execute("SELECT current_setting('threads')").fetchone()[0]) == 2
    # DuckDB reports the limit back in MiB: 512 MB is 488.2 MiB.
    assert con.execute("SELECT current_setting('memory_limit')").fetchone()[0] == (
        "488.2 MiB"
    )


@pytest.mark.parametrize(
    "key",
    ["threads; DROP TABLE t", "memory limit", "1threads", "", "threads--"],
)
def test_settings_reject_a_key_that_is_not_an_identifier(lake, key):
    with pytest.raises(DuckstreamError, match="identifier"):
        apply_settings(lake, {key: 2})


@pytest.mark.parametrize("value", [None, [1, 2], {"a": 1}, object()])
def test_settings_reject_a_value_that_is_not_a_scalar(lake, value):
    with pytest.raises(DuckstreamError, match="unsupported value type"):
        apply_settings(lake, {"threads": value})


def test_a_string_setting_with_a_quote_is_escaped_not_injected(lake):
    """The value is quoted, so the worst case is a rejected setting."""
    with pytest.raises(DuckstreamError, match="could not apply setting"):
        apply_settings(lake, {"memory_limit": "1GB'; DROP TABLE t; --"})
    # The connection is still usable, i.e. nothing was executed as SQL.
    assert lake.execute("SELECT 1").fetchone()[0] == 1


def test_no_settings_is_fine(lake):
    apply_settings(lake, None)
    apply_settings(lake, {})


def test_settings_must_be_a_dict(lake):
    with pytest.raises(DuckstreamError, match="must be a dict"):
        apply_settings(lake, [("threads", 2)])


# ---------------------------------------------------------------------------
# snapshots — CONTEXT.md 1.4
# ---------------------------------------------------------------------------


def test_one_transaction_is_exactly_one_snapshot(lake):
    """CONTEXT.md 1.4: 2 inserts inside ``BEGIN ... COMMIT`` produce 1 snapshot.

    This is the measurement the exactly-once claim rests on, so it is asserted
    with more than one statement in the transaction and with a DDL statement
    among them — the shape a real trigger has.
    """
    lake.execute("CREATE TABLE t (i INTEGER)")
    before = snapshot_count(lake)

    lake.execute("BEGIN TRANSACTION")
    lake.execute("CREATE TABLE u (i INTEGER)")
    lake.execute("INSERT INTO t SELECT * FROM range(0, 3)")
    lake.execute("INSERT INTO t SELECT * FROM range(3, 6)")
    lake.execute("INSERT INTO u VALUES (1)")
    lake.execute("COMMIT")

    assert snapshot_count(lake) - before == 1


def test_autocommit_statements_are_one_snapshot_each(lake):
    """The control for the test above — the other half of 1.4."""
    lake.execute("CREATE TABLE t (i INTEGER)")
    before = snapshot_count(lake)
    for i in range(5):
        lake.execute("INSERT INTO t VALUES (?)", [i])
    assert snapshot_count(lake) - before == 5


def test_a_rolled_back_transaction_adds_no_snapshot(lake):
    lake.execute("CREATE TABLE t (i INTEGER)")
    before = snapshot_count(lake)

    lake.execute("BEGIN TRANSACTION")
    lake.execute("INSERT INTO t VALUES (99)")
    lake.execute("ROLLBACK")

    assert snapshot_count(lake) == before
    assert lake.execute("SELECT count(*) FROM t").fetchone()[0] == 0


def test_a_transaction_that_writes_nothing_adds_no_snapshot(lake):
    """CONTEXT.md 1.8: the idle-trigger path must stay off the ~17 ms commit."""
    before = snapshot_count(lake)
    lake.execute("BEGIN TRANSACTION")
    lake.execute("SELECT 1")
    lake.execute("COMMIT")
    assert snapshot_count(lake) == before


def test_snapshots_returns_rows_without_needing_pytz(lake):
    """``snapshot_time`` is ``TIMESTAMP WITH TIME ZONE``; pytz is not a dep."""
    lake.execute("CREATE TABLE t (i INTEGER)")
    lake.execute("INSERT INTO t VALUES (1)")

    history = snapshots(lake)
    assert len(history) == snapshot_count(lake)
    assert [row["snapshot_id"] for row in history] == sorted(
        row["snapshot_id"] for row in history
    )
    latest = history[-1]
    assert set(latest) == {"snapshot_id", "snapshot_time", "schema_version", "changes"}
    assert isinstance(latest["snapshot_time"], str)
    assert latest["snapshot_time"]


def test_snapshot_count_matches_the_history_length(lake):
    lake.execute("CREATE TABLE t (i INTEGER)")
    for i in range(3):
        lake.execute("INSERT INTO t VALUES (?)", [i])
    assert snapshot_count(lake) == len(snapshots(lake))


@pytest.mark.parametrize("alias", ["not-an-alias", "lake'; --"])
def test_snapshot_helpers_reject_a_bad_alias(lake, alias):
    with pytest.raises(DuckstreamError, match="identifier"):
        snapshot_count(lake, alias)
    with pytest.raises(DuckstreamError, match="identifier"):
        snapshots(lake, alias)


# ---------------------------------------------------------------------------
# file introspection
# ---------------------------------------------------------------------------


def test_list_files_accepts_a_schema_qualified_name(lake):
    lake.execute("CREATE SCHEMA marts")
    lake.execute("CREATE TABLE marts.t (i INTEGER)")
    lake.execute("INSERT INTO marts.t SELECT * FROM range(0, 3)")

    by_qualified = list_files(lake, DEFAULT_ALIAS, "marts.t")
    by_keyword = list_files(lake, DEFAULT_ALIAS, "t", schema="marts")
    assert len(by_qualified) == 1
    assert by_qualified == by_keyword
    assert data_file_count(lake, DEFAULT_ALIAS, "marts.t") == 1


def test_list_files_is_empty_before_anything_is_written(lake):
    lake.execute("CREATE TABLE t (i INTEGER)")
    assert list_files(lake, DEFAULT_ALIAS, "t") == []
    assert data_file_count(lake, DEFAULT_ALIAS, "t") == 0


def test_every_trigger_writes_its_own_data_file(lake):
    """CONTEXT.md 1.8 observed 30 small batches producing 30 parquet files.

    That is the cost of disabling inlining and the reason compaction is a
    framework concern; asserting it here keeps the cost visible.
    """
    lake.execute("CREATE TABLE t (i INTEGER)")
    for i in range(4):
        lake.execute("BEGIN TRANSACTION")
        lake.execute("INSERT INTO t VALUES (?)", [i])
        lake.execute("COMMIT")
    assert data_file_count(lake, DEFAULT_ALIAS, "t") == 4


@pytest.mark.parametrize("table", ["bad-name", "a.b.c", "", "t; DROP TABLE u"])
def test_list_files_rejects_a_table_that_is_not_an_identifier(lake, table):
    with pytest.raises(DuckstreamError):
        list_files(lake, DEFAULT_ALIAS, table)


def test_list_files_rejects_a_bad_alias(lake):
    lake.execute("CREATE TABLE t (i INTEGER)")
    with pytest.raises(DuckstreamError, match="identifier"):
        list_files(lake, "not-an-alias", "t")


# ---------------------------------------------------------------------------
# the module owns no connection
# ---------------------------------------------------------------------------


def test_attach_lake_does_not_hold_the_catalog_open_after_the_caller_closes(
    catalog_path, data_path
):
    """CONTEXT.md 1.6: one held DuckDB file locks every other process out.

    ``lake.py`` never opens a connection of its own, so once the caller closes
    theirs the catalog file is free. Approximated here by re-opening it, which
    is what a second process would do.
    """
    first = duckdb.connect()
    attach_lake(first, catalog_path, data_path=data_path)
    first.execute("CREATE TABLE t (i INTEGER)")
    first.execute("INSERT INTO t VALUES (1)")
    first.close()

    second = duckdb.connect()
    try:
        attach_lake(second, catalog_path)
        assert second.execute("SELECT count(*) FROM t").fetchone()[0] == 1
    finally:
        second.close()
