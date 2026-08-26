"""The star-schema half: DDL, the SCD2 dimension, and the read-time views.

duckstream owns aggregation and the exactly-once commit. It does not own
surrogate keys, validity intervals or slowly-changing attributes, and this
module is where that boundary is drawn rather than blurred.

**It runs inside the engine's own attached session**, between the drain and the
detach -- see `pipeline.py`. That matters for two reasons. It inherits the
catalog lock the engine already holds, so the dimension is maintained under the
same single-writer guarantee as the marts. And it costs no second `ATTACH`,
which on this Pi is ~11 ms warm but would also be a second lock acquisition and
therefore a second thing that can fail.

**What it must not do is join the dimension into the fact.** The fact carries
the natural key and the views resolve the surrogate at read time. Writing the
key into the fact would need it assigned inside duckstream's commit, which
cannot be arranged from here -- and trap 1 forbids the obvious shortcut anyway:
never a scalar subquery in a MERGE or JOIN against DuckLake (`Out of buffer`,
and only on the second batch).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
SQL = HERE / "sql"

#: DDL for the tables duckstream does not create. Its own sinks handle
#: `curated.fact_accelerometer`, `marts.accel_hourly_summary` and
#: `marts.accel_minute_spectrum`; these two are ours.
DDL = """
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS curated.location_hlp (
    location_key  INTEGER,
    location_name VARCHAR,
    city          VARCHAR,
    country       VARCHAR,
    ins_tmstmp    TIMESTAMP,
    upd_tmstmp    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS curated.location_dim (
    location_key   INTEGER,
    location_name  VARCHAR,
    city           VARCHAR,
    country        VARCHAR,
    is_current     BOOLEAN,
    valid_from     TIMESTAMP,
    valid_to       TIMESTAMP,
    ins_tmstmp     TIMESTAMP,
    upd_tmstmp     TIMESTAMP,
    oper           VARCHAR
);
"""


def _statements(text: str) -> list[str]:
    """Split a script into statements: strip comments **first**, then split.

    The order is the whole of it, and getting it wrong is not a subtle failure.
    Splitting on ``;`` first cuts a *comment* that contains a semicolon in two,
    and the tail of that comment then starts a line without its ``--``, so it
    survives the filter and reaches the parser as prose. That happened here on
    the first run, on a sentence reading "...derives avg/stddev from it; a
    rounded stored value would corrupt...", and DuckDB reported
    ``syntax error at or near "a"``.

    Still deliberately simple-minded beyond that: these are our own scripts and
    contain no string literal with a semicolon in it. A real SQL splitter would
    be more code than the scripts it parses.
    """
    stripped = "\n".join(
        line for line in text.splitlines()
        if line.strip() and not line.strip().startswith("--")
    )
    return [chunk.strip() for chunk in stripped.split(";") if chunk.strip()]


def _exists(con: Any, alias: str, schema: str, name: str) -> bool:
    return bool(
        con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? "
            "LIMIT 1",
            [alias, schema, name],
        ).fetchone()
    )


def ensure(con: Any, alias: str = "lake") -> None:
    """Create the schemas and the two dimension tables, **only if missing**.

    "Idempotent" is not the same as "free". `CREATE TABLE IF NOT EXISTS` against
    DuckLake writes a snapshot even when the table is already there, and a
    2-second daemon calling this every cycle adds tens of thousands of snapshots
    a day to a pipeline that is doing nothing. Measured here: three snapshots
    per *idle* cycle before this check existed. `Engine._prepare_model` avoids
    the same trap by preparing once, and this has to match it.
    """
    if _exists(con, alias, "curated", "location_dim") and _exists(
        con, alias, "curated", "location_hlp"
    ):
        return
    con.execute(f"USE {alias}")
    for statement in _statements(DDL):
        con.execute(statement)


def has_work(con: Any, alias: str = "lake") -> bool:
    """Whether the dimension is actually behind the fact.

    A read, so it writes no snapshot. The alternative -- running the SCD2
    statements unconditionally and letting them match nothing -- still opens a
    transaction and still commits one, which on an idle 2-second daemon is
    where a large part of those tens of thousands of daily snapshots came from.
    """
    con.execute(f"USE {alias}")
    if not _exists(con, alias, "curated", "fact_accelerometer"):
        return False
    new_locations = con.execute(
        """
        SELECT 1 FROM curated.fact_accelerometer f
        WHERE NOT EXISTS (SELECT 1 FROM curated.location_hlp h
                          WHERE lower(h.location_name) = lower(f.location))
        LIMIT 1
        """
    ).fetchone()
    if new_locations:
        return True
    # An attribute change, or a helper row with no current dimension row.
    return bool(
        con.execute(
            """
            SELECT 1 FROM curated.location_hlp h
            LEFT JOIN curated.location_dim d
                   ON d.location_key = h.location_key AND d.is_current = TRUE
            WHERE d.location_key IS NULL
               OR d.city <> h.city OR d.country <> h.country
            LIMIT 1
            """
        ).fetchone()
    )


def maintain(con: Any, alias: str = "lake") -> bool:
    """Assign keys to new locations, then apply SCD2. One transaction.

    Returns whether it did anything. Guarded by :func:`has_work` so an idle
    cycle costs two reads rather than a commit.

    One transaction across both scripts is the requirement, not a convenience:
    `location_dim.sql` expires a changed row and inserts its replacement as two
    statements, so a failure between them would leave a location with **no**
    current row -- and every read-time join in `views.sql` is
    `is_current = TRUE`, so those rows would silently vanish from the marts
    until the next successful run.
    """
    if not has_work(con, alias):
        return False
    con.execute(f"USE {alias}")
    con.execute("BEGIN")
    try:
        for script in ("curated/location_hlp.sql", "curated/location_dim.sql"):
            for statement in _statements((SQL / script).read_text()):
                con.execute(statement)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return True


#: Each view, with the tables it reads. Both halves are load-bearing.
#:
#: The *name* is checked because `CREATE OR REPLACE VIEW` is DDL and DDL writes
#: a catalog snapshot even when the definition is unchanged.
#:
#: The *dependencies* are checked because duckstream creates a sink table
#: **lazily, on first write**. On a fresh catalog with no data yet those tables
#: do not exist, so creating a view over one fails -- and creating all three in
#: a single pass made that failure take the whole cycle down with it. On a new
#: deployment whose sensor has not started, every cycle then failed, and
#: `ProcessingTime` gave up after `max_consecutive_errors`: the pipeline died
#: before it ever ran. Each view is now created on its own, when its inputs
#: appear, so an empty catalog is a normal starting state rather than a fault.
VIEWS = (
    ("marts", "v_accel_hourly_summary",
     (("marts", "accel_hourly_summary"), ("curated", "location_dim"))),
    ("marts", "v_accel_minute_spectrum",
     (("marts", "accel_minute_spectrum"), ("curated", "location_dim"))),
    ("curated", "v_fact_accelerometer",
     (("curated", "fact_accelerometer"), ("curated", "location_dim"))),
)


def create_views(con: Any, alias: str = "lake", *, force: bool = False) -> list[str]:
    """Create each read-time view whose inputs exist. Returns the ones created.

    Never raises for a missing input -- that is the ordinary state of a catalog
    that has not been written to yet, not an error. ``force=True`` redeploys
    definitions after editing `views.sql`.
    """
    created: list[str] = []
    for schema, name, deps in VIEWS:
        if not force and _exists(con, alias, schema, name):
            continue
        if not all(_exists(con, alias, s, t) for s, t in deps):
            continue                      # its inputs have not been written yet
        statement = _statement_for(schema, name)
        if statement is None:
            continue
        con.execute(f"USE {alias}")
        con.execute(statement)
        created.append(f"{schema}.{name}")
    return created


def _statement_for(schema: str, name: str) -> str | None:
    """The single CREATE VIEW statement defining ``schema.name``.

    Matched by the view's own name rather than by position in the file, so
    reordering `views.sql` cannot silently pair a name with the wrong body.
    """
    needle = f"view {schema}.{name}".lower()
    for statement in _statements((SQL / "marts/views.sql").read_text()):
        if needle in " ".join(statement.lower().split()):
            return statement
    return None
