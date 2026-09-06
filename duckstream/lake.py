"""The DuckLake session: attach, settings, and the introspection everything reads.

DuckLake is duckstream's storage layer from phase 1, not an optional backend
(``PLAN.md``, "Storage: DuckLake is the foundation"). This module owns the small
amount of setup that makes the rest of the framework's guarantees true, and the
handful of catalog queries that let tests and metrics *prove* they are true
rather than assert them by faith.

Three things here are load-bearing.

**Inlining is disabled, unconditionally.** ``CONTEXT.md`` section 1.7 measured a
3-row insert producing **zero** data files at the default row limit of 10, and
**one** parquet file with the limit at 0. Inlining is therefore not a performance
knob, it is a switch between two code paths — and the small-batch path is the one
carrying the roughly twelve open correctness bugs listed in section 2.3. Which
path a trigger takes would otherwise depend on how busy the last minute was,
which is the worst kind of surface to debug. :func:`attach_lake` sets the limit
to 0 before it attaches, refuses a caller ``settings`` dict that tries to raise
it, and re-checks the effective value afterwards.

**``DATA_PATH`` belongs only to catalog creation.** An existing catalog already
records its data path; passing a different one is an error and passing the same
one is noise. :func:`attach_lake` detects which case it is in.

**Nothing here opens or owns a connection.** Every function takes ``con`` from
the caller. ``CONTEXT.md`` section 1.6 measured that while one process holds a
DuckDB *file*, no other process can open it even read-only — so a module that
quietly held one would lock the user out of their own warehouse.

Windows note: paths are normalised to forward slashes and single quotes are
escaped before anything reaches SQL. DuckDB accepts forward slashes on Windows,
and a backslash inside a SQL string literal is a trap not worth stepping into.
"""

from __future__ import annotations

import os
import re
from typing import Any

from duckstream.errors import DuckstreamError

__all__ = [
    "DEFAULT_ALIAS",
    "INLINING_SETTING",
    "attach_lake",
    "apply_settings",
    "data_file_count",
    "list_files",
    "normalise_path",
    "snapshot_count",
    "snapshots",
]


#: The DuckLake setting that decides which code path a small write takes.
INLINING_SETTING = "ducklake_default_data_inlining_row_limit"

#: The alias duckstream attaches DuckLake under unless told otherwise.
DEFAULT_ALIAS = "lake"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SETTING_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: Catalog DSNs duckstream cannot probe on the filesystem. For these the
#: create-versus-attach decision falls back to trying and retrying.
_REMOTE_CATALOG_PREFIXES = (
    "postgres:",
    "postgresql:",
    "mysql:",
    "md:",
    "motherduck:",
)

_INSTALL_HINT = (
    "Run `INSTALL ducklake;` once on this device while it has network access. "
    "ATTACH autoloads the extension, but autoload still has to fetch it the "
    "first time, so a disconnected deployment fails on its first run."
)


# --------------------------------------------------------------------------
# SQL rendering helpers
# --------------------------------------------------------------------------


def normalise_path(path: Any) -> str:
    """Render ``path`` as a forward-slash string fit for a SQL literal.

    Accepts ``str``, ``os.PathLike`` and anything ``os.fspath`` understands.
    Backslashes become forward slashes: DuckDB accepts those on Windows, and it
    keeps the value out of any escaping argument with the SQL string parser.
    """
    if path is None:
        raise DuckstreamError("a path is required, got None")
    text = os.fspath(path) if hasattr(path, "__fspath__") else str(path)
    return text.replace("\\", "/")


def _quote_literal(value: str) -> str:
    """Quote ``value`` as a SQL string literal, doubling embedded quotes."""
    return "'" + str(value).replace("'", "''") + "'"


def _quote_identifier(name: str, *, what: str = "identifier") -> str:
    """Validate ``name`` as a plain identifier and return it double-quoted.

    Deliberately stricter than SQL: only ``[A-Za-z_][A-Za-z0-9_]*`` is accepted.
    Aliases, schema and table names all arrive from config, so the cheapest
    defence is to refuse anything that is not a bare identifier rather than to
    try to escape it correctly.
    """
    if not isinstance(name, str) or not _IDENTIFIER.match(name):
        raise DuckstreamError(
            f"{what} {name!r} is not a plain SQL identifier. "
            f"Use letters, digits and underscores, starting with a letter or "
            f"underscore."
        )
    return f'"{name}"'


def _render_setting_value(key: str, value: Any) -> str:
    """Render ``value`` as a literal, refusing anything not obviously safe."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise DuckstreamError(f"setting {key!r} cannot be {value!r}")
        return repr(value)
    if isinstance(value, str):
        return _quote_literal(value)
    raise DuckstreamError(
        f"setting {key!r} has unsupported value type {type(value).__name__}. "
        f"Settings must be str, int, float or bool so that nothing arbitrary "
        f"is interpolated into SQL."
    )


# --------------------------------------------------------------------------
# Catalog DSN handling
# --------------------------------------------------------------------------


def _catalog_target(catalog: Any) -> str:
    """Strip an optional leading ``ducklake:`` and normalise slashes.

    ``PLAN.md`` writes the catalog both ways — ``Engine(catalog=
    "ducklake:catalog.ducklake")`` in the Python example and a bare path in
    prose. Accept both rather than making the caller remember which.
    """
    text = normalise_path(catalog)
    if text.lower().startswith("ducklake:"):
        text = text[len("ducklake:") :]
    if not text:
        raise DuckstreamError("catalog must be a non-empty path or DSN")
    return text


def _catalog_exists(target: str) -> bool | None:
    """Does this catalog already exist?  ``None`` means "cannot tell".

    File-backed catalogs — the DuckDB default and SQLite — are a filesystem
    check. A PostgreSQL or MotherDuck DSN is not, so the caller falls back to
    attempting the attach.
    """
    lowered = target.lower()
    for prefix in _REMOTE_CATALOG_PREFIXES:
        if lowered.startswith(prefix):
            return None
    path = target
    for prefix in ("duckdb:", "sqlite:"):
        if lowered.startswith(prefix):
            path = target[len(prefix) :]
            break
    if not path:
        return None
    try:
        return os.path.exists(path)
    except OSError:  # pragma: no cover - exotic path, treat as unknown
        return None


def _attached_aliases(con) -> set[str]:
    try:
        rows = con.execute("SELECT database_name FROM duckdb_databases()").fetchall()
    except Exception:  # pragma: no cover - very old builds
        return set()
    return {row[0] for row in rows}


def _install_and_load(con) -> None:
    """``INSTALL ducklake; LOAD ducklake;`` explicitly.

    ``CONTEXT.md`` section 1.7 verified that ``ATTACH 'ducklake:...'`` autoloads
    the extension. Being explicit still costs nothing, and it turns the
    air-gapped first-run failure into a message that names the fix.
    """
    for statement in ("INSTALL ducklake", "LOAD ducklake"):
        try:
            con.execute(statement)
        except Exception as exc:
            raise DuckstreamError(
                f"could not {statement.split()[0].lower()} the DuckLake "
                f"extension: {exc}. {_INSTALL_HINT}"
            ) from exc


# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------


def _check_inlining_request(key: str, value: Any) -> None:
    if key != INLINING_SETTING:
        return
    try:
        numeric = int(value)
    except (TypeError, ValueError) as exc:
        raise DuckstreamError(
            f"{INLINING_SETTING} must be 0. duckstream disables DuckLake data "
            f"inlining unconditionally; {value!r} is not even a number."
        ) from exc
    if numeric != 0:
        raise DuckstreamError(
            f"{INLINING_SETTING}={numeric} was requested, but duckstream "
            f"disables DuckLake data inlining unconditionally and will not let "
            f"a settings dict re-enable it. At the default of 10, any trigger "
            f"writing fewer than 10 rows silently takes the inlining path — "
            f"measured in CONTEXT.md 1.7 as a 3-row insert producing zero data "
            f"files — and that path carries the open correctness bugs listed in "
            f"CONTEXT.md 2.3. Which path a batch takes would then depend on how "
            f"busy the last minute was. Remove the setting, or pass 0."
        )


def apply_settings(con, settings: dict[str, Any] | None) -> None:
    """Apply caller settings with ``SET``, refusing anything unsafe.

    Keys must be plain identifiers and values must be ``str``, ``int``,
    ``float`` or ``bool``, so that nothing arbitrary is interpolated into SQL.
    ``ducklake_default_data_inlining_row_limit`` is special-cased: a non-zero
    value raises rather than being quietly accepted.
    """
    if not settings:
        return
    if not isinstance(settings, dict):
        raise DuckstreamError(
            f"settings must be a dict, got {type(settings).__name__}"
        )
    for key, value in settings.items():
        if not isinstance(key, str) or not _SETTING_NAME.match(key):
            raise DuckstreamError(
                f"setting name {key!r} is not a plain identifier. Settings are "
                f"interpolated into `SET`, so only letters, digits and "
                f"underscores are accepted."
            )
        _check_inlining_request(key, value)
        rendered = _render_setting_value(key, value)
        try:
            con.execute(f"SET {key} = {rendered}")
        except Exception as exc:
            raise DuckstreamError(f"could not apply setting {key}={value!r}: {exc}") from exc


def _disable_inlining(con) -> None:
    try:
        con.execute(f"SET {INLINING_SETTING} = 0")
    except Exception as exc:
        raise DuckstreamError(
            f"could not disable DuckLake data inlining: {exc}. {_INSTALL_HINT}"
        ) from exc


def _assert_inlining_disabled(con) -> None:
    try:
        row = con.execute(
            f"SELECT current_setting('{INLINING_SETTING}')"
        ).fetchone()
    except Exception as exc:  # pragma: no cover - setting always exists once loaded
        raise DuckstreamError(
            f"could not read {INLINING_SETTING} back: {exc}"
        ) from exc
    effective = int(row[0]) if row and row[0] is not None else -1
    if effective != 0:
        raise DuckstreamError(
            f"{INLINING_SETTING} is {effective} after setup, expected 0. "
            f"duckstream refuses to run with DuckLake data inlining enabled "
            f"(CONTEXT.md 1.7 and 2.3)."
        )


# --------------------------------------------------------------------------
# Attach
# --------------------------------------------------------------------------


def _attach_statement(target: str, alias: str, data_path: str | None) -> str:
    dsn = _quote_literal(f"ducklake:{target}")
    stmt = f"ATTACH {dsn} AS {_quote_identifier(alias, what='catalog alias')}"
    if data_path is not None:
        stmt += f" (DATA_PATH {_quote_literal(data_path)})"
    return stmt


def attach_lake(
    con,
    catalog: Any,
    *,
    data_path: Any | None = None,
    alias: str = DEFAULT_ALIAS,
    settings: dict[str, Any] | None = None,
) -> None:
    """Attach a DuckLake catalog on ``con`` and make it the current database.

    Performs the setup ``PLAN.md`` specifies under "Required DuckLake setup":
    explicit ``INSTALL``/``LOAD``, ``ATTACH``, ``USE``, and the settings the
    engine must own rather than inherit.

    Parameters
    ----------
    con:
        An open DuckDB connection. This module never opens one of its own and
        never closes this one — see the module docstring and ``CONTEXT.md`` 1.6.
    catalog:
        Catalog path or DSN, with or without a leading ``ducklake:``.
    data_path:
        Where parquet data files go. Passed to ``ATTACH`` **only when the
        catalog is being created**; an existing catalog already records it, and
        supplying a different one is an error DuckLake raises itself.
    alias:
        Catalog alias, default ``"lake"``. Must be a plain identifier.
    settings:
        Extra ``SET`` values such as ``memory_limit`` or ``threads``. A non-zero
        ``ducklake_default_data_inlining_row_limit`` here raises: inlining is
        not a caller decision.

    Notes
    -----
    Re-attaching an alias that is already present on ``con`` is a no-op apart
    from ``USE`` and the settings, so calling this twice in one session — which
    a CLI plus a library user easily can — does not fail.
    """
    alias_sql = _quote_identifier(alias, what="catalog alias")
    target = _catalog_target(catalog)
    normalised_data_path = None if data_path is None else normalise_path(data_path)

    _install_and_load(con)

    # Before ATTACH, so that no write on this connection can ever have taken the
    # inlining path, not even one issued between attach and the settings pass.
    _disable_inlining(con)

    if alias not in _attached_aliases(con):
        exists = _catalog_exists(target)
        if exists is True:
            # Already recorded; DATA_PATH would be at best redundant.
            statements = [_attach_statement(target, alias, None)]
        elif exists is False:
            statements = [_attach_statement(target, alias, normalised_data_path)]
        else:
            # Remote catalog: cannot probe it, so try to create and fall back to
            # a plain attach if it turns out to exist already.
            statements = [
                _attach_statement(target, alias, normalised_data_path),
                _attach_statement(target, alias, None),
            ]
        last_error: Exception | None = None
        for statement in statements:
            try:
                con.execute(statement)
                last_error = None
                break
            except Exception as exc:
                last_error = exc
        if last_error is not None:
            raise DuckstreamError(
                f"could not attach DuckLake catalog {target!r} as {alias!r}: "
                f"{last_error}. {_INSTALL_HINT}"
            ) from last_error

    con.execute(f"USE {alias_sql}")

    apply_settings(con, settings)

    # Caller settings ran first so that a bad one is reported on its own terms;
    # this re-assert is what makes "unconditionally" true regardless.
    _disable_inlining(con)
    _assert_inlining_disabled(con)


# --------------------------------------------------------------------------
# Introspection
# --------------------------------------------------------------------------


def snapshot_count(con, alias: str = DEFAULT_ALIAS) -> int:
    """Number of DuckLake snapshots in ``alias``.

    The measurement the exactly-once claim rests on: ``CONTEXT.md`` 1.4 found
    that two inserts inside ``BEGIN ... COMMIT`` advance this by exactly 1,
    while five autocommitted inserts advance it by 5. Tests take this before and
    after a trigger and assert a delta of 1.
    """
    _quote_identifier(alias, what="catalog alias")
    row = con.execute(
        f"SELECT count(*) FROM ducklake_snapshots({_quote_literal(alias)})"
    ).fetchone()
    return int(row[0]) if row else 0


def snapshots(con, alias: str = DEFAULT_ALIAS) -> list[dict[str, Any]]:
    """The snapshot history of ``alias``, oldest first.

    ``snapshot_time`` is returned as text on purpose. The column is ``TIMESTAMP
    WITH TIME ZONE``, and converting one of those to Python needs ``pytz``,
    which is not a duckstream dependency — ``SELECT * FROM ducklake_snapshots()``
    fails outright without it. Casting in SQL keeps this helper dependency-free,
    which matters because tests and the CLI both call it.
    """
    _quote_identifier(alias, what="catalog alias")
    rows = con.execute(
        "SELECT snapshot_id, snapshot_time::VARCHAR AS snapshot_time, "
        "schema_version, changes "
        f"FROM ducklake_snapshots({_quote_literal(alias)}) ORDER BY snapshot_id"
    ).fetchall()
    return [
        {
            "snapshot_id": int(row[0]),
            "snapshot_time": row[1],
            "schema_version": int(row[2]) if row[2] is not None else None,
            "changes": row[3],
        }
        for row in rows
    ]


def _split_table(table: str, schema: str | None) -> tuple[str, str]:
    if not isinstance(table, str) or not table:
        raise DuckstreamError("table must be a non-empty name")
    if schema is None and "." in table:
        parts = table.split(".")
        if len(parts) != 2:
            raise DuckstreamError(
                f"table {table!r} must be 'table' or 'schema.table'"
            )
        schema, table = parts
    resolved_schema = schema or "main"
    _quote_identifier(resolved_schema, what="schema name")
    _quote_identifier(table, what="table name")
    return resolved_schema, table


def list_files(
    con,
    alias: str,
    table: str,
    *,
    schema: str | None = None,
) -> list[dict[str, Any]]:
    """Rows of ``ducklake_list_files`` for one table, as plain dicts.

    ``table`` may be ``"name"`` or ``"schema.name"``; ``schema`` overrides the
    parsed one and defaults to ``main``.

    This is how "inlining stayed off" is proved rather than assumed. With
    inlining on, ``CONTEXT.md`` 1.7 measured a 3-row insert leaving this
    **empty** — the rows went into the catalog instead of a data file. A
    non-empty result after a small batch is the evidence that the batch took the
    ordinary parquet path.
    """
    _quote_identifier(alias, what="catalog alias")
    resolved_schema, table_name = _split_table(table, schema)
    sql = (
        "SELECT * FROM ducklake_list_files("
        f"{_quote_literal(alias)}, {_quote_literal(table_name)}, "
        f"schema => {_quote_literal(resolved_schema)})"
    )
    cursor = con.execute(sql)
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def data_file_count(
    con,
    alias: str,
    table: str,
    *,
    schema: str | None = None,
) -> int:
    """How many parquet data files back ``table`` right now.

    Counts entries with a non-null ``data_file``; delete files are not data
    files. Zero after a write means the rows were inlined into the catalog,
    which duckstream treats as a setup failure rather than an optimisation.
    """
    return sum(
        1
        for row in list_files(con, alias, table, schema=schema)
        if row.get("data_file")
    )
