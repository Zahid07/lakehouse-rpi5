"""A small read-only web backend over the accelerometer lakehouse.

Standard library only, beyond the `duckdb` the pipeline already needs. On a Pi
with 4 GB of RAM, adding a web framework to serve six JSON endpoints is a cost
with no matching benefit, and `duckstream` itself is deliberately down to four
dependencies.

**The one design constraint that shapes everything here: the catalog is
single-attach.** Measured on this machine -- while one process holds a DuckLake
catalog, a second cannot `ATTACH` it *even READ_ONLY*, and even while the holder
sits idle.

The obvious response is "attach per request, detach immediately", and that is
what this file did first. **It starved the pipeline.** Measured: 60 API requests
during active writes all returned 200, while **six of eight pipeline cycles
failed** with `Conflicting lock is held ... by PID <the server>`. Holding the
catalog *briefly* is not enough if you take it *often* -- one page open makes
four requests every three seconds, two pages make eight, and the writer needs a
clear window of a second or more. Worse, the reader retried on conflict and the
pipeline did not, so every collision was resolved in the reader's favour. A
dashboard that stops the pipeline is worse than no dashboard.

So the catalog is read **once per refresh interval by a single background
thread**, and every HTTP request is served from that snapshot without touching
the catalog at all. Catalog contact is then a fixed ~one short read every few
seconds no matter how many browsers are open, which leaves the writer the rest
of the time. Two supporting rules:

* the refresher **yields** rather than competes -- one attempt, no retry loop,
  and a longer wait after a conflict, because the pipeline has no retry of its
  own and must be allowed to win;
* the landing tree is read with `read_parquet` and needs no catalog, so the
  freshest numbers -- what has landed but is not yet processed -- are always
  available even while the pipeline holds the lock.

Run it::

    python app/server.py                 # http://0.0.0.0:8080
    python app/server.py --port 9000 --refresh 5
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb

HERE = Path(__file__).resolve().parent
ROOT = Path(os.environ.get("DS_ROOT", str(Path.home() / "duckstream-accel")))
LANDING = Path(os.environ.get("DS_LANDING", str(ROOT / "landing")))
CATALOG = ROOT / "catalog.ducklake"

#: Fallback timer between catalog reads. Slow on purpose: the pipeline pokes
#: `/api/refresh` the instant it finishes a cycle, and that poked read is the
#: one guaranteed not to collide with a write. This timer only covers the
#: cases where no notification arrives -- pipeline not running, started
#: without `--notify`, or a lost request.
DEFAULT_REFRESH = 15.0

#: Extra wait after losing the race to the pipeline. Longer than the refresh
#: interval on purpose: the writer has no retry of its own, so the reader is
#: the one that has to give ground.
YIELD_SECONDS = 4.0


class CatalogBusy(RuntimeError):
    """The pipeline holds the catalog. Expected, transient, not a fault."""


def _is_lock_conflict(exc: Exception) -> bool:
    text = str(exc).lower()
    return "conflicting lock" in text or "could not set lock" in text


class Reader:
    """One attach, many queries, then detach. Used only by the refresher.

    Batching every query of a refresh into a *single* attach matters as much as
    the caching does: seven endpoints attaching separately is seven chances to
    collide with the pipeline, where one attach is one chance and holds the
    catalog for barely longer.
    """

    def __init__(self) -> None:
        self.con = duckdb.connect()
        self.con.execute("INSTALL ducklake")
        self.con.execute("LOAD ducklake")
        try:
            self.con.execute(
                f"ATTACH 'ducklake:{CATALOG.as_posix()}' AS l (READ_ONLY)"
            )
        except Exception as exc:  # noqa: BLE001
            self.con.close()
            if _is_lock_conflict(exc):
                raise CatalogBusy(str(exc)[:200]) from exc
            raise
        self.con.execute("SET temp_directory=''")

    def query(self, sql: str, params: list | None = None) -> list[dict]:
        cur = self.con.execute(sql, params or [])
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

    def table_exists(self, name: str) -> bool:
        schema, _, table = name.partition(".")
        return bool(self.query(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_catalog='l' AND table_schema=? AND table_name=? LIMIT 1",
            [schema, table],
        ))

    def close(self) -> None:
        try:
            self.con.execute("DETACH l")     # release the lock explicitly
        except Exception:  # noqa: BLE001
            pass
        self.con.close()

    def __enter__(self) -> "Reader":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# -- the snapshot, refreshed by one background thread ------------------------

CACHE: dict = {"state": "starting", "catalog_exists": CATALOG.exists()}
CACHE_LOCK = threading.Lock()


def read_landing() -> dict:
    """The fresh end of the pipe. No catalog, so nothing can block it.

    `chunks_total` counts every chunk that has ever landed, processed or not.
    It is **not** a backlog and must not be labelled as one: the pipeline never
    deletes a chunk after consuming it -- a tier-three model reads consumed
    files back when it recomputes a window, so removing one would silently
    rebuild that window from part of its data. The count therefore only ever
    grows. `read_catalog` pairs it with what has actually been consumed to give
    a backlog that means something.
    """
    out: dict = {}
    files = sorted(glob.glob(str(LANDING / "**" / "*.parquet"), recursive=True))
    out["chunks_total"] = len(files)
    if not files:
        out["chunks_processed"] = 0
        out["chunks_remaining"] = 0
        return out
    # Newest few only: reading every file would make this slower the longer the
    # deployment has run, which is the one thing a status endpoint must not do.
    flist = "[" + ",".join(f"'{f}'" for f in files[-8:]) + "]"
    con = duckdb.connect()
    try:
        rows, latest = con.execute(
            f"SELECT count(*), max(timestamp) FROM read_parquet({flist})"
        ).fetchone()
        out["landing_recent_rows"] = rows
        out["landing_latest"] = latest.isoformat() if latest else None
    except Exception as exc:  # noqa: BLE001
        out["landing_error"] = str(exc)[:200]
    finally:
        con.close()
    return out


def read_progress(r: Reader, total: int) -> dict:
    """Chunks processed and chunks remaining -- a backlog that means something.

    A chunk counts as **processed only when every model has consumed it**, not
    when any one has. The models run in the same cycle but consume
    independently, and reporting the fastest would show zero remaining while a
    slower model was still behind -- which is exactly the reassuring-but-wrong
    number this replaced.

    So: group `consumed_files` by chunk, count how many distinct models have
    taken each, and call a chunk done when that reaches the number of models
    that have consumed anything at all.
    """
    if not r.table_exists("duckstream.consumed_files"):
        return {"chunks_processed": 0, "chunks_remaining": total}

    models = r.query(
        "SELECT count(DISTINCT model_name) AS n FROM l.duckstream.consumed_files"
    )[0]["n"]
    if not models:
        return {"chunks_processed": 0, "chunks_remaining": total}

    done = r.query(
        "SELECT count(*) AS n FROM ("
        "  SELECT relpath FROM l.duckstream.consumed_files "
        "  GROUP BY relpath HAVING count(DISTINCT model_name) >= ?"
        ")",
        [models],
    )[0]["n"]

    return {
        "chunks_processed": done,
        # Clamped at zero: a chunk deleted from the landing tree by hand would
        # otherwise make this negative, and a negative backlog is nonsense on a
        # dashboard even if the arithmetic is honest.
        "chunks_remaining": max(0, total - done),
        "models_consuming": models,
    }


def read_throughput(r: Reader, limit: int = 120) -> list[dict]:
    """Per-batch timings: when, how many chunks, how big, how long.

    Nothing is instrumented for this -- duckstream already records both halves
    and they only needed joining. `duckstream.batches` carries `started_at` and
    `committed_at` per batch, and `duckstream.consumed_files` carries the
    `size` of every chunk with the `batch_id` that took it. So chunks, bytes,
    rows and duration all fall out of one join.

    Reported **per model**, not summed, because the three models process the
    same chunks at very different speeds -- the FFT model is roughly half the
    throughput of the other two on this hardware, and averaging that away would
    hide the one number worth watching.
    """
    if not (r.table_exists("duckstream.batches")
            and r.table_exists("duckstream.consumed_files")):
        return []
    return r.query(
        """
        SELECT b.model_name,
               b.batch_id,
               b.committed_at,
               date_diff('millisecond', b.started_at, b.committed_at) AS ms,
               b.rows_in,
               b.rows_out,
               count(f.relpath)             AS chunks,
               coalesce(sum(f."size"), 0)   AS bytes
        FROM l.duckstream.batches b
        LEFT JOIN l.duckstream.consumed_files f
               ON f.model_name = b.model_name AND f.batch_id = b.batch_id
        GROUP BY ALL
        ORDER BY b.committed_at DESC
        LIMIT ?
        """,
        [limit],
    )


def read_catalog(r: Reader) -> dict:
    """Everything the page shows, in one attach."""
    out: dict = {"hourly": [], "spectrum": [], "recent": [], "dimension": [],
                 "throughput": []}

    if r.table_exists("curated.fact_accelerometer"):
        row = r.query(
            "SELECT count(*) AS readings, max(timestamp) AS latest, "
            "count(DISTINCT location) AS locations FROM l.curated.fact_accelerometer"
        )[0]
        out["readings"] = row["readings"]
        out["locations"] = row["locations"]
        out["latest_reading"] = row["latest"].isoformat() if row["latest"] else None
        # x, y and z as well as magnitude: the chart only needs magnitude, but
        # the fact table below shows the raw reading, and fetching them
        # separately would be a second pass over the same rows.
        out["recent"] = list(reversed(r.query(
            "SELECT timestamp, location, x, y, z, magnitude "
            "FROM l.curated.fact_accelerometer ORDER BY timestamp DESC LIMIT 600"
        )))
    for label, table in (("hours", "marts.accel_hourly_summary"),
                         ("minutes", "marts.accel_minute_spectrum")):
        if r.table_exists(table):
            out[label] = r.query(f"SELECT count(*) AS n FROM l.{table}")[0]["n"]
    if r.table_exists("curated.location_dim"):
        out["dimension"] = r.query(
            "SELECT location_key, location_name, city, country "
            "FROM l.curated.location_dim WHERE is_current ORDER BY 1"
        )
    if r.table_exists("marts.v_accel_hourly_summary"):
        out["hourly"] = r.query(
            "SELECT hour_ts, location_name, city, sample_count, avg_x, avg_y, "
            "avg_z, avg_magnitude, min_magnitude, max_magnitude, stddev_magnitude "
            "FROM l.marts.v_accel_hourly_summary ORDER BY hour_ts DESC LIMIT 24"
        )
    if r.table_exists("marts.v_accel_minute_spectrum"):
        # A minute at 100 Hz is ~3,000 bins; sending them all for four windows
        # is megabytes of JSON to draw a few hundred pixels. The structure that
        # matters in accelerometer data is at the low end.
        bins = 240
        rows = r.query(
            "SELECT window_ts, location_name, sample_count, "
            "freq_hz[1:?] AS freq, fft_magnitude[1:?] AS mag, "
            "len(fft_magnitude) AS total_bins "
            "FROM l.marts.v_accel_minute_spectrum ORDER BY window_ts DESC LIMIT 4",
            [bins, bins],
        )
        for row in rows:
            row["shown_bins"] = len(row.get("mag") or [])
        out["spectrum"] = rows
    out["throughput"] = list(reversed(read_throughput(r)))   # oldest first, for a time axis
    return out


def refresh_once() -> None:
    """One pass. Never raises -- a failed refresh keeps the previous snapshot."""
    snapshot: dict = {
        "catalog": str(CATALOG),
        "catalog_exists": CATALOG.exists(),
        "server_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    snapshot.update(read_landing())

    if not CATALOG.exists():
        snapshot["state"] = "waiting for the first commit"
    else:
        try:
            with Reader() as reader:
                snapshot.update(read_catalog(reader))
                snapshot.update(
                    read_progress(reader, snapshot.get("chunks_total", 0))
                )
            snapshot["state"] = "live"
        except CatalogBusy:
            # The pipeline is mid-cycle. Keep the previous catalog figures and
            # say the page is showing them -- stale and honest beats empty.
            with CACHE_LOCK:
                previous = dict(CACHE)
            # `chunks_processed` is carried too, but `chunks_total` is not:
            # that one comes from the files and is always current, so the
            # remaining count is recomputed from a fresh total against the last
            # known processed figure -- which is the right way round. A backlog
            # that froze while chunks kept landing would hide the very thing it
            # exists to show.
            for key in ("readings", "locations", "latest_reading", "hours",
                        "minutes", "dimension", "hourly", "spectrum", "recent",
                        "throughput", "chunks_processed", "models_consuming"):
                if key in previous:
                    snapshot[key] = previous[key]
            snapshot["chunks_remaining"] = max(
                0,
                snapshot.get("chunks_total", 0)
                - snapshot.get("chunks_processed", 0),
            )
            snapshot["state"] = "pipeline is writing"
            snapshot["stale"] = True
        except Exception as exc:  # noqa: BLE001
            snapshot["state"] = "error"
            snapshot["error"] = f"{type(exc).__name__}: {str(exc)[:200]}"

    snapshot["refreshed_at"] = time.time()
    with CACHE_LOCK:
        CACHE.clear()
        CACHE.update(snapshot)


#: Set by ``POST /api/refresh``. The pipeline pokes this the moment a cycle
#: finishes, which is the moment it has detached -- so the read that follows is
#: the one read guaranteed *not* to collide with a write.
WAKE = threading.Event()


def refresher(interval: float, stop: threading.Event) -> None:
    """Refresh when poked, and on a slow timer as a fallback.

    Push beats polling here for a reason beyond freshness. A timer picks its
    moment blindly and can land mid-write; the pipeline's notification arrives
    *after* it has released the catalog, so a poked refresh is the one that
    cannot conflict. The timer stays as a fallback for when the pipeline is not
    running, was started without ``--notify``, or the notification was lost --
    and it is deliberately slow, because with push it should rarely be the
    thing that fires.
    """
    while not stop.is_set():
        started = time.monotonic()
        refresh_once()
        with CACHE_LOCK:
            busy = CACHE.get("stale", False)
        # Yield harder after losing the race: the pipeline has no retry of its
        # own, so competing on equal terms means the writer loses.
        wait = YIELD_SECONDS if busy else interval
        remaining = max(0.5, wait - (time.monotonic() - started))

        # Wake early if the pipeline says it has just finished a cycle. The
        # flag is cleared before the next read rather than after, so a poke
        # arriving *during* a read still schedules one more.
        if WAKE.wait(remaining):
            WAKE.clear()
        if stop.is_set():
            return


def snapshot() -> dict:
    with CACHE_LOCK:
        out = dict(CACHE)
    age = time.time() - out.get("refreshed_at", 0)
    out["age_seconds"] = round(age, 1) if out.get("refreshed_at") else None
    return out



# -- live history queries ----------------------------------------------------
#
# Everything above is served from the cached snapshot and never touches the
# catalog. History is the one exception, and it is a deliberate one: browsing
# backwards is **user-initiated and occasional**, not a 3-second poll, so a
# single attach now and then is a very different proposition from the
# per-request attaching that starved the pipeline earlier. The discipline that
# makes it safe is the same either way -- one attempt, no retry loop, and a 503
# rather than competing with the writer.

def live_readings(limit: int, before: str | None, location: str | None) -> dict:
    """A page of `curated.fact_accelerometer`, oldest-bounded by `before`.

    Keyset pagination on `timestamp`, not OFFSET: an offset scan re-reads every
    skipped row, so paging back through a day of 100 Hz data would get slower
    with every click. `before` is the oldest timestamp already shown, so each
    page starts exactly where the last ended.
    """
    if not CATALOG.exists():
        return {"rows": [], "state": "waiting for the first commit"}

    where = []
    params: list = []
    if before:
        where.append("timestamp < ?")
        params.append(before)
    if location:
        where.append("location = ?")
        params.append(location)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    params.append(max(1, min(limit, 2000)))

    with Reader() as r:                      # raises CatalogBusy -> 503
        if not r.table_exists("curated.fact_accelerometer"):
            return {"rows": [], "state": "no fact table yet"}
        rows = r.query(
            f"SELECT timestamp, location, x, y, z, magnitude "
            f"FROM l.curated.fact_accelerometer {clause} "
            f"ORDER BY timestamp DESC LIMIT ?",
            params,
        )
        span = r.query(
            "SELECT min(timestamp) AS earliest, max(timestamp) AS latest, "
            "count(*) AS total FROM l.curated.fact_accelerometer"
        )[0]
    return {
        "rows": rows,
        "earliest": span["earliest"].isoformat() if span["earliest"] else None,
        "latest": span["latest"].isoformat() if span["latest"] else None,
        "total": span["total"],
        "state": "live",
    }


ROUTES = {
    "/api/status": lambda q: {
        k: v for k, v in snapshot().items()
        if k not in ("hourly", "spectrum", "recent", "throughput")
    },
    "/api/hourly": lambda q: snapshot().get("hourly", []),
    "/api/spectrum": lambda q: snapshot().get("spectrum", []),
    "/api/recent": lambda q: snapshot().get("recent", []),
    "/api/throughput": lambda q: snapshot().get("throughput", []),
    # The only endpoint that reads the catalog live; see `live_readings`.
    "/api/readings": lambda q: live_readings(
        int(q.get("limit", ["50"])[0]),
        (q.get("before") or [None])[0],
        (q.get("location") or [None])[0],
    ),
    "/api/all": lambda q: snapshot(),
}


def encode(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"{type(value).__name__} is not JSON serialisable")


class Handler(BaseHTTPRequestHandler):
    server_version = "accel-lakehouse/1.0"

    def _cors(self) -> None:
        """Allow any origin. This is a read-only dashboard on a private LAN.

        Without it a React dev server on another machine cannot call this at
        all: the browser blocks the response before the app ever sees it, and
        the failure looks like "the backend is down" rather than "the browser
        refused it". `*` is right here because every endpoint is read-only,
        carries no credentials and exposes nothing that is not already on the
        page -- narrow it to the dev server's origin if this ever leaves a
        trusted network.
        """
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self) -> None:  # noqa: N802
        """The preflight a cross-origin POST to /api/refresh triggers."""
        self.send_response(204)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, payload) -> None:
        body = json.dumps(payload, default=encode).encode("utf-8")
        self._send(code, body, "application/json; charset=utf-8")

    def do_POST(self) -> None:  # noqa: N802
        """Only /api/refresh. Accepts POST as well as GET so the notification
        reads as the state change it is, while `curl localhost:8080/api/refresh`
        still works from a shell."""
        if urlparse(self.path).path == "/api/refresh":
            WAKE.set()
            self._json(202, {"accepted": True})
        else:
            self._json(404, {"error": "no such route"})

    def do_GET(self) -> None:  # noqa: N802  (BaseHTTPRequestHandler's API)
        parsed = urlparse(self.path)
        route = parsed.path

        if route in ("/", "/index.html"):
            page = HERE / "index.html"
            if not page.exists():
                self._json(500, {"error": "index.html is missing"})
                return
            self._send(200, page.read_bytes(), "text/html; charset=utf-8")
            return

        if route == "/api/refresh":
            # Accepted, not done: the refresh happens on the background thread
            # so this returns at once. The pipeline calls it from its own loop
            # and must never be made to wait on a dashboard.
            WAKE.set()
            self._json(202, {"accepted": True})
            return

        handler = ROUTES.get(route)
        if handler is None:
            self._json(404, {"error": f"no route {route}"})
            return

        try:
            self._json(200, handler(parse_qs(parsed.query)))
        except FileNotFoundError:
            self._json(200, {"state": "waiting for the first commit",
                             "catalog_exists": False})
        except CatalogBusy as exc:
            # 503 with Retry-After rather than 500: this is the pipeline doing
            # its job, and the page should say so and try again shortly.
            body = json.dumps({"error": "catalog busy", "detail": str(exc)[:200]})
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Retry-After", "1")
            self.send_header("Content-Length", str(len(body)))
            self._cors()   # error responses need it too, or the browser hides why
            self.end_headers()
            self.wfile.write(body.encode())
        except Exception as exc:  # noqa: BLE001
            self._json(500, {"error": type(exc).__name__, "detail": str(exc)[:400]})

    def log_message(self, fmt: str, *args) -> None:
        # One tidy line per request; the default writes to stderr with a
        # timestamp format that is hard to grep alongside the pipeline log.
        print(f"{self.address_string()} {fmt % args}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument(
        "--refresh", type=float, default=DEFAULT_REFRESH,
        help=(
            "seconds between catalog reads (default %(default)s). This is the "
            "only thing that touches the catalog, whatever the traffic — raise "
            "it if the pipeline reports lock conflicts"
        ),
    )
    args = parser.parse_args()

    print(f"catalog : {CATALOG}")
    print(f"landing : {LANDING}")
    print(f"serving : http://{args.host}:{args.port}/")
    print(f"refresh : one catalog read every {args.refresh:g}s, cached — HTTP "
          f"requests never touch the catalog")

    stop = threading.Event()
    worker = threading.Thread(
        target=refresher, args=(args.refresh, stop), daemon=True,
        name="catalog-refresher",
    )
    worker.start()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        stop.set()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
