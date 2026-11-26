"""Read-only web backend for the engine vibration pipeline, over ClickHouse.

Ported from lakehouse-rpi5/engine_app/server.py. The endpoints and their JSON
shapes are kept, so index.html needed only small changes. What went away is
the reason most of the original existed: the DuckLake catalog was
single-attach, so every read had to be rationed around the pipeline's writes.
ClickHouse serves concurrent readers and writers, so this simply queries it.

Standard library only -- ClickHouse is reached over its HTTP interface, and
even the spectrogram's uint8 quantisation and base64 happen inside ClickHouse.

One background thread builds a snapshot and every HTTP request is served from
it, so ClickHouse's cost is fixed however many browsers are open. It refreshes
in three tiers, because the data changes at three speeds:

    fast   ( 5 s)  loss, active machines, waveforms, queue backlog, summary
    medium (10 s)  spectrogram (live minute via the stft_db UDF), ingest rate
    slow   (30 s)  minute health, anomaly score, machines, refresh jobs

Minute values change once a minute, so reading them every 5 s -- as the first
version did -- spent most of ClickHouse's CPU re-deriving the same numbers.

Run it::

    CH_PASSWORD=... RMQ_PASSWORD=... python3 app/server.py      # :8090
    python3 app/server.py --port 8090 --fast 5 --medium 10 --slow 30

CH_PASSWORD is prompted for if unset. RMQ_PASSWORD is optional; without it
the queue-backlog figures are simply absent.
"""

from __future__ import annotations

import argparse
import base64
import getpass
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import hostmetrics

HERE = Path(__file__).resolve().parent

CH_URL = os.environ.get("CH_URL", "http://localhost:8123")
CH_USER = os.environ.get("CH_USER", "default")
CH_DATABASE = os.environ.get("CH_DATABASE", "engine")
RMQ_API = os.environ.get("RMQ_API", "http://localhost:15672/api")
RMQ_USER = os.environ.get("RMQ_USER", "iot")
RMQ_QUEUE = os.environ.get("RMQ_QUEUE", "clickhouse_engine")
SAMPLE_RATE_HZ = float(os.environ.get("ENG_SAMPLE_RATE_HZ", "500"))

#: The dB range the uint8 quantisation spans. Fixed, never auto-scaled: a
#: spectrogram whose scale moves is one where two minutes cannot be compared.
DB_MIN, DB_MAX = -90.0, -10.0


class ClickHouseError(RuntimeError):
    pass


class ClickHouse:
    """The HTTP interface, with query parameters bound server-side."""

    def __init__(self, url: str, user: str, password: str, database: str) -> None:
        self.url, self.user, self.password, self.database = url, user, password, database

    def _post(self, query: str, params: dict | None, body: bytes | None,
              fmt: str | None) -> bytes:
        args = {
            "database": self.database,
            # ISO with a Z: every column is UTC, and the browser parses it as such.
            "date_time_output_format": "iso",
            "output_format_json_quote_64bit_integers": "0",
        }
        if fmt:
            args["default_format"] = fmt
        for name, value in (params or {}).items():
            args[f"param_{name}"] = str(value)
        if body is None:
            data = query.encode()
        else:
            args["query"] = query
            data = body
        request = urllib.request.Request(
            f"{self.url}/?{urllib.parse.urlencode(args)}", data=data, method="POST",
            headers={"X-ClickHouse-User": self.user, "X-ClickHouse-Key": self.password},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace").strip()
            raise ClickHouseError(detail.splitlines()[0][:300] if detail else str(exc)) from None
        except OSError as exc:
            raise ClickHouseError(f"cannot reach ClickHouse at {self.url}: {exc}") from None

    def query(self, sql: str, **params) -> list[dict]:
        return json.loads(self._post(sql, params, None, "JSON"))["data"]

    def insert(self, table: str, rows: list[dict]) -> None:
        body = "\n".join(json.dumps(r, default=str) for r in rows).encode()
        self._post(f"INSERT INTO {table} FORMAT JSONEachRow", None, body, None)


CH: ClickHouse | None = None
HOST: hostmetrics.HostMetrics | None = None


def rabbitmq_queue(password: str | None) -> dict:
    """Backlog and rates of the ClickHouse queue, from the management API."""
    if not password:
        return {}
    url = f"{RMQ_API}/queues/%2F/{urllib.parse.quote(RMQ_QUEUE)}"
    token = base64.b64encode(f"{RMQ_USER}:{password}".encode()).decode()
    request = urllib.request.Request(url, headers={"Authorization": f"Basic {token}"})
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            q = json.loads(response.read())
    except (OSError, ValueError) as exc:
        return {"queue_error": str(exc)[:160]}
    stats = q.get("message_stats", {})
    return {
        "queue_name": RMQ_QUEUE,
        "queue_ready": q.get("messages_ready"),
        "queue_unacked": q.get("messages_unacknowledged"),
        "queue_consumers": q.get("consumers"),
        "queue_in_rate": round(stats.get("publish_details", {}).get("rate", 0.0), 1),
        "queue_out_rate": round(stats.get("deliver_get_details", {}).get("rate", 0.0), 1),
    }


# -- the snapshot -------------------------------------------------------------

CACHE: dict = {"state": "starting"}
CACHE_LOCK = threading.Lock()


def read_recent() -> dict:
    """The fresh end of the pipe: loss, lag and waveforms from the raw table."""
    out: dict = {}
    # Rows that ARRIVED in the last minute, per machine. By arrival, not by
    # sample timestamp: when ingest falls behind, readings land late, and a
    # window on `timestamp` then reports every machine as silent while all of
    # them are publishing. The timestamp bound is for the primary key: it
    # limits the scan to the last 15 minutes of readings (so data up to 15
    # minutes late still counts), and ClickHouse moves the inserted_at test
    # to PREWHERE by itself -- measured, an explicit PREWHERE gained nothing.
    # GROUPED BY MACHINE because `seq` is a per-producer counter starting at 0:
    # pooled, two producers would report -100% loss when nothing is wrong.
    per = CH.query(
        """
        SELECT machine, count() AS n, min(seq) AS lo, max(seq) AS hi,
               uniqExact(seq) AS distinct_seq, max(timestamp) AS latest,
               dateDiff('millisecond', max(inserted_at), now64(3)) / 1000 AS age_s,
               round(avg(dateDiff('millisecond', timestamp, inserted_at))) AS delay_ms
        FROM fact_engine_vibration
        WHERE inserted_at >= now64(3) - INTERVAL 60 SECOND
          AND timestamp >= now64(6) - INTERVAL 15 MINUTE
        GROUP BY machine ORDER BY machine
        """
    )
    rows = expected_total = dup_total = active = 0
    breakdown = []
    delays = []
    for r in per:
        exp = r["hi"] - r["lo"] + 1
        dup = r["n"] - r["distinct_seq"]
        rows += r["n"]
        expected_total += exp
        dup_total += dup
        is_active = r["age_s"] <= 90
        active += is_active
        delays.append(r["delay_ms"])
        breakdown.append({
            "machine": r["machine"], "rows": r["n"],
            "loss_pct": round(100.0 * (1.0 - r["distinct_seq"] / exp), 3) if exp else None,
            "duplicate_rows": dup, "latest": r["latest"],
            "seconds_since_seen": round(r["age_s"], 1), "active": is_active,
            "delay_ms": r["delay_ms"],
        })
    out["machines_recent"] = breakdown
    out["active_machines"] = active
    if HOST:
        HOST.set_active_machines(active)
    if expected_total:
        out["loss_pct"] = round(100.0 * (1.0 - (rows - dup_total) / expected_total), 3)
        out["duplicate_rows"] = dup_total
    if delays:
        # Sensor timestamp -> row stored in ClickHouse: MQTT, RabbitMQ and the
        # engine's 1 s flush, measured on every row rather than estimated.
        out["ingest_delay_ms"] = max(delays)

    # One waveform PER MACHINE, capped at the four most recently seen:
    # pooling producers would interleave two unrelated signals.
    waveforms = {}
    for entry in sorted(breakdown, key=lambda b: b["latest"], reverse=True)[:4]:
        wave = CH.query(
            """
            SELECT vib_r FROM fact_engine_vibration
            WHERE machine = {m:String}
              AND timestamp >= parseDateTime64BestEffort({t:String}, 6, 'UTC') - INTERVAL 3 SECOND
            ORDER BY timestamp DESC LIMIT 1250
            """, m=entry["machine"], t=entry["latest"])
        if wave:
            v = [round(w["vib_r"], 5) for w in reversed(wave)]
            waveforms[entry["machine"]] = {
                "machine": entry["machine"], "dt_ms": 1000.0 / SAMPLE_RATE_HZ, "v": v}
    out["waveforms"] = waveforms
    return out


def read_summary() -> dict:
    """Counts and freshness, from metadata only.

    count() and max(timestamp) are answered from each part's stored row count
    and min/max index -- measured 8 rows read instead of all 11 M. The first
    version also took uniqExact(machine) over the whole table, whose cost grew
    with retention until it used most of a core; machines now come from the
    dimension helper.
    """
    out: dict = {}
    fact = CH.query(
        "SELECT count() AS readings, max(timestamp) AS latest, "
        "dateDiff('second', max(timestamp), now64(3)) AS lag FROM fact_engine_vibration")[0]
    out["readings"] = fact["readings"]
    if fact["readings"]:
        out["latest_reading"] = fact["latest"]
        out["fact_lag_seconds"] = fact["lag"]
    out["machine_count"] = CH.query("SELECT count() AS n FROM machine_hlp FINAL")[0]["n"]
    return out


def read_throughput() -> list[dict]:
    """Rows per 10 s per machine, by the time ClickHouse stored them: the
    replacement for the original's per-batch timings."""
    return CH.query(
        """
        SELECT toStartOfInterval(inserted_at, INTERVAL 10 SECOND) AS bucket, machine,
               count() AS rows,
               round(avg(dateDiff('millisecond', timestamp, inserted_at))) AS delay_ms
        FROM fact_engine_vibration
        WHERE timestamp >= now64(6) - INTERVAL 11 MINUTE
          AND inserted_at >= now64(3) - INTERVAL 10 MINUTE
        GROUP BY bucket, machine ORDER BY bucket, machine
        """)


def read_marts() -> dict:
    """The minute-level views. Refreshed on the slow tier."""
    out: dict = {}
    stored = CH.query(
        "SELECT (SELECT count() FROM engine_minute_health FINAL) AS minutes, "
        "(SELECT count() FROM engine_minute_spectrogram FINAL) AS spectrograms, "
        "(SELECT dateDiff('second', max(minute_ts), now()) FROM engine_minute_health) "
        "AS stored_lag")[0]
    out.update(minutes=stored["minutes"], spectrograms=stored["spectrograms"])
    if stored["minutes"]:
        out["stored_lag_seconds"] = stored["stored_lag"]

    out["machines"] = CH.query(
        "SELECT machine_key, machine_name, site, asset_type, nominal_rpm, bearing_balls, "
        "is_current, valid_from, valid_to, oper FROM machine_dim "
        "ORDER BY machine_key, valid_from")

    # Per machine: a flat LIMIT 90 over two machines would silently halve
    # each one's history the moment a second producer starts.
    health = CH.query(
        """
        SELECT minute_ts, machine_name, sample_count, effective_hz, rms_r, peak_r,
               crest_r, kurtosis_r, skew_r, rms_a, axial_ratio, avg_rpm, min_rpm,
               max_rpm, shaft_hz, mode_min, mode_max, mode_changed, seq_deficit, source
        FROM v_engine_minute_health
        WHERE minute_ts >= now() - INTERVAL 3 HOUR
        ORDER BY minute_ts DESC LIMIT 90 BY machine_name
        """)
    out["health"] = list(reversed(health))
    if health:
        out["mart_lag_seconds"] = int(
            (datetime.now(timezone.utc)
             - datetime.fromisoformat(health[0]["minute_ts"].replace("Z", "+00:00"))
             ).total_seconds())
        for k in ("rms_r", "crest_r", "kurtosis_r", "avg_rpm", "axial_ratio", "mode_max"):
            out[f"latest_{k}"] = health[0].get(k)

    # The baseline reads stored minutes in one pass; only the rows returned
    # are limited.
    out["anomaly"] = list(reversed(CH.query(
        """
        SELECT minute_ts, machine_name, score, status, top_driver, z_rms, z_crest,
               z_kurtosis, z_axial, z_skew, baseline_minutes, mode_max, mode_changed
        FROM v_engine_minute_anomaly
        WHERE minute_ts >= now() - INTERVAL 3 HOUR
        ORDER BY minute_ts DESC LIMIT 90 BY machine_name
        """)))

    out["jobs"] = CH.query(
        "SELECT view, status, last_success_time, exception FROM system.view_refreshes "
        "WHERE database = {db:String} ORDER BY view", db=CH_DATABASE)
    return out


def read_spectrogram(minutes: int = 15) -> list[dict]:
    """The newest windows per machine, quantised to uint8 and base64'd IN SQL.

    The quantisation step over an 80 dB range is 0.31 dB, finer than any
    256-entry colour ramp can show. The time filter reaches the stored table,
    so this never reads spectrogram history it will not return.
    """
    rows = CH.query(
        """
        SELECT window_ts, machine_name, sample_count, n_bins, n_frames, df_hz,
               hop_seconds, frame_seconds, sample_rate_hz, source,
               base64Encode(arrayStringConcat(arrayMap(
                   x -> char(toUInt8(greatest(0, least(255,
                            (x - {lo:Float64}) / ({hi:Float64} - {lo:Float64}) * 255)))),
                   spec_db))) AS data
        FROM v_engine_minute_spectrogram
        WHERE window_ts >= toStartOfMinute(now()) - INTERVAL {m:UInt32} MINUTE
        ORDER BY window_ts DESC LIMIT {m:UInt32} BY machine_name
        """, lo=DB_MIN, hi=DB_MAX, m=minutes)
    rows.reverse()                              # oldest first, for a time axis
    for machine in {r["machine_name"] for r in rows}:
        mine = [r for r in rows if r["machine_name"] == machine]
        mine[-1]["in_progress"] = mine[-1]["source"] == "live"
    return rows


#: Keys each tier owns. A tier that did not run this pass keeps its last values.
MEDIUM_KEYS = ("spectrogram", "throughput")
SLOW_KEYS = ("minutes", "spectrograms", "stored_lag_seconds", "machines", "health",
             "mart_lag_seconds", "anomaly", "jobs", "latest_rms_r", "latest_crest_r",
             "latest_kurtosis_r", "latest_avg_rpm", "latest_axial_ratio", "latest_mode_max")


def refresh_once(rmq_password: str | None, medium: bool, slow: bool) -> None:
    """One pass. Never raises -- a failed refresh keeps the previous figures."""
    snapshot: dict = {"server_time": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    try:
        exists = CH.query(
            "SELECT count() AS n FROM system.tables WHERE database = {db:String} "
            "AND name IN ('fact_engine_vibration', 'v_engine_minute_health')",
            db=CH_DATABASE)[0]["n"] == 2
        snapshot["tables_exist"] = exists
        if exists:
            snapshot.update(read_recent())
            snapshot.update(read_summary())
            if medium:
                snapshot["spectrogram"] = read_spectrogram()
                snapshot["throughput"] = read_throughput()
            if slow:
                snapshot.update(read_marts())
            snapshot["state"] = "live"
        else:
            snapshot["state"] = "waiting for tables"
    except ClickHouseError as exc:
        snapshot["state"] = "error"
        snapshot["error"] = str(exc)
    snapshot.update(rabbitmq_queue(rmq_password))
    snapshot["refreshed_at"] = time.time()
    with CACHE_LOCK:
        for key in (() if medium else MEDIUM_KEYS) + (() if slow else SLOW_KEYS):
            if key in CACHE and key not in snapshot:
                snapshot[key] = CACHE[key]
        if snapshot["state"] == "error":
            # Stale and honest beats empty: keep the last good figures.
            for key, value in CACHE.items():
                snapshot.setdefault(key, value)
            snapshot["stale"] = True
        CACHE.clear()
        CACHE.update(snapshot)


def refresher(fast: float, medium: float, slow: float, rmq_password: str | None,
              stop: threading.Event) -> None:
    last_medium = last_slow = 0.0
    while not stop.is_set():
        started = time.monotonic()
        do_medium = started - last_medium >= medium
        do_slow = started - last_slow >= slow
        refresh_once(rmq_password, medium=do_medium, slow=do_slow)
        # A failed pass is retried on the next fast tick, not a full slow period later.
        if CACHE.get("state") == "live":
            last_medium = started if do_medium else last_medium
            last_slow = started if do_slow else last_slow
        stop.wait(max(0.5, fast - (time.monotonic() - started)))


def snapshot() -> dict:
    with CACHE_LOCK:
        out = dict(CACHE)
    if out.get("refreshed_at"):
        out["age_seconds"] = round(time.time() - out["refreshed_at"], 1)
    return out


# -- the one live query: paging back through raw readings ---------------------

def live_readings(limit: int, before: str | None, machine: str | None) -> dict:
    """A page of the raw table, keyset-paginated on timestamp, not OFFSET:
    an offset re-reads every row it skips, and at 500 Hz that is millions."""
    where, params = [], {"lim": max(1, min(limit, 2000))}
    if before:
        where.append("timestamp < parseDateTime64BestEffort({before:String}, 6, 'UTC')")
        params["before"] = before
    if machine:
        where.append("machine = {machine:String}")
        params["machine"] = machine
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    rows = CH.query(
        f"SELECT timestamp, machine, seq, vib_r, vib_a, rpm, mode "
        f"FROM fact_engine_vibration {clause} ORDER BY timestamp DESC LIMIT {{lim:UInt32}}",
        **params)
    span = CH.query("SELECT min(timestamp) AS earliest, max(timestamp) AS latest, "
                    "count() AS total FROM fact_engine_vibration")[0]
    return {"rows": rows, "earliest": span["earliest"], "latest": span["latest"],
            "total": span["total"], "state": "live"}


ROUTES = {
    "/api/status": lambda q: {
        k: v for k, v in snapshot().items()
        if k not in ("health", "spectrogram", "throughput", "machines",
                     "waveforms", "anomaly")
    },
    "/api/health": lambda q: snapshot().get("health", []),
    "/api/anomaly": lambda q: snapshot().get("anomaly", []),
    "/api/spectrogram": lambda q: {
        "db_min": DB_MIN, "db_max": DB_MAX, "encoding": "u8",
        "columns": snapshot().get("spectrogram", []),
    },
    "/api/waveform": lambda q: snapshot().get("waveforms", {}),
    "/api/machines": lambda q: snapshot().get("machines", []),
    "/api/throughput": lambda q: snapshot().get("throughput", []),
    "/api/readings": lambda q: live_readings(
        int(q.get("limit", ["50"])[0]),
        (q.get("before") or [None])[0],
        (q.get("machine") or [None])[0],
    ),
    "/api/host": lambda q: HOST.series(
        minutes=float((q.get("minutes") or ["30"])[0]),
        points=int((q.get("points") or ["240"])[0]),
    ),
    "/api/all": lambda q: snapshot(),
}


class Handler(BaseHTTPRequestHandler):
    server_version = "engine-clickhouse/1.0"

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        # Read-only endpoints on a private LAN, so any origin may read them.
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, payload) -> None:
        self._send(code, json.dumps(payload, default=str).encode(),
                   "application/json; charset=utf-8")

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            self._send(200, (HERE / "index.html").read_bytes(), "text/html; charset=utf-8")
            return
        handler = ROUTES.get(parsed.path)
        if handler is None:
            self._json(404, {"error": f"no route {parsed.path}"})
            return
        try:
            self._json(200, handler(parse_qs(parsed.query)))
        except ClickHouseError as exc:
            self._json(502, {"error": "clickhouse", "detail": str(exc)})
        except Exception as exc:  # noqa: BLE001
            self._json(500, {"error": type(exc).__name__, "detail": str(exc)[:400]})

    def log_message(self, fmt: str, *args) -> None:
        pass


def main() -> int:
    global CH, HOST
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--fast", type=float, default=5.0,
                        help="loss, waveforms, backlog (default %(default)s s)")
    parser.add_argument("--medium", type=float, default=10.0,
                        help="spectrogram, ingest rate (default %(default)s s)")
    parser.add_argument("--slow", type=float, default=30.0,
                        help="minute health, anomaly, machines (default %(default)s s)")
    args = parser.parse_args()

    password = os.environ.get("CH_PASSWORD")
    if password is None:
        password = getpass.getpass(f"ClickHouse password for {CH_USER}: ")
    CH = ClickHouse(CH_URL, CH_USER, password, CH_DATABASE)
    try:
        CH.query("SELECT 1 AS ok")
    except ClickHouseError as exc:
        raise SystemExit(f"ClickHouse: {exc}")
    rmq_password = os.environ.get("RMQ_PASSWORD")

    HOST = hostmetrics.HostMetrics(
        CH, interval=float(os.environ.get("ENG_HOST_INTERVAL", "5")),
        flush_seconds=float(os.environ.get("ENG_HOST_FLUSH", "30")))

    print(f"clickhouse : {CH_URL} database {CH_DATABASE} as {CH_USER}")
    print(f"rabbitmq   : {RMQ_API} queue {RMQ_QUEUE}"
          + ("" if rmq_password else "  (RMQ_PASSWORD unset: no backlog figures)"))
    print(f"serving    : http://{args.host}:{args.port}/")

    stop = threading.Event()
    threading.Thread(target=refresher, daemon=True, name="refresher",
                     args=(args.fast, args.medium, args.slow, rmq_password, stop)).start()
    threading.Thread(target=HOST.run, args=(stop,), daemon=True, name="host-sampler").start()

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
