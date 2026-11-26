"""MQTT -> landing tree, at 500 readings a second.

Same durability contract as `duckstream_pipeline/ingest.py`, and the same reason
for it: `paho` acknowledges a QoS-1 message on arrival unless `manual_ack` is
set, which is at-*most*-once for anything still buffered. `MqttLandingWriter`
releases a token per record only after the completion marker is on disk.

Two settings differ from the accelerometer pipeline, both because of the rate.

**`--flush-seconds 10` rather than 30.** `duckstream/landing.py` creates one
flat directory per flush and `FileSource._scan` walks all of them on *every*
trigger including idle ones. At a 5-second flush that is 17,280 directories a
day; 10 seconds halves it and still gives 200-400 KB parquet chunks, which is a
much better DuckLake write size than either extreme.

**`--flush-rows 5000`** is 10 seconds at 500 Hz, so the two triggers coincide
rather than fighting.

If 500 msg/s turns out not to be achievable, `verify --check rate` says so
exactly -- from `seq`, not from a guess -- and the README lists the escalation.

**Audit columns** (for the ClickHouse comparison): every landed chunk stamps
its rows with `landed_at` (when the chunk was written) and `chunk` (its
directory name). `chunk` is what links a reading to the duckstream batch that
committed it -- `duckstream.consumed_files.relpath` starts with it -- which is
how the audit views derive each reading's `stored_at`. See
~/clickhouse_implementation/engine_pipeline/research/AUDIT.md.

**Batched messages**: producers may send many readings per message, one per
line (50 = one 100 ms block at 500 Hz); every reading lands as its own row.

**Broker login**: the Pi's MQTT broker is RabbitMQ, which refuses anonymous
clients. The login comes from --user/--password, ENG_MQTT_USER /
ENG_MQTT_PASSWORD, or a prompt.
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from duckstream.landing import LandingWriter  # noqa: E402
from duckstream.sources.mqtt import MqttLandingWriter  # noqa: E402

from engine_pipeline.decode import decode_readings  # noqa: E402


class BatchMqttLandingWriter(MqttLandingWriter):
    """MqttLandingWriter for messages that carry several readings (one per line).

    The base class assumes one message = one row. Here every valid reading of
    a message is buffered together, and the message's token (what acknowledges
    it to the broker) rides on the last of them -- so the message is acked only
    once the landed chunk containing all of its readings is on disk, which is
    the base class's at-least-once guarantee, unchanged. Refused lines are
    counted in `undecodable`; a message with no valid line is acked, as the base
    class does, because it will not decode on redelivery either.
    """

    def _on_message(self, client, userdata, message):
        records = self.decoder(message.topic, message.payload)
        valid = [r for r in records if r is not None]
        self.undecodable += len(records) - len(valid)
        if not valid:
            self._ack(message)
            return
        with self._lock:
            self.writer.extend(valid, tokens=[None] * (len(valid) - 1) + [message])
            if self.writer.due():
                self._flush_locked()


class AuditLandingWriter(LandingWriter):
    """LandingWriter that stamps each chunk's rows with landed_at and chunk.

    Only `_write` changes, and it is called with the temp file inside the
    chunk's directory, so the directory name is known exactly. The audit
    columns get explicit types: `from_pylist` would give an all-NULL `sent_at`
    (an old producer) the type null, and one such file among timestamped ones
    breaks reading the chunks together.
    """

    AUDIT_TYPES = {"run_id": "string", "chunk": "string", "sent_at": "timestamp",
                   "received_at": "timestamp", "landed_at": "timestamp"}

    def _write(self, temp, records):
        import pyarrow as pa
        import pyarrow.parquet as pq

        landed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        chunk = Path(temp).parent.name
        columns: dict[str, None] = {}
        for record in records:
            for key in record:
                columns.setdefault(key, None)
        rows = [{**{k: r.get(k) for k in columns}, "landed_at": landed_at, "chunk": chunk}
                for r in records]
        table = pa.Table.from_pylist(rows)
        for name, kind in self.AUDIT_TYPES.items():
            if name in table.column_names:
                typ = pa.string() if kind == "string" else pa.timestamp("us")
                i = table.column_names.index(name)
                table = table.set_column(i, name, table.column(i).cast(typ))
        pq.write_table(table, temp)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default=os.environ.get("ENG_MQTT_HOST", "localhost"))
    parser.add_argument("--port", type=int,
                        default=int(os.environ.get("ENG_MQTT_PORT", "1883")))
    parser.add_argument("--topic",
                        default=os.environ.get("ENG_MQTT_TOPIC", "engine/vibration"))
    parser.add_argument("--user", default=os.environ.get("ENG_MQTT_USER", "iot"))
    parser.add_argument("--password", default=os.environ.get("ENG_MQTT_PASSWORD"),
                        help="or ENG_MQTT_PASSWORD; prompted for if neither is given")
    parser.add_argument("--flush-rows", type=int,
                        default=int(os.environ.get("ENG_FLUSH_ROWS", "5000")),
                        help="10 s at 500 Hz (default: %(default)s)")
    parser.add_argument("--flush-seconds", type=float,
                        default=float(os.environ.get("ENG_FLUSH_SECONDS", "10")),
                        help=(
                            "and land at least this often. The one that matters "
                            "for a stopped machine: nothing calls _on_message "
                            "when nothing is published, so without a time "
                            "trigger the last readings before it stopped are "
                            "held for ever -- and those are the interesting ones"
                        ))
    args = parser.parse_args(argv)
    if args.user and args.password is None:
        if not sys.stdin.isatty():
            # Started in the background (nohup ... &): a password prompt would
            # wait forever, unseen, while every reading published meanwhile is
            # discarded by the broker for want of a subscriber. Fail loudly.
            sys.exit("ingest: no MQTT password -- set ENG_MQTT_PASSWORD "
                     "(running without a terminal, so it cannot be prompted for)")
        args.password = getpass.getpass(f"MQTT password for {args.user}: ")

    root = os.environ.setdefault("ENG_ROOT", str(Path.home() / "engine-lake"))
    landing = Path(os.environ.setdefault("ENG_LANDING", str(Path(root) / "landing")))
    landing.mkdir(parents=True, exist_ok=True)

    writer = BatchMqttLandingWriter(
        landing,
        args.topic,
        host=args.host,
        port=args.port,
        qos=1,                      # QoS 0 has nothing to acknowledge
        decoder=decode_readings,
        flush_rows=args.flush_rows,
        flush_seconds=args.flush_seconds,
        username=args.user or None,
        password=args.password,
    )
    # Same writer, same configuration, plus the audit stamp: switching the
    # class keeps every setting MqttLandingWriter gave it.
    writer.writer.__class__ = AuditLandingWriter

    print(f"broker  : {args.host}:{args.port}  topic {args.topic!r}  user {args.user!r}")
    print(f"landing : {landing}")
    print(f"flush   : {args.flush_rows} rows or {args.flush_seconds}s, "
          f"whichever first")
    print("acknowledging only once the marker is on disk (trap 30)")

    try:
        writer.run_forever()
    except KeyboardInterrupt:
        print("\nstopping; landing whatever is buffered")
    finally:
        landed = writer.close()
        if landed is not None:
            print(f"landed {landed.rows} final reading(s) -> {landed.directory}")
        print(f"totals: landed={writer.landed} undecodable={writer.undecodable}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
