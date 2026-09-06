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
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from duckstream.sources.mqtt import MqttLandingWriter  # noqa: E402

from engine_pipeline.decode import decode_reading  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default=os.environ.get("ENG_MQTT_HOST", "localhost"))
    parser.add_argument("--port", type=int,
                        default=int(os.environ.get("ENG_MQTT_PORT", "1883")))
    parser.add_argument("--topic",
                        default=os.environ.get("ENG_MQTT_TOPIC", "engine/vibration"))
    parser.add_argument("--flush-rows", type=int, default=5000,
                        help="10 s at 500 Hz (default: %(default)s)")
    parser.add_argument("--flush-seconds", type=float, default=10.0,
                        help=(
                            "and land at least this often. The one that matters "
                            "for a stopped machine: nothing calls _on_message "
                            "when nothing is published, so without a time "
                            "trigger the last readings before it stopped are "
                            "held for ever -- and those are the interesting ones"
                        ))
    args = parser.parse_args(argv)

    root = os.environ.setdefault("ENG_ROOT", str(Path.home() / "engine-lake"))
    landing = Path(os.environ.setdefault("ENG_LANDING", str(Path(root) / "landing")))
    landing.mkdir(parents=True, exist_ok=True)

    writer = MqttLandingWriter(
        landing,
        args.topic,
        host=args.host,
        port=args.port,
        qos=1,                      # QoS 0 has nothing to acknowledge
        decoder=decode_reading,
        flush_rows=args.flush_rows,
        flush_seconds=args.flush_seconds,
    )

    print(f"broker  : {args.host}:{args.port}  topic {args.topic!r}")
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
