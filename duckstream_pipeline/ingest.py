"""MQTT -> landing tree. The replacement for `subscriber.py`.

Same broker, same topic, same output shape. One behavioural difference, and it
is the whole reason to switch.

**`subscriber.py` is at-most-once for anything still buffered.** `paho`
acknowledges a QoS-1 message the moment it arrives unless `manual_ack` is set,
and that file does not set it. So the broker is told a reading was handled while
it is still sitting in a Python list; if the process dies, the buffer goes with
it and the broker has nothing left to redeliver. Nothing reports the loss --
from the broker's side the delivery succeeded.

`duckstream.sources.mqtt.MqttLandingWriter` inverts that: it sets `manual_ack`,
and releases an acknowledgement token per record **only after the completion
marker is on disk**. A crash before the marker leaves an unmarked directory the
file source never reads, and the broker redelivers everything unacked.

The price is stated rather than hidden: duplicates. At-least-once means the same
reading can land in two files, and duckstream does not de-duplicate -- the two
files genuinely differ and nothing marks one as a repeat. **Exactly-once is over
files, not over readings.** The marts key on (window, location) and merge, so a
redelivered reading folds into the window it belonged to; the fact table is an
unwindowed append and would show it twice, which is the honest outcome for a
row-level log.

Run it under systemd or a supervisor::

    python -m duckstream_pipeline.ingest
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from duckstream.sources.mqtt import MqttLandingWriter  # noqa: E402

from duckstream_pipeline.decode import decode_reading  # noqa: E402

# Matching `subscriber.py`: a local, anonymous broker.
BROKER_HOST = os.environ.get("DS_MQTT_HOST", "localhost")
BROKER_PORT = int(os.environ.get("DS_MQTT_PORT", "1883"))
TOPIC = os.environ.get("DS_MQTT_TOPIC", "sensors/accel")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default=BROKER_HOST)
    parser.add_argument("--port", type=int, default=BROKER_PORT)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument(
        "--flush-rows", type=int, default=30_000,
        help=(
            "land once this many readings are buffered. 30,000 at 100 Hz is "
            "about five minutes of one sensor"
        ),
    )
    parser.add_argument(
        "--flush-seconds", type=float, default=30.0,
        help=(
            "and land at least this often. This is the one that matters for a "
            "quiet topic: nothing calls _on_message when no messages arrive, "
            "so without a time trigger a sensor that stops holds its last "
            "readings for ever -- and those are exactly the ones somebody wants"
        ),
    )
    args = parser.parse_args(argv)

    root = os.environ.setdefault("DS_ROOT", str(Path.home() / "duckstream-accel"))
    landing = Path(os.environ.setdefault("DS_LANDING", str(Path(root) / "landing")))
    landing.mkdir(parents=True, exist_ok=True)

    writer = MqttLandingWriter(
        landing,
        args.topic,
        host=args.host,
        port=args.port,
        qos=1,                       # QoS 0 has nothing to acknowledge
        decoder=decode_reading,      # parses the timestamp; see decode.py
        flush_rows=args.flush_rows,
        flush_seconds=args.flush_seconds,
    )

    print(f"broker  : {args.host}:{args.port}  topic {args.topic!r}")
    print(f"landing : {landing}")
    print(f"flush   : {args.flush_rows} rows or {args.flush_seconds}s, "
          f"whichever first")
    print("acknowledging only once the marker is on disk (trap 30)")

    try:
        # Connects, subscribes on every connect -- a reconnect after a network
        # blip gets a fresh session unless the broker kept one, and a client
        # that subscribed only the first time comes back connected, healthy and
        # receiving nothing -- and polls `tick()` so a quiet topic still lands.
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
