"""MQTT payload -> one row. Same contract as the accelerometer decoder.

`decode(topic, payload) -> Mapping | None`, one message = one row, timestamps
**naive UTC** because `duckstream.windows` floors naive timestamps and trap 5
(a TIMESTAMP WITH TIME ZONE column needs `pytz`, which is not a dependency)
makes aware ones a liability.

Two deliberate differences from `duckstream_pipeline/decode.py`:

* the producer publishes **naive** ISO already, so the timezone branch is never
  taken. At 500 messages a second that is worth having;
* `seq` is carried. It is a monotonic per-run sample index and it is what turns
  "are we achieving 500 Hz" into an exact measurement rather than an estimate --
  `verify --check rate` computes loss as ``1 - count(*)/(max(seq)-min(seq)+1)``
  and duplicates as ``count(*) - count(DISTINCT seq)``. Neither needs a catalog.

`mode` is the simulator's commanded fault label, carried so the whole chain can
be checked against ground truth. **A real machine has no such column** -- it is
the thing you are trying to infer, not an input. Said plainly here because a
reader could otherwise mistake this for a diagnosis the pipeline produced.

A row that cannot be parsed is **refused**, not landed with NULLs. A NULL event
time is not an error downstream, it is an *undated row* -- a tier-one model
folds it into a NULL window and a tier-three model drops and counts it -- so
turning "the sensor sent nonsense" into that would silently reclassify a bug as
a data property.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping

REQUIRED = ("timestamp", "machine", "seq", "vib_r", "vib_a", "rpm", "mode")


def _to_utc_naive(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def decode_reading(topic: str, payload: bytes) -> Mapping[str, Any] | None:
    """One vibration reading, or ``None`` to refuse the message."""
    try:
        record = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(record, dict):
        return None
    if any(field not in record for field in REQUIRED):
        return None

    when = _to_utc_naive(record["timestamp"])
    if when is None:
        return None

    machine = record["machine"]
    mode = record["mode"]
    if not isinstance(machine, str) or not machine:
        return None
    if not isinstance(mode, str) or not mode:
        return None

    try:
        seq = int(record["seq"])
        vib_r = float(record["vib_r"])
        vib_a = float(record["vib_a"])
        rpm = float(record["rpm"])
    except (TypeError, ValueError):
        return None

    return {
        "timestamp": when,
        "machine": machine,
        "seq": seq,
        "vib_r": vib_r,
        "vib_a": vib_a,
        "rpm": rpm,
        "mode": mode,
        "topic": record.get("topic", topic),
    }
