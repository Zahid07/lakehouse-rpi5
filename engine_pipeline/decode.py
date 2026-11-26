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

Audit fields (added for the ClickHouse comparison, see
~/clickhouse_implementation/engine_pipeline/research/AUDIT.md): `run_id` and
`sent_at` are carried when the producer sends them -- optional, so an older
producer still decodes -- and `received_at` is stamped here, the moment this
ingest process decoded the message. A missing or unparseable `sent_at` is
NULL, never a guess.

Batched messages: a producer may pack several readings into one message, one
JSON object per line (the ClickHouse comparison's producers send 50, one
100 ms block at 500 Hz). `decode_readings` returns every valid reading in a
message; `decode_reading` is kept for single-reading messages.

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


def decode_readings(topic: str, payload: bytes) -> list[Mapping[str, Any] | None]:
    """Every reading in a message, one per line; ``None`` marks a refused line.

    Each line is decoded on its own, so one bad reading is refused without
    losing the rest of its batch.
    """
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        return [None]
    received_at = datetime.now(timezone.utc).replace(tzinfo=None)
    out = []
    for line in text.splitlines():
        if line.strip():
            try:
                out.append(_decode_record(topic, json.loads(line), received_at))
            except ValueError:
                out.append(None)
    return out or [None]


def decode_reading(topic: str, payload: bytes) -> Mapping[str, Any] | None:
    """One vibration reading, or ``None`` to refuse the message."""
    try:
        record = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    return _decode_record(topic, record, datetime.now(timezone.utc).replace(tzinfo=None))


def _decode_record(topic: str, record: Any, received_at: datetime) -> Mapping[str, Any] | None:
    """One parsed JSON object -> one row, or ``None`` to refuse it."""
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

    run_id = record.get("run_id")
    return {
        "timestamp": when,
        "machine": machine,
        "seq": seq,
        "vib_r": vib_r,
        "vib_a": vib_a,
        "rpm": rpm,
        "mode": mode,
        "topic": record.get("topic", topic),
        # audit
        "run_id": run_id if isinstance(run_id, str) else "",
        "sent_at": _to_utc_naive(record.get("sent_at")),
        "received_at": received_at,
    }
