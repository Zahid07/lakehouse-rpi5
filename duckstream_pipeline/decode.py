"""The MQTT payload decoder: one JSON object per message, with a real timestamp.

``duckstream.sources.mqtt.decode_json`` is the default and would work here, but
it would land ``timestamp`` as a **string**, because that is what the sensor
publishes and ``pa.Table.from_pylist`` infers from the value it is given. The
existing tree shows what that costs: two files under ``accel_data/20260716/``
carry ``timestamp VARCHAR`` while the other 346 carry ``timestamp TIMESTAMP``,
and no single ``read_parquet`` can span both.

So the timestamp is parsed here, at the only point where the raw text is still
available and a failure is still cheap. A row that cannot be parsed is
**refused** rather than landed with a NULL time: a NULL event time is not an
error downstream, it is an *undated row*, which a tier-one model folds into a
NULL window and a tier-three model drops and counts (``CONTEXT.md`` section 4's
ratified rule). Turning "the sensor sent something malformed" into that is a
silent reclassification of a bug as a data property.

Refusing returns ``None``, which the landing writer counts as ``undecodable``
and acknowledges -- it will not decode on redelivery either, and leaving it
unacked would make the broker replay it for ever.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping

#: Columns the pipeline requires of every reading. A message missing any of
#: them is refused rather than landed short: `pa.Table.from_pylist` takes the
#: union of keys across the batch, so a short record lands as NULLs and looks
#: exactly like a sensor that reported nothing.
REQUIRED = ("timestamp", "location", "x", "y", "z")


def _to_utc_naive(value: Any) -> datetime | None:
    """An ISO-8601 instant as a naive UTC datetime, or ``None``.

    Naive-UTC rather than timezone-aware on purpose. ``CONTEXT.md`` trap 5 is
    the neighbouring hazard -- a ``TIMESTAMP WITH TIME ZONE`` column coming
    back from DuckDB needs ``pytz``, which is not a dependency -- and the
    windowing arithmetic in ``duckstream.windows`` floors a naive timestamp.
    The sensor publishes ``+00:00``, so converting and dropping the offset
    loses nothing and keeps every downstream column one type.
    """
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
    """One accelerometer reading, or ``None`` to refuse the message."""
    try:
        record = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(record, dict):
        # A JSON array or a bare number is not a row, and inventing a column
        # name for it would be inventing data.
        return None
    if any(field not in record for field in REQUIRED):
        return None

    when = _to_utc_naive(record["timestamp"])
    if when is None:
        return None

    try:
        x = float(record["x"])
        y = float(record["y"])
        z = float(record["z"])
    except (TypeError, ValueError):
        return None

    location = record["location"]
    if not isinstance(location, str) or not location:
        return None

    return {
        "timestamp": when,
        "location": location,
        "x": x,
        "y": y,
        "z": z,
        # The topic is kept because a wildcard subscription is the norm and it
        # is then the only thing distinguishing two readings -- but only when
        # the payload has not claimed the name itself.
        "topic": record.get("topic", topic),
    }
