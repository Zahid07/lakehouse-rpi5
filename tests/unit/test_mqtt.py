"""The MQTT adapter, tested against a fake client rather than a broker.

`duckstream.landing` carries the durability and is tested on its own. What is
left here is the adapter, and what it has to get right is **when it
acknowledges** — which is the whole difference between at-least-once and
at-most-once, and which nothing observes until a process dies with a full
buffer.

`paho-mqtt` is an optional dependency and is deliberately not installed for
these tests. That is the point of the seam: the class is exercised through the
same callbacks paho would call, so the logic is covered on every machine, and
the only untested line is `connect`'s `import paho`. A test that needed a broker
would run nowhere and prove it on no machine at all.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from duckstream.errors import ConfigError, DuckstreamError
from duckstream.landing import MARKER
from duckstream.sources.mqtt import MqttLandingWriter, decode_json


class FakeMessage:
    """What paho hands to `on_message`."""

    def __init__(self, topic: str, payload: bytes, mid: int = 1, qos: int = 1):
        self.topic = topic
        self.payload = payload
        self.mid = mid
        self.qos = qos


class FakeClient:
    """A paho client that records acknowledgements instead of making them."""

    def __init__(self):
        self.acked: list[tuple[int, int]] = []
        self.subscribed: list[tuple[str, int]] = []
        self.disconnected = False

    def ack(self, mid, qos):
        self.acked.append((mid, qos))

    def subscribe(self, topic, qos=0):
        self.subscribed.append((topic, qos))

    def disconnect(self):
        self.disconnected = True


def make_writer(tmp_path: Path, **kwargs) -> tuple[MqttLandingWriter, FakeClient]:
    settings = dict(host="localhost", flush_rows=3, flush_seconds=None)
    settings.update(kwargs)
    writer = MqttLandingWriter(tmp_path / "landing", "sensors/#", **settings)
    client = FakeClient()
    writer._client = client
    return writer, client


def message(index: int, mid: int | None = None) -> FakeMessage:
    payload = json.dumps({"sensor_id": "s1", "value": float(index)}).encode()
    return FakeMessage("sensors/accel", payload, mid=index if mid is None else mid)


def deliver(writer: MqttLandingWriter, message: FakeMessage) -> None:
    writer._on_message(writer._client, None, message)


# ==========================================================================
# Acknowledgement -- the at-least-once guarantee
# ==========================================================================


def test_nothing_is_acknowledged_before_it_is_durable(tmp_path):
    """The single most important assertion in phase 5.

    paho acks QoS-1 messages on arrival by default. That is at-*most*-once for
    anything still buffered: the broker is told it was handled, the process
    dies, and it is gone with nothing to report it. Two of three messages here
    are buffered and neither may be acknowledged.
    """
    writer, client = make_writer(tmp_path, flush_rows=3)
    deliver(writer, message(0))
    deliver(writer, message(1))

    assert writer.writer.pending == 2
    assert client.acked == [], "a buffered message was acknowledged"


def test_acknowledgement_happens_only_after_the_marker_exists(tmp_path):
    writer, client = make_writer(tmp_path, flush_rows=3)
    for index in range(3):
        deliver(writer, message(index))

    assert writer.writer.pending == 0, "the row trigger did not fire"
    assert client.acked == [(0, 1), (1, 1), (2, 1)]

    landed = [p for p in (tmp_path / "landing").iterdir() if p.is_dir()]
    assert len(landed) == 1
    assert (landed[0] / MARKER).is_file(), "acked before the marker existed"


def test_exactly_the_landed_messages_are_acknowledged(tmp_path):
    """Not more, not fewer. Acking a message still buffered loses it."""
    writer, client = make_writer(tmp_path, flush_rows=2)
    for index in range(3):
        deliver(writer, message(index))

    # 0 and 1 landed and were acked; 2 is still buffered and must not be.
    assert client.acked == [(0, 1), (1, 1)]
    assert writer.writer.pending == 1

    writer.flush()
    assert client.acked == [(0, 1), (1, 1), (2, 1)]


def test_closing_lands_and_acknowledges_the_buffer(tmp_path):
    """A clean shutdown must not lose what it is holding."""
    writer, client = make_writer(tmp_path, flush_rows=1000)
    for index in range(4):
        deliver(writer, message(index))
    assert client.acked == []

    writer.close()
    assert client.acked == [(index, 1) for index in range(4)]
    assert client.disconnected
    assert writer.landed == 4


def test_a_failed_landing_acknowledges_nothing(tmp_path):
    """A full disk must hold the messages, not drop them.

    Nothing is acked, so the broker still owns every one of them and will
    re-deliver after a restart. That is at-least-once doing its job.
    """
    writer, client = make_writer(tmp_path, flush_rows=2)

    def explode(temp, records):
        raise OSError("no space left on device")

    writer.writer._write = explode
    deliver(writer, message(0))
    with pytest.raises(OSError):
        deliver(writer, message(1))

    assert client.acked == [], "a message was acked despite the write failing"
    assert writer.writer.pending == 2, "records were dropped by a failed write"


# ==========================================================================
# Decoding
# ==========================================================================


def test_the_topic_becomes_a_column(tmp_path):
    """A wildcard subscription makes the topic the only distinguishing field."""
    record = decode_json("sensors/accel", json.dumps({"value": 1.0}).encode())
    assert record == {"value": 1.0, "topic": "sensors/accel"}


def test_a_payload_that_names_its_own_topic_keeps_it(tmp_path):
    """The message's own field wins: it is the one the user wrote."""
    payload = json.dumps({"value": 1.0, "topic": "mine"}).encode()
    assert decode_json("sensors/accel", payload)["topic"] == "mine"


@pytest.mark.parametrize(
    "payload",
    [b"not json at all", b"[1, 2, 3]", b"42", b'"a string"', b"\xff\xfe"],
)
def test_anything_that_is_not_a_json_object_is_refused(payload):
    """A row is built from keys, so a list or a scalar has no shape.

    Guessing a column name for a bare value would be inventing data.
    """
    assert decode_json("sensors/accel", payload) is None


def test_an_undecodable_message_is_counted_and_acknowledged(tmp_path):
    """Counted, because silence here looks exactly like an idle topic.

    Acked, because it will not decode on redelivery either and leaving it
    unacked makes the broker replay it for ever.
    """
    writer, client = make_writer(tmp_path, flush_rows=100)
    deliver(writer, FakeMessage("sensors/accel", b"not json", mid=7))

    assert writer.undecodable == 1
    assert writer.writer.pending == 0, "an undecodable message was buffered"
    assert client.acked == [(7, 1)]


def test_a_custom_decoder_is_used(tmp_path):
    def csv_ish(topic, payload):
        sensor, value = payload.decode().split(",")
        return {"sensor_id": sensor, "value": float(value), "topic": topic}

    writer, client = make_writer(tmp_path, flush_rows=1, decoder=csv_ish)
    deliver(writer, FakeMessage("sensors/accel", b"s9,3.5", mid=1))
    assert writer.landed == 1


# ==========================================================================
# Subscription and configuration
# ==========================================================================


def test_every_connect_resubscribes(tmp_path):
    """A reconnect with no resubscribe is a client that receives nothing.

    It comes back connected and healthy-looking, which is why this is a test
    rather than a comment.
    """
    writer, client = make_writer(tmp_path)
    writer._on_connect(client, None)
    writer._on_connect(client, None)
    assert client.subscribed == [("sensors/#", 1), ("sensors/#", 1)]


def test_several_topics_are_all_subscribed(tmp_path):
    writer = MqttLandingWriter(
        tmp_path / "landing", ["a/#", "b/#"], flush_rows=1, qos=2
    )
    client = FakeClient()
    writer._on_connect(client, None)
    assert client.subscribed == [("a/#", 2), ("b/#", 2)]


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"topics": []}, "at least one topic"),
        ({"qos": 3}, "qos"),
        ({"decoder": "not callable"}, "callable"),
    ],
)
def test_incoherent_settings_are_refused_at_construction(tmp_path, kwargs, match):
    settings = dict(topics="sensors/#", flush_rows=1)
    settings.update(kwargs)
    topics = settings.pop("topics")
    with pytest.raises(ConfigError, match=match):
        MqttLandingWriter(tmp_path / "landing", topics, **settings)


def test_the_missing_dependency_names_itself(tmp_path, monkeypatch):
    """`paho-mqtt` is optional, so its absence must say so and say what to do.

    An ImportError from deep inside a lazy import is the sort of thing that gets
    reported as "duckstream is broken".
    """
    import builtins

    real_import = builtins.__import__

    def refuse(name, *args, **kwargs):
        if name.startswith("paho"):
            raise ImportError("No module named 'paho'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", refuse)
    writer = MqttLandingWriter(tmp_path / "landing", "a/#", flush_rows=1)
    with pytest.raises(DuckstreamError) as caught:
        writer.connect()
    message = str(caught.value)
    assert "paho-mqtt" in message
    assert "duckstream[mqtt]" in message


# ==========================================================================
# The time trigger
# ==========================================================================


def test_tick_lands_a_quiet_topic(tmp_path):
    """A topic that goes quiet must not hold its last readings for ever.

    Nothing calls `_on_message` when nothing is published, so without `tick`
    the buffer would sit there until the next message -- which on a sensor that
    has stopped is never, and those last readings are exactly the ones somebody
    wants.
    """
    writer, client = make_writer(
        tmp_path, flush_rows=None, flush_seconds=60.0
    )
    deliver(writer, message(0))
    assert writer.tick() is None, "flushed before the trigger fired"

    # Age the buffer rather than sleeping for a minute.
    writer.writer._opened_at -= dt.timedelta(seconds=61)
    batch = writer.tick()

    assert batch is not None and batch.rows == 1
    assert client.acked == [(0, 1)]
