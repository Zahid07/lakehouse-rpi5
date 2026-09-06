"""MQTT, modelled the only way it can be: a landing writer, not a source.

``CONTEXT.md`` section 4 settled this before any of it was built, and the
registry has been refusing ``type: mqtt`` as a *source* with the reason ever
since phase 1. It is worth restating, because the shape of this module is
entirely determined by it:

    MQTT has no replayable offset. Once a message is acked it is gone from the
    broker. Exactly-once is therefore impossible **directly** — there is nothing
    to resume from and nothing to replay.

So this does not implement :class:`~duckstream.protocols.Source`, and it never
will. It subscribes, hands messages to a
:class:`~duckstream.landing.LandingWriter`, and acknowledges them **after** they
are durable. A :class:`~duckstream.sources.files.FileSource` pointed at the same
directory is then the replayable source, and exactly-once holds from there:

    broker --> MqttLandingWriter --> landing/ --> FileSource --> engine
              at least once                       exactly once

The two halves are deliberately separate processes. The writer is a daemon that
must stay connected; the engine is a drain-and-exit job under cron
(``CONTEXT.md`` 1.8: ~235 ms of process start a tick, and 1.6 — a DuckDB *file*
held open locks everyone else out). Fusing them would put a long-lived network
loop inside the process that holds the catalog, which is the one thing the
trigger model exists to avoid.

Acknowledgement is the whole design
-----------------------------------

``paho`` acknowledges QoS-1 messages automatically as they arrive. That is
at-**most**-once for anything still buffered: the broker is told the message was
handled, the process dies, and it is gone with nothing to report it. This class
therefore sets ``manual_ack`` and acks only the tokens
:meth:`~duckstream.landing.LandingWriter.flush` hands back, which it hands back
only once the completion marker is on disk.

The cost is real and worth stating: messages are re-delivered after a crash, so
the landing tree can contain the same reading twice, in two different files. That
is what at-least-once means. duckstream does not de-duplicate it, and the file
source cannot -- the two files are genuinely different files. If a duplicate
matters, the model needs a key it can merge on rather than an ``append`` sink.

``paho-mqtt`` is an optional dependency
---------------------------------------

Imported lazily, inside :meth:`MqttLandingWriter.connect`, so the package
imports on a machine that has never heard of MQTT — which is most of them, and
all of them during the tests of everything else. Install it with
``pip install duckstream[mqtt]``.

Everything durability depends on lives in :mod:`duckstream.landing` and has no
MQTT in it at all, so it is tested without a broker. What is left here is the
adapter, and it is deliberately thin enough to read in one sitting.
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any, Callable, Mapping

from duckstream.errors import ConfigError, DuckstreamError
from duckstream.landing import MARKER, LandedBatch, LandingWriter

__all__ = ["MqttLandingWriter", "decode_json"]


def decode_json(topic: str, payload: bytes) -> Mapping[str, Any] | None:
    """The default decoder: a JSON object per message, plus its topic.

    Returns ``None`` for anything that is not a JSON **object**, which is the
    one shape a row can be built from. A JSON array or a bare number is not a
    row and guessing a column name for it would be inventing data.

    ``topic`` is added as a column because a subscription is usually a wildcard
    and the topic is then the only thing distinguishing two readings. It is
    added only when the payload has not already claimed the name -- the
    message's own field wins, because it is the one the user wrote.
    """
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, dict):
        return None
    if "topic" in decoded:
        return decoded
    return {**decoded, "topic": topic}


class MqttLandingWriter:
    """Subscribe to a broker and land the messages durably, at least once.

    Args:
        path: Landing tree root, the directory a ``FileSource`` will read.
        topics: One topic filter, or several. Wildcards are the broker's.
        host / port / keepalive: Ordinary MQTT connection settings.
        qos: 0, 1 or 2. **1 or 2 is the whole point** -- at QoS 0 the broker
            never re-delivers, so a message lost before it is landed is simply
            lost and no acknowledgement discipline can help. Allowed, because a
            user may genuinely not care, and warned about in
            :meth:`connect` rather than refused.
        decoder: ``(topic, payload) -> mapping | None``. Returning ``None``
            drops the message as undecodable; it is counted, never silent.
        username / password: Optional credentials.
        client_id: Optional stable client id. A **stable** one is what lets a
            broker keep a session and re-deliver what was never acked, so it is
            worth setting in production and is why this is exposed at all.
        flush_rows / flush_seconds / marker / filename / base_dir: passed
            straight to :class:`~duckstream.landing.LandingWriter`.

    Usage::

        writer = MqttLandingWriter("landing/", "sensors/#", host="localhost")
        writer.run_forever()          # Ctrl-C lands the buffer and exits

    Then point a model at the same directory, and cron the engine:

        source: {type: file, path: "landing/", marker: _READY}
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        topics: str | list[str],
        *,
        host: str = "localhost",
        port: int = 1883,
        keepalive: int = 60,
        qos: int = 1,
        decoder: Callable[[str, bytes], Mapping[str, Any] | None] = decode_json,
        username: str | None = None,
        password: str | None = None,
        client_id: str | None = None,
        flush_rows: int | None = 10_000,
        flush_seconds: float | None = 60.0,
        marker: str | None = MARKER,
        filename: str = "data.parquet",
        base_dir: str | os.PathLike[str] | None = None,
    ) -> None:
        self.topics = [topics] if isinstance(topics, str) else list(topics)
        if not self.topics:
            raise ConfigError(
                "an MQTT landing writer needs at least one topic; with none it "
                "connects, subscribes to nothing and lands nothing for ever"
            )
        if qos not in (0, 1, 2):
            raise ConfigError(f"qos must be 0, 1 or 2, got {qos!r}")
        if not callable(decoder):
            raise ConfigError(
                f"decoder must be callable, got {type(decoder).__name__}"
            )
        self.host = host
        self.port = port
        self.keepalive = keepalive
        self.qos = qos
        self.decoder = decoder
        self.username = username
        self.password = password
        self.client_id = client_id

        self.writer = LandingWriter(
            path,
            marker=marker,
            flush_rows=flush_rows,
            flush_seconds=flush_seconds,
            filename=filename,
            base_dir=base_dir,
        )
        #: Messages the decoder refused. Counted rather than logged and
        #: forgotten: a decoder that silently drops every message looks
        #: identical to a topic nobody is publishing to.
        self.undecodable = 0
        #: Messages landed and acknowledged.
        self.landed = 0
        self._client: Any = None
        # paho's network thread and the caller's tick loop both land. See
        # `_flush_locked` for why the lock is here and not in LandingWriter.
        self._lock = threading.RLock()

    # -- the broker --------------------------------------------------------

    def connect(self) -> Any:
        """Build the ``paho`` client, wire the callbacks, and connect.

        ``paho-mqtt`` is imported here rather than at module scope so that
        importing duckstream, or any part of it, never requires MQTT to be
        installed.
        """
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:  # pragma: no cover - depends on the env
            raise DuckstreamError(
                "the MQTT landing writer needs `paho-mqtt`, which duckstream "
                "does not install by default -- it is optional so a deployment "
                "with no MQTT in it carries no MQTT dependency. Install it with "
                "`pip install duckstream[mqtt]` or `pip install paho-mqtt`."
            ) from exc

        client = mqtt.Client(client_id=self.client_id or "")
        # Without this, paho acknowledges a QoS-1 message the moment it arrives
        # -- before it is durable -- and a crash loses the buffer with the
        # broker believing it was delivered. See the module docstring.
        client.manual_ack = True
        if self.username is not None:
            client.username_pw_set(self.username, self.password)
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.connect(self.host, self.port, self.keepalive)
        self._client = client
        return client

    def _on_connect(self, client: Any, userdata: Any, *args: Any) -> None:
        """Subscribe on **every** connect, not once at startup.

        A reconnect after a network blip gives a fresh session unless the broker
        kept one, and a client that subscribed only the first time comes back
        connected, healthy and receiving nothing at all.
        """
        for topic in self.topics:
            client.subscribe(topic, qos=self.qos)

    def _on_message(self, client: Any, userdata: Any, message: Any) -> None:
        record = self.decoder(message.topic, message.payload)
        if record is None:
            self.undecodable += 1
            # Acked anyway: it is not going to decode on redelivery either, and
            # leaving it unacked makes the broker replay it for ever. The count
            # above is the record that it happened.
            self._ack(message)
            return
        with self._lock:
            self.writer.add(record, token=message)
            if self.writer.due():
                self._flush_locked()

    # -- landing -----------------------------------------------------------

    def flush(self) -> LandedBatch | None:
        """Land the buffer, then acknowledge exactly what was landed.

        The order is the guarantee. :meth:`LandingWriter.flush` returns only
        after the completion marker exists, so every token it hands back names a
        message that is now on disk. Acking before this point would be
        at-most-once wearing at-least-once's clothes.
        """
        with self._lock:
            return self._flush_locked()

    def _flush_locked(self) -> LandedBatch | None:
        """:meth:`flush`, with :attr:`_lock` already held.

        The lock lives here rather than inside
        :class:`~duckstream.landing.LandingWriter` because *this* is the class
        with two threads in it: ``paho``'s network thread delivers messages
        while the caller's thread drives the time trigger, and both land. The
        writer itself stays single-threaded by design -- putting a lock in it
        would advertise a promise nothing needs and that its callers would then
        come to rely on.
        """
        batch = self.writer.flush()
        if batch is None:
            return None
        for token in batch.tokens:
            self._ack(token)
        self.landed += batch.rows
        return batch

    def _ack(self, message: Any) -> None:
        client = self._client
        if client is None or message is None:
            return
        ack = getattr(client, "ack", None)
        if callable(ack):
            ack(message.mid, message.qos)

    # -- running -----------------------------------------------------------

    def run_forever(self, poll_seconds: float | None = None) -> None:
        """Connect and pump until interrupted, landing the buffer on the way out.

        The network runs on ``paho``'s own background thread (``loop_start``),
        which reconnects by itself -- what a daemon on a flaky network needs --
        while this thread does nothing but call :meth:`tick`.

        **That loop is not idle waiting; it is the time trigger.** A quiet topic
        produces no messages, so nothing calls ``_on_message`` and nothing would
        otherwise notice that ``flush_seconds`` had passed. On a sensor that
        stops reporting, "flush on the next message" can mean never -- and the
        last readings before it went quiet are exactly the ones somebody wants.

        The ``finally`` is what stops a clean shutdown being a data loss:
        whatever is buffered is landed and acknowledged before the process
        leaves.
        """
        import time

        if poll_seconds is None:
            poll_seconds = min(self.writer.flush_seconds or 1.0, 1.0)
        client = self.connect()
        client.loop_start()
        try:
            while True:
                time.sleep(poll_seconds)
                self.tick()
        except KeyboardInterrupt:  # pragma: no cover - operator action
            pass
        finally:
            try:
                client.loop_stop()
            except Exception:  # pragma: no cover - best effort on shutdown
                pass
            self.close()

    def tick(self) -> LandedBatch | None:
        """Flush if the time trigger has fired. Safe to call as often as you like.

        Public because a caller embedding this writer in its own event loop has
        to drive the time trigger themselves; :meth:`run_forever` is only the
        loop duckstream provides. The sensible period is ``flush_seconds``, and
        calling it more often costs a lock and a comparison.
        """
        with self._lock:
            if self.writer.due():
                return self._flush_locked()
        return None

    def close(self) -> LandedBatch | None:
        """Land and acknowledge whatever is buffered, then disconnect."""
        batch = self.flush()
        client, self._client = self._client, None
        if client is not None:
            try:
                client.disconnect()
            except Exception:  # pragma: no cover - best effort on shutdown
                pass
        return batch

    def __enter__(self) -> "MqttLandingWriter":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        return (
            f"MqttLandingWriter({self.writer.path!r}, {self.topics!r}, "
            f"host={self.host!r}, port={self.port!r}, qos={self.qos!r})"
        )
