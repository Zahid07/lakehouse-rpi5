"""Measure how many readings/s RabbitMQ's MQTT plugin accepts on this Pi.
# NOTE: copied from the test session; paths under /tmp/claude-1000/... were a temporary
# working folder. Point S= (and the ClickHouse test port 19000) at your own setup to re-run.

N publisher processes, QoS 1, as fast as the broker acknowledges, for a fixed
time. Counts broker-acknowledged messages (on_publish), not attempts.
Usage: mqtt_bench.py <publishers> <readings_per_message> <seconds>
"""
import json
import multiprocessing as mp
import sys
import threading
import time

import paho.mqtt.client as mqtt

TOPIC = "engine/bench"


def publisher(idx, per_msg, seconds, out):
    acked = [0]
    ok = threading.Event()
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"bench-{idx}")
    c.username_pw_set("guest", "guest")
    c.on_connect = lambda *a: ok.set()
    c.on_publish = lambda *a: acked.__setitem__(0, acked[0] + 1)
    c.max_inflight_messages_set(500)
    c.connect("localhost", 1883)
    c.loop_start()
    ok.wait(5)
    row = {"timestamp": "2026-09-27 00:00:00.000000", "machine": f"B{idx}", "seq": 0,
           "vib_r": 0.012345, "vib_a": 0.004321, "rpm": 1799.87, "mode": "healthy"}
    payload = "\n".join(json.dumps({**row, "seq": i}) for i in range(per_msg))
    end = time.monotonic() + seconds
    sent = 0
    while time.monotonic() < end:
        # keep at most ~1000 un-acked messages in flight / queued locally
        if sent - acked[0] < 1000:
            c.publish(TOPIC, payload, qos=1)
            sent += 1
        else:
            time.sleep(0.001)
    time.sleep(1)
    c.loop_stop()
    c.disconnect()
    out.put(acked[0])


if __name__ == "__main__":
    n, per_msg, seconds = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
    q = mp.Queue()
    procs = [mp.Process(target=publisher, args=(i, per_msg, seconds, q)) for i in range(n)]
    t0 = time.monotonic()
    for p in procs:
        p.start()
    total = sum(q.get() for _ in procs)
    for p in procs:
        p.join()
    print(f"{n} publishers x {per_msg} readings/msg: {total/seconds:,.0f} msg/s acked "
          f"= {total*per_msg/seconds:,.0f} readings/s")
