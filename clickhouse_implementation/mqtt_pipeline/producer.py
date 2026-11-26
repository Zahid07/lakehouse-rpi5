#!/usr/bin/env python3
"""Simulated accelerometer that publishes readings over MQTT to RabbitMQ on the Pi.

Runs on the laptop.  Setup:  pip install paho-mqtt
Usage:  python producer.py [--password <pw>] [--host 192.168.0.104] [--location lab] [--interval 1]
"""
import argparse
import getpass
import json
import os
import random
import sys
import threading
import time
from datetime import datetime

import paho.mqtt.client as mqtt

NOISE_STD = 0.02  # m/s^2, realistic small jitter for a resting sensor


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default="192.168.0.104")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--user", default="iot")
    p.add_argument("--password", default=os.environ.get("RMQ_PASSWORD"),
                   help="or set RMQ_PASSWORD; prompted for if neither is given")
    p.add_argument("--location", default="lab")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between readings")
    args = p.parse_args()
    if not args.password:
        args.password = getpass.getpass(f"Password for {args.user}: ")
    return args


def main():
    args = parse_args()
    # RabbitMQ maps MQTT topic "sensors/lab/accel" to routing key "sensors.lab.accel" on amq.topic.
    topic = f"sensors/{args.location}/accel"

    connected = threading.Event()
    fatal = threading.Event()
    acked = 0

    def on_connect(client, userdata, flags, rc, props):
        if rc.is_failure:
            print(f"Connection refused by broker: {rc}")
            if rc in ("Bad user name or password", "Not authorized"):
                fatal.set()  # retrying won't help
            return
        print(f"Connected to {args.host}:{args.port}")
        connected.set()

    def on_disconnect(client, userdata, flags, rc, props):
        connected.clear()
        if not fatal.is_set():
            print(f"Disconnected ({rc}), retrying...")

    def on_publish(client, userdata, mid, rc, props):
        nonlocal acked
        acked += 1  # QoS 1: broker confirmed it received the message

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"producer-{args.location}")
    client.username_pw_set(args.user, args.password)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish
    try:
        client.connect(args.host, args.port, keepalive=30)
    except OSError as e:
        sys.exit(f"Cannot reach {args.host}:{args.port}: {e}")
    client.loop_start()  # background thread handles network I/O and reconnects

    print(f"Publishing to '{topic}' every {args.interval}s. Ctrl+C to stop.")
    sent = 0
    try:
        while True:
            if fatal.is_set():
                sys.exit("Login rejected: check --user / --password.")
            if not connected.wait(timeout=5):
                print("Not connected yet, waiting...")
                continue
            payload = {
                "timestamp": datetime.now().isoformat(),
                "location": args.location,
                "x": round(random.gauss(0, NOISE_STD), 4),
                "y": round(random.gauss(0, NOISE_STD), 4),
                "z": round(random.gauss(9.81, NOISE_STD), 4),
            }
            client.publish(topic, json.dumps(payload), qos=1)
            sent += 1
            print(f"[{sent} sent, {acked} acked] {payload}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()
        print(f"Stopped: {sent} sent, {acked} acknowledged by the broker.")


if __name__ == "__main__":
    main()
