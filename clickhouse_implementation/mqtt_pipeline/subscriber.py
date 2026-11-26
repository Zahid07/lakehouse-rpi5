#!/usr/bin/env python3
"""Reads the producer's MQTT messages from RabbitMQ over AMQP.

The MQTT plugin publishes every MQTT message to the 'amq.topic' exchange, with
'/' in the topic turned into '.' (sensors/lab/accel -> sensors.lab.accel).
This script binds a durable queue to 'sensors.#', so messages published while
it is stopped wait in the queue instead of being lost.

Runs on the Pi (python3-pika is installed by setup_rabbitmq.sh) or anywhere
with:  pip install pika
Usage:  python3 subscriber.py [--password <pw>] [--host localhost] [--queue sensor_readings]
"""
import argparse
import getpass
import json
import os

import pika

EXCHANGE = "amq.topic"
BINDING_KEY = "sensors.#"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5672)
    p.add_argument("--user", default="iot")
    p.add_argument("--password", default=os.environ.get("RMQ_PASSWORD"),
                   help="or set RMQ_PASSWORD; prompted for if neither is given")
    p.add_argument("--queue", default="sensor_readings")
    args = p.parse_args()
    if not args.password:
        args.password = getpass.getpass(f"Password for {args.user}: ")
    return args


def main():
    args = parse_args()
    params = pika.ConnectionParameters(
        host=args.host,
        port=args.port,
        credentials=pika.PlainCredentials(args.user, args.password),
        heartbeat=30,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=args.queue, durable=True)
    channel.queue_bind(queue=args.queue, exchange=EXCHANGE, routing_key=BINDING_KEY)
    channel.basic_qos(prefetch_count=100)

    def on_message(ch, method, properties, body):
        try:
            data = json.loads(body)
            print(f"{method.routing_key:<24} {data['timestamp']}  {data['location']:<8} "
                  f"x={data['x']:+.4f}  y={data['y']:+.4f}  z={data['z']:+.4f}")
        except (ValueError, KeyError):
            print(f"{method.routing_key:<24} (unparsed) {body!r}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=args.queue, on_message_callback=on_message)
    print(f"Waiting on queue '{args.queue}' (bound to {EXCHANGE} '{BINDING_KEY}'). Ctrl+C to stop.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
