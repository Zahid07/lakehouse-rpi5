import time
import json
import random
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

BROKER_IP = "192.168.0.104"    # replace with your Pi's current IP
PORT = 1883
TOPIC = "sensors/accel"

LOCATION = "Islamabad_F10"
SAMPLE_RATE_HZ = 100
SAMPLE_INTERVAL_S = 1.0 / SAMPLE_RATE_HZ

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(BROKER_IP, PORT, keepalive=60)

try:
    while True:
        NOISE_STD = 0.02  # m/s^2, realistic small jitter for a resting sensor

        payload = {
            "timestamp": datetime.now().isoformat(),
            "location": LOCATION,
            "x": round(random.gauss(0, NOISE_STD), 4),
            "y": round(random.gauss(0, NOISE_STD), 4),
            "z": round(random.gauss(9.81, NOISE_STD), 4),
        }

        client.publish(TOPIC, json.dumps(payload))
        print(f"Sent: {payload}")

        time.sleep(SAMPLE_INTERVAL_S)

except KeyboardInterrupt:
    print("Stopped.")
    client.disconnect()