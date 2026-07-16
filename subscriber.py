import json
import os
import threading
import queue
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import paho.mqtt.client as mqtt

BROKER_IP = "localhost"
PORT = 1883
TOPIC = "sensors/accel"

BASE_OUTPUT_DIR = Path(__file__).resolve().parent / "pipeline" / "accel_data"
FLUSH_INTERVAL_S = 0.5 * 60  # write to disk every 4 minutes

write_queue = queue.Queue()
buffer = []
window_start_time = None


def writer_thread():
    """Runs in the background, writing parquet files without blocking MQTT."""
    while True:
        records, start_time = write_queue.get()
        if records is None:  # sentinel to stop
            break
        if not records:
            continue

        date_str = start_time.strftime("%Y%m%d")
        time_str = start_time.strftime("%H%M%S")
        folder = BASE_OUTPUT_DIR / date_str / time_str
        os.makedirs(folder, exist_ok=True)

        data_path = folder / "data.parquet"
        tmp_path = folder / "data.parquet.tmp"

        df = pd.DataFrame(records)
        df.to_parquet(tmp_path, index=False)

        # Atomic rename first, so the file is never seen half-written...
        os.replace(tmp_path, data_path)

        # ...THEN drop the _READY marker, only once the parquet file
        # is fully in place. The downstream pipeline should only fetch
        # a folder once _READY exists in it.
        (folder / "_READY").touch()

        print(f"Saved {data_path} ({len(records)} records), marked ready")


def on_connect(client, userdata, flags, reason_code, properties):
    print("Connected with result code", reason_code)
    client.subscribe(TOPIC)


def on_message(client, userdata, msg):
    global buffer, window_start_time

    data = json.loads(msg.payload.decode())
    now = datetime.now(timezone.utc)

    if window_start_time is None:
        window_start_time = now

    if (now - window_start_time).total_seconds() >= FLUSH_INTERVAL_S:
        write_queue.put((buffer, window_start_time))  # hand off, don't block
        buffer = []
        window_start_time = now
    # print(data)

    buffer.append(data)


threading.Thread(target=writer_thread, daemon=True).start()

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER_IP, PORT, keepalive=60)

try:
    client.loop_forever()
except KeyboardInterrupt:
    if buffer:
        write_queue.put((buffer, window_start_time))
    write_queue.put((None, None))


# import json
# import os
# import threading
# import queue
# from pathlib import Path
# from datetime import datetime, timezone
# import pandas as pd
# import paho.mqtt.client as mqtt

# BROKER_IP = "localhost"
# PORT = 1883
# TOPIC = "sensors/accel"

# BASE_OUTPUT_DIR = Path(__file__).resolve().parent / "pipeline" / "accel_data"
# FLUSH_INTERVAL_S = 4 * 60  # write to disk every 4 minutes

# write_queue = queue.Queue()
# buffer = []
# window_start_time = None

# def on_connect(client, userdata, flags, rc):
#     print("Connected with result code", rc)
#     client.subscribe(TOPIC)

# def on_message(client, userdata, msg):
#     global buffer, window_start_time

#     data = json.loads(msg.payload.decode())
#     now = datetime.now(timezone.utc)

#     if window_start_time is None:
#         window_start_time = now

#     # if (now - window_start_time).total_seconds() >= FLUSH_INTERVAL_S:
#     #     write_queue.put((buffer, window_start_time))  # hand off, don't block
#     #     buffer = []
#     #     window_start_time = now

#     # buffer.append(data)

# client = mqtt.Client()
# client.on_connect = on_connect
# client.on_message = on_message

# client.connect(BROKER_IP, PORT, keepalive=60)
# client.loop_forever()