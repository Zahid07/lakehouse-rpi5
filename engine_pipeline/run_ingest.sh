#!/usr/bin/env bash
# engine_pipeline: ingest
set -uo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export ENG_ROOT="${ENG_ROOT:-$HOME/engine-lake}"
export ENG_LANDING="${ENG_LANDING:-$ENG_ROOT/landing}"
export ENG_MEMORY_LIMIT="${ENG_MEMORY_LIMIT:-1200MB}"
export ENG_SAMPLE_RATE_HZ="${ENG_SAMPLE_RATE_HZ:-500}"
export ENG_FRAME_SIZE="${ENG_FRAME_SIZE:-512}"
export ENG_HOP_SIZE="${ENG_HOP_SIZE:-500}"
export ENG_MQTT_HOST="${ENG_MQTT_HOST:-localhost}"
export ENG_MQTT_PORT="${ENG_MQTT_PORT:-1883}"
export ENG_MQTT_TOPIC="${ENG_MQTT_TOPIC:-engine/vibration}"
export ENG_MACHINE="${ENG_MACHINE:-Karachi_ENG01}"
export ENG_NOMINAL_RPM="${ENG_NOMINAL_RPM:-1800}"
export ENG_CONTROL_FILE="${ENG_CONTROL_FILE:-$ENG_ROOT/producer.mode}"
export ENG_FACT_RETAIN_HOURS="${ENG_FACT_RETAIN_HOURS:-6}"
# RabbitMQ serves MQTT on the Pi and needs a login; the password comes from
# ENG_MQTT_PASSWORD or a prompt, never from this file.
export ENG_MQTT_USER="${ENG_MQTT_USER:-iot}"
# --- Fair comparison with the ClickHouse port --------------------------------
# (~/clickhouse_implementation/engine_pipeline/research/AUDIT.md). Two profiles:
#
#   designed (default):  ENG_FLUSH_SECONDS=10  ENG_PIPELINE_INTERVAL="2 seconds"
#       this pipeline as its authors tuned it: readings land every 10 s and
#       are committed on the next 2 s cycle (~6 s sent -> queryable).
#   matched:             ENG_FLUSH_SECONDS=1   ENG_PIPELINE_INTERVAL="1 second"
#       the same freshness target as ClickHouse, whose consumer writes about
#       once a second. Expect more landing directories (one per second; each
#       cycle rescans them all) and cycles that may run back to back
#       ("OVERRUN" in the log) -- that cost is part of the comparison.
#
# Both: ENG_FLUSH_ROWS=5000 (ClickHouse writes blocks of up to 5000 rows), the
# same producer settings (--batch 50, 500 Hz), one implementation running at a
# time, and the same run_id in the producers and sysmon.
# Example:  ENG_FLUSH_SECONDS=1 ENG_PIPELINE_INTERVAL="1 second" ./engine_pipeline/run_ingest.sh
export ENG_FLUSH_SECONDS="${ENG_FLUSH_SECONDS:-10}"
export ENG_FLUSH_ROWS="${ENG_FLUSH_ROWS:-5000}"
export ENG_PIPELINE_INTERVAL="${ENG_PIPELINE_INTERVAL:-2 seconds}"
mkdir -p "$ENG_LANDING"
cd "$REPO" || exit 1
exec "$REPO/.venv/bin/python" -u -m engine_pipeline.ingest "$@"
