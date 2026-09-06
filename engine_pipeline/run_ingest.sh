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
mkdir -p "$ENG_LANDING"
cd "$REPO" || exit 1
exec "$REPO/.venv/bin/python" -u -m engine_pipeline.ingest "$@"
