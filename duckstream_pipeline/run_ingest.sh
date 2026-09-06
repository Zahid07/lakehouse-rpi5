#!/usr/bin/env bash
# MQTT -> landing. Run under systemd or a supervisor; it is meant to stay up.
set -uo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DS_ROOT="${DS_ROOT:-$HOME/duckstream-accel}"
export DS_LANDING="${DS_LANDING:-$DS_ROOT/landing}"
mkdir -p "$DS_LANDING"
cd "$REPO" || exit 1
exec "$REPO/.venv/bin/python" -m duckstream_pipeline.ingest "$@"
