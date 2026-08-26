#!/usr/bin/env bash
# The pipeline. Drains the landing tree and maintains the star schema, on a
# schedule, releasing the catalog between cycles.
set -uo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DS_ROOT="${DS_ROOT:-$HOME/duckstream-accel}"
export DS_LANDING="${DS_LANDING:-$DS_ROOT/landing}"
export DS_MEMORY_LIMIT="${DS_MEMORY_LIMIT:-1200MB}"
mkdir -p "$DS_LANDING"
cd "$REPO" || exit 1
exec "$REPO/.venv/bin/python" -m duckstream_pipeline.pipeline "$@"
