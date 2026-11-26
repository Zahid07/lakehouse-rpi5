#!/usr/bin/env bash
# Stop a DuckDB-pipeline run started with duck_run_start.sh, then export it.
#
#   research/tools/duck_run_stop.sh <run_id> [--no-wait] [--no-export]
#
# Stop the producers on the laptop FIRST. This waits 2 minutes (so the last
# chunks land and commit), stops the ingest and pipeline cleanly, stops sysmon,
# and runs export_duck_run.py into research/runs/<run_id>/.
set -uo pipefail

RUN_ID="${1:?usage: $0 <run_id> [--no-wait] [--no-export]}"
shift
WAIT=120; EXPORT=1
for a in "$@"; do
    case "$a" in --no-wait) WAIT=0 ;; --no-export) EXPORT=0 ;; esac
done
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EP="$(cd "$HERE/../.." && pwd)"
LAKE="${LAKE_REPO:-$HOME/python_scripts/lakehouse-rpi5}"
OUT="$EP/research/runs/$RUN_ID"
ENG_ROOT="$(grep '^ENG_ROOT=' "$OUT/run.env" 2>/dev/null | cut -d= -f2-)"
ENG_ROOT="${ENG_ROOT:-$HOME/engine-lake-$RUN_ID}"

remote=$(curl -s -u guest:guest http://localhost:15672/api/connections 2>/dev/null | python3 -c "
import json,sys
try: print(sum(1 for c in json.load(sys.stdin) if 'MQTT' in c.get('protocol','') and c.get('peer_host') not in ('127.0.0.1','::1')))
except Exception: print('?')")
[[ "$remote" == 0 || "$remote" == "?" ]] || echo "WARNING: $remote producer(s) still connected -- stop them on the laptop."

if (( WAIT > 0 )); then
    echo "waiting ${WAIT}s for the last chunks to land and commit..."; sleep "$WAIT"
fi
date -u +"%Y-%m-%d %H:%M:%S" > "$OUT/stopped_utc" 2>/dev/null || true

echo "stopping ingest and pipeline..."
pkill -INT -f "python -u -m engine_pipeline" || true
for _ in $(seq 1 30); do pgrep -f "python -u -m engine_pipeline" >/dev/null || break; sleep 1; done
if pgrep -f "python -u -m engine_pipeline" >/dev/null; then
    echo "  still running after 30 s; forcing"; pkill -9 -f "python -u -m engine_pipeline" || true
fi
pkill -f "sysmon.py --implementation duckdb --run-id $RUN_ID" || true
sleep 1
pgrep -af "python -u -m engine_pipeline|sysmon.py" | grep -v pgrep && echo "WARNING: processes above still running" || echo "all stopped"

if (( EXPORT )); then
    start=$(cat "$OUT/started_utc")
    end=$(date -u -d "+1 minute" +"%Y-%m-%d %H:%M:%S")
    echo "exporting $start -> $end UTC into $OUT"
    "$LAKE/.venv/bin/python" "$HERE/export_duck_run.py" --root "$ENG_ROOT" --out "$OUT" --from "$start" --to "$end" \
        && { echo; echo "per producer:"; column -s, -t < "$OUT/summary_per_producer.csv"; }
fi
