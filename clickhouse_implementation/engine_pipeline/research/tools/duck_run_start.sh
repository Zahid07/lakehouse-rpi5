#!/usr/bin/env bash
# Start one DuckDB-pipeline experiment run, and refuse to report success until
# it is really collecting. Run on the Pi.
#
#   research/tools/duck_run_start.sh <run_id> [designed|matched]
#
# Starts sysmon, the DuckDB ingest and the DuckDB pipeline (one of each), on a
# fresh ENG_ROOT (~/engine-lake-<run_id>), subscribed to MQTT topic
# engine/vibration_duck. Then checks: one ingest + one pipeline running, the
# ingest connected, and RabbitMQ holding the subscription for the topic. Only
# then does it print the producer command.
#
# Why the checks: an earlier run "ran" for 2 hours while its ingest sat at an
# invisible password prompt -- nothing subscribed to the topic, so RabbitMQ
# discarded every reading while still confirming them to the producers.
#
# Logs and results go to research/runs/<run_id>/. Stop with duck_run_stop.sh.
set -euo pipefail

RUN_ID="${1:?usage: $0 <run_id> [designed|matched]}"
PROFILE="${2:-designed}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EP="$(cd "$HERE/../.." && pwd)"                       # clickhouse_implementation/engine_pipeline
LAKE="${LAKE_REPO:-$HOME/python_scripts/lakehouse-rpi5}"
OUT="$EP/research/runs/$RUN_ID"

export ENG_ROOT="${ENG_ROOT:-$HOME/engine-lake-$RUN_ID}"
export ENG_LANDING="$ENG_ROOT/landing"
export ENG_MQTT_TOPIC="${ENG_MQTT_TOPIC:-engine/vibration_duck}"
export ENG_MQTT_USER="${ENG_MQTT_USER:-iot}"
case "$PROFILE" in
    designed) export ENG_FLUSH_SECONDS=10 ENG_PIPELINE_INTERVAL="2 seconds" ;;
    matched)  export ENG_FLUSH_SECONDS=1  ENG_PIPELINE_INTERVAL="1 second" ;;
    *) echo "profile must be 'designed' or 'matched'" >&2; exit 1 ;;
esac
ROUTING_KEY="${ENG_MQTT_TOPIC//\//.}"

fail() { echo "NOT STARTED: $*" >&2; exit 1; }

# -- preconditions ------------------------------------------------------------
if pgrep -f "python -u -m engine_pipeline|sysmon.py" >/dev/null; then
    pgrep -af "python -u -m engine_pipeline|sysmon.py" | grep -v pgrep >&2 || true
    fail "DuckDB processes or sysmon already running (above). Stop them first (duck_run_stop.sh)."
fi
[[ -e "$ENG_ROOT" ]] && fail "$ENG_ROOT already exists; use a new run_id (each run needs a fresh catalog)."
[[ -e "$OUT" ]] && fail "$OUT already exists; use a new run_id."
if systemctl is-active --quiet clickhouse-server; then
    echo "WARNING: clickhouse-server is running. Its idle CPU would be counted against DuckDB."
    read -rp "Stop it first (sudo systemctl stop clickhouse-server). Continue anyway? [y/N] " ok
    [[ "$ok" == [yY] ]] || exit 1
fi
if [[ -z "${ENG_MQTT_PASSWORD:-}" ]]; then
    read -rsp "MQTT password for $ENG_MQTT_USER: " ENG_MQTT_PASSWORD; echo
    export ENG_MQTT_PASSWORD
fi
[[ -n "$ENG_MQTT_PASSWORD" ]] || fail "empty MQTT password"

# -- start ------------------------------------------------------------------------
mkdir -p "$OUT"
date -u +"%Y-%m-%d %H:%M:%S" > "$OUT/started_utc"
printf 'run_id=%s\nprofile=%s\nENG_ROOT=%s\ntopic=%s\nflush_seconds=%s\npipeline_interval=%s\n' \
    "$RUN_ID" "$PROFILE" "$ENG_ROOT" "$ENG_MQTT_TOPIC" "$ENG_FLUSH_SECONDS" "$ENG_PIPELINE_INTERVAL" > "$OUT/run.env"

# Started as direct children with every stream redirected, so this script can
# exit (and nothing it started holds the caller's terminal or pipe open).
nohup python3 "$HERE/sysmon.py" --implementation duckdb --run-id "$RUN_ID" \
    --path "$ENG_ROOT" --out "$OUT/sysmon.csv" > "$OUT/sysmon.log" 2>&1 < /dev/null &
pid_sysmon=$!
nohup bash -c 'cd "$1" && exec ./engine_pipeline/run_ingest.sh' _ "$LAKE" > "$OUT/ingest.log" 2>&1 < /dev/null &
pid_ingest=$!
nohup bash -c 'cd "$1" && exec ./engine_pipeline/run.sh' _ "$LAKE" > "$OUT/pipeline.log" 2>&1 < /dev/null &
pid_pipe=$!

# -- verify -----------------------------------------------------------------------
echo "started; verifying (up to 20 s)..."
ok_bind=0
for _ in $(seq 1 20); do
    sleep 1
    if curl -s -u "$ENG_MQTT_USER:$ENG_MQTT_PASSWORD" "http://localhost:15672/api/exchanges/%2F/amq.topic/bindings/source" \
         | python3 -c "import json,sys; sys.exit(0 if any(b['routing_key']=='$ROUTING_KEY' and b['destination'].startswith('mqtt-subscription') for b in json.load(sys.stdin)) else 1)" 2>/dev/null
    then ok_bind=1; break; fi
done
n_ingest=$(pgrep -fc "python -u -m engine_pipeline.ingest" || true)
n_pipe=$(pgrep -fc "python -u -m engine_pipeline.pipeline" || true)
n_sysmon=$(pgrep -fc "sysmon.py --implementation duckdb --run-id $RUN_ID" || true)

problems=()
[[ "$n_ingest" == 1 ]] || problems+=("ingest processes: $n_ingest (expected 1) -- see $OUT/ingest.log")
[[ "$n_pipe" == 1 ]]   || problems+=("pipeline processes: $n_pipe (expected 1) -- see $OUT/pipeline.log")
[[ "$n_sysmon" == 1 ]] || problems+=("sysmon not running -- see $OUT/sysmon.log")
[[ "$ok_bind" == 1 ]]  || problems+=("RabbitMQ has no subscription for '$ROUTING_KEY' -- the ingest is not connected (wrong password?)")
if ((${#problems[@]})); then
    echo; echo "PROBLEM -- do NOT start producers:"; printf '  - %s\n' "${problems[@]}"
    echo "Ingest log:"; tail -5 "$OUT/ingest.log" | sed 's/^/  /'
    # Stop everything this attempt started, so a retry starts clean.
    kill -INT "$pid_ingest" "$pid_pipe" 2>/dev/null; kill "$pid_sysmon" 2>/dev/null
    sleep 3; kill -9 "$pid_ingest" "$pid_pipe" 2>/dev/null
    rm -rf "$ENG_ROOT" "$OUT"
    echo "Stopped and removed this attempt; fix the problem and run the same command again."
    exit 1
fi

cat <<EOF

OK -- collecting. run $RUN_ID, profile $PROFILE (flush ${ENG_FLUSH_SECONDS}s, cycle $ENG_PIPELINE_INTERVAL)
  ingest   : subscribed to $ENG_MQTT_TOPIC  ($OUT/ingest.log)
  pipeline : $ENG_ROOT  ($OUT/pipeline.log)
  sysmon   : $OUT/sysmon.csv

Now record the idle baseline (~10 min), then start producers on the laptop:
  python producer.py --host 192.168.0.104 --topic $ENG_MQTT_TOPIC --run-id $RUN_ID --machine Karachi_ENG01

Watch:  tail -f $OUT/pipeline.log      (cycles should commit rows once producers run)
Stop:   $HERE/duck_run_stop.sh $RUN_ID   (after stopping the producers)
EOF
