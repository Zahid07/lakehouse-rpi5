#!/usr/bin/env bash
# NOTE: copied from the test session; paths under /tmp/claude-1000/... were a temporary
# working folder. Point S= (and the ClickHouse test port 19000) at your own setup to re-run.
# Run N real producers for SECS seconds against the bench queue and sample the pipeline.
# Usage: load_test.sh <producers> <seconds> [extra producer args...]
N=$1; SECS=$2; shift 2
PY=/home/zahid/python_scripts/lakehouse-rpi5/.venv/bin/python
P=/home/zahid/clickhouse_implementation/engine_pipeline/producer.py
S=/tmp/claude-1000/-home-zahid-clickhouse-implementation/74221c83-230e-4a0e-a619-7f68f6455031/scratchpad
TAG=$(date +%s)
pids=()
for i in $(seq 1 "$N"); do
    $PY -u "$P" --host localhost --user guest --password guest --machine "L${TAG}_$i" \
        --seconds "$SECS" --quiet "$@" > "$S/prod_$i.log" 2>&1 &
    pids+=($!)
done
for t in $(seq 10 10 $((SECS + 20))); do
    sleep 10
    q=$(curl -s -u guest:guest localhost:15672/api/queues/%2F/bench_engine | python3 -c "
import json,sys; q=json.load(sys.stdin); st=q.get('message_stats',{})
print(f\"ready {q.get('messages_ready',0):>6} unacked {q.get('messages_unacknowledged',0):>5} deliver/s {st.get('deliver_get_details',{}).get('rate',0):>6.0f}\")")
    ch=$(clickhouse-client --port 19000 -q "SELECT count(), round(countIf(inserted_at >= now64(3) - INTERVAL 10 SECOND)/10), round(dateDiff('millisecond', max(timestamp), now64(3))/1000, 1) FROM engine.fact_engine_vibration WHERE machine LIKE 'L${TAG}_%'" | awk '{printf "stored %8d  rows/s %5d  behind %6ss", $1, $2, $3}')
    cpu=$(top -bn1 -w 200 | awk '/clickhouse/ {c+=$9} /beam.smp/ {r+=$9} /python/ {p+=$9} END {printf "CPU ch %3.0f%% rmq %3.0f%% producers %3.0f%%", c, r, p}')
    echo "t=${t}s  $q | $ch | $cpu"
done
wait "${pids[@]}"
clickhouse-client --port 19000 -q "
SELECT machine, count() AS stored, max(seq) + 1 AS sent, round(100 * (1 - uniqExact(seq) / (max(seq) + 1)), 2) AS loss_pct
FROM engine.fact_engine_vibration WHERE machine LIKE 'L${TAG}_%' GROUP BY machine ORDER BY machine FORMAT PrettyCompactMonoBlock"
grep -h "emitted" "$S"/prod_*.log
