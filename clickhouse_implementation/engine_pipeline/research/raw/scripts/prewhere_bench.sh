#!/usr/bin/env bash
# NOTE: copied from the test session; paths under /tmp/claude-1000/... were a temporary
# working folder. Point S= (and the ClickHouse test port 19000) at your own setup to re-run.
# Compare read volume/time: auto-PREWHERE off, default, explicit PREWHERE.
C="clickhouse-client --port 19000"
run() {  # tag, query, [settings]
    for i in 1 2 3; do $C --log_comment "$1" -q "$2 FORMAT Null ${3:-}" ; done
}
F=engine.fact_engine_vibration

# Q1: rows that arrived in the last minute (dashboard: loss / active machines)
run q1_off     "SELECT machine, count(), uniqExact(seq) FROM $F WHERE inserted_at >= now64(3) - INTERVAL 60 SECOND AND timestamp >= now64(6) - INTERVAL 1 HOUR GROUP BY machine" "SETTINGS optimize_move_to_prewhere = 0"
run q1_auto    "SELECT machine, count(), uniqExact(seq) FROM $F WHERE inserted_at >= now64(3) - INTERVAL 60 SECOND AND timestamp >= now64(6) - INTERVAL 1 HOUR GROUP BY machine"
run q1_pre     "SELECT machine, count(), uniqExact(seq) FROM $F PREWHERE inserted_at >= now64(3) - INTERVAL 60 SECOND WHERE timestamp >= now64(6) - INTERVAL 1 HOUR GROUP BY machine"
run q1_pre15   "SELECT machine, count(), uniqExact(seq) FROM $F PREWHERE inserted_at >= now64(3) - INTERVAL 60 SECOND WHERE timestamp >= now64(6) - INTERVAL 15 MINUTE GROUP BY machine"

# Q2: ingest rate chart (10 s buckets by arrival, last 10 min)
run q2_auto    "SELECT toStartOfInterval(inserted_at, INTERVAL 10 SECOND) b, machine, count(), avg(dateDiff('millisecond', timestamp, inserted_at)) FROM $F WHERE timestamp >= now64(6) - INTERVAL 11 MINUTE AND inserted_at >= now64(3) - INTERVAL 10 MINUTE GROUP BY b, machine"
run q2_pre     "SELECT toStartOfInterval(inserted_at, INTERVAL 10 SECOND) b, machine, count(), avg(dateDiff('millisecond', timestamp, inserted_at)) FROM $F PREWHERE inserted_at >= now64(3) - INTERVAL 10 MINUTE WHERE timestamp >= now64(6) - INTERVAL 11 MINUTE GROUP BY b, machine"

# Q3: live readings page for one machine (key: machine, timestamp)
run q3_auto    "SELECT * FROM $F WHERE machine = 'Karachi_ENG03' ORDER BY timestamp DESC LIMIT 50"
run q3_pre     "SELECT * FROM $F PREWHERE machine = 'Karachi_ENG03' ORDER BY timestamp DESC LIMIT 50"

# Q4: a non-key filter: readings above a vibration threshold, last hour
run q4_off     "SELECT count(), max(vib_a), avg(rpm) FROM $F WHERE vib_r > 0.02 AND timestamp >= now64(6) - INTERVAL 1 HOUR" "SETTINGS optimize_move_to_prewhere = 0"
run q4_auto    "SELECT count(), max(vib_a), avg(rpm) FROM $F WHERE vib_r > 0.02 AND timestamp >= now64(6) - INTERVAL 1 HOUR"
run q4_pre     "SELECT count(), max(vib_a), avg(rpm) FROM $F PREWHERE vib_r > 0.02 WHERE timestamp >= now64(6) - INTERVAL 1 HOUR"

# Q5: summary: count + latest (old version had uniqExact(machine))
run q5_old     "SELECT count(), uniqExact(machine), max(timestamp) FROM $F"
run q5_new     "SELECT count(), max(timestamp) FROM $F"

$C -q "SYSTEM FLUSH LOGS"
$C -q "
SELECT log_comment AS test, round(avg(query_duration_ms)) AS ms, round(avg(read_rows)) AS rows_read,
       formatReadableSize(avg(read_bytes)) AS bytes_read,
       round(avg(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1000) AS cpu_ms
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment LIKE 'q%' AND event_time > now() - INTERVAL 10 MINUTE
GROUP BY log_comment ORDER BY log_comment FORMAT PrettyCompactMonoBlock"
