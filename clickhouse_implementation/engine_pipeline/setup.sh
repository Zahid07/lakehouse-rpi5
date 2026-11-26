#!/usr/bin/env bash
# Install the engine pipeline into ClickHouse. Run on the Pi; no sudo needed.
#
#   1. RabbitMQ: create the durable queue 'clickhouse_engine', bound to
#      amq.topic "engine.vibration" (what MQTT topic engine/vibration becomes).
#   2. ClickHouse: apply sql/01..07 in order, with the RabbitMQ login filled in.
#
# Safe to re-run: tables use IF NOT EXISTS and views CREATE OR REPLACE. To
# change a refreshable view (the *_mv objects), DROP VIEW it first.
#
# Passwords come from RMQ_PASSWORD / CLICKHOUSE_PASSWORD, or are prompted for.
set -euo pipefail
# Bash 5.2+: otherwise "&" in a replacement means "the matched text", which
# would corrupt a password containing "&".
shopt -u patsub_replacement 2>/dev/null || true

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RMQ_API="${RMQ_API:-http://localhost:15672/api}"
RMQ_USER="${RMQ_USER:-iot}"
RMQ_QUEUE="${RMQ_QUEUE:-clickhouse_engine}"
# MQTT topic engine/vibration arrives on amq.topic as "engine.vibration".
RMQ_ROUTING_KEY="${RMQ_ROUTING_KEY:-engine.vibration}"
CH_USER="${CLICKHOUSE_USER:-default}"
CH_PORT="${CLICKHOUSE_PORT:-9000}"

if [[ -z "${RMQ_PASSWORD+set}" ]]; then
    read -rsp "RabbitMQ password for $RMQ_USER: " RMQ_PASSWORD; echo
fi
if [[ -z "${CLICKHOUSE_PASSWORD+set}" ]]; then   # empty is a valid password
    read -rsp "ClickHouse password for $CH_USER: " CLICKHOUSE_PASSWORD; echo
fi
export CLICKHOUSE_PASSWORD   # read by clickhouse-client; keeps it out of `ps`

echo "==> RabbitMQ: queue '$RMQ_QUEUE' bound to amq.topic '$RMQ_ROUTING_KEY'"
code=$(curl -s -o /dev/null -w '%{http_code}' -u "$RMQ_USER:$RMQ_PASSWORD" \
    -X PUT -H 'content-type: application/json' -d '{"durable":true}' \
    "$RMQ_API/queues/%2F/$RMQ_QUEUE")
[[ "$code" == 201 || "$code" == 204 ]] || { echo "   creating queue failed (HTTP $code)"; exit 1; }
code=$(curl -s -o /dev/null -w '%{http_code}' -u "$RMQ_USER:$RMQ_PASSWORD" \
    -X POST -H 'content-type: application/json' -d "{\"routing_key\":\"$RMQ_ROUTING_KEY\"}" \
    "$RMQ_API/bindings/%2F/e/amq.topic/q/$RMQ_QUEUE")
[[ "$code" == 201 ]] || { echo "   binding failed (HTTP $code)"; exit 1; }

echo "==> ClickHouse: checking the stft_db UDF is installed"
have_udf=$(clickhouse-client --port "$CH_PORT" --user "$CH_USER" \
    --query "SELECT count() FROM system.functions WHERE name = 'stft_db'")
if [[ "$have_udf" != 1 ]]; then
    echo "   stft_db is missing: run  sudo ./udf/install_udf.sh  first."
    exit 1
fi

echo "==> ClickHouse: applying sql/"
# Single quotes and backslashes escaped for the SQL string literal.
escaped=${RMQ_PASSWORD//\\/\\\\}
escaped=${escaped//\'/\\\'}
for file in "$HERE"/sql/0*.sql; do
    sql=$(<"$file")
    sql=${sql//\{\{RMQ_USER\}\}/$RMQ_USER}
    sql=${sql//\{\{RMQ_PASSWORD\}\}/$escaped}
    sql=${sql//\{\{RMQ_QUEUE\}\}/$RMQ_QUEUE}
    printf '   %-24s' "$(basename "$file")"
    clickhouse-client --port "$CH_PORT" --user "$CH_USER" --multiquery <<<"$sql"
    echo ok
done

echo "==> ClickHouse: re-attaching the RabbitMQ consumer"
# If the queue was ever deleted, RabbitMQ cancelled ClickHouse's consumer, and
# the RabbitMQ engine does not re-subscribe while its connection stays open:
# it sits connected to nothing while messages pile up. Detach/attach makes it
# reconnect and subscribe afresh. No data is touched.
clickhouse-client --port "$CH_PORT" --user "$CH_USER" --query "DETACH TABLE engine.vibration_queue"
clickhouse-client --port "$CH_PORT" --user "$CH_USER" --query "ATTACH TABLE engine.vibration_queue"
echo "   done"

echo
echo "Done. The refreshable views run on their own; check them with:"
echo "  SELECT view, status, last_success_time, exception FROM system.view_refreshes WHERE database = 'engine';"
