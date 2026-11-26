#!/usr/bin/env bash
# Drop everything the engine pipeline created, then recreate it with setup.sh.
# ALL DATA IN THE `engine` DATABASE IS LOST. Run on the Pi; no sudo needed.
#
#   ./reset.sh               drop ClickHouse objects AND the RabbitMQ queue
#                            (messages waiting in it are lost too)
#   ./reset.sh --keep-queue  keep the queue and its waiting messages; they are
#                            imported into the fresh tables afterwards
#
# The stft_db UDF stays installed. Passwords as for setup.sh (RMQ_PASSWORD,
# CLICKHOUSE_PASSWORD, or prompted). Stop the producers first.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RMQ_API="${RMQ_API:-http://localhost:15672/api}"
RMQ_USER="${RMQ_USER:-iot}"
RMQ_QUEUE="${RMQ_QUEUE:-clickhouse_engine}"
CH_USER="${CLICKHOUSE_USER:-default}"
CH_PORT="${CLICKHOUSE_PORT:-9000}"
KEEP_QUEUE=0
[[ "${1:-}" == "--keep-queue" ]] && KEEP_QUEUE=1

if [[ -z "${RMQ_PASSWORD+set}" ]]; then
    read -rsp "RabbitMQ password for $RMQ_USER: " RMQ_PASSWORD; echo
fi
if [[ -z "${CLICKHOUSE_PASSWORD+set}" ]]; then   # empty is a valid password
    read -rsp "ClickHouse password for $CH_USER: " CLICKHOUSE_PASSWORD; echo
fi
export RMQ_PASSWORD CLICKHOUSE_PASSWORD

ch() { clickhouse-client --port "$CH_PORT" --user "$CH_USER" "$@"; }

echo "This deletes ALL data in the ClickHouse database 'engine':"
ch --query "SELECT '  ' || name || ' (' || engine || ')' || if(total_rows IS NULL, '', ', ' || toString(total_rows) || ' rows')
            FROM system.tables WHERE database = 'engine' ORDER BY name" || true
if [[ $KEEP_QUEUE == 0 ]]; then
    waiting=$(curl -s -u "$RMQ_USER:$RMQ_PASSWORD" "$RMQ_API/queues/%2F/$RMQ_QUEUE" \
              | python3 -c "import json,sys; print(json.load(sys.stdin).get('messages', 0))" 2>/dev/null || echo 0)
    echo "and the RabbitMQ queue '$RMQ_QUEUE' with $waiting waiting message(s)."
else
    echo "The RabbitMQ queue '$RMQ_QUEUE' and its messages are kept."
fi
read -rp "Type 'reset' to continue: " answer
[[ "$answer" == "reset" ]] || { echo "Cancelled; nothing changed."; exit 1; }

echo "==> ClickHouse: dropping everything (sql/drop_all.sql)"
ch --multiquery < "$HERE/sql/drop_all.sql"

if [[ $KEEP_QUEUE == 0 ]]; then
    echo "==> RabbitMQ: deleting queue '$RMQ_QUEUE'"
    code=$(curl -s -o /dev/null -w '%{http_code}' -u "$RMQ_USER:$RMQ_PASSWORD" \
        -X DELETE "$RMQ_API/queues/%2F/$RMQ_QUEUE")
    [[ "$code" == 204 || "$code" == 404 ]] || { echo "   delete failed (HTTP $code)"; exit 1; }
fi

echo "==> recreating with setup.sh"
exec "$HERE/setup.sh"
