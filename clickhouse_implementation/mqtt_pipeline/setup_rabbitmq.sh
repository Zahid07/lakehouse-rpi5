#!/usr/bin/env bash
# Replace Mosquitto with RabbitMQ (+ MQTT and management plugins) on the Pi.
# Usage: sudo ./setup_rabbitmq.sh [password-for-iot-user]
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Run with sudo: sudo $0" >&2
    exit 1
fi

RMQ_USER="iot"
RMQ_PASS="${1:-$(openssl rand -base64 12 | tr -dc 'A-Za-z0-9')}"

echo "==> Stopping and disabling Mosquitto (frees port 1883)"
systemctl disable --now mosquitto || true

echo "==> Installing RabbitMQ and the Python AMQP client for the subscriber"
apt-get update
apt-get install -y rabbitmq-server python3-pika

echo "==> Writing /etc/rabbitmq/rabbitmq.conf"
cat > /etc/rabbitmq/rabbitmq.conf <<'EOF'
# MQTT: devices publish here exactly as they did to Mosquitto
mqtt.listeners.tcp.default = 1883
mqtt.allow_anonymous       = false
mqtt.vhost                 = /
mqtt.exchange              = amq.topic

# Web UI on :15672
management.tcp.port = 15672

# 4 GB Pi shared with ClickHouse: apply publisher back-pressure at ~800 MB
vm_memory_high_watermark.relative = 0.2
EOF

echo "==> Enabling plugins"
rabbitmq-plugins enable --offline rabbitmq_mqtt rabbitmq_management

systemctl enable rabbitmq-server
systemctl restart rabbitmq-server
rabbitmqctl await_startup

echo "==> Creating user '$RMQ_USER'"
if rabbitmqctl list_users --silent | awk '{print $1}' | grep -qx "$RMQ_USER"; then
    rabbitmqctl change_password "$RMQ_USER" "$RMQ_PASS"
else
    rabbitmqctl add_user "$RMQ_USER" "$RMQ_PASS"
fi
rabbitmqctl set_user_tags "$RMQ_USER" administrator
rabbitmqctl set_permissions -p / "$RMQ_USER" ".*" ".*" ".*"

echo
echo "==> Listening ports"
ss -ltn | grep -E ":(1883|5672|15672)\b" || true
echo
echo "RabbitMQ is up."
echo "  MQTT:     192.168.0.104:1883"
echo "  AMQP:     192.168.0.104:5672"
echo "  Web UI:   http://192.168.0.104:15672"
echo "  User:     $RMQ_USER"
echo "  Password: $RMQ_PASS"
