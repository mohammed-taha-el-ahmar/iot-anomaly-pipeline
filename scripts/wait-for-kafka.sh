#!/usr/bin/env bash
# Polls Kafka until the broker responds, then exits 0.
set -euo pipefail

BOOTSTRAP="${1:-localhost:29092}"
MAX_RETRIES=30
RETRY_DELAY=2

for i in $(seq 1 "$MAX_RETRIES"); do
  if docker exec iot-kafka kafka-topics --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    echo "[wait-for-kafka] Kafka is ready."
    exit 0
  fi
  echo "[wait-for-kafka] waiting for Kafka... ($i/$MAX_RETRIES)"
  sleep "$RETRY_DELAY"
done

echo "[wait-for-kafka] Kafka did not become ready in time." >&2
exit 1
