#!/usr/bin/env bash
# Creates the sensor-readings topic with 6 partitions (one bucket per
# group of devices) so per-device ordering is preserved while allowing
# parallel consumption across partitions.
set -euo pipefail

BOOTSTRAP="${1:-localhost:29092}"
TOPIC="sensor-readings"
PARTITIONS=6
REPLICATION=1

docker exec iot-kafka kafka-topics \
  --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION" \
  --config retention.ms=604800000

echo "[create-topics] topic '$TOPIC' ready with $PARTITIONS partitions."
