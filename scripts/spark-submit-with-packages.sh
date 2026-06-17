#!/usr/bin/env bash
# Wrapper around spark-submit that injects Delta + Kafka connector packages
# matching the locally installed Spark major version.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <spark-submit args...>" >&2
  echo "Example: $0 spark/jobs/stream_to_bronze.py" >&2
  exit 1
fi

SPARK_VERSION_RAW="$(spark-submit --version 2>&1 | grep -Eo 'version [0-9]+\.[0-9]+\.[0-9]+' | head -n1 | awk '{print $2}')"
SPARK_MAJOR="${SPARK_VERSION_RAW%%.*}"

if [[ -z "${SPARK_VERSION_RAW}" ]]; then
  echo "[spark-submit-with-packages] Could not detect Spark version from spark-submit --version" >&2
  exit 1
fi

if [[ "${SPARK_MAJOR}" == "4" ]]; then
  PACKAGES="io.delta:delta-spark_2.13:4.0.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
else
  PACKAGES="io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
fi

# Allow manual override when needed.
PACKAGES="${SPARK_EXTRA_PACKAGES:-$PACKAGES}"

echo "[spark-submit-with-packages] Spark ${SPARK_VERSION_RAW} -> --packages ${PACKAGES}"
exec spark-submit --packages "$PACKAGES" "$@"
