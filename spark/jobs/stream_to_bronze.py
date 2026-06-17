"""
Bronze: Kafka -> Delta, raw and untouched.

Preserves the exact payload (plus Kafka metadata) so it can be replayed
or reprocessed if downstream logic or thresholds change. No parsing,
filtering, or cleaning happens here by design.
"""
import os
import sys
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "conf"))

from pyspark.sql.functions import col  # noqa: E402
from spark_session import get_spark  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[2]

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = "sensor-readings"
BRONZE_PATH = os.environ.get(
    "BRONZE_PATH", str(PROJECT_ROOT / "data" / "delta" / "bronze" / "sensor_readings")
)
CHECKPOINT_PATH = os.environ.get(
    "BRONZE_CHECKPOINT", str(PROJECT_ROOT / "data" / "checkpoints" / "bronze_sensor_readings")
)


def main():
    spark = get_spark("bronze-sensor-readings")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze = raw.select(
        col("key").cast("string").alias("device_key"),
        col("value").cast("string").alias("raw_payload"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start(BRONZE_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
