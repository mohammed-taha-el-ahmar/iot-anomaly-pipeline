"""
Silver: Bronze -> Delta, parsed and cleaned.

Parses the JSON payload, casts types, drops malformed/duplicate records,
and applies watermarked dedup on (device_id, event_time) to handle
at-least-once delivery from Kafka.
"""
import os
import sys
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "conf"))

from pyspark.sql.functions import col, from_json  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spark_session import get_spark  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = os.environ.get(
    "BRONZE_PATH", str(PROJECT_ROOT / "data" / "delta" / "bronze" / "sensor_readings")
)
SILVER_PATH = os.environ.get(
    "SILVER_PATH", str(PROJECT_ROOT / "data" / "delta" / "silver" / "sensor_readings")
)
CHECKPOINT_PATH = os.environ.get(
    "SILVER_CHECKPOINT", str(PROJECT_ROOT / "data" / "checkpoints" / "silver_sensor_readings")
)

PAYLOAD_SCHEMA = StructType([
    StructField("device_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("temperature_c", StringType()),  # cast explicitly below for validation
    StructField("vibration_mm_s", StringType()),
    StructField("pressure_psi", StringType()),
    StructField("injected_anomaly", BooleanType()),
])


def main():
    spark = get_spark("silver-sensor-readings")

    bronze = spark.readStream.format("delta").load(BRONZE_PATH)

    parsed = bronze.select(
        from_json(col("raw_payload"), PAYLOAD_SCHEMA).alias("data")
    ).select("data.*")

    cleaned = (
        parsed.filter(col("device_id").isNotNull())
        .filter(col("event_time").isNotNull())
        .withColumn("temperature_c", col("temperature_c").cast("double"))
        .withColumn("vibration_mm_s", col("vibration_mm_s").cast("double"))
        .withColumn("pressure_psi", col("pressure_psi").cast("double"))
        .filter(col("temperature_c").isNotNull())
        .filter(col("vibration_mm_s").isNotNull())
        .filter(col("pressure_psi").isNotNull())
        .withWatermark("event_time", "5 minutes")
        .dropDuplicates(["device_id", "event_time"])
    )

    query = (
        cleaned.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start(SILVER_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
