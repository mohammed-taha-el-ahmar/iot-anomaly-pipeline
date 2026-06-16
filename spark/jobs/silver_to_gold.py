"""
Gold: Silver -> Delta, anomaly alerts.

Detects anomalies per device using a rolling z-score over a windowed
aggregate (per-device, per-minute) and flags readings that exceed the
threshold. This is the table the dashboard reads from.

Anomaly rule (placeholder, tune against backtested thresholds):
    |reading - rolling_mean| > Z_THRESHOLD * rolling_stddev
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "conf"))

from pyspark.sql.functions import abs as sabs  # noqa: E402
from pyspark.sql.functions import avg, col, stddev, when, window  # noqa: E402
from spark_session import get_spark  # noqa: E402

SILVER_PATH = os.environ.get("SILVER_PATH", "/opt/data/delta/silver/sensor_readings")
GOLD_PATH = os.environ.get("GOLD_PATH", "/opt/data/delta/gold/anomaly_alerts")
CHECKPOINT_PATH = os.environ.get(
    "GOLD_CHECKPOINT", "/opt/data/checkpoints/gold_anomaly_alerts"
)

Z_THRESHOLD = float(os.environ.get("Z_THRESHOLD", "3.0"))


def main():
    spark = get_spark("gold-anomaly-alerts")

    silver = spark.readStream.format("delta").load(SILVER_PATH)

    windowed = (
        silver.withWatermark("event_time", "5 minutes")
        .groupBy(window(col("event_time"), "1 minute"), col("device_id"))
        .agg(
            avg("temperature_c").alias("temp_mean"),
            stddev("temperature_c").alias("temp_std"),
            avg("vibration_mm_s").alias("vib_mean"),
            stddev("vibration_mm_s").alias("vib_std"),
        )
    )

    alerts = windowed.withColumn(
        "is_temp_anomaly",
        when(
            (col("temp_std").isNotNull())
            & (sabs(col("temp_mean")) > Z_THRESHOLD * col("temp_std")),
            True,
        ).otherwise(False),
    ).withColumn(
        "is_vibration_anomaly",
        when(
            (col("vib_std").isNotNull())
            & (sabs(col("vib_mean")) > Z_THRESHOLD * col("vib_std")),
            True,
        ).otherwise(False),
    ).withColumn(
        "alert_severity",
        when(col("is_temp_anomaly") & col("is_vibration_anomaly"), "critical")
        .when(col("is_temp_anomaly") | col("is_vibration_anomaly"), "warning")
        .otherwise("normal"),
    )

    query = (
        alerts.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start(GOLD_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
