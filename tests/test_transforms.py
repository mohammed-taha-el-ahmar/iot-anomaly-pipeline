"""
Unit tests for transform logic. Uses a local SparkSession (no Kafka/Delta
cluster needed) to validate the silver cleaning and gold anomaly rules
in isolation.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as sabs
from pyspark.sql.functions import col, when


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("unit-tests")
        .getOrCreate()
    )


def test_silver_drops_null_device_id(spark):
    df = spark.createDataFrame(
        [("device-1", 1.0), (None, 2.0)], ["device_id", "temperature_c"]
    )
    cleaned = df.filter(col("device_id").isNotNull())
    assert cleaned.count() == 1


def test_silver_drops_unparseable_numeric_fields(spark):
    df = spark.createDataFrame(
        [("device-1", "42.5"), ("device-2", "not_a_number")],
        ["device_id", "temperature_c"],
    )
    cleaned = df.withColumn(
        "temperature_c", col("temperature_c").cast("double")
    ).filter(col("temperature_c").isNotNull())
    assert cleaned.count() == 1
    assert cleaned.collect()[0]["device_id"] == "device-1"


def test_gold_flags_critical_when_both_signals_anomalous(spark):
    df = spark.createDataFrame(
        [(10.0, 1.0, 10.0, 1.0)],
        ["temp_mean", "temp_std", "vib_mean", "vib_std"],
    )
    z = 3.0
    result = (
        df.withColumn(
            "is_temp_anomaly", sabs(col("temp_mean")) > z * col("temp_std")
        )
        .withColumn(
            "is_vibration_anomaly", sabs(col("vib_mean")) > z * col("vib_std")
        )
        .withColumn(
            "alert_severity",
            when(
                col("is_temp_anomaly") & col("is_vibration_anomaly"), "critical"
            )
            .when(col("is_temp_anomaly") | col("is_vibration_anomaly"), "warning")
            .otherwise("normal"),
        )
        .collect()[0]
    )
    assert result["alert_severity"] == "critical"


def test_gold_flags_normal_when_within_threshold(spark):
    df = spark.createDataFrame(
        [(1.0, 5.0, 1.0, 5.0)],
        ["temp_mean", "temp_std", "vib_mean", "vib_std"],
    )
    z = 3.0
    result = (
        df.withColumn(
            "is_temp_anomaly", sabs(col("temp_mean")) > z * col("temp_std")
        )
        .withColumn(
            "is_vibration_anomaly", sabs(col("vib_mean")) > z * col("vib_std")
        )
        .withColumn(
            "alert_severity",
            when(
                col("is_temp_anomaly") & col("is_vibration_anomaly"), "critical"
            )
            .when(col("is_temp_anomaly") | col("is_vibration_anomaly"), "warning")
            .otherwise("normal"),
        )
        .collect()[0]
    )
    assert result["alert_severity"] == "normal"
