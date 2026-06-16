"""
Shared Spark session factory with Kafka + Delta Lake packages configured.
"""
from pyspark.sql import SparkSession

DELTA_VERSION = "3.1.0"
SPARK_VERSION_SUFFIX = "_2.12"

PACKAGES = ",".join([
    f"io.delta:delta-spark{SPARK_VERSION_SUFFIX}:{DELTA_VERSION}",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
])


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
