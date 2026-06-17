"""
Shared Spark session factory with Kafka + Delta Lake packages configured.
"""
from pyspark import __version__ as PYSPARK_VERSION
from pyspark.sql import SparkSession

def _package_coords() -> str:
    """Return Maven package coordinates matching the active Spark major line."""
    spark_version = PYSPARK_VERSION

    if spark_version.startswith("4."):
        # Spark 4 uses Scala 2.13 artifacts.
        return ",".join([
            "io.delta:delta-spark_2.13:4.0.0",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
        ])

    # Spark 3.5.x baseline for this project.
    return ",".join([
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    ])


def get_spark(app_name: str) -> SparkSession:
    packages = _package_coords()
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
