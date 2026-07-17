"""Spark session construction for local runs.

Glue supplies its own session and carries the Iceberg runtime, so this is used by
local development and tests only. The catalog is named the same in both, so jobs
reference tables identically wherever they run.
"""
from pyspark.sql import SparkSession

# Matches the Iceberg version in the Glue 5.1 runtime (ADR-007). Iceberg ships as a
# Spark runtime jar rather than a Python package, so it is resolved at session start
# instead of being pinned in requirements.
ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"


def local_session(
    app_name: str,
    warehouse: str = "data/warehouse",
    shuffle_partitions: int = 8,
) -> SparkSession:
    """Build a local session with a filesystem-backed Iceberg catalog.

    On Glue the `lakehouse` catalog is backed by the Glue Data Catalog; locally it is
    a directory. Only the catalog configuration differs between the two.

    The Spark default of 200 shuffle partitions is sized for a cluster. On a local
    sample it produces hundreds of near-empty tasks and scheduling dominates the work.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", ICEBERG_RUNTIME)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
