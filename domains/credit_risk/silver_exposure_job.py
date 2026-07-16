"""Land one snapshot date of credit exposures in silver.

Reads a single bronze partition, conforms it, and separates records that fail
validation into a quarantine table.

Usage:
  python -m domains.credit_risk.silver_exposure_job \
      <bronze_root> <table> <quarantine_table> <snapshot_date>
"""
import sys

from pyspark.sql import DataFrame, SparkSession, functions as F

from dataplatform.lakehouse.session import local_session
from domains.credit_risk.silver_exposure import conform, validate

DATASET = "credit_exposure_snapshot"


def write_partition(df: DataFrame, table: str) -> None:
    """Replace the snapshot dates the incoming data covers, leaving the rest alone.

    The source re-lands a full copy of T-1 (ADR-002), so a rerun must converge on the
    same rows rather than append to them. overwritePartitions replaces only the
    partitions present in df, and commits the removal and the write together.
    """
    writer = df.writeTo(table)
    if df.sparkSession.catalog.tableExists(table):
        writer.overwritePartitions()
    else:
        writer.partitionedBy(F.col("snapshot_date")).create()


def run(
    spark: SparkSession,
    bronze_root: str,
    table: str,
    quarantine_table: str,
    snapshot_date: str,
) -> tuple[int, int]:
    partition = f"{bronze_root}/{DATASET}/snapshot_date={snapshot_date}"
    passed, quarantined = validate(conform(spark.read.parquet(partition)))

    # snapshot_date is encoded in the bronze path rather than the file, so reading a
    # partition directly drops it and it has to be carried back in.
    partition_date = F.lit(snapshot_date).cast("date")
    passed = passed.withColumn("snapshot_date", partition_date)
    quarantined = quarantined.withColumn("snapshot_date", partition_date)

    passed.cache()
    quarantined.cache()
    passed_count, quarantined_count = passed.count(), quarantined.count()

    write_partition(passed, table)
    if quarantined_count:
        write_partition(quarantined, quarantine_table)

    return passed_count, quarantined_count


if __name__ == "__main__":
    bronze_root, table, quarantine_table, snapshot_date = sys.argv[1:5]

    spark = local_session(f"silver_exposure_{snapshot_date}")
    for name in (table, quarantine_table):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name.rsplit('.', 1)[0]}")

    passed, quarantined = run(spark, bronze_root, table, quarantine_table, snapshot_date)
    spark.stop()

    total = passed + quarantined
    rate = quarantined / total * 100 if total else 0
    print(f"{snapshot_date}  passed={passed:,}  quarantined={quarantined:,} ({rate:.2f}%)")
