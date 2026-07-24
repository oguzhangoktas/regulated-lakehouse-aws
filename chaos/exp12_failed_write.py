"""Chaos 12: a write that fails partway.

A job dies mid-write — a worker is lost, a transform raises on one row, a session is
killed. The question is what the table holds afterwards: everything, nothing, or some of
it.

Iceberg commits by swapping metadata once the data is written, so a write that never
reaches its commit leaves the table exactly as it was. That is worth demonstrating rather
than assuming, because the corollary is less obvious: the files the failed attempt wrote
are still on disk, referenced by nothing.

Runs against a scratch table it drops at the end.

Usage:
  python -m chaos.exp12_failed_write
"""
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, functions as F

from dataplatform.lakehouse.session import local_session

SCRATCH_NS = "lakehouse.chaos_scratch"
SCRATCH = f"{SCRATCH_NS}.atomic"
WAREHOUSE = Path("data/warehouse/chaos_scratch/atomic")
ROWS = 2_000


def state(spark) -> str:
    table = spark.read.table(SCRATCH)
    referenced = spark.read.table(f"{SCRATCH}.all_data_files").count()
    snapshots = spark.read.table(f"{SCRATCH}.snapshots").count()
    on_disk = len(list(WAREHOUSE.rglob("*.parquet")))
    return (f"{table.count():>6,} rows  {snapshots} snapshot(s)  "
            f"{referenced} file(s) referenced  {on_disk} on disk")


def frame(spark, n: int, offset: int = 0) -> DataFrame:
    return spark.range(n).select(
        (F.col("id") + offset).alias("id"),
        F.lit("2018-12-31").cast("date").alias("reporting_date"),
    )


def main() -> None:
    spark = local_session("chaos_12_failed_write")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SCRATCH_NS}")
    spark.sql(f"DROP TABLE IF EXISTS {SCRATCH}")
    shutil.rmtree(WAREHOUSE, ignore_errors=True)

    frame(spark, ROWS).writeTo(SCRATCH).partitionedBy(F.col("reporting_date")).create()
    print(f"committed:      {state(spark)}")

    # A transform that raises on one row, so the write fails after some of its files
    # have already been written.
    @F.udf("long")
    def refuse(value):
        if value == ROWS + 1_500:
            raise ValueError("injected failure")
        return value

    incoming = frame(spark, ROWS, offset=ROWS).withColumn("id", refuse(F.col("id")))

    try:
        incoming.writeTo(SCRATCH).overwritePartitions()
        print("the write succeeded — the injected failure did not fire")
    except Exception as exc:
        print(f"write failed:   {type(exc).__name__}")

    print(f"after failure:  {state(spark)}")

    ids = spark.read.table(SCRATCH).agg(F.min("id"), F.max("id")).first()
    print(f"\nthe table still holds its original rows: id {ids[0]} to {ids[1]}")

    spark.sql(f"DROP TABLE IF EXISTS {SCRATCH}")
    shutil.rmtree(WAREHOUSE, ignore_errors=True)
    spark.stop()


if __name__ == "__main__":
    main()
