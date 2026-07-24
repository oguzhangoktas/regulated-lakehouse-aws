"""Chaos 07: the same date processed twice.

Claim under test: a rerun converges rather than accumulating, because a job replaces the
partitions its output covers instead of appending to them.

Every experiment before this one read and wrote nothing. This one has to write: the claim
is about what happens on disk, and reasoning about `overwritePartitions` is not the same
as watching it. It writes to a scratch table of its own and drops it at the end, so no
table the platform uses is touched.

Three questions, in order:

  1. does processing the same date twice double the rows
  2. does a rerun that produces fewer rows leave the surplus behind
  3. does writing one date disturb another

Usage:
  python -m chaos.exp07_rerun [reporting_date]
"""
import sys

from pyspark.sql import DataFrame, functions as F

from dataplatform.lakehouse.session import local_session
from domains.credit_risk.gold_engine_input import build

SILVER = "lakehouse.silver_credit_risk.exposure"
SCRATCH_NS = "lakehouse.chaos_scratch"
SCRATCH = f"{SCRATCH_NS}.rerun"
SLICE = 1_000


def write_partition(df: DataFrame, table: str) -> None:
    """The pattern the jobs use: create once, replace covered partitions thereafter."""
    writer = df.writeTo(table)
    if df.sparkSession.catalog.tableExists(table):
        writer.overwritePartitions()
    else:
        writer.partitionedBy(F.col("reporting_date")).create()


def counts(spark) -> str:
    table = spark.read.table(SCRATCH)
    by_date = {
        str(row["reporting_date"]): row["n"]
        for row in table.groupBy("reporting_date").agg(F.count("*").alias("n")).collect()
    }
    return f"{table.count():,} rows  {by_date}"


def main(reporting_date: str) -> None:
    spark = local_session("chaos_07_rerun")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SCRATCH_NS}")
    spark.sql(f"DROP TABLE IF EXISTS {SCRATCH}")

    book = spark.read.table(SILVER).filter(
        F.col("snapshot_date") == F.lit(reporting_date).cast("date")
    )
    day = build(book, reporting_date).limit(SLICE).cache()

    write_partition(day, SCRATCH)
    print(f"first run:        {counts(spark)}")

    write_partition(day, SCRATCH)
    print(f"same date again:  {counts(spark)}")

    write_partition(day.limit(SLICE // 2), SCRATCH)
    print(f"rerun, half size: {counts(spark)}")

    neighbour = day.withColumn(
        "reporting_date", F.date_sub(F.col("reporting_date"), 31)
    )
    write_partition(neighbour, SCRATCH)
    print(f"neighbouring date:{counts(spark)}")

    write_partition(day, SCRATCH)
    print(f"original restored:{counts(spark)}")

    spark.sql(f"DROP TABLE IF EXISTS {SCRATCH}")
    print(f"\nscratch table dropped: {not spark.catalog.tableExists(SCRATCH)}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "2018-12-31")
