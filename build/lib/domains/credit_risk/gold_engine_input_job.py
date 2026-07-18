"""Publish the credit-risk engine input for a reporting date.

Reads the month-end position from silver, applies scope, derives the declared fields
and enforces the contract before anything is published.

Quarantine is written before the dataset assertions run: when a dataset is rejected,
the rows explaining why are what the investigation starts from.

Usage:
  python -m domains.credit_risk.gold_engine_input_job \
      <silver_table> <gold_table> <quarantine_table> <reporting_date>
"""
import sys

from pyspark.sql import DataFrame, SparkSession, functions as F

from dataplatform.contracts.contract import Contract
from domains.credit_risk.gold_engine_input import build

CONTRACT = "credit_risk_exposure_input"


def write_partition(df: DataFrame, table: str) -> None:
    writer = df.writeTo(table)
    if df.sparkSession.catalog.tableExists(table):
        writer.overwritePartitions()
    else:
        writer.partitionedBy(F.col("reporting_date")).create()


def run(
    spark: SparkSession,
    silver_table: str,
    gold_table: str,
    quarantine_table: str,
    reporting_date: str,
) -> tuple[int, int]:
    book = spark.read.table(silver_table).filter(
        F.col("snapshot_date") == F.lit(reporting_date).cast("date")
    )

    contract = Contract.named(CONTRACT)
    passed, quarantined = contract.enforce(build(book, reporting_date))

    passed.cache()
    quarantined.cache()
    passed_count, quarantined_count = passed.count(), quarantined.count()

    if quarantined_count:
        write_partition(quarantined, quarantine_table)

    # Raises if the dataset is unfit as a whole. Nothing reaches the engine: a wrong
    # regulatory figure costs more than a late one.
    contract.assert_dataset(passed, quarantined)

    write_partition(passed, gold_table)
    return passed_count, quarantined_count


if __name__ == "__main__":
    from dataplatform.lakehouse.session import local_session

    silver_table, gold_table, quarantine_table, reporting_date = sys.argv[1:5]

    spark = local_session(f"gold_engine_input_{reporting_date}")
    for name in (gold_table, quarantine_table):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name.rsplit('.', 1)[0]}")

    passed, quarantined = run(spark, silver_table, gold_table, quarantine_table, reporting_date)
    spark.stop()

    print(f"{reporting_date}  published={passed:,}  quarantined={quarantined:,}")
