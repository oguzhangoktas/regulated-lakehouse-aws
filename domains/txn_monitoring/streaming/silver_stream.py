"""Conform and validate the transaction stream into silver.

Reads the bronze stream, applies the same contract engine credit_risk uses in batch,
and writes passing transactions to silver and failures to quarantine. The contract is
enforced per micro-batch through foreachBatch, so streaming reuses the batch quality
logic unchanged rather than reimplementing it.

Only TRANSFER and CASH_OUT carry fraud (measured), so detection downstream reads
silver filtered to those; silver itself keeps every type for completeness.

Usage:
  python -m domains.txn_monitoring.streaming.silver_stream [--once]
"""
import argparse

from pyspark.sql import DataFrame, functions as F

from dataplatform.contracts.contract import Contract
from dataplatform.lakehouse.session import streaming_session

CONTRACT = "txn_monitoring_transaction"
SOURCE = "lakehouse.bronze_txn_monitoring.transactions"
SILVER = "lakehouse.silver_txn_monitoring.transactions"
QUARANTINE = "lakehouse.quarantine_txn_monitoring.transactions"

MONEY = "decimal(18,2)"


def conform(df: DataFrame) -> DataFrame:
    """Cast money to decimal; carry the stream anchor and the fraud label through."""
    return df.select(
        F.col("step").cast("int"),
        F.col("type"),
        F.col("amount").cast(MONEY),
        F.col("name_orig"),
        F.col("old_balance_orig").cast(MONEY),
        F.col("new_balance_orig").cast(MONEY),
        F.col("name_dest"),
        F.col("old_balance_dest").cast(MONEY),
        F.col("new_balance_dest").cast(MONEY),
        F.col("is_fraud").cast("int"),
        F.col("kafka_offset").cast("long"),
    )


def append_or_create(df: DataFrame, table: str) -> None:
    if df.sparkSession.catalog.tableExists(table):
        df.writeTo(table).append()
    else:
        df.writeTo(table).using("iceberg").create()


def make_batch_handler(contract: Contract):
    """Apply the contract to each micro-batch, then write results.

    foreachBatch gives an ordinary DataFrame per micro-batch, so the batch contract
    engine runs unchanged inside the stream.
    """
    def handle(batch: DataFrame, batch_id: int) -> None:
        passed, quarantined = contract.enforce(conform(batch))
        append_or_create(passed, SILVER)
        if quarantined.count():
            append_or_create(quarantined, QUARANTINE)
    return handle


def run(once: bool, s3: bool) -> None:
    spark = streaming_session("txn_silver_stream", s3=s3)
    ckpt = f"s3://oglh-artifacts-915909866528/txn-checkpoints/{SILVER}" if s3 else f"data/checkpoints/{SILVER}"
    for ns in ("silver_txn_monitoring", "quarantine_txn_monitoring"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{ns}")

    contract = Contract.named(CONTRACT)
    stream = spark.readStream.format("iceberg").load(SOURCE)

    writer = (
        stream.writeStream
        .foreachBatch(make_batch_handler(contract))
        .option("checkpointLocation", ckpt)
    )
    if once:
        writer = writer.trigger(availableNow=True)

    writer.start().awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--s3", action="store_true",
                        help="write to S3 under the Glue catalog instead of local disk")
    args = parser.parse_args()
    run(args.once, args.s3)
