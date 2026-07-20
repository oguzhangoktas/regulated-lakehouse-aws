"""Consume transactions from Kafka and land them in bronze.

The first stage of the streaming pipeline: read the topic, parse the JSON payload,
and write each micro-batch to an Iceberg table. Structured Streaming records its
Kafka offsets in the checkpoint, so this is exactly-once — a restart resumes from
the last committed offset rather than reprocessing or dropping messages.

Bronze here is the streaming equivalent of the batch bronze layer: the transactions
as received, unshaped, the source of any downstream rebuild.

Usage:
  python -m domains.txn_monitoring.streaming.bronze_stream [--once]
"""
import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

from dataplatform.lakehouse.session import streaming_session

TOPIC = "transactions"
BOOTSTRAP = "localhost:9092"
TABLE = "lakehouse.bronze_txn_monitoring.transactions"

# The producer's message shape. Declared explicitly rather than inferred: a schema
# guess from streaming data is not stable across micro-batches.
PAYLOAD = StructType([
    StructField("step", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("name_orig", StringType()),
    StructField("old_balance_orig", DoubleType()),
    StructField("new_balance_orig", DoubleType()),
    StructField("name_dest", StringType()),
    StructField("old_balance_dest", DoubleType()),
    StructField("new_balance_dest", DoubleType()),
    StructField("is_fraud", IntegerType()),
])


def build_stream(spark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka gives key/value as bytes plus metadata. Keep the parsed payload and the
    # broker metadata that anchors each record in the stream.
    return (
        raw.select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), PAYLOAD).alias("txn"),
        )
        .select("kafka_partition", "kafka_offset", "kafka_timestamp", "txn.*")
    )


def run(once: bool) -> None:
    spark = streaming_session("txn_bronze_stream")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze_txn_monitoring")

    stream = build_stream(spark)

    writer = (
        stream.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", f"data/checkpoints/{TABLE}")
        .toTable(TABLE)
        if not once
        else (
            stream.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", f"data/checkpoints/{TABLE}")
            .trigger(availableNow=True)
            .toTable(TABLE)
        )
    )
    writer.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true",
                        help="process what is currently in the topic, then stop")
    args = parser.parse_args()
    run(args.once)
