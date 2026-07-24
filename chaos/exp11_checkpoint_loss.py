"""Chaos 11: the checkpoint is gone.

Structured Streaming records the offsets it has processed in the checkpoint. That record
is the whole of the exactly-once guarantee under append mode — the sink appends what it
is handed and has no way to know it has seen a message before. Delete the checkpoint and
`startingOffsets: earliest` does exactly what it says.

The real pipeline holds 6.3 million rows, so this uses a scratch topic and a scratch
table. The mechanism is the same one bronze_stream relies on.

Everything it creates is removed at the end: the topic, the table and the checkpoint.

Usage:
  python -m chaos.exp11_checkpoint_loss
"""
import json
import shutil
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from dataplatform.lakehouse.session import streaming_session

BOOTSTRAP = "localhost:9092"
TOPIC = "chaos-checkpoint"
SCRATCH_NS = "lakehouse.chaos_scratch"
SCRATCH = f"{SCRATCH_NS}.checkpoint_loss"
CHECKPOINT = Path("data/checkpoints/chaos_checkpoint_loss")
MESSAGES = 100

PAYLOAD = StructType([
    StructField("n", IntegerType()),
    StructField("body", StringType()),
])


def reset_topic(admin: AdminClient) -> None:
    if TOPIC in admin.list_topics(timeout=10).topics:
        admin.delete_topics([TOPIC])
        time.sleep(3)
    admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
    time.sleep(3)


def publish() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for n in range(MESSAGES):
        producer.produce(TOPIC, value=json.dumps({"n": n, "body": f"message {n}"}))
    producer.flush()


def consume_once(spark) -> None:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), PAYLOAD).alias("m")
    ).select("m.*")

    (
        parsed.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", str(CHECKPOINT))
        .trigger(availableNow=True)
        .toTable(SCRATCH)
        .awaitTermination()
    )


def landed(spark) -> str:
    table = spark.read.table(SCRATCH)
    return f"{table.count():>4,} rows, {table.select('n').distinct().count():>4,} distinct"


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        admin.list_topics(timeout=10)
    except Exception:
        print("kafka is not reachable on localhost:9092 — start it with `make up`")
        return

    spark = streaming_session("chaos_11_checkpoint_loss")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SCRATCH_NS}")
    spark.sql(f"DROP TABLE IF EXISTS {SCRATCH}")
    shutil.rmtree(CHECKPOINT, ignore_errors=True)

    reset_topic(admin)
    publish()
    print(f"published {MESSAGES} messages\n")

    consume_once(spark)
    print(f"run 1, fresh checkpoint:   {landed(spark)}")

    consume_once(spark)
    print(f"run 2, checkpoint intact:  {landed(spark)}   nothing reprocessed")

    shutil.rmtree(CHECKPOINT, ignore_errors=True)
    print("\ncheckpoint deleted\n")

    consume_once(spark)
    print(f"run 3, checkpoint gone:    {landed(spark)}")

    spark.sql(f"DROP TABLE IF EXISTS {SCRATCH}")
    shutil.rmtree(CHECKPOINT, ignore_errors=True)
    admin.delete_topics([TOPIC])
    spark.stop()
    print("\ntopic, table and checkpoint removed")


if __name__ == "__main__":
    main()
