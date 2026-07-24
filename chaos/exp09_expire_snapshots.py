"""Chaos 09: expiring snapshots costs the history it reclaims.

Iceberg keeps every commit as a snapshot, and a snapshot keeps its data files alive. That
is what makes time travel and rollback possible, and it is also why a table's storage
exceeds the data it holds.

`expire_snapshots` reclaims that storage. It is not free: what it reclaims is exactly the
ability to read the table as it was. The two are the same thing seen from either side, so
a retention period is a decision about how far back a recovery can reach.

This runs against a scratch table it drops at the end. Expiry is irreversible and is not
demonstrated on anything the platform uses.

Usage:
  python -m chaos.exp09_expire_snapshots
"""
from pyspark.sql import DataFrame, functions as F

from dataplatform.lakehouse.session import local_session
from domains.credit_risk.gold_engine_input import build

SILVER = "lakehouse.silver_credit_risk.exposure"
SCRATCH_NS = "lakehouse.chaos_scratch"
SCRATCH = f"{SCRATCH_NS}.history"
MB = 1024 * 1024


def write(df: DataFrame) -> None:
    writer = df.writeTo(SCRATCH)
    if df.sparkSession.catalog.tableExists(SCRATCH):
        writer.overwritePartitions()
    else:
        writer.partitionedBy(F.col("reporting_date")).create()


def storage(spark) -> str:
    live = spark.read.table(f"{SCRATCH}.files").agg(
        F.count("*"), F.sum("file_size_in_bytes")).first()
    everything = spark.read.table(f"{SCRATCH}.all_data_files").agg(
        F.count("*"), F.sum("file_size_in_bytes")).first()
    held = (everything[1] or 0) - (live[1] or 0)
    return (f"{live[0]} live file(s) {(live[1] or 0) / MB:.1f} MB, "
            f"{everything[0]} total {(everything[1] or 0) / MB:.1f} MB, "
            f"history holds {held / MB:.1f} MB")


def snapshot_ids(spark) -> list:
    return [r["snapshot_id"] for r in
            spark.read.table(f"{SCRATCH}.snapshots").orderBy("committed_at").collect()]


def read_at(spark, snapshot_id) -> str:
    try:
        rows = spark.read.option("snapshot-id", snapshot_id).table(SCRATCH).count()
        return f"{rows:,} rows"
    except Exception as exc:
        return f"unavailable — {type(exc).__name__}: {str(exc).splitlines()[0][:90]}"


def main() -> None:
    spark = local_session("chaos_09_expire_snapshots")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SCRATCH_NS}")
    spark.sql(f"DROP TABLE IF EXISTS {SCRATCH}")

    book = spark.read.table(SILVER).filter(
        F.col("snapshot_date") == F.lit("2018-12-31").cast("date")
    )
    day = build(book, "2018-12-31").cache()

    for label, rows in [("v1", 3_000), ("v2", 1_000), ("v3", 5_000)]:
        write(day.limit(rows))
        print(f"{label}: wrote {rows:,} rows   {storage(spark)}")

    first, _, last = snapshot_ids(spark)
    print(f"\ntime travel to the first snapshot: {read_at(spark, first)}")
    print(f"time travel to the last snapshot:  {read_at(spark, last)}")

    print("\nexpiring everything but the current snapshot")
    spark.sql(
        f"CALL lakehouse.system.expire_snapshots("
        f"table => '{SCRATCH.removeprefix('lakehouse.')}', "
        f"older_than => TIMESTAMP '{__import__('datetime').datetime.now()}', "
        f"retain_last => 1)"
    )

    print(f"after expiry: {storage(spark)}")
    print(f"snapshots remaining: {len(snapshot_ids(spark))}")
    print(f"\ntime travel to the first snapshot: {read_at(spark, first)}")
    print(f"time travel to the last snapshot:  {read_at(spark, last)}")

    spark.sql(f"DROP TABLE IF EXISTS {SCRATCH}")
    spark.stop()


if __name__ == "__main__":
    main()
