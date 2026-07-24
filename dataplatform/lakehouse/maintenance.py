"""Table maintenance: expiring history and compacting files.

Iceberg records every commit as a snapshot, and a snapshot keeps its data files alive. A
table's storage therefore grows with its edit history as well as with its contents, and
nothing reclaims that on its own. The S3 lifecycle rule expires noncurrent object
versions, but Iceberg never overwrites a data file — it writes new ones and repoints the
metadata — so its superseded files are current versions of distinct objects and that rule
never sees them. Only expiry does.

Retention is a recovery decision rather than a storage one: the history that costs
storage is exactly the history a rollback can reach. Thirty days matches the object
versioning policy and covers a monthly reporting cycle, which is how long a bad write can
go unnoticed. The floor keeps a rarely written table from ageing out of its own history.

Compaction addresses the opposite problem — many small files rather than few large ones.
This platform writes infrequently and in bulk and does not have that problem today; a
deployment writing every few hours would.

Expiry is irreversible, so this reports by default and acts only when asked.

Usage:
  python -m dataplatform.lakehouse.maintenance --table <table> --s3
  python -m dataplatform.lakehouse.maintenance --table <table> --s3 --apply
"""
import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, functions as F

RETENTION_DAYS = 30
RETAIN_LAST = 5
TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024
MB = 1024 * 1024


def procedure_target(table: str) -> tuple[str, str]:
    """Split a qualified name into the catalog and the identifier a procedure takes.

    Iceberg procedures are called on the catalog and given the rest of the name, so
    `lakehouse.silver_credit_risk.exposure` becomes a call on `lakehouse` for
    `silver_credit_risk.exposure`.
    """
    catalog, identifier = table.split(".", 1)
    return catalog, identifier


def storage(spark: SparkSession, table: str) -> dict:
    """What the table holds, and what its history holds on top of that."""
    live = spark.read.table(f"{table}.files").agg(
        F.count("*"), F.sum("file_size_in_bytes")
    ).first()
    everything = spark.read.table(f"{table}.all_data_files").agg(
        F.count("*"), F.sum("file_size_in_bytes")
    ).first()
    snapshots = spark.read.table(f"{table}.snapshots").count()

    live_bytes = live[1] or 0
    all_bytes = everything[1] or 0
    return {
        "snapshots": snapshots,
        "live_files": live[0],
        "live_bytes": live_bytes,
        "all_files": everything[0],
        "all_bytes": all_bytes,
        "held_by_history": all_bytes - live_bytes,
    }


def expirable(spark: SparkSession, table: str, older_than_days: int, retain_last: int) -> int:
    """How many snapshots the retention would remove, without removing them."""
    cutoff = datetime.now() - timedelta(days=older_than_days)
    total = spark.read.table(f"{table}.snapshots").count()
    old = spark.read.table(f"{table}.snapshots").filter(
        F.col("committed_at") < F.lit(cutoff)
    ).count()
    return max(0, min(old, total - retain_last))


def expire_snapshots(
    spark: SparkSession,
    table: str,
    older_than_days: int = RETENTION_DAYS,
    retain_last: int = RETAIN_LAST,
) -> dict:
    """Drop snapshots past the retention, keeping a floor. Irreversible."""
    catalog, identifier = procedure_target(table)
    cutoff = datetime.now() - timedelta(days=older_than_days)

    before = storage(spark, table)
    spark.sql(
        f"CALL {catalog}.system.expire_snapshots("
        f"table => '{identifier}', "
        f"older_than => TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S}', "
        f"retain_last => {retain_last})"
    )
    return {"before": before, "after": storage(spark, table)}


def compact(
    spark: SparkSession,
    table: str,
    target_file_size_bytes: int = TARGET_FILE_SIZE_BYTES,
) -> dict:
    """Rewrite the table's files towards a target size."""
    catalog, identifier = procedure_target(table)

    before = storage(spark, table)
    spark.sql(
        f"CALL {catalog}.system.rewrite_data_files("
        f"table => '{identifier}', "
        f"options => map('target-file-size-bytes', '{target_file_size_bytes}'))"
    )
    return {"before": before, "after": storage(spark, table)}


def describe(label: str, state: dict) -> str:
    return (
        f"{label:8s} {state['snapshots']:>4} snapshots  "
        f"{state['live_files']:>4} live files {state['live_bytes'] / MB:>8,.0f} MB  "
        f"history holds {state['held_by_history'] / MB:>8,.0f} MB"
    )


def main() -> None:
    from dataplatform.lakehouse.session import local_session

    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--warehouse", default="s3://oglh-gold-915909866528/")
    parser.add_argument("--retention-days", type=int, default=RETENTION_DAYS)
    parser.add_argument("--retain-last", type=int, default=RETAIN_LAST)
    parser.add_argument("--s3", action="store_true")
    parser.add_argument("--compact", action="store_true",
                        help="rewrite files towards the target size as well")
    parser.add_argument("--apply", action="store_true",
                        help="perform the maintenance; without this it only reports")
    args = parser.parse_args()

    spark = local_session("maintenance", warehouse=args.warehouse, s3=args.s3)
    print(args.table)
    print(describe("current", storage(spark, args.table)))

    if not args.apply:
        count = expirable(spark, args.table, args.retention_days, args.retain_last)
        print(f"would expire {count} snapshot(s) older than {args.retention_days} days, "
              f"keeping at least {args.retain_last}")
        print("run again with --apply to perform it")
        spark.stop()
        return

    result = expire_snapshots(spark, args.table, args.retention_days, args.retain_last)
    print(describe("expired", result["after"]))
    reclaimed = result["before"]["all_bytes"] - result["after"]["all_bytes"]
    print(f"reclaimed {reclaimed / MB:,.0f} MB")

    if args.compact:
        result = compact(spark, args.table)
        print(describe("compacted", result["after"]))

    spark.stop()


if __name__ == "__main__":
    main()
