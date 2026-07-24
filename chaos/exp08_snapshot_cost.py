"""Chaos 08: what history costs.

Iceberg writes a snapshot on every commit, and a snapshot keeps its data files alive.
Files superseded by a later write are not deleted while any retained snapshot still
points at them, so a table's storage is the data it holds plus the data it used to hold.

ADR-006 records that table history is operational rather than the audit trail. This
measures what that history costs today, which is the number that decides how long to keep
it.

Nothing here writes or expires anything. Expiring snapshots is irreversible and is
demonstrated separately against a scratch table.

Usage:
  python -m chaos.exp08_snapshot_cost --s3
"""
import argparse

from pyspark.sql import functions as F

from dataplatform.lakehouse.session import local_session

WAREHOUSE = "s3://oglh-gold-915909866528/"

TABLES = [
    "lakehouse.silver_credit_risk.exposure",
    "lakehouse.gold_credit_risk.engine_input",
    "lakehouse.gold_credit_risk.rwa_output",
    "lakehouse.silver_txn_monitoring.transactions",
    "lakehouse.gold_txn_monitoring.alerts",
]

MB = 1024 * 1024


def summarise(spark, table: str) -> None:
    snapshots = spark.read.table(f"{table}.snapshots")
    oldest, newest, count = snapshots.agg(
        F.min("committed_at"), F.max("committed_at"), F.count("*")
    ).first()

    live = spark.read.table(f"{table}.files").agg(
        F.count("*"), F.sum("file_size_in_bytes")
    ).first()
    everything = spark.read.table(f"{table}.all_data_files").agg(
        F.count("*"), F.sum("file_size_in_bytes")
    ).first()

    live_files, live_bytes = live[0], live[1] or 0
    all_files, all_bytes = everything[0], everything[1] or 0
    retained = all_bytes - live_bytes

    name = table.rsplit(".", 2)
    print(f"\n{name[-2]}.{name[-1]}")
    print(f"  snapshots     {count:>6,}   {oldest} -> {newest}")
    print(f"  live files    {live_files:>6,}   {live_bytes / MB:>10,.0f} MB")
    print(f"  all files     {all_files:>6,}   {all_bytes / MB:>10,.0f} MB")
    if live_bytes:
        print(f"  held by history {retained / MB:>8,.0f} MB "
              f"({retained / live_bytes * 100:.0f}% on top of live data)")


def main(s3: bool) -> None:
    spark = local_session("chaos_08_snapshot_cost", warehouse=WAREHOUSE, s3=s3)

    for table in TABLES:
        try:
            summarise(spark, table)
        except Exception as exc:
            print(f"\n{table}\n  unavailable: {str(exc).splitlines()[0][:120]}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3", action="store_true")
    main(parser.parse_args().s3)
