"""Produce fraud alerts from the silver transaction stream.

Runs the detection rules over silver and writes the flagged transactions to a gold
alert table — the report a compliance team acts on. Each alert carries the rule that
fired and the transaction's identifying detail.

The sweep rule is the detector (measured 97.9% recall at 100% precision). Other
rules can be added here; each contributes alerts tagged with its own name, so an
analyst sees why a transaction was flagged.

The label column is carried through for evaluation only. In production there is no
label at detection time; here it lets the alert table be scored against ground truth.

Usage:
  python -m domains.txn_monitoring.detection.alert_job
"""
from pyspark.sql import DataFrame, SparkSession, functions as F

from dataplatform.lakehouse.session import local_session
from domains.txn_monitoring.detection.sweep_rule import sweep_alerts

SILVER = "lakehouse.silver_txn_monitoring.transactions"
ALERTS = "lakehouse.gold_txn_monitoring.alerts"


def build_alerts(silver: DataFrame) -> DataFrame:
    """Union the alerts from every detection rule into one report."""
    sweep = sweep_alerts(silver).select(
        "step", "name_orig", "name_dest", "type", "amount", "rule", "label",
    )
    # Further rules union here as they are added.
    return sweep


def write_alerts(df: DataFrame, table: str) -> None:
    if df.sparkSession.catalog.tableExists(table):
        df.writeTo(table).overwritePartitions()
    else:
        df.writeTo(table).partitionedBy(F.col("rule")).using("iceberg").create()


def run(spark: SparkSession) -> int:
    silver = spark.read.table(SILVER)
    alerts = build_alerts(silver)
    write_alerts(alerts, ALERTS)
    return alerts.count()


if __name__ == "__main__":
    spark = local_session("txn_alert_job")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold_txn_monitoring")
    n = run(spark)
    spark.stop()
    print(f"alerts written: {n:,}")
