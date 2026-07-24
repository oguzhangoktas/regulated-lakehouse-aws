"""Chaos 05: the unit change, watched.

Experiment 04 scaled every amount by a hundred and passed every contract rule, because
each rule checks the data against itself and the fault was uniform. This runs the same
fault with the drift monitor comparing the period against the one before it.

The local warehouse holds one reporting date; the twelve month-ends were processed on
Glue and live in S3, so the baseline comes from there with --s3.

The experiment writes nothing.

Usage:
  python -m chaos.exp05_drift_monitor --s3
  python -m chaos.exp05_drift_monitor --previous 2018-11-30 --current 2018-12-31 --s3
"""
import argparse

from pyspark.sql import functions as F

from dataplatform.contracts.contract import Contract
from dataplatform.lakehouse.session import local_session
from dataplatform.quality.drift import compare, measure
from domains.credit_risk.gold_engine_input import build

SILVER = "lakehouse.silver_credit_risk.exposure"
CONTRACT = "credit_risk_exposure_input"
WAREHOUSE = "s3://oglh-gold-915909866528/"
MONEY = ["original_amount", "outstanding_amount", "provision_amount"]
FACTOR = 100


def measures():
    """Built on call, not held as a constant: a Column binds to the active session."""
    return {
        "exposure_count": F.count("*"),
        "total_outstanding": F.sum("outstanding_amount"),
        "total_original": F.sum("original_amount"),
    }


def period(spark, reporting_date: str):
    book = spark.read.table(SILVER).filter(
        F.col("snapshot_date") == F.lit(reporting_date).cast("date")
    )
    return build(book, reporting_date)


def report(label: str, breaches: list) -> None:
    if not breaches:
        print(f"{label}: no breach")
        return
    print(f"{label}: {len(breaches)} breach(es)")
    for breach in breaches:
        print(f"  {breach}")


def main(previous_date: str, reporting_date: str, s3: bool) -> None:
    spark = local_session("chaos_05_drift_monitor", warehouse=WAREHOUSE, s3=s3)

    before = period(spark, previous_date)
    if not before.count():
        print(f"no rows at {previous_date}. available snapshot dates:")
        (spark.read.table(SILVER).select("snapshot_date").distinct()
         .orderBy("snapshot_date").show(20, truncate=False))
        spark.stop()
        return

    clean = period(spark, reporting_date)

    faulted = clean
    for column in MONEY:
        faulted = faulted.withColumn(column, F.col(column) * F.lit(FACTOR))

    contract = Contract.named(CONTRACT)
    passed, quarantined = contract.enforce(faulted)
    print(f"contract: {passed.count():,} passed, {quarantined.count():,} quarantined\n")

    baseline = measure(before, measures())
    report("clean period  ", compare(baseline, measure(clean, measures())))
    report("faulted period", compare(baseline, measure(faulted, measures())))

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--previous", default="2018-11-30")
    parser.add_argument("--current", default="2018-12-31")
    parser.add_argument("--s3", action="store_true",
                        help="read silver from S3 under the Glue catalog")
    args = parser.parse_args()
    main(args.previous, args.current, args.s3)
