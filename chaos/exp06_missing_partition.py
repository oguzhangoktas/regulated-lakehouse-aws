"""Chaos 06: the upstream partition never arrived.

A source can fail to deliver. The question is not whether that is possible but what the
pipeline does about it: stop, or succeed with nothing. A run that succeeds with nothing
publishes a regulatory figure of zero, and zero does not look like a failure anywhere
downstream — it looks like a bank that held no risk that month.

This traces the chain for a reporting date with no data upstream, stage by stage, and
reports where it stops. If it does not stop, it asks what the drift monitor would have
said.

The experiment writes nothing.

Usage:
  python -m chaos.exp06_missing_partition --s3
"""
import argparse

from pyspark.sql import functions as F

from dataplatform.contracts.contract import Contract, ContractViolation
from dataplatform.lakehouse.session import local_session
from dataplatform.quality.drift import compare, measure
from domains.credit_risk.gold_engine_input import build
from domains.credit_risk.rwa_output_job import reconcile
from domains.credit_risk.vendor_rwa_engine import run_engine

SILVER = "lakehouse.silver_credit_risk.exposure"
INPUT_CONTRACT = "credit_risk_exposure_input"
OUTPUT_CONTRACT = "credit_risk_rwa_output"
WAREHOUSE = "s3://oglh-gold-915909866528/"


def measures():
    return {
        "exposure_count": F.count("*"),
        "total_outstanding": F.sum("outstanding_amount"),
    }


def period(spark, reporting_date: str):
    book = spark.read.table(SILVER).filter(
        F.col("snapshot_date") == F.lit(reporting_date).cast("date")
    )
    return build(book, reporting_date)


def main(absent_date: str, baseline_date: str, s3: bool) -> None:
    spark = local_session("chaos_06_missing_partition", warehouse=WAREHOUSE, s3=s3)

    print(f"reporting date with no upstream data: {absent_date}\n")

    book = spark.read.table(SILVER).filter(
        F.col("snapshot_date") == F.lit(absent_date).cast("date")
    )
    print(f"1. silver rows for the date:  {book.count():,}")

    engine_input = build(book, absent_date)
    print(f"2. engine input rows:         {engine_input.count():,}")

    input_contract = Contract.named(INPUT_CONTRACT)
    passed, quarantined = input_contract.enforce(engine_input)
    try:
        input_contract.assert_dataset(passed, quarantined)
        print("3. input contract:            passed on an empty set")
    except ContractViolation as exc:
        print(f"3. input contract:            HALTED — {exc}")
        spark.stop()
        return

    engine_output = run_engine(passed)
    output_contract = Contract.named(OUTPUT_CONTRACT)
    out_passed, out_quarantined = output_contract.enforce(engine_output)
    try:
        output_contract.assert_dataset(out_passed, out_quarantined)
        print("4. output contract:           passed on an empty set")
    except ContractViolation as exc:
        print(f"4. output contract:           HALTED — {exc}")
        spark.stop()
        return

    try:
        reconcile(passed, out_passed, absent_date)
        print("5. reconciliation:            passed, nothing sent and nothing returned")
    except ContractViolation as exc:
        print(f"5. reconciliation:            HALTED — {exc}")
        spark.stop()
        return

    total_rwa = out_passed.select(F.sum("rwa")).first()[0] or 0
    print(f"\nRUN SUCCEEDED — it would publish {out_passed.count():,} exposures, "
          f"RWA {float(total_rwa) / 1e9:,.2f}bn")

    baseline = period(spark, baseline_date)
    if baseline.count():
        breaches = compare(
            measure(baseline, measures()), measure(engine_input, measures())
        )
        print(f"\nagainst {baseline_date}, the drift monitor reports {len(breaches)}:")
        for breach in breaches:
            print(f"  {breach}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--absent", default="2019-01-31")
    parser.add_argument("--baseline", default="2018-12-31")
    parser.add_argument("--s3", action="store_true")
    args = parser.parse_args()
    main(args.absent, args.baseline, args.s3)
