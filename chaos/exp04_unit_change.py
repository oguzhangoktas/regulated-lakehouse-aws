"""Chaos 04: the source changes units.

This experiment tests no claim. It looks for a limit, and expects to find one.

Fault injected: an upstream change delivers money in cents rather than the units the
platform assumes. Every amount is multiplied by a hundred. Nothing about the shape of the
data changes: the values stay positive, stay inside their declared ranges, keep their
types, and the grain stays unique.

The question is what stops it. If the answer is nothing, the platform publishes a
regulatory figure that is wrong by two orders of magnitude and looks entirely healthy.

The experiment writes nothing.

Usage:
  python -m chaos.exp04_unit_change [reporting_date]
"""
import sys

from pyspark.sql import functions as F

from dataplatform.contracts.contract import Contract, ContractViolation
from dataplatform.lakehouse.session import local_session
from domains.credit_risk.gold_engine_input import build
from domains.credit_risk.vendor_rwa_engine import run_engine

SILVER = "lakehouse.silver_credit_risk.exposure"
CONTRACT = "credit_risk_exposure_input"
MONEY = ["original_amount", "outstanding_amount", "provision_amount"]
FACTOR = 100


def total_rwa(engine_input) -> float:
    value = run_engine(engine_input).select(F.sum("rwa")).first()[0]
    return float(value or 0)


def main(reporting_date: str) -> None:
    spark = local_session("chaos_04_unit_change")

    book = spark.read.table(SILVER).filter(
        F.col("snapshot_date") == F.lit(reporting_date).cast("date")
    )
    clean = build(book, reporting_date)

    faulted = clean
    for column in MONEY:
        faulted = faulted.withColumn(column, F.col(column) * F.lit(FACTOR))

    contract = Contract.named(CONTRACT)
    passed, quarantined = contract.enforce(faulted)
    print(f"passed:      {passed.count():>9,}")
    print(f"quarantined: {quarantined.count():>9,}")

    halted = False
    try:
        contract.assert_dataset(passed, quarantined)
    except ContractViolation as exc:
        halted = True
        print(f"\nHALTED — {exc}")

    if not halted:
        print("\nNOT HALTED — every validation passed")
        before = total_rwa(clean)
        after = total_rwa(passed)
        print(f"  RWA as reported before: {before / 1e9:>10,.2f}bn")
        print(f"  RWA as reported after:  {after / 1e9:>10,.2f}bn")
        print(f"  overstated by a factor of {after / before:,.0f}" if before else "")

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "2018-12-31")
