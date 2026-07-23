"""Chaos 01: a duplicated grain in the engine input.

Claim under test: a dataset whose grain repeats is wrong as a whole, so the contract
stops it and nothing publishes.

Fault injected: one exposure appears twice in the silver slice handed to the build. The
cause varies in production — a feed delivered twice, a rerun that appended instead of
replacing, a join that fanned out — but the shape is the same.

The experiment writes nothing. It reads a slice of silver, duplicates a row in memory,
and runs the same contract the job runs.

Usage:
  python -m chaos.exp01_duplicate_grain [reporting_date]
"""
import sys

from pyspark.sql import functions as F

from dataplatform.contracts.contract import Contract, ContractViolation
from dataplatform.lakehouse.session import local_session
from domains.credit_risk.gold_engine_input import build

SILVER = "lakehouse.silver_credit_risk.exposure"
CONTRACT = "credit_risk_exposure_input"


def main(reporting_date: str) -> None:
    spark = local_session("chaos_01_duplicate_grain")

    book = spark.read.table(SILVER).filter(
        F.col("snapshot_date") == F.lit(reporting_date).cast("date")
    )
    clean = build(book, reporting_date).cache()

    if not clean.count():
        print(f"no rows at {reporting_date}. available snapshot dates:")
        (spark.read.table(SILVER).select("snapshot_date").distinct()
         .orderBy("snapshot_date").show(20, truncate=False))
        spark.stop()
        return

    print(f"clean:   {clean.count():>9,} exposures")

    faulted = clean.union(clean.limit(1))
    print(f"faulted: {faulted.count():>9,} exposures, one duplicated")

    contract = Contract.named(CONTRACT)
    passed, quarantined = contract.enforce(faulted)
    print(f"row rules: {passed.count():,} passed, {quarantined.count():,} quarantined")

    try:
        contract.assert_dataset(passed, quarantined)
    except ContractViolation as exc:
        print(f"\nHALTED — nothing publishes\n  {exc}")
        spark.stop()
        return

    print("\nNOT HALTED — the duplicate would reach the engine")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "2018-12-31")
