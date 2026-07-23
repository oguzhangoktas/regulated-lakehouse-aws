"""Chaos 03: a column disappears from the source.

Claim under test: a missing field is a schema break rather than a data defect, so the run
stops instead of producing a partial result.

The interesting question is not whether it stops but *where*. The same missing column
surfaces differently depending on how far it travels before anything looks for it, and
the two errors read very differently at three in the morning.

Fault injected: interest_rate is dropped, first before the engine input is built and then
after, so both failure modes are visible in one run.

The experiment writes nothing.

Usage:
  python -m chaos.exp03_schema_break [reporting_date]
"""
import sys

from pyspark.sql import functions as F

from dataplatform.contracts.contract import Contract, ContractViolation
from dataplatform.lakehouse.session import local_session
from domains.credit_risk.gold_engine_input import build

SILVER = "lakehouse.silver_credit_risk.exposure"
CONTRACT = "credit_risk_exposure_input"
MISSING = "interest_rate"


def main(reporting_date: str) -> None:
    spark = local_session("chaos_03_schema_break")

    book = spark.read.table(SILVER).filter(
        F.col("snapshot_date") == F.lit(reporting_date).cast("date")
    )
    contract = Contract.named(CONTRACT)

    print(f"--- dropped upstream: {MISSING} missing from silver ---")
    try:
        build(book.drop(MISSING), reporting_date).count()
        print("no error — the transform tolerated the missing column")
    except Exception as exc:
        print(f"{type(exc).__name__}")
        print(f"  {str(exc).splitlines()[0][:200]}")

    print(f"\n--- dropped at the boundary: {MISSING} missing from the engine input ---")
    try:
        contract.enforce(build(book, reporting_date).drop(MISSING))
        print("no error — the contract accepted a frame missing a declared field")
    except ContractViolation as exc:
        print("ContractViolation")
        print(f"  {exc}")

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "2018-12-31")
