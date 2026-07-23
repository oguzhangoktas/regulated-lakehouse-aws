"""Chaos 02: quarantine over the threshold.

Claim under test: a few bad records are isolated and the rest publishes, but when a large
share fails the problem is systemic — the source is wrong, not the record — and nothing
publishes.

Fault injected: an unknown rating grade on 2% of exposures. The contract allows 1%.
Selection is by hash of the exposure id, so the same rows fail on every run.

The experiment writes nothing.

Usage:
  python -m chaos.exp02_quarantine_threshold [reporting_date]
"""
import sys

from pyspark.sql import functions as F

from dataplatform.contracts.contract import Contract, ContractViolation
from dataplatform.lakehouse.session import local_session
from domains.credit_risk.gold_engine_input import build

SILVER = "lakehouse.silver_credit_risk.exposure"
CONTRACT = "credit_risk_exposure_input"
CORRUPT_ONE_IN = 50  # 2%


def main(reporting_date: str) -> None:
    spark = local_session("chaos_02_quarantine_threshold")

    book = spark.read.table(SILVER).filter(
        F.col("snapshot_date") == F.lit(reporting_date).cast("date")
    )
    clean = build(book, reporting_date)

    faulted = clean.withColumn(
        "rating_grade",
        F.when(F.abs(F.hash("exposure_id")) % CORRUPT_ONE_IN == 0, F.lit("Z"))
        .otherwise(F.col("rating_grade")),
    )

    contract = Contract.named(CONTRACT)
    passed, quarantined = contract.enforce(faulted)

    kept, dropped = passed.count(), quarantined.count()
    share = dropped / (kept + dropped) * 100 if kept + dropped else 0.0
    print(f"passed:      {kept:>9,}")
    print(f"quarantined: {dropped:>9,}  ({share:.2f}%)")
    print(f"limit:       {contract.assertions.get('max_quarantine_pct')}%")

    try:
        contract.assert_dataset(passed, quarantined)
    except ContractViolation as exc:
        print(f"\nHALTED — nothing publishes\n  {exc}")
        spark.stop()
        return

    print(f"\nNOT HALTED — {kept:,} exposures would publish without the rest")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "2018-12-31")
