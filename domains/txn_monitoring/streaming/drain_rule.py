"""Balance-drain detection: a transfer that empties the origin account.

Measured on the source: 97.6% of fraud empties the origin account (new balance
zero, old balance positive). Drain alone is not enough — normal CASH_OUT drains
too — so this pairs the drain with a high amount and the two fraud-carrying types.

Unlike velocity this is per-transaction, not windowed: it inspects one transaction's
balance movement rather than an account's activity over time. It is the other kind of
signal, which is what makes combining the two worthwhile.
"""
from pyspark.sql import DataFrame, functions as F

MONITORED_TYPES = ["TRANSFER", "CASH_OUT"]
# Legit median is ~74k; fraud median ~441k. A threshold above the legit body keeps
# the everyday drains out while catching the large ones.
AMOUNT_THRESHOLD = 200_000


def drain_alerts(silver: DataFrame) -> DataFrame:
    """Transactions that empty the origin account while moving a large amount."""
    drained = (
        silver.filter(F.col("type").isin(MONITORED_TYPES))
        .filter(F.col("new_balance_orig") == 0)
        .filter(F.col("old_balance_orig") > 0)
        .filter(F.col("amount") >= AMOUNT_THRESHOLD)
    )

    return drained.select(
        F.col("step"),
        F.col("name_orig"),
        F.col("type"),
        F.col("amount"),
        F.col("old_balance_orig"),
        F.lit("drain").alias("rule"),
        F.col("is_fraud").alias("any_fraud"),
    )
