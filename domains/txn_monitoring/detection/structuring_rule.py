"""Structuring detection: transfers deliberately kept below a reporting threshold.

Structuring is splitting a large movement into several transactions each just under a
reporting threshold, to avoid the report the single large transaction would trigger.
The signature is an account making multiple transfers in a short window, each below the
threshold but summing above it.

This is a windowed, stateful rule: it needs an account's transactions over time, not a
single transaction. PaySim has no structuring (its accounts do not repeat), so this is
exercised against a synthetic red-team scenario (ADR-010), not the real stream.
"""
from pyspark.sql import DataFrame, functions as F

MONITORED_TYPES = ["TRANSFER", "CASH_OUT"]

# The reporting threshold structuring tries to stay under. A transaction is "just under"
# if it falls in the band below the threshold.
THRESHOLD = 200_000
BAND = 0.8  # transactions between 80% and 100% of the threshold are suspicious
WINDOW = "24 hours"
MIN_COUNT = 3          # at least this many just-under transfers in the window
MIN_TOTAL = THRESHOLD  # summing to more than a single report would have covered


def with_event_time(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "event_time",
        F.expr("timestamp('2018-01-01') + make_interval(0, 0, 0, 0, step, 0, 0)"),
    )


def structuring_alerts(txns: DataFrame) -> DataFrame:
    """Accounts making several just-under-threshold transfers that sum above it."""
    just_under = (
        with_event_time(txns)
        .filter(F.col("type").isin(MONITORED_TYPES))
        .filter(F.col("amount") >= THRESHOLD * BAND)
        .filter(F.col("amount") < THRESHOLD)
    )

    windowed = (
        just_under
        .withWatermark("event_time", WINDOW)
        .groupBy(F.window("event_time", WINDOW).alias("w"), F.col("name_orig"))
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
        )
    )

    return (
        windowed
        .filter((F.col("txn_count") >= MIN_COUNT) & (F.col("total_amount") >= MIN_TOTAL))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("name_orig"),
            F.col("txn_count"),
            F.col("total_amount"),
            F.lit("structuring").alias("rule"),
        )
    )
