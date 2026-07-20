"""Velocity detection: an account moving money unusually fast.

The rule that justifies streaming. A batch job answers "how much did this account
move last month"; velocity answers "how much in the last few hours, right now",
over a sliding event-time window that advances as transactions arrive.

Only TRANSFER and CASH_OUT carry fraud (measured), so only those are counted. An
account breaching either a count or a total-amount threshold inside the window
raises an alert.

PaySim records step as an hourly integer, not a timestamp. It is projected onto a
fixed reference date to give the window an event-time axis; in production this is
the transaction's own timestamp.
"""
from pyspark.sql import DataFrame, functions as F

REFERENCE = "2018-01-01"
MONITORED_TYPES = ["TRANSFER", "CASH_OUT"]

WINDOW = "2 hours"
SLIDE = "1 hour"
WATERMARK = "3 hours"

COUNT_THRESHOLD = 3          # transactions in the window
AMOUNT_THRESHOLD = 1_000_000  # total moved in the window


def with_event_time(df: DataFrame) -> DataFrame:
    """Project the hourly step onto a real timestamp the window can range over."""
    return df.withColumn(
        "event_time",
        F.expr(f"timestamp('{REFERENCE}') + make_interval(0, 0, 0, 0, step, 0, 0)"),
    )


def velocity_alerts(silver: DataFrame) -> DataFrame:
    """Accounts breaching a count or amount threshold in a sliding window."""
    monitored = with_event_time(silver).filter(F.col("type").isin(MONITORED_TYPES))

    windowed = (
        monitored
        .withWatermark("event_time", WATERMARK)
        .groupBy(
            F.window("event_time", WINDOW, SLIDE).alias("w"),
            F.col("name_orig"),
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.max("is_fraud").alias("any_fraud"),
        )
    )

    breached = windowed.filter(
        (F.col("txn_count") >= COUNT_THRESHOLD)
        | (F.col("total_amount") >= AMOUNT_THRESHOLD)
    )

    return breached.select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        F.col("name_orig"),
        F.col("txn_count"),
        F.col("total_amount"),
        F.lit("velocity").alias("rule"),
        F.when(F.col("txn_count") >= COUNT_THRESHOLD, F.lit("count"))
         .otherwise(F.lit("amount")).alias("breach_type"),
        F.col("any_fraud"),
    )
