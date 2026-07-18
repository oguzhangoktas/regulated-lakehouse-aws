"""Build the exposure input the credit-risk engine consumes.

Silver holds the whole book. The engine takes the exposures that carry capital: those
with a balance, and those in default (ADR-003). Settled exposures stay in silver for
reporting and do not reach the engine.

Fields the source does not record are derived here, at the boundary that declares
them, rather than being carried through silver.
"""
from pyspark.sql import Column, DataFrame, functions as F

EXPOSURE_CLASS = "retail_other"
CURRENCY = "USD"
DEFAULT_AT_DPD = 90


def in_scope(df: DataFrame) -> DataFrame:
    """Exposures consuming capital at the reporting date (ADR-003)."""
    return df.filter((F.col("outstanding_amount") > 0) | (F.col("status") == "defaulted"))


def default_date(as_of: Column) -> Column:
    """The date an exposure crossed into default.

    The source records payment history, not a default date. days_past_due counts from
    the last payment and default is declared at 90 (ADR-005), so the crossing is
    days_past_due - 90 days before the reporting date.
    """
    return F.when(
        F.col("status") == "defaulted",
        F.date_sub(as_of, F.col("days_past_due") - DEFAULT_AT_DPD),
    )


def build(df: DataFrame, reporting_date: str) -> DataFrame:
    as_of = F.lit(reporting_date).cast("date")

    return in_scope(df).select(
        as_of.alias("reporting_date"),
        "exposure_id",
        "customer_id",
        # This book is unsecured consumer term lending throughout, so these are
        # constant. The engine requires them because it serves several books.
        F.lit(EXPOSURE_CLASS).alias("exposure_class"),
        "original_amount",
        "outstanding_amount",
        F.lit(0).cast("decimal(18,2)").alias("undrawn_amount"),
        F.lit(CURRENCY).alias("currency"),
        "rating_grade",
        "rating_subgrade",
        "origination_date",
        "maturity_date",
        "term_months",
        "interest_rate",
        "status",
        "days_past_due",
        "default_flag",
        default_date(as_of).alias("default_date"),
        "provision_amount",
        F.lit(False).alias("collateral_flag"),
        F.lit(0).cast("decimal(18,2)").alias("collateral_value"),
        "purpose",
        "region",
    )
