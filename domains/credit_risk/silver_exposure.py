"""Conform the credit exposure snapshot from source semantics to platform semantics.

The source table carries the lender's own vocabulary and column names, and holds
money as floating point. Silver applies the platform's names and types, derives
the fields the source does not carry, and separates records that fail validation.

Scope is not applied here. Silver holds the whole book; the engine takes a subset
of it (ADR-003).
"""
from pyspark.sql import Column, DataFrame, functions as F

# Money is cast to decimal rather than kept as float. Binary floating point cannot
# represent most decimal fractions exactly, and the error accumulates when summing
# a book of this size into a reported figure.
MONEY = "decimal(18,2)"

VALID_GRADES = ["A", "B", "C", "D", "E", "F", "G"]


def rules() -> list[tuple[str, Column]]:
    """Conditions a record must satisfy to reach silver.

    Built on call rather than held as a module constant: a Column is bound to the
    active session, so constructing one at import time fails before a session exists.

    Records failing any rule go to quarantine carrying the rule names, rather than
    being corrected in place.
    """
    return [
        ("exposure_id_present", F.col("exposure_id").isNotNull()),
        ("customer_id_present", F.col("customer_id").isNotNull()),
        ("original_amount_positive", F.col("original_amount") > 0),
        ("outstanding_not_negative", F.col("outstanding_amount") >= 0),
        # Tolerance covers capitalised interest and fees added after origination.
        (
            "outstanding_within_original",
            F.col("outstanding_amount") <= F.col("original_amount") * F.lit(1.1),
        ),
        ("days_past_due_not_negative", F.col("days_past_due") >= 0),
        (
            "default_flag_matches_status",
            (F.col("status") == F.lit("defaulted")) == F.col("default_flag"),
        ),
        (
            "past_due_has_days_past_due",
            (F.col("status") != F.lit("past_due")) | (F.col("days_past_due") > 0),
        ),
        (
            "provision_within_outstanding",
            F.col("provision_amount") <= F.col("outstanding_amount"),
        ),
        ("rating_grade_known", F.col("rating_grade").isin(VALID_GRADES)),
        ("interest_rate_plausible", F.col("interest_rate").between(0, 100)),
    ]


def conform(df: DataFrame) -> DataFrame:
    """Map source columns onto the platform schema."""
    return df.select(
        F.col("exposure_id"),
        F.col("customer_id"),
        F.col("issue_d").cast("date").alias("origination_date"),
        F.col("term_months").cast("smallint").alias("term_months"),
        # The source records a term, not an end date.
        F.expr("add_months(issue_d, term_months)").cast("date").alias("maturity_date"),
        F.col("loan_amnt").cast(MONEY).alias("original_amount"),
        F.col("outstanding_amount").cast(MONEY).alias("outstanding_amount"),
        F.col("provision_amount").cast(MONEY).alias("provision_amount"),
        F.col("int_rate").cast("decimal(6,4)").alias("interest_rate"),
        F.col("grade").alias("rating_grade"),
        F.col("sub_grade").alias("rating_subgrade"),
        F.col("status"),
        F.col("days_past_due").cast("int").alias("days_past_due"),
        F.col("default_flag").cast("boolean").alias("default_flag"),
        F.col("purpose"),
        F.col("addr_state").alias("region"),
        # Attributes the book is reported on, carried through unchanged.
        F.col("annual_inc").cast(MONEY).alias("annual_income"),
        F.col("dti").cast("decimal(8,4)").alias("debt_to_income"),
        F.col("fico_range_low").cast("smallint").alias("fico_low"),
        F.col("fico_range_high").cast("smallint").alias("fico_high"),
        F.col("home_ownership"),
        F.col("emp_length"),
        F.col("verification_status"),
        F.col("application_type"),
    )


def validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split conformed records into those that pass every rule and those that do not."""
    failed = F.array_compact(
        F.array(*[F.when(~cond, F.lit(name)) for name, cond in rules()])
    )
    tagged = df.withColumn("failed_rules", failed)

    passed = tagged.filter(F.size("failed_rules") == 0).drop("failed_rules")
    quarantined = tagged.filter(F.size("failed_rules") > 0)
    return passed, quarantined
