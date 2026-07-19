"""Stand-in for the third-party RWA engine.

This is not the real calculation. The bank's RWA is produced by a vendor application;
what this platform owns is the data on either side of it: a validated input to the
engine's contract, and a reconciled, reportable output.

This mock stands at that boundary so the integration is exercised end to end. The risk
weights are illustrative placeholders under the Basel standardised approach for retail
exposures, not the vendor's model. Everything the engine consumes and returns is a
contract; the numbers between are the vendor's to produce.
"""
from pyspark.sql import DataFrame, functions as F

# Illustrative standardised-approach weights for retail, keyed by rating grade.
# Placeholders for the vendor's model, not a reproduction of it.
RISK_WEIGHT = {
    "A": 0.20, "B": 0.35, "C": 0.55, "D": 0.75,
    "E": 1.00, "F": 1.25, "G": 1.50,
}
DEFAULTED_WEIGHT = 1.50  # defaulted exposures, net of provisions
CAPITAL_RATIO = 0.08     # 8% minimum


def _weight() -> F.Column:
    mapping = F.create_map(*[x for kv in RISK_WEIGHT.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    return (
        F.when(F.col("status") == "defaulted", F.lit(DEFAULTED_WEIGHT))
        .otherwise(mapping[F.col("rating_grade")])
    )


def run_engine(engine_input: DataFrame) -> DataFrame:
    """Produce exposure-level RWA and capital requirement for one reporting date.

    EAD is the outstanding balance, net of specific provisions for defaulted
    exposures. RWA is EAD times the risk weight; capital is RWA times the minimum
    ratio.
    """
    ead = F.when(
        F.col("status") == "defaulted",
        F.greatest(F.col("outstanding_amount") - F.col("provision_amount"), F.lit(0)),
    ).otherwise(F.col("outstanding_amount"))

    return engine_input.select(
        "reporting_date",
        "exposure_id",
        "customer_id",
        "rating_grade",
        "status",
        ead.cast("decimal(18,2)").alias("ead"),
        _weight().alias("risk_weight"),
        (ead * _weight()).cast("decimal(18,2)").alias("rwa"),
        (ead * _weight() * F.lit(CAPITAL_RATIO)).cast("decimal(18,2)").alias("capital_required"),
    )
