"""Synthetic red-team scenarios for detection coverage.

Two fraud typologies that exist in the real world but not in PaySim, whose accounts do
not repeat: structuring (many just-under-threshold transfers) and velocity bursts (many
transfers in a short window). These are generated as a small, clearly-labelled test set,
kept entirely separate from the real PaySim stream (ADR-010). They test whether the
rules catch the typologies, not how much real fraud is caught — that is measured on
PaySim's real label.

Every generated row is labelled scenario so a rule's hits can be checked against the
intended pattern.
"""
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession

SCHEMA = ("step int, type string, amount decimal(18,2), name_orig string, "
          "name_dest string, old_balance_orig decimal(18,2), "
          "new_balance_orig decimal(18,2), is_fraud int, scenario string")


def _row(step, typ, amount, orig, dest, old_bal, scenario):
    return (step, typ, Decimal(amount), orig, dest, Decimal(old_bal),
            Decimal(old_bal - amount), 1, scenario)


def structuring_rows():
    """One account, four transfers each just under 200k within a day, summing above it."""
    rows = []
    bal = 1_000_000
    for i in range(4):
        rows.append(_row(step=10 + i, typ="TRANSFER", amount=190_000,
                         orig="STRUCT-1", dest=f"MULE-{i}", old_bal=bal,
                         scenario="structuring"))
        bal -= 190_000
    return rows


def velocity_burst_rows():
    """One account, six transfers in two hours — a burst PaySim never contains."""
    rows = []
    bal = 3_000_000
    for i in range(6):
        rows.append(_row(step=100, typ="TRANSFER", amount=50_000,
                         orig="BURST-1", dest=f"DEST-{i}", old_bal=bal,
                         scenario="velocity_burst"))
        bal -= 50_000
    return rows


def build(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(structuring_rows() + velocity_burst_rows(), SCHEMA)


if __name__ == "__main__":
    from dataplatform.lakehouse.session import local_session

    spark = local_session("red_team_build")
    df = build(spark)
    df.show(truncate=False)
    print(f"scenarios: {df.select('scenario').distinct().count()}, rows: {df.count()}")
    spark.stop()
