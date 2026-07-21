"""Screen transaction destinations against the watchlist by fuzzy name match.

Real sanctions screening never relies on exact names: transliteration, spelling and
word order all vary, so a party is matched by similarity, not equality. Jaro-Winkler
scores the similarity and weights matching prefixes, which suits names.

A destination scoring above the threshold against any listed entity raises a
screening alert carrying the matched entity, its program, and the score, so an
analyst can adjudicate. The threshold trades recall against false positives; it is
set high enough that only close matches surface.

This is the unstructured side of detection: the signal is in free-text names, not in
numeric fields.

Usage:
  python -m domains.txn_monitoring.detection.watchlist_screen [--threshold 0.90]
"""
import argparse

import jellyfish
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from dataplatform.lakehouse.session import local_session

SILVER = "lakehouse.silver_txn_monitoring.transactions"
WATCHLIST = "lakehouse.gold_txn_monitoring.watchlist"
SCREEN_ALERTS = "lakehouse.gold_txn_monitoring.screening_alerts"

# PaySim destinations are opaque ids, so a name is derived deterministically from the
# id for screening. In production the destination name comes from the payment message.
MATCH = StructType([
    StructField("entity_id", StringType()),
    StructField("program", StringType()),
    StructField("score", FloatType()),
])


def best_match_udf(watchlist_rows):
    """A UDF that returns the closest watchlist entity for a name, if above cutoff.

    The watchlist is small, so it is broadcast into the UDF and every destination is
    compared against all entries. For a large list this becomes a blocking or indexing
    problem, out of scope here.
    """
    listed = [(r["entity_id"], r["name"], r["program"]) for r in watchlist_rows]

    def match(name):
        if not name:
            return None
        best = None
        for entity_id, listed_name, program in listed:
            score = jellyfish.jaro_winkler_similarity(name.lower(), listed_name.lower())
            if best is None or score > best[2]:
                best = (entity_id, program, score)
        return best

    return F.udf(match, MATCH)


def screen(spark: SparkSession, threshold: float) -> DataFrame:
    watchlist = spark.read.table(WATCHLIST).collect()
    match = best_match_udf(watchlist)

    # Screen on the beneficiary name attached to each destination. seeded_hit is the
    # ground truth: destinations deliberately given a near-variant of a listed name.
    names = spark.read.table("lakehouse.gold_txn_monitoring.dest_names")

    scored = names.withColumn("m", match(F.col("dest_name")))
    return (
        scored.filter(F.col("m.score") >= threshold)
        .select(
            "name_dest", "dest_name", "seeded_hit",
            F.col("m.entity_id").alias("watchlist_entity"),
            F.col("m.program").alias("program"),
            F.round("m.score", 3).alias("match_score"),
        )
    )


def run(threshold: float) -> int:
    spark = local_session("watchlist_screen")
    alerts = screen(spark, threshold)
    alerts.writeTo(SCREEN_ALERTS).using("iceberg").createOrReplace()
    n = alerts.count()
    print(f"screening alerts (threshold {threshold}): {n:,}")
    alerts.orderBy(F.desc("match_score")).show(10, truncate=False)
    spark.stop()
    return n


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=float, default=0.90)
    args = parser.parse_args()
    run(args.threshold)
