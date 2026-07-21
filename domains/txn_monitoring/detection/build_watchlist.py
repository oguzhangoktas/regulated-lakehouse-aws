"""Build a synthetic sanctions-style watchlist for destination screening.

Real sanctions lists (OFAC and equivalents) are licensed, so this generates a
synthetic list with the shape of a real one: an entity id, a canonical name, and a
program (why the party is listed). PaySim uses opaque account ids, not names, so a
name is attached to a sample of destination accounts to give the fuzzy matcher
something to screen against.

The list is deliberately small; screening is about matching quality, not volume.

Usage:
  python -m domains.txn_monitoring.detection.build_watchlist
"""
import random


from dataplatform.lakehouse.session import local_session

SILVER = "lakehouse.silver_txn_monitoring.transactions"
WATCHLIST = "lakehouse.gold_txn_monitoring.watchlist"

# A synthetic name pool. Real screening deals with transliteration and spelling
# variants; these give the matcher realistic near-misses to resolve.
FIRST = ["Mohammed", "Viktor", "Chen", "Dmitri", "Ahmed", "Sergei", "Ali", "Ivan",
         "Omar", "Nikolai", "Hassan", "Yuri", "Karim", "Boris", "Tariq"]
LAST = ["Al-Rashid", "Petrov", "Wang", "Volkov", "Hussein", "Ivanov", "Khan",
        "Sokolov", "Nazarov", "Kuznetsov", "Rahman", "Morozov", "Aziz", "Popov"]
PROGRAMS = ["SANCTIONS-A", "SANCTIONS-B", "PEP", "TERROR-FINANCE"]


def run() -> None:
    spark = local_session("build_watchlist")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold_txn_monitoring")

    rng = random.Random(42)
    entities = []
    for i in range(40):
        name = f"{rng.choice(FIRST)} {rng.choice(LAST)}"
        entities.append((f"WL-{i:04d}", name, rng.choice(PROGRAMS)))

    watchlist = spark.createDataFrame(entities, "entity_id string, name string, program string")
    watchlist.writeTo(WATCHLIST).using("iceberg").createOrReplace()

    print(f"watchlist entities: {watchlist.count()}")
    watchlist.show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    run()
