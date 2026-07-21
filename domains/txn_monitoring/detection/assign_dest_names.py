"""Attach beneficiary names to destination accounts for screening.

PaySim destinations are opaque ids. Real screening runs on the beneficiary name in
the payment message, so a name is attached to each destination here, standing in for
that field. Most destinations get an ordinary random name; a controlled few get a
near-variant of a watchlisted name — a spelling or transliteration slip — so the
fuzzy matcher has genuine near-misses to resolve rather than only exact hits.

Usage:
  python -m domains.txn_monitoring.detection.assign_dest_names
"""
import random


from dataplatform.lakehouse.session import local_session

SILVER = "lakehouse.silver_txn_monitoring.transactions"
WATCHLIST = "lakehouse.gold_txn_monitoring.watchlist"
DEST_NAMES = "lakehouse.gold_txn_monitoring.dest_names"

ORDINARY_FIRST = ["James", "Maria", "Wei", "Anna", "David", "Sofia", "Liam", "Elena"]
ORDINARY_LAST = ["Johnson", "Garcia", "Muller", "Rossi", "Brown", "Novak", "Silva"]


def vary(name: str, rng: random.Random) -> str:
    """A small spelling/transliteration slip, the kind screening must still catch."""
    ops = [
        lambda s: s.replace("i", "y", 1),
        lambda s: s.replace("o", "0" if False else "u", 1),
        lambda s: s[:-1] if len(s) > 4 else s,        # drop last char
        lambda s: s.replace(" ", "  "),               # double space
        lambda s: s.replace("ss", "s"),
    ]
    return rng.choice(ops)(name)


def run() -> None:
    spark = local_session("assign_dest_names")
    rng = random.Random(7)

    listed = [r["name"] for r in spark.read.table(WATCHLIST).collect()]

    dests = [r["name_dest"] for r in
             spark.read.table(SILVER).select("name_dest").distinct().limit(5000).collect()]

    rows = []
    for d in dests:
        if rng.random() < 0.02:  # ~2% get a near-variant of a listed name
            rows.append((d, vary(rng.choice(listed), rng), True))
        else:
            rows.append((d, f"{rng.choice(ORDINARY_FIRST)} {rng.choice(ORDINARY_LAST)}", False))

    df = spark.createDataFrame(rows, "name_dest string, dest_name string, seeded_hit boolean")
    df.writeTo(DEST_NAMES).using("iceberg").createOrReplace()

    seeded = df.filter("seeded_hit").count()
    print(f"dest names: {df.count():,}  seeded near-hits: {seeded}")
    df.filter("seeded_hit").show(8, truncate=False)
    spark.stop()


if __name__ == "__main__":
    run()
