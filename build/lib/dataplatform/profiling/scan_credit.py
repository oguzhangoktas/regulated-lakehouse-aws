"""Full scan of the columns needed for exposure modelling.

Sampling is unreliable for distributions here: the accepted-loans file is ordered
by issue date, so a head sample only covers the earliest vintages. Chunked full
scan instead, restricted to the relevant columns to keep memory bounded.
"""
import sys
from collections import Counter

import pandas as pd

KEY_COLS = [
    "id", "issue_d", "grade", "sub_grade", "loan_status",
    "loan_amnt", "funded_amnt", "out_prncp", "int_rate", "term",
    "purpose", "addr_state", "annual_inc", "dti", "application_type",
]
CATEGORICAL = ["grade", "loan_status", "term", "purpose", "application_type"]


def scan(path: str, chunksize: int = 250_000) -> None:
    total = 0
    nulls = Counter()
    cats = {c: Counter() for c in CATEGORICAL}
    years = Counter()

    for chunk in pd.read_csv(path, usecols=KEY_COLS, chunksize=chunksize,
                             low_memory=False):
        total += len(chunk)
        for c in KEY_COLS:
            nulls[c] += int(chunk[c].isna().sum())
        for c in CATEGORICAL:
            cats[c].update(chunk[c].dropna().astype(str))
        years.update(chunk["issue_d"].dropna().astype(str).str[-4:])
        print(f"  scanned {total:,}", end="\r")

    print(f"\n\n{total:,} rows\n")
    print("null rate")
    for c in KEY_COLS:
        print(f"  {c:20s} {nulls[c] / total * 100:6.2f}%")
    for c in ("grade", "loan_status"):
        print(f"\n{c}")
        for k, n in sorted(cats[c].items()):
            print(f"  {k:45s} {n:>9,}  {n / total * 100:5.2f}%")
    print("\nrows per issue year")
    for y, n in sorted(years.items()):
        print(f"  {y}: {n:>9,}")


if __name__ == "__main__":
    scan(sys.argv[1])
