"""Scan the full transaction log for the facts that shape the transaction-monitoring
domain: fraud concentration by type, amount distribution, the balance-drain
signature, and the time span.

The generic profiler samples; these questions need the whole file, so this reads it
in chunks and aggregates.
"""
import sys

import pandas as pd

TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]


def scan(path: str, chunksize: int = 500_000) -> None:
    n = 0
    fraud = 0
    flagged = 0
    step_min, step_max = None, None
    by_type = {t: {"count": 0, "fraud": 0, "amount_sum": 0.0} for t in TYPES}
    fraud_amounts, legit_amounts = [], []
    # A known PaySim signature: fraudulent transfers drain the origin account exactly,
    # so oldbalanceOrg - amount == newbalanceOrig == 0. Count how often this holds.
    drain_fraud, drain_legit = 0, 0

    for chunk in pd.read_csv(path, chunksize=chunksize):
        n += len(chunk)
        fraud += int(chunk["isFraud"].sum())
        flagged += int(chunk["isFlaggedFraud"].sum())

        lo, hi = chunk["step"].min(), chunk["step"].max()
        step_min = lo if step_min is None else min(step_min, lo)
        step_max = hi if step_max is None else max(step_max, hi)

        for t in TYPES:
            sub = chunk[chunk["type"] == t]
            by_type[t]["count"] += len(sub)
            by_type[t]["fraud"] += int(sub["isFraud"].sum())
            by_type[t]["amount_sum"] += float(sub["amount"].sum())

        is_fraud = chunk["isFraud"] == 1
        fraud_amounts.append(chunk.loc[is_fraud, "amount"])
        legit_amounts.append(chunk.loc[~is_fraud, "amount"].sample(frac=0.02, random_state=1))

        drained = (chunk["newbalanceOrig"] == 0) & (chunk["oldbalanceOrg"] > 0)
        drain_fraud += int((drained & is_fraud).sum())
        drain_legit += int((drained & ~is_fraud).sum())

    fa = pd.concat(fraud_amounts)
    la = pd.concat(legit_amounts)

    print(f"\nrows: {n:,}   time span: step {step_min}..{step_max} "
          f"(~{(step_max - step_min + 1) / 24:.0f} days hourly)")
    print(f"fraud: {fraud:,} ({fraud / n * 100:.3f}%)   "
          f"flagged by source rule: {flagged:,} ({flagged / max(fraud,1) * 100:.2f}% of fraud)\n")

    print(f"{'type':10s} {'count':>12s} {'fraud':>8s} {'fraud %':>9s} {'avg amount':>14s}")
    for t in TYPES:
        d = by_type[t]
        c, f = d["count"], d["fraud"]
        avg = d["amount_sum"] / c if c else 0
        print(f"{t:10s} {c:>12,} {f:>8,} {f / c * 100 if c else 0:>8.3f}% {avg:>14,.0f}")

    print(f"\namount  fraud   median {fa.median():>12,.0f}   mean {fa.mean():>12,.0f}   max {fa.max():>14,.0f}")
    print(f"amount  legit   median {la.median():>12,.0f}   mean {la.mean():>12,.0f}   max {la.max():>14,.0f}")

    print("\nbalance-drain signature (origin emptied):")
    print(f"  among fraud:  {drain_fraud:,} / {fraud:,} ({drain_fraud / max(fraud,1) * 100:.1f}%)")
    print(f"  among legit:  {drain_legit:,}")


if __name__ == "__main__":
    scan(sys.argv[1])
