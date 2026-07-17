"""Profile a CSV source: dtypes, null rates, cardinality.

Reads a bounded sample rather than the full file; sources here run to several GB.
Null rates from a sample are indicative only when the file is unordered.
"""
import sys

import pandas as pd


def profile(path: str, n: int = 5000) -> None:
    df = pd.read_csv(path, nrows=n, low_memory=False)
    print(f"\n{path}")
    print(f"rows sampled: {len(df):,}   columns: {df.shape[1]}\n")
    for col in df.columns:
        print(f"{col:35s} | {str(df[col].dtype):10s} | "
              f"non-null {df[col].notna().mean() * 100:5.1f}% | "
              f"distinct {df[col].nunique(dropna=True)}")


if __name__ == "__main__":
    profile(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 5000)
