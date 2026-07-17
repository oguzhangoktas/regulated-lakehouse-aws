"""Build the loan master extract: raw CSV to Parquet.

The snapshot simulator reads this once per snapshot date, so the 1.6GB source is
reduced up front to the columns that matter.

  static  - set at origination, constant over the life of the loan
  outcome - final observed state; drives the derived trajectory

dtype: chunked reads infer types per chunk, so `id` returns int64 in some chunks
and object in others (the file carries trailing summary rows). Enforced as string
and validated against a numeric pattern; rejected rows are counted, not dropped
silently.

PII: zip_code, emp_title, url, title and desc are not carried. customer_id is a
hash of the loan id.
"""
import hashlib
import sys

import pandas as pd

STATIC = [
    "id", "issue_d", "term", "loan_amnt", "funded_amnt", "int_rate", "installment",
    "grade", "sub_grade", "purpose", "addr_state", "annual_inc", "dti",
    "application_type", "home_ownership", "emp_length", "verification_status",
    "fico_range_low", "fico_range_high",
]
OUTCOME = [
    "loan_status", "out_prncp", "total_pymnt", "total_rec_prncp",
    "last_pymnt_d", "recoveries",
]


def pseudonymise(loan_id: str) -> str:
    return hashlib.sha256(f"lc-{loan_id}".encode()).hexdigest()[:16]


def build(raw_csv: str, out_parquet: str, chunksize: int = 250_000) -> None:
    parts, total, rejected = [], 0, 0

    for chunk in pd.read_csv(
        raw_csv,
        usecols=STATIC + OUTCOME,
        chunksize=chunksize,
        low_memory=False,
        dtype={"id": "string"},          # enforce at the boundary
    ):
        total += len(chunk)
        # Reject rows whose natural key isn't a clean integer (footer junk, blanks)
        valid = chunk["id"].str.fullmatch(r"\d+").fillna(False)
        rejected += int((~valid).sum())
        parts.append(chunk[valid])
        print(f"  ...read {total:,} rows", end="\r")

    df = pd.concat(parts, ignore_index=True)
    print(f"\nRead {total:,} rows | rejected {rejected:,} non-conforming | kept {len(df):,}")

    df["issue_d"] = pd.to_datetime(df["issue_d"], format="%b-%Y", errors="coerce")
    df["last_pymnt_d"] = pd.to_datetime(df["last_pymnt_d"], format="%b-%Y", errors="coerce")
    df["term_months"] = df["term"].str.extract(r"(\d+)").astype("Int16")
    df = df.drop(columns=["term"])

    df["customer_id"] = df["id"].map(pseudonymise)
    df["exposure_id"] = "LC-" + df["id"]

    no_issue_date = int(df["issue_d"].isna().sum())
    if no_issue_date:
        print(f"  dropping {no_issue_date:,} rows with unparseable issue_d")
    df = df[df["issue_d"].notna()]

    df.to_parquet(out_parquet, index=False, compression="snappy")
    print(f"Wrote {len(df):,} loans -> {out_parquet}")


if __name__ == "__main__":
    build(sys.argv[1], sys.argv[2])
