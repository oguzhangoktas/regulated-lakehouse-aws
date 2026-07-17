"""Generate daily full snapshots of the credit exposure table.

The source system (ADR-002) lands a full copy of the T-1 portfolio every day,
append-only. The public dataset is a single extract with one row per loan and its
final observed state, so the daily dimension does not exist in it and is derived
here.

Derived vs. sourced:
  sourced   loan attributes at origination, final loan_status, last_pymnt_d,
            recoveries
  derived   outstanding balance, days past due, status and provisions at a given
            snapshot date

Lifecycle:
  performing -> past_due (1-89 dpd) -> defaulted (90+ dpd) -> written_off (180+ dpd)
  settled loans move to closed at their final payment
  written_off and closed exposures stay on the feed for RETENTION_DAYS at zero
  balance before dropping out, matching the source system's retention

Balance amortises on the standard annuity schedule and stops accruing once
payments stop. Provisions are booked against defaulted exposures net of the
recoveries recorded in the source.

Source status values are wider than the engine contract accepts; the mapping and
scope filter are applied downstream (ADR-003).
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_AT_DPD = 90
WRITE_OFF_AT_DPD = 180
RETENTION_DAYS = 30

DEFAULTED_FINAL = {"Charged Off", "Default",
                   "Does not meet the credit policy. Status:Charged Off"}
SETTLED_FINAL = {"Fully Paid", "Does not meet the credit policy. Status:Fully Paid"}


def months_between(start: pd.Series, end: pd.Timestamp) -> np.ndarray:
    return (end.year - start.dt.year) * 12 + (end.month - start.dt.month)


def amortised_balance(principal, annual_rate_pct, term_months, m_elapsed):
    """Remaining principal on an annuity schedule after m_elapsed payments."""
    r = (annual_rate_pct / 100.0) / 12.0
    n = term_months.astype("float64")
    m = np.clip(m_elapsed, 0, n)
    with np.errstate(divide="ignore", invalid="ignore"):
        num = np.power(1 + r, n) - np.power(1 + r, m)
        den = np.power(1 + r, n) - 1
        # den == 0 when the rate is zero; fall back to straight-line
        bal = np.where(den == 0, principal * (1 - m / np.maximum(n, 1)),
                       principal * num / den)
    return np.maximum(np.nan_to_num(bal), 0.0)


def build_snapshot(master: pd.DataFrame, snap: pd.Timestamp) -> pd.DataFrame:
    df = master[master["issue_d"] <= snap].copy()
    if df.empty:
        return df

    m = months_between(df["issue_d"], snap)
    paid_m = months_between(df["last_pymnt_d"].fillna(df["issue_d"]), snap)
    months_paid = np.maximum(m - paid_m, 0)

    def_final = df["loan_status"].isin(DEFAULTED_FINAL).to_numpy()
    set_final = df["loan_status"].isin(SETTLED_FINAL).to_numpy()
    stopped = m > months_paid

    stop_at = np.where(def_final | set_final, months_paid, m)
    df["outstanding_amount"] = amortised_balance(
        df["loan_amnt"].to_numpy(),
        df["int_rate"].to_numpy(),
        df["term_months"].astype("float64").to_numpy(),
        np.minimum(m, stop_at),
    )

    days_since_pymnt = (snap - df["last_pymnt_d"]).dt.days.fillna(0).to_numpy()
    dpd = np.where(def_final & stopped, np.maximum(days_since_pymnt, 0), 0)

    written_off = dpd >= WRITE_OFF_AT_DPD
    closed = set_final & stopped

    status = np.full(len(df), "performing", dtype=object)
    status = np.where(dpd > 0, "past_due", status)
    status = np.where(dpd >= DEFAULT_AT_DPD, "defaulted", status)
    status = np.where(written_off, "written_off", status)
    status = np.where(closed, "closed", status)

    df["status"] = status
    df["days_past_due"] = dpd.astype("int32")
    df["default_flag"] = df["status"] == "defaulted"

    exited = written_off | closed
    df.loc[exited, "outstanding_amount"] = 0.0

    gross = df["outstanding_amount"].to_numpy()
    recov = df["recoveries"].fillna(0).to_numpy()
    df["provision_amount"] = np.where(
        df["status"].to_numpy() == "defaulted",
        np.maximum(gross - recov, 0.0),
        0.0,
    )

    days_since_exit = np.where(
        written_off, dpd - WRITE_OFF_AT_DPD,
        np.where(closed, days_since_pymnt, -1),
    )
    df = df[~exited | (days_since_exit <= RETENTION_DAYS)]

    df["snapshot_date"] = snap.date().isoformat()
    return df


def run(master_path: str, out_dir: str, start: str, end: str,
        freq: str = "D") -> None:
    master = pd.read_parquet(master_path)
    out = Path(out_dir) / "credit_exposure_snapshot"
    for snap in pd.date_range(start, end, freq=freq):
        s = build_snapshot(master, snap)
        part = out / f"snapshot_date={snap.date().isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        s.drop(columns=["snapshot_date"]).to_parquet(
            part / "part-0.parquet", index=False, compression="snappy")
        print(f"{snap.date()}  rows={len(s):>9,}  "
              f"balance={s['outstanding_amount'].sum() / 1e9:6.2f}bn")


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4],
        sys.argv[5] if len(sys.argv) > 5 else "D")
