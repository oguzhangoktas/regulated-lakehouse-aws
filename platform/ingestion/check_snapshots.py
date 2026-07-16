"""Compare the generated book against expected portfolio behaviour.

Checks the month-end series a real book would be judged on: balance growth, the
performing/past due/defaulted split, NPL ratio and provisions. A book whose
defaulted population only ever grows indicates the exit path is not firing.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from simulate_ods_snapshots import build_snapshot  # noqa: E402


def check(master_path: str, start: str, end: str) -> None:
    master = pd.read_parquet(master_path)
    print(f"{'month_end':12s} {'rows':>10s} {'balance':>9s} {'perf':>10s} "
          f"{'past_due':>9s} {'default':>8s} {'NPL%':>6s} {'prov':>8s}")
    for snap in pd.date_range(start, end, freq="ME"):
        df = build_snapshot(master, snap)
        live = df[df["outstanding_amount"] > 0]
        d = df["status"] == "defaulted"
        npl = (df.loc[d, "outstanding_amount"].sum()
               / max(live["outstanding_amount"].sum(), 1) * 100)
        print(f"{str(snap.date()):12s} {len(df):>10,} "
              f"{live['outstanding_amount'].sum() / 1e9:>8.2f}bn "
              f"{(df['status'] == 'performing').sum():>10,} "
              f"{(df['status'] == 'past_due').sum():>9,} {d.sum():>8,} "
              f"{npl:>5.2f}% {df['provision_amount'].sum() / 1e6:>7.0f}m")


if __name__ == "__main__":
    check(sys.argv[1], sys.argv[2], sys.argv[3])
