"""Load ODS snapshots into the bronze layer.

Bronze holds the source extract unchanged; provenance is tracked in run metadata
rather than by adding columns to the landed files.

Loading a snapshot_date replaces that partition. The source re-lands a full copy
of T-1 (ADR-002), so a rerun of the same date must not accumulate rows.

Partitions whose objects already match the local files on name and size are
skipped, so an interrupted run resumes rather than re-transferring what landed.

Usage:
  python platform/ingestion/load_bronze.py <local_dir> <bucket> <dataset>
      [--dry-run] [--force] [--only snapshot_date=YYYY-MM-DD]
"""
import sys
from pathlib import Path

import boto3
from botocore.config import Config

s3 = boto3.client("s3", config=Config(retries={"max_attempts": 10, "mode": "adaptive"}))


def remote_objects(bucket: str, prefix: str) -> dict:
    paginator = s3.get_paginator("list_objects_v2")
    return {
        obj["Key"].rsplit("/", 1)[-1]: obj["Size"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    }


def delete_prefix(bucket: str, prefix: str) -> int:
    keys = [{"Key": f"{prefix}{name}"} for name in remote_objects(bucket, prefix)]
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys[i:i + 1000]})
    return len(keys)


def load(local_dir: str, bucket: str, dataset: str, dry_run: bool = False,
         force: bool = False, only: str | None = None) -> None:
    root = Path(local_dir) / dataset
    partitions = sorted(p for p in root.iterdir() if p.is_dir())
    if only:
        partitions = [p for p in partitions if p.name == only]
    if not partitions:
        raise SystemExit(f"no partitions under {root}")

    uploaded = skipped = 0
    sent_bytes = 0

    for part in partitions:
        prefix = f"{dataset}/{part.name}/"
        files = sorted(part.glob("*.parquet"))
        local = {f.name: f.stat().st_size for f in files}
        size = sum(local.values())

        if not force and remote_objects(bucket, prefix) == local:
            skipped += 1
            print(f"{part.name}  {size / 1e6:6.1f}MB  present")
            continue

        if dry_run:
            print(f"{part.name}  {size / 1e6:6.1f}MB  would upload")
            continue

        replaced = delete_prefix(bucket, prefix)
        for f in files:
            s3.upload_file(str(f), bucket, prefix + f.name)

        uploaded += 1
        sent_bytes += size
        print(f"{part.name}  {size / 1e6:6.1f}MB  {'replaced' if replaced else 'new'}")

    if not dry_run:
        print(f"\n{uploaded} uploaded ({sent_bytes / 1e9:.2f}GB), {skipped} already present")


if __name__ == "__main__":
    only = sys.argv[sys.argv.index("--only") + 1] if "--only" in sys.argv else None
    load(sys.argv[1], sys.argv[2], sys.argv[3],
         "--dry-run" in sys.argv, "--force" in sys.argv, only)
