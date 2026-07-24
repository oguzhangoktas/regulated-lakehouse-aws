# Runbook: table maintenance

Checking what a table's history costs, rolling back a write that should not have
happened, and reclaiming storage when the ratio justifies it.

None of this runs on a schedule. The current position is that history is retained without
expiry because the cost is a few cents a month (ADR-016), so these are operations you
perform deliberately rather than procedures that fire on their own.

---

## What is this table carrying

```bash
python -m dataplatform.lakehouse.maintenance \
  --table lakehouse.silver_credit_risk.exposure --s3
```

```
current    31 snapshots    12 live files      310 MB  history holds      486 MB
would expire 0 snapshot(s) older than 30 days, keeping at least 5
```

Reports and changes nothing. The number to watch is the last one against the one before
it: history holding half again as much as the live data is unremarkable at 300 MB and
would not be at 300 TB.

If the ratio looks wrong, the usual cause is write frequency rather than data volume. A
table with two or three snapshots per partition has been rewritten during development; a
table with hundreds is being written on a short cycle, and that is the condition where
both expiry and compaction stop being optional.

---

## A bad write published — roll it back

Rollback moves the table's current state to an earlier snapshot. The data files are still
there, so it is fast and does not reprocess anything.

**Find the snapshot to return to:**

```sql
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added
FROM lakehouse.silver_credit_risk.exposure.snapshots
ORDER BY committed_at DESC
LIMIT 10
```

**Check it before committing to it.** Reading at a snapshot changes nothing:

```python
spark.read.option("snapshot-id", <id>).table("lakehouse.silver_credit_risk.exposure").count()
```

**Roll back:**

```sql
CALL lakehouse.system.rollback_to_snapshot(
  table => 'silver_credit_risk.exposure',
  snapshot_id => <id>
)
```

**What this does and does not do.** It changes what the table returns now. It does not
delete the snapshot you rolled back from, so rolling forward again is a second rollback
to that id — the mistake is recoverable while its snapshot survives, which is the reason
this runbook is short on urgency and the reason expiry is treated carefully below.

**When to reprocess instead.** If the fault is upstream rather than in the write —
wrong source data rather than wrong logic — rolling back restores a state that is also
wrong. Fix the source and rerun the date; the job replaces the partition, so the result
converges either way.

---

## Reclaiming storage

Only do this when the ratio justifies it, and understand what is being traded.

**What expiry removes is the ability to do everything above.** The storage reclaimed and
the rollback points lost are the same thing seen from either side. Measured on a scratch
table: three writes, then expiry retaining one snapshot, and a read at the first snapshot
that had returned 3,000 rows failed with `Cannot find snapshot with ID`.

**Check first:**

```bash
python -m dataplatform.lakehouse.maintenance --table <table> --s3
```

**Then, if the number justifies it:**

```bash
python -m dataplatform.lakehouse.maintenance --table <table> --s3 --apply
```

The default retention keeps thirty days and at least five snapshots. Thirty days matches
the object versioning policy and covers a monthly reporting cycle, which is roughly how
long a bad write can go unnoticed. The floor of five keeps a rarely written table from
ageing out of its own history.

**The trap.** The S3 lifecycle rule does not do this for you. It expires noncurrent
object versions, and Iceberg never overwrites a data file — it writes new ones and
repoints the metadata, so superseded files are current versions of distinct objects that
the rule never sees. Measured on the silver prefix: 836 MB of objects against 310 MB of
live data. Nothing but snapshot expiry reclaims the difference.

---

## Many small files

```bash
python -m dataplatform.lakehouse.maintenance --table <table> --s3 --apply --compact
```

Rewrites files towards 128 MB. Not needed on this platform today — live files average
25 MB across twelve partitions — and worth knowing why: small files come from frequent
small writes, and these tables are written infrequently and in bulk. A full topic
consumed in `availableNow` mode is one large batch, not thousands of micro-batches.

The symptom, when it appears, is query time growing while data volume does not: hundreds
of file opens per scan, and metadata that takes longer to plan than the data takes to
read.

Compaction rewrites data, so it creates a snapshot of its own and temporarily increases
storage before expiry reclaims the originals. Run it when there is room for both.
