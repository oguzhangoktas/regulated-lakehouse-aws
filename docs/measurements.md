# Measurements

Numbers taken from the platform as built. Athena bills on bytes scanned.

## Storage

Source extract to loan master: 1.6GB CSV to 127MB Parquet. Column pruning (151 to 27),
columnar layout and snappy together account for it; the format alone does not.

Bronze partition (2018-12-31): 69.6MB. The conformed silver partition holds the same
rows at 28.1MB, on fewer columns with money as decimal.

## Athena, silver_credit_risk.exposure, 12 month-end partitions

| Query | Scanned |
|---|---|
| `count(*) group by snapshot_date` | 0 B |
| `sum(outstanding_amount)`, no filter | 33.4 MB |
| `sum(outstanding_amount)` for one snapshot_date | 2.85 MB |
| one snapshot_date, one column filtered | 3.58 MB |
| one snapshot_date, seven columns filtered | 9.69 MB |

Partition pruning: 11.7x less scanned on one partition out of twelve.

Column pruning: the partition is 28.1MB on disk. One column reads 13% of it, seven
columns 34%.

The count query reads nothing: Iceberg manifests carry per-partition record counts,
so the answer comes from metadata. The same query over plain Parquet reads every
file footer.

At this volume the absolute cost is immaterial. The ratios hold as the book grows.

## Glue, silver job, G.1X x 2

First run 138s including table creation; subsequent runs 89-114s per snapshot date.
Row counts written by Glue match the local run exactly across all 12 month ends
(980,614 at 2018-01-31 through 1,058,624 at 2018-12-31), which is what ADR-007 is for.
