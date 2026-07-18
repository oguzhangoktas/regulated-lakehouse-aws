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

## Scope: whole book vs engine input

Silver holds every exposure; the engine takes those carrying capital (ADR-003).
Settled loans lingering in the retention window are the difference.

| reporting_date | silver | engine input | excluded |
|---|---|---|---|
| 2018-01-31 |   980,614 |   965,778 | 14,836 |
| 2018-02-28 |   979,735 |   966,071 | 13,664 |
| 2018-03-31 |   986,121 |   971,951 | 14,170 |
| 2018-04-30 |   990,716 |   975,895 | 14,821 |
| 2018-05-31 | 1,004,736 |   989,834 | 14,902 |
| 2018-06-30 | 1,013,273 |   998,092 | 15,181 |
| 2018-07-31 | 1,021,232 | 1,005,087 | 16,145 |
| 2018-08-31 | 1,032,505 | 1,014,785 | 17,720 |
| 2018-09-30 | 1,032,585 | 1,018,706 | 13,879 |
| 2018-10-31 | 1,047,929 | 1,029,309 | 18,620 |
| 2018-11-30 | 1,053,691 | 1,035,437 | 18,254 |
| 2018-12-31 | 1,058,624 | 1,039,782 | 18,842 |

Both counts resolve from Iceberg manifests, scanning no data.
