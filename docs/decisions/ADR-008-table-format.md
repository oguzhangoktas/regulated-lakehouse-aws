# ADR-008: Apache Iceberg as the table format

## Context
Silver and gold need atomic writes, reproducible reads as of a past version, and
schema enforcement. Object storage provides none of these: a table is a set of files,
a failed write leaves a partial set with no marker, and there is no record of which
files constitute the table at a point in time.

Glue 5.1 carries Iceberg 1.10.0, Delta Lake 3.3.2 and Hudi 1.0.2 natively. All three
solve the same problem with a transaction log.

## Decision
Iceberg, for consistency with the tooling this platform reads with.

Athena supports Iceberg for reads, writes, DDL and time travel; its Delta support is
read-only. dbt-athena materialises Iceberg tables natively and does not materialise
Delta. Gold is queried through Athena and modelled in dbt, so the table format has to
be the one those tools treat as first class.

## Consequences
- The same catalog name resolves to a filesystem directory locally and to the Glue
  Data Catalog on Glue. Only catalog configuration differs between the two.
- Iceberg ships as a Spark runtime jar, resolved at session start, rather than a
  Python package pinned in requirements.
- overwritePartitions replaces only the partitions the incoming data covers, which
  matches the daily full-snapshot pattern (ADR-002) without naming the partition in
  the write.
- Table history is bounded by snapshot expiry. It is an operational mechanism, not
  the regulatory record: reproduction as of a reporting date comes from the
  snapshot_date partitions (ADR-006).

## Rejected alternatives
- Delta Lake: equivalent capability and supported by Glue, but read-only in Athena
  and unsupported by dbt-athena, which puts the friction in the layer that is queried
  most.
- Parquet alone: no atomic commit, no history, no schema enforcement.
