# ADR-012: Streaming storage on S3, compute local

## Context
The credit-risk domain runs on AWS: its layers are S3 buckets and its jobs run on
Glue, so its tables are queryable from Athena. The transaction-monitoring domain was
built locally — Kafka, Spark Structured Streaming and Flink in Docker, writing Iceberg
tables to local disk. That left the two domains inconsistent: one in the cloud, one on
a laptop, and only one visible in Athena.

The domains share a platform and should share a home. The question was how much of the
streaming domain to move to AWS.

## Forces
- **Consistency.** Both domains should live in the same S3 buckets and the same Glue
  catalog, and both should be queryable from Athena.
- **Cost.** Streaming compute on AWS means a continuously running Glue Streaming job
  and a managed Kafka (MSK), together well into the tens or hundreds of dollars a
  month. The project runs on a $10 budget.
- **Fidelity.** The architecture should read the way these systems are actually built,
  and be honest about what is real versus stood in.

## Decision
Storage moves to AWS; compute stays local.

- **Storage (real on AWS).** The bronze, silver and gold Iceberg tables are written to
  the same per-layer S3 buckets credit-risk uses, under a `txn_monitoring/` prefix,
  registered in the Glue Data Catalog. Both domains are now queryable from Athena. This
  is genuinely on AWS, not stood in.
- **Compute (local, migration-ready, documented).** Kafka, Spark Structured Streaming
  and Flink run locally in Docker. The session writes to the Glue catalog and S3 with a
  single `s3=True` flag; the same Spark code runs unchanged on Glue Streaming. Kafka
  maps to MSK, Flink to a managed Flink service. The compute is not moved because a
  continuously running streaming cluster would exceed the budget — the same
  cost-conscious framing as the mock vendor engine and the local Kafka broker.

## Consequences
- The two domains are consistent: same buckets, same catalog, same Athena. A reviewer
  sees one platform, not a cloud half and a laptop half.
- Iceberg tables use S3FileIO; Structured Streaming checkpoints go through the s3a
  Hadoop filesystem. Both are configured in the session's S3 mode.
- Moving compute to Glue Streaming later is a deployment change, not a rewrite: the
  Spark logic is unchanged and the catalog and storage are already on AWS.
- The honest boundary is explicit: storage and results are real on AWS; streaming
  compute is local by choice, and the code is written to move.
