# ADR-006: One bucket per layer

## Context
The medallion layers (ADR-001) hold data with different sensitivity, different
consumers and different retention needs. Bronze holds the source extract. Gold holds
figures handed to the risk engine and reported to business units. Quarantine holds
records that failed validation.

## Decision
One bucket per layer rather than prefixes within a shared bucket.

All four buckets carry versioning, default encryption at rest, and a full public
access block. Lifecycle rules discard incomplete multipart uploads after 7 days and
expire noncurrent versions after 30 days.

## Consequences
- Access can be granted per layer. A consumer of engine inputs has no path to the
  source extract.
- A misdirected write or a bad delete is contained to one layer.
- Versioning retains replaced objects. Reloading a snapshot_date replaces the
  partition (ADR-002), and each replacement leaves the previous object as a noncurrent
  version: absent from a bucket listing, still billed. Versioning is treated as an
  operational undo rather than the audit trail, so 30 days is sufficient.
  Reproduction as of a past reporting date comes from the snapshot_date partitions and
  Iceberg history, not from object versions.

  Amended by ADR-016: no snapshot expiry is configured, so table history is not
  bounded. History is retained deliberately and the cost is measured there.
- Interrupted uploads leave billable parts that no listing shows. The 7 day rule
  bounds that exposure.
- Four buckets is more configuration than one, and bucket names are globally unique so
  the account id is appended.

## Rejected alternatives
- Single bucket with layer prefixes: fewer resources, but access policy and lifecycle
  then have to be expressed as prefix conditions on one bucket, and a policy error
  exposes every layer at once.
