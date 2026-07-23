# ADR-002: Source pattern — daily T-1 full snapshot, append-only

## Context
The real ODS (Oracle, Level-1) is loaded daily with a FULL snapshot of the previous
day's (T-1) portfolio. The table is append-only: it is never truncated and reloaded.
There is no CDC. The vendor risk engine runs MONTHLY on the month-end snapshot, and
the run happens some days after month-end (not on the 1st).

## Decision
- Bronze mirrors the ODS: partitioned by `snapshot_date`, append-only, immutable once written.
- Grain of bronze = one row per exposure per `snapshot_date` (not per exposure).
- No MERGE/CDC on ingestion — the source is already a full snapshot.
- Engine input is built from the MONTH-END snapshot (e.g. 2018-03-31), not from the run date.
- `reporting_date` (as-of business date) is separated from `run_date` (when we execute).
  Reruns must reproduce identical figures for a given `reporting_date`.

## Consequences
- Idempotency is achieved by PARTITION OVERWRITE of a `snapshot_date`, so re-running a
  day's load cannot duplicate rows. Verified by reloading a loaded partition with
  --force: object count and total bytes in bronze were unchanged (42 objects,
  2,906,256,161 bytes before and after).
- Point-in-time auditability comes free: "what did the regulator see for 31 March?" is
  answerable from the snapshot partition + Iceberg time travel.
- Storage grows linearly with time (full snapshot daily) — accepted deliberately; this is
  what the real system does and it is what makes the history auditable. Cost is managed
  with Parquet + partition pruning + lifecycle policies.

## Rejected alternatives
- **CDC/DMS-style incremental**: closer to "modern best practice", but it does NOT reflect
  the source system this models. Adopting an ingestion pattern the source does not
  have would misrepresent the problem.
- **Truncate-and-load**: destroys history; impossible to answer point-in-time regulatory questions.
