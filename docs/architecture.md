# Architecture

A regulated financial data platform on AWS. One shared platform serves several
risk and compliance domains; credit risk and transaction monitoring are both built
end to end on the same foundation.

The design mirrors how these workloads actually run in a bank, with one change:
the bank cannot use cloud, so this rebuilds the same patterns cloud-native.

## The problem being modelled

A bank produces regulatory figures — capital held against credit risk, market
risk measures, suspicious-activity reports. The heavy calculations for credit and
market risk are done by third-party vendor engines. The data engineering around
those engines is the work: preparing a correct, validated input to the engine's
contract, and taking the engine's output back for reconciliation and reporting.

This platform owns that data engineering. The vendor engine itself is a boundary,
represented here by a clearly labelled mock (see "The engine boundary"). The value
is in the data on either side of it, not in the calculation between.

## Layering

Data moves through four layers, each with one responsibility (ADR-001):

```
source (ODS)  ->  bronze  ->  silver  ->  gold        ->  reporting
                  raw         conformed   engine I/O
                  immutable   validated
                                          quarantine (records that failed validation)
```

- **bronze** holds the source extract unchanged and immutable. Any downstream
  layer can be rebuilt from it, so a logic error is recoverable without going
  back to the source system.
- **silver** applies the platform's names and types, casts money to decimal, and
  splits records that fail validation into quarantine. It holds the whole book.
- **gold** holds what the engine consumes and produces: the engine input (a
  subset of silver — only exposures carrying capital), and the reconciled RWA
  output.
- **quarantine** keeps records that failed a rule, with the rule names attached,
  rather than dropping or silently correcting them.

Why four layers rather than one transformation: intermediate state is
inspectable when a figure is questioned, and a defect is fixed by reprocessing
from bronze rather than re-extracting from the source.

## The source pattern

The real source is an Oracle operational data store loaded daily with a full
snapshot of the previous day's portfolio, append-only, never truncated, with no
change-data-capture (ADR-002). Bronze mirrors this: partitioned by
`snapshot_date`, immutable once written.

The public dataset behind the platform is a single static extract with no time
dimension, so the daily feed is derived from it by a simulator (ADR-005). Loan
attributes and final outcomes are real; the daily trajectory — balance, days past
due, status — is derived. The derived book was validated against portfolio
metrics before being accepted (NPL between 1.25% and 2.03% across 2018, after
adding a write-off path).

The engine runs monthly, on the month-end snapshot. The reporting date is that
month end, separate from the date the job runs: reprocessing a month must
reproduce the same figures, so `reporting_date` is an as-of date, not a run date.

## The engine boundary

Credit-risk RWA is produced by a vendor engine. Here it is a mock
(`vendor_rwa_engine`) with illustrative standardised-approach risk weights — not
a reproduction of the vendor's model. Its purpose is to exercise the integration,
not to compute regulatory capital.

Both sides of the boundary are governed by a contract (`dataplatform/contracts`):

- the **input contract** declares the schema and the rules the engine input must
  satisfy. Row-level failures are quarantined; a dataset-level failure (duplicate
  grain, too many quarantined rows) means nothing is published.
- the **output contract** governs what the engine returns. After it passes, the
  output is **reconciled** against the input: exposure count and total EAD sent
  must equal those returned, or the run fails rather than publishing an
  unexplained number.

Scope matters here. Silver holds the whole book; the engine takes only exposures
that carry capital — those with a balance, or in default (ADR-003). Settled loans
stay in the platform for reporting but do not reach the engine. Engine scope is
not reporting scope.

## Point-in-time reproduction

Three mechanisms, deliberately separate:

- **snapshot_date partitions** are the regulatory record. "What did the regulator
  see for 31 March?" is answered from that partition.
- **Iceberg table history** is operational — recent versions of a table, bounded
  by snapshot expiry. Not the archive.
- **S3 object versioning** is an object-level undo for a bad write, expiring after
  30 days (ADR-006). Not the audit trail.

## Table format and storage

Tables are Apache Iceberg (ADR-008): atomic commits, schema enforcement, time
travel. Chosen over Delta because Athena and dbt-athena treat Iceberg as
first class, and gold is queried through both.

Each layer is a separate S3 bucket (ADR-006), so access can be granted per layer
and a bad write is contained to one. One Iceberg catalog spans them: a Glue
database per layer carries its own location, so `lakehouse.silver_credit_risk`
resolves into the silver bucket while `lakehouse.gold_credit_risk` resolves into
gold. A second domain reuses the same buckets under its own prefix (see "Storage
and compute across the two domains").

## Runtime

Jobs are developed locally on samples and run on AWS Glue with full data. Local,
CI and Glue all run the Glue 5.1 runtime — Python 3.11, Spark 3.5.6, Java 17
(ADR-007) — so a test passing locally is evidence the job runs on Glue. Job code
ships as a wheel; the Glue scripts are thin entry points that resolve arguments
and call the package.

## Orchestration

A local Airflow triggers the Glue jobs through `GlueJobOperator`. Orchestration
runs locally; the Spark workload runs on Glue. Airflow's IAM identity can start
and watch jobs but reaches no data — orchestrating and executing are separated.

The credit-risk DAG runs silver, engine input and RWA in order for one reporting
date, and backfills the 2018 month ends with one active run at a time so
concurrent writes to a partition cannot collide. The three jobs are idempotent
(partition overwrite by reporting date), so a retried or backfilled run converges
rather than duplicating.

## The credit-risk flow, end to end

```
ODS snapshot (bronze, S3, by snapshot_date)
      |
      v
silver_exposure        conform + validate; failures -> quarantine
      |
      v
gold_engine_input      scope filter (ADR-003) + derive fields + enforce input contract
      |
      v
vendor_rwa_engine      mock engine: EAD, risk weight, RWA, capital
      |
      v
rwa_output             enforce output contract + reconcile against input -> gold
```

Orchestrated by the `credit_risk_rwa` DAG, backfilled across all twelve 2018
month ends. Verified in Athena: twelve reporting dates, capital exactly 8% of
RWA, average risk weight declining from 0.52 to 0.48 as the book improves.

## The transaction-monitoring domain

A second domain on the same platform, detecting suspicious transactions on a
payment stream. Where credit risk is monthly batch around a vendor engine, this is
streaming and owns its detection logic — the same medallion and contract
foundation, a different shape of work.

### Flow

```
source -> producer -> Kafka topic "transactions" -> Spark Structured Streaming
          (event-time order,                        |
           keyed by account)                        |-> bronze  (raw stream, Iceberg)
                                                     |-> silver  (contract engine via
                                                     |            foreachBatch; quarantine)
                                                     |-> detection -> gold alerts
```

The producer replays the source onto Kafka in event-time order, keyed by the
origin account so an account's transactions land on one partition and stateful
rules see them in order. In production the topic is AWS MSK; locally it is a Kafka
broker in Docker, the same API.

### Reusing the platform in a streaming context

The silver stage runs the **same contract engine** the batch domain uses, applied
per micro-batch through `foreachBatch`. Streaming and batch share one quality path
rather than reimplementing validation per paradigm — the clearest evidence that
the platform layer is genuinely shared. Bronze is consumed exactly-once:
Structured Streaming records its Kafka offsets in the checkpoint, so a restart
resumes rather than reprocessing or dropping.

### Detection, measured against ground truth

The source carries a real fraud label, so every rule is measured, not assumed. The
method (ADR-009) is measure-first: rules that do not discriminate are discarded on
evidence.

- **Whole-account sweep** is the detector. Fraud transfers the origin's entire
  balance to the cent, which no legitimate transaction does — measured 96-99% of
  fraud, 0% of legitimate. On the real label: 97.9% recall at 100% precision,
  against the source's own flag rule at 0.19% recall.
- **Watchlist screening** is the unstructured side: destinations are matched to a
  synthetic sanctions-style list by Jaro-Winkler similarity, which tolerates the
  spelling and transliteration variance real screening faces. Against seeded
  near-variants: 100% recall, zero false positives.

Detection is two-dimensional — a behavioural signal and a name-matching signal,
each measured — unioned into a gold alert table tagged by rule.

### Two engines, one logic

Spark Structured Streaming is the backbone (ADR-011). The sweep rule is also
expressed in Flink SQL, consuming the same Kafka topic and producing the same
alerts, on a real Flink cluster. Flink is the industry default for low-latency
fraud work; the rule ports cleanly and the trade-off is shown in code, not just
asserted. The engine was chosen on the workload — Spark because the platform is
Spark and the latency budget allows it, Flink present because the domain expects
it.

## Storage and compute across the two domains

Both domains write to the **same per-layer S3 buckets and the same Glue Data
Catalog**, separated by a domain prefix (`credit_risk/`, `txn_monitoring/`), and
both are queryable from Athena. One platform, not a cloud half and a laptop half.

The streaming compute — Kafka, Spark, Flink — runs locally, while its storage is on
AWS (ADR-012). This is a deliberate cost boundary: a continuously running streaming
cluster and a managed Kafka would dominate the budget, whereas the storage and
results are genuinely on AWS and the Spark code is written to move to Glue
Streaming unchanged. The honest line is explicit — storage and results real on AWS,
streaming compute local by choice, code ready to migrate. Iceberg tables are
written with S3FileIO; Structured Streaming checkpoints go through the s3a Hadoop
filesystem.

## What each domain reuses

The platform layer — ingestion, medallion storage, contracts, quality,
observability, the Iceberg catalog, the runtime and orchestration patterns — is
shared. A domain adds its sources, its transformations, and its contracts. Credit
risk is the reference implementation and transaction monitoring is built on the
same foundation — the contract engine, the Iceberg catalog, the medallion layering
— while owning its own streaming ingestion and detection logic. Market risk would
reuse the vendor-engine pattern on time-series data.
