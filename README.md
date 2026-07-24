# Regulated Financial Data Lakehouse on AWS

An end-to-end data platform that produces regulatory figures from real financial data,
built cloud-native on AWS. One shared foundation serves two domains with very different
shapes: credit risk as monthly batch around a third-party engine, and transaction
monitoring as a stream with its own detection logic.

The design follows how these workloads run in a bank, with one change: banking regulation
keeps this kind of work off the cloud, so this rebuilds the same patterns cloud-native to
show they carry over unchanged.

---

## What it does

**Credit risk — batch, around a vendor engine.** A bank holds capital against the risk in
its loan book, and the size of that capital is produced by a risk engine that consumes a
precisely-shaped, validated input. The engineering around that engine is the work:
landing the source, conforming and validating it, shaping the engine input to a contract,
and reconciling the output before anything is reported. Run over a real portfolio
(LendingClub accepted loans, 2.26M loans) for every month-end of 2018.

```
ODS snapshot -> bronze -> silver -> engine input -> risk engine -> RWA output
```

**Transaction monitoring — streaming, owning its detection.** Transactions arrive on
Kafka and are consumed exactly-once into the same medallion layering, validated by the
same contract engine, and screened by rules the platform owns rather than a vendor's.
Detection runs in two dimensions: a behavioural signal on the transaction itself, and
fuzzy name screening against a sanctions-style watchlist — the unstructured side.

```
payment stream -> Kafka -> bronze -> silver -> detection -> alerts
```

The two domains share the platform layer — contracts, catalog, layering, runtime — and
differ in everything above it. That is the point: the foundation is reused rather than
described as reusable.

---

## Results

Measured on the platform as built, not estimated (full numbers in
`docs/measurements.md`).

**Regulatory output.** Across all twelve 2018 month-ends: 12,010,727 exposure-months,
capital exactly 8% of RWA, and the average risk weight declining from 0.52 to 0.48 as the
book's composition improves. December: EAD 11.2 bn, RWA 5.40 bn.

**Detection, against a real fraud label.** 6,362,620 transactions streamed. The
whole-account sweep signature — fraud transfers the origin's entire balance to the cent,
which no legitimate transaction does — was discovered by profiling rather than assumed,
and measured at **97.9% recall with 100% precision**, against the source's own flag rule
at 0.19% recall. Two other rules were discarded on evidence when they failed to
discriminate; the method is recorded in ADR-009.

**Screening.** Destinations matched against a synthetic sanctions list by Jaro-Winkler
similarity, which tolerates the transliteration and spelling variance real screening
faces. Against deliberately seeded near-variants: **100% recall, zero false positives**.

**Reconciliation.** Every engine run checks that the exposure count and total EAD sent
equal those returned; a dropped exposure or a distorted figure fails the run rather than
publishing.

**Query cost.** Partition pruning scans 11.7x less on one month of twelve. Count queries
scan zero bytes, answered from Iceberg manifests.

**Resilience.** Twelve fault-injection experiments (`chaos/`) with the outcomes recorded
as runbooks. Three of them changed the platform.

---

## Why it is built this way

Every significant choice is recorded as an architecture decision (`docs/decisions/`).
The ones that shape the whole platform:

**Medallion layering (bronze / silver / gold / quarantine).** Raw data lands unchanged and
immutable in bronze, so any downstream layer can be rebuilt from it and a logic error is
fixed by reprocessing rather than re-extracting from the source. Records that fail
validation are quarantined with the reason attached, never silently dropped or corrected.

**One quality path for both paradigms.** The contract engine was written for batch. The
streaming silver stage applies it unchanged, per micro-batch, through `foreachBatch` — so
validation is not reimplemented per paradigm and cannot drift between them.

**Contracts are enforced, not just declared.** Each boundary has a data contract (schema
+ rules) expressed as data and evaluated at runtime, so a rule cannot drift from its
declaration. A row that breaks a rule is quarantined; a dataset that breaks a
dataset-level rule — a duplicate grain, too many quarantined rows, nothing arriving at
all — is not published, because a wrong regulatory figure is worse than a late one.

**The risk engine is a boundary, not a calculation.** In a bank the engine is a
third-party application. Here it is a clearly-labelled mock with illustrative risk weights
— its job is to exercise the integration, not to reproduce a vendor's model. The value,
and the code, is on both sides of it.

**Detection rules are measured, not assumed.** The transaction source carries a real fraud
label, so every rule was scored against it before being kept. Two plausible rules were
discarded on evidence and one non-obvious signature was adopted on evidence. Typologies
the data lacks are covered by a separate labelled synthetic set, never mixed into the real
stream (ADR-010).

**Two engines, one logic.** Spark Structured Streaming is the backbone. The same detection
rule is also expressed in Flink SQL on a real Flink cluster, reading the same topic and
producing the same alerts — so the engine choice is demonstrated rather than asserted, and
the trade-off is shown in code (ADR-011).

**The source is a daily full snapshot, append-only.** The real operational data store is
loaded daily with a full copy of the previous day's portfolio and never truncated. Bronze
mirrors this, partitioned by snapshot date. Because the public dataset is a single static
extract, the daily feed is *derived* from it: loan attributes and final outcomes are real,
only the day-to-day trajectory is generated — and it was validated against portfolio
metrics before being used.

**Engine scope is not reporting scope.** Silver holds the whole book. The engine receives
only exposures that carry capital. Settled loans stay in the platform for reporting and
never reach the engine.

**Point-in-time reproduction has three separate mechanisms.** The snapshot-date partitions
are the regulatory record. Iceberg table history is operational recovery. S3 object
versioning is an undo for a bad write. Each does one job; none is asked to be the audit
trail on its own.

**Apache Iceberg as the table format.** Atomic commits, schema enforcement and time travel
over object storage, chosen over Delta because Athena and dbt treat Iceberg as first class
and the gold layer is queried through both.

**Runtime parity.** Jobs are developed locally on samples and run on AWS Glue with full
data. Local, CI and Glue all run the same runtime (Python 3.11, Spark 3.5.6, Java 17), so
a test passing locally is real evidence the job runs on Glue.

**Least-privilege access.** The job role can read bronze but not write it, which makes
bronze's immutability a permission rather than a convention. Orchestration has its own
identity that can start and watch jobs but reach no data — executing and orchestrating are
separated.

---

## Architecture

```
CREDIT RISK (batch, monthly)

  Oracle-style ODS ──► bronze ──► silver ──► gold engine input ──► risk engine
  (daily T-1 snapshot)  raw        the whole   scope + contract    (vendor boundary,
                        immutable  book                             mocked)
                                     │                                 │
                                     └──► quarantine    output contract + reconcile
                                                                       │
                                                                       ▼
                                                                 gold RWA output

TRANSACTION MONITORING (streaming)

  payment stream ──► Kafka ──► bronze ──► silver ──────► detection ──► gold alerts
  (producer, keyed   (MSK in    raw       same contract   sweep rule
   by account)        prod)     stream    engine, via     + watchlist screening
                                     │     foreachBatch          │
                                     └──► quarantine             └──► screening alerts

                     the same rule also runs on Flink SQL, on the same topic

BOTH

  one Iceberg catalog over per-layer S3 buckets · Glue Data Catalog · Athena
  dbt reporting models on top of gold · orchestrated by Airflow triggering Glue
```

Full detail, including the reasoning behind each element, is in `docs/architecture.md`.

---

## Reliability and governance

**Data classification.** Every field in both domains is classified and the handling for
each level is written down (`docs/data_classification.md`), separating assertions about a
party — a fraud label, a screening hit — from attributes of one. Minimisation is enforced
rather than described: eight borrower attributes present in silver are not declared by the
engine contract and therefore reach neither the engine nor reporting, and a test holds
that boundary in place.

**Fault injection.** Twelve experiments in `chaos/` inject a fault and record what the
platform does, with the outcomes written as runbooks (`docs/runbooks/`) rather than as a
report. Three of them changed the platform: a uniform change of units passed every
contract rule and overstated RWA a hundredfold, which produced a period-over-period drift
monitor; an absent input produced a successful run reporting zero, which produced an
arrival assertion; and a streaming contract was found to declare three things that did
nothing.

**Drift monitoring.** Contracts answer whether a dataset is internally sound. They cannot
answer whether it is plausible next to the period before it, and several faults leave the
data entirely self-consistent. The monitor reports rather than halts, and runs beside the
pipeline rather than inside it: a gate must be certain, and a book can move sharply for
legitimate reasons (ADR-015).

**Table maintenance.** Retained Iceberg history costs measured storage — 157% on top of
live data on silver — and the S3 lifecycle rule cannot reclaim it. History is kept anyway,
because the cost is cents and a snapshot cannot be recreated once expired, and the
maintenance procedures exist as code with a stated retention rather than as a command to
be recalled during an incident (ADR-016).

---

## Stack

SQL · Python · PySpark · Spark Structured Streaming · Apache Kafka · Apache Flink ·
Apache Iceberg · dbt · AWS (S3, Glue, Athena, IAM) · Airflow · Terraform · Docker ·
GitHub Actions.

Infrastructure is Terraform (`infra/`). Job code is a Python package (`dataplatform/`,
`domains/`) shipped to Glue as a wheel; the Glue scripts (`glue/jobs/`) are thin entry
points holding no business logic. Orchestration is a local Airflow (`airflow/dags/`)
triggering Glue jobs. Everything is tested (`tests/`, 71 tests) and linted in CI on every
push, in an environment matching the Glue runtime.

---

## Layout

```
dataplatform/        shared platform, used by both domains
  ingestion/         source simulation and bronze loading
  lakehouse/         Spark session on the Iceberg catalog, table maintenance
  contracts/         data-contract engine + contract definitions
  quality/           period-over-period drift monitoring
  profiling/         dataset profiling, reused across domains
domains/
  credit_risk/       silver conformance, engine input, engine, RWA output
  txn_monitoring/    producer, streaming layers, detection rules, screening, Flink SQL
glue/jobs/           thin Glue entry points
dbt/                 reporting models on top of gold, with tests
infra/               Terraform: S3 layers, Glue jobs and databases, IAM, Athena
airflow/dags/        the orchestration DAGs, one per domain
chaos/               fault-injection experiments
tests/               unit tests for transforms, contracts, reconciliation, detection
docs/
  architecture.md         the design, with reasoning
  decisions/              one record per significant decision (ADRs)
  runbooks/               what to do when something fails, from the experiments
  data_classification.md  what is sensitive, and how each level is handled
  data_dictionary.md      source columns, distributions, constraints
  measurements.md         measured results
```

---

## Running it

Prerequisites: an AWS account, the AWS CLI configured, Docker, and Python 3.11.

```bash
# 1. install the package and dev tooling
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -e .

# 2. run the tests
make test

# 3. provision the infrastructure
cd infra && terraform init && terraform apply && cd ..

# 4. start Kafka, Postgres, Airflow and Flink
make up
```

**Credit risk**

```bash
# build the loan master and generate the source snapshots
python -m dataplatform.ingestion.build_loan_master <raw_csv> data/master/loan_master.parquet
python -m dataplatform.ingestion.simulate_ods_snapshots data/master/loan_master.parquet data/ods 2018-01-01 2018-12-31 ME

# load bronze, package and upload the job code, then backfill the month-ends
python -m dataplatform.ingestion.load_bronze data/ods <bronze-bucket> credit_exposure_snapshot
make package
# then trigger the credit_risk_rwa DAG
```

**Transaction monitoring**

```bash
# replay the transaction log onto Kafka
python -m domains.txn_monitoring.streaming.producer <paysim_csv>

# consume into bronze and silver, then detect
python -m domains.txn_monitoring.streaming.bronze_stream --once --s3
python -m domains.txn_monitoring.streaming.silver_stream --once --s3
python -m domains.txn_monitoring.detection.alert_job --s3

# the same rule on Flink
docker compose exec flink-jobmanager ./bin/sql-client.sh -f /opt/flink/jobs/sweep.sql
```

**Reporting**

```bash
cd dbt && dbt run && dbt test
```

Jobs can be developed and run entirely locally on a sample before running on AWS — the
code takes paths and table names as arguments and does not know whether it is reading a
local directory or S3.
