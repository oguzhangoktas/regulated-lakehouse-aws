# Regulated Financial Data Lakehouse on AWS

An end-to-end data platform that produces regulatory credit-risk figures from a
real loan portfolio, built cloud-native on AWS. One shared platform serves several
risk and compliance domains; the credit-risk domain is implemented end to end and
the rest reuse the same foundation.

The design follows how these workloads run in a bank, with one change: banking
regulation keeps this kind of work off the cloud, so this rebuilds the same
patterns cloud-native to show they carry over unchanged.

---

## What it does

A bank holds capital against the risk in its loan book. The size of that capital
is produced by a risk engine that consumes a precisely-shaped, validated input and
returns exposure-level results. The engineering around that engine is the work:
landing the source data, conforming and validating it, shaping the engine input to
a contract, and reconciling the engine output before it is reported.

This platform owns that engineering, over a real portfolio (LendingClub accepted
loans, 2.26M loans, 2007–2018). It runs the full chain for each month-end of 2018:

```
source snapshot -> bronze -> silver -> engine input -> risk engine -> RWA output
```

and produces, per reporting date, the exposure-at-default, risk-weighted assets
and capital requirement for the book — reconciled, validated, and queryable.

---

## Why it is built this way

Every significant choice is recorded as an architecture decision
(`docs/decisions/`). The ones that shape the whole platform:

**Medallion layering (bronze / silver / gold / quarantine).** Raw data lands
unchanged and immutable in bronze, so any downstream layer can be rebuilt from it
and a logic error is fixed by reprocessing rather than re-extracting from the
source. Validation happens at the silver boundary; records that fail are
quarantined with the reason, never silently dropped or corrected.

**The source is a daily full snapshot, append-only.** The real operational data
store is loaded daily with a full copy of the previous day's portfolio and never
truncated. Bronze mirrors this exactly, partitioned by snapshot date. Because the
public dataset is a single static extract, the daily feed is *derived* from it by
a simulator: the loan attributes and final outcomes are real, only the day-to-day
trajectory (balance, days past due, status) is generated — and it was validated
against portfolio metrics (non-performing ratio between 1.25% and 2.03% across
2018) before being used.

**The risk engine is a boundary, not a calculation.** In a bank the engine is a
third-party application. Here it is a clearly-labelled mock with illustrative risk
weights — its job is to exercise the integration, not to reproduce a vendor's
model. The value, and the code, is on both sides of it: a contract-validated input
going in, and a reconciled output coming back.

**Contracts are enforced, not just declared.** Each boundary has a data contract
(schema + rules) expressed as data and evaluated at runtime, so a rule cannot
drift from its declaration. A row that breaks a rule is quarantined; a dataset
that breaks a dataset-level rule (a duplicate grain, too many quarantined rows) is
not published at all — a wrong regulatory figure is worse than a late one.

**Engine scope is not reporting scope.** Silver holds the whole book. The engine
receives only exposures that carry capital — those with a balance, or in default.
Settled loans stay in the platform for reporting but never reach the engine.

**Point-in-time reproduction has three separate mechanisms.** The snapshot-date
partitions are the regulatory record ("what was reported for 31 March"). Iceberg
table history is operational. S3 object versioning is an undo for a bad write.
Each does one job; none is asked to be the audit trail on its own.

**Apache Iceberg as the table format.** Atomic commits, schema enforcement and
time travel over object storage, chosen over Delta because Athena and dbt treat
Iceberg as first class and the gold layer is queried through both.

**Runtime parity.** Jobs are developed locally on samples and run on AWS Glue with
full data. Local, CI and Glue all run the same runtime (Python 3.11, Spark 3.5.6,
Java 17), so a test passing locally is real evidence the job runs on Glue.

**Least-privilege access.** The job role can read bronze but not write it, which
makes bronze's immutability a permission rather than a convention. Orchestration
has its own identity that can start and watch jobs but reach no data — executing
and orchestrating are separated.

---

## Architecture

```
                         ┌──────────────────────────────┐
   Oracle-style ODS ───► │ bronze   raw, immutable        │  S3, by snapshot_date
   (daily T-1 snapshot)  │          (source of rebuilds)  │
                         └───────────────┬────────────────┘
                                         │  conform + validate
                                         ▼
                         ┌──────────────────────────────┐   failures
                         │ silver   typed, validated      │ ──────────► quarantine
                         │          the whole book        │
                         └───────────────┬────────────────┘
                                         │  scope filter + derive + input contract
                                         ▼
                         ┌──────────────────────────────┐
                         │ gold     engine input          │
                         └───────────────┬────────────────┘
                                         │
                                         ▼
                             risk engine (vendor boundary, mocked)
                                         │  output contract + reconciliation
                                         ▼
                         ┌──────────────────────────────┐
                         │ gold     RWA output            │  queried via Athena
                         └──────────────────────────────┘

  Orchestrated by a local Airflow triggering AWS Glue jobs.
  Each layer is a separate S3 bucket; one Iceberg catalog spans them.
```

Full detail, including the reasoning behind each element, is in
`docs/architecture.md`. Individual decisions are in `docs/decisions/`.

---

## Results

Measured on the platform as built (full numbers in `docs/measurements.md`):

- **Storage.** 1.6 GB source CSV reduces to a 127 MB Parquet master — column
  pruning, columnar layout and compression together, not the format alone.
- **Query cost.** Partition pruning scans 11.7× less on one month of twelve. Count
  queries scan zero bytes, answered from Iceberg manifests.
- **Scope.** Silver holds the whole book; the engine input is smaller every month
  (December: 1,058,624 exposures in silver, 1,039,782 sent to the engine — the
  difference is settled loans).
- **Regulatory output.** Across all twelve 2018 month-ends: capital is exactly 8%
  of RWA, and the average risk weight declines from 0.52 to 0.48 as the book's
  composition improves. December: EAD 11.2 bn, RWA 5.40 bn.
- **Reconciliation.** Every engine run checks that the exposure count and total EAD
  sent equal those returned; a dropped exposure or a distorted figure fails the
  run rather than publishing.
- **Idempotency.** Reloading a partition leaves the row count unchanged, verified
  at both the object level (bronze) and the table level (Iceberg overwrite).

---

## Stack

SQL · Python · PySpark · AWS (S3, Glue, Athena, IAM) · Apache Iceberg · Airflow ·
Terraform · Docker · GitHub Actions.

Infrastructure is Terraform (`infra/`). Job code is a Python package
(`dataplatform/`, `domains/`) shipped to Glue as a wheel; the Glue scripts
(`glue/jobs/`) are thin entry points. Orchestration is a local Airflow
(`airflow/dags/`) triggering the Glue jobs. Everything is tested (`tests/`) and
linted in CI on every push.

---

## Layout

```
dataplatform/        shared platform: ingestion, lakehouse, contracts, quality
  ingestion/         source simulation and bronze loading
  lakehouse/         Spark session on the Iceberg catalog
  contracts/         data-contract engine + contract definitions
domains/
  credit_risk/       silver conformance, engine input, engine, RWA output
glue/jobs/           thin Glue entry points
infra/               Terraform: S3 layers, Glue jobs, IAM, Athena
airflow/dags/        the orchestration DAG
tests/               unit tests for transforms, contracts, reconciliation
docs/
  architecture.md    the design, with reasoning
  decisions/         one record per significant decision (ADRs)
  data_dictionary.md source columns, distributions, constraints
  measurements.md    measured results
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

# 4. build the loan master and generate the source snapshots
python -m dataplatform.ingestion.build_loan_master <raw_csv> data/master/loan_master.parquet
python -m dataplatform.ingestion.simulate_ods_snapshots data/master/loan_master.parquet data/ods 2018-01-01 2018-11-30 ME
python -m dataplatform.ingestion.simulate_ods_snapshots data/master/loan_master.parquet data/ods 2018-12-01 2018-12-31 D

# 5. load bronze, then package and upload the job code
python -m dataplatform.ingestion.load_bronze data/ods <bronze-bucket> credit_exposure_snapshot
make package

# 6. start Airflow and backfill the 2018 month-ends
docker compose up -d
# then trigger the credit_risk_rwa DAG (see docs/architecture.md)
```

Jobs can also be developed and run entirely locally on a sample before running on
Glue — the code takes paths and table names as arguments and does not know whether
it is reading a local directory or S3.
