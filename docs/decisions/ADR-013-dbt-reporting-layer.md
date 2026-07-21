# ADR-013: A dbt reporting layer on top of gold

## Context
Gold holds the pipelines' primary outputs — RWA per exposure, one row per alert. The
figures a risk or monitoring team actually reads are aggregates of these: monthly RWA
and capital trends, capital by rating grade, alert volume and catch rate per rule. These
summaries were being computed ad hoc in Athena queries, versioned nowhere and tested
never.

## Decision
A dbt project (`dbt/`, dbt-athena) builds a reporting layer in a `gold_reporting`
database on top of the gold tables. Each report is a dbt model — a versioned SQL
transformation with declared sources, materialized as an Athena table and covered by
data tests.

The gold tables are declared as dbt sources; the models read from them via `source()`,
so dbt tracks the lineage from gold to each report. Tests (`not_null`, `unique`) assert
the grain and completeness of each model — a reporting table with a duplicated
reporting date or rule would fail the build.

## Why dbt for this layer, not Spark
- The transformations are aggregate SQL over tables that already exist. Spark would add
  a cluster and a job for what is a set of GROUP BYs Athena runs directly.
- dbt gives what ad hoc SQL lacks: version control, declared dependencies, generated
  lineage, and tests that run in the same command as the build.
- Iceberg was chosen partly because Athena and dbt-athena treat it as first class
  (ADR-008); this layer is that choice paying off.

## Consequences
- Reporting is reproducible and tested: `dbt run` rebuilds every report, `dbt test`
  checks its grain, and lineage from gold is explicit.
- The reporting layer is separate from gold: gold stays the source of truth, reporting
  is the summarised read. A change to a report does not touch the pipeline output.
- `gold_reporting` is provisioned in Terraform alongside the other databases, so the
  reporting layer is infrastructure, not a manual Athena artefact.
- The dbt profile carries account and workgroup detail and is gitignored, like the
  Airflow connection; the project and models are versioned.
