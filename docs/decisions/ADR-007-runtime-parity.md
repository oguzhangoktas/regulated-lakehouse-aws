# ADR-007: Pin the runtime to the Glue target

## Context
Spark jobs are developed locally and run on AWS Glue. Installing latest produced
PySpark 4.2 on Python 3.14 locally, against a Glue 5.1 runtime of Spark 3.5.6,
Python 3.11 and Scala 2.12.18. CI was separately running 3.12.

A major Spark version gap means code can pass locally and fail on Glue, and the
failure surfaces after deployment rather than in test.

## Decision
Local environment, CI and Glue all run Python 3.11, Spark 3.5.6 and Java 17, matching
Glue 5.1. Python dependencies are pinned in requirements.txt; the Iceberg runtime is a
Spark jar and is pinned at the session instead (ADR-008).

The package is installed in editable mode from pyproject.toml so modules import by
path rather than through sys.path manipulation.

## Consequences
- A test passing locally is evidence the job will run on Glue.
- The platform tracks the Glue release rather than the newest Spark. Upgrading Spark
  means waiting for a Glue version that carries it.
- Python 3.11 is behind the current release; libraries needing newer versions are
  unavailable.

## Rejected alternatives
- Latest local, Glue version at deploy: shifts version defects from test to production.
- Separate environments per component: three Pythons across local, CI and Glue, with
  no single environment representing the target.

## Note
The shared package was named `platform`, which shadows the standard library module of
that name. Renamed to `dataplatform` before Spark jobs started importing it.
