# Regulated Financial Data Lakehouse on AWS

An end-to-end, cloud-native data platform that rebuilds regulated banking data
workloads — credit-risk (Basel-style RWA), market-risk (VaR), and real-time AML
transaction monitoring — on AWS.

**Stack:** AWS (S3, Glue, Athena, Redshift), PySpark, dbt, Airflow, Kafka +
Spark Structured Streaming, Terraform, GitHub Actions.

Built to demonstrate production-grade data engineering on regulated financial
data. See `docs/` for architecture and design decisions.

## Local dev
- `make up` — start local stack (Postgres)
- `make test` — run tests
- `make lint` — lint
- `make down` — stop local stack