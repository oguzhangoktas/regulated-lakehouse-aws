"""Micro-batch orchestration for the transaction-monitoring stream.

The streaming pipeline processes the transaction feed in periodic micro-batches: each
run consumes whatever has arrived on the topic since the last run, in availableNow mode,
then stops. This is the common cost-managed alternative to an always-on streaming
cluster — the same model Glue Streaming and Databricks jobs use when continuous compute
is not warranted.

The chain is bronze, then silver, then the alert job, in order. bronze and silver are
Structured Streaming jobs in availableNow mode; the alert job is a batch pass over the
new silver rows. Each stage is idempotent — Structured Streaming resumes from its
checkpoint, and the alert job overwrites by rule — so a retried run converges.

Compute currently runs locally (ADR-012): the streaming jobs read Kafka and write S3
under the Glue catalog from a local Spark, while their storage and results are on AWS.
This DAG is the orchestration for the migrated deployment, where the same jobs run as
Glue Streaming jobs; the job names below are those Glue jobs. Airflow only triggers and
watches — it reaches no data — exactly as in the credit-risk DAG.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

REGION = "eu-central-1"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="txn_monitoring_stream",
    description="Micro-batch: consume the transaction stream, conform, detect",
    start_date=pendulum.datetime(2018, 1, 1, tz="UTC"),
    schedule=timedelta(hours=6),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["txn_monitoring"],
) as dag:
    # Consume new transactions from Kafka into bronze, exactly-once via the checkpoint.
    bronze = GlueJobOperator(
        task_id="bronze_stream",
        job_name="oglh-txn-bronze-stream",
        region_name=REGION,
        script_args={"--mode": "availableNow"},
    )
    # Conform and validate new bronze rows into silver, reusing the batch contract engine.
    silver = GlueJobOperator(
        task_id="silver_stream",
        job_name="oglh-txn-silver-stream",
        region_name=REGION,
        script_args={"--mode": "availableNow"},
    )
    # Run detection over silver and publish alerts to gold.
    alerts = GlueJobOperator(
        task_id="alert_job",
        job_name="oglh-txn-alert-job",
        region_name=REGION,
    )

    bronze >> silver >> alerts
