"""Monthly credit-risk RWA pipeline: silver conformance, engine input, RWA output.

Each task triggers a Glue job and waits. The chain runs on Glue; Airflow only
orchestrates, so its AWS credentials can start jobs but reach no data.

Each run covers one reporting month. The reporting date passed to the jobs is that
month's last calendar day, matching the month-end snapshots in the source. catchup
backfills each month in order, one run at a time, since the jobs write Iceberg tables
partitioned by reporting date and concurrent runs on one partition would collide.
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

# logical_date is the first day of the run's month; the reporting date is that
# month's last day. Verified by rendering before backfilling.
REPORTING_DATE = "{{ logical_date.end_of('month').format('YYYY-MM-DD') }}"


with DAG(
    dag_id="credit_risk_rwa",
    description="Silver to RWA for one reporting date",
    start_date=pendulum.datetime(2018, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2018, 12, 31, tz="UTC"),
    schedule="@monthly",
    catchup=True,
    default_args=default_args,
    max_active_runs=1,
    tags=["credit_risk"],
) as dag:
    silver = GlueJobOperator(
        task_id="silver_exposure",
        job_name="oglh-silver-exposure",
        region_name=REGION,
        script_args={"--snapshot_date": REPORTING_DATE},
    )
    engine_input = GlueJobOperator(
        task_id="gold_engine_input",
        job_name="oglh-gold-engine-input",
        region_name=REGION,
        script_args={"--reporting_date": REPORTING_DATE},
    )
    rwa = GlueJobOperator(
        task_id="rwa_output",
        job_name="oglh-rwa-output",
        region_name=REGION,
        script_args={"--reporting_date": REPORTING_DATE},
    )

    silver >> engine_input >> rwa
