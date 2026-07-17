"""Glue entry point for the credit exposure silver job.

Glue supplies the session and reads the Iceberg catalog configuration from job
parameters (infra/glue.tf), so this only resolves arguments and calls the job.
The logic lives in the package, which Glue loads as a wheel.
"""
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

from domains.credit_risk.silver_exposure_job import run

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bronze_root", "table", "quarantine_table", "snapshot_date"],
)

spark = SparkSession.builder.getOrCreate()

passed, quarantined = run(
    spark,
    args["bronze_root"],
    args["table"],
    args["quarantine_table"],
    args["snapshot_date"],
)

print(f"{args['snapshot_date']} passed={passed} quarantined={quarantined}")
