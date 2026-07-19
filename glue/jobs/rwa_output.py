"""Glue entry point for the RWA engine output job.

Glue supplies the session and the Iceberg catalog configuration. The logic lives in
the package, loaded as a wheel.
"""
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

from domains.credit_risk.rwa_output_job import run

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "engine_input_table", "output_table", "quarantine_table", "reporting_date"],
)

spark = SparkSession.builder.getOrCreate()

rows, total_rwa = run(
    spark,
    args["engine_input_table"],
    args["output_table"],
    args["quarantine_table"],
    args["reporting_date"],
)

print(f"{args['reporting_date']} exposures={rows} total_rwa={total_rwa}")
