"""Glue entry point for the credit-risk engine input job.

Glue supplies the session and the Iceberg catalog configuration. The logic lives in
the package, loaded as a wheel.
"""
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

from domains.credit_risk.gold_engine_input_job import run

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "silver_table", "gold_table", "quarantine_table", "reporting_date"],
)

spark = SparkSession.builder.getOrCreate()

published, quarantined = run(
    spark,
    args["silver_table"],
    args["gold_table"],
    args["quarantine_table"],
    args["reporting_date"],
)

print(f"{args['reporting_date']} published={published} quarantined={quarantined}")
