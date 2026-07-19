"""Run the RWA engine for a reporting date and reconcile what it returns.

The engine is a vendor black box (vendor_rwa_engine stands in for it). This job owns
the data on both sides: it hands the engine a contract-valid input, checks the output
against its contract, and reconciles the output against the input before publishing.

Reconciliation is the point of the job. The engine could silently drop exposures or
distort a figure; the count and EAD sent must match the count and EAD returned, or the
run fails rather than publishing an unexplained number.
"""
import sys

from pyspark.sql import DataFrame, SparkSession, functions as F

from dataplatform.contracts.contract import Contract, ContractViolation
from domains.credit_risk.vendor_rwa_engine import run_engine

OUTPUT_CONTRACT = "credit_risk_rwa_output"


def reconcile(engine_input: DataFrame, engine_output: DataFrame, reporting_date: str) -> None:
    """Fail unless the engine returned exactly what it was given, exposure for exposure.

    Checks count and total EAD. EAD is defined the same way on both sides (net of
    provisions for defaulted exposures), so the totals must agree to the cent.
    """
    in_count = engine_input.count()
    out_count = engine_output.count()
    if in_count != out_count:
        raise ContractViolation(
            f"{reporting_date}: sent {in_count} exposures, engine returned {out_count}"
        )

    sent_ead = engine_input.select(
        F.sum(
            F.when(
                F.col("status") == "defaulted",
                F.greatest(F.col("outstanding_amount") - F.col("provision_amount"), F.lit(0)),
            ).otherwise(F.col("outstanding_amount"))
        )
    ).first()[0]
    returned_ead = engine_output.select(F.sum("ead")).first()[0]

    if abs((sent_ead or 0) - (returned_ead or 0)) > 1:
        raise ContractViolation(
            f"{reporting_date}: EAD sent {sent_ead} != EAD returned {returned_ead}"
        )


def write_partition(df: DataFrame, table: str) -> None:
    writer = df.writeTo(table)
    if df.sparkSession.catalog.tableExists(table):
        writer.overwritePartitions()
    else:
        writer.partitionedBy(F.col("reporting_date")).create()


def run(
    spark: SparkSession,
    engine_input_table: str,
    output_table: str,
    quarantine_table: str,
    reporting_date: str,
) -> tuple[int, str]:
    engine_input = spark.read.table(engine_input_table).filter(
        F.col("reporting_date") == F.lit(reporting_date).cast("date")
    )

    engine_output = run_engine(engine_input)

    contract = Contract.named(OUTPUT_CONTRACT)
    passed, quarantined = contract.enforce(engine_output)

    passed.cache()
    if quarantined.count():
        write_partition(quarantined, quarantine_table)
    contract.assert_dataset(passed, quarantined)

    reconcile(engine_input, passed, reporting_date)

    write_partition(passed, output_table)

    total_rwa = passed.select(F.sum("rwa")).first()[0] or 0
    return passed.count(), f"{total_rwa / 1_000_000_000:.2f}bn"


if __name__ == "__main__":
    from dataplatform.lakehouse.session import local_session

    engine_input_table, output_table, quarantine_table, reporting_date = sys.argv[1:5]

    spark = local_session(f"rwa_output_{reporting_date}")
    for name in (output_table, quarantine_table):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name.rsplit('.', 1)[0]}")

    rows, total_rwa = run(spark, engine_input_table, output_table, quarantine_table, reporting_date)
    spark.stop()

    print(f"{reporting_date}  exposures={rows:,}  total_rwa={total_rwa}")
