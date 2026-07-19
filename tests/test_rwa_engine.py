from datetime import date
from decimal import Decimal

import pytest

from dataplatform.contracts.contract import ContractViolation
from domains.credit_risk.rwa_output_job import reconcile
from domains.credit_risk.vendor_rwa_engine import CAPITAL_RATIO, RISK_WEIGHT, run_engine

INPUT_SCHEMA = (
    "reporting_date date, exposure_id string, customer_id string, "
    "outstanding_amount decimal(18,2), provision_amount decimal(18,2), "
    "rating_grade string, status string"
)


def input_row(**overrides):
    base = {
        "reporting_date": date(2018, 12, 31),
        "exposure_id": "LC-1",
        "customer_id": "abc",
        "outstanding_amount": Decimal("1000.00"),
        "provision_amount": Decimal("0.00"),
        "rating_grade": "B",
        "status": "performing",
    }
    base.update(overrides)
    return base


def frame(spark, *rows):
    return spark.createDataFrame([input_row(**r) for r in (rows or [{}])], INPUT_SCHEMA)


def test_rwa_is_ead_times_grade_weight(spark):
    row = run_engine(frame(spark, {"rating_grade": "B", "outstanding_amount": Decimal("1000.00")})).first()

    assert row["ead"] == Decimal("1000.00")
    assert row["rwa"] == Decimal(str(1000 * RISK_WEIGHT["B"]))


def test_capital_is_eight_percent_of_rwa(spark):
    row = run_engine(frame(spark)).first()

    assert row["capital_required"] == (row["rwa"] * Decimal(str(CAPITAL_RATIO))).quantize(Decimal("0.01"))


def test_defaulted_ead_is_net_of_provisions(spark):
    row = run_engine(
        frame(spark, {"status": "defaulted", "outstanding_amount": Decimal("1000.00"),
                      "provision_amount": Decimal("400.00")})
    ).first()

    assert row["ead"] == Decimal("600.00")
    assert row["risk_weight"] == Decimal("1.5000")


def test_reconcile_passes_when_engine_returns_its_input(spark):
    engine_input = frame(spark, {"exposure_id": "LC-1"}, {"exposure_id": "LC-2"})

    reconcile(engine_input, run_engine(engine_input), "2018-12-31")


def test_reconcile_fails_when_the_engine_drops_an_exposure(spark):
    engine_input = frame(spark, {"exposure_id": "LC-1"}, {"exposure_id": "LC-2"})
    dropped = run_engine(engine_input).filter("exposure_id = 'LC-1'")

    with pytest.raises(ContractViolation, match="returned 1"):
        reconcile(engine_input, dropped, "2018-12-31")


def test_reconcile_fails_when_the_engine_distorts_ead(spark):
    engine_input = frame(spark, {"outstanding_amount": Decimal("1000.00")})
    from pyspark.sql import functions as F
    distorted = run_engine(engine_input).withColumn("ead", F.lit(Decimal("9999.00")))

    with pytest.raises(ContractViolation, match="EAD sent"):
        reconcile(engine_input, distorted, "2018-12-31")
