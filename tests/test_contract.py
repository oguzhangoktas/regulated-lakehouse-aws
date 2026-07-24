from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from dataplatform.contracts.contract import Contract, ContractViolation

SCHEMA = (
    "reporting_date date, exposure_id string, customer_id string, "
    "exposure_class string, original_amount decimal(18,2), "
    "outstanding_amount decimal(18,2), undrawn_amount decimal(18,2), "
    "currency string, rating_grade string, rating_subgrade string, "
    "origination_date date, maturity_date date, term_months int, "
    "interest_rate decimal(6,4), status string, days_past_due int, "
    "default_flag boolean, default_date date, provision_amount decimal(18,2), "
    "collateral_flag boolean, collateral_value decimal(18,2), purpose string, "
    "region string"
)


@pytest.fixture(scope="module")
def contract():
    return Contract.load("dataplatform/contracts/credit_risk_exposure_input.yaml")


def row(**overrides):
    base = {
        "reporting_date": date(2018, 12, 31),
        "exposure_id": "LC-1",
        "customer_id": "abc123",
        "exposure_class": "retail_other",
        "original_amount": Decimal("10000.00"),
        "outstanding_amount": Decimal("4000.00"),
        "undrawn_amount": Decimal("0.00"),
        "currency": "USD",
        "rating_grade": "B",
        "rating_subgrade": "B3",
        "origination_date": date(2016, 1, 1),
        "maturity_date": date(2019, 1, 1),
        "term_months": 36,
        "interest_rate": Decimal("12.5000"),
        "status": "performing",
        "days_past_due": 0,
        "default_flag": False,
        "default_date": None,
        "provision_amount": Decimal("0.00"),
        "collateral_flag": False,
        "collateral_value": Decimal("0.00"),
        "purpose": "debt_consolidation",
        "region": "CA",
    }
    base.update(overrides)
    return base


def frame(spark, *rows):
    return spark.createDataFrame([row(**r) for r in (rows or [{}])], SCHEMA)


def failures(quarantined):
    return quarantined.first()["failed_rules"]


def test_clean_row_passes(spark, contract):
    passed, quarantined = contract.enforce(frame(spark))

    assert passed.count() == 1
    assert quarantined.count() == 0


def test_missing_field_is_a_schema_break(spark, contract):
    df = frame(spark).drop("collateral_value")

    with pytest.raises(ContractViolation, match="collateral_value"):
        contract.enforce(df)


def test_undeclared_columns_do_not_reach_the_consumer(spark, contract):
    df = frame(spark).withColumn("internal_debug_flag", F.lit("set upstream"))

    passed, _ = contract.enforce(df)

    assert "internal_debug_flag" not in passed.columns


def test_missing_required_value_is_quarantined(spark, contract):
    _, quarantined = contract.enforce(frame(spark, {"customer_id": None}))

    assert failures(quarantined) == ["customer_id_present"]


def test_value_outside_allowed_list_is_quarantined(spark, contract):
    _, quarantined = contract.enforce(frame(spark, {"rating_grade": "Z"}))

    assert failures(quarantined) == ["rating_grade_allowed"]


def test_optional_field_may_be_null(spark, contract):
    passed, _ = contract.enforce(frame(spark, {"default_date": None}))

    assert passed.count() == 1


def test_reporting_date_must_be_month_end(spark, contract):
    _, quarantined = contract.enforce(frame(spark, {"reporting_date": date(2018, 12, 15)}))

    assert failures(quarantined) == ["reporting_date_is_month_end"]


def test_defaulted_exposure_satisfying_every_rule_passes(spark, contract):
    passed, _ = contract.enforce(
        frame(
            spark,
            {
                "status": "defaulted",
                "default_flag": True,
                "days_past_due": 120,
                "default_date": date(2018, 9, 1),
                "provision_amount": Decimal("3500.00"),
            },
        )
    )

    assert passed.count() == 1


def test_defaulted_status_must_carry_the_flag(spark, contract):
    _, quarantined = contract.enforce(
        frame(spark, {"status": "defaulted", "default_flag": False, "days_past_due": 120,
                      "default_date": date(2018, 9, 1)})
    )

    assert failures(quarantined) == ["defaulted_carries_flag"]


def test_provision_without_default_is_quarantined(spark, contract):
    _, quarantined = contract.enforce(frame(spark, {"provision_amount": Decimal("100.00")}))

    assert failures(quarantined) == ["provision_only_when_defaulted"]


def test_a_row_can_break_several_rules(spark, contract):
    _, quarantined = contract.enforce(
        frame(spark, {"rating_grade": "Z", "days_past_due": -5})
    )

    assert set(failures(quarantined)) == {"rating_grade_allowed", "days_past_due_min"}


def test_duplicate_grain_fails_the_dataset(spark, contract):
    passed, quarantined = contract.enforce(frame(spark, {}, {}))

    with pytest.raises(ContractViolation, match="duplicate the grain"):
        contract.assert_dataset(passed, quarantined)


def test_quarantine_over_the_limit_fails_the_dataset(spark, contract):
    rows = [{"exposure_id": f"LC-{i}"} for i in range(9)]
    rows.append({"exposure_id": "LC-9", "rating_grade": "Z"})

    passed, quarantined = contract.enforce(frame(spark, *rows))

    with pytest.raises(ContractViolation, match="10.00% of rows quarantined"):
        contract.assert_dataset(passed, quarantined)


def test_clean_dataset_satisfies_the_assertions(spark, contract):
    rows = [{"exposure_id": f"LC-{i}"} for i in range(10)]

    passed, quarantined = contract.enforce(frame(spark, *rows))

    contract.assert_dataset(passed, quarantined)


def test_named_loads_from_package_resources():
    """The path Glue uses: resolve the contract through the package, not the fs."""
    from_fs = Contract.load("dataplatform/contracts/credit_risk_exposure_input.yaml")
    from_pkg = Contract.named("credit_risk_exposure_input")

    assert from_pkg.field_names == from_fs.field_names
    assert from_pkg.row_rules == from_fs.row_rules


def test_an_absent_dataset_fails_the_assertions(spark, contract):
    """Nothing arriving satisfies every other assertion, so arrival is its own check."""
    passed, quarantined = contract.enforce(frame(spark).limit(0))

    with pytest.raises(ContractViolation, match="0 rows arrived"):
        contract.assert_dataset(passed, quarantined)


def test_a_contract_without_a_minimum_accepts_an_absent_dataset(spark):
    """A streaming batch is legitimately empty, so the check is declared where it applies."""
    streaming = Contract.named("txn_monitoring_transaction")
    empty = spark.createDataFrame([], "name_orig string, step int, kafka_offset long")

    streaming.assert_dataset(empty, empty)
