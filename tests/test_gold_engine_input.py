from datetime import date
from decimal import Decimal

from domains.credit_risk.gold_engine_input import build, in_scope

SILVER_SCHEMA = (
    "exposure_id string, customer_id string, origination_date date, "
    "maturity_date date, term_months smallint, original_amount decimal(18,2), "
    "outstanding_amount decimal(18,2), provision_amount decimal(18,2), "
    "interest_rate decimal(6,4), rating_grade string, rating_subgrade string, "
    "status string, days_past_due int, default_flag boolean, purpose string, "
    "region string, snapshot_date date"
)


def silver_row(**overrides):
    base = {
        "exposure_id": "LC-1",
        "customer_id": "abc123",
        "origination_date": date(2016, 1, 1),
        "maturity_date": date(2019, 1, 1),
        "term_months": 36,
        "original_amount": Decimal("10000.00"),
        "outstanding_amount": Decimal("4000.00"),
        "provision_amount": Decimal("0.00"),
        "interest_rate": Decimal("12.5000"),
        "rating_grade": "B",
        "rating_subgrade": "B3",
        "status": "performing",
        "days_past_due": 0,
        "default_flag": False,
        "purpose": "debt_consolidation",
        "region": "CA",
        "snapshot_date": date(2018, 12, 31),
    }
    base.update(overrides)
    return base


def frame(spark, *rows):
    return spark.createDataFrame([silver_row(**r) for r in (rows or [{}])], SILVER_SCHEMA)


def test_settled_exposure_is_out_of_scope(spark):
    df = frame(spark, {"status": "closed", "outstanding_amount": Decimal("0.00")})

    assert in_scope(df).count() == 0


def test_defaulted_with_zero_balance_stays_in_scope(spark):
    df = frame(spark, {"status": "defaulted", "outstanding_amount": Decimal("0.00"),
                       "default_flag": True, "days_past_due": 200})

    assert in_scope(df).count() == 1


def test_performing_with_balance_is_in_scope(spark):
    assert in_scope(frame(spark)).count() == 1


def test_reporting_date_is_set_from_the_argument(spark):
    row = build(frame(spark), "2018-12-31").first()

    assert row["reporting_date"] == date(2018, 12, 31)


def test_constant_fields_are_derived(spark):
    row = build(frame(spark), "2018-12-31").first()

    assert row["exposure_class"] == "retail_other"
    assert row["currency"] == "USD"
    assert row["undrawn_amount"] == Decimal("0.00")
    assert row["collateral_flag"] is False


def test_default_date_is_null_when_performing(spark):
    row = build(frame(spark), "2018-12-31").first()

    assert row["default_date"] is None


def test_default_date_is_derived_from_days_past_due(spark):
    df = frame(spark, {"status": "defaulted", "default_flag": True, "days_past_due": 100})

    # 100 dpd at 2018-12-31, default declared at 90: crossed 10 days before.
    assert build(df, "2018-12-31").first()["default_date"] == date(2018, 12, 21)


def test_output_carries_exactly_the_contract_fields(spark):
    from dataplatform.contracts.contract import Contract

    contract = Contract.named("credit_risk_exposure_input")
    output = build(frame(spark), "2018-12-31")

    assert set(output.columns) == set(contract.field_names)
