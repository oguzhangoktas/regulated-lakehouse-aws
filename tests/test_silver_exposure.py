from datetime import date
from decimal import Decimal

from domains.credit_risk.silver_exposure import conform, validate

SOURCE_SCHEMA = (
    "exposure_id string, customer_id string, issue_d date, term_months smallint, "
    "loan_amnt double, outstanding_amount double, provision_amount double, "
    "int_rate double, grade string, sub_grade string, status string, "
    "days_past_due int, default_flag boolean, purpose string, addr_state string, "
    "annual_inc double, dti double, fico_range_low double, fico_range_high double, "
    "home_ownership string, emp_length string, verification_status string, "
    "application_type string"
)


def source_row(**overrides):
    row = {
        "exposure_id": "LC-1",
        "customer_id": "abc123",
        "issue_d": date(2016, 1, 1),
        "term_months": 36,
        "loan_amnt": 10000.0,
        "outstanding_amount": 4000.0,
        "provision_amount": 0.0,
        "int_rate": 12.5,
        "grade": "B",
        "sub_grade": "B3",
        "status": "performing",
        "days_past_due": 0,
        "default_flag": False,
        "purpose": "debt_consolidation",
        "addr_state": "CA",
        "annual_inc": 60000.0,
        "dti": 18.5,
        "fico_range_low": 700.0,
        "fico_range_high": 704.0,
        "home_ownership": "RENT",
        "emp_length": "5 years",
        "verification_status": "Verified",
        "application_type": "Individual",
    }
    row.update(overrides)
    return row


def frame(spark, *rows):
    return spark.createDataFrame([source_row(**r) for r in rows] or [source_row()],
                                 SOURCE_SCHEMA)


def test_money_becomes_decimal(spark):
    row = conform(frame(spark)).first()

    assert row["original_amount"] == Decimal("10000.00")
    assert isinstance(row["outstanding_amount"], Decimal)


def test_maturity_derived_from_term(spark):
    row = conform(frame(spark, {"issue_d": date(2016, 1, 1), "term_months": 36})).first()

    assert row["maturity_date"] == date(2019, 1, 1)


def test_clean_record_passes(spark):
    passed, quarantined = validate(conform(frame(spark)))

    assert passed.count() == 1
    assert quarantined.count() == 0


def test_outstanding_above_original_is_quarantined(spark):
    df = conform(frame(spark, {"loan_amnt": 1000.0, "outstanding_amount": 5000.0}))

    passed, quarantined = validate(df)

    assert passed.count() == 0
    assert quarantined.first()["failed_rules"] == ["outstanding_within_original"]


def test_default_flag_must_match_status(spark):
    df = conform(frame(spark, {"status": "defaulted", "default_flag": False}))

    assert validate(df)[1].first()["failed_rules"] == ["default_flag_matches_status"]


def test_unknown_grade_is_quarantined(spark):
    df = conform(frame(spark, {"grade": "Z"}))

    assert validate(df)[1].first()["failed_rules"] == ["rating_grade_known"]


def test_record_can_fail_several_rules(spark):
    df = conform(frame(spark, {"grade": "Z", "days_past_due": -5}))

    assert set(validate(df)[1].first()["failed_rules"]) == {
        "rating_grade_known",
        "days_past_due_not_negative",
    }
