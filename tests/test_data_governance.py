"""The classification claims that depend on code, pinned as tests.

A control that is documented and not verified drifts: a field added to a contract, or an
identifier added to a reporting model, would leave docs/data_classification.md describing
a platform that no longer exists. These assert the claims rather than restating them.
"""
from datetime import date
from decimal import Decimal
from pathlib import Path

from dataplatform.contracts.contract import Contract
from dataplatform.ingestion.build_loan_master import pseudonymise
from domains.credit_risk.gold_engine_input import build

REPO_ROOT = Path(__file__).resolve().parents[1]

# Attributes silver holds about the borrower rather than about the exposure. The engine
# derives capital from exposure and rating, so it is given neither these nor anything
# derived from them.
BORROWER_ATTRIBUTES = [
    "annual_income",
    "debt_to_income",
    "fico_low",
    "fico_high",
    "home_ownership",
    "emp_length",
    "verification_status",
    "application_type",
]

# Keys that resolve to a single party or account. Reporting is aggregate, so none of
# these belongs in a model there.
IDENTIFIERS = ["customer_id", "exposure_id", "name_orig", "name_dest"]

SILVER_SCHEMA = (
    "exposure_id string, customer_id string, origination_date date, maturity_date date, "
    "term_months smallint, original_amount decimal(18,2), "
    "outstanding_amount decimal(18,2), provision_amount decimal(18,2), "
    "interest_rate decimal(6,4), rating_grade string, rating_subgrade string, "
    "status string, days_past_due int, default_flag boolean, purpose string, "
    "region string, annual_income decimal(18,2), debt_to_income decimal(8,4), "
    "fico_low smallint, fico_high smallint, home_ownership string, emp_length string, "
    "verification_status string, application_type string, snapshot_date date"
)

SILVER_ROW = (
    "LC-1", "abc123", date(2016, 1, 1), date(2019, 1, 1), 36,
    Decimal("10000.00"), Decimal("4000.00"), Decimal("0.00"), Decimal("12.5000"),
    "B", "B3", "performing", 0, False, "debt_consolidation", "CA",
    Decimal("60000.00"), Decimal("18.5000"), 700, 704,
    "RENT", "5 years", "Verified", "Individual", date(2018, 12, 31),
)


def test_engine_input_contract_declares_no_borrower_attributes():
    contract = Contract.named("credit_risk_exposure_input")

    assert set(BORROWER_ATTRIBUTES).isdisjoint(contract.field_names)


def test_rwa_output_contract_declares_no_borrower_attributes():
    contract = Contract.named("credit_risk_rwa_output")

    assert set(BORROWER_ATTRIBUTES).isdisjoint(contract.field_names)


def test_borrower_attributes_do_not_survive_the_engine_boundary(spark):
    """Silver carries them; what the engine receives does not."""
    silver = spark.createDataFrame([SILVER_ROW], SILVER_SCHEMA)
    assert set(BORROWER_ATTRIBUTES).issubset(silver.columns)

    engine_input = build(silver, "2018-12-31")

    assert set(BORROWER_ATTRIBUTES).isdisjoint(engine_input.columns)


def test_the_customer_key_is_a_stable_pseudonym():
    """The same loan resolves to the same party across every snapshot."""
    assert pseudonymise("12345") == pseudonymise("12345")


def test_the_customer_key_separates_loans():
    assert pseudonymise("12345") != pseudonymise("12346")


def test_the_customer_key_does_not_carry_the_source_id():
    assert "12345" not in pseudonymise("12345")


def test_reporting_models_expose_no_individual_identifier():
    """Every reporting model is an aggregate, so no party reaches that layer."""
    models = sorted((REPO_ROOT / "dbt" / "models" / "reporting").glob("*.sql"))
    assert models, "no reporting models found"

    for model in models:
        sql = model.read_text(encoding="utf-8").lower()
        for identifier in IDENTIFIERS:
            assert identifier not in sql, f"{model.name} references {identifier}"
