"""Maintenance targets the right table through the right catalog."""
from dataplatform.lakehouse.maintenance import procedure_target


def test_a_qualified_name_splits_into_catalog_and_identifier():
    """Iceberg procedures are called on the catalog and given the rest of the name."""
    assert procedure_target("lakehouse.silver_credit_risk.exposure") == (
        "lakehouse",
        "silver_credit_risk.exposure",
    )
