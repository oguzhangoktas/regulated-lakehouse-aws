from decimal import Decimal


def test_session_runs_and_iceberg_catalog_is_registered(spark):
    df = spark.createDataFrame(
        [("LC-1", Decimal("100.00"))], "exposure_id string, amount decimal(18,2)"
    )

    assert df.count() == 1
    assert spark.conf.get("spark.sql.catalog.lakehouse") == "org.apache.iceberg.spark.SparkCatalog"
