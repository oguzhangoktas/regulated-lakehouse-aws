from datetime import datetime
from decimal import Decimal

from domains.txn_monitoring.streaming.velocity_rule import velocity_alerts, with_event_time

SCHEMA = "step int, type string, amount decimal(18,2), name_orig string, is_fraud int"


def txn(step, typ, amount, acct="A1", fraud=0):
    return (step, typ, Decimal(str(amount)), acct, fraud)


def test_event_time_projects_step(spark):
    df = spark.createDataFrame([txn(5, "TRANSFER", 100)], SCHEMA)

    row = with_event_time(df).first()

    # step 5 hours after the reference date
    assert row["event_time"] == datetime(2018, 1, 1, 5, 0, 0)


def test_count_breach_raises_alert(spark):
    # four transfers by one account inside two hours
    rows = [txn(1, "TRANSFER", 100), txn(1, "TRANSFER", 100),
            txn(2, "TRANSFER", 100), txn(2, "CASH_OUT", 100)]
    df = spark.createDataFrame(rows, SCHEMA)

    alerts = velocity_alerts(df).collect()

    assert any(a["txn_count"] >= 3 and a["breach_type"] == "count" for a in alerts)


def test_amount_breach_raises_alert(spark):
    df = spark.createDataFrame([txn(1, "TRANSFER", 2_000_000)], SCHEMA)

    alerts = velocity_alerts(df).collect()

    assert any(a["total_amount"] >= 1_000_000 and a["breach_type"] == "amount" for a in alerts)


def test_payment_type_is_ignored(spark):
    # PAYMENT never carries fraud, so it is not counted even in volume
    rows = [txn(1, "PAYMENT", 5_000_000) for _ in range(10)]
    df = spark.createDataFrame(rows, SCHEMA)

    assert velocity_alerts(df).count() == 0


def test_quiet_account_raises_nothing(spark):
    df = spark.createDataFrame([txn(1, "TRANSFER", 100)], SCHEMA)

    assert velocity_alerts(df).count() == 0
