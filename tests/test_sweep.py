from decimal import Decimal

from domains.txn_monitoring.detection.sweep_rule import sweep_alerts

SCHEMA = ("step int, type string, amount decimal(18,2), name_orig string, "
          "name_dest string, old_balance_orig decimal(18,2), is_fraud int")


def txn(typ, amount, old_bal, fraud=0):
    return (1, typ, Decimal(str(amount)), "A1", "B1", Decimal(str(old_bal)), fraud)


def frame(spark, *rows):
    return spark.createDataFrame(list(rows), SCHEMA)


def test_whole_account_transfer_is_flagged(spark):
    # amount equals the opening balance exactly
    alerts = sweep_alerts(frame(spark, txn("TRANSFER", 5000, 5000, fraud=1)))

    assert alerts.count() == 1
    assert alerts.first()["rule"] == "whole_account_sweep"


def test_partial_transfer_is_not_flagged(spark):
    # a remainder is left behind — normal behaviour
    alerts = sweep_alerts(frame(spark, txn("TRANSFER", 3000, 5000)))

    assert alerts.count() == 0


def test_cent_tolerance_absorbs_float_noise(spark):
    alerts = sweep_alerts(frame(spark, txn("TRANSFER", 4999.50, 5000.00, fraud=1)))

    assert alerts.count() == 1


def test_payment_type_is_ignored(spark):
    # PAYMENT never carries fraud even when it empties the account
    alerts = sweep_alerts(frame(spark, txn("PAYMENT", 5000, 5000)))

    assert alerts.count() == 0


def test_zero_opening_balance_is_ignored(spark):
    # can't sweep an already-empty account
    alerts = sweep_alerts(frame(spark, txn("CASH_OUT", 0, 0)))

    assert alerts.count() == 0
