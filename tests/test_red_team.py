"""The synthetic red-team scenarios exercise the typology rules that PaySim cannot.

These assert coverage of known fraud patterns, separate from the precision/recall
measured on PaySim's real label.
"""
from domains.txn_monitoring.testdata.red_team import build
from domains.txn_monitoring.detection.structuring_rule import structuring_alerts
from domains.txn_monitoring.streaming.velocity_rule import velocity_alerts


def test_structuring_scenario_is_flagged(spark):
    red = build(spark)

    alerts = structuring_alerts(red).collect()

    flagged = {a["name_orig"] for a in alerts}
    assert "STRUCT-1" in flagged
    assert all(a["txn_count"] >= 3 for a in alerts)


def test_velocity_burst_scenario_is_flagged(spark):
    red = build(spark)

    alerts = velocity_alerts(red).collect()

    flagged = {a["name_orig"] for a in alerts}
    assert "BURST-1" in flagged


def test_structuring_rule_ignores_the_burst_account(spark):
    # the burst transfers are well under the just-under band, so structuring skips them
    red = build(spark)

    flagged = {a["name_orig"] for a in structuring_alerts(red).collect()}

    assert "BURST-1" not in flagged
