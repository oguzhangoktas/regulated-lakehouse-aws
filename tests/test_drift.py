"""The drift monitor: what counts as a move worth explaining."""
from decimal import Decimal

from pyspark.sql import functions as F

from dataplatform.quality.drift import Breach, compare, measure

SCHEMA = "exposure_id string, outstanding_amount decimal(18,2)"

def measures():
    """Built on call, not held as a constant: a Column binds to the active session."""
    return {
        "exposure_count": F.count("*"),
        "total_outstanding": F.sum("outstanding_amount"),
    }


def frame(spark, *amounts):
    rows = [(f"LC-{i}", Decimal(str(a))) for i, a in enumerate(amounts)]
    return spark.createDataFrame(rows, SCHEMA)


def test_measure_reduces_a_period_to_one_number_each(spark):
    result = measure(frame(spark, 100, 200, 300), measures())

    assert result == {"exposure_count": 3.0, "total_outstanding": 600.0}


def test_a_move_inside_tolerance_is_not_a_breach():
    previous = {"total_outstanding": 1_000.0}
    current = {"total_outstanding": 1_100.0}

    assert compare(previous, current, tolerance=0.25) == []


def test_a_move_beyond_tolerance_is_a_breach():
    previous = {"total_outstanding": 1_000.0}
    current = {"total_outstanding": 100_000.0}

    breaches = compare(previous, current, tolerance=0.25)

    assert [b.measure for b in breaches] == ["total_outstanding"]
    assert breaches[0].kind == "drift"
    assert breaches[0].ratio == 100.0


def test_a_collapse_is_a_breach_too():
    """Half the book arriving is as wrong as a hundred times too much."""
    breaches = compare({"exposure_count": 1_000.0}, {"exposure_count": 500.0}, 0.25)

    assert breaches[0].kind == "drift"


def test_a_period_identical_to_the_one_before_is_a_breach():
    """A book this size does not reproduce itself to the cent."""
    breaches = compare({"total_outstanding": 1_000.0}, {"total_outstanding": 1_000.0})

    assert breaches[0].kind == "unchanged"


def test_an_absent_baseline_is_a_breach():
    breaches = compare({"exposure_count": 0.0}, {"exposure_count": 1_000.0})

    assert breaches[0].kind == "drift"
    assert breaches[0].ratio == float("inf")


def test_only_the_measures_that_moved_are_reported():
    previous = {"exposure_count": 1_000.0, "total_outstanding": 1_000.0}
    current = {"exposure_count": 1_050.0, "total_outstanding": 100_000.0}

    breaches = compare(previous, current, tolerance=0.25)

    assert [b.measure for b in breaches] == ["total_outstanding"]


def test_a_breach_reads_as_a_sentence():
    drift = Breach("total_rwa", 5_400_000_000.0, 540_000_000_000.0, "drift")
    unchanged = Breach("exposure_count", 1_000.0, 1_000.0, "unchanged")

    assert "100.00x" in str(drift)
    assert "unchanged" in str(unchanged)
