"""Period-over-period drift.

A contract answers whether a dataset is internally sound. It cannot answer whether the
dataset is plausible next to the period before it, and several faults leave the data
entirely self-consistent: every amount scaled by the same factor, a feed that delivered
half the book, the previous snapshot re-delivered unchanged, a rating distribution that
shifted wholesale.

Each of those is invisible to a rule that only looks within the dataset, and each is
obvious against the period before.

This reports rather than halts. A large move is evidence of a problem, not proof of one —
a portfolio acquisition moves a book sharply and legitimately — so it belongs beside the
pipeline as a monitor, not inside it as a gate. A gate must be certain; this is not.

Both directions matter. Too much movement is one fault; none at all is another, since a
book of this size does not reproduce itself to the cent between reporting dates.
"""
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame

DEFAULT_TOLERANCE = 0.25


@dataclass(frozen=True)
class Breach:
    """A measure whose movement between two periods needs explaining."""

    measure: str
    previous: float
    current: float
    kind: str  # "drift" or "unchanged"

    @property
    def ratio(self) -> float:
        return self.current / self.previous if self.previous else float("inf")

    def __str__(self) -> str:
        if self.kind == "unchanged":
            return f"{self.measure}: unchanged at {self.current:,.2f}"
        return (
            f"{self.measure}: {self.previous:,.2f} -> {self.current:,.2f} "
            f"({self.ratio:,.2f}x)"
        )


def measure(df: DataFrame, measures: dict[str, Column]) -> dict[str, float]:
    """Reduce a period to one number per measure.

    The measures are supplied by the caller because what is worth watching differs by
    dataset: exposure count and total balance for a credit book, alert count and rate for
    a monitoring feed.
    """
    row = df.agg(*[expr.alias(name) for name, expr in measures.items()]).first()
    return {name: float(row[name] or 0) for name in measures}


def compare(
    previous: dict[str, float],
    current: dict[str, float],
    tolerance: float = DEFAULT_TOLERANCE,
) -> list[Breach]:
    """Measures that moved more than tolerance allows, or did not move at all."""
    breaches = []

    for name, before in previous.items():
        after = current[name]

        if before == after:
            breaches.append(Breach(name, before, after, "unchanged"))
        elif not before or abs(after / before - 1) > tolerance:
            breaches.append(Breach(name, before, after, "drift"))

    return breaches
