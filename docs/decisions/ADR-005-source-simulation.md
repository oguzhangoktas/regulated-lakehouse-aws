# ADR-005: Simulated source feed

## Context
The source system lands a full snapshot of the previous day's portfolio every day
(ADR-002). The public dataset backing this platform (LendingClub accepted loans,
2007-2018) is a single extract holding one row per loan with its final observed state.
It carries no time dimension.

Without a daily feed there is no load cycle to run, no month-end position to hand to
the engine, no backfill, no reprocessing path to test, and no late data.

## Decision
Derive the daily feed from the real portfolio rather than fabricate a portfolio.

  sourced   loan attributes at origination, final loan_status, last_pymnt_d, recoveries
  derived   balance, days past due, status and provisions at a given snapshot date

Balance follows the annuity schedule implied by the loan's own terms and stops accruing
at the last recorded payment. Status transitions follow the observed outcome: loans that
were settled close at their final payment; loans that were charged off accrue days past
due from theirs.

## Consequences
- Portfolio composition, vintage distribution and grade mix are real. Only the path
  between origination and final state is derived.
- The derived book was checked against portfolio metrics before being accepted. The
  first version had no write-off path, which drove the defaulted population to 19.6% of
  the book. With write-off at 180 days past due the NPL ratio sits between 1.25% and
  2.03% across 2018 and fluctuates rather than accumulating.
- Loans issued after the extract cut-off do not exist, so the book only grows from
  loans already in the dataset.
- Days past due is accurate to the month, not the day (see data_dictionary.md).

## Rejected alternatives
- Treating the single extract as one snapshot: no daily cycle, no month-end series,
  no way to exercise reprocessing or backfill.
- Fully synthetic portfolio: removes the real distributions and the data quality
  problems that make validation worth building.
