# ADR-004: Snapshot retention window

## Context
The source lands a full snapshot of the exposure table daily (ADR-002). Measured
size is 66MB per snapshot in Parquet at ~1.06M exposures. A full year of daily
snapshots is roughly 24GB.

The engine consumes the month-end snapshot only, and runs monthly. Daily snapshots
are required to exercise the daily load cycle, not to produce regulatory figures.

## Decision
Retain daily snapshots for a rolling window, month-end snapshots for history:

  - daily     December 2018 (31 snapshots)
  - month-end January to November 2018 (11 snapshots)

Approximately 2.7GB in total.

This follows the source system's own retention policy: daily detail is held for a
limited window, month-end positions are archived for regulatory reporting.

## Consequences
- The daily load cycle can be run and backfilled over December.
- Twelve month-end reporting dates are available for engine runs and trend reporting.
- Daily coverage does not exist outside December. Downstream code must not assume
  a continuous daily series across the full history.

## Rejected alternatives
- Full year of daily snapshots: 24GB for no additional capability.
- Month-end only: the daily load cycle could not be exercised or tested.
