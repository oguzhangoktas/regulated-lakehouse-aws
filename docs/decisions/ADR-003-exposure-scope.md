# ADR-003: Exposure scope for the risk engine input

## Context
RWA requires capital only against OPEN risk. Profiling of the real portfolio
(2,260,701 loans) showed:
- `Fully Paid` = 47.63% of rows, `out_prncp` = 0 (no exposure left)
- `Current` = 38.85% (live exposure)
- `Charged Off` = 11.88% — this is the real impairment population
- literal `Default` = only 40 rows (0.00%) — a trap: using it as the default definition
  would mis-state the entire portfolio

## Decision
An exposure enters the engine input if:
  `outstanding_amount > 0` **OR** `status = defaulted`

Status mapping:
- `Current`                              -> performing
- `In Grace Period`, `Late (16-30)`      -> past_due
- `Late (31-120)`                        -> past_due (dpd >= 31)
- `Charged Off`, `Default`               -> defaulted
- `Fully Paid`                           -> EXCLUDED (no exposure, no capital)
- `Does not meet the credit policy...`   -> flagged as out-of-policy, segmented separately

Defaulted exposures are NOT dropped: under Basel they carry their own risk weights,
net of specific provisions, so they are passed to the engine in a separate bucket.

## Consequences
- Engine input is roughly half the raw portfolio — scope is a business rule, not a filter of convenience.
- Settled loans remain in the PLATFORM (silver/gold) for business reporting, vintage
  performance and historical analysis; they simply do not go to the engine.
- This makes explicit that **engine input scope != reporting scope**.

## Rejected alternatives
- Sending all 2.26M rows: wrong (no exposure = no capital) and wasteful.
- Dropping defaulted exposures: wrong — defaulted exposures still consume capital.
