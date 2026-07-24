# ADR-016: Table history is retained without expiry

## Context

Iceberg records every commit as a snapshot, and a snapshot keeps its data files alive.
Files superseded by a later write are not deleted while any retained snapshot still
references them, so a table's storage grows with its edit history as well as with its
contents.

Measured across the credit-risk tables:

| Table | Snapshots | Live | Retained | Overhead |
|---|---|---|---|---|
| silver_credit_risk.exposure | 31 | 310 MB | 797 MB | 157% |
| gold_credit_risk.engine_input | 26 | 243 MB | 525 MB | 116% |
| gold_credit_risk.rwa_output | 26 | 255 MB | 551 MB | 116% |

About a gigabyte of superseded data across three tables, and the object listing for the
silver prefix confirms it is physically present: 143 objects totalling 836 MB against 12
live files totalling 310 MB.

Two things about that were not previously true on paper.

**The S3 lifecycle rule cannot reclaim it.** The buckets expire noncurrent object
versions after thirty days, which was written for the bronze pattern where reloading a
snapshot date deletes and rewrites the same keys. Iceberg never overwrites a data file —
it writes new ones and repoints the metadata — so its superseded files are current
versions of distinct objects and that rule never sees them. Only snapshot expiry reclaims
them.

**ADR-006 describes a bound that does not exist.** It records table history as
"operational — recent versions of a table, bounded by snapshot expiry". Nothing expires
snapshots, so the history is not bounded by anything. That sentence is corrected by this
decision.

## Decision

History is retained without expiry, deliberately, and the cost is accepted as measured.

Maintenance is available as code (`dataplatform/lakehouse/maintenance.py`) with a stated
retention — thirty days, keeping at least five snapshots — which is not applied. It
reports by default and acts only when explicitly asked, because expiry is irreversible.

## Why retain

**The cost is small enough to be the wrong thing to optimise.** A gigabyte on S3 Standard
is a few cents a month. What it buys is the ability to read a table as it was and to roll
back a bad write without reprocessing. Trading that for cents is a poor exchange, and
expiring history to demonstrate that the platform can expire history would be theatre
rather than engineering.

**The loss is not recoverable and the gain is.** Storage can be reclaimed at any later
point; a snapshot, once expired, cannot be recreated. Where one side of a decision is
reversible and the other is not, the reversible side is the safer default until there is
a reason to move.

**Reprocessing does not depend on it.** Worth being precise, because it is easy to keep
history for the wrong reason: a backfill reads bronze and rewrites the partition, so it
needs no snapshot at all. The regulatory record is the `snapshot_date` partitions, which
are immutable and untouched by expiry. What history provides is narrower than it first
appears — reading the table as it stood, and rolling back a write that should not have
happened.

## What would change it

- **Scale.** The overhead is a ratio, and a ratio of 157% is uninteresting at 300 MB and
  material at 300 TB. The figure to watch is retained storage against live storage, which
  `maintenance.storage()` reports.
- **Write frequency.** These tables carry roughly two and a half writes per partition,
  accumulated during development rather than by a monthly cycle. The micro-batch
  deployment the transaction-monitoring DAG describes writes every six hours, which
  produces snapshots at a rate where both expiry and compaction become routine rather
  than optional.
- **A retention obligation of its own.** If a regulator asked how far back a table could
  be reproduced, the answer would need a stated period rather than "whatever has
  accumulated". The thirty-day default is already chosen for that case: it matches the
  object versioning policy and covers a monthly reporting cycle, which is how long a bad
  write can go unnoticed.

## On compaction

The same maintenance module rewrites files towards a target size. It is not needed here
either, and for a reason worth recording: the live files average 25 MB across twelve
partitions, and the streaming tables hold two files of 83 MB. Small files come from
frequent small writes, and this platform writes infrequently and in bulk — a full topic
consumed in `availableNow` mode is one large batch, not thousands of micro-batches.

A deployment writing every six hours would accumulate them, which is the same condition
that makes expiry routine. Both are properties of write frequency rather than of data
volume.

## Consequences

- Storage grows with edit history, and the ratio is a number someone has to look at
  rather than a threshold that fires. Nothing alerts on it.
- Retention is a decision on the record rather than an absence. The distinction matters:
  the previous state was not "we chose to keep everything" but "nothing removed anything
  and a document said otherwise".
- The maintenance procedures are code with a stated policy rather than a command to be
  recalled during an incident. Their default is to report.
- ADR-006's description of history as bounded is superseded by this.
