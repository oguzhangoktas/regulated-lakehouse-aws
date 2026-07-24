# Platform map

One page for the whole platform: what a record passes through between arriving and being
reported, and what checks it at each step.

The spine is vertical and shared by both domains. The controls are not inside the layers —
they sit on the transitions between them. Five layers, four boundaries, and one control that
deliberately runs beside the spine rather than on it.

`architecture.md` explains why each element is the way it is. The ADRs carry the decisions.
This file carries the shape.

---

## The spine

```text
          credit_risk (batch)                      txn_monitoring (stream)
          ───────────────────                      ───────────────────────

            loan tape, CSV                            Kafka topic `transactions`
       daily T-1 full snapshot                        6,362,620 messages
       append-only, no CDC                            local KRaft broker; MSK in AWS
                   │                                            │
 ══════════════════╪═════════ 1 · ARRIVAL ══════════════════════╪══════════════════
                   │                                            │
                BRONZE                                        BRONZE
       Parquet, partitioned by                     Iceberg, append from Kafka,
       snapshot_date; as received;                 offsets held by the checkpoint;
       read-only in the Glue role                  no transformation
                   │                                            │
 ══════════════════╪═══════ 2 · CONFORMANCE ════════════════════╪══════════════════
                   │                                            │
                SILVER                                        SILVER
       typed, deduplicated,                        same contract engine, reached
       row rules in code,                          through foreachBatch;
       quarantine table                            quarantine table
                   │                                            │
 ══════════════════╪════ 3 · ENGINE BOUNDARY ═════════════════╪═══════════════════
                   │                                            │
                 GOLD                                          GOLD
       scope filter → engine input                 sweep / structuring / velocity
       → vendor RWA engine (mock)                  rules → alerts, partitioned by rule
       → rwa_output                                watchlist screening on
       + reconciliation                            destination names
                   │                                            │
 ══════════════════╪═══════ 4 · PUBLICATION ═════════════════╪═════════════════════
                   │                                            │
                   └──────────────────┬─────────────────────────┘
                                      │
                             GOLD_REPORTING  (dbt)
                    rwa_monthly_trend     capital_by_grade
                    alerts_by_rule        alerts_by_type
                    aggregate only; no individual reaches this layer


        ┌──────────────────────────────────────────────────────────────┐
        │  drift monitor — reads gold across periods, off the spine.    │
        │  Reports; does not block. See "The side note" below.          │
        └──────────────────────────────────────────────────────────────┘
```

Everything above the reporting layer is per-domain. The reporting layer is where the two
domains meet, and they meet only as aggregates.

---

## 1 · Arrival

**What runs.** Batch: the source file is parsed at the CSV boundary, where types are coerced
once and for all, and written whole into a `snapshot_date` partition. Stream: a Structured
Streaming consumer appends to bronze with a checkpoint holding the offsets; the sink is
`append`, so exactly-once rests entirely on that checkpoint directory.

**What it catches.** A source that is absent: silver reads a path, and a missing path fails
loudly. Type damage at the CSV boundary, which is where type drift actually lives — bronze is
Parquet, so beyond this point the schema is fixed by the file format. Redelivery of messages
already consumed, as long as the checkpoint is intact.

**What it cannot catch.** Anything about meaning. Bronze is immutable and as-received by
design, so no semantic check exists here — the layer's purpose is to be the thing you can go
back to, which requires that it not have been improved. The checkpoint is a single point of
truth with two silent failure modes, both measured: a checkpoint written under the wrong
filesystem scheme means no data arrives, and a checkpoint deleted means every message arrives
a second time and is appended (100 messages became 200 rows over 100 distinct keys). Neither
raises an error.

---

## 2 · Conformance

**What runs.** Conform to declared types, then row rules. Failing rows go to a quarantine
table, which is written *before* any assertion is evaluated, so the evidence is on disk even
when the job stops. Credit risk carries its rules inline in code; the stream reaches the same
contract engine through `foreachBatch`, which is the one place where the batch and streaming
paths provably share a quality path rather than resembling one.

**What it catches.** Null, range and relational faults per row. Poison messages resolve
cleanly: malformed or non-JSON payloads null the whole struct and fire eleven rules; a single
field with the wrong type nulls only that field, so nine rules still pass and the quarantine
record names exactly what broke.

**What it cannot catch.** Three structural gaps, all located by experiment rather than by
reading:

- Silver has no dataset assertions, only row rules. An empty-but-present bronze partition
  passes here quietly and is caught two layers later.
- An unknown extra field is dropped silently and the row publishes. Forward compatibility is
  the intended behaviour; nobody noticing that a producer changed shape is the cost of it.
- There is no contract between silver and the gold build. A column lost inside silver
  surfaces only as a Spark `AnalysisException`, which names a column and then suggests
  unrelated ones — against the boundary error one layer down, which names contract, version
  and field.

---

## 3 · Engine boundary

The strongest gate on the platform, and the only one with a versioned declaration behind it.

**What runs.** A YAML contract, loaded by the contract engine: grain, declared fields, row
rules, and dataset assertions. `min_rows` is evaluated first, because when several things are
wrong its message is the one worth reading. Unique-grain and quarantine-ratio assertions
follow. The input is scope-filtered, handed to the engine, and what comes back is reconciled
against what went in.

**What it catches.**

- A duplicate grain. Worth stating plainly: all 1,039,783 rows passed every row rule
  *including* the duplicate. Row-level validation is structurally blind to a set-level fault.
- A quarantine ratio above its limit — 2.02% against a 1% limit halts the run and withholds
  1,018,769 valid rows with it, because a partially published regulatory figure does not look
  partial.
- An absent input publishing as a successful zero. Every other assertion is a negative claim,
  and on an empty set every negative claim is vacuously true; `min_rows` is the one positive
  claim, and it counts rows that *arrived* rather than rows that passed.
- The engine dropping an exposure or distorting an EAD, through reconciliation, which is
  fault-injected in tests rather than assumed.
- Minimisation. Eight borrower attributes present in silver are not declared by the contract,
  so the projection removes them and they never reach the engine, its output, or reporting.

**What it cannot catch.** One family of faults, and it is a large one. Every rule checks the
data against itself, so a fault that is *uniform* is invisible: multiplying every money column
by 100 passed every rule, quarantined nothing, and moved RWA from 5.40bn to 540.34bn.
The same blindness covers a truncated feed above `min_rows`, a stale snapshot re-delivered
unchanged, a wholesale distribution shift, and an engine applying weights that are wrong but
consistent. Contracts protect structure, not meaning.

Also: declaration is not enforcement. The engine reads contract, version, grain, fields, row
rules and dataset assertions. Anything else written in the YAML — a `pii:` marking, a
declared decimal precision — is documentation, enforced elsewhere or not at all.

---

## 4 · Publication

**What runs.** dbt models over gold, sources declared rather than referenced by string, with
schema and uniqueness tests. The reporting schema is a separate database in the catalog.

**What it catches.** Broken references, failed not-null and uniqueness tests, and any
individual identifier reaching the reporting layer — asserted in CI, since it is a schema
property and belongs at build time rather than run time.

**What it cannot catch.** A number that is wrong but well formed. Every test at this boundary
is structural. Correctness of the figure is settled at boundary 3, or it is not settled.

---

## The side note

The drift monitor sits beside the spine. It reduces a period to a small set of measures,
compares them with the prior period, and reports both movement outside tolerance and the
absence of movement, which is what a re-delivered stale snapshot looks like. Against the
faulted period it read total outstanding at 100.69x and total original at 100.72x while the
exposure count stayed within tolerance — which is the diagnosis as much as the alarm: same
rows, a hundred times the money, so a unit problem rather than a volume problem.

It reports and does not block, and that is the design rather than an unfinished edge. A gate
must be certain — a duplicate grain *is* wrong. A drift signal is probabilistic; an
acquisition moves a book legitimately. A gate that blocks legitimate work has its threshold
widened until it never fires, at which point the control is gone but the documentation still
claims it. Not wiring the monitor into the jobs keeps the distinction honest.

---

## Where the controls live

| Boundary | Control | Implementation |
|---|---|---|
| 1 | Bronze immutability | Glue role grants read-only on bronze |
| 1 | Stream exactly-once | Structured Streaming checkpoint, `append` sink |
| 2 | Row rules, credit | `domains/credit_risk/` silver job, inline rules |
| 2 | Row rules, stream | contract engine via `foreachBatch` in the silver stream job |
| 2 | Quarantine | written before assertions are evaluated, both domains |
| 3 | Contract engine | `dataplatform/quality/contract.py` + the contract YAMLs |
| 3 | Reconciliation | credit risk gold job, engine output vs engine input |
| 3 | Minimisation | absence of declarations in the engine-input contract |
| 4 | Reporting tests | `dbt/` — sources, schema tests |
| 4 | No individuals in reporting | `tests/test_data_governance.py` |
| beside | Drift monitor | `dataplatform/quality/drift.py`, run out of band |
| beside | Table maintenance | `dataplatform/lakehouse/maintenance.py`, reports unless `--apply` |

Point-in-time is held in three separate places, on purpose: `snapshot_date` partitions are the
regulatory record, Iceberg snapshots are the operational history, and S3 object versioning is
a short undo window. Only the first is a statement about the business.

---

## Drawing order

The map is meant to be reproducible on a whiteboard from memory. In order:

1. One vertical line. Five boxes on it: source, bronze, silver, gold, reporting.
2. Four horizontal bars across the line, between the boxes. Number them 1 to 4.
3. Split the spine into two columns, batch on the left, stream on the right. They rejoin at
   reporting and nowhere else.
4. At each bar write three things: what runs, what it catches, what it does not.
5. One box off to the side, touching gold, with an arrow that reports rather than blocks.

Steps 1 and 2 are the argument. The rest is detail.
```


<invoke name="present_files">
<parameter name="filepaths">["/mnt/user-data/outputs/platform_map.md"]