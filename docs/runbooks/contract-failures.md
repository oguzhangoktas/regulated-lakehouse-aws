# Runbook: contract and schema failures

What to do when a credit-risk job stops at a contract, and what to do about the one
failure that does not stop anything.

Each section starts from what you see, because that is what you have at the point you
need this. The behaviour described was produced deliberately by the experiments in
`chaos/` against the December 2018 book of 1,039,782 exposures, so the figures are
observed rather than expected.

---

## "N rows duplicate the grain"

```
ContractViolation: credit_risk_exposure_input 1.0.0:
  1 rows duplicate the grain ['exposure_id', 'reporting_date']
```

**What it means.** An exposure appears more than once for the reporting date. Nothing
published — the gold table still holds whatever the last successful run wrote for that
date, so downstream reporting is stale rather than wrong.

**Diagnose.** Find the repeated keys in the layer feeding the job:

```sql
select exposure_id, snapshot_date, count(*)
from silver_credit_risk.exposure
where snapshot_date = date '<reporting_date>'
group by 1, 2
having count(*) > 1
```

If silver is clean, the duplication came from the transform rather than the feed — check
whether a join was added that can match more than one row on the right.

Usual causes, most common first: the source delivered the same file twice; a rerun
appended instead of replacing a partition; a join fanned out.

**Recover.** Fix the cause upstream, then rerun the date. The job overwrites the
partition, so a rerun converges rather than compounding — there is nothing to clean up
first.

**Why this check exists.** A duplicate is invisible to row-level validation. In the
experiment all 1,039,783 rows passed every row rule, including the duplicate: each row
was individually perfect. The fault was in the set, not the record, and only a
dataset-level assertion can see it. Left unstopped it becomes double counting — an
overstated exposure that looks entirely plausible in a report.

---

## "X% of rows quarantined, limit is Y%"

```
ContractViolation: credit_risk_exposure_input 1.0.0:
  2.02% of rows quarantined, limit is 1.0%
```

**What it means.** Individually bad records crossed a share the contract treats as
systemic. Nothing published, including the 1,018,769 rows that were entirely valid.

**Diagnose.** Quarantine is written *before* the assertion runs, so the evidence is
already on disk when the job fails. Start there:

```sql
select failed_rules, count(*)
from quarantine_credit_risk.engine_input
where reporting_date = date '<reporting_date>'
group by 1
order by 2 desc
```

One rule dominating points at a single upstream change — a new value in an enumerated
field, a column that started arriving null. Failures spread evenly across rules point at
something structural: a shifted delimiter, a partial load, the wrong file.

**Recover.** Fix the source and rerun the date. If the source is right and the contract
is too strict — a genuinely new rating grade, say — the contract changes, with a version
bump, and that is a review rather than an incident.

**Why the whole set stops.** A few bad records are a defect and can be isolated; a large
share means the source is wrong and the surviving rows cannot be trusted just because
they happened to parse. In regulatory reporting a partial publication is worse than none:
an incomplete figure does not look incomplete, it looks finished and wrong. A late number
can be explained; a quietly short one cannot.

---

## "N rows arrived, at least M expected"

```
ContractViolation: credit_risk_exposure_input 1.0.0: 0 rows arrived, at least 1 expected
```

**What it means.** There is no data upstream for the reporting date. Nothing published,
and the gold table still holds whatever the last successful run wrote.

**Diagnose.** Establish how far up the absence goes:

```sql
select snapshot_date, count(*)
from silver_credit_risk.exposure
where snapshot_date >= date '<reporting_date>' - interval '3' month
group by 1
order by 1
```

A gap in silver sends you to bronze. If bronze holds the partition and silver does not,
the silver job ran and wrote nothing, which is a different fault with the same symptom.

Usual causes: the source did not deliver; the silver job was skipped, or ran for a
different date; a backfill was triggered for a date the chain has not processed yet.

**Recover.** Process the missing upstream date, then rerun this one. Nothing needs
undoing first — the halt happened before any write.

**Why this check exists.** It was added because without it the chain completed
successfully on an empty input. Traced stage by stage against a date with no data, the
input contract passed, the output contract passed, reconciliation passed, and the run
reported zero exposures and an RWA of 0.00bn as a success.

Every other assertion is a statement that nothing is wrong: no row duplicates the grain,
no excessive share is quarantined, what was sent equals what returned. On an empty
dataset all of them are true, because there is nothing there to be wrong. Only an
assertion that something is expected can tell an absent dataset from a clean one.

That distinction matters more here than the arithmetic faults. A wrong figure is at least
a figure, and looks odd next to last month. A missing figure reported as zero looks like a
bank that held no risk, and nothing downstream is built to question it.

**What this check does not cover.** It is declared on the credit contracts and not on the
streaming one, because a micro-batch with no new messages is legitimate. And the credit
chain still has an asymmetry: silver has no dataset assertions of its own, so a bronze
partition that exists but is empty passes through silver quietly and is caught two layers
later at the engine boundary rather than where it entered.

---

## A column is missing

This surfaces in two different places depending on where the column disappeared, and the
two errors are not equally useful.

**Upstream, before the transform:**

```
AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter
with name `interest_rate` cannot be resolved. Did you mean one of the following?
[`maturity_date`, `snapshot_date`, `default_flag`, ...]
```

**At the contract boundary:**

```
ContractViolation: credit_risk_exposure_input 1.0.0: missing fields ['interest_rate']
```

**What it means.** Either way nothing published, which is correct: a missing field is a
schema break, and no row-level outcome is meaningful when the shape is wrong.

**Diagnose.** Compare what the layer holds against what the next stage expects:

```sql
select column_name
from information_schema.columns
where table_schema = 'silver_credit_risk' and table_name = 'exposure'
```

Then check the failing stage's requirements — the contract YAML if the error is a
`ContractViolation`, the transform's `select` if it is an `AnalysisException`.

**Note the asymmetry.** The contract names the contract, its version and the missing
field. The Spark error names a column and suggests unrelated ones; it does not say what
needed it or why. That difference is not luck — the contract error is better because
something is declared at that boundary. Between silver and the engine input build there
is no such declaration, so a column lost in silver produces the weaker error. Adding a
check that the build's input carries what it reads would close that gap cheaply.

**Recover.** Restore the column upstream and rerun. If the column is gone for good, the
contract changes with a version bump and the transform follows.

---

## Nothing failed, and the figures are wrong

There is no error here. That is the finding.

**What was done.** Every money column arrived multiplied by a hundred, as it would if a
source began sending minor units. Nothing else changed.

**What happened.** Every validation passed. Zero rows quarantined. The reported RWA moved
from 5.40bn to 540.34bn and the run looked healthy at every stage.

**Why nothing caught it.** Each rule checks the data against itself. Amounts stayed
positive, so the minimum held. Types were unchanged. The grain stayed unique. The
relationships between amounts survived because every amount scaled together —
`outstanding <= original * 1.1` compares two numbers that both grew a hundredfold, and
holds exactly as before.

Contracts protect structure. They do not protect meaning. A uniform change of scale is
invisible to every self-referential check, and no rule in the contract anchors a
magnitude to anything outside the dataset.

**Detect.** No contract will. The drift monitor does: it reduces a period to a few
measures and compares them against the period before (ADR-015). Against the November
book, the faulted December period reported

```
total_outstanding:  11,337,494,677.02 -> 1,141,589,430,564.00  (100.69x)
total_original:     16,350,044,250.00 -> 1,646,772,077,500.00  (100.72x)
```

with `exposure_count` unbreached. That last part is the diagnosis as well as the alarm:
the same number of exposures carrying a hundred times the money is a unit problem, not a
volume one. The ratio is 100.69 rather than 100 because the baseline is the prior month,
so the book's own movement is folded in — the monitor measures change between periods,
not absolute truth.

The monitor reports rather than halting, and is not wired into the jobs, so it catches
this after publication rather than before. Run it against a period when a figure looks
wrong, and on a schedule if the class of fault matters more than the cost of looking.

**What it still does not cover.** A control total published by the source would be
stronger: the engine boundary reconciles what the vendor returns against what was sent,
but the ingestion boundary does not reconcile what the source sent against what the
source says it sent. That asymmetry remains, and closing it needs the source to publish
totals. An absolute bound on a credible loan amount would catch this particular fault but
is brittle, and misses any rescaling that stays inside the bound.

**Recover.** Correct the units at ingestion and rerun the affected dates. Because the
jobs overwrite by partition, reprocessing restores the correct figures without cleanup.
The harder part is the reporting already issued on the wrong numbers, which is a
disclosure problem rather than a pipeline one.

---

## Rerunning a date

Every recovery above ends the same way: fix the cause, rerun the date. That is safe
because a job replaces the partitions its output covers rather than appending to them,
and the behaviour was measured rather than assumed:

| Action | Rows after |
|---|---|
| process a date | 1,000 |
| process the same date again | 1,000 |
| rerun producing half as many rows | **500** |
| process a neighbouring date | 1,500 across both |
| rerun the first date at full size | 2,000 across both |

Two things to know before you rerun something at three in the morning.

**A rerun that produces fewer rows shrinks the partition.** It does not merge with what
was there. That is correct — the source is authoritative and a partition should hold what
the current run says it holds — but it means a partial upstream fix leaves a partial
partition rather than the previous fuller one. If the rerun's input is incomplete, fix
the input before rerunning rather than after.

**Writing one date does not disturb another.** Partitions are replaced individually, so a
backfill can process months in any order and a rerun of one month cannot damage the rest.
This is what makes the credit-risk backfill safe to restart from the middle.

---

## What these experiments established

| Fault | Caught by | Published |
|---|---|---|
| Duplicated grain | dataset assertion | nothing |
| 2.02% quarantined against a 1% limit | dataset assertion | nothing |
| No data for the reporting date | nothing, until an arrival check was added | **a successful run reporting 0.00bn** |
| Column missing upstream | the transform, weakly | nothing |
| Column missing at the boundary | the contract, clearly | nothing |
| Money in the wrong units | no contract; the drift monitor, after the fact | **everything, wrong by 100x** |

Three of the four faults were stopped by the layer built to stop them. The fourth passed
every gate because every gate asks whether the data is internally consistent, and it was.

The conclusion is not that the contracts are weak. It is that they answer one question —
is this dataset structurally sound — and that a second question, is it plausible compared
to what came before, needed a different mechanism. The contracts are gates and must be
certain; drift is a monitor and reports. Both are needed, and neither substitutes for the
other.
