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

## What these experiments established

| Fault | Caught by | Published |
|---|---|---|
| Duplicated grain | dataset assertion | nothing |
| 2.02% quarantined against a 1% limit | dataset assertion | nothing |
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
