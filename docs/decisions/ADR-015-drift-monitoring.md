# ADR-015: Drift monitoring beside the pipeline

## Context

The contracts validate a dataset against itself: types, ranges, relationships between
columns, uniqueness of the grain. That covers a large class of faults and stops them
before anything publishes.

It does not cover faults that leave the data internally consistent. Scaling every amount
by the same factor preserves every ratio the contract checks. A feed delivering half the
book produces rows that are all individually valid. The previous snapshot re-delivered
unchanged is, by construction, valid data. A rating distribution that shifts wholesale
stays inside the allowed values. A vendor engine applying the wrong risk weights still
returns output where RWA equals exposure times weight.

Injecting the first of those made the size of the gap concrete: multiplying every amount
by a hundred passed every rule, quarantined nothing, and moved the reported RWA from
5.40bn to 540.34bn. Nothing in the platform was positioned to notice, because every check
it had was asking whether the dataset was consistent with itself, and it was.

Each of those faults is invisible within a period and obvious against the period before.

## Decision

A drift monitor (`dataplatform/quality/drift.py`) reduces a reporting period to a few
measures — exposure count, total exposure, total RWA — and compares them against the
period before. A measure that moves more than a tolerance allows, or does not move at
all, is reported as a breach.

It runs beside the pipeline, not inside it. It reports; it does not halt.

## Why a monitor and not a gate

Because a gate has to be certain and this signal is not.

A duplicated grain is wrong. There is no reading of the business in which the same
exposure legitimately appears twice for one reporting date, so the contract can refuse to
publish and be right every time. That certainty is what earns it the authority to stop a
run.

A book moving sharply is different. A portfolio acquisition, a securitisation, a change
in origination volume — each moves the measures hard and each is legitimate. A gate on
this signal would stop the pipeline on real business events, and the predictable response
to a gate that blocks legitimate work is that someone widens the threshold until it stops
firing. The control survives as configuration and dies as a control.

Reporting keeps the signal honest. A breach means a person looks and decides, which is
the correct response to evidence that is strong but not conclusive.

This also matches how the two kinds of check are operated in practice: validation belongs
in the job, observability belongs next to it.

## Why both directions

Too much movement is one fault. None at all is another: a book of this size does not
reproduce itself to the cent between reporting dates, so measures identical to the prior
period point at a feed that delivered the same file twice, or a job that read the wrong
partition. A tolerance band alone would pass that silently, so an exact match is reported
as its own kind of breach.

## Consequences

- The class of faults that survives internal validation now has somewhere to be caught.
  Against the November book, the hundredfold fault reported as a 100.69x move in total
  exposure with the exposure count unbreached — which is the diagnosis as well as the
  alarm, since the same number of exposures carrying a hundred times the money is a unit
  problem rather than a volume one.
- Because it is not wired into the jobs, a fault surfaces when the monitor runs rather
  than before the data publishes. This is detection, not prevention, and the gap between
  the two is however long it takes someone to look.
- The tolerance is one number for every measure. Row count and total exposure do not
  really deserve the same band, and a per-measure tolerance is the obvious next step.
- The measures are supplied by the caller rather than fixed, so a domain declares what is
  worth watching. Nothing yet declares them for transaction monitoring, where alert volume
  and catch rate are the equivalents.
- Comparing a period against its predecessor is weaker than reconciling against a control
  total published by the source, which is what a bank does. That remains unavailable here
  because the simulated source publishes no totals, and it is the stronger control if one
  ever does.
