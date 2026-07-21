# ADR-010: Synthetic red-team scenarios for typology coverage

## Context
The detection rules are measured against PaySim's real fraud label: the sweep rule at
97.9% recall, the watchlist screen against seeded near-variants. But two fraud
typologies that matter in this domain do not occur in PaySim at all:

- **structuring** — splitting a large movement into several transfers each just under a
  reporting threshold. PaySim has no reporting-threshold behaviour.
- **velocity** — an account moving money many times in a short window. PaySim accounts do
  not repeat (at most one to three transactions each), so there is no velocity to detect.

Two rules address these typologies — a structuring rule and the velocity rule — but with
no examples in the real data, they cannot be measured against the label the way the sweep
rule is.

## Decision
Fraud is never fabricated into the real stream. Doing so would be circular: a rule
"catching" fraud that the same author invented proves nothing, and it would corrupt the
precision and recall measured on the real label, which is the project's main evidence.

Instead, a small, clearly-labelled synthetic test set exercises the two typologies,
kept entirely separate from the PaySim stream (`domains/txn_monitoring/testdata/`). Each
generated transaction carries a `scenario` label, so a rule's hits are checked against
the pattern it is meant to catch. This is typology coverage, not a fraud-detection
measurement, and it is documented as such.

## Why this is the honest split
- **Real fraud, real label, real measurement.** The sweep rule and the watchlist screen
  are measured against ground truth that exists independently of this project. Those
  numbers stand on their own.
- **Synthetic typologies, labelled, separate.** Structuring and velocity are known
  real-world patterns absent from this dataset. Testing them on synthetic scenarios shows
  the rules work, without pretending the coverage is a measurement of real fraud.
- **No mixing.** The synthetic set never enters the PaySim bronze/silver/gold path, so no
  headline metric is inflated by invented data.

This mirrors real practice: fraud teams generate synthetic typology tests to check rule
coverage, alongside — never blended into — measurement on labelled production data.

## Consequences
- The velocity rule stays in the codebase with a clear purpose: it does not discriminate
  in PaySim (ADR-009), but it is a working, tested rule for a typology this data lacks,
  exercised by the red-team set.
- The structuring rule is added on the same basis: a real typology, tested synthetically
  because the data has no natural example.
- Any reader can see exactly which numbers are measured on real fraud and which are
  coverage on synthetic scenarios. The distinction is explicit, not buried.
