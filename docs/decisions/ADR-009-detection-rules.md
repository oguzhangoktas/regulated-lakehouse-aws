# ADR-009: Detection rules chosen by measurement, not assumption

## Context
Transaction monitoring needs rules that flag suspicious transactions with usable
precision. The obvious candidates were velocity (an account moving money unusually
fast) and amount or balance thresholds. Each was measured against the real fraud
label rather than assumed to work.

## What the data showed

**Velocity does not discriminate in this data.** The source records at most one
transaction per fraudulent account and three per legitimate account across the whole
period — accounts do not repeat. A windowed "many transactions in a short interval"
rule therefore has almost nothing to act on. An early velocity run appeared to catch
33% of fraud, but that was unrelated accounts co-occurring in a window, not a real
velocity pattern.

**Threshold and drain rules hit a wall of class imbalance.** Fraud is 0.13% of
transactions. Even signals that separate fraud from legitimate at 50–65% cannot lift
precision above a few percent, because the legitimate tail alone outnumbers all fraud
by hundreds to one. Calibrating thresholds did not change this.

**The whole-account sweep signature is decisive.** Fraud transfers the origin's
entire opening balance, to the cent: measured at 96.4% of fraudulent transfers and
99.4% of fraudulent cash-outs, against 0.0% of legitimate transactions. A person
leaves a remainder; automated fraud sweeps the account.

## Decision
The whole-account sweep is the detector: a transaction on TRANSFER or CASH_OUT whose
amount equals the origin's opening balance within a cent. Measured on the real label:
97.9% recall at 100% precision, against the source's own flag rule at 0.19% recall.

Velocity is kept in the codebase as a worked example of event-time windowing and
watermarking, and is exercised by a synthetic red-team scenario (ADR-010), but it is
not a detector for this data and is documented as such.

## Consequences
- Detection is a per-transaction test, not a windowed one, because the signal lives
  in a single transaction's balance movement.
- The rule is specific to a dataset where fraud sweeps the account. On data where
  fraud is partial, the signature would differ and the rule would be re-derived by the
  same measure-first method.
- Rules that do not discriminate are removed from detection rather than tuned
  indefinitely; the measurement, not intuition, decides.

## Note on method
Each rule was validated against ground truth before being kept. Two plausible rules
were discarded on evidence and one non-obvious rule was adopted on evidence. The
discipline — measure against the label, keep what works — matters more than any single
rule.
