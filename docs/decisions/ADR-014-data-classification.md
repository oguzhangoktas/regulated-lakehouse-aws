# ADR-014: Data classification and personal data handling

## Context

The platform describes itself as regulated, and a regulated platform is expected to know
what personal data it holds, where it flows, and what protects it. Several controls were
already in place — direct identifiers excluded at ingestion, a pseudonymous customer key,
per-layer buckets with least-privilege access, encryption at rest — but they had been
introduced one at a time for local reasons. There was no statement of what is sensitive
and why, and no position on the questions a regulated deployment is asked: how personal
data is minimised, how strong the pseudonym is, and what happens when erasure is
requested.

Two properties of this platform make those questions sharper than they look. Its most
sensitive artefact is not a personal attribute but an assertion: a screening alert claims
that a payment beneficiary may be a sanctioned or politically exposed person, produced by
a fuzzy match that is wrong some of the time by construction. And its core design is
immutability — snapshot partitions, table history, object versioning — which is exactly
what an erasure request cuts against.

## Decision

### Classification

Four levels, applied across both domains and every layer, recorded in
`data_classification.md`: PUBLIC, INTERNAL, CONFIDENTIAL for pseudonymous keys and
re-identifying attributes, and RESTRICTED for assertions about a party — fraud labels,
watchlist entries and screening alerts.

Separating the last level from the rest is the substantive part. A balance is a fact
about an account; a screening hit is a claim about a person, and a wrong one causes harm
on its own. Those need different handling even though the claim carries less data.

### Minimisation is enforced at the engine boundary

Silver holds the whole book, including borrower attributes: income, debt-to-income, FICO
range, employment length, home ownership, verification status, application type. The
engine input contract does not declare any of them, and the contract engine projects to
its declared fields. Eight attributes therefore stop at that boundary and appear in
neither the engine input, the RWA output, nor reporting.

This was already true; the decision is to treat it as a control rather than a side
effect. The engine computes capital from exposure and rating, so it is given exposure and
rating. Adding a field upstream cannot change that without a deliberate change to the
contract, which is reviewable.

### Pseudonymisation, and its accepted weakness

`customer_id` is an unsalted SHA-256 of the source loan id, truncated. The loan ids are
published in the source file, so anyone holding it can hash all of them and invert the
mapping. This is pseudonymisation rather than anonymisation, and a weak form: it removes
the identifier from casual view but not from a determined holder of the source.

It is kept as is, because the source carries no real person and a stronger scheme would
add key management without protecting anyone. The production form is recorded instead: a
keyed HMAC with the key held in a managed key store, which resists inversion by anyone
without the key, at the cost of managing that key and of breaking the mapping when it
rotates.

### Encryption stays with S3-managed keys

Default server-side encryption is enabled on every layer bucket and on Athena results,
using S3-managed keys. A customer-managed key would add a key-usage audit trail in
CloudTrail and a key-level access policy on top of IAM, at a per-request cost.

For a platform holding no real personal data on a fixed budget, the audit trail has
nothing to attest and the cost is real, so SSE-S3 is chosen. The condition that would
reverse this is explicit: real personal data, or an auditor who needs to see who
decrypted what, requires the customer-managed key.

### Declaration is not yet enforcement

The contract YAMLs mark `customer_id` as `pii: pseudonymised`. The contract engine reads
schema, row rules and dataset assertions, and does not read that marking. It is
documentation.

It stays that way for now rather than being quietly presented as a control. Enforcement
has a clear shape when it is worth building: the engine refuses to publish a field marked
as raw personal data across a boundary, and asserts the shape of one marked pseudonymised,
so that removing the hash in `build_loan_master` would fail the contract rather than
silently ship identifiers downstream.

## Erasure and immutability

An erasure request under GDPR Article 17 cuts directly against this platform's design. A
party appears in every daily bronze partition since origination; Iceberg retains
superseded rows until snapshots expire; object versioning holds overwritten objects for
thirty days. Erasing a party is not one delete but many, across layers built to be
immutable.

The position taken here is that erasure is assessed before it is engineered, because most
of this data is not erasable on request. Article 17(3)(b) exempts processing necessary to
comply with a legal obligation, and regulatory capital reporting and transaction-monitoring
records are exactly that: a bank is required to retain them for a defined period, and that
obligation overrides an erasure request for the data it covers. Refusing erasure with a
documented legal basis is the correct answer for that data, not a workaround.

That narrows the problem rather than dissolving it, and what remains is real:

- **Data outside the retention obligation** is erasable, and this platform holds some —
  the borrower attributes silver carries but the engine never receives are not part of any
  regulatory submission.
- **A wrong assertion is not covered by a retention obligation in the same way.** A
  screening alert that was adjudicated as a false positive is a claim about a person that
  proved untrue. Retention of the monitoring record is one thing; leaving an unqualified
  allegation in a table an analyst can query is another. Correction, under Article 16, is
  the more relevant right here, and the alert tables have no notion of adjudication
  outcome today.
- **Locating everything held about a party is required regardless.** Article 15 access
  applies even where erasure does not. Answering "what do you hold about this party" today
  means scanning every snapshot partition of every layer in both domains, which is possible
  but neither fast nor rehearsed.

No erasure mechanism is built. The design that would support one is recorded: per-subject
encryption with erasure implemented as key destruction, which suits immutable storage
because it does not require rewriting it. Its cost is that the key store becomes the
system on which the platform's compliance depends.

The reporting layer needs none of this. Every model is an aggregate over a non-identifying
dimension, so no individual reaches it and there is nothing there to erase or correct.

## Consequences

- Classification is written down and covers both domains, so a new field or table can be
  placed on the scale before it ships rather than after someone asks.
- The engine boundary is a minimisation control with a stated purpose, and widening it now
  requires changing a contract.
- Three limits are on the record rather than implied away: the pseudonym is invertible by
  a holder of the public source, encryption produces no key-usage audit trail, and the
  `pii` marking is inert.
- Access control remains per layer. Column-level and row-level restriction, and read-access
  logging, are absent; the audit trail covers what the platform wrote, not what anyone read.
- Erasure has a position and no implementation. The position is that most of this data is
  retained under obligation, that locating data about a party matters sooner than deleting
  it, and that a false screening alert is a correction problem before it is an erasure one.

## Note

The point of this decision is not that the platform now handles personal data completely.
It is that what it does handle is written down, what it does not is named, and the
condition that would change each choice is stated. A control that is claimed and absent is
worse than one that is absent and acknowledged.
