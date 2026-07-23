# Data classification

Which data this platform holds, how sensitive each field is, and what handling each
level requires. It covers both domains and every layer, so a change can be checked
against it before it ships.

## Standing of this document

The sources are public: an anonymised consumer lending extract and a synthetic payment
simulation. Neither carries a real person. The watchlist is generated, not a licensed
sanctions list.

The classification is applied as if the data were real, because the platform's controls
are only meaningful under that assumption. Where a control is not implemented, this
document says so rather than implying it.

Field-level detail about the lending source — which columns exist, their distributions
and their constraints — is in `data_dictionary.md`. This document is about handling.

## Levels

| Level | Definition | Handling |
|---|---|---|
| **PUBLIC** | Reference data with no link to any party. | No restriction. |
| **INTERNAL** | Business data that cannot identify a party on its own. | Layer access controls only. |
| **CONFIDENTIAL** | Pseudonymous keys, and attributes that could re-identify a party in combination. | Minimise at every boundary; carry only where a consumer declares a need. |
| **RESTRICTED** | An assertion *about* a party — a fraud label, a sanctions or PEP match. | Narrowest access. A false positive is a harm in itself, so these must not spread into general reporting. |

The distinction that matters most is the last one. A balance is a fact about an account;
a screening hit is a claim about a person. The second is more sensitive than the first
even though it contains less data.

## credit_risk

### Excluded at the source boundary

`zip_code`, `emp_title`, `url`, `title`, `desc` identify the borrower directly and are
dropped in `build_loan_master.py`. They exist in the source file and in no layer of this
platform. `member_id` is empty across the source and is not carried either.

This is the cheapest control available: a field that is never ingested cannot leak,
cannot be mishandled, and needs no policy.

### bronze — daily exposure snapshot

| Field | Level | Note |
|---|---|---|
| `exposure_id` | CONFIDENTIAL | Business key, derived from the source loan id. |
| `customer_id` | CONFIDENTIAL | Pseudonym. See "Known limits". |
| `annual_inc`, `dti`, `fico_range_low`, `fico_range_high` | CONFIDENTIAL | Borrower financial attributes. |
| `emp_length`, `home_ownership`, `verification_status`, `application_type` | CONFIDENTIAL | Borrower circumstance. |
| `addr_state` | CONFIDENTIAL | Coarse location; re-identifying in combination. |
| `loan_amnt`, `int_rate`, `term`, `installment`, `out_prncp`, `total_pymnt`, `recoveries` | INTERNAL | Contract and payment amounts. |
| `grade`, `sub_grade`, `purpose`, `loan_status`, `issue_d`, `last_pymnt_d` | INTERNAL | Product and lifecycle attributes. |

### silver — conformed exposure

Silver holds the whole book with platform names and types. The classification carries
over unchanged: `customer_id` and `exposure_id` CONFIDENTIAL, borrower attributes
(`annual_income`, `debt_to_income`, `fico_low`, `fico_high`, `home_ownership`,
`emp_length`, `verification_status`, `application_type`, `region`) CONFIDENTIAL, and the
exposure's own figures (`original_amount`, `outstanding_amount`, `provision_amount`,
`interest_rate`, `rating_grade`, `rating_subgrade`, `status`, `days_past_due`,
`default_flag`, `term_months`, dates) INTERNAL.

### gold — engine input

The engine input contract declares 23 fields. Eight of the borrower attributes silver
carries are **not among them**: `annual_income`, `debt_to_income`, `fico_low`,
`fico_high`, `home_ownership`, `emp_length`, `verification_status`, `application_type`.

The contract engine projects to its declared fields, so those eight cannot cross the
boundary even if a future change adds them upstream. The vendor engine computes capital
from exposure and rating; it has no need for the borrower's income or employment, and
therefore does not receive them.

`customer_id` and `exposure_id` remain CONFIDENTIAL and are carried because the output
must reconcile exposure for exposure.

### gold — RWA output

`reporting_date`, `exposure_id`, `customer_id`, `rating_grade`, `status`, `ead`,
`risk_weight`, `rwa`, `capital_required`. The keys are CONFIDENTIAL; the figures are
INTERNAL. The eight borrower attributes are absent here too, since they never reached
the engine.

## txn_monitoring

### bronze — raw stream

| Field | Level | Note |
|---|---|---|
| `name_orig`, `name_dest` | CONFIDENTIAL | Account identifiers. Opaque in this source; a real feed carries account numbers. |
| `amount`, `old_balance_orig`, `new_balance_orig`, `old_balance_dest`, `new_balance_dest` | CONFIDENTIAL | Balances are a financial attribute of an identifiable account. |
| `type`, `step` | INTERNAL | Transaction type and event clock. |
| `is_fraud` | **RESTRICTED** | An assertion about the transaction and, by extension, the account holder. |
| `kafka_partition`, `kafka_offset`, `kafka_timestamp` | INTERNAL | Stream provenance; no party data. |

`is_fraud` exists only because this source carries ground truth, and it is used to
measure detection quality. A production stream has no such column at ingestion.

### silver — conformed transactions

Same classification as bronze. Silver keeps every transaction type for completeness;
detection filters downstream.

### gold — alerts

`step`, `name_orig`, `name_dest`, `type`, `amount`, `rule`, `label`.

The table is **RESTRICTED as a whole**, not field by field. A row here states that a
named account was flagged by a named rule. `rule` on its own is PUBLIC; attached to an
account it is an allegation.

### gold — watchlist, destination names, screening alerts

| Table | Level | Note |
|---|---|---|
| `watchlist` (`entity_id`, `name`, `program`) | **RESTRICTED** | A list of parties and why they are listed. Generated here; a real list is licensed and access-controlled. |
| `dest_names` (`name_dest`, `dest_name`, `seeded_hit`) | CONFIDENTIAL | Beneficiary names attached to accounts. `seeded_hit` is test scaffolding, not a production field. |
| `screening_alerts` (`name_dest`, `dest_name`, `seeded_hit`, `watchlist_entity`, `program`, `match_score`) | **RESTRICTED** | The most sensitive artefact in the platform: a name, the listed party it resembles, the programme that party is listed under, and a similarity score. |

A screening alert is a claim that a payment beneficiary may be a sanctioned or
politically exposed person. It is produced by a fuzzy match, so some alerts are wrong by
construction — the threshold is a trade-off, not a truth. Handling follows from that: the
narrowest access, no propagation into general reporting, and adjudication by a named
analyst before any consequence.

## The reporting layer

The four dbt models are aggregates: `rwa_monthly_trend` and `capital_by_grade` group by
reporting date and rating grade; `alerts_by_rule` and `alerts_by_type` group by rule and
transaction type.

None of them carries `customer_id`, `exposure_id`, `name_orig` or `name_dest`. No
individual-level data reaches the reporting layer. This holds today because every model
is a `GROUP BY` over a non-identifying dimension, and it is the property to preserve when
a model is added.

## Controls in place

1. **Exclusion at the source.** Direct identifiers are dropped before ingestion
   (`build_loan_master.py`), so they exist in no layer.
2. **Pseudonymisation.** `customer_id` is a hash of the source loan id rather than the id
   itself. See "Known limits" for the strength of this.
3. **Minimisation at the engine boundary.** The contract declares the fields the engine
   consumes and the engine projects to them, so eight borrower attributes present in
   silver do not reach the engine, its output, or reporting.
4. **Layer isolation and least privilege.** Each layer is a separate bucket with its own
   access policy (ADR-006). The Glue role can read bronze but not write it; the
   orchestration identity can start jobs and reach no data at all.
5. **Encryption at rest.** Default server-side encryption on every layer bucket, and on
   Athena query results.
6. **Quarantine inherits classification.** Quarantined rows are the same records that
   failed a rule, so quarantine tables carry the classification of the layer they came
   from. They are not a lower-sensitivity dumping ground.
7. **Aggregation before reporting.** Reporting models expose no individual.

## Known limits

Stated plainly, because a control that is claimed and absent is worse than one that is
absent and named.

**The `pii` declaration is not enforced.** Contract YAMLs mark `customer_id` as
`pii: pseudonymised`. The contract engine reads schema, row rules and dataset assertions;
it does not read that marking, so nothing checks it. The declaration is documentation.
Enforcement would mean the engine refusing to publish a field marked as raw personal data
across a boundary, and asserting the shape of a pseudonymised one.

**The pseudonym is not strong.** `customer_id` is an unsalted SHA-256 of the source loan
id, truncated. The loan ids are published in the source file, so anyone holding it can
hash all of them and invert the mapping. This is pseudonymisation, not anonymisation, and
a weak form of it: it removes the identifier from casual view but not from a determined
holder of the source. A keyed HMAC with the key held in a managed key store would resist
that, at the cost of key management and of breaking the mapping if the key rotates.

**Encryption uses S3-managed keys.** SSE-S3 protects the data at rest but produces no
key-usage audit trail and offers no key-level access policy. A customer-managed key would
give both, at a per-request cost. The choice here is the cost-conscious one and is
recorded as such rather than presented as sufficient for a regulated deployment.

**Access control is per layer, not per column or row.** A principal with read access to
gold can read every column of every row in it. Column-level or row-level restriction —
for example, letting a reporting consumer read `rwa_output` without `customer_id` — would
need Lake Formation or an equivalent, which is not configured.

**Access is not reviewed.** Object-level access logging is not enabled and no periodic
review of who read what exists. The audit trail today covers what the platform *wrote*,
through snapshot partitions and table history, not what anyone *read*.

**Erasure is unresolved.** A request to erase a party conflicts with immutable bronze,
retained table history and object versioning. See ADR-014.
