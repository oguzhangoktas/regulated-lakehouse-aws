# ADR-001: Medallion layering

## Context
Three domains (credit risk, market risk, transaction monitoring) consume overlapping
source data and are subject to regulatory reporting. Figures must be reproducible as of
a past reporting date, and any published number must be traceable back to the source
records it came from.

## Decision
Data moves through four layers, each with a single responsibility:

  bronze      source extract, landed unchanged, immutable once written
  silver      typed, validated, conformed; source semantics mapped to platform semantics
  gold        engine inputs, engine outputs, reporting marts
  quarantine  records that failed validation, retained with the reason

Domains share the layers. Domain-specific logic lives under domains/, anything reused
across domains lives under platform/.

## Consequences
- Bronze can be re-read to rebuild every downstream layer, so a logic error is
  recoverable without going back to the source system.
- Landing raw data unchanged means bronze carries the source's quality problems.
  Validation is deferred to the silver boundary rather than done on ingest.
- Four layers is more storage and more moving parts than transforming in one step.

## Rejected alternatives
- Single transformation from source to reporting tables: cheaper to build, but a
  defect requires a new extract from the source system, and intermediate state is not
  inspectable when a figure is questioned.
