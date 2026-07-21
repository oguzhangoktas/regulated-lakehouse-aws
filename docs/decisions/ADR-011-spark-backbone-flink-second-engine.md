# ADR-011: Spark as the streaming backbone, Flink as a second engine

## Context
Fraud and transaction-monitoring systems are commonly built on Kafka with Flink,
which is the industry default for low-latency, stateful stream processing. The
platform here already runs Spark everywhere: the batch domain, the runtime parity
with Glue, and the shared session and contract code are all Spark. The streaming
domain had to choose an engine without fragmenting that foundation.

## Decision
Spark Structured Streaming is the backbone. The whole detection pipeline — bronze,
silver, and the sweep rule — runs on Spark, reusing the batch contract engine and the
same Iceberg catalog, so streaming and batch share one codebase and one runtime.

Flink is included as a second engine for one rule. The sweep rule is expressed in
Flink SQL, consuming the same Kafka topic and producing the same alerts, to
demonstrate the rule ports cleanly across engines and that the trade-off is
understood rather than assumed.

## Why Spark for the backbone
- One runtime across batch and streaming: the contract engine, the session, and the
  Iceberg integration are shared, not reimplemented per paradigm.
- Runtime parity with Glue (ADR-007) already holds for Spark, so streaming jobs inherit
  the same develop-locally-run-on-Glue path.
- The detection logic here is not latency-bound. Alerts on a monitoring feed are
  useful within seconds; sub-second processing would not change the outcome, so Spark's
  micro-batch model is sufficient.

## Why Flink is still present
- It is the industry standard for this domain, so the platform shows a working command
  of it, not just an awareness.
- Flink processes each event on arrival rather than in micro-batches, and its CEP
  support targets stateful pattern matching directly. On a rule that needed
  sub-second latency or complex event-pattern detection across a sequence, Flink would
  be the right engine, and the sweep-in-Flink shows the migration path.

## Consequences
- The sweep rule exists twice, in Spark and in Flink SQL. They are kept in sync by
  hand; the logic is small (a typed filter) so this is low cost, and the duplication is
  the point — it is the evidence that the choice is deliberate.
- The Flink cluster (JobManager + TaskManager) runs in Docker with the Kafka connector
  baked into the image. In production this maps to a managed Flink service.
- If a future rule is genuinely latency- or pattern-bound, Flink becomes the backbone
  for that rule rather than a demonstration, and this decision is revisited.

## Note
The engine was chosen on the workload, not on fashion. Spark because the platform is
Spark and the latency budget allows it; Flink present because the domain expects it and
the trade-off should be shown in code, not only described.
