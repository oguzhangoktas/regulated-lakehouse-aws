-- The whole-account sweep rule in Flink SQL, reading the same Kafka topic as Spark.
--
-- A second engine over one detection logic. Spark Structured Streaming is the backbone
-- (ADR-011); this expresses the same rule in Flink SQL against the same `transactions`
-- topic, so the engine choice is demonstrated, not asserted.
--
-- Flink is the industry default for low-latency fraud work: it processes each event on
-- arrival rather than in micro-batches, and its CEP support targets stateful pattern
-- matching. The sweep rule is stateless per event, so both engines yield the same
-- alerts; the point is that the rule ports cleanly and that the trade-off is understood.
--
-- Run:
--   docker compose exec flink-jobmanager \
--     ./bin/sql-client.sh -f /opt/flink/jobs/sweep.sql

CREATE TABLE transactions (
    step INT,
    `type` STRING,
    amount DOUBLE,
    name_orig STRING,
    old_balance_orig DOUBLE,
    new_balance_orig DOUBLE,
    name_dest STRING,
    is_fraud INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-sweep',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- Print sink: alerts stream to the TaskManager stdout. Parity with the Spark rule is
-- the goal, not a production sink.
CREATE TABLE sweep_alerts (
    name_orig STRING,
    name_dest STRING,
    `type` STRING,
    amount DOUBLE,
    is_fraud INT
) WITH (
    'connector' = 'print'
);

-- The whole-account sweep: a TRANSFER or CASH_OUT whose amount equals the origin's
-- opening balance to the cent. Identical logic to the Spark rule.
INSERT INTO sweep_alerts
SELECT name_orig, name_dest, `type`, amount, is_fraud
FROM transactions
WHERE `type` IN ('TRANSFER', 'CASH_OUT')
  AND old_balance_orig > 0
  AND ABS(amount - old_balance_orig) <= 1;
