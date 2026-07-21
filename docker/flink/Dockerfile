# Flink with the Kafka SQL connector baked in, so the cluster comes stream-ready.
# The connector is not in the base image; adding it here means `docker compose up`
# needs no manual jar step.
FROM flink:1.20.0-scala_2.12-java17

RUN curl -sfo /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
