"""Chaos 10: a message the parser cannot use.

A stream accepts whatever is published to it. A producer change, a partial write, an
encoding mistake — the topic carries the result either way, and the pipeline meets it
without the option of rejecting the delivery.

This runs seven payload variants through the same schema bronze parses with and the same
contract silver enforces, and reports what each becomes and where it is caught. It uses
the real code rather than the real topic: nothing is published to Kafka and no table is
written.

Each case is enforced on its own, because the contract projects to its declared fields
and a correlating column would not survive that.

Usage:
  python -m chaos.exp10_poison_message
"""
from pyspark.sql import functions as F

from dataplatform.contracts.contract import Contract
from dataplatform.lakehouse.session import local_session
from domains.txn_monitoring.streaming.bronze_stream import PAYLOAD
from domains.txn_monitoring.streaming.silver_stream import CONTRACT, conform

GOOD = (
    '{"step": 1, "type": "TRANSFER", "amount": 5000.0, "name_orig": "C1", '
    '"old_balance_orig": 5000.0, "new_balance_orig": 0.0, "name_dest": "C2", '
    '"old_balance_dest": 0.0, "new_balance_dest": 5000.0, "is_fraud": 1}'
)

CASES = [
    ("well formed", GOOD),
    ("truncated json", GOOD[:60]),
    ("not json at all", "TRANSFER,5000,C1,C2"),
    ("empty payload", ""),
    ("missing a field", GOOD.replace('"name_dest": "C2", ', "")),
    ("wrong type", GOOD.replace('"amount": 5000.0', '"amount": "five thousand"')),
    ("unknown extra field", GOOD[:-1] + ', "channel": "mobile"}'),
]

FIELDS = [f.name for f in PAYLOAD.fields]


def main() -> None:
    spark = local_session("chaos_10_poison_message")
    contract = Contract.named(CONTRACT)

    print(f"{'case':22s} {'parsed':10s} outcome")

    for offset, (label, value) in enumerate(CASES):
        # The bronze parse, unchanged: a string to a struct of the declared shape.
        bronze = (
            spark.createDataFrame([(value,)], "value string")
            .select(F.from_json(F.col("value"), PAYLOAD).alias("txn"))
            .select("txn.*")
            .withColumn("kafka_offset", F.lit(offset).cast("long"))
        )

        row = bronze.first()
        nulls = sum(row[field] is None for field in FIELDS)
        parsed = "no" if nulls == len(FIELDS) else ("partial" if nulls else "yes")

        passed, quarantined = contract.enforce(conform(bronze))

        if passed.count():
            outcome = "published to silver"
        else:
            broken = quarantined.first()["failed_rules"]
            shown = ", ".join(broken[:3]) + ("..." if len(broken) > 3 else "")
            outcome = f"quarantined ({len(broken)}): {shown}"

        print(f"{label:22s} {parsed:10s} {outcome}")

        if offset == 0:
            survives = "kafka_offset" in passed.columns
            anchor = f"\nthe stream anchor survives the contract: {survives}"

    print(anchor)
    print("bronze holds every one of them: it has no contract")
    spark.stop()


if __name__ == "__main__":
    main()
