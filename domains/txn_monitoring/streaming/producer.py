"""Publish the transaction log to Kafka in event-time order.

Stands in for the source system that emits transactions onto the stream. In
production that is core banking writing to MSK; here it replays PaySim, ordered by
step (the hourly clock), so the consumer sees transactions as they would arrive.

Each message is keyed by the origin account, so all of an account's transactions
land on one partition and a stateful rule downstream sees them in order.

Usage:
  python -m domains.txn_monitoring.streaming.producer <csv> [--rate N] [--limit N]
"""
import argparse
import json
import time

import pandas as pd
from confluent_kafka import Producer

TOPIC = "transactions"
BOOTSTRAP = "localhost:9092"


def rows_in_event_time(path: str, limit: int | None):
    """Yield transactions ordered by step, so the stream is monotonic in event time."""
    cols = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
            "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud"]
    df = pd.read_csv(path, usecols=cols)
    df = df.sort_values("step", kind="stable")
    if limit:
        df = df.head(limit)
    return df


def run(path: str, rate: int, limit: int | None) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50})
    df = rows_in_event_time(path, limit)

    sent = 0
    for row in df.itertuples(index=False):
        message = {
            "step": int(row.step),
            "type": row.type,
            "amount": float(row.amount),
            "name_orig": row.nameOrig,
            "old_balance_orig": float(row.oldbalanceOrg),
            "new_balance_orig": float(row.newbalanceOrig),
            "name_dest": row.nameDest,
            "old_balance_dest": float(row.oldbalanceDest),
            "new_balance_dest": float(row.newbalanceDest),
            "is_fraud": int(row.isFraud),
        }
        # Block until the queue has room rather than overflowing it; BufferError
        # means the local queue is full, so drain it and retry the same message.
        while True:
            try:
                producer.produce(TOPIC, key=row.nameOrig, value=json.dumps(message))
                break
            except BufferError:
                producer.poll(0.5)
        sent += 1

        if sent % 100000 == 0:
            producer.flush()
            print(f"sent {sent:,}")
            if rate:
                time.sleep(100000 / rate)

    producer.flush()
    print(f"done, sent {sent:,} to {TOPIC}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv")
    parser.add_argument("--rate", type=int, default=0,
                        help="messages per second; 0 sends as fast as possible")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(args.csv, args.rate, args.limit)
