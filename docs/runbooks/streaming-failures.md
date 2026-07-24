# Runbook: streaming failures

What to do when the transaction stream produces something unexpected. The behaviour
described was produced deliberately by the experiments in `chaos/`, on scratch topics and
scratch tables rather than the real ones.

Two of these failures are silent. Neither raises an error, and both leave a pipeline that
reports success.

---

## Bronze holds more rows than were published

**Symptom.** Row counts in bronze exceed what the source sent, and every duplicated
message is an exact copy — the same offsets, the same payloads, twice.

**Diagnose.** Compare rows against distinct stream positions:

```sql
SELECT count(*) AS rows,
       count(DISTINCT kafka_partition || ':' || kafka_offset) AS positions
FROM bronze_txn_monitoring.transactions
```

If rows exceed positions, the stream was replayed. Check whether the checkpoint directory
still exists and when it was created; a checkpoint younger than the table is the answer.

**Cause.** Structured Streaming records the offsets it has processed in the checkpoint,
and that record is the whole of the exactly-once guarantee. Under `outputMode("append")`
the sink appends what it is handed and has no way to know it has seen a message before.
Delete the checkpoint and `startingOffsets: earliest` does exactly what it says.

Measured: 100 messages published, consumed once for 100 rows, consumed again with the
checkpoint intact for 100 rows and nothing reprocessed, then the checkpoint deleted and
consumed again — 200 rows, 100 distinct.

**What removes a checkpoint.** It is an ordinary directory. A cleanup script, a
recreated container with the path on ephemeral storage, a mistaken `rm -rf`, a migration
that moved the tables and left the checkpoint behind. Nothing warns, because from the
stream's point of view a missing checkpoint is indistinguishable from a first run.

**Recover.** Bronze is the raw landing layer and is rebuilt from the topic, so the
straightforward path is to truncate the affected range and replay it with a checkpoint in
place. Silver and gold are derived and follow. If the topic's retention has passed and
the messages are gone, the duplicates have to be removed in place instead — deduplicate
on `kafka_partition` and `kafka_offset`, which is what those columns are carried for.

**The other direction.** A checkpoint that exists but points somewhere unexpected causes
the opposite: the stream believes it has already consumed the topic and writes nothing,
while reporting success. This was hit during the S3 migration, when the tables moved and
the checkpoint did not. The symptom is a table that stays empty while the job exits
cleanly, and the fix is to clear the checkpoint so the stream reads from the beginning.

---

## A transaction is in quarantine with every rule failed

**Symptom.** A quarantine row where `failed_rules` lists eleven entries — effectively
every rule the contract has.

**What it means.** The payload could not be parsed at all. `from_json` returns a null
struct for malformed input, so every field is null and every `required` rule fires
together. The row is a parse failure wearing the costume of a data failure.

**Diagnose.** Distinguish the parse failures from the ordinary ones by the count:

```sql
SELECT size(failed_rules) AS broken, count(*)
FROM quarantine_txn_monitoring.transactions
GROUP BY 1
ORDER BY 1 DESC
```

Eleven means the payload was unparseable. One or two means a real field was wrong. What
was measured:

| Payload | Result |
|---|---|
| well formed | published |
| truncated, non-JSON, or empty | struct entirely null — 11 rules |
| a field missing | 2 rules: the field, and the rule that depends on it |
| a field of the wrong type | 1 rule — Spark nulls that field only, the other nine survive |
| an unknown extra field | silently dropped, **published** |

**Trace it back.** `kafka_partition` and `kafka_offset` are carried through bronze and
silver for this, so a bad row leads back to the exact message. Consume that offset
directly to see what was actually sent.

**Note what is not caught.** A producer adding a field passes through unnoticed. That is
forward compatibility working, and it also means a schema change upstream leaves no trace
until someone asks why a field is missing downstream.

**Note also that bronze accepts everything.** It has no contract, by design — it is the
record of what was delivered, not of what was valid. So bronze's row count is the number
of messages received rather than the number of usable transactions, and the two are only
equal when nothing has gone wrong upstream.

---

## A job died mid-write

**Symptom.** A streaming or batch job failed partway through a write.

**What the table holds.** Exactly what it held before. Iceberg writes data files first and
commits by swapping metadata, so a write that never reaches its commit changes nothing.
Measured: a transform raising mid-write left the row count, the snapshot count and the
file list identical.

**So the recovery is to rerun it.** There is no partial state to clean up first and no
need to establish what was written before deciding.

**One case needs more.** If the process died without a chance to abort — killed, out of
memory, the container pulled away — the files it had already written may survive with no
snapshot referencing them. They cost storage, appear in no query and are invisible to
`expire_snapshots`, which only removes what a snapshot released. Iceberg's
`remove_orphan_files` removes them and is deliberately not in the maintenance module,
because that condition has not been produced here and a procedure that deletes unreferenced
files is not one to add untested.

An ordinary failure does not leave them: Spark's commit protocol deletes a failed task's
temporary output itself. The distinction that matters is whether the process got to run
its cleanup, not whether the job succeeded.

**And this is not what concurrency control is for.** Atomicity means a failed write cannot
corrupt the table. It does not mean two writers can safely target the same partition —
they will not corrupt anything, but one of them will fail. That is why the DAGs run one
active run at a time: to avoid failed runs, not to avoid damage.

---

## kafka_offset is null for older rows

Not a failure. The column was added to the contract and the tables after the transaction
stream had already been processed, so rows written before that hold null.

Iceberg added the column as metadata without rewriting data, which is why it was
instantaneous, and reads return null where the underlying files have no such column. The
value cannot be recovered for those rows because it was never written — they were
processed at a time when the contract projected the anchor away.

Anything relying on the anchor should filter to rows that carry it rather than assume it
is present.
