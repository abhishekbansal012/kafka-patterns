# Module 12: Exactly-Once Semantics (EOS)

## Goal

Demonstrate Kafka's exactly-once processing guarantee using the consume-transform-produce (CTP) pattern. This ensures that a message is consumed, processed, and the output produced exactly once — even in the presence of failures and retries.

## What This Module Does

- Configures Kafka transactions on the producer side (`transactional.id`)
- Sets consumer isolation level to `read_committed` (only sees committed messages)
- Implements atomic consume-transform-produce within a Kafka transaction
- Uses Spring's `KafkaTransactionManager` to coordinate consumer offset commit with producer output

## Technical Details

### ExactlyOnceProcessor

- `@Transactional("kafkaTransactionManager")`: Wraps the entire operation in a Kafka transaction
- `@KafkaListener`: Consumes from `exactly-once-input` topic
- **Transform**: Enriches the order by calculating tax (10%) and total
- **Produce**: Writes enriched output to `exactly-once-output` topic
- If ANY step fails, the entire transaction rolls back:
  - Consumer offset is NOT committed (message will be re-delivered)
  - Produced output is NOT visible to `read_committed` consumers

### ExactlyOnceConfig

- **Producer**:
  - `transactional.id` prefix: Enables transactional producer
  - `enable.idempotence = true`: Prevents duplicate produces from network retries
  - `acks = all`: All in-sync replicas must acknowledge
- **Consumer**:
  - `isolation.level = read_committed`: Only reads messages from committed transactions
  - Offset commit is part of the Kafka transaction (not a separate operation)

### How Kafka Transactions Work

```
beginTransaction()
  → consume from input topic
  → transform the message
  → produce to output topic
  → sendOffsetsToTransaction(consumed offsets)
commitTransaction()
```

If `commitTransaction()` succeeds: output is visible, offsets are committed.
If anything fails: `abortTransaction()` — output is invisible, offsets not committed.

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Kafka transactions | `KafkaTransactionManager` wraps CTP atomically |
| Idempotent producer | `enable.idempotence=true` deduplicates network retries |
| read_committed | Consumer only sees committed transaction output |
| Atomic CTP | Consume + transform + produce in one transaction |
| Offset as part of TX | Consumer offsets committed within the transaction |
| Rollback on failure | Partial writes are invisible, message re-delivered |

## When To Use Exactly-Once

- **Stream processing**: Consume → transform → produce pipelines
- **Financial calculations**: Tax computation, balance updates
- **Data enrichment**: Adding derived fields to events
- **Any CTP pattern** where duplicate output would cause inconsistency

## Performance Impact

- Transactions add ~5-20ms latency per batch
- Throughput is slightly lower due to transaction coordination
- `read_committed` consumers have slight delay waiting for transaction commit
- Recommended for correctness-critical paths, not necessarily for all consumers
