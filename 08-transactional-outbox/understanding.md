# Module 08: Transactional Outbox Pattern

## Goal

Solve the dual-write problem: when a service needs to update its database AND publish an event to Kafka, doing both atomically is impossible without coordination. The Transactional Outbox pattern guarantees that if the database write succeeds, the event will eventually be published to Kafka.

## What This Module Does

- Writes business entity (Order) and outbox event in a single database transaction
- A scheduled poller reads pending outbox entries and publishes them to Kafka
- Marks events as PUBLISHED after successful send, or FAILED after max retries
- Handles Kafka downtime gracefully (events queue up in the outbox table)

## Technical Details

### OrderService (Write Side)

- `@Transactional` method that:
  1. Saves `OrderEntity` to the orders table
  2. Saves `OutboxEvent` (with status PENDING) to the outbox table
- Both writes succeed or both roll back — atomicity guaranteed by the database

### OutboxEvent Entity

- **Fields**: `id`, `aggregateId`, `aggregateType`, `eventType`, `topic`, `payload` (JSON string), `status` (PENDING/PUBLISHED/FAILED), `createdAt`, `publishedAt`, `retryCount`
- The `payload` is the serialized Kafka message — decouples the outbox from Kafka serialization

### OutboxPublisher (Poller)

- `@Scheduled(fixedDelay = 2000ms)` — polls every 2 seconds
- Queries `findPendingEvents()` from outbox table
- For each pending event:
  - Sends to Kafka using `KafkaTemplate.send().get()` (blocking for reliability)
  - On success: sets status to PUBLISHED, records `publishedAt`
  - On failure: increments `retryCount`; after 5 retries, marks as FAILED
- Runs within `@Transactional` to update statuses atomically

### Flow

```
API Request → [DB Transaction: Save Order + Save OutboxEvent(PENDING)]
                                    ↓
Scheduler (every 2s) → Query PENDING events → Publish to Kafka → Mark PUBLISHED
```

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Dual-write problem | DB + Kafka cannot be atomic without outbox |
| Atomic local write | Order + OutboxEvent in same DB transaction |
| Polling publisher | Scheduler reads and publishes pending events |
| Eventual delivery | Events are guaranteed to reach Kafka (eventually) |
| Retry with cap | Max 5 publish attempts before marking FAILED |
| Kafka downtime resilience | Events accumulate in outbox, published when Kafka recovers |

## Trade-offs

| Pro | Con |
|-----|-----|
| Guaranteed delivery | Added latency (poll interval) |
| Simple implementation | Requires DB polling (not push-based) |
| Works with any DB | Outbox table grows until cleanup |
| No distributed transactions | Ordering depends on outbox query order |

## Alternative: Change Data Capture (CDC)

Instead of polling, tools like Debezium can tail the database transaction log and stream outbox entries to Kafka in near real-time. This eliminates polling overhead but adds infrastructure complexity.
