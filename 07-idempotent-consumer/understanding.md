# Module 07: Idempotent Consumer Pattern

## Goal

Demonstrate how to achieve effectively-once processing from Kafka's at-least-once delivery guarantee. Since Kafka can deliver a message more than once (consumer crash after processing but before offset commit), the consumer must detect and skip duplicates.

## What This Module Does

- Tracks processed event IDs in a database table (`processed_events`)
- Before processing, checks if the event ID already exists (deduplication)
- Processes new messages within a database transaction alongside the deduplication record
- Uses manual acknowledgment to ensure offset commit only after successful persist

## Technical Details

### IdempotentOrderConsumer

- **Algorithm**:
  1. Extract `eventId` from incoming `OrderEvent`
  2. Query `ProcessedEventRepository.existsById(eventId)` — if true, skip (duplicate)
  3. Execute business logic (`processOrder`)
  4. Save `ProcessedEvent` record (same DB transaction)
  5. Acknowledge Kafka offset

- **Transactional guarantee**: `@Transactional` wraps steps 3 and 4 — if business logic fails, the deduplication record is also rolled back, allowing re-delivery to succeed on retry

### ProcessedEvent Entity

- **Fields**: `eventId` (PK), `topic`, `partition`, `offset`, `processedAt`, `consumerGroup`
- Acts as an idempotency key table
- Unique constraint on `eventId` prevents duplicate inserts even under race conditions

### ProcessedEventRepository

- Spring Data JPA repository
- `existsById()` check is the deduplication gate
- Could be extended with TTL cleanup for old entries

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Deduplication | Check `eventId` in DB before processing |
| Idempotency key | `eventId` field uniquely identifies each message |
| At-least-once → effectively-once | Duplicates detected and skipped |
| Transactional processing | Business logic + dedup record in same TX |
| Manual ack | Offset committed only after successful persist |
| Graceful duplicate handling | Log warning and acknowledge (no error) |

## When To Use This Pattern

- Financial transactions (prevent double-charges)
- Inventory updates (prevent double-decrements)
- Any operation that is NOT naturally idempotent
- Systems where Kafka consumers may restart mid-batch

## Performance Considerations

- DB lookup on every message adds latency (~1-5ms with index)
- Consider in-memory cache (Redis) for high-throughput scenarios
- TTL-based cleanup prevents unbounded table growth
- Composite unique index on `(eventId, consumerGroup)` for multi-consumer setups
