# Module 04: Consumer Groups & Rebalancing

## Goal

Demonstrate how Kafka consumer groups enable parallel processing and how partition rebalancing works when consumers join or leave a group. This module is essential for understanding Kafka's horizontal scaling model.

## What This Module Does

- Configures manual acknowledgment (offset commit only after successful processing)
- Implements a `ConsumerRebalanceListener` to log partition assignment/revocation
- Shows concurrent consumer configuration via `ConcurrentKafkaListenerContainerFactory`
- Demonstrates at-least-once delivery semantics through manual ack

## Technical Details

### ManualAckConsumer

- Listens on `orders-grouped` topic with group ID `order-processing-group`
- Uses `Acknowledgment.acknowledge()` only after successful processing
- If processing throws an exception, the offset is NOT committed → message will be re-delivered on next poll
- This guarantees at-least-once delivery (no message loss, possible duplicates)

### RebalanceLogger

- Implements `ConsumerRebalanceListener` interface
- `onPartitionsRevoked()`: Logs which partitions are being taken away (finish in-flight work here)
- `onPartitionsAssigned()`: Logs which partitions this consumer now owns
- Rebalancing is triggered when:
  - A new consumer joins the group
  - An existing consumer crashes or shuts down
  - Partitions are added to the topic
  - Session timeout expires (heartbeat missed)

### ConsumerGroupConfig

- Configures `ConcurrentKafkaListenerContainerFactory` with manual ack mode
- Sets concurrency level (number of threads per consumer instance)
- Registers the `RebalanceLogger` for partition lifecycle events

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Consumer group | Multiple consumers share partitions within a group |
| Rebalancing | `ConsumerRebalanceListener` logs assign/revoke events |
| Manual ack | `AckMode.MANUAL` — commit only after success |
| At-least-once | Failed messages are re-delivered (not acknowledged) |
| Concurrency | Multiple threads consume from different partitions |
| Partition ownership | Each partition is owned by exactly one consumer in a group |

## Design Considerations

- Max parallelism = number of partitions (extra consumers are idle)
- Rebalancing causes a brief pause in consumption ("stop the world")
- Long processing times can trigger session timeout → unintended rebalance
- Manual ack gives reliability but requires careful error handling
- Consider `nack()` with backoff for transient failures
