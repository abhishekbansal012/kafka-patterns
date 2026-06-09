# Module: Common (Shared Library)

## Goal

Provide shared domain events, topic name constants, and base classes used across all pattern modules. This avoids code duplication and ensures consistency in event structure and topic naming across the entire project.

## What This Module Contains

### KafkaTopics (Topic Name Registry)

A centralized constants class that defines all Kafka topic names used across the 14 modules. This ensures:
- No magic strings scattered across modules
- Topic names can be found in one place
- Refactoring a topic name propagates consistently

Topics are organized by module:
- Module 01: `orders`, `order-confirmations`
- Module 02: `orders-avro`
- Module 03: `orders-partitioned`
- Module 04: `orders-grouped`
- Module 05: `orders-retry`
- Module 06: `orders-dlt`, `orders-dlt.DLT`
- Module 07: `orders-idempotent`
- Module 08: `outbox-events`
- Module 09: `saga-orders`, `saga-payments`, `saga-inventory`, `saga-shipping`
- Module 10: `event-store`
- Module 11: `cqrs-commands`, `cqrs-events`
- Module 12: `exactly-once-input`, `exactly-once-output`
- Module 13: `payments`, `payment-results`, `payment-notifications`
- Module 14: `bank-accounts`, `bank-transfers`, `bank-fraud`, `bank-notifications`

### BaseEvent (Abstract Event Class)

Base class for all domain events with common metadata fields:
- `eventId`: Unique identifier (UUID) for deduplication
- `eventType`: Discriminator string (e.g., `ORDER_CREATED`)
- `timestamp`: When the event was created (`Instant`)
- `source`: Which module/service produced the event

Uses Lombok's `@SuperBuilder` for fluent construction in subclasses.

### OrderEvent (Domain Event)

The primary event type used across most modules, extending `BaseEvent`:
- `orderId`, `customerId`, `productId`: Business identifiers
- `quantity`: Item count
- `amount`: Order value (`BigDecimal` for financial precision)
- `status`: Enum (`CREATED`, `CONFIRMED`, `PROCESSING`, `SHIPPED`, `DELIVERED`, `CANCELLED`, `FAILED`)

## How Modules Depend On Common

Each submodule declares `implementation project(':common')` in its `build.gradle`, giving it access to:
- Topic name constants (no hardcoded strings)
- Shared event classes (consistent serialization across producer/consumer boundaries)
- Base event metadata (every event has an ID, type, and timestamp)
