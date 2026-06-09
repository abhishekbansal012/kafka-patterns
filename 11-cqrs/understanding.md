# Module 11: CQRS (Command Query Responsibility Segregation)

## Goal

Demonstrate CQRS — separating the write model (commands) from the read model (queries). Commands produce domain events via Kafka, and a projector consumes those events to maintain an optimized read model. The two sides are eventually consistent.

## What This Module Does

- **Command side**: Validates commands and publishes domain events to Kafka (no direct DB writes)
- **Query side**: A projector listens to events and maintains a denormalized read model in the database
- Shows eventual consistency between write and read sides
- Demonstrates independent scaling and optimization of read vs write paths

## Technical Details

### CommandHandler (Write Side)

- `handleCreateProduct()`: Validates the command, publishes a `PRODUCT_CREATED` event to Kafka
- `handleUpdateStock()`: Publishes a `STOCK_UPDATED` event with quantity delta
- Does NOT write to the read database directly
- Events are published to `cqrs-events` topic, keyed by `productId`

### ProductProjector (Read Side)

- `@KafkaListener` on `cqrs-events` topic
- Handles `PRODUCT_CREATED`: Creates a `ProductReadModel` entry in the database
- Handles `STOCK_UPDATED`: Finds existing model, applies quantity change, updates `inStock` flag
- Optimized for query patterns (denormalized, pre-computed fields like `inStock`)

### ProductReadModel Entity

- **Fields**: `productId`, `name`, `category`, `price`, `stockQuantity`, `inStock` (computed), `lastUpdated`
- Designed for fast reads — no joins, pre-computed booleans
- Can be rebuilt from scratch by replaying all events from the beginning

### CqrsController

- Exposes write endpoints (POST commands) and read endpoints (GET queries)
- Write path: Accepts command → publishes event → returns immediately
- Read path: Queries the read model directly (fast, no event processing)

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Command/Query separation | Different models for writes vs reads |
| Event-driven sync | Kafka events connect write → read side |
| Eventual consistency | Read model lags behind writes slightly |
| Optimized read model | Denormalized, pre-computed `inStock` flag |
| Projection rebuild | Can replay all events to rebuild read model |
| Independent scaling | Read and write sides scale separately |

## Why CQRS?

- **Read-heavy systems**: Read model is optimized for queries (no joins, pre-computed)
- **Different scaling needs**: Scale read replicas independently from write capacity
- **Complex domains**: Write model can be rich (DDD aggregates) while read model is flat
- **Multiple read models**: Same events can feed different projections (search, analytics, reports)

## Eventual Consistency Trade-off

- After a write, the read model may take milliseconds to catch up
- Acceptable for most UIs (user refreshes page)
- For strong consistency needs, query the event store directly (slower but consistent)
