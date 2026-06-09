# Module 10: Event Sourcing

## Goal

Demonstrate Event Sourcing — storing the full history of state changes as an append-only sequence of events rather than just the current state. The current state is derived by replaying events, enabling temporal queries, audit trails, and full state reconstruction.

## What This Module Does

- Maintains an append-only event store (database table) with versioned events
- Reconstructs aggregate state by replaying all events from the store
- Publishes events to Kafka after persistence for downstream consumers
- Uses versioning to enforce ordering and detect concurrency conflicts

## Technical Details

### EventStoreService

- **`appendEvent()`**:
  1. Looks up the current max version for the aggregate
  2. Increments version (optimistic concurrency control)
  3. Serializes event payload to JSON
  4. Saves `EventStoreEntry` to the database
  5. Publishes to Kafka topic (`event-store`) for other services

- **`loadAggregate()`**:
  1. Fetches all events for an aggregate, ordered by version (ascending)
  2. Creates a new `AccountAggregate` instance
  3. Replays each event via `aggregate.apply(eventType, payload)`
  4. Returns the fully reconstructed aggregate with current state

### EventStoreEntry Entity

- **Fields**: `id` (auto), `aggregateId`, `aggregateType`, `eventType`, `version`, `payload` (JSON), `createdAt`
- Append-only: no updates, no deletes
- Unique constraint on `(aggregateId, version)` prevents duplicate versions

### AccountAggregate

- Reconstructed in-memory by replaying events
- `apply()` method handles different event types (e.g., ACCOUNT_CREATED, MONEY_DEPOSITED, MONEY_WITHDRAWN)
- Tracks derived state: balance, version, account status

### Kafka Integration

- Events are published to Kafka AFTER being persisted to the event store
- Other services can build read models (projections) from the event stream
- Kafka acts as the distribution mechanism, not the source of truth (DB is)

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Append-only store | Events are only inserted, never updated |
| Aggregate replay | State built by applying events in order |
| Versioning | Each event has a monotonically increasing version |
| Temporal queries | Can reconstruct state at any point in time |
| Event publishing | Kafka distributes events to other services |
| Optimistic concurrency | Version check prevents conflicting writes |

## Benefits of Event Sourcing

- **Complete audit trail**: Every state change is recorded
- **Temporal queries**: "What was the balance at 3pm yesterday?"
- **Event replay**: Rebuild read models or fix bugs retroactively
- **Debugging**: Full history of what happened and when
- **Decoupling**: New services can replay history to build their own projections

## Trade-offs

| Pro | Con |
|-----|-----|
| Full history | Event store grows indefinitely |
| Audit compliance | Requires snapshots for large aggregates |
| Replay capability | Schema evolution of old events is complex |
| Temporal queries | Eventually consistent read models |
