# Module 09: Saga Pattern (Orchestrator-based)

## Goal

Demonstrate how to coordinate a distributed transaction across multiple services using the Saga pattern. Instead of a single ACID transaction spanning services, a saga breaks the operation into a sequence of local transactions with compensating actions on failure.

## What This Module Does

- Implements an **orchestrator** that drives the saga through its steps
- Coordinates three participants: Payment, Inventory, and Shipping
- Persists saga state to track progress and enable recovery
- Executes compensating transactions (rollback) when a step fails
- Uses Kafka topics for command/reply communication between orchestrator and participants

## Technical Details

### OrderSagaOrchestrator

- **Starts a saga**: Creates `SagaState` (status: IN_PROGRESS), sends first command
- **Handles replies**: Listens on `saga-orders` topic for participant responses
- **Advances on success**: Moves to next step and sends the corresponding command
- **Compensates on failure**: Rolls back completed steps in reverse order

### Saga Steps (Happy Path)

```
INITIATED → PAYMENT → INVENTORY → SHIPPING → COMPLETED
```

1. Send `PROCESS_PAYMENT` to `saga-payments` topic
2. On payment success → Send `RESERVE_INVENTORY` to `saga-inventory` topic
3. On inventory success → Send `SCHEDULE_SHIPPING` to `saga-shipping` topic
4. On shipping success → Mark saga COMPLETED

### Compensation (Failure Path)

```
Failure at SHIPPING  → RELEASE_INVENTORY + REFUND_PAYMENT
Failure at INVENTORY → REFUND_PAYMENT
Failure at PAYMENT   → Nothing to compensate
```

### Participants

- **PaymentService**: Processes/refunds payments, replies with success/failure
- **InventoryService**: Reserves/releases inventory, replies with success/failure
- **ShippingService**: Schedules shipping, replies with success/failure

### SagaState Entity

- **Fields**: `sagaId`, `orderId`, `currentStep` (enum), `status` (IN_PROGRESS/COMPLETED/COMPENSATING/COMPENSATED), `failureReason`, `createdAt`, `updatedAt`
- Persisted to database for recovery if orchestrator crashes mid-saga

### Communication Model

- **Commands**: Orchestrator → Participants (via dedicated topic per participant)
- **Replies**: Participants → Orchestrator (via `saga-orders` topic)
- Each command includes `sagaId` for correlation

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Distributed transaction | Multi-service order fulfillment |
| Orchestrator | Central coordinator drives saga steps |
| Compensating transactions | Refund, release inventory on failure |
| Saga state machine | INITIATED → PAYMENT → INVENTORY → SHIPPING → COMPLETED |
| Command/Reply | Kafka topics for async communication |
| Failure isolation | One service failure doesn't corrupt others |
| State persistence | Saga state in DB for crash recovery |

## Orchestrator vs Choreography

| Orchestrator (this module) | Choreography |
|---------------------------|--------------|
| Central coordinator | Each service reacts to events |
| Easier to understand flow | No single point of coordination |
| Explicit compensation logic | Distributed compensation |
| Single place to monitor | Harder to trace full flow |
