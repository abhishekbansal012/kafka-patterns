# Module 13: Payment Processing Demo

## Goal

Demonstrate a production-grade payment pipeline that combines multiple Kafka patterns into a cohesive system. This module shows how patterns like Transactional Outbox, Idempotent Consumer, Retry + DLT, and event-driven notifications work together in a real-world payment flow.

## What This Module Does

- Accepts payment requests via REST API with idempotency keys
- Prevents duplicate charges using idempotency key lookup
- Publishes payment events to Kafka for downstream processing
- Processes payments through a simulated gateway
- Sends notification events on completion
- Routes failed payments to a DLT for manual review

## Technical Details

### PaymentService

- **`initiatePayment()`**:
  1. **Idempotency check**: Looks up `idempotencyKey` in the database — if found, returns existing payment (no duplicate charge)
  2. Creates `Payment` entity with status PENDING
  3. Publishes to `payments` topic for async processing
  4. Returns immediately (non-blocking)

- **`processPayment()`**:
  1. Loads payment by ID, checks it's still PENDING
  2. Calls simulated payment gateway
  3. On success: Updates status to COMPLETED, publishes notification to `payment-notifications`
  4. On failure: Updates status to FAILED, stores failure reason

### Payment Entity

- **Fields**: `paymentId`, `orderId`, `customerId`, `amount`, `currency`, `status` (PENDING/COMPLETED/FAILED), `idempotencyKey` (unique), `failureReason`, `createdAt`, `processedAt`
- Unique constraint on `idempotencyKey` prevents double-charges at DB level

### Gateway Simulation

- Payments under $10,000: Succeed
- Payments $10,000+: Declined (simulates gateway rejection)

### Patterns Combined

| Pattern | Role in This Module |
|---------|-------------------|
| Transactional Outbox | Payment + event saved atomically (if extended) |
| Idempotent Consumer | `idempotencyKey` prevents duplicate processing |
| Retry + DLT | Failed payments can be retried or sent to DLT |
| Event-driven notification | Success → notification event published |

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Idempotency key | Client-provided key prevents duplicate charges |
| Async processing | Payment initiation returns immediately |
| Event-driven | Status changes published as events |
| Failure isolation | Gateway failures don't crash the service |
| Notification events | Downstream services react to payment completion |
| Status machine | PENDING → COMPLETED or PENDING → FAILED |

## Production Considerations

- Idempotency keys should have a TTL (e.g., 24 hours) to allow retries after failures
- Gateway timeout handling needs separate retry logic
- PCI compliance requires encryption of card data (not shown in demo)
- Payment notifications may trigger email/SMS via separate consumer
- DLT monitoring dashboards for operations team
