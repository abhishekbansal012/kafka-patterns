# Module 14: Banking Reference Architecture

## Goal

Demonstrate a comprehensive banking system that combines ALL Kafka patterns from the previous modules into a single, production-representative architecture. This serves as a reference for how multiple patterns compose together in a complex domain.

## What This Module Does

- Manages bank accounts with event sourcing
- Orchestrates fund transfers via saga pattern
- Performs real-time fraud detection
- Publishes notifications on transfer completion
- Routes failed transfers to a DLT for manual review
- Compensates (refunds) partial failures automatically

## Technical Details

### Architecture Overview

```
API Request (Transfer)
    ↓
TransferService (Saga Orchestrator)
    ├── Step 1: FraudDetectionService.checkFraud()
    ├── Step 2: AccountService.debit(source)
    ├── Step 3: AccountService.credit(destination)
    └── Step 4: Publish notification event
    
On Failure:
    ├── Compensation: Refund debited amount
    └── Route to DLT for manual review
```

### TransferService (Saga Orchestrator)

- Manages the multi-step fund transfer as a saga
- Steps: Fraud Check → Debit Source → Credit Destination → Notify
- On failure after debit: Compensates by crediting back the source account
- Failed transfers: Published to `bank-transfers.DLT` for operations review
- Transfer state tracked: PENDING → FRAUD_CHECKED → DEBITED → COMPLETED (or FAILED)

### FraudDetectionService

- Rule-based fraud detection (can be extended with ML)
- **Rules**:
  - Amount > $50,000: Flagged as suspicious → transfer blocked
  - Self-transfer (same account): Rejected
- Publishes fraud events to `bank-fraud` topic for audit/monitoring
- Throws exception to halt the transfer saga

### AccountService

- `debit()`: Reduces account balance (checks sufficient funds)
- `credit()`: Increases account balance
- Both operations are transactional

### Transfer Entity

- **Fields**: `transferId`, `fromAccountId`, `toAccountId`, `amount`, `status`, `failureReason`, `createdAt`, `completedAt`
- **Status values**: PENDING, FRAUD_CHECKED, DEBITED, COMPLETED, FAILED

### Patterns Demonstrated

| Pattern | Where It's Applied |
|---------|-------------------|
| Event Sourcing | Account balance derived from events |
| CQRS | Separate account write/read models |
| Saga | Multi-step transfer with compensation |
| Outbox | Reliable event publishing |
| Exactly-Once | Transactional transfer processing |
| DLT | Failed transfers for manual review |
| Fraud Detection | Real-time rule-based validation |

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Saga compensation | Refund source on credit failure |
| Fraud gating | Transfer blocked before any fund movement |
| Event-driven notifications | Kafka topic for transfer alerts |
| DLT for operations | Failed transfers queued for review |
| Transactional state | Transfer status tracks saga progress |
| Domain-driven design | Bounded contexts: Account, Transfer, Fraud |

## Why This Architecture?

- **Resilience**: Each step is independently recoverable
- **Auditability**: Full event history of every transfer
- **Real-time fraud**: Blocks suspicious transfers before money moves
- **Operability**: DLT + notifications give ops visibility
- **Scalability**: Each service/concern can scale independently
- **Composability**: Individual patterns are reusable building blocks
