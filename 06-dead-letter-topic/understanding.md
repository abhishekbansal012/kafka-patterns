# Module 06: Dead Letter Topic (DLT) Pattern

## Goal

Demonstrate how to handle "poison pill" messages — messages that will never succeed regardless of retries. Instead of blocking the consumer or losing the message, failed messages are routed to a Dead Letter Topic for monitoring, alerting, and manual remediation.

## What This Module Does

- Configures `DeadLetterPublishingRecoverer` to route failed messages to a DLT
- Implements a main consumer that throws on invalid messages (negative amounts, missing customer ID)
- Provides a dedicated DLT consumer for monitoring and alerting
- Preserves error context in message headers (original topic, exception, timestamp)

## Technical Details

### OrderConsumerWithDlt (Main Consumer)

- Listens on `orders-dlt` topic
- Throws `IllegalArgumentException` for negative order amounts (poison pill)
- Throws `IllegalStateException` for missing customer ID
- Failed messages are captured by the error handler and forwarded to the DLT

### DltConsumer (DLT Monitor)

- Consumes from the dead letter topic
- Logs failed messages with error details from headers
- In production: would trigger alerts, persist for manual review, or attempt reprocessing after a fix

### DltConfig

- Configures `DefaultErrorHandler` with `DeadLetterPublishingRecoverer`
- DLT naming convention: `<original-topic>.DLT`
- Error headers added by Spring Kafka:
  - `kafka_dlt-original-topic`: Source topic name
  - `kafka_dlt-exception-message`: Exception message
  - `kafka_dlt-exception-stacktrace`: Full stack trace
  - `kafka_dlt-original-timestamp`: When the original message was produced

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Poison pill handling | Messages that always fail are isolated |
| DLT routing | `DeadLetterPublishingRecoverer` sends to DLT |
| Error headers | Original topic, exception, timestamp preserved |
| DLT monitoring | Separate consumer reads DLT for alerting |
| Consumer resilience | Main consumer continues after routing to DLT |
| Manual remediation | DLT messages can be reprocessed after fixing root cause |

## DLT vs Retry

| Scenario | Approach |
|----------|----------|
| Transient failure (network, timeout) | Retry with backoff |
| Permanent failure (bad data, validation) | Route directly to DLT |
| Combined | Retry N times, then DLT as final fallback |

Module 05 (Retry) and Module 06 (DLT) are complementary — production systems typically use both together.
