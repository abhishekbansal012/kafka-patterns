# Module 05: Retry Pattern

## Goal

Demonstrate non-blocking retry for Kafka consumers using Spring Kafka's `@RetryableTopic`. When a message fails processing, it's moved to a retry topic (not blocking the main consumer), and retried with exponential backoff until either succeeding or exhausting retries.

## What This Module Does

- Uses `@RetryableTopic` annotation for automatic retry topic creation
- Implements exponential backoff (1s → 2s → 4s) with configurable max delay
- Routes permanently failed messages to a Dead Letter Topic (DLT) via `@DltHandler`
- Simulates transient failures that succeed after retries, and permanent failures that go to DLT

## Technical Details

### RetryableOrderConsumer

- **Retry configuration**:
  - `attempts = 4` (1 original + 3 retries)
  - `backoff`: initial delay 1s, multiplier 2x, max delay 10s
  - `dltStrategy = ALWAYS_RETRY_ON_ERROR` — always send to DLT on final failure
  - `include = {RuntimeException.class}` — only retry on RuntimeExceptions
  - `autoCreateTopics = true` — retry and DLT topics are auto-created

- **Retry flow**:
  1. Message arrives on `orders-retry` topic
  2. Processing fails → moves to `orders-retry-retry-0` (waits 1s)
  3. Retry fails → `orders-retry-retry-1` (waits 2s)
  4. Retry fails → `orders-retry-retry-2` (waits 4s)
  5. All retries exhausted → `orders-retry-dlt`

- **DLT handler** (`@DltHandler`): Logs the failure for manual intervention

### Failure Simulation

- Orders with `amount > 500`: Always fail → end up in DLT (permanent failure)
- Orders with `amount < 500`: Fail first 2 attempts, succeed on 3rd (transient failure)

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Non-blocking retry | Failed messages go to separate retry topics |
| Exponential backoff | Delay doubles: 1s → 2s → 4s |
| DLT fallback | Messages exhausting retries go to dead letter topic |
| Retry topics | Auto-created: `topic-retry-0`, `topic-retry-1`, etc. |
| Selective retry | Only `RuntimeException` triggers retry |
| Main consumer unblocked | Retry happens on separate topics/consumers |

## Why Non-Blocking Retry?

- **Blocking retry** (in-place): Holds up the partition, delays all subsequent messages
- **Non-blocking retry** (topic-based): Message moves to a retry topic, main consumer continues processing other messages immediately
- Trade-off: More topics and consumers, but much better throughput under failure conditions
