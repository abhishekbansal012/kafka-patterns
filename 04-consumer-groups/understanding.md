# Module 04: Consumer Groups & Rebalancing

## What is a Consumer Group?

A **Consumer Group** is a set of consumer instances (threads or processes) that cooperate to consume messages from one or more Kafka topics. Kafka uses the `group.id` to identify which consumers belong together.

**The core rule**: Within a consumer group, each partition is assigned to exactly one consumer. No two consumers in the same group ever read from the same partition simultaneously.

### Why We Need Consumer Groups

1. **Parallel processing (horizontal scaling)**: A single consumer can't keep up with high-throughput topics. Consumer groups let you split the work across multiple threads or machines. Add more consumers → more partitions are processed in parallel → higher throughput.

2. **Offset tracking per group**: Kafka tracks the last committed offset *per group*. This means the group collectively knows where it left off. If a consumer restarts, it resumes from the last committed offset — no message is missed, no manual bookkeeping needed.

3. **Fault tolerance**: If one consumer in the group crashes, its partitions are automatically reassigned to surviving consumers (rebalancing). The system self-heals without manual intervention.

4. **Load balancing**: Kafka automatically distributes partitions evenly across consumers in the group. You don't write load-balancing logic — Kafka's group coordinator handles it.

### What Effect Does the Consumer Group Have?

| Effect | Explanation |
|--------|-------------|
| Partitions are exclusive | Each partition goes to exactly one consumer in the group — no duplicates within the group |
| Offset is group-scoped | Two different groups reading the same topic maintain independent offsets |
| Scaling is bounded | Max useful consumers = number of partitions (extras sit idle) |
| Rebalancing is automatic | Add/remove consumers → Kafka redistributes partitions |
| Ordering guarantee | Messages within a partition are processed in order by a single consumer |

### What "Each Partition Goes to Exactly One Consumer" Means

At any given time, within a single consumer group, **only one consumer thread/instance reads from a particular partition**. No two consumers in the same group will ever receive messages from the same partition simultaneously.

**Why this matters:**
- It guarantees **ordering** — messages in a partition are always processed sequentially by one consumer. If two consumers could read the same partition, you'd get race conditions and out-of-order processing.
- It prevents **duplicate processing** within the group — each message is delivered to exactly one consumer, not broadcast to all of them.

**In this module's setup (6 partitions, 3 threads):**

```
Topic: orders-grouped (6 partitions)
Group: order-processing-group (3 consumer threads)

Partition 0 ──→ order-group-consumer-0-C-1  ✓ (exclusive owner)
Partition 1 ──→ order-group-consumer-0-C-1  ✓ (same consumer, it got 2)
Partition 2 ──→ order-group-consumer-1-C-1  ✓
Partition 3 ──→ order-group-consumer-1-C-1  ✓
Partition 4 ──→ order-group-consumer-2-C-1  ✓
Partition 5 ──→ order-group-consumer-2-C-1  ✓

❌ Partition 0 ──→ consumer-0 AND consumer-1 simultaneously
   ^ This NEVER happens within the same group
```

**What happens if you have more consumers than partitions:**

```
6 partitions, 8 consumers:
  consumer-0 → partition [0]
  consumer-1 → partition [1]
  consumer-2 → partition [2]
  consumer-3 → partition [3]
  consumer-4 → partition [4]
  consumer-5 → partition [5]
  consumer-6 → idle (nothing to read)
  consumer-7 → idle (nothing to read)
```

The extras sit completely idle — they join the group, receive zero partitions, and wait. This is why the number of partitions is the **upper bound on parallelism** within a group. In this module, 6 partitions means at most 6 consumer threads can be actively working. Setting `setConcurrency(8)` would waste 2 threads.

### When You Need Different Consumer Groups

Use **separate consumer groups** when multiple services or subsystems need to independently process the same messages:

```
Topic: "orders"
   │
   ├── group.id = "order-processing-group"
   │       → Processes orders, updates inventory, sends confirmation
   │       → Offset: partition-0 at offset 150
   │
   ├── group.id = "analytics-group"
   │       → Aggregates order metrics, builds dashboards
   │       → Offset: partition-0 at offset 148 (slightly behind)
   │
   └── group.id = "notification-group"
           → Sends push notifications, emails
           → Offset: partition-0 at offset 150
```

Each group gets **every message** independently. They maintain their own offsets and progress at their own pace.

**Use the same group** when you want to split work (each message processed once):
- 3 consumers in `"order-processing-group"` → each message goes to exactly one of them

**Use different groups** when you want broadcast (each message processed by all):
- `"order-processing-group"` AND `"analytics-group"` both read the same topic → both see every message

### Real-World Examples

| Scenario | Same or Different Group? | Why |
|----------|--------------------------|-----|
| 3 instances of the same microservice handling orders | Same group | Split the load — each order processed once |
| Order service + Analytics service reading `orders` topic | Different groups | Both need every message independently |
| Blue/green deployment (old + new version running temporarily) | Same group | New instances take over partitions from old ones via rebalance |
| Replay historical data for a new feature | New group (fresh offsets) | Start reading from `earliest` without affecting existing consumers |

### In This Module

The code uses a single consumer group `"order-processing-group"` with `concurrency=3` to demonstrate the **same-group load-splitting** pattern:

```java
@KafkaListener(
    id = "order-group-consumer",
    topics = KafkaTopics.ORDERS_GROUPED,
    groupId = "order-processing-group",  // ← the group identity
    containerFactory = "manualAckFactory"
)
```

All 3 threads (and any additional app instances) share this group ID, so Kafka divides the 6 partitions among them. If you wanted an analytics service to also read from `orders-grouped`, you'd create a separate listener with `groupId = "analytics-group"` — it would receive all messages independently without interfering with the order processing group's offsets.

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

**Important: `ConsumerGroupConfig` is NOT per topic.** It defines a reusable **container factory** (`manualAckFactory`). Any `@KafkaListener` in the application can reference this factory via `containerFactory = "manualAckFactory"`, regardless of which topic it subscribes to. The factory is topic-agnostic — it only controls *how* consumption happens (concurrency, ack mode, rebalance listener), not *what* is consumed.

| Concern | Where it's decided | Scope |
|---------|-------------------|-------|
| Which topic to consume | `@KafkaListener(topics = ...)` | Per listener method |
| Which group to join | `@KafkaListener(groupId = ...)` | Per listener method |
| Concurrency / ack mode / rebalance listener | `manualAckFactory` bean in `ConsumerGroupConfig` | Per factory (shared by all listeners referencing it) |
| Topic creation (partitions, replicas) | `ordersGroupedTopic()` bean | Per topic (convenience — lives in same class, not coupled) |

If you had a second listener on a different topic, it could reuse the same factory:
```java
@KafkaListener(
    topics = "another-topic",
    groupId = "another-group",
    containerFactory = "manualAckFactory"  // same factory, different topic
)
public void consumeOther(...) { ... }
```
Both listeners would get concurrency=3, manual ack, and the rebalance logger — but they'd subscribe to different topics with independent group IDs. You can also define multiple factory beans (e.g., `batchFactory`, `autoAckFactory`) for listeners with different consumption needs.

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Consumer group | Multiple consumers share partitions within a group |
| Rebalancing | `ConsumerRebalanceListener` logs assign/revoke events |
| Manual ack | `AckMode.MANUAL` — commit only after success |
| At-least-once | Failed messages are re-delivered (not acknowledged) |
| Concurrency | Multiple threads consume from different partitions |
| Partition ownership | Each partition is owned by exactly one consumer in a group |

## How the Code Achieves Consumer Groups

### 1. Forming a Consumer Group

All consumers that share the same `group.id` form a single consumer group. Kafka's group coordinator then distributes partitions among them.

**In `application.yml`:**
```yaml
spring:
  kafka:
    consumer:
      group-id: order-processing-group
      enable-auto-commit: false
```

**In `ManualAckConsumer.java`:**
```java
@KafkaListener(
    topics = KafkaTopics.ORDERS_GROUPED,
    groupId = "order-processing-group",
    containerFactory = "manualAckFactory"
)
```

The `groupId` on the `@KafkaListener` explicitly binds this consumer to the `order-processing-group`. Every instance (or thread) using this group ID competes for partitions from the `orders-grouped` topic.

### 2. Parallel Consumption via Concurrency

A single application instance simulates multiple consumers within the group by spawning concurrent listener threads.

**In `ConsumerGroupConfig.java`:**
```java
factory.setConcurrency(3); // 3 consumer threads
```

**In `application.yml`:**
```yaml
spring:
  kafka:
    listener:
      concurrency: 3
```

This creates 3 `KafkaMessageListenerContainer` threads inside one JVM. Each thread acts as an independent consumer within the group. Kafka assigns a subset of the 6 partitions (configured on the topic) to each thread. With 6 partitions and concurrency=3, each thread gets roughly 2 partitions.

### 3. Partition Distribution Across the Topic

The topic is created with 6 partitions to allow meaningful distribution:

**In `ConsumerGroupConfig.java`:**
```java
@Bean
public NewTopic ordersGroupedTopic() {
    return TopicBuilder.name(KafkaTopics.ORDERS_GROUPED)
            .partitions(6)
            .replicas(1)
            .build();
}
```

With 6 partitions and 3 concurrent threads, Kafka's partition assignor (default: RangeAssignor) gives each thread 2 partitions. If you run a second instance of the app (also with concurrency=3), the 6 partitions get redistributed across all 6 threads — 1 partition each.

### 4. Key-Based Routing for Ordered Processing

The producer uses the customer ID as the message key, ensuring all orders from the same customer land on the same partition:

**In `ConsumerGroupController.java`:**
```java
kafkaTemplate.send(KafkaTopics.ORDERS_GROUPED, event.getCustomerId(), event);
```

Kafka hashes the key (`CUST-0` through `CUST-4`) to determine the target partition. This guarantees ordering per customer within the group — all messages for `CUST-2` go to the same partition, processed by the same consumer thread.

### 5. Rebalance Awareness

When the group membership changes (thread starts/stops, new instance joins), Kafka triggers a rebalance. The `RebalanceLogger` hooks into this lifecycle:

**In `RebalanceLogger.java`:**
```java
@Override
public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    log.warn("⚠️  PARTITIONS REVOKED: {} — Finish processing before handoff!",
            formatPartitions(partitions));
}

@Override
public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    log.info("✅ PARTITIONS ASSIGNED: {} — Ready to consume",
            formatPartitions(partitions));
}
```

**Registered in `ConsumerGroupConfig.java`:**
```java
factory.getContainerProperties().setConsumerRebalanceListener(rebalanceLogger);
```

During a rebalance: revoked is called first (commit pending work), then assigned is called (start consuming new partitions). This gives the application a hook to flush buffers or release resources before ownership changes.

### 6. Manual Acknowledgment for At-Least-Once Delivery

Auto-commit is disabled at two levels to ensure messages are acknowledged only after successful processing:

**In `application.yml`:**
```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false
    listener:
      ack-mode: manual
```

**In `ConsumerGroupConfig.java`:**
```java
factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
```

**In `ManualAckConsumer.java`:**
```java
try {
    processOrder(event);
    acknowledgment.acknowledge(); // offset committed only here
} catch (Exception e) {
    log.error("Processing failed — NOT acknowledging (will retry)", ...);
    // offset NOT committed → Kafka re-delivers on next poll
}
```

If a consumer thread crashes mid-processing (before `acknowledge()`), the partition is reassigned during rebalance and the new owner re-reads from the last committed offset — no message is lost.

### 7. Session & Heartbeat Tuning

The consumer group coordinator uses heartbeats to detect dead consumers:

**In `application.yml`:**
```yaml
spring:
  kafka:
    consumer:
      properties:
        session.timeout.ms: 30000
        heartbeat.interval.ms: 10000
        max.poll.records: 10
```

- `session.timeout.ms=30000`: If no heartbeat is received for 30s, the consumer is considered dead and rebalance triggers.
- `heartbeat.interval.ms=10000`: Heartbeats are sent every 10s (rule of thumb: 1/3 of session timeout).
- `max.poll.records=10`: Limits batch size per poll to prevent long processing causing session timeout.

### End-to-End Flow

```
POST /api/consumer-groups/produce/20
    │
    ▼
ConsumerGroupController produces 20 OrderEvents
    │  (key = customerId → deterministic partition)
    ▼
Topic: orders-grouped (6 partitions)
    │
    ├─── Partition 0,1 → Consumer Thread 1 ─┐
    ├─── Partition 2,3 → Consumer Thread 2 ─┤── ManualAckConsumer.consume()
    └─── Partition 4,5 → Consumer Thread 3 ─┘
                                              │
                                              ▼
                                    processOrder() succeeds?
                                      ├─ YES → acknowledgment.acknowledge()
                                      └─ NO  → skip ack → re-delivery on next poll
```

When a second app instance joins with the same `group-id`, Kafka triggers rebalance:
- `onPartitionsRevoked()` fires on existing threads
- Partitions are redistributed (e.g., 2 per thread across 2 instances = 1 each)
- `onPartitionsAssigned()` fires with new assignments

## Deep Dive: Partition Assignment & Rebalancing Mechanics

### How Kafka Assigns 2 Partitions to Each Thread

This is not random — it follows a deterministic protocol between the broker's **Group Coordinator** and the consumers.

#### Step-by-Step: What Happens at Startup

1. **App starts → Spring creates 3 consumer threads** (because of `factory.setConcurrency(3)` in `ConsumerGroupConfig.java`). Each thread internally creates its own `KafkaConsumer` instance.

2. **Each consumer sends a `JoinGroup` request** to the Kafka broker's Group Coordinator (the broker responsible for `order-processing-group`). The request includes:
   - `group.id`: `"order-processing-group"` (from `@KafkaListener(groupId = ...)`)
   - `session.timeout.ms`: `30000` (from `application.yml`)
   - Subscribed topics: `["orders-grouped"]`

3. **Group Coordinator elects a "leader" consumer** — the first one to join becomes the leader. The leader receives the full list of group members and their subscriptions.

4. **Leader runs the partition assignment strategy** (default: `RangeAssignor`). With 6 partitions and 3 consumers:
   ```
   Partitions: [0, 1, 2, 3, 4, 5]
   Consumers:  [consumer-0, consumer-1, consumer-2]
   
   RangeAssignor logic:
     partitions_per_consumer = 6 / 3 = 2
     consumer-0 → partitions [0, 1]
     consumer-1 → partitions [2, 3]
     consumer-2 → partitions [4, 5]
   ```

5. **Leader sends assignment back** to the Group Coordinator via a `SyncGroup` response.

6. **Each consumer receives its assignment** and starts polling only from its partitions.

#### What Code Controls This

| What | Where | Effect |
|------|-------|--------|
| Number of consumers in this JVM | `factory.setConcurrency(3)` in `ConsumerGroupConfig.java` | 3 threads → 3 `JoinGroup` requests |
| Number of partitions to distribute | `.partitions(6)` in `ordersGroupedTopic()` bean | 6 slots to divide among consumers |
| Which group they join | `groupId = "order-processing-group"` in `@KafkaListener` | All 3 threads join the same group |
| Assignment strategy | Kafka default (`RangeAssignor`) — not overridden in this module | Contiguous ranges of partitions per consumer |

#### Why Exactly 2 Per Thread

```
total_partitions / total_consumers_in_group = partitions_per_consumer
6 / 3 = 2
```

If you changed `setConcurrency(6)`, each thread would get exactly 1 partition. If you set `setConcurrency(8)`, 6 threads get 1 partition each and 2 threads sit idle (no partitions to assign).

---

### How Rebalancing Works When a New Instance Joins

#### The Trigger

When you run a second instance:
```bash
SERVER_PORT=8095 ./gradlew :04-consumer-groups:bootRun --args='--server.port=8095'
```

That second JVM also has `setConcurrency(3)`, so it creates 3 more consumer threads — all sending `JoinGroup` for `"order-processing-group"`.

#### The Rebalance Protocol (Step by Step)

1. **New consumers send `JoinGroup`** to the Group Coordinator. The coordinator detects new members and initiates a rebalance.

2. **All existing consumers are notified** — their next `poll()` returns a "rebalance in progress" signal. Spring Kafka intercepts this and calls:
   ```java
   // RebalanceLogger.java
   @Override
   public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
       log.warn("⚠️  PARTITIONS REVOKED: {} — Finish processing before handoff!",
               formatPartitions(partitions));
   }
   ```
   This fires on the **first instance's 3 threads** — all 6 partitions are revoked temporarily.

3. **All consumers (old + new) send a fresh `JoinGroup`**. Now there are 6 consumers in the group.

4. **Leader re-runs `RangeAssignor`** with the new membership:
   ```
   Partitions: [0, 1, 2, 3, 4, 5]
   Consumers:  [instance1-thread0, instance1-thread1, instance1-thread2,
                instance2-thread0, instance2-thread1, instance2-thread2]
   
   New assignment:
     instance1-thread0 → partition [0]
     instance1-thread1 → partition [1]
     instance1-thread2 → partition [2]
     instance2-thread0 → partition [3]
     instance2-thread1 → partition [4]
     instance2-thread2 → partition [5]
   ```

5. **Each consumer receives new assignment**, and Spring Kafka calls:
   ```java
   // RebalanceLogger.java
   @Override
   public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
       log.info("✅ PARTITIONS ASSIGNED: {} — Ready to consume",
               formatPartitions(partitions));
   }
   ```

6. **Consumption resumes** — each thread now owns only 1 partition (6 partitions ÷ 6 threads).

#### Code That Enables Rebalance Awareness

The key registration happens in `ConsumerGroupConfig.java`:
```java
factory.getContainerProperties().setConsumerRebalanceListener(rebalanceLogger);
```

Without this line, rebalancing still happens (it's a Kafka protocol-level mechanism), but you wouldn't have application-level hooks to:
- Flush in-flight batches before partitions are taken away
- Initialize state for newly assigned partitions
- Log what's happening for debugging

#### What Happens When the Second Instance Stops

1. The stopping instance's consumers send `LeaveGroup` to the coordinator (graceful shutdown) — or the coordinator detects missed heartbeats after `session.timeout.ms=30000` (crash).

2. Rebalance triggers again — the 3 remaining consumers get all 6 partitions back (2 each).

3. **Critical**: Any messages the stopped instance consumed but hadn't acknowledged (no `acknowledgment.acknowledge()`) will be **re-delivered** to the new partition owner. This is the at-least-once guarantee in action.

#### Timeline Visualization

```
Time ──────────────────────────────────────────────────────────►

Instance 1 starts (3 threads)
├── JoinGroup × 3
├── Leader assigns: thread0=[0,1], thread1=[2,3], thread2=[4,5]
├── onPartitionsAssigned([0,1]) on thread0
├── onPartitionsAssigned([2,3]) on thread1
├── onPartitionsAssigned([4,5]) on thread2
├── Consuming normally...
│
│   Instance 2 starts (3 more threads)
│   ├── JoinGroup × 3 → triggers rebalance
│   │
├── onPartitionsRevoked([0,1]) on thread0  ─┐
├── onPartitionsRevoked([2,3]) on thread1   ├── "stop the world" pause
├── onPartitionsRevoked([4,5]) on thread2  ─┘
│   │
│   Leader re-assigns: 1 partition per thread
│   │
├── onPartitionsAssigned([0]) on inst1-thread0
├── onPartitionsAssigned([1]) on inst1-thread1
├── onPartitionsAssigned([2]) on inst1-thread2
│   ├── onPartitionsAssigned([3]) on inst2-thread0
│   ├── onPartitionsAssigned([4]) on inst2-thread1
│   ├── onPartitionsAssigned([5]) on inst2-thread2
│   │
│   Consuming normally (load split 50/50)...
│   │
│   Instance 2 stops (Ctrl+C)
│   ├── LeaveGroup × 3 → triggers rebalance
│   │
├── onPartitionsRevoked([0]) on inst1-thread0
├── onPartitionsRevoked([1]) on inst1-thread1
├── onPartitionsRevoked([2]) on inst1-thread2
│
│   Leader re-assigns: back to 2 per thread
│
├── onPartitionsAssigned([0,1]) on thread0
├── onPartitionsAssigned([2,3]) on thread1
├── onPartitionsAssigned([4,5]) on thread2
├── Consuming normally (full load)...
```

## Performance Impact of Rebalancing

### What Happens During the Rebalance Window

A rebalance is a **"stop the world" event** for the consumer group. During this period:

1. **All consumers in the group stop consuming** — no new messages are fetched from any partition
2. **No new offsets are committed** — in-progress work must complete or be abandoned
3. **The group is in a "rebalancing" state** — new `poll()` calls block until the rebalance completes

**Clarification: "Stop the world" does NOT mean in-flight messages are killed.**

What actually stops:
- No new `poll()` calls are made → no new messages arrive from the broker
- No new offsets are committed
- The consumer group is in a transitional state — nobody gets new work

What does NOT stop:
- If a thread is already inside `processOrder()`, that code **continues running to completion**. Spring Kafka does not interrupt the thread.

The sequence in this module's code:
```
Thread is currently executing:
    consume() {
        processOrder(event);          ← STILL RUNNING, not interrupted
        acknowledgment.acknowledge(); ← will still execute after processOrder returns
    }

    ↓ (processOrder finishes, acknowledge() completes)

THEN Spring Kafka fires:
    onPartitionsRevoked([...])  ← only NOW does revocation happen

THEN the thread stops polling for new messages until rebalance completes:
    onPartitionsAssigned([...])  ← new assignment received

THEN polling resumes with new partition ownership
```

So the message being processed **will finish processing and get acknowledged**. It won't be left unprocessed or abandoned mid-way. The "stop the world" pause is about the **gap between finishing the current batch and receiving the next batch** — no new work is assigned during that window.

**The only exception**: If `processOrder()` takes so long that `max.poll.interval.ms` (default 300000ms / 5 minutes) expires, the broker considers the consumer dead and forcibly reassigns its partitions. The same message may then be re-delivered to another consumer (duplicate processing, but not lost).

```
Timeline during rebalance:
──────────────────────────────────────────────────────────►
[consuming]  [REBALANCE - no consumption]  [consuming again]
             │                          │
             ├── onPartitionsRevoked()   │
             ├── JoinGroup/SyncGroup     │
             ├── onPartitionsAssigned()  │
             └──────── 1-30+ seconds ────┘
```

### How Long Does It Take?

The rebalance duration depends on:

| Factor | Impact |
|--------|--------|
| Number of consumers in group | More consumers = more JoinGroup/SyncGroup round-trips |
| `session.timeout.ms` (30000 in this module) | Max wait for a dead consumer to be detected |
| `max.poll.interval.ms` (default 300000) | If a consumer is stuck processing, rebalance won't trigger until this expires |
| Network latency to broker | Each protocol message adds latency |
| Work done in `onPartitionsRevoked()` | Flushing buffers/committing offsets adds time |

For this module's setup (3-6 consumers, local broker), rebalances typically take **1-5 seconds**. In production with many consumers across a network, it can take **10-30+ seconds**.

### What Happens to a Message Being Processed During Rebalance

This is the critical question. Let's trace what happens in `ManualAckConsumer`:

```java
public void consume(..., Acknowledgment acknowledgment) {
    // Thread is HERE when rebalance triggers
    processOrder(event);           // ← still running
    acknowledgment.acknowledge();  // ← may or may not reach this
}
```

**Scenario: Thread is mid-`processOrder()` when rebalance starts**

1. **The thread is NOT interrupted** — Spring Kafka waits for the current `consume()` invocation to finish before revoking partitions. The `onPartitionsRevoked()` callback fires only after the listener method returns.

2. **Two possible outcomes:**

   **Case A: `processOrder()` finishes quickly (before rebalance timeout)**
   ```
   processOrder() completes
       → acknowledgment.acknowledge() succeeds
       → offset committed
       → onPartitionsRevoked() fires
       → partition handed off
       → new owner starts from committed offset (message NOT re-delivered)
   ```

   **Case B: `processOrder()` takes too long (exceeds `max.poll.interval.ms`)**
   ```
   processOrder() still running...
       → max.poll.interval.ms expires
       → Kafka considers this consumer dead
       → Consumer is forcibly removed from group
       → Partition reassigned to another consumer
       → acknowledgment.acknowledge() fails silently (consumer is no longer in group)
       → new owner reads from last committed offset
       → MESSAGE IS RE-DELIVERED (duplicate processing)
   ```

3. **In this module's code**, if an exception occurs during processing:
   ```java
   } catch (Exception e) {
       log.error("Processing failed — NOT acknowledging (will retry)", ...);
       // offset NOT committed
   }
   ```
   The message will be re-delivered to whoever owns the partition after rebalance.

### The "Duplicate Processing" Risk

During rebalancing, the at-least-once guarantee means:

```
Before rebalance:
  Thread-0 owns partition [0,1]
  Thread-0 reads message at offset=50 from partition-0
  Thread-0 starts processOrder()...

Rebalance happens:
  Partition-0 is reassigned to Thread-3 (new instance)
  Thread-0's acknowledge() may not have been called yet

After rebalance:
  Thread-3 starts reading partition-0 from last committed offset (say, offset=49)
  Thread-3 re-processes message at offset=50  ← DUPLICATE
  Meanwhile Thread-0's processOrder() may also complete ← both processed it
```

This is why **idempotent consumers** (Module 07) are important — your processing logic should handle duplicates gracefully.

### How This Module's Code Mitigates the Impact

| Mechanism | Code | How It Helps |
|-----------|------|--------------|
| Short processing time | `Thread.sleep((long)(Math.random() * 100))` in `processOrder()` | Keeps processing well under `max.poll.interval.ms` |
| Small poll batches | `max.poll.records: 10` in `application.yml` | Less in-flight work during rebalance |
| Manual ack after success | `acknowledgment.acknowledge()` only after `processOrder()` | Uncommitted messages get re-delivered (no loss) |
| Rebalance logging | `RebalanceLogger` registered on factory | Visibility into when pauses occur |
| Heartbeat tuning | `heartbeat.interval.ms: 10000` | Frequent heartbeats prevent false-positive timeouts |

### Strategies to Reduce Rebalance Impact (Beyond This Module)

1. **Cooperative Sticky Assignor** — Only revokes partitions that actually need to move (Kafka 2.4+). Set `partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor` in consumer properties.

2. **Static group membership** — Assign a `group.instance.id` to each consumer. On restart, the same consumer reclaims its old partitions without triggering a full rebalance.

3. **Increase `session.timeout.ms`** — Gives slow consumers more time before being declared dead (trade-off: slower detection of actually dead consumers).

4. **Reduce processing time** — Offload heavy work to async threads; acknowledge quickly.

## Design Considerations

- Max parallelism = number of partitions (extra consumers are idle)
- Rebalancing causes a brief pause in consumption ("stop the world")
- Long processing times can trigger session timeout → unintended rebalance
- Manual ack gives reliability but requires careful error handling
- Consider `nack()` with backoff for transient failures


## How to Run

### Prerequisites

- Java 21+
- A running Kafka broker on `localhost:9092` (use Docker, Homebrew, or a local install)
- Gradle wrapper is included in the project root

### Start Kafka (if not already running)

Using Docker (quickest):
```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=1 \
  -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  bitnami/kafka:latest
```

### Build the Project

From the project root:
```bash
./gradlew :04-consumer-groups:build
```

### Run the Application

```bash
./gradlew :04-consumer-groups:bootRun
```

The app starts on port **8094** with 3 concurrent consumer threads automatically listening on `orders-grouped` topic.

## How to Test

### Test 1: Basic Consumer Group Load Balancing

Produce 20 messages and observe them distributed across threads:

```bash
curl -X POST http://localhost:8094/api/consumer-groups/produce/20
```

**What to observe in logs:**
- Messages are consumed by different threads (look for `thread=` in log lines)
- Each thread handles a subset of partitions (e.g., `partition=0`, `partition=1` on one thread)
- All messages for the same `customerId` land on the same partition (key-based routing)

Example log output:
```
Consumer [thread=order-group-consumer-0-C-1] received: orderId=..., partition=0, offset=5
Consumer [thread=order-group-consumer-1-C-1] received: orderId=..., partition=2, offset=3
Consumer [thread=order-group-consumer-2-C-1] received: orderId=..., partition=4, offset=7
```

**How multiple threads consume messages — the code chain:**

1. **`ManualAckConsumer.java`** declares the listener with `id = "order-group-consumer"`:
   ```java
   @KafkaListener(
       id = "order-group-consumer",
       topics = KafkaTopics.ORDERS_GROUPED,
       groupId = "order-processing-group",
       containerFactory = "manualAckFactory"
   )
   ```
   The `id` sets the thread name prefix (instead of the default verbose class name). The `containerFactory` points to our custom factory that enables concurrency.

2. **`ConsumerGroupConfig.java`** sets `concurrency(3)` on that factory:
   ```java
   factory.setConcurrency(3);
   ```
   This tells Spring Kafka to create **3 independent `KafkaMessageListenerContainer` instances** under one `ConcurrentMessageListenerContainer`. Each container runs its own polling loop in a separate thread, and each thread gets its own Kafka consumer instance internally.

3. **Kafka broker** assigns partitions to these 3 consumers (all share `groupId = "order-processing-group"`). With 6 partitions on the topic, each thread is assigned 2 partitions. Each thread only receives messages from its assigned partitions.

4. **Thread naming**: The thread names follow the pattern `{id}-{containerIndex}-C-{consumerIndex}`:
   - `order-group-consumer-0-C-1` → first container's consumer thread
   - `order-group-consumer-1-C-1` → second container's consumer thread
   - `order-group-consumer-2-C-1` → third container's consumer thread

5. **The log line** in `ManualAckConsumer.consume()` prints `Thread.currentThread().getName()`:
   ```java
   log.info("Consumer [thread={}] received: orderId={}, partition={}, offset={}",
       Thread.currentThread().getName(), event.getOrderId(), partition, offset);
   ```
   This is how you visually confirm that different threads are handling different partitions.

**In summary**: `setConcurrency(3)` creates 3 threads → Kafka assigns 2 partitions per thread → each thread calls `ManualAckConsumer.consume()` independently → the log prints the thread name proving parallel consumption.

### Test 2: Observe Rebalancing by Running a Second Instance

Open a second terminal and run another instance on a different port:

```bash
SERVER_PORT=8095 ./gradlew :04-consumer-groups:bootRun --args='--server.port=8095'
```

**What to observe:**
- In the first instance's logs: `⚠️ PARTITIONS REVOKED: [...]` — some partitions are taken away
- In the second instance's logs: `✅ PARTITIONS ASSIGNED: [...]` — it receives those partitions
- After rebalance settles, each instance owns ~3 partitions (6 total ÷ 2 instances)

Now produce more messages:
```bash
curl -X POST http://localhost:8094/api/consumer-groups/produce/20
```

Messages are now split across both instances.

### Test 3: Observe Rebalance on Consumer Shutdown

Stop the second instance (Ctrl+C). Watch the first instance's logs:

**What to observe:**
- `✅ PARTITIONS ASSIGNED: [orders-grouped-0, orders-grouped-1, ..., orders-grouped-5]`
- The first instance reclaims all 6 partitions
- Any unacknowledged messages from the stopped instance are re-delivered to the first instance

### Test 4: At-Least-Once Delivery Verification

To see re-delivery behavior, you can simulate a failure by temporarily modifying `processOrder()` to throw an exception for certain orders. Without acknowledgment, those messages will reappear on the next poll cycle.

**What to observe:**
- `Processing failed for orderId=... — NOT acknowledging (will retry)` in logs
- The same message appears again in subsequent log entries (re-delivery)
- Offset for that partition does not advance until acknowledgment succeeds

### Test 5: Produce a Large Batch to See Partition Distribution

```bash
curl -X POST http://localhost:8094/api/consumer-groups/produce/100
```

**What to observe:**
- With `max.poll.records=10`, each consumer thread fetches at most 10 records per poll
- Messages are evenly distributed across 6 partitions (~16-17 per partition)
- The 5 customer keys (`CUST-0` to `CUST-4`) consistently map to the same partitions

## What to Observe — Summary

| Scenario | Key Log / Behavior |
|----------|-------------------|
| Single instance, produce messages | 3 threads consume from different partitions |
| Second instance joins | `PARTITIONS REVOKED` + `PARTITIONS ASSIGNED` on both |
| Second instance stops | First instance gets all 6 partitions back |
| Processing failure | Message re-delivered (no ack = no offset commit) |
| Same customer key | Always routed to same partition → same thread |
| Large batch | `max.poll.records=10` limits per-poll batch size |
