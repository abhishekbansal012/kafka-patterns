# Module 03: Partitioning Strategies

## Goal

Demonstrate how custom partitioning controls message distribution across topic partitions. Partitioning directly impacts ordering guarantees, load balancing, and data locality — making it a critical design decision for any Kafka system.

## What This Module Does

- Implements custom `Partitioner` interface with two strategies
- Shows priority-based routing (high-priority messages get dedicated partitions)
- Shows region-based routing (geographic data locality)
- Compares custom partitioners against Kafka's default (murmur2 hash on key)

## Technical Details

### PriorityPartitioner

Routes messages to partitions based on priority level encoded in the key.

- **Key format**: `PRIORITY-orderId` (e.g., `HIGH-order123`, `LOW-order456`)
- **Partition mapping**:
  - `HIGH` → Partition 0 (dedicated fast-lane for SLA-sensitive messages)
  - `MEDIUM` → Partitions 1–2 (shared pool)
  - `LOW` → Partitions 3+ (bulk processing)
- **Use case**: VIP customer handling, SLA-based processing tiers

### RegionBasedPartitioner

Routes messages based on geographic region for data locality.

- **Key format**: `REGION-orderId` (e.g., `US-EAST-order123`)
- **Partition mapping**:
  - `US-EAST` → Partition 0
  - `US-WEST` → Partition 1
  - `EU` → Partition 2
  - `APAC` → Partition 3
  - Unknown → Hash-based fallback
- **Use case**: Regional compliance (GDPR), co-located consumers, geographic isolation

### How Custom Partitioners Plug In

- Implement `org.apache.kafka.clients.producer.Partitioner`
- Three methods: `partition()`, `close()`, `configure()`
- Registered via producer config property: `partitioner.class`
- Cluster metadata (partition count) available inside `partition()` method

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Default partitioning | Murmur2 hash on key (uniform distribution) |
| Custom partitioner | `Partitioner` interface implementation |
| Ordering guarantee | Messages with same key always go to same partition |
| Data locality | Region-based partitioner co-locates related data |
| Priority lanes | Dedicated partitions for high-priority traffic |
| Fallback strategy | Hash-based fallback when region is unknown |

## Why Custom Partitioning? Benefits Over Default

### Region-Based Partitioning Benefits

| Benefit | Explanation |
|---------|-------------|
| **Logical grouping** | All EU orders go to partition 2. A dedicated EU consumer processes only EU messages without filtering — no `if (region == EU)` checks needed. |
| **Specialized consumers** | Each region gets its own consumer with region-specific logic (tax rules, currency, language). No consumer handles all regions. |
| **Failure isolation** | A traffic spike in US-EAST (partition 0) won't slow down APAC processing (partition 3). They're independent streams. |
| **Independent scaling** | EU backlog growing? Add more consumers for partition 2 without touching APAC or US consumers. |
| **Ordering per region** | All EU messages maintain strict relative order within partition 2. |

> **Important nuance:** If your Kafka cluster is in a single region (e.g., all brokers in India), region-based partitioning gives you **logical isolation** — not physical data locality. All partitions still physically live on brokers in India. True geographic data locality (where EU data physically stays in the EU) requires a **multi-region Kafka setup** (see below).

### Priority-Based Partitioning Benefits

| Benefit | Explanation |
|---------|-------------|
| **SLA guarantees** | HIGH priority gets a dedicated partition (0). More consumer resources assigned to it ensures VIP orders process in milliseconds. |
| **No head-of-line blocking** | LOW messages piling up on partitions 3-5 don't block HIGH on partition 0 — they're separate streams with separate consumers. |
| **Selective scaling** | Scale consumers independently per tier. Need faster HIGH processing? Add consumers for partition 0. LOW backlog? Scale partitions 3-5. |
| **Ordering within tier** | All HIGH messages go to partition 0, maintaining strict ordering relative to each other. |

### Default Partitioning (murmur2 hash) vs Custom

| Aspect | Default (hash on key) | Custom partitioner |
|--------|----------------------|-------------------|
| Distribution | Uniform across all partitions | Semantic grouping by business logic |
| Use case | Generic load balancing | SLA tiers, regional compliance, data locality |
| Ordering | Per-key ordering | Per-group ordering (all HIGH ordered, all EU ordered) |
| Risk | None (uniform) | Hot partitions if one group has disproportionate traffic |
| Consumer design | All consumers are identical | Consumers can specialize per partition/group |

### The Tradeoff

Custom partitioners trade **uniform distribution** for **semantic grouping**. Messages that should be processed together, with different SLAs, or in different locations are physically grouped on the broker. This lets the consumer side specialize.

The risk: hot partitions. If 90% of traffic is HIGH priority, partition 0 becomes a bottleneck while partitions 3-5 sit idle. Monitor partition lag and adjust your strategy accordingly.

### When Does Region-Based Partitioning Actually Provide Data Locality?

Region-based partitioning gives **physical** data locality only when the Kafka infrastructure itself is multi-region. Here's when each benefit actually applies:

| Scenario | Logical isolation | Physical data locality | GDPR compliance |
|----------|:-:|:-:|:-:|
| Single-region cluster (all brokers in India) | ✅ | ❌ | ❌ |
| Multi-region cluster with rack-aware replication | ✅ | ✅ | ✅ |
| Separate clusters per region + MirrorMaker | ✅ | ✅ | ✅ |
| Confluent Cluster Linking (multi-region) | ✅ | ✅ | ✅ |

### What is a Multi-Region Kafka Cluster?

A multi-region Kafka cluster is a setup where brokers are deployed across multiple geographic locations (data centers / cloud regions), and Kafka's replication is configured to be aware of this geography.

**How it works:**

1. **Broker rack awareness** — Each broker is tagged with its physical location using `broker.rack` config (e.g., `broker.rack=eu-west-1`). Kafka uses this to place partition replicas across racks/regions.

2. **Replica placement** — With rack-aware replication, Kafka ensures that a partition's leader and replicas are spread across regions. You can configure it so that partition 2's leader is always in the EU region.

3. **Follower fetching** — Kafka 2.4+ introduced `replica.selector.class` which allows consumers to read from the closest replica (not just the leader). An EU consumer can read from an EU follower without cross-region traffic.

#### Glossary: Key Multi-Region Concepts

**MirrorMaker (MirrorMaker 2)**

A Kafka tool that replicates data between two separate Kafka clusters. It's essentially a consumer on Cluster A + a producer on Cluster B, running continuously.

- Cluster A (EU) has orders from EU customers
- MirrorMaker reads those messages and writes them to Cluster B (US) — or selectively doesn't, keeping EU data in EU
- MM2 (version 2) supports topic filtering, offset translation, and automatic consumer group sync
- Think of it as a bridge between independent Kafka clusters

Real-world use: You have a Kafka cluster in Frankfurt and one in Virginia. MirrorMaker replicates only the topics/partitions each region needs. Region-based partitioning makes this filtering trivial — replicate partitions 0-1 (US) to the US cluster, keep partition 2 (EU) only in the EU cluster.

**Broker Rack Awareness**

A Kafka broker config (`broker.rack`) that tells Kafka which physical location (rack, availability zone, or region) a broker lives in.

```properties
# broker config on an EU broker
broker.rack=eu-west-1

# broker config on a US broker
broker.rack=us-east-1
```

Without this, Kafka has no idea that Broker 1 and Broker 2 are in the same data center — it might put all replicas in one rack, defeating the purpose of replication for fault tolerance. With it, Kafka intelligently distributes replicas across locations.

**Replica Placement**

How Kafka decides which brokers hold the leader and follower copies of a partition.

- Every partition has 1 leader (handles reads/writes) and N-1 followers (replicas for durability)
- With rack awareness enabled, Kafka spreads replicas across different racks/regions
- Example with `replication-factor=3` and rack awareness:

```
Partition 2 (EU data):
  Leader   → Broker in eu-west-1     (primary reads/writes)
  Replica 1 → Broker in us-east-1    (failover + cross-region availability)
  Replica 2 → Broker in ap-south-1   (failover)
```

This guarantees that if an entire region goes down, the partition still has a live replica elsewhere to promote to leader.

**Follower Fetching (KIP-392, Kafka 2.4+)**

By default, consumers can only read from the **partition leader** — even if a follower replica is sitting right next to them in the same data center.

Follower fetching changes this: consumers read from the **nearest replica** instead of always going to the leader.

```
Without follower fetching:
  EU Consumer → reads from US-EAST (leader) → cross-region latency (~100ms+)

With follower fetching:
  EU Consumer → reads from EU (local follower) → local latency (~1-5ms)
```

Configured via:
- Broker side: `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector`
- Consumer side: `client.rack=eu-west-1` (tells Kafka where the consumer is)

Kafka matches the consumer's rack to the closest replica and serves reads from there. The follower might be slightly behind the leader (async replication lag), so you trade milliseconds of staleness for significantly lower read latency.

**Three common multi-region patterns:**

```
Pattern 1: Stretched Cluster (single cluster, brokers in multiple regions)
┌─────────────────────────────────────────────────┐
│              Single Kafka Cluster                │
│                                                 │
│  Region: EU          Region: US-EAST            │
│  ┌──────────┐        ┌──────────┐              │
│  │ Broker 1 │        │ Broker 3 │              │
│  │ Broker 2 │        │ Broker 4 │              │
│  └──────────┘        └──────────┘              │
│  Partition 2 leader   Partition 0 leader        │
│  (EU data here)       (US data here)            │
└─────────────────────────────────────────────────┘
- Region-based partitioner routes EU data to partition 2
- Partition 2 leader lives on EU broker → data stays in EU
- EU consumer reads locally, zero cross-region traffic
```

```
Pattern 2: Separate Clusters + MirrorMaker 2
┌────────────┐    MirrorMaker    ┌────────────┐
│ EU Cluster │ ←───────────────→ │ US Cluster │
│ (Frankfurt)│                   │ (Virginia) │
└────────────┘                   └────────────┘
- Each region has its own cluster
- MirrorMaker replicates only what's needed
- Region partitioner ensures data originates in correct cluster
```

```
Pattern 3: Confluent Cluster Linking
┌────────────┐    Cluster Link   ┌────────────┐
│ EU Cluster │ ←────────────────→│ US Cluster │
│            │   (async mirror)  │            │
└────────────┘                   └────────────┘
- Managed replication between clusters
- Consumer reads from local cluster
- Region partitioner pre-sorts data for clean replication boundaries
```

**Why region-based partitioning matters for multi-region:**

In all three patterns, region-based partitioning ensures that data is **pre-sorted by geography at the producer level**. This makes it straightforward to:
- Place partition leaders in the correct region (Pattern 1)
- Replicate only relevant partitions to each regional cluster (Pattern 2 & 3)
- Guarantee that EU customer data never physically touches a US broker

**Without multi-region infrastructure**, region-based partitioning still provides valuable logical grouping — specialized consumers, failure isolation, and per-region ordering. Just not physical data residency.

## Design Considerations

- More partitions = more parallelism, but also more resource overhead
- Custom partitioners can cause hot partitions if distribution is skewed
- Partition count cannot be decreased after topic creation
- Consumer count is bounded by partition count (extra consumers sit idle)

## Implementation Details

### Project Structure

```
03-partitioning-strategies/
├── build.gradle                          # Depends on :common module
└── src/main/java/com/scaleatdesign/kafka/partitioning/
    ├── PartitioningApplication.java      # Spring Boot entry point (port 8093)
    ├── config/
    │   └── PartitioningConfig.java       # Topic creation + custom KafkaTemplate beans
    ├── controller/
    │   └── PartitioningController.java   # REST endpoints demonstrating each strategy
    └── partitioner/
        ├── PriorityPartitioner.java      # Priority-based partition routing
        └── RegionBasedPartitioner.java   # Region-based partition routing
```

### How the Custom Partitioner is Wired In

The key integration point is in `PartitioningConfig.java`. A dedicated `KafkaTemplate` bean is created with the custom partitioner set via producer config:

```java
@Bean("regionPartitionedTemplate")
public KafkaTemplate<String, OrderEvent> regionPartitionedTemplate() {
    Map<String, Object> props = Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
        ProducerConfig.PARTITIONER_CLASS_CONFIG, RegionBasedPartitioner.class  // <-- custom partitioner
    );
    ProducerFactory<String, OrderEvent> factory = new DefaultKafkaProducerFactory<>(props);
    return new KafkaTemplate<>(factory);
}
```

The `PARTITIONER_CLASS_CONFIG` property tells Kafka's producer to use our class instead of the default `DefaultPartitioner`. Kafka instantiates it reflectively and calls `configure()` once, then `partition()` for every message sent.

The topic is created with 6 partitions to give enough room for routing strategies to spread across:

```java
@Bean
public NewTopic ordersPartitionedTopic() {
    return TopicBuilder.name(KafkaTopics.ORDERS_PARTITIONED)
            .partitions(6)
            .replicas(1)
            .build();
}
```

### PriorityPartitioner — Implementation Walkthrough

```java
public class PriorityPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        int numPartitions = cluster.partitionsForTopic(topic).size();

        if (key == null || numPartitions <= 1) {
            return 0;  // Safety fallback
        }

        String keyStr = key.toString().toUpperCase();

        if (keyStr.startsWith("HIGH")) {
            return 0;  // Dedicated fast-lane partition
        } else if (keyStr.startsWith("MEDIUM")) {
            // Spread across ~1/3 of available partitions starting at index 1
            int partition = 1 + (Math.abs(keyStr.hashCode()) % Math.max(1, numPartitions / 3));
            return Math.min(partition, numPartitions - 1);
        } else {
            // LOW: use the upper half of partition space
            int lowStart = Math.max(2, numPartitions / 2);
            int partition = lowStart + (Math.abs(keyStr.hashCode()) % (numPartitions - lowStart));
            return Math.min(partition, numPartitions - 1);
        }
    }
}
```

**How it works with 6 partitions:**

| Priority | Partition(s) | Strategy |
|----------|-------------|----------|
| HIGH | 0 | Fixed — all high-priority land here for fastest processing |
| MEDIUM | 1–2 | Hash within range — distributes across `numPartitions / 3` slots |
| LOW | 3–5 | Hash within range — uses upper half for bulk traffic |

The partition math ensures that even if `numPartitions` changes (e.g., topic scaled up), the routing logic adapts without code changes. The `Math.min` guard prevents out-of-bounds when partition count is small.

### RegionBasedPartitioner — Implementation Walkthrough

```java
public class RegionBasedPartitioner implements Partitioner {

    private static final Map<String, Integer> REGION_PARTITION_MAP = Map.of(
        "US-EAST", 0,
        "US-WEST", 1,
        "EU",      2,
        "APAC",    3
    );

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        int numPartitions = cluster.partitionsForTopic(topic).size();

        if (key == null) return 0;

        String keyStr = key.toString();
        for (Map.Entry<String, Integer> entry : REGION_PARTITION_MAP.entrySet()) {
            if (keyStr.startsWith(entry.getKey())) {
                return entry.getValue() % numPartitions;  // Modulo guard
            }
        }

        // Fallback: hash-based for unknown regions
        return Math.abs(keyStr.hashCode()) % numPartitions;
    }
}
```

**How it works:**

1. A static `Map<String, Integer>` defines the region → partition mapping at class level.
2. The key is parsed by prefix matching (`startsWith`) — no regex, no parsing overhead.
3. The `% numPartitions` guard ensures we never return a partition index beyond what the topic has.
4. Unknown regions fall through to a hash-based assignment — still deterministic (same key = same partition), just not region-aligned.

This approach gives you **data locality** — all EU orders go to partition 2, meaning a consumer dedicated to EU compliance processing can subscribe to just that partition.

### Controller — Three Partitioning Strategies Exposed via REST

The `PartitioningController` exposes three endpoints that demonstrate different ways to control partition assignment:

#### 1. Region-Based Partitioning (`POST /api/partitioning/region`)

Uses the custom `regionPartitionedTemplate` (backed by `RegionBasedPartitioner`):

```java
String key = region + "-" + event.getOrderId();  // e.g., "US-EAST-order123"
regionTemplate.send(KafkaTopics.ORDERS_PARTITIONED, key, event);
```

The key prefix determines the partition. The partitioner picks it up and routes accordingly.

#### 2. Explicit Partition Selection (`POST /api/partitioning/explicit/{partition}`)

Bypasses any partitioner entirely by specifying the partition index directly in the `send()` call:

```java
defaultTemplate.send(KafkaTopics.ORDERS_PARTITIONED, partition, event.getOrderId(), event);
```

When you pass a partition number to `KafkaTemplate.send()`, Kafka ignores the partitioner and writes directly to that partition. Useful for testing or forced routing.

#### 3. Key-Based Ordering (`POST /api/partitioning/key-ordering`)

Demonstrates Kafka's default guarantee — same key always routes to same partition:

```java
String key = customerId;  // Fixed key for all 5 messages
defaultTemplate.send(KafkaTopics.ORDERS_PARTITIONED, key, event);
```

Sends 5 orders for the same `customerId`. Since the default partitioner uses `murmur2(key) % numPartitions`, all 5 land on the same partition, preserving ordering.

### Configuration (application.yml)

```yaml
spring:
  application:
    name: 03-partitioning-strategies
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: partitioning-consumer-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.scaleatdesign.*

server:
  port: 8093
```

Key points:
- The **default** producer config uses `StringSerializer` for keys and `JsonSerializer` for values — this applies to the `defaultTemplate`.
- The **custom** `regionPartitionedTemplate` overrides these in Java config and additionally sets `PARTITIONER_CLASS_CONFIG`.
- Consumer uses `JsonDeserializer` with trusted packages to deserialize `OrderEvent` objects.
- Runs on port `8093` to avoid conflicts with other modules in this project.

### How to Test

```bash
# Region-based partitioning (observe partition assignments in logs)
curl -X POST http://localhost:8093/api/partitioning/region

# Explicit partition (force message to partition 2)
curl -X POST http://localhost:8093/api/partitioning/explicit/2

# Key-based ordering (5 messages, same partition guaranteed)
curl -X POST http://localhost:8093/api/partitioning/key-ordering
```
