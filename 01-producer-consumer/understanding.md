# Module 01: Basic Producer-Consumer Pattern

## Goal

Demonstrate the foundational Kafka pattern — producing messages to a topic and consuming them. This is the starting point for any Kafka-based system and covers the essential mechanics of message flow.

## What This Module Does

- Exposes a REST API (`/api/orders`) that accepts order requests
- Produces `OrderEvent` messages to Kafka using `KafkaTemplate`
- Consumes messages using `@KafkaListener` annotation
- Shows both async (callback-based) and sync (blocking) send approaches
- Demonstrates two consumption styles: typed payload with header extraction, and raw `ConsumerRecord`

## Technical Details

### Producer (`OrderProducer`)

- Uses `KafkaTemplate<String, OrderEvent>` for type-safe message sending
- Messages are keyed by `orderId` — this guarantees ordering within a partition for the same order
- `CompletableFuture` callbacks log partition and offset on success, or error on failure
- Also provides a synchronous `.get()` variant for cases where send confirmation is needed before proceeding

### Consumer (`OrderConsumer`)

- **Typed approach**: Spring auto-deserializes JSON into `OrderEvent`; partition, offset, and key are extracted via `@Header`
- **Raw approach**: Uses `ConsumerRecord<String, OrderEvent>` for full metadata control (timestamp, headers, etc.)
- Two separate consumer groups (`order-consumer-typed`, `order-consumer-raw`) allow independent consumption

### Serialization

- Producer: `StringSerializer` (key) + `JsonSerializer` (value)
- Consumer: `StringDeserializer` (key) + `JsonDeserializer` (value)
- Trusted packages configured to allow deserialization of `com.scaleatdesign.*` classes

### Configuration

- Bootstrap server: `localhost:9092`
- Topic: `orders` (3 partitions, 1 replica)
- Consumer offset reset: `earliest` (reads from beginning on first join)
- Server port: `8091`

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Async produce | `CompletableFuture` + `whenComplete` callback |
| Key-based routing | `orderId` as message key → same partition for same order |
| Consumer groups | Two independent groups consuming same/different topics |
| JSON serde | Spring Kafka's built-in JSON serializer/deserializer |
| REST → Kafka bridge | Controller triggers producer on HTTP request |
