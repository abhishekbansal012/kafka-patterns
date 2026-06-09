# Module 02: Avro Serialization + Schema Registry

## Goal

Demonstrate schema-driven serialization using Apache Avro and Confluent Schema Registry. This replaces plain JSON with a strongly-typed, versioned schema that enables safe schema evolution across producers and consumers.

## What This Module Does

- Defines an Avro schema (`order.avsc`) for order events
- Produces messages serialized with Avro using `KafkaAvroSerializer`
- Consumes messages deserialized with `KafkaAvroDeserializer` into generated Java classes
- Registers schemas automatically with Confluent Schema Registry
- Supports schema evolution (backward/forward compatibility)

## Technical Details

### Avro Schema (`order.avsc`)

- Record type: `OrderAvro` in namespace `com.scaleatdesign.kafka.avro`
- Fields: `eventId`, `eventType`, `timestamp` (logical type: timestamp-millis), `orderId`, `customerId`, `productId`, `quantity`, `amount` (decimal with precision 10, scale 2), `status` (enum)
- Enum `OrderStatusAvro`: CREATED, CONFIRMED, PROCESSING, SHIPPED, DELIVERED, CANCELLED, FAILED
- Gradle plugin generates Java classes from `.avsc` at compile time

### Producer Configuration

- Value serializer: `io.confluent.kafka.serializers.KafkaAvroSerializer`
- Schema Registry URL: `http://localhost:8081`
- `auto.register.schemas: true` — schema is registered on first produce
- Subject naming: `TopicNameStrategy` (subject = `<topic>-value`)

### Consumer Configuration

- Value deserializer: `io.confluent.kafka.serializers.KafkaAvroDeserializer`
- `specific.avro.reader: true` — deserializes into generated `OrderAvro` class (not `GenericRecord`)
- Same Schema Registry URL for schema lookup

### Infrastructure Requirements

- Kafka broker: `localhost:9092`
- Confluent Schema Registry: `localhost:8081`
- Server port: `8092`

## Key Concepts Illustrated

| Concept | How It's Shown |
|---------|---------------|
| Schema definition | `.avsc` file with typed fields and logical types |
| Code generation | Gradle Avro plugin generates Java classes |
| Schema Registry | Auto-registration + lookup on produce/consume |
| Type safety | Specific Avro reader gives compile-time type safety |
| Schema evolution | Add optional fields without breaking consumers |
| Binary encoding | Avro binary format is more compact than JSON |

## Why Avro Over JSON?

- **Compact**: Binary encoding is 30-50% smaller than JSON
- **Schema enforcement**: Producers cannot send malformed data
- **Evolution**: Add/remove fields with compatibility guarantees
- **Type safety**: Generated classes catch errors at compile time
- **Registry**: Central schema catalog for all services
